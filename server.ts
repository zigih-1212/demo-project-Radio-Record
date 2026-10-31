import express, { type NextFunction, type Request, type Response } from "express";
import { Pool } from "pg";
import Redis from "ioredis";
import amqp, { type Channel, type ConfirmChannel, type ChannelModel } from "amqplib";
import { z } from "zod";
import { randomUUID } from "node:crypto";
import process from "node:process";

const envSchema = z.object({
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  DATABASE_URL: z.string().min(1).default("postgres://postgres:postgres@localhost:5432/pulseops"),
  REDIS_URL: z.string().min(1).default("redis://localhost:6379"),
  AMQP_URL: z.string().min(1).default("amqp://pulseops:pulseops@localhost:5672"),
  NODE_ENV: z.string().default("development"),
  CORS_ORIGIN: z.string().default("*"),
  WORKER_CONCURRENCY: z.coerce.number().int().min(1).max(50).default(5),
});
const env = envSchema.parse(process.env);

const PORT = env.PORT;
const ROLE = process.argv[2] === "worker" ? "worker" : "api";
const DATABASE_URL = env.DATABASE_URL;
const REDIS_URL = env.REDIS_URL;
const AMQP_URL = env.AMQP_URL;
const QUEUE = "pulseops.jobs.v1";
const DLQ = "pulseops.jobs.dlq";

const pool = new Pool({ connectionString: DATABASE_URL, max: 10, idleTimeoutMillis: 30_000 });
const redis = new Redis(REDIS_URL, {
  maxRetriesPerRequest: 1,
  enableOfflineQueue: false,
  retryStrategy: (times) => (times > 3 ? null : Math.min(times * 200, 1_000)),
  lazyConnect: true,
});
redis.on("error", (e: any) => console.error(JSON.stringify({ level: "error", msg: "redis", error: e?.code || e?.message || String(e) })));

let broker: ChannelModel | null = null;
let publisher: ConfirmChannel | null = null;
let consumer: Channel | null = null;
let shuttingDown = false;
let flushRunning = false;
const workerId = `worker-${randomUUID().slice(0, 8)}`;

type JobStatus = "queued" | "processing" | "succeeded" | "failed" | "cancelled";
type JobType = "campaign_publish" | "sync_catalog" | "send_digest" | "import_orders";
type JobPriority = "low" | "normal" | "high";

const createJobSchema = z.object({
  title: z.string().trim().min(3).max(120),
  type: z.enum(["campaign_publish", "sync_catalog", "send_digest", "import_orders"]),
  priority: z.enum(["low", "normal", "high"]).default("normal"),
  simulateFailure: z.boolean().default(false),
});

const listJobsSchema = z.object({
  status: z.enum(["queued", "processing", "succeeded", "failed", "cancelled"]).optional(),
  type: z.enum(["campaign_publish", "sync_catalog", "send_digest", "import_orders"]).optional(),
  priority: z.enum(["low", "normal", "high"]).optional(),
  q: z.string().trim().max(120).default(""),
  limit: z.coerce.number().int().min(1).max(100).default(50),
  offset: z.coerce.number().int().min(0).default(0),
});

const validStatuses = new Set<JobStatus>(["queued", "processing", "succeeded", "failed", "cancelled"]);

async function waitForDependencies(): Promise<void> {
  await pool.query("SELECT 1");
  await redis.connect().catch(() => undefined);
  await redis.ping();
  broker = await amqp.connect(AMQP_URL);
  publisher = await broker.createConfirmChannel();
  await publisher.assertQueue(QUEUE, { durable: true, arguments: { "x-max-priority": 10 } });
  await publisher.assertQueue(DLQ, { durable: true });
  console.log(JSON.stringify({ level: "info", msg: "dependencies ready", role: ROLE }));
}

async function initializeSchema(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS jobs (
      id UUID PRIMARY KEY,
      title VARCHAR(120) NOT NULL,
      job_type VARCHAR(40) NOT NULL CHECK (job_type IN ('campaign_publish','sync_catalog','send_digest','import_orders')),
      priority VARCHAR(10) NOT NULL CHECK (priority IN ('low','normal','high')),
      status VARCHAR(12) NOT NULL CHECK (status IN ('queued','processing','succeeded','failed','cancelled')),
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
      max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
      error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      completed_at TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS jobs_status_created_idx ON jobs(status, created_at DESC);
    CREATE INDEX IF NOT EXISTS jobs_created_idx ON jobs(created_at DESC);
    CREATE TABLE IF NOT EXISTS outbox (
      id BIGSERIAL PRIMARY KEY,
      event_key UUID NOT NULL UNIQUE,
      aggregate_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
      event_type VARCHAR(60) NOT NULL,
      payload JSONB NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      sent_at TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(available_at, id) WHERE sent_at IS NULL;
    CREATE TABLE IF NOT EXISTS job_events (
      id BIGSERIAL PRIMARY KEY,
      job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
      event_type VARCHAR(40) NOT NULL,
      message TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events(job_id, created_at);
  `);
  await pool.query(`ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;`).catch(() => undefined);
  await pool.query(`ALTER TABLE jobs ADD CONSTRAINT jobs_status_check CHECK (status IN ('queued','processing','succeeded','failed','cancelled'));`).catch(() => undefined);

  const existing = await pool.query<{ count: string }>("SELECT COUNT(*)::text AS count FROM jobs");
  if (Number(existing.rows[0]?.count ?? 0) > 0) return;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const samples = [
      { title: "Публикация подборки выходного дня", type: "campaign_publish", priority: "high", status: "succeeded", attempts: 1, minutesAgo: 4 },
      { title: "Синхронизация каталога партнёров", type: "sync_catalog", priority: "normal", status: "succeeded", attempts: 1, minutesAgo: 18 },
      { title: "Импорт заказов за сутки", type: "import_orders", priority: "normal", status: "failed", attempts: 3, minutesAgo: 31 },
      { title: "Подготовка email-дайджеста", type: "send_digest", priority: "low", status: "queued", attempts: 0, minutesAgo: 1 },
    ] as const;
    let queuedId: string | null = null;
    for (const sample of samples) {
      const id = randomUUID();
      await client.query(
        `INSERT INTO jobs(id,title,job_type,priority,status,payload,attempts,max_attempts,error,created_at,updated_at,completed_at)
         VALUES($1,$2,$3,$4,$5,$6,$7,5,$8,NOW()-($9 * INTERVAL '1 minute'),NOW()-($9 * INTERVAL '1 minute'),
         CASE WHEN $5 IN ('succeeded','failed','cancelled') THEN NOW()-($9 * INTERVAL '1 minute') ELSE NULL END)`,
        [id, sample.title, sample.type, sample.priority, sample.status, JSON.stringify({ demoSeed: true }), sample.attempts,
          sample.status === "failed" ? "Временная ошибка источника данных (демо)" : null, sample.minutesAgo],
      );
      await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'created','Демо-запись')`, [id]);
      if (sample.status === "queued") queuedId = id;
    }
    if (queuedId) {
      await client.query(
        `INSERT INTO outbox(event_key,aggregate_id,event_type,payload) VALUES($1,$2,'job.queued',$3::jsonb)`,
        [randomUUID(), queuedId, JSON.stringify({ jobId: queuedId })],
      );
      await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'queued','Поставлено в очередь')`, [queuedId]);
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function addEvent(jobId: string, type: string, message?: string): Promise<void> {
  await pool.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,$2,$3)`, [jobId, type, message ?? null]);
}

async function flushOutbox(): Promise<void> {
  if (flushRunning || !publisher || shuttingDown) return;
  flushRunning = true;
  const client = await pool.connect();
  let selectedIds: string[] = [];
  try {
    await client.query("BEGIN");
    const result = await client.query<{ id: string; event_key: string; payload: unknown; priority: JobPriority }>(
      `SELECT o.id::text, o.event_key::text, o.payload, j.priority
       FROM outbox o JOIN jobs j ON j.id=o.aggregate_id
       WHERE o.sent_at IS NULL AND o.available_at <= NOW()
       ORDER BY o.id LIMIT 25 FOR UPDATE OF o SKIP LOCKED`,
    );
    selectedIds = result.rows.map((row) => row.id);
    if (result.rows.length === 0) {
      await client.query("COMMIT");
      return;
    }
    for (const row of result.rows) {
      publisher.sendToQueue(QUEUE, Buffer.from(JSON.stringify(row.payload)), {
        persistent: true,
        contentType: "application/json",
        messageId: row.event_key,
        priority: row.priority === "high" ? 10 : row.priority === "normal" ? 5 : 1,
        timestamp: Math.floor(Date.now() / 1000),
      });
    }
    await publisher.waitForConfirms();
    await client.query("UPDATE outbox SET sent_at=NOW(), last_error=NULL WHERE id = ANY($1::text[]::bigint[])", [selectedIds]);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    if (selectedIds.length) {
      const message = error instanceof Error ? error.message.slice(0, 500) : "Unknown publisher error";
      await pool.query(
        `UPDATE outbox SET attempts=attempts+1, last_error=$1,
         available_at=NOW() + (LEAST(attempts + 1, 6) * INTERVAL '5 seconds')
         WHERE id = ANY($2::text[]::bigint[]) AND sent_at IS NULL`,
        [message, selectedIds],
      ).catch(() => undefined);
    }
    console.error(JSON.stringify({ level: "error", msg: "outbox dispatch failed", error: error instanceof Error ? ((error as any).code || error.message || String(error)) : String(error) }));
  } finally {
    client.release();
    flushRunning = false;
  }
}

function startOutboxDispatcher(): void {
  void flushOutbox();
  const timer = setInterval(() => void flushOutbox(), 1_000);
  timer.unref();
}

async function rateLimit(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const key = `pulseops:rate:${req.ip ?? "unknown"}`;
    const count = await redis.incr(key);
    if (count === 1) await redis.expire(key, 60);
    if (count > 120) {
      res.status(429).json({ error: "Слишком много запросов. Повтори через минуту." });
      return;
    }
  } catch {
    // degrade open
  }
  next();
}

function requestId(req: Request, res: Response, next: NextFunction): void {
  const id = (req.headers["x-request-id"] as string) || randomUUID();
  (req as any).requestId = id;
  res.setHeader("X-Request-Id", id);
  next();
}

const APP_HTML = `<!doctype html>
<html lang="ru"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>PulseOps</title>
<style>body{font-family:system-ui,sans-serif;margin:0;background:#f5f7fb;color:#172234}header{background:#111c31;color:#fff;padding:16px 24px}main{max-width:1100px;margin:24px auto;padding:0 16px}.card{background:#fff;border:1px solid #e7ebf2;border-radius:12px;padding:16px;margin-bottom:16px}table{width:100%;border-collapse:collapse}th,td{padding:10px;border-bottom:1px solid #f0f2f7;font-size:13px;text-align:left}.btn{border:0;border-radius:8px;padding:8px 12px;font-weight:600;cursor:pointer}.btn-primary{background:#4969e8;color:#fff}.status{padding:4px 8px;border-radius:999px;font-size:11px;font-weight:700}.status-queued{background:#fff6df;color:#ac7417}.status-processing{background:#edf1ff;color:#4969e8}.status-succeeded{background:#e6f7f0;color:#148765}.status-failed{background:#fff0f0;color:#c64c56}.status-cancelled{background:#eee;color:#666}input,select{padding:8px;border:1px solid #dfe5ef;border-radius:8px}</style>
</head><body><header><h1>PulseOps — фоновые задачи</h1></header><main>
<div class="card"><button class="btn btn-primary" onclick="openModal()">+ Новая задача</button> <button class="btn" onclick="refresh()">Обновить</button> <span id="health"></span></div>
<div class="card"><table><thead><tr><th>Задача</th><th>Тип</th><th>Приоритет</th><th>Статус</th><th>Попытки</th><th>Действия</th></tr></thead><tbody id="rows"></tbody></table></div>
<div id="modal" style="display:none;position:fixed;inset:0;background:#0006;display:grid;place-items:center"><div class="card" style="width:min(90%,420px)"><h3>Новая задача</h3>
<input id="title" placeholder="Название" style="width:100%;margin-bottom:8px"/><select id="type" style="width:100%;margin-bottom:8px"><option value="campaign_publish">Публикация кампании</option><option value="sync_catalog">Синхронизация каталога</option><option value="send_digest">Email-дайджест</option><option value="import_orders">Импорт заказов</option></select>
<select id="priority" style="width:100%;margin-bottom:8px"><option value="low">Низкий</option><option value="normal" selected>Обычный</option><option value="high">Высокий</option></select>
<label><input type="checkbox" id="fail"/> Смоделировать ошибку</label><br/><br/>
<button class="btn btn-primary" onclick="submit()">Создать</button> <button class="btn" onclick="closeModal()">Отмена</button>
</div></div>
<script>
async function api(p,o={}){const r=await fetch(p,{...o,headers:{'Content-Type':'application/json',...(o.headers||{})}});const b=await r.json().catch(()=>({error:'bad json'}));if(!r.ok)throw new Error(b.error||'error');return b;}
async function refresh(){try{const [j,h]=await Promise.all([api('/api/jobs'),api('/api/health')]);document.getElementById('rows').innerHTML=j.items.map(x=>'<tr><td>'+x.title+'</td><td>'+x.job_type+'</td><td>'+x.priority+'</td><td><span class="status status-'+x.status+'">'+x.status+'</span></td><td>'+x.attempts+'</td><td>'+(x.status==='failed'?'<button onclick="retry(\\''+x.id+'\\')">Повторить</button> ':'')+(x.status==='queued'||x.status==='processing'?'<button onclick="cancel(\\''+x.id+'\\')">Отменить</button>':'')+'</td></tr>').join('');document.getElementById('health').textContent='Сервисы: '+(h.ok?'OK':'ПРОБЛЕМА');}catch(e){document.getElementById('health').textContent=e.message;}}
async function retry(id){await api('/api/jobs/'+id+'/retry',{method:'POST'});refresh();}
async function cancel(id){await api('/api/jobs/'+id+'/cancel',{method:'POST'});refresh();}
function openModal(){document.getElementById('modal').style.display='grid';}
function closeModal(){document.getElementById('modal').style.display='none';}
async function submit(){const body={title:document.getElementById('title').value,type:document.getElementById('type').value,priority:document.getElementById('priority').value,simulateFailure:document.getElementById('fail').checked};await api('/api/jobs',{method:'POST',body:JSON.stringify(body)});closeModal();refresh();}
refresh();setInterval(refresh,5000);
</script></main></body></html>`;

const app = express();
app.disable("x-powered-by");
app.use(requestId);
app.use((req, _res, next) => {
  const origin = env.CORS_ORIGIN === "*" ? "*" : env.CORS_ORIGIN;
  _res.setHeader("Access-Control-Allow-Origin", origin);
  _res.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS");
  _res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization,Idempotency-Key,X-Request-Id");
  if (req.method === "OPTIONS") { _res.status(204).end(); return; }
  next();
});
app.use(express.json({ limit: "32kb" }));
app.use((_req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  next();
});

app.get("/", (_req, res) => res.type("html").send(APP_HTML));

app.get("/api/health", async (_req, res) => {
  const checks = { database: false, redis: false, rabbitmq: Boolean(broker), worker: false, outboxPending: 0 };
  try { await pool.query("SELECT 1"); checks.database = true; } catch { /* health only */ }
  try { checks.redis = (await redis.ping()) === "PONG"; } catch { /* health only */ }
  try { checks.worker = (await redis.get("pulseops:worker:heartbeat")) !== null; } catch { /* health only */ }
  try {
    const r = await pool.query<{ c: string }>("SELECT COUNT(*)::text AS c FROM outbox WHERE sent_at IS NULL");
    checks.outboxPending = Number(r.rows[0]?.c ?? 0);
  } catch { /* health only */ }
  const ok = checks.database && checks.redis && checks.rabbitmq;
  res.status(ok ? 200 : 503).json({ ok, role: ROLE, checks, worker: checks.worker, timestamp: new Date().toISOString() });
});

app.get("/api/stats", async (_req, res, next) => {
  try {
    const cached = await redis.get("pulseops:stats:v1").catch(() => null);
    if (cached) { res.json(JSON.parse(cached)); return; }
    const result = await pool.query<{ total: string; queued: string; processing: string; succeeded: string; failed: string; cancelled: string }>(
      `SELECT COUNT(*)::text AS total,
       COUNT(*) FILTER (WHERE status='queued')::text AS queued,
       COUNT(*) FILTER (WHERE status='processing')::text AS processing,
       COUNT(*) FILTER (WHERE status='succeeded')::text AS succeeded,
       COUNT(*) FILTER (WHERE status='failed')::text AS failed,
       COUNT(*) FILTER (WHERE status='cancelled')::text AS cancelled FROM jobs`,
    );
    const row = result.rows[0];
    const stats = Object.fromEntries(Object.entries(row).map(([key, value]) => [key, Number(value)]));
    await redis.set("pulseops:stats:v1", JSON.stringify(stats), "EX", 3).catch(() => undefined);
    res.json(stats);
  } catch (error) { next(error); }
});

app.get("/api/metrics", async (_req, res, next) => {
  try {
    const result = await pool.query<{ status: string; c: string }>("SELECT status, COUNT(*)::text AS c FROM jobs GROUP BY status");
    const lines = ["# TYPE pulseops_jobs gauge"];
    for (const row of result.rows) lines.push(`pulseops_jobs{status="${row.status}"} ${row.c}`);
    const outbox = await pool.query<{ c: string }>("SELECT COUNT(*)::text AS c FROM outbox WHERE sent_at IS NULL");
    lines.push(`pulseops_outbox_pending ${outbox.rows[0]?.c ?? 0}`);
    res.type("text/plain").send(lines.join("\n") + "\n");
  } catch (error) { next(error); }
});

app.get("/api/jobs", async (req, res, next) => {
  try {
    const parsed = listJobsSchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.issues.map((i) => `${i.path.join(".") || "query"}: ${i.message}`).join("; ") });
      return;
    }
    const { status, type, priority, q, limit, offset } = parsed.data;
    const conditions: string[] = [];
    const params: any[] = [];
    let i = 1;
    if (status) { conditions.push(`status=$${i++}`); params.push(status); }
    if (type) { conditions.push(`job_type=$${i++}`); params.push(type); }
    if (priority) { conditions.push(`priority=$${i++}`); params.push(priority); }
    if (q) { conditions.push(`title ILIKE $${i++}`); params.push(`%${q}%`); }
    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
    const countResult = await pool.query<{ c: string }>(`SELECT COUNT(*)::text AS c FROM jobs ${where}`, params);
    const total = Number(countResult.rows[0]?.c ?? 0);
    const dataParams = [...params, limit, offset];
    const result = await pool.query(
      `SELECT id,title,job_type,priority,status,attempts,max_attempts,error,payload,created_at,updated_at,completed_at
       FROM jobs ${where} ORDER BY created_at DESC LIMIT $${i++} OFFSET $${i++}`,
      dataParams,
    );
    res.json({ items: result.rows, total, limit, offset });
  } catch (error) { next(error); }
});

app.get("/api/jobs/:id", async (req, res, next) => {
  const idParsed = z.string().uuid().safeParse(req.params.id);
  if (!idParsed.success) { res.status(400).json({ error: "Некорректный ID задачи" }); return; }
  try {
    const job = await pool.query("SELECT * FROM jobs WHERE id=$1", [idParsed.data]);
    if (job.rowCount === 0) { res.status(404).json({ error: "Задача не найдена" }); return; }
    const events = await pool.query("SELECT * FROM job_events WHERE job_id=$1 ORDER BY created_at", [idParsed.data]);
    res.json({ item: job.rows[0], events: events.rows });
  } catch (error) { next(error); }
});

app.post("/api/jobs", rateLimit, async (req, res, next) => {
  const parsed = createJobSchema.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.issues.map((issue) => `${issue.path.join(".") || "body"}: ${issue.message}`).join("; ") });
    return;
  }
  const idemKey = req.headers["idempotency-key"] as string | undefined;
  if (idemKey) {
    const cached = await redis.get(`pulseops:idem:${idemKey}`).catch(() => null);
    if (cached) { res.status(200).json({ item: JSON.parse(cached), idempotent: true }); return; }
  }
  const client = await pool.connect();
  const id = randomUUID();
  try {
    await client.query("BEGIN");
    const { title, type, priority, simulateFailure } = parsed.data;
    const payload = { simulateFailure };
    const result = await client.query(
      `INSERT INTO jobs(id,title,job_type,priority,status,payload) VALUES($1,$2,$3,$4,'queued',$5::jsonb)
       RETURNING id,title,job_type,priority,status,attempts,created_at`,
      [id, title, type, priority, JSON.stringify(payload)],
    );
    await client.query(
      `INSERT INTO outbox(event_key,aggregate_id,event_type,payload) VALUES($1,$2,'job.queued',$3::jsonb)`,
      [randomUUID(), id, JSON.stringify({ jobId: id })],
    );
    await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'created','Создана пользователем')`, [id]);
    await client.query("COMMIT");
    await redis.del("pulseops:stats:v1").catch(() => undefined);
    if (idemKey) await redis.set(`pulseops:idem:${idemKey}`, JSON.stringify(result.rows[0]), "EX", 86400).catch(() => undefined);
    res.status(201).json({ item: result.rows[0] });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    next(error);
  } finally { client.release(); }
});

app.post("/api/jobs/:id/retry", rateLimit, async (req, res, next) => {
  const idParsed = z.string().uuid().safeParse(req.params.id);
  if (!idParsed.success) { res.status(400).json({ error: "Некорректный ID задачи" }); return; }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const job = await client.query<{ attempts: number; max_attempts: number }>(
      `SELECT attempts,max_attempts FROM jobs WHERE id=$1`, [idParsed.data],
    );
    if (job.rowCount === 0) { await client.query("ROLLBACK"); res.status(404).json({ error: "Задача не найдена" }); return; }
    if (job.rows[0].attempts >= job.rows[0].max_attempts) {
      await client.query("ROLLBACK");
      res.status(409).json({ error: "Превышен лимит попыток" });
      return;
    }
    const result = await client.query<{ id: string }>(
      `UPDATE jobs SET status='queued',error=NULL,updated_at=NOW(),completed_at=NULL
       WHERE id=$1 AND status='failed' RETURNING id`, [idParsed.data],
    );
    if (result.rowCount !== 1) {
      await client.query("ROLLBACK");
      res.status(409).json({ error: "Повтор доступен только для задачи со статусом «Ошибка»" });
      return;
    }
    await client.query(
      `INSERT INTO outbox(event_key,aggregate_id,event_type,payload) VALUES($1,$2,'job.queued',$3::jsonb)`,
      [randomUUID(), idParsed.data, JSON.stringify({ jobId: idParsed.data })],
    );
    await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'retried','Возвращено в очередь')`, [idParsed.data]);
    await client.query("COMMIT");
    await redis.del("pulseops:stats:v1").catch(() => undefined);
    res.json({ ok: true, id: idParsed.data, status: "queued" });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    next(error);
  } finally { client.release(); }
});

app.post("/api/jobs/:id/cancel", rateLimit, async (req, res, next) => {
  const idParsed = z.string().uuid().safeParse(req.params.id);
  if (!idParsed.success) { res.status(400).json({ error: "Некорректный ID задачи" }); return; }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query<{ id: string }>(
      `UPDATE jobs SET status='cancelled',updated_at=NOW(),completed_at=NOW()
       WHERE id=$1 AND status IN ('queued','processing') RETURNING id`, [idParsed.data],
    );
    if (result.rowCount !== 1) {
      await client.query("ROLLBACK");
      res.status(409).json({ error: "Отмена доступна только для задач в очереди или в работе" });
      return;
    }
    await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'cancelled','Отменено пользователем')`, [idParsed.data]);
    await client.query("COMMIT");
    await redis.del("pulseops:stats:v1").catch(() => undefined);
    res.json({ ok: true, id: idParsed.data, status: "cancelled" });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    next(error);
  } finally { client.release(); }
});

app.use((err: unknown, req: Request, res: Response, _next: NextFunction) => {
  console.error(JSON.stringify({ level: "error", msg: "unhandled", requestId: (req as any).requestId, error: err instanceof Error ? (((err as any).code || err.message || String(err))) : String(err) }));
  if (res.headersSent) return;
  res.status(500).json({ error: "Внутренняя ошибка сервера", requestId: (req as any).requestId });
});

async function runWorker(): Promise<void> {
  if (!broker) throw new Error("RabbitMQ connection is not ready");
  consumer = await broker.createChannel();
  await consumer.assertQueue(QUEUE, { durable: true, arguments: { "x-max-priority": 10 } });
  await consumer.assertQueue(DLQ, { durable: true });
  await consumer.prefetch(env.WORKER_CONCURRENCY);
  let processed = 0;
  const heartbeat = async () => redis.set("pulseops:worker:heartbeat", JSON.stringify({ workerId, ts: Date.now(), processed }), "EX", 30).catch(() => undefined);
  await heartbeat();
  const heartbeatTimer = setInterval(() => void heartbeat(), 10_000);
  heartbeatTimer.unref();
  await consumer.consume(QUEUE, (message) => {
    if (!message || !consumer) return;
    void (async () => {
      let jobId = "";
      try {
        const event = JSON.parse(message.content.toString()) as { jobId?: unknown };
        if (typeof event.jobId !== "string") throw new Error("Malformed job event: jobId missing");
        jobId = event.jobId;
        const claimed = await pool.query<{ id: string; payload: Record<string, unknown>; max_attempts: number }>(
          `UPDATE jobs SET status='processing',attempts=attempts+1,updated_at=NOW(),error=NULL
           WHERE id=$1 AND status='queued' RETURNING id,payload,max_attempts`, [jobId],
        );
        if (claimed.rowCount === 0) { consumer!.ack(message); return; }
        await addEvent(jobId, "processing", `Подхвачено ${workerId}`);
        await new Promise((resolve) => setTimeout(resolve, 850));
        if (claimed.rows[0].payload?.simulateFailure === true) {
          await pool.query(
            `UPDATE jobs SET status='failed',error=$2,updated_at=NOW(),completed_at=NOW() WHERE id=$1 AND status='processing'`,
            [jobId, "Смоделированная ошибка обработчика — задача доступна для повтора"],
          );
          await addEvent(jobId, "failed", "Смоделированная ошибка");
        } else {
          await pool.query(
            `UPDATE jobs SET status='succeeded',error=NULL,updated_at=NOW(),completed_at=NOW() WHERE id=$1 AND status='processing'`, [jobId],
          );
          await addEvent(jobId, "succeeded", "Успешно обработано");
        }
        processed++;
        await redis.del("pulseops:stats:v1").catch(() => undefined);
        consumer!.ack(message);
      } catch (error) {
        const detail = (error instanceof Error ? (((error as any).code || error.message || String(error))).slice(0, 500) : "Unknown worker error");
        console.error(JSON.stringify({ level: "error", msg: "worker task failed", jobId, error: detail }));
        if (jobId) {
          const job = await pool.query<{ attempts: number; max_attempts: number }>("SELECT attempts,max_attempts FROM jobs WHERE id=$1", [jobId]).catch(() => null);
          if (job && job.rows[0] && job.rows[0].attempts >= job.rows[0].max_attempts) {
            try { consumer!.sendToQueue(DLQ, message.content, { persistent: true }); } catch { /* ignore */ }
            await pool.query(`UPDATE jobs SET status='failed',error=$2,updated_at=NOW(),completed_at=NOW() WHERE id=$1`, [jobId, detail]).catch(() => undefined);
            await addEvent(jobId, "dead_letter", "Перемещено в DLQ").catch(() => undefined);
          } else {
            await pool.query(`UPDATE jobs SET status='failed',error=$2,updated_at=NOW(),completed_at=NOW() WHERE id=$1 AND status='processing'`, [jobId, detail]).catch(() => undefined);
            await addEvent(jobId, "failed", detail).catch(() => undefined);
          }
        }
        consumer!.nack(message, false, false);
      }
    })();
  });
  console.log(JSON.stringify({ level: "info", msg: "worker listening", queue: QUEUE, workerId, prefetch: env.WORKER_CONCURRENCY }));
}

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(JSON.stringify({ level: "info", msg: "shutdown", signal }));
  try { await consumer?.close(); } catch { /* connection may already be closed */ }
  try { await publisher?.close(); } catch { /* connection may already be closed */ }
  try { await broker?.close(); } catch { /* connection may already be closed */ }
  await redis.quit().catch(() => undefined);
  await pool.end().catch(() => undefined);
  process.exit(0);
}

async function main(): Promise<void> {
  try {
    await waitForDependencies();
    await initializeSchema();
    if (ROLE === "worker") {
      await runWorker();
    } else {
      app.listen(PORT, () => console.log(JSON.stringify({ level: "info", msg: "PulseOps ready", url: `http://localhost:${PORT}` })));
      startOutboxDispatcher();
    }
  } catch (error) {
    console.error(JSON.stringify({ level: "error", msg: "startup failed", error: error instanceof Error ? ((error as any).code || error.message || String(error)) : String(error) }));
    process.exitCode = 1;
    try { await consumer?.close(); } catch { /* startup may be incomplete */ }
    try { await publisher?.close(); } catch { /* startup may be incomplete */ }
    try { await broker?.close(); } catch { /* startup may be incomplete */ }
    await redis.quit().catch(() => undefined);
    await pool.end().catch(() => undefined);
  }
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
void main();
