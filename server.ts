import express, { type NextFunction, type Request, type Response } from "express";
import { Pool, type PoolClient } from "pg";
import Redis from "ioredis";
import amqp, { type Channel, type ConfirmChannel, type ChannelModel } from "amqplib";
import { z } from "zod";
import { randomUUID } from "node:crypto";
import process from "node:process";

/* ─────────────────────────────────────────────
   PulseOps — async job processing demo
   Two run modes:
   • Docker mode (default): PostgreSQL + Redis + RabbitMQ, API and worker as separate processes
   • Demo mode (DEMO_MODE=1): everything in-memory, single process, no Docker required
   ───────────────────────────────────────────── */

const envSchema = z.object({
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  DATABASE_URL: z.string().min(1).default("postgres://pulseops:pulseops@localhost:5432/pulseops"),
  REDIS_URL: z.string().min(1).default("redis://localhost:6379"),
  AMQP_URL: z.string().min(1).default("amqp://pulseops:pulseops@localhost:5672"),
  NODE_ENV: z.string().default("development"),
  CORS_ORIGIN: z.string().default("*"),
  WORKER_CONCURRENCY: z.coerce.number().int().min(1).max(50).default(5),
});
const env = envSchema.parse(process.env);

const PORT = env.PORT;
const ROLE = process.argv[2] === "worker" ? "worker" : "api";
const DEMO_MODE = process.env.DEMO_MODE === "1";
const DATABASE_URL = env.DATABASE_URL;
const REDIS_URL = env.REDIS_URL;
const AMQP_URL = env.AMQP_URL;
const QUEUE = "pulseops.jobs.v1";
const DLQ = "pulseops.jobs.dlq";
const workerId = `worker-${randomUUID().slice(0, 8)}`;

type JobStatus = "queued" | "processing" | "succeeded" | "failed" | "cancelled";
type JobType = "campaign_publish" | "sync_catalog" | "send_digest" | "import_orders";
type JobPriority = "low" | "normal" | "high";

interface JobRow {
  id: string;
  title: string;
  job_type: JobType;
  priority: JobPriority;
  status: JobStatus;
  payload: Record<string, unknown>;
  attempts: number;
  max_attempts: number;
  error: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
}
interface JobEventRow {
  id: number;
  job_id: string;
  event_type: string;
  message: string | null;
  created_at: string;
}
interface OutboxRow {
  id: string;
  event_key: string;
  payload: unknown;
  priority: JobPriority;
}
interface ListFilter {
  status?: JobStatus;
  type?: JobType;
  priority?: JobPriority;
  q: string;
  limit: number;
  offset: number;
}
interface CreateJobInput {
  title: string;
  type: JobType;
  priority: JobPriority;
  simulateFailure: boolean;
}

interface IStore {
  init(): Promise<void>;
  ping(): Promise<boolean>;
  listJobs(f: ListFilter): Promise<{ items: JobRow[]; total: number }>;
  getJob(id: string): Promise<JobRow | null>;
  getEvents(jobId: string): Promise<JobEventRow[]>;
  createJob(input: CreateJobInput): Promise<JobRow>;
  retryJob(id: string): Promise<"ok" | "not_found" | "not_failed" | "limit">;
  cancelJob(id: string): Promise<"ok" | "not_found" | "not_active">;
  claim(jobId: string): Promise<JobRow | null>;
  finish(jobId: string, status: "succeeded" | "failed", error?: string | null): Promise<void>;
  addEvent(jobId: string, type: string, message?: string | null): Promise<void>;
  outboxPending(): Promise<OutboxRow[]>;
  markOutboxSent(ids: string[]): Promise<void>;
  deferOutbox(ids: string[], err: string): Promise<void>;
  stats(): Promise<Record<string, number>>;
  outboxCount(): Promise<number>;
}

interface ICache {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, exSeconds?: number): Promise<void>;
  del(key: string): Promise<void>;
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number): Promise<void>;
  ping(): Promise<boolean>;
}

interface QueueMessage {
  jobId: string;
  raw: Buffer;
  ack(): void;
  nack(): void;
  dlq(): void;
}
interface IQueue {
  publish(payload: unknown, priority: JobPriority): Promise<void>;
  publishDLQ(payload: unknown): Promise<void>;
  consume(handler: (msg: QueueMessage) => void): Promise<void>;
  close(): Promise<void>;
  healthy(): boolean;
}

/* ─────────────── Postgres store ─────────────── */

class PostgresStore implements IStore {
  constructor(private pool: Pool) {}

  async init(): Promise<void> {
    await this.pool.query(`
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
    await this.pool.query(`ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;`).catch(() => undefined);
    await this.pool.query(
      `ALTER TABLE jobs ADD CONSTRAINT jobs_status_check CHECK (status IN ('queued','processing','succeeded','failed','cancelled'));`,
    ).catch(() => undefined);

    const existing = await this.pool.query<{ count: string }>("SELECT COUNT(*)::text AS count FROM jobs");
    if (Number(existing.rows[0]?.count ?? 0) > 0) return;

    const client = await this.pool.connect();
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
          [
            id, sample.title, sample.type, sample.priority, sample.status,
            JSON.stringify({ demoSeed: true }), sample.attempts,
            sample.status === "failed" ? "Временная ошибка источника данных (демо)" : null, sample.minutesAgo,
          ],
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

  async ping(): Promise<boolean> {
    try {
      await this.pool.query("SELECT 1");
      return true;
    } catch {
      return false;
    }
  }

  async listJobs(f: ListFilter): Promise<{ items: JobRow[]; total: number }> {
    const conditions: string[] = [];
    const params: unknown[] = [];
    let i = 1;
    if (f.status) { conditions.push(`status=$${i++}`); params.push(f.status); }
    if (f.type) { conditions.push(`job_type=$${i++}`); params.push(f.type); }
    if (f.priority) { conditions.push(`priority=$${i++}`); params.push(f.priority); }
    if (f.q) { conditions.push(`title ILIKE $${i++}`); params.push(`%${f.q}%`); }
    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
    const countResult = await this.pool.query<{ c: string }>(`SELECT COUNT(*)::text AS c FROM jobs ${where}`, params);
    const total = Number(countResult.rows[0]?.c ?? 0);
    const dataParams = [...params, f.limit, f.offset];
    const result = await this.pool.query(
      `SELECT id,title,job_type,priority,status,attempts,max_attempts,error,payload,created_at,updated_at,completed_at
       FROM jobs ${where} ORDER BY created_at DESC LIMIT $${i++} OFFSET $${i++}`,
      dataParams,
    );
    return { items: result.rows.map(toJobRow), total };
  }

  async getJob(id: string): Promise<JobRow | null> {
    const result = await this.pool.query("SELECT * FROM jobs WHERE id=$1", [id]);
    return result.rowCount ? toJobRow(result.rows[0]) : null;
  }

  async getEvents(jobId: string): Promise<JobEventRow[]> {
    const result = await this.pool.query(
      "SELECT id::text AS id, job_id::text AS job_id, event_type, message, created_at FROM job_events WHERE job_id=$1 ORDER BY created_at",
      [jobId],
    );
    return result.rows.map((row) => ({ ...row, id: Number(row.id), created_at: new Date(row.created_at).toISOString() }));
  }

  async createJob(input: CreateJobInput): Promise<JobRow> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const id = randomUUID();
      const result = await client.query(
        `INSERT INTO jobs(id,title,job_type,priority,status,payload) VALUES($1,$2,$3,$4,'queued',$5::jsonb)
         RETURNING *`,
        [id, input.title, input.type, input.priority, JSON.stringify({ simulateFailure: input.simulateFailure })],
      );
      await client.query(
        `INSERT INTO outbox(event_key,aggregate_id,event_type,payload) VALUES($1,$2,'job.queued',$3::jsonb)`,
        [randomUUID(), id, JSON.stringify({ jobId: id })],
      );
      await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'created','Создана пользователем')`, [id]);
      await client.query("COMMIT");
      return toJobRow(result.rows[0]);
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async retryJob(id: string): Promise<"ok" | "not_found" | "not_failed" | "limit"> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const job = await client.query<{ attempts: number; max_attempts: number }>(
        `SELECT attempts,max_attempts FROM jobs WHERE id=$1`, [id],
      );
      if (job.rowCount === 0) { await client.query("ROLLBACK"); return "not_found"; }
      if (job.rows[0].attempts >= job.rows[0].max_attempts) { await client.query("ROLLBACK"); return "limit"; }
      const result = await client.query(
        `UPDATE jobs SET status='queued',error=NULL,updated_at=NOW(),completed_at=NULL
         WHERE id=$1 AND status='failed' RETURNING id`, [id],
      );
      if (result.rowCount !== 1) { await client.query("ROLLBACK"); return "not_failed"; }
      await client.query(
        `INSERT INTO outbox(event_key,aggregate_id,event_type,payload) VALUES($1,$2,'job.queued',$3::jsonb)`,
        [randomUUID(), id, JSON.stringify({ jobId: id })],
      );
      await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'retried','Возвращено в очередь')`, [id]);
      await client.query("COMMIT");
      return "ok";
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async cancelJob(id: string): Promise<"ok" | "not_found" | "not_active"> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const exists = await client.query("SELECT 1 FROM jobs WHERE id=$1", [id]);
      if (exists.rowCount === 0) { await client.query("ROLLBACK"); return "not_found"; }
      const result = await client.query(
        `UPDATE jobs SET status='cancelled',updated_at=NOW(),completed_at=NOW()
         WHERE id=$1 AND status IN ('queued','processing') RETURNING id`, [id],
      );
      if (result.rowCount !== 1) { await client.query("ROLLBACK"); return "not_active"; }
      await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'cancelled','Отменено пользователем')`, [id]);
      await client.query("COMMIT");
      return "ok";
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async claim(jobId: string): Promise<JobRow | null> {
    const result = await this.pool.query(
      `UPDATE jobs SET status='processing',attempts=attempts+1,updated_at=NOW(),error=NULL
       WHERE id=$1 AND status='queued' RETURNING *`, [jobId],
    );
    return result.rowCount ? toJobRow(result.rows[0]) : null;
  }

  async finish(jobId: string, status: "succeeded" | "failed", error?: string | null): Promise<void> {
    await this.pool.query(
      `UPDATE jobs SET status=$2,error=$3,updated_at=NOW(),completed_at=NOW() WHERE id=$1`,
      [jobId, status, error ?? null],
    );
  }

  async addEvent(jobId: string, type: string, message?: string | null): Promise<void> {
    await this.pool.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,$2,$3)`, [jobId, type, message ?? null]);
  }

  async outboxPending(): Promise<OutboxRow[]> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<{ id: string; event_key: string; payload: unknown; priority: JobPriority }>(
        `SELECT o.id::text, o.event_key::text, o.payload, j.priority
         FROM outbox o JOIN jobs j ON j.id=o.aggregate_id
         WHERE o.sent_at IS NULL AND o.available_at <= NOW()
         ORDER BY o.id LIMIT 25 FOR UPDATE OF o SKIP LOCKED`,
      );
      await client.query("COMMIT");
      return result.rows;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async markOutboxSent(ids: string[]): Promise<void> {
    if (!ids.length) return;
    await this.pool.query("UPDATE outbox SET sent_at=NOW(), last_error=NULL WHERE id = ANY($1::text[]::bigint[])", [ids]);
  }

  async deferOutbox(ids: string[], err: string): Promise<void> {
    if (!ids.length) return;
    await this.pool.query(
      `UPDATE outbox SET attempts=attempts+1, last_error=$1,
       available_at=NOW() + (LEAST(attempts + 1, 6) * INTERVAL '5 seconds')
       WHERE id = ANY($2::text[]::bigint[]) AND sent_at IS NULL`,
      [err, ids],
    );
  }

  async stats(): Promise<Record<string, number>> {
    const result = await this.pool.query<{ status: string; c: string }>(
      "SELECT status, COUNT(*)::text AS c FROM jobs GROUP BY status",
    );
    const stats: Record<string, number> = { queued: 0, processing: 0, succeeded: 0, failed: 0, cancelled: 0 };
    for (const row of result.rows) stats[row.status] = Number(row.c);
    return stats;
  }

  async outboxCount(): Promise<number> {
    const result = await this.pool.query<{ c: string }>(
      "SELECT COUNT(*)::text AS c FROM outbox WHERE sent_at IS NULL",
    );
    return Number(result.rows[0]?.c ?? 0);
  }
}

function toJobRow(row: any): JobRow {
  return {
    id: row.id,
    title: row.title,
    job_type: row.job_type,
    priority: row.priority,
    status: row.status,
    payload: row.payload ?? {},
    attempts: Number(row.attempts),
    max_attempts: Number(row.max_attempts),
    error: row.error ?? null,
    created_at: new Date(row.created_at).toISOString(),
    updated_at: new Date(row.updated_at).toISOString(),
    completed_at: row.completed_at ? new Date(row.completed_at).toISOString() : null,
  };
}

/* ─────────────── In-memory store (demo mode) ─────────────── */

interface OutboxEntry extends OutboxRow {
  aggregate_id: string;
  event_type: string;
  sent: boolean;
  attempts: number;
  availableAt: number;
}

class MemoryStore implements IStore {
  private jobs = new Map<string, JobRow>();
  private events: JobEventRow[] = [];
  private outbox: OutboxEntry[] = [];
  private eventSeq = 1;

  async init(): Promise<void> {
    const now = Date.now();
    const samples: Array<{ title: string; type: JobType; priority: JobPriority; status: JobStatus; attempts: number; minutesAgo: number }> = [
      { title: "Публикация подборки выходного дня", type: "campaign_publish", priority: "high", status: "succeeded", attempts: 1, minutesAgo: 4 },
      { title: "Синхронизация каталога партнёров", type: "sync_catalog", priority: "normal", status: "succeeded", attempts: 1, minutesAgo: 18 },
      { title: "Импорт заказов за сутки", type: "import_orders", priority: "normal", status: "failed", attempts: 3, minutesAgo: 31 },
      { title: "Подготовка email-дайджеста", type: "send_digest", priority: "low", status: "queued", attempts: 0, minutesAgo: 1 },
    ];
    for (const sample of samples) {
      const id = randomUUID();
      const ts = new Date(now - sample.minutesAgo * 60_000).toISOString();
      this.jobs.set(id, {
        id,
        title: sample.title,
        job_type: sample.type,
        priority: sample.priority,
        status: sample.status,
        payload: { demoSeed: true },
        attempts: sample.attempts,
        max_attempts: 5,
        error: sample.status === "failed" ? "Временная ошибка источника данных (демо)" : null,
        created_at: ts,
        updated_at: ts,
        completed_at: sample.status === "queued" ? null : ts,
      });
      this.events.push({ id: this.eventSeq++, job_id: id, event_type: "created", message: "Демо-запись", created_at: ts });
      if (sample.status === "queued") {
        this.outbox.push({ id: randomUUID(), event_key: randomUUID(), aggregate_id: id, event_type: "job.queued", payload: { jobId: id }, priority: sample.priority, sent: false, attempts: 0, availableAt: 0 });
        this.events.push({ id: this.eventSeq++, job_id: id, event_type: "queued", message: "Поставлено в очередь", created_at: ts });
      }
    }
  }

  async ping(): Promise<boolean> {
    return true;
  }

  async listJobs(f: ListFilter): Promise<{ items: JobRow[]; total: number }> {
    let items = [...this.jobs.values()];
    if (f.status) items = items.filter((j) => j.status === f.status);
    if (f.type) items = items.filter((j) => j.job_type === f.type);
    if (f.priority) items = items.filter((j) => j.priority === f.priority);
    if (f.q) items = items.filter((j) => j.title.toLowerCase().includes(f.q.toLowerCase()));
    items.sort((a, b) => b.created_at.localeCompare(a.created_at));
    const total = items.length;
    return { items: items.slice(f.offset, f.offset + f.limit), total };
  }

  async getJob(id: string): Promise<JobRow | null> {
    return this.jobs.get(id) ?? null;
  }

  async getEvents(jobId: string): Promise<JobEventRow[]> {
    return this.events.filter((e) => e.job_id === jobId);
  }

  async createJob(input: CreateJobInput): Promise<JobRow> {
    const id = randomUUID();
    const ts = new Date().toISOString();
    const job: JobRow = {
      id, title: input.title, job_type: input.type, priority: input.priority, status: "queued",
      payload: { simulateFailure: input.simulateFailure }, attempts: 0, max_attempts: 5,
      error: null, created_at: ts, updated_at: ts, completed_at: null,
    };
    this.jobs.set(id, job);
    this.outbox.push({ id: randomUUID(), event_key: randomUUID(), aggregate_id: id, event_type: "job.queued", payload: { jobId: id }, priority: input.priority, sent: false, attempts: 0, availableAt: 0 });
    this.events.push({ id: this.eventSeq++, job_id: id, event_type: "created", message: "Создана пользователем", created_at: ts });
    return job;
  }

  async retryJob(id: string): Promise<"ok" | "not_found" | "not_failed" | "limit"> {
    const job = this.jobs.get(id);
    if (!job) return "not_found";
    if (job.attempts >= job.max_attempts) return "limit";
    if (job.status !== "failed") return "not_failed";
    job.status = "queued";
    job.error = null;
    job.updated_at = new Date().toISOString();
    job.completed_at = null;
    this.outbox.push({ id: randomUUID(), event_key: randomUUID(), aggregate_id: id, event_type: "job.queued", payload: { jobId: id }, priority: job.priority, sent: false, attempts: 0, availableAt: 0 });
    this.events.push({ id: this.eventSeq++, job_id: id, event_type: "retried", message: "Возвращено в очередь", created_at: new Date().toISOString() });
    return "ok";
  }

  async cancelJob(id: string): Promise<"ok" | "not_found" | "not_active"> {
    const job = this.jobs.get(id);
    if (!job) return "not_found";
    if (job.status !== "queued" && job.status !== "processing") return "not_active";
    job.status = "cancelled";
    job.updated_at = new Date().toISOString();
    job.completed_at = new Date().toISOString();
    this.events.push({ id: this.eventSeq++, job_id: id, event_type: "cancelled", message: "Отменено пользователем", created_at: new Date().toISOString() });
    return "ok";
  }

  async claim(jobId: string): Promise<JobRow | null> {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== "queued") return null;
    job.status = "processing";
    job.attempts += 1;
    job.updated_at = new Date().toISOString();
    job.error = null;
    return job;
  }

  async finish(jobId: string, status: "succeeded" | "failed", error?: string | null): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;
    job.status = status;
    job.error = error ?? null;
    job.updated_at = new Date().toISOString();
    job.completed_at = new Date().toISOString();
  }

  async addEvent(jobId: string, type: string, message?: string | null): Promise<void> {
    this.events.push({ id: this.eventSeq++, job_id: jobId, event_type: type, message: message ?? null, created_at: new Date().toISOString() });
  }

  async outboxPending(): Promise<OutboxRow[]> {
    const now = Date.now();
    return this.outbox.filter((o) => !o.sent && o.availableAt <= now).slice(0, 25);
  }

  async markOutboxSent(ids: string[]): Promise<void> {
    for (const o of this.outbox) if (ids.includes(o.id)) o.sent = true;
  }

  async deferOutbox(ids: string[], _err: string): Promise<void> {
    for (const o of this.outbox) {
      if (ids.includes(o.id)) {
        o.attempts += 1;
        o.availableAt = Date.now() + Math.min(o.attempts + 1, 6) * 5000;
      }
    }
  }

  async stats(): Promise<Record<string, number>> {
    const stats: Record<string, number> = { queued: 0, processing: 0, succeeded: 0, failed: 0, cancelled: 0 };
    for (const job of this.jobs.values()) stats[job.status] = (stats[job.status] ?? 0) + 1;
    return stats;
  }

  async outboxCount(): Promise<number> {
    return this.outbox.filter((o) => !o.sent).length;
  }
}

/* ─────────────── Cache adapters ─────────────── */

class RedisCache implements ICache {
  constructor(private redis: Redis) {}
  async get(key: string) { return this.redis.get(key); }
  async set(key: string, value: string, exSeconds?: number) {
    if (exSeconds) await this.redis.set(key, value, "EX", exSeconds);
    else await this.redis.set(key, value);
  }
  async del(key: string) { await this.redis.del(key); }
  async incr(key: string) { return this.redis.incr(key); }
  async expire(key: string, seconds: number) { await this.redis.expire(key, seconds); }
  async ping() { return (await this.redis.ping()) === "PONG"; }
}

class MemoryCache implements ICache {
  private data = new Map<string, { value: string; expiresAt?: number }>();
  private counters = new Map<string, { count: number; expiresAt: number }>();

  async get(key: string) {
    const entry = this.data.get(key);
    if (!entry) return null;
    if (entry.expiresAt && entry.expiresAt < Date.now()) { this.data.delete(key); return null; }
    return entry.value;
  }
  async set(key: string, value: string, exSeconds?: number) {
    this.data.set(key, { value, expiresAt: exSeconds ? Date.now() + exSeconds * 1000 : undefined });
  }
  async del(key: string) { this.data.delete(key); }
  async incr(key: string) {
    const now = Date.now();
    let counter = this.counters.get(key);
    if (!counter || counter.expiresAt < now) {
      counter = { count: 0, expiresAt: now + 60_000 };
      this.counters.set(key, counter);
    }
    counter.count += 1;
    return counter.count;
  }
  async expire(key: string, seconds: number) {
    const counter = this.counters.get(key);
    if (counter) counter.expiresAt = Date.now() + seconds * 1000;
  }
  async ping() { return true; }
}

/* ─────────────── Queue adapters ─────────────── */

class RabbitQueue implements IQueue {
  private publisher: ConfirmChannel | null = null;

  constructor(private broker: ChannelModel) {}

  private async getPublisher(): Promise<ConfirmChannel> {
    if (!this.publisher) {
      this.publisher = await this.broker.createConfirmChannel();
      await this.publisher.assertQueue(QUEUE, { durable: true, arguments: { "x-max-priority": 10 } });
      await this.publisher.assertQueue(DLQ, { durable: true });
    }
    return this.publisher;
  }

  async publish(payload: unknown, priority: JobPriority): Promise<void> {
    const channel = await this.getPublisher();
    channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(payload)), {
      persistent: true,
      contentType: "application/json",
      priority: priority === "high" ? 10 : priority === "normal" ? 5 : 1,
      timestamp: Math.floor(Date.now() / 1000),
    });
    await channel.waitForConfirms();
  }

  async publishDLQ(payload: unknown): Promise<void> {
    const channel = await this.getPublisher();
    channel.sendToQueue(DLQ, Buffer.from(JSON.stringify(payload)), { persistent: true });
    await channel.waitForConfirms();
  }

  async consume(handler: (msg: QueueMessage) => void): Promise<void> {
    const channel = await this.broker.createChannel();
    await channel.assertQueue(QUEUE, { durable: true, arguments: { "x-max-priority": 10 } });
    await channel.prefetch(env.WORKER_CONCURRENCY);
    channel.consume(QUEUE, (message) => {
      if (!message) return;
      let jobId = "";
      try {
        const event = JSON.parse(message.content.toString()) as { jobId?: unknown };
        if (typeof event.jobId !== "string") throw new Error("Malformed job event");
        jobId = event.jobId;
      } catch {
        channel.nack(message, false, false);
        return;
      }
      handler({
        jobId,
        raw: message.content,
        ack: () => channel.ack(message),
        nack: () => channel.nack(message, false, false),
        dlq: () => {
          try { channel.sendToQueue(DLQ, message.content, { persistent: true }); } catch { /* ignore */ }
        },
      });
    });
    console.log(JSON.stringify({ level: "info", msg: "worker listening", queue: QUEUE, workerId, prefetch: env.WORKER_CONCURRENCY }));
  }

  async close(): Promise<void> {
    try { await this.publisher?.close(); } catch { /* already closed */ }
  }

  healthy(): boolean {
    // RabbitQueue is only constructed after a successful amqp.connect()
    return true;
  }
}

class MemoryQueue implements IQueue {
  private items: Array<{ jobId: string; priority: JobPriority }> = [];
  private dlqItems: QueueMessage[] = [];
  private timer: ReturnType<typeof setInterval> | null = null;
  private active = 0;

  async publish(payload: unknown, priority: JobPriority): Promise<void> {
    const jobId = (payload as { jobId?: string }).jobId;
    if (typeof jobId !== "string") return;
    this.items.push({ jobId, priority });
    this.items.sort((a, b) => priorityWeight(b.priority) - priorityWeight(a.priority));
  }

  async publishDLQ(payload: unknown): Promise<void> {
    const jobId = (payload as { jobId?: string }).jobId ?? "";
    this.dlqItems.push({ jobId, raw: Buffer.from(JSON.stringify(payload)), ack: () => undefined, nack: () => undefined, dlq: () => undefined });
  }

  async consume(handler: (msg: QueueMessage) => void): Promise<void> {
    if (this.timer) return;
    this.timer = setInterval(() => {
      while (this.items.length > 0 && this.active < env.WORKER_CONCURRENCY) {
        const item = this.items.shift()!;
        this.active += 1;
        handler({
          jobId: item.jobId,
          raw: Buffer.from(JSON.stringify({ jobId: item.jobId })),
          ack: () => { this.active -= 1; },
          nack: () => { this.active -= 1; },
          dlq: () => { void this.publishDLQ({ jobId: item.jobId }); },
        });
      }
    }, 100);
    this.timer.unref();
  }

  async close(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  healthy(): boolean {
    return true;
  }
}

function priorityWeight(p: JobPriority): number {
  return p === "high" ? 3 : p === "normal" ? 2 : 1;
}

/* ─────────────── Shared state ─────────────── */

let store!: IStore;
let cache!: ICache;
let queue!: IQueue;
let pgPool: Pool | null = null;
let redisClient: Redis | null = null;
let amqpBroker: ChannelModel | null = null;
let shuttingDown = false;
let flushRunning = false;
let processedCount = 0;

/* ─────────────── Schemas ─────────────── */

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

/* ─────────────── Outbox dispatcher ─────────────── */

async function flushOutbox(): Promise<void> {
  if (flushRunning || shuttingDown) return;
  flushRunning = true;
  const selectedIds: string[] = [];
  try {
    const pending = await store.outboxPending();
    if (pending.length === 0) return;
    for (const row of pending) {
      selectedIds.push(row.id);
      await queue.publish(row.payload, row.priority);
    }
    await store.markOutboxSent(selectedIds);
  } catch (error) {
    const message = error instanceof Error ? ((error as any).code || error.message || String(error)) : String(error);
    console.error(JSON.stringify({ level: "error", msg: "outbox dispatch failed", error: message }));
    if (selectedIds.length) {
      await store.deferOutbox(selectedIds, message).catch(() => undefined);
    }
  } finally {
    flushRunning = false;
  }
}

function startOutboxDispatcher(): void {
  void flushOutbox();
  const timer = setInterval(() => void flushOutbox(), 1_000);
  timer.unref();
}

/* ─────────────── Worker ─────────────── */

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function handleJobMessage(jobId: string, actions: { ack(): void; nack(): void; dlq(): void }): Promise<void> {
  try {
    const claimed = await store.claim(jobId);
    if (!claimed) { actions.ack(); return; }
    await store.addEvent(jobId, "processing", `Подхвачено ${workerId}`);
    await delay(850);
    if (claimed.payload?.simulateFailure === true) {
      await store.finish(jobId, "failed", "Смоделированная ошибка обработчика — задача доступна для повтора");
      await store.addEvent(jobId, "failed", "Смоделированная ошибка");
    } else {
      await store.finish(jobId, "succeeded");
      await store.addEvent(jobId, "succeeded", "Успешно обработано");
    }
    processedCount += 1;
    await cache.del("pulseops:stats:v1").catch(() => undefined);
    actions.ack();
  } catch (error) {
    const detail = error instanceof Error ? (((error as any).code || error.message || String(error))).slice(0, 500) : "Unknown worker error";
    console.error(JSON.stringify({ level: "error", msg: "worker task failed", jobId, error: detail }));
    const job = await store.getJob(jobId).catch(() => null);
    if (job && job.attempts >= job.max_attempts) {
      actions.dlq();
      await store.finish(jobId, "failed", detail).catch(() => undefined);
      await store.addEvent(jobId, "dead_letter", "Перемещено в DLQ").catch(() => undefined);
    } else {
      await store.finish(jobId, "failed", detail).catch(() => undefined);
      await store.addEvent(jobId, "failed", detail).catch(() => undefined);
    }
    actions.nack();
  }
}

function startWorkerHeartbeat(): void {
  const heartbeat = async () => {
    await cache.set("pulseops:worker:heartbeat", JSON.stringify({ workerId, ts: Date.now(), processed: processedCount }), 30).catch(() => undefined);
  };
  void heartbeat();
  const timer = setInterval(() => void heartbeat(), 10_000);
  timer.unref();
}

/* ─────────────── HTTP app ─────────────── */

const APP_HTML = `<!doctype html>
<html lang="ru"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>PulseOps</title>
<style>body{font-family:system-ui,sans-serif;margin:0;background:#f5f7fb;color:#172234}header{background:#111c31;color:#fff;padding:16px 24px}main{max-width:1100px;margin:24px auto;padding:0 16px}.card{background:#fff;border:1px solid #e7ebf2;border-radius:12px;padding:16px;margin-bottom:16px}table{width:100%;border-collapse:collapse}th,td{padding:10px;border-bottom:1px solid #f0f2f7;font-size:13px;text-align:left}.btn{border:0;border-radius:8px;padding:8px 12px;font-weight:600;cursor:pointer}.btn-primary{background:#4969e8;color:#fff}.status{padding:4px 8px;border-radius:999px;font-size:11px;font-weight:700}.status-queued{background:#fff6df;color:#ac7417}.status-processing{background:#edf1ff;color:#4969e8}.status-succeeded{background:#e6f7f0;color:#148765}.status-failed{background:#fff0f0;color:#c64c56}.status-cancelled{background:#eee;color:#666}input,select{padding:8px;border:1px solid #dfe5ef;border-radius:8px}.badge{display:inline-block;background:#4969e8;color:#fff;border-radius:6px;padding:2px 8px;font-size:11px;margin-left:8px}</style>
</head><body><header><h1>PulseOps — фоновые задачи</h1></header><main>
<div class="card"><button class="btn btn-primary" onclick="openModal()">+ Новая задача</button> <button class="btn" onclick="refresh()">Обновить</button> <span id="health"></span><span id="mode" class="badge"></span></div>
<div class="card"><table><thead><tr><th>Задача</th><th>Тип</th><th>Приоритет</th><th>Статус</th><th>Попытки</th><th>Действия</th></tr></thead><tbody id="rows"></tbody></table></div>
<div id="modal" style="display:none;position:fixed;inset:0;background:#0006;place-items:center"><div class="card" style="width:min(90%,420px);margin:auto"><h3>Новая задача</h3>
<input id="title" placeholder="Название" style="width:100%;margin-bottom:8px"/><select id="type" style="width:100%;margin-bottom:8px"><option value="campaign_publish">Публикация кампании</option><option value="sync_catalog">Синхронизация каталога</option><option value="send_digest">Email-дайджест</option><option value="import_orders">Импорт заказов</option></select>
<select id="priority" style="width:100%;margin-bottom:8px"><option value="low">Низкий</option><option value="normal" selected>Обычный</option><option value="high">Высокий</option></select>
<label><input type="checkbox" id="fail"/> Смоделировать ошибку</label><br/><br/>
<button class="btn btn-primary" onclick="submit()">Создать</button> <button class="btn" onclick="closeModal()">Отмена</button>
</div></div>
<script>
async function api(p,o={}){const r=await fetch(p,{...o,headers:{'Content-Type':'application/json',...(o.headers||{})}});const b=await r.json().catch(()=>({error:'bad json'}));if(!r.ok)throw new Error(b.error||'error');return b;}
async function refresh(){try{const [j,h]=await Promise.all([api('/api/jobs'),api('/api/health')]);document.getElementById('rows').innerHTML=j.items.map(x=>'<tr><td>'+x.title+'</td><td>'+x.job_type+'</td><td>'+x.priority+'</td><td><span class="status status-'+x.status+'">'+x.status+'</span></td><td>'+x.attempts+'</td><td>'+(x.status==='failed'?'<button onclick="retry(\\''+x.id+'\\')">Повторить</button> ':'')+(x.status==='queued'||x.status==='processing'?'<button onclick="cancel(\\''+x.id+'\\')">Отменить</button>':'')+'</td></tr>').join('');document.getElementById('health').textContent='Сервисы: '+(h.ok?'OK':'ПРОБЛЕМА');document.getElementById('mode').textContent=h.mode==='demo'?'демо-режим (без Docker)':'Docker';}catch(e){document.getElementById('health').textContent=e.message;}}
async function retry(id){await api('/api/jobs/'+id+'/retry',{method:'POST'});refresh();}
async function cancel(id){await api('/api/jobs/'+id+'/cancel',{method:'POST'});refresh();}
function openModal(){document.getElementById('modal').style.display='grid';}
function closeModal(){document.getElementById('modal').style.display='none';}
async function submit(){const body={title:document.getElementById('title').value,type:document.getElementById('type').value,priority:document.getElementById('priority').value,simulateFailure:document.getElementById('fail').checked};await api('/api/jobs',{method:'POST',body:JSON.stringify(body)});closeModal();refresh();}
refresh();setInterval(refresh,5000);
</script></main></body></html>`;

const app = express();
app.disable("x-powered-by");
app.use((req, _res, next) => {
  const id = (req.headers["x-request-id"] as string) || randomUUID();
  (req as any).requestId = id;
  _res.setHeader("X-Request-Id", id);
  next();
});
app.use((req, res, next) => {
  const origin = env.CORS_ORIGIN === "*" ? "*" : env.CORS_ORIGIN;
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization,Idempotency-Key,X-Request-Id");
  if (req.method === "OPTIONS") { res.status(204).end(); return; }
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
  const checks = { database: false, redis: false, rabbitmq: false, worker: false, outboxPending: 0 };
  checks.database = await store.ping();
  checks.redis = await cache.ping();
  checks.rabbitmq = queue.healthy();
  try { checks.worker = (await cache.get("pulseops:worker:heartbeat")) !== null; } catch { /* ignore */ }
  try { checks.outboxPending = await store.outboxCount(); } catch { /* ignore */ }
  const ok = checks.database && checks.redis && checks.rabbitmq;
  res.status(ok ? 200 : 503).json({ ok, role: ROLE, mode: DEMO_MODE ? "demo" : "docker", checks, worker: checks.worker, timestamp: new Date().toISOString() });
});

app.get("/api/stats", async (req, res, next) => {
  try {
    const cached = await cache.get("pulseops:stats:v1").catch(() => null);
    if (cached) { res.json(JSON.parse(cached)); return; }
    const stats = await store.stats();
    await cache.set("pulseops:stats:v1", JSON.stringify(stats), 3).catch(() => undefined);
    res.json(stats);
  } catch (error) { next(error); }
});

app.get("/api/metrics", async (_req, res, next) => {
  try {
    const stats = await store.stats();
    const lines = ["# TYPE pulseops_jobs gauge"];
    for (const [status, count] of Object.entries(stats)) lines.push(`pulseops_jobs{status="${status}"} ${count}`);
    lines.push(`pulseops_outbox_pending ${await store.outboxCount()}`);
    res.type("text/plain").send(lines.join("\n") + "\n");
  } catch (error) { next(error); }
});

app.get("/api/jobs", async (req, res, next) => {
  try {
    const parsed = listJobsSchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.issues.map((issue) => `${issue.path.join(".") || "query"}: ${issue.message}`).join("; ") });
      return;
    }
    const result = await store.listJobs(parsed.data);
    res.json({ items: result.items, total: result.total, limit: parsed.data.limit, offset: parsed.data.offset });
  } catch (error) { next(error); }
});

app.get("/api/jobs/:id", async (req, res, next) => {
  const idParsed = z.string().uuid().safeParse(req.params.id);
  if (!idParsed.success) { res.status(400).json({ error: "Некорректный ID задачи" }); return; }
  try {
    const job = await store.getJob(idParsed.data);
    if (!job) { res.status(404).json({ error: "Задача не найдена" }); return; }
    const events = await store.getEvents(idParsed.data);
    res.json({ item: job, events });
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
    const cached = await cache.get(`pulseops:idem:${idemKey}`).catch(() => null);
    if (cached) { res.status(200).json({ item: JSON.parse(cached), idempotent: true }); return; }
  }
  try {
    const job = await store.createJob(parsed.data);
    await cache.del("pulseops:stats:v1").catch(() => undefined);
    if (idemKey) await cache.set(`pulseops:idem:${idemKey}`, JSON.stringify(job), 86400).catch(() => undefined);
    res.status(201).json({ item: job });
  } catch (error) { next(error); }
});

app.post("/api/jobs/:id/retry", rateLimit, async (req, res, next) => {
  const idParsed = z.string().uuid().safeParse(req.params.id);
  if (!idParsed.success) { res.status(400).json({ error: "Некорректный ID задачи" }); return; }
  try {
    const result = await store.retryJob(idParsed.data);
    if (result === "not_found") { res.status(404).json({ error: "Задача не найдена" }); return; }
    if (result === "limit") { res.status(409).json({ error: "Превышен лимит попыток" }); return; }
    if (result === "not_failed") { res.status(409).json({ error: "Повтор доступен только для задачи со статусом «Ошибка»" }); return; }
    await cache.del("pulseops:stats:v1").catch(() => undefined);
    res.json({ ok: true, id: idParsed.data, status: "queued" });
  } catch (error) { next(error); }
});

app.post("/api/jobs/:id/cancel", rateLimit, async (req, res, next) => {
  const idParsed = z.string().uuid().safeParse(req.params.id);
  if (!idParsed.success) { res.status(400).json({ error: "Некорректный ID задачи" }); return; }
  try {
    const result = await store.cancelJob(idParsed.data);
    if (result === "not_found") { res.status(404).json({ error: "Задача не найдена" }); return; }
    if (result === "not_active") { res.status(409).json({ error: "Отмена доступна только для задач в очереди или в работе" }); return; }
    await cache.del("pulseops:stats:v1").catch(() => undefined);
    res.json({ ok: true, id: idParsed.data, status: "cancelled" });
  } catch (error) { next(error); }
});

function rateLimit(req: Request, res: Response, next: NextFunction): void {
  void (async () => {
    try {
      const key = `pulseops:rate:${req.ip ?? "unknown"}`;
      const count = await cache.incr(key);
      if (count === 1) await cache.expire(key, 60);
      if (count > 120) {
        res.status(429).json({ error: "Слишком много запросов. Повтори через минуту." });
        return;
      }
    } catch {
      // degrade open
    }
    next();
  })();
}

app.use((err: unknown, req: Request, res: Response, _next: NextFunction) => {
  console.error(JSON.stringify({ level: "error", msg: "unhandled", requestId: (req as any).requestId, error: err instanceof Error ? (((err as any).code || err.message || String(err))) : String(err) }));
  if (res.headersSent) return;
  res.status(500).json({ error: "Внутренняя ошибка сервера", requestId: (req as any).requestId });
});

/* ─────────────── Startup ─────────────── */

async function waitForDependencies(): Promise<void> {
  pgPool = new Pool({ connectionString: DATABASE_URL, max: 10, idleTimeoutMillis: 30_000 });
  redisClient = new Redis(REDIS_URL, {
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    retryStrategy: (times) => (times > 3 ? null : Math.min(times * 200, 1_000)),
    lazyConnect: true,
  });
  redisClient.on("error", (e: any) => console.error(JSON.stringify({ level: "error", msg: "redis", error: e?.code || e?.message || String(e) })));
  amqpBroker = await amqp.connect(AMQP_URL);

  await pgPool.query("SELECT 1");
  await redisClient.connect().catch(() => undefined);
  await redisClient.ping();
  const publisher = await amqpBroker.createConfirmChannel();
  await publisher.assertQueue(QUEUE, { durable: true, arguments: { "x-max-priority": 10 } });
  await publisher.assertQueue(DLQ, { durable: true });
  await publisher.close();
  console.log(JSON.stringify({ level: "info", msg: "dependencies ready", role: ROLE }));
}

async function main(): Promise<void> {
  try {
    if (DEMO_MODE) {
      store = new MemoryStore();
      cache = new MemoryCache();
      queue = new MemoryQueue();
      await store.init();
      startOutboxDispatcher();
      await queue.consume((msg) => void handleJobMessage(msg.jobId, msg));
      startWorkerHeartbeat();
      app.listen(PORT, () =>
        console.log(JSON.stringify({ level: "info", msg: "PulseOps ready (DEMO MODE, in-memory)", url: `http://localhost:${PORT}` })),
      );
      return;
    }

    await waitForDependencies();
    store = new PostgresStore(pgPool!);
    cache = new RedisCache(redisClient!);
    queue = new RabbitQueue(amqpBroker!);
    await store.init();

    if (ROLE === "worker") {
      startWorkerHeartbeat();
      await queue.consume((msg) => void handleJobMessage(msg.jobId, msg));
    } else {
      app.listen(PORT, () =>
        console.log(JSON.stringify({ level: "info", msg: "PulseOps ready", url: `http://localhost:${PORT}` })),
      );
      startOutboxDispatcher();
    }
  } catch (error) {
    console.error(JSON.stringify({ level: "error", msg: "startup failed", error: error instanceof Error ? (((error as any).code || error.message || String(error))) : String(error) }));
    process.exitCode = 1;
    await shutdown();
  }
}

async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(JSON.stringify({ level: "info", msg: "shutdown" }));
  try { await queue?.close(); } catch { /* ignore */ }
  try { await redisClient?.quit(); } catch { /* ignore */ }
  try { await pgPool?.end(); } catch { /* ignore */ }
  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
void main();
