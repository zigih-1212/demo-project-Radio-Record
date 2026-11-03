import { Pool, type PoolClient } from "pg";
import { randomUUID } from "node:crypto";
import { env } from "../config.js";
import { runMigrations } from "./migrate.js";
import type {
  CreateJobInput,
  JobListResult,
  JobRow,
  JobEventRow,
  ListJobsFilter,
  OutboxRow,
  JobStats,
  UserRow,
  Role,
  IStore,
} from "../types.js";

const PRIORITY_SQL = "CASE priority WHEN 'high' THEN 3 WHEN 'normal' THEN 2 ELSE 1 END";

function toIso(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function toJobRow(r: Record<string, unknown>): JobRow {
  return {
    id: String(r.id),
    title: String(r.title),
    job_type: r.job_type as JobRow["job_type"],
    priority: r.priority as JobRow["priority"],
    status: r.status as JobRow["status"],
    payload: (r.payload ?? {}) as Record<string, unknown>,
    attempts: Number(r.attempts),
    max_attempts: Number(r.max_attempts),
    error: r.error == null ? null : String(r.error),
    created_at: toIso(r.created_at),
    updated_at: toIso(r.updated_at),
    completed_at: r.completed_at == null ? null : toIso(r.completed_at),
    available_at: toIso(r.available_at),
    lease_expires_at: r.lease_expires_at == null ? null : toIso(r.lease_expires_at),
    locked_by: r.locked_by == null ? null : String(r.locked_by),
  };
}

function toEventRow(r: Record<string, unknown>): JobEventRow {
  return {
    id: Number(r.id),
    job_id: String(r.job_id),
    event_type: String(r.event_type),
    message: r.message == null ? null : String(r.message),
    created_at: toIso(r.created_at),
  };
}

function toOutboxRow(r: Record<string, unknown>): OutboxRow {
  return {
    id: String(r.id),
    event_key: String(r.event_key),
    aggregate_id: String(r.aggregate_id),
    event_type: String(r.event_type),
    payload: r.payload ?? {},
    attempts: Number(r.attempts),
    last_error: r.last_error == null ? null : String(r.last_error),
    created_at: toIso(r.created_at),
    available_at: toIso(r.available_at),
    sent_at: r.sent_at == null ? null : toIso(r.sent_at),
    job_priority: r.job_priority == null ? undefined : (r.job_priority as OutboxRow["job_priority"]),
  };
}

function toUserRow(r: Record<string, unknown>): UserRow {
  return {
    id: String(r.id),
    email: String(r.email),
    password_hash: String(r.password_hash),
    role: r.role as Role,
    created_at: toIso(r.created_at),
  };
}

export class PostgresStore implements IStore {
  constructor(private readonly pool: Pool) {}

  async init(): Promise<void> {
    await runMigrations(this.pool);
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
          `INSERT INTO jobs(id,title,job_type,priority,status,payload,attempts,max_attempts,error,created_at,updated_at,completed_at,available_at)
           VALUES($1,$2,$3,$4,$5,$6,$7,5,$8,NOW()-($9 * INTERVAL '1 minute'),NOW()-($9 * INTERVAL '1 minute'),
           CASE WHEN $5 IN ('succeeded','failed','cancelled') THEN NOW()-($9 * INTERVAL '1 minute') ELSE NULL END, NOW())`,
          [id, sample.title, sample.type, sample.priority, sample.status, JSON.stringify({ demoSeed: true }), sample.attempts,
            sample.status === "failed" ? "Временная ошибка источника данных (демо)" : null, sample.minutesAgo],
        );
        await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'created','Демо-запись')`, [id]);
        if (sample.status === "queued") queuedId = id;
      }
      if (queuedId) {
        await client.query(
          `INSERT INTO outbox(id,event_key,aggregate_id,event_type,payload) VALUES($1,$2,$3,'job.queued',$4::jsonb)`,
          [randomUUID(), randomUUID(), queuedId, JSON.stringify({ jobId: queuedId })],
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

  async ping(): Promise<void> {
    await this.pool.query("SELECT 1");
  }

  async listJobs(filter: ListJobsFilter): Promise<JobListResult> {
    const conditions: string[] = [];
    const params: unknown[] = [];
    let i = 1;
    if (filter.status) { conditions.push(`status=$${i++}`); params.push(filter.status); }
    if (filter.type) { conditions.push(`job_type=$${i++}`); params.push(filter.type); }
    if (filter.priority) { conditions.push(`priority=$${i++}`); params.push(filter.priority); }
    if (filter.q) { conditions.push(`title ILIKE $${i++}`); params.push(`%${filter.q}%`); }
    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
    const countResult = await this.pool.query<{ c: string }>(`SELECT COUNT(*)::text AS c FROM jobs ${where}`, params);
    const total = Number(countResult.rows[0]?.c ?? 0);
    const dataParams = [...params, filter.limit, filter.offset];
    const result = await this.pool.query(
      `SELECT * FROM jobs ${where} ORDER BY created_at DESC, id DESC LIMIT $${i++} OFFSET $${i++}`,
      dataParams,
    );
    return { items: result.rows.map(toJobRow), total, limit: filter.limit, offset: filter.offset };
  }

  async getJob(id: string): Promise<JobRow | null> {
    const result = await this.pool.query("SELECT * FROM jobs WHERE id=$1", [id]);
    return result.rowCount ? toJobRow(result.rows[0] as Record<string, unknown>) : null;
  }

  async getEvents(jobId: string): Promise<JobEventRow[]> {
    const result = await this.pool.query(
      "SELECT * FROM job_events WHERE job_id=$1 ORDER BY created_at, id",
      [jobId],
    );
    return result.rows.map(toEventRow);
  }

  async createJob(input: CreateJobInput): Promise<JobRow> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const id = randomUUID();
      await client.query(
        `INSERT INTO jobs(id,title,job_type,priority,status,payload,available_at)
         VALUES($1,$2,$3,$4,'queued',$5::jsonb,NOW())`,
        [id, input.title, input.type, input.priority, JSON.stringify({ simulateFailure: input.simulateFailure })],
      );
      await client.query(
        `INSERT INTO outbox(id,event_key,aggregate_id,event_type,payload) VALUES($1,$2,$3,'job.queued',$4::jsonb)`,
        [randomUUID(), randomUUID(), id, JSON.stringify({ jobId: id })],
      );
      await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'created','Создана пользователем')`, [id]);
      await client.query("COMMIT");
      const created = await this.getJob(id);
      if (!created) throw new Error("Failed to read created job");
      return created;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async retryJob(id: string): Promise<JobRow | null> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<Record<string, unknown>>(
        `UPDATE jobs SET status='queued',error=NULL,completed_at=NULL,lease_expires_at=NULL,locked_by=NULL,available_at=NOW(),updated_at=NOW()
         WHERE id=$1 AND status='failed' AND attempts < max_attempts RETURNING *`,
        [id],
      );
      if (result.rowCount === 0) {
        await client.query("ROLLBACK");
        return null;
      }
      const jobId = String(result.rows[0].id);
      await client.query(
        `INSERT INTO outbox(id,event_key,aggregate_id,event_type,payload) VALUES($1,$2,$3,'job.queued',$4::jsonb)`,
        [randomUUID(), randomUUID(), jobId, JSON.stringify({ jobId })],
      );
      await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'retried','Возвращено в очередь')`, [jobId]);
      await client.query("COMMIT");
      return toJobRow(result.rows[0]);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async cancelJob(id: string): Promise<JobRow | null> {
    const result = await this.pool.query<Record<string, unknown>>(
      `UPDATE jobs SET status='cancelled',completed_at=NOW(),lease_expires_at=NULL,locked_by=NULL,updated_at=NOW()
       WHERE id=$1 AND status IN ('queued','processing') RETURNING *`,
      [id],
    );
    if (result.rowCount === 0) return null;
    const jobId = String(result.rows[0].id);
    await this.pool.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'cancelled','Отменено пользователем')`, [jobId]);
    return toJobRow(result.rows[0]);
  }

  async claim(jobId: string, workerId: string, leaseTtlSeconds: number): Promise<JobRow | null> {
    const result = await this.pool.query<Record<string, unknown>>(
      `UPDATE jobs SET status='processing',attempts=attempts+1,lease_expires_at=NOW()+($3 * INTERVAL '1 second'),locked_by=$2,updated_at=NOW(),error=NULL
       WHERE id=$1 AND status='queued' AND available_at<=NOW() RETURNING *`,
      [jobId, workerId, leaseTtlSeconds],
    );
    return result.rowCount ? toJobRow(result.rows[0]) : null;
  }

  async finish(jobId: string, workerId: string, status: "succeeded" | "failed", error: string | null): Promise<boolean> {
    const result = await this.pool.query(
      `UPDATE jobs SET status=$3,error=$4,completed_at=NOW(),lease_expires_at=NULL,locked_by=NULL,updated_at=NOW()
       WHERE id=$1 AND status='processing' AND locked_by=$2`,
      [jobId, workerId, status, error],
    );
    return (result.rowCount ?? 0) > 0;
  }

  async addEvent(jobId: string, eventType: string, message?: string): Promise<void> {
    await this.pool.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,$2,$3)`, [jobId, eventType, message ?? null]);
  }

  async resetExpiredLeases(nowIso: string): Promise<string[]> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<{ id: string }>(
        `UPDATE jobs SET status='queued',lease_expires_at=NULL,locked_by=NULL,available_at=NOW(),updated_at=NOW()
         WHERE status='processing' AND lease_expires_at < $1 RETURNING id`,
        [nowIso],
      );
      const ids = result.rows.map((r) => String(r.id));
      for (const id of ids) {
        await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'requeued','Lease expired — returned to queue')`, [id]);
      }
      await client.query("COMMIT");
      return ids;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async resetForReplay(jobId: string): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<Record<string, unknown>>(
        `UPDATE jobs SET status='queued',attempts=0,error=NULL,completed_at=NULL,lease_expires_at=NULL,locked_by=NULL,available_at=NOW(),updated_at=NOW()
         WHERE id=$1 AND status='failed' RETURNING id`,
        [jobId],
      );
      if (result.rowCount === 0) {
        await client.query("ROLLBACK");
        return false;
      }
      const id = String(result.rows[0].id);
      await client.query(
        `INSERT INTO outbox(id,event_key,aggregate_id,event_type,payload) VALUES($1,$2,$3,'job.queued',$4::jsonb)`,
        [randomUUID(), randomUUID(), id, JSON.stringify({ jobId: id })],
      );
      await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'replayed','Reset for replay by admin')`, [id]);
      await client.query("COMMIT");
      return true;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async scheduleRetry(jobId: string, availableAtIso: string): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<Record<string, unknown>>(
        `UPDATE jobs SET status='queued',lease_expires_at=NULL,locked_by=NULL,available_at=$2,updated_at=NOW()
         WHERE id=$1 AND status='processing' RETURNING id`,
        [jobId, availableAtIso],
      );
      if (result.rowCount === 0) {
        await client.query("ROLLBACK");
        return false;
      }
      const id = String(result.rows[0].id);
      await client.query(
        `INSERT INTO outbox(id,event_key,aggregate_id,event_type,payload,available_at) VALUES($1,$2,$3,'job.retry',$4::jsonb,$5::timestamptz)`,
        [randomUUID(), randomUUID(), id, JSON.stringify({ jobId: id }), availableAtIso],
      );
      await client.query(`INSERT INTO job_events(job_id,event_type,message) VALUES($1,'retry_scheduled','Очередная попытка запланирована')`, [id]);
      await client.query("COMMIT");
      return true;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async outboxPending(): Promise<OutboxRow[]> {
    const result = await this.pool.query<Record<string, unknown>>(
      `SELECT o.*, j.priority AS job_priority FROM outbox o LEFT JOIN jobs j ON j.id=o.aggregate_id
       WHERE o.sent_at IS NULL AND o.available_at <= NOW()
       ORDER BY o.id LIMIT $1 FOR UPDATE OF o SKIP LOCKED`,
      [env.OUTBOX_BATCH],
    );
    return result.rows.map(toOutboxRow);
  }

  async markOutboxSent(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    await this.pool.query("UPDATE outbox SET sent_at=NOW(), last_error=NULL WHERE id = ANY($1::uuid[]) AND sent_at IS NULL", [ids]);
  }

  async deferOutbox(ids: string[], _delaySeconds: number, error?: string): Promise<void> {
    if (ids.length === 0) return;
    await this.pool.query(
      `UPDATE outbox SET attempts=attempts+1, last_error=$2,
       available_at = NOW() + make_interval(secs => LEAST(POWER(2, attempts), 300)) + make_interval(secs => (random() * 2))
       WHERE id = ANY($1::uuid[]) AND sent_at IS NULL`,
      [ids, error ?? null],
    );
  }

  async insertOutbox(eventKey: string, aggregateId: string, eventType: string, payload: unknown, availableAtIso?: string): Promise<void> {
    await this.pool.query(
      `INSERT INTO outbox(id,event_key,aggregate_id,event_type,payload,available_at) VALUES($1,$2,$3,$4,$5::jsonb,COALESCE($6::timestamptz,NOW()))`,
      [randomUUID(), eventKey, aggregateId, eventType, JSON.stringify(payload), availableAtIso ?? null],
    );
  }

  async stats(): Promise<JobStats> {
    const result = await this.pool.query<{ total: string; queued: string; processing: string; succeeded: string; failed: string; cancelled: string }>(
      `SELECT COUNT(*)::text AS total,
       COUNT(*) FILTER (WHERE status='queued')::text AS queued,
       COUNT(*) FILTER (WHERE status='processing')::text AS processing,
       COUNT(*) FILTER (WHERE status='succeeded')::text AS succeeded,
       COUNT(*) FILTER (WHERE status='failed')::text AS failed,
       COUNT(*) FILTER (WHERE status='cancelled')::text AS cancelled FROM jobs`,
    );
    const row = result.rows[0];
    return {
      total: Number(row?.total ?? 0),
      queued: Number(row?.queued ?? 0),
      processing: Number(row?.processing ?? 0),
      succeeded: Number(row?.succeeded ?? 0),
      failed: Number(row?.failed ?? 0),
      cancelled: Number(row?.cancelled ?? 0),
    };
  }

  async outboxCount(): Promise<number> {
    const result = await this.pool.query<{ c: string }>("SELECT COUNT(*)::text AS c FROM outbox WHERE sent_at IS NULL");
    return Number(result.rows[0]?.c ?? 0);
  }

  async findUserByEmail(email: string): Promise<UserRow | null> {
    const result = await this.pool.query("SELECT * FROM app_users WHERE email=$1", [email]);
    return result.rowCount ? toUserRow(result.rows[0] as Record<string, unknown>) : null;
  }

  async getUserById(id: string): Promise<UserRow | null> {
    const result = await this.pool.query("SELECT * FROM app_users WHERE id=$1", [id]);
    return result.rowCount ? toUserRow(result.rows[0] as Record<string, unknown>) : null;
  }

  async createUser(email: string, passwordHash: string, role: Role): Promise<UserRow> {
    const inserted = await this.pool.query<Record<string, unknown>>(
      `INSERT INTO app_users(id,email,password_hash,role) VALUES($1,$2,$3,$4)
       ON CONFLICT (email) DO NOTHING RETURNING *`,
      [randomUUID(), email, passwordHash, role],
    );
    if (inserted.rowCount) return toUserRow(inserted.rows[0]);
    // Lost an insert race (api and worker seeding in parallel on a cold start).
    // Return the winning row instead of throwing a unique violation.
    const existing = await this.pool.query("SELECT * FROM app_users WHERE email=$1", [email]);
    if (!existing.rowCount) throw new Error("Failed to create or read user");
    return toUserRow(existing.rows[0] as Record<string, unknown>);
  }
}
