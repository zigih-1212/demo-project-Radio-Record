import { randomUUID } from "node:crypto";
import type {
  CreateJobInput,
  JobEventRow,
  JobListResult,
  JobRow,
  JobStats,
  ListJobsFilter,
  OutboxRow,
  Role,
  UserRow,
  IStore,
} from "../types.js";

function nowIso(): string {
  return new Date().toISOString();
}

function secondsFromNow(seconds: number): string {
  return new Date(Date.now() + seconds * 1000).toISOString();
}

export class MemoryStore implements IStore {
  private jobs = new Map<string, JobRow>();
  private events: JobEventRow[] = [];
  private outbox = new Map<string, OutboxRow>();
  private users = new Map<string, UserRow>();
  private eventSeq = 0;

  async init(): Promise<void> {
    const samples: Array<{ title: string; type: JobRow["job_type"]; priority: JobRow["priority"]; status: JobRow["status"]; attempts: number; minutesAgo: number }> = [
      { title: "Публикация подборки выходного дня", type: "campaign_publish", priority: "high", status: "succeeded", attempts: 1, minutesAgo: 4 },
      { title: "Синхронизация каталога партнёров", type: "sync_catalog", priority: "normal", status: "succeeded", attempts: 1, minutesAgo: 18 },
      { title: "Импорт заказов за сутки", type: "import_orders", priority: "normal", status: "failed", attempts: 3, minutesAgo: 31 },
      { title: "Подготовка email-дайджеста", type: "send_digest", priority: "low", status: "queued", attempts: 0, minutesAgo: 1 },
    ];
    let queuedId: string | null = null;
    for (const sample of samples) {
      const id = randomUUID();
      const ts = new Date(Date.now() - sample.minutesAgo * 60_000).toISOString();
      const job: JobRow = {
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
        available_at: nowIso(),
        lease_expires_at: null,
        locked_by: null,
      };
      this.jobs.set(id, job);
      this.pushEvent(id, "created", "Демо-запись");
      if (sample.status === "queued") queuedId = id;
    }
    if (queuedId) {
      this.insertOutboxRow(randomUUID(), randomUUID(), queuedId, "job.queued", { jobId: queuedId }, nowIso());
      this.pushEvent(queuedId, "queued", "Поставлено в очередь");
    }
  }

  private pushEvent(jobId: string, eventType: string, message: string): void {
    this.eventSeq += 1;
    this.events.push({ id: this.eventSeq, job_id: jobId, event_type: eventType, message, created_at: nowIso() });
  }

  private insertOutboxRow(id: string, eventKey: string, aggregateId: string, eventType: string, payload: unknown, availableAt: string): void {
    this.outbox.set(id, {
      id,
      event_key: eventKey,
      aggregate_id: aggregateId,
      event_type: eventType,
      payload,
      attempts: 0,
      last_error: null,
      created_at: nowIso(),
      available_at: availableAt,
      sent_at: null,
      job_priority: this.jobs.get(aggregateId)?.priority,
    });
  }

  async ping(): Promise<void> {
    /* no-op */
  }

  async listJobs(filter: ListJobsFilter): Promise<JobListResult> {
    let items = [...this.jobs.values()];
    if (filter.status) items = items.filter((j) => j.status === filter.status);
    if (filter.type) items = items.filter((j) => j.job_type === filter.type);
    if (filter.priority) items = items.filter((j) => j.priority === filter.priority);
    const q = filter.q?.toLowerCase();
    if (q) items = items.filter((j) => j.title.toLowerCase().includes(q));
    items.sort((a, b) => (a.created_at < b.created_at ? 1 : a.created_at > b.created_at ? -1 : a.id < b.id ? 1 : -1));
    const total = items.length;
    const page = items.slice(filter.offset, filter.offset + filter.limit);
    return { items: page.map((j) => ({ ...j })), total, limit: filter.limit, offset: filter.offset };
  }

  async getJob(id: string): Promise<JobRow | null> {
    const job = this.jobs.get(id);
    return job ? { ...job } : null;
  }

  async getEvents(jobId: string): Promise<JobEventRow[]> {
    return this.events.filter((e) => e.job_id === jobId).map((e) => ({ ...e }));
  }

  async createJob(input: CreateJobInput): Promise<JobRow> {
    const id = randomUUID();
    const ts = nowIso();
    const job: JobRow = {
      id,
      title: input.title,
      job_type: input.type,
      priority: input.priority,
      status: "queued",
      payload: { simulateFailure: input.simulateFailure },
      attempts: 0,
      max_attempts: 5,
      error: null,
      created_at: ts,
      updated_at: ts,
      completed_at: null,
      available_at: ts,
      lease_expires_at: null,
      locked_by: null,
    };
    this.jobs.set(id, job);
    this.insertOutboxRow(randomUUID(), randomUUID(), id, "job.queued", { jobId: id }, ts);
    this.pushEvent(id, "created", "Создана пользователем");
    return { ...job };
  }

  async retryJob(id: string): Promise<JobRow | null> {
    const job = this.jobs.get(id);
    if (!job || job.status !== "failed" || job.attempts >= job.max_attempts) return null;
    job.status = "queued";
    job.error = null;
    job.completed_at = null;
    job.lease_expires_at = null;
    job.locked_by = null;
    job.available_at = nowIso();
    job.updated_at = nowIso();
    this.insertOutboxRow(randomUUID(), randomUUID(), id, "job.queued", { jobId: id }, nowIso());
    this.pushEvent(id, "retried", "Возвращено в очередь");
    return { ...job };
  }

  async cancelJob(id: string): Promise<JobRow | null> {
    const job = this.jobs.get(id);
    if (!job || (job.status !== "queued" && job.status !== "processing")) return null;
    job.status = "cancelled";
    job.completed_at = nowIso();
    job.lease_expires_at = null;
    job.locked_by = null;
    job.updated_at = nowIso();
    this.pushEvent(id, "cancelled", "Отменено пользователем");
    return { ...job };
  }

  async claim(jobId: string, workerId: string, leaseTtlSeconds: number): Promise<JobRow | null> {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== "queued" || Date.parse(job.available_at) > Date.now()) return null;
    job.status = "processing";
    job.attempts += 1;
    job.lease_expires_at = secondsFromNow(leaseTtlSeconds);
    job.locked_by = workerId;
    job.updated_at = nowIso();
    job.error = null;
    return { ...job };
  }

  async finish(jobId: string, workerId: string, status: "succeeded" | "failed", error: string | null): Promise<boolean> {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== "processing" || job.locked_by !== workerId) return false;
    job.status = status;
    job.error = error;
    job.completed_at = nowIso();
    job.lease_expires_at = null;
    job.locked_by = null;
    job.updated_at = nowIso();
    return true;
  }

  async addEvent(jobId: string, eventType: string, message?: string): Promise<void> {
    this.pushEvent(jobId, eventType, message ?? "");
  }

  async resetExpiredLeases(nowIsoArg: string): Promise<string[]> {
    const now = Date.parse(nowIsoArg);
    const reset: string[] = [];
    for (const job of this.jobs.values()) {
      if (job.status === "processing" && job.lease_expires_at && Date.parse(job.lease_expires_at) < now) {
        job.status = "queued";
        job.lease_expires_at = null;
        job.locked_by = null;
        job.available_at = nowIso();
        job.updated_at = nowIso();
        this.pushEvent(job.id, "requeued", "Lease expired — returned to queue");
        reset.push(job.id);
      }
    }
    return reset;
  }

  async resetForReplay(jobId: string): Promise<boolean> {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== "failed") return false;
    job.status = "queued";
    job.attempts = 0;
    job.error = null;
    job.completed_at = null;
    job.lease_expires_at = null;
    job.locked_by = null;
    job.available_at = nowIso();
    job.updated_at = nowIso();
    this.insertOutboxRow(randomUUID(), randomUUID(), jobId, "job.queued", { jobId }, nowIso());
    this.pushEvent(jobId, "replayed", "Reset for replay by admin");
    return true;
  }

  async scheduleRetry(jobId: string, availableAtIso: string): Promise<boolean> {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== "processing") return false;
    job.status = "queued";
    job.lease_expires_at = null;
    job.locked_by = null;
    job.available_at = availableAtIso;
    job.updated_at = nowIso();
    this.insertOutboxRow(randomUUID(), randomUUID(), jobId, "job.retry", { jobId: job.id }, availableAtIso);
    this.pushEvent(jobId, "retry_scheduled", "Очередная попытка запланирована");
    return true;
  }

  async outboxPending(): Promise<OutboxRow[]> {
    const now = nowIso();
    return [...this.outbox.values()]
      .filter((o) => o.sent_at == null && o.available_at <= now)
      .sort((a, b) => (a.id < b.id ? -1 : 1))
      .slice(0, 25)
      .map((o) => ({ ...o }));
  }

  async markOutboxSent(ids: string[]): Promise<void> {
    const ts = nowIso();
    for (const id of ids) {
      const row = this.outbox.get(id);
      if (row && row.sent_at == null) row.sent_at = ts;
    }
  }

  async deferOutbox(ids: string[], _delaySeconds: number, error?: string): Promise<void> {
    for (const id of ids) {
      const row = this.outbox.get(id);
      if (!row || row.sent_at != null) continue;
      row.attempts += 1;
      row.last_error = error ?? null;
      const backoff = Math.min(2 ** row.attempts, 300);
      const jitter = Math.random() * 2;
      row.available_at = secondsFromNow(backoff + jitter);
    }
  }

  async insertOutbox(eventKey: string, aggregateId: string, eventType: string, payload: unknown, availableAtIso?: string): Promise<void> {
    this.insertOutboxRow(randomUUID(), eventKey, aggregateId, eventType, payload, availableAtIso ?? nowIso());
  }

  async stats(): Promise<JobStats> {
    const all = [...this.jobs.values()];
    return {
      total: all.length,
      queued: all.filter((j) => j.status === "queued").length,
      processing: all.filter((j) => j.status === "processing").length,
      succeeded: all.filter((j) => j.status === "succeeded").length,
      failed: all.filter((j) => j.status === "failed").length,
      cancelled: all.filter((j) => j.status === "cancelled").length,
    };
  }

  async outboxCount(): Promise<number> {
    return [...this.outbox.values()].filter((o) => o.sent_at == null).length;
  }

  async findUserByEmail(email: string): Promise<UserRow | null> {
    for (const user of this.users.values()) {
      if (user.email === email) return { ...user };
    }
    return null;
  }

  async getUserById(id: string): Promise<UserRow | null> {
    const user = this.users.get(id);
    return user ? { ...user } : null;
  }

  async createUser(email: string, passwordHash: string, role: Role): Promise<UserRow> {
    const user: UserRow = { id: randomUUID(), email, password_hash: passwordHash, role, created_at: nowIso() };
    this.users.set(user.id, user);
    return { ...user };
  }
}
