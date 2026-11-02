import { ConflictError, NotFoundError } from "../errors.js";
import { logger } from "../logger.js";
import type { CreateJobInput, ICache, IStore, JobListResult, JobRow, JobStats, ListJobsFilter } from "../types.js";

const STATS_CACHE_KEY = "pulseops:stats:v1";
const IDEM_PREFIX = "pulseops:idem:";

export interface CreatedJobResult {
  item: JobRow;
  idempotent?: boolean;
}

export interface JobDetailResult {
  job: JobRow;
  events: Array<{ id: number; job_id: string; event_type: string; message: string | null; created_at: string }>;
}

/**
 * JobService — business logic layer. Controllers/routes must stay thin.
 */
export class JobService {
  constructor(
    private readonly store: IStore,
    private readonly cache: ICache,
  ) {}

  async create(input: CreateJobInput, idempotencyKey?: string): Promise<CreatedJobResult> {
    if (idempotencyKey) {
      const cached = await this.cache.get(`${IDEM_PREFIX}${idempotencyKey}`).catch(() => null);
      if (cached) {
        try {
          return { item: JSON.parse(cached) as JobRow, idempotent: true };
        } catch {
          // corrupted cache entry — fall through to creation
        }
      }
    }
    const item = await this.store.createJob(input);
    if (idempotencyKey) {
      await this.cache.set(`${IDEM_PREFIX}${idempotencyKey}`, JSON.stringify(item), 86_400).catch(() => undefined);
    }
    await this.cache.del(STATS_CACHE_KEY).catch(() => undefined);
    logger.info("job created", { jobId: item.id, type: item.job_type, priority: item.priority });
    return { item };
  }

  async list(filter: ListJobsFilter): Promise<JobListResult> {
    return this.store.listJobs(filter);
  }

  async get(id: string): Promise<JobDetailResult> {
    const job = await this.store.getJob(id);
    if (!job) throw new NotFoundError("Задача не найдена");
    const events = await this.store.getEvents(id);
    return { job, events };
  }

  async retry(id: string): Promise<JobRow> {
    const job = await this.store.getJob(id);
    if (!job) throw new NotFoundError("Задача не найдена");
    if (job.attempts >= job.max_attempts) {
      throw new ConflictError("Превышен лимит попыток — задачу можно только восстановить из DLQ");
    }
    const updated = await this.store.retryJob(id);
    if (!updated) {
      throw new ConflictError("Повтор доступен только для задачи со статусом «Ошибка»");
    }
    await this.cache.del(STATS_CACHE_KEY).catch(() => undefined);
    return updated;
  }

  async cancel(id: string): Promise<JobRow> {
    const updated = await this.store.cancelJob(id);
    if (!updated) {
      throw new ConflictError("Отмена доступна только для задач в очереди или в работе");
    }
    await this.cache.del(STATS_CACHE_KEY).catch(() => undefined);
    return updated;
  }

  async stats(): Promise<JobStats> {
    return this.store.stats();
  }

  /** Safe DLQ replay: resets a failed job to queued (attempts=0) and lets the
   *  outbox republish it. Source of truth is the database, not the queue. */
  async dlqReplay(jobId: string): Promise<JobRow> {
    const ok = await this.store.resetForReplay(jobId);
    if (!ok) {
      throw new ConflictError("Восстановление из DLQ доступно только для задач со статусом «Ошибка»");
    }
    await this.cache.del(STATS_CACHE_KEY).catch(() => undefined);
    const job = await this.store.getJob(jobId);
    if (!job) throw new NotFoundError("Задача не найдена");
    logger.info("job replayed from DLQ", { jobId });
    return job;
  }

  async metricsText(): Promise<string> {
    const s = await this.store.stats();
    const pending = await this.store.outboxCount();
    const lines = [
      "# TYPE pulseops_jobs gauge",
      `pulseops_jobs{status="queued"} ${s.queued}`,
      `pulseops_jobs{status="processing"} ${s.processing}`,
      `pulseops_jobs{status="succeeded"} ${s.succeeded}`,
      `pulseops_jobs{status="failed"} ${s.failed}`,
      `pulseops_jobs{status="cancelled"} ${s.cancelled}`,
      `pulseops_jobs_total ${s.total}`,
      `pulseops_outbox_pending ${pending}`,
    ];
    return lines.join("\n") + "\n";
  }
}
