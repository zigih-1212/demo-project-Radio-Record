import { randomUUID } from "node:crypto";
import { env } from "../config.js";
import { errorMessage, logger } from "../logger.js";
import type { ICache, IQueue, IStore, JobRow, QueueMessage } from "../types.js";

const STATS_CACHE_KEY = "pulseops:stats:v1";
const HEARTBEAT_KEY = "pulseops:worker:heartbeat";

/**
 * Worker service.
 *
 * Guarantees:
 * - at-least-once delivery (messages are acked only after the job reaches a
 *   terminal state or a scheduled retry);
 * - lease-based claiming: a claim sets lease_expires_at/locked_by, so a
 *   crashed worker's jobs are recovered by the reaper;
 * - side-effect-once: enforced by the store (atomic claim + locked_by finish
 *   guard), not by scanning events — see handleJobMessage;
 * - exponential backoff WITH jitter on business failures, up to max_attempts;
 * - messages that exhaust attempts are published to the DLQ;
 * - graceful stop: in-flight jobs are drained before the consumer closes.
 */
export class WorkerService {
  readonly workerId = `worker-${randomUUID().slice(0, 8)}`;
  private processed = 0;
  private reaperTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private readonly inFlight = new Set<Promise<void>>();
  private stopping = false;

  constructor(
    private readonly store: IStore,
    private readonly queue: IQueue,
    private readonly cache: ICache,
  ) {}

  start(): void {
    void this.queue.consume((msg) => {
      if (this.stopping) {
        // Shutting down: hand the message back instead of starting new work.
        msg.nack(true);
        return;
      }
      const task = this.handleJobMessage(msg)
        .catch((err: unknown) => {
          logger.error("job handler crashed", { jobId: msg.jobId, error: errorMessage(err) });
        })
        .finally(() => {
          this.inFlight.delete(task);
        });
      this.inFlight.add(task);
    });
    this.reaperTimer = setInterval(() => void this.reap(), env.REAPER_INTERVAL_S * 1000);
    this.reaperTimer.unref?.();
    this.heartbeatTimer = setInterval(() => void this.heartbeat(), 10_000);
    this.heartbeatTimer.unref?.();
    void this.heartbeat();
    logger.info("worker started", { workerId: this.workerId, concurrency: env.WORKER_CONCURRENCY });
  }

  /**
   * Graceful shutdown: stop timers, let in-flight jobs finish (bounded by
   * drainTimeoutMs), then close the consumer. Jobs still running after the
   * timeout are left to the lease reaper of another worker / the next start.
   */
  async stop(drainTimeoutMs = 30_000): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    if (this.reaperTimer) clearInterval(this.reaperTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.reaperTimer = null;
    this.heartbeatTimer = null;

    const pending = [...this.inFlight];
    if (pending.length > 0) {
      logger.info("draining in-flight jobs", { count: pending.length, timeoutMs: drainTimeoutMs });
      let timer: NodeJS.Timeout | null = null;
      const timedOut = await Promise.race<boolean>([
        Promise.allSettled(pending).then(() => false),
        new Promise<boolean>((resolve) => {
          timer = setTimeout(() => resolve(true), drainTimeoutMs);
        }),
      ]);
      if (timer) clearTimeout(timer);
      if (timedOut) {
        logger.warn("drain timeout — unfinished jobs will be recovered by the lease reaper", {
          count: pending.length,
        });
      }
    }
    await this.queue.close().catch(() => undefined);
  }

  private async heartbeat(): Promise<void> {
    await this.cache
      .set(HEARTBEAT_KEY, JSON.stringify({ workerId: this.workerId, ts: Date.now(), processed: this.processed }), 30)
      .catch(() => undefined);
  }

  /** Recover jobs whose lease expired (worker crash / restart mid-job). */
  private async reap(): Promise<void> {
    try {
      const ids = await this.store.resetExpiredLeases(new Date().toISOString());
      if (ids.length > 0) {
        logger.warn("expired leases requeued", { count: ids.length, ids });
      }
    } catch (err) {
      logger.error("lease reaper failed", { error: errorMessage(err) });
    }
  }

  async handleJobMessage(msg: QueueMessage): Promise<void> {
    if (!msg.jobId) {
      logger.warn("malformed queue message moved to DLQ", { raw: msg.raw.slice(0, 200) });
      msg.dlq();
      return;
    }
    const jobId = msg.jobId;

    let claimed: JobRow | null = null;
    try {
      claimed = await this.store.claim(jobId, this.workerId, env.JOB_LEASE_TTL_S);
    } catch (err) {
      // transient store failure — do not lose the message, redeliver later
      logger.error("claim failed (store down?) — requeueing message", { jobId, error: errorMessage(err) });
      msg.nack(true);
      return;
    }
    if (!claimed) {
      // not claimable: terminal, cancelled, not yet available, or already
      // processed by another worker (redelivery dedupe)
      msg.ack();
      return;
    }

    try {
      await this.store.addEvent(jobId, "processing", `Подхвачено ${this.workerId}`);

      // Side-effect-once is enforced by the store, not by scanning events:
      // claim() atomically moves queued->processing under a lease
      // (UPDATE ... WHERE status='queued' AND available_at<=NOW()), and
      // finish() only succeeds for the lease holder (locked_by guard).
      // A redelivered message for a processing or terminal job gets
      // claim=null and is acked without side effects. The previous
      // event-based guard broke the retry cycle: an intermediate retryable
      // failure writes a 'failed' event, so attempt 2 was skipped without
      // finishing and the job stranded as 'processing' forever.

      // ── simulated business work (replace with real side effect) ──
      await new Promise((resolve) => setTimeout(resolve, 850));
      const payload = claimed.payload as { simulateFailure?: unknown } | null;
      if (payload?.simulateFailure === true) {
        throw new Error("Смоделированная ошибка обработчика");
      }

      const finished = await this.store.finish(jobId, this.workerId, "succeeded", null);
      if (!finished) {
        // finish() returns false when the reaper stole the lease (job was
        // requeued) OR when the job was cancelled/terminated mid-flight.
        // Republish only in the requeued case — never resurrect a job that
        // already reached a terminal state.
        const current = await this.store.getJob(jobId);
        if (current && current.status === "queued") {
          await this.store.insertOutbox(randomUUID(), jobId, "job.queued", { jobId });
          logger.warn("lease lost before finish — job republished to outbox", { jobId });
        } else if (current) {
          logger.warn("finish skipped: job left processing state before finish", { jobId, status: current.status });
        } else {
          logger.warn("finish skipped: job not found", { jobId });
        }
      } else {
        await this.store.addEvent(jobId, "succeeded", "Успешно обработано");
      }
      this.processed++;
      msg.ack();
    } catch (err) {
      await this.handleFailure(jobId, claimed.attempts, claimed.max_attempts, err);
      msg.ack();
    } finally {
      await this.cache.del(STATS_CACHE_KEY).catch(() => undefined);
    }
  }

  private async handleFailure(jobId: string, attempts: number, maxAttempts: number, err: unknown): Promise<void> {
    const detail = errorMessage(err);
    if (attempts < maxAttempts) {
      // exponential backoff WITH jitter, then reschedule via outbox
      const backoff = Math.min(2 ** attempts, 300) + Math.random() * 2;
      const availableAt = new Date(Date.now() + backoff * 1000).toISOString();
      const scheduled = await this.store.scheduleRetry(jobId, availableAt).catch(() => false);
      if (!scheduled) {
        await this.store.insertOutbox(randomUUID(), jobId, "job.queued", { jobId }).catch(() => undefined);
      }
      await this.store
        .addEvent(jobId, "failed", `Попытка ${attempts}/${maxAttempts}. Повтор через ~${Math.round(backoff)}с. ${detail}`)
        .catch(() => undefined);
      logger.warn("job failed — retry scheduled with backoff", { jobId, attempts, backoff: Math.round(backoff) });
    } else {
      await this.store.finish(jobId, this.workerId, "failed", detail).catch(() => undefined);
      await this.store
        .addEvent(jobId, "failed", `Превышено число попыток (${maxAttempts}). ${detail}`)
        .catch(() => undefined);
      this.queue.publishDLQ({ jobId, reason: "max_attempts", error: detail });
      logger.error("job failed permanently — published to DLQ", { jobId, attempts, error: detail });
    }
  }
}
