import { logger, errorMessage } from "../logger.js";
import type { IQueue, IStore } from "../types.js";

/**
 * Outbox dispatcher — polls the outbox table and publishes pending events
 * to the broker. Each row is published and marked sent individually so a
 * failure on one row never blocks or loses the others.
 */
export class OutboxDispatcher {
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(
    private readonly store: IStore,
    private readonly queue: IQueue,
  ) {}

  start(intervalMs = 1000): void {
    if (this.timer) return;
    void this.tick();
    this.timer = setInterval(() => void this.tick(), intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const rows = await this.store.outboxPending();
      for (const row of rows) {
        try {
          this.queue.publish(row.payload, row.job_priority ?? "normal");
          await this.store.markOutboxSent([row.id]);
        } catch (err) {
          // per-row defer with exponential backoff + jitter (computed in store)
          await this.store.deferOutbox([row.id], 0, errorMessage(err)).catch(() => undefined);
          logger.warn("outbox row deferred", { outboxId: row.id, error: errorMessage(err) });
        }
      }
    } catch (err) {
      logger.error("outbox dispatch failed", { error: errorMessage(err) });
    } finally {
      this.running = false;
    }
  }
}
