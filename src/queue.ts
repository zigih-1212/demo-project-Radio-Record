import amqp, { type Channel, type ChannelModel, type ConfirmChannel, type ConsumeMessage } from "amqplib";
import { DLQ, env, QUEUE } from "./config.js";
import { errorMessage, logger } from "./logger.js";
import type { IQueue, QueueHandler, QueueMessage } from "./types.js";

const PRIORITY_WEIGHT = { high: 10, normal: 5, low: 1 } as const;

function parseJobId(raw: string): string {
  try {
    const parsed = JSON.parse(raw) as { jobId?: unknown };
    return typeof parsed.jobId === "string" ? parsed.jobId : "";
  } catch {
    return "";
  }
}

const RECONNECT_BASE_MS = 1_000;
const RECONNECT_MAX_MS = 30_000;

/**
 * Exponential backoff for broker reconnects (capped). Exported so the growth
 * curve is unit-testable without a live RabbitMQ.
 */
export function reconnectDelayMs(attempt: number): number {
  return Math.min(RECONNECT_BASE_MS * 2 ** Math.max(0, attempt), RECONNECT_MAX_MS);
}

interface ConsumerRegistration {
  handler: QueueHandler;
  assign: (ch: Channel) => void;
}

export class RabbitQueue implements IQueue {
  private broker: ChannelModel | null = null;
  private publisher: ConfirmChannel | null = null;
  private consumerChannel: Channel | null = null;
  private dlqChannel: Channel | null = null;
  private closed = false;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectAttempt = 0;
  private connecting: Promise<void> | null = null;
  private readonly consumers = new Map<string, ConsumerRegistration>();

  async connect(): Promise<void> {
    await this.ensureBroker();
  }

  /**
   * Idempotent connection + publish-channel setup. Concurrent callers share a
   * single in-flight attempt so a reconnect storm cannot open parallel sockets.
   */
  private async ensureBroker(): Promise<void> {
    if (this.closed) throw new Error("RabbitMQ queue is closed");
    if (this.broker && this.publisher) return;
    if (this.connecting) return this.connecting;

    const attempt = (async (): Promise<void> => {
      if (!this.broker) {
        const broker = await amqp.connect(env.AMQP_URL);
        broker.on("error", (e: unknown) => logger.error("rabbit broker error", { error: errorMessage(e) }));
        broker.on("close", () => this.onBrokerClosed());
        this.broker = broker;
      }
      if (!this.publisher) {
        const publisher = await this.broker.createConfirmChannel();
        publisher.on("error", (e: unknown) => logger.error("rabbit publisher error", { error: errorMessage(e) }));
        await publisher.assertQueue(QUEUE, { durable: true, arguments: { "x-max-priority": 10 } });
        await publisher.assertQueue(DLQ, { durable: true });
        this.publisher = publisher;
      }
      this.reconnectAttempt = 0;
    })();

    this.connecting = attempt;
    try {
      await attempt;
    } finally {
      if (this.connecting === attempt) this.connecting = null;
    }
  }

  /**
   * Connection-level failure: channels die with the socket and must be rebuilt
   * from a NEW connection. Previously only channel refs were nulled and the
   * consumer retried createChannel() on a dead ChannelModel forever, so the
   * service never healed after a broker restart.
   */
  private onBrokerClosed(): void {
    this.broker = null;
    this.publisher = null;
    this.consumerChannel = null;
    this.dlqChannel = null;
    if (this.closed) return;
    logger.warn("rabbit connection closed — reconnect scheduled");
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.closed || this.reconnectTimer) return;
    const delay = reconnectDelayMs(this.reconnectAttempt);
    this.reconnectAttempt += 1;
    logger.warn("rabbit reconnect scheduled", { delayMs: delay, attempt: this.reconnectAttempt });
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.reconnectAll();
    }, delay);
    this.reconnectTimer.unref?.();
  }

  /** Rebuild the connection, the publish channel and any dead consumers. */
  private async reconnectAll(): Promise<void> {
    if (this.closed) return;
    try {
      await this.ensureBroker();
      for (const [queue, registration] of this.consumers) {
        const liveChannel = queue === QUEUE ? this.consumerChannel : this.dlqChannel;
        if (!liveChannel) {
          await this.setupConsumer(queue, registration.handler, registration.assign);
        }
      }
      logger.info("rabbit reconnected", { queues: [...this.consumers.keys()] });
    } catch (error) {
      logger.error("rabbit reconnect failed", { error: errorMessage(error) });
      this.scheduleReconnect();
    }
  }

  publish(payload: unknown, priority: "low" | "normal" | "high" = "normal"): void {
    if (!this.publisher) {
      // Kick off recovery in the background; the caller (outbox dispatcher)
      // defers this row with backoff, so nothing is lost.
      void this.reconnectAll().catch(() => undefined);
      throw new Error("RabbitMQ publisher not connected");
    }
    this.publisher.sendToQueue(QUEUE, Buffer.from(JSON.stringify(payload)), {
      persistent: true,
      contentType: "application/json",
      priority: PRIORITY_WEIGHT[priority],
      timestamp: Math.floor(Date.now() / 1000),
    });
  }

  publishDLQ(payload: unknown): void {
    if (!this.publisher) {
      void this.reconnectAll().catch(() => undefined);
      throw new Error("RabbitMQ publisher not connected");
    }
    this.publisher.sendToQueue(DLQ, Buffer.from(JSON.stringify(payload)), { persistent: true, contentType: "application/json" });
  }

  async waitForConfirms(): Promise<void> {
    if (!this.publisher) return;
    await this.publisher.waitForConfirms().catch(() => undefined);
  }

  async consume(handler: QueueHandler): Promise<void> {
    const registration: ConsumerRegistration = {
      handler,
      assign: (ch: Channel) => { this.consumerChannel = ch; },
    };
    this.consumers.set(QUEUE, registration);
    await this.setupConsumer(QUEUE, handler, registration.assign);
  }

  async consumeDLQ(handler: QueueHandler): Promise<void> {
    const registration: ConsumerRegistration = {
      handler,
      assign: (ch: Channel) => { this.dlqChannel = ch; },
    };
    this.consumers.set(DLQ, registration);
    await this.setupConsumer(DLQ, handler, registration.assign);
  }

  private async setupConsumer(queue: string, handler: QueueHandler, assign: (ch: Channel) => void): Promise<void> {
    if (this.closed) return;
    try {
      await this.ensureBroker();
      if (!this.broker) throw new Error("broker unavailable");
      const channel = await this.broker.createChannel();
      await channel.assertQueue(queue, { durable: true, arguments: { "x-max-priority": 10 } });
      await channel.prefetch(env.WORKER_CONCURRENCY);
      channel.on("error", (e: unknown) => logger.error("rabbit consumer channel error", { queue, error: errorMessage(e) }));
      channel.on("close", () => {
        if (this.consumerChannel === channel) this.consumerChannel = null;
        if (this.dlqChannel === channel) this.dlqChannel = null;
        if (!this.closed) {
          logger.warn("rabbit consumer channel closed — reconnect scheduled", { queue });
          this.scheduleReconnect();
        }
      });
      assign(channel);
      await channel.consume(queue, (message) => {
        if (message) handler(this.toMessage(message, channel));
      });
      logger.info("rabbit consumer ready", { queue });
    } catch (error) {
      logger.error("rabbit consumer setup failed", { queue, error: errorMessage(error) });
      this.scheduleReconnect();
    }
  }

  private toMessage(message: ConsumeMessage, channel: Channel): QueueMessage {
    const raw = message.content.toString();
    // A channel that died mid-processing rejects acks; the broker then
    // redelivers (at-least-once), so a closed channel must not crash the worker.
    const safe = (fn: () => void): void => {
      try {
        fn();
      } catch (error) {
        logger.warn("rabbit ack failed on closed channel", { error: errorMessage(error) });
      }
    };
    return {
      jobId: parseJobId(raw),
      raw,
      ack: () => safe(() => channel.ack(message)),
      nack: (requeue = false) => safe(() => channel.nack(message, false, requeue)),
      dlq: () => safe(() => {
        channel.sendToQueue(DLQ, message.content, { persistent: true });
        channel.ack(message);
      }),
    };
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.consumers.clear();
    try { await this.consumerChannel?.close(); } catch { /* already closed */ }
    try { await this.dlqChannel?.close(); } catch { /* already closed */ }
    try { await this.publisher?.close(); } catch { /* already closed */ }
    try { await this.broker?.close(); } catch { /* already closed */ }
    this.consumerChannel = null;
    this.dlqChannel = null;
    this.publisher = null;
    this.broker = null;
  }

  healthy(): boolean {
    return this.broker !== null && this.publisher !== null;
  }
}

export class MemoryQueue implements IQueue {
  private pending: Array<{ payload: unknown; raw: string; priority: number }> = [];
  private dlqMessages: Array<{ payload: unknown; raw: string; priority: number }> = [];
  private mainHandler: QueueHandler | null = null;
  private dlqHandler: QueueHandler | null = null;
  private timer: NodeJS.Timeout | null = null;
  private active = 0;

  constructor(private readonly concurrency: number = env.WORKER_CONCURRENCY) {}

  publish(payload: unknown, priority: "low" | "normal" | "high" = "normal"): void {
    this.pending.push({ payload, raw: JSON.stringify(payload), priority: PRIORITY_WEIGHT[priority] });
  }

  publishDLQ(payload: unknown): void {
    this.dlqMessages.push({ payload, raw: JSON.stringify(payload), priority: PRIORITY_WEIGHT.normal });
  }

  async consume(handler: QueueHandler): Promise<void> {
    this.mainHandler = handler;
    this.ensureTimer();
  }

  async consumeDLQ(handler: QueueHandler): Promise<void> {
    this.dlqHandler = handler;
    this.ensureTimer();
  }

  private ensureTimer(): void {
    if (this.timer) return;
    this.timer = setInterval(() => this.drain(), 100);
    this.timer.unref?.();
  }

  private drain(): void {
    this.drainFrom(this.pending, this.mainHandler);
    this.drainFrom(this.dlqMessages, this.dlqHandler);
  }

  private drainFrom(
    array: Array<{ payload: unknown; raw: string; priority: number }>,
    handler: QueueHandler | null,
  ): void {
    if (!handler) return;
    while (this.active < this.concurrency && array.length > 0) {
      const message = array.shift();
      if (!message) break;
      this.active += 1;
      const done = (): void => {
        this.active -= 1;
      };
      handler({
        jobId: parseJobId(message.raw),
        raw: message.raw,
        ack: done,
        nack: (requeue = false) => {
          if (requeue) array.unshift(message);
          done();
        },
        dlq: () => {
          this.dlqMessages.push(message);
          done();
        },
      });
    }
  }

  async close(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  healthy(): boolean {
    return true;
  }
}
