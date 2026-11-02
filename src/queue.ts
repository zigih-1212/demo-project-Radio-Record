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

export class RabbitQueue implements IQueue {
  private broker: ChannelModel | null = null;
  private publisher: ConfirmChannel | null = null;
  private consumerChannel: Channel | null = null;
  private dlqChannel: Channel | null = null;
  private closed = false;
  private reconnectTimer: NodeJS.Timeout | null = null;

  async connect(): Promise<void> {
    if (!this.broker) {
      this.broker = await amqp.connect(env.AMQP_URL);
      this.broker.on("error", (e: unknown) => logger.error("rabbit broker error", { error: errorMessage(e) }));
      this.broker.on("close", () => {
        this.publisher = null;
        this.consumerChannel = null;
        this.dlqChannel = null;
      });
    }
    if (!this.publisher) {
      this.publisher = await this.broker.createConfirmChannel();
      this.publisher.on("error", (e: unknown) => logger.error("rabbit publisher error", { error: errorMessage(e) }));
      await this.publisher.assertQueue(QUEUE, { durable: true, arguments: { "x-max-priority": 10 } });
      await this.publisher.assertQueue(DLQ, { durable: true });
    }
  }

  publish(payload: unknown, priority: "low" | "normal" | "high" = "normal"): void {
    if (!this.publisher) throw new Error("RabbitMQ publisher not connected");
    this.publisher.sendToQueue(QUEUE, Buffer.from(JSON.stringify(payload)), {
      persistent: true,
      contentType: "application/json",
      priority: PRIORITY_WEIGHT[priority],
      timestamp: Math.floor(Date.now() / 1000),
    });
  }

  publishDLQ(payload: unknown): void {
    if (!this.publisher) throw new Error("RabbitMQ publisher not connected");
    this.publisher.sendToQueue(DLQ, Buffer.from(JSON.stringify(payload)), { persistent: true, contentType: "application/json" });
  }

  async waitForConfirms(): Promise<void> {
    if (!this.publisher) return;
    await this.publisher.waitForConfirms().catch(() => undefined);
  }

  async consume(handler: QueueHandler): Promise<void> {
    await this.setupConsumer(QUEUE, handler, (ch) => { this.consumerChannel = ch; });
  }

  async consumeDLQ(handler: QueueHandler): Promise<void> {
    await this.setupConsumer(DLQ, handler, (ch) => { this.dlqChannel = ch; });
  }

  private async setupConsumer(queue: string, handler: QueueHandler, assign: (ch: Channel) => void): Promise<void> {
    if (this.closed || !this.broker) return;
    try {
      const channel = await this.broker.createChannel();
      await channel.assertQueue(queue, { durable: true, arguments: { "x-max-priority": 10 } });
      await channel.prefetch(env.WORKER_CONCURRENCY);
      channel.on("error", (e: unknown) => logger.error("rabbit consumer channel error", { queue, error: errorMessage(e) }));
      channel.on("close", () => {
        if (this.consumerChannel === channel) this.consumerChannel = null;
        if (this.dlqChannel === channel) this.dlqChannel = null;
        if (!this.closed) this.scheduleReconnect(queue, handler, assign);
      });
      assign(channel);
      await channel.consume(queue, (message) => {
        if (message) handler(this.toMessage(message, channel));
      });
      logger.info("rabbit consumer ready", { queue });
    } catch (error) {
      logger.error("rabbit consumer setup failed", { queue, error: errorMessage(error) });
      this.scheduleReconnect(queue, handler, assign);
    }
  }

  private scheduleReconnect(queue: string, handler: QueueHandler, assign: (ch: Channel) => void): void {
    if (this.closed || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.setupConsumer(queue, handler, assign);
    }, 3_000);
    this.reconnectTimer.unref?.();
  }

  private toMessage(message: ConsumeMessage, channel: Channel): QueueMessage {
    const raw = message.content.toString();
    return {
      jobId: parseJobId(raw),
      raw,
      ack: () => channel.ack(message),
      nack: (requeue = false) => channel.nack(message, false, requeue),
      dlq: () => {
        channel.sendToQueue(DLQ, message.content, { persistent: true });
        channel.ack(message);
      },
    };
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
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
    return this.broker !== null;
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
