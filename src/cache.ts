import Redis from "ioredis";
import { env } from "./config.js";
import { errorMessage } from "./logger.js";
import { logger } from "./logger.js";
import type { ICache } from "./types.js";

export class RedisCache implements ICache {
  private client: Redis | null = null;

  private async conn(): Promise<Redis> {
    if (!this.client) {
      this.client = new Redis(env.REDIS_URL, {
        maxRetriesPerRequest: 1,
        enableOfflineQueue: false,
        retryStrategy: (times) => (times > 3 ? null : Math.min(times * 200, 1_000)),
        lazyConnect: true,
      });
      this.client.on("error", (e: unknown) =>
        logger.error("redis error", { error: errorMessage(e) }),
      );
      await this.client.connect().catch(() => undefined);
    }
    return this.client;
  }

  async get(key: string): Promise<string | null> {
    try {
      return await (await this.conn()).get(key);
    } catch {
      return null;
    }
  }

  async set(key: string, value: string, ttlSeconds?: number): Promise<void> {
    try {
      const client = await this.conn();
      if (ttlSeconds) await client.set(key, value, "EX", ttlSeconds);
      else await client.set(key, value);
    } catch {
      /* cache is optional */
    }
  }

  async del(key: string): Promise<void> {
    try {
      await (await this.conn()).del(key);
    } catch {
      /* cache is optional */
    }
  }

  async incr(key: string): Promise<number> {
    try {
      return await (await this.conn()).incr(key);
    } catch {
      return 0;
    }
  }

  async expire(key: string, seconds: number): Promise<void> {
    try {
      await (await this.conn()).expire(key, seconds);
    } catch {
      /* cache is optional */
    }
  }

  async ping(): Promise<boolean> {
    try {
      return (await (await this.conn()).ping()) === "PONG";
    } catch {
      return false;
    }
  }

  async quit(): Promise<void> {
    await this.client?.quit().catch(() => undefined);
    this.client = null;
  }
}

export class MemoryCache implements ICache {
  private store = new Map<string, { value: string; expiresAt: number | null }>();

  private prune(): void {
    const now = Date.now();
    for (const [key, entry] of this.store) {
      if (entry.expiresAt != null && entry.expiresAt < now) this.store.delete(key);
    }
  }

  async get(key: string): Promise<string | null> {
    this.prune();
    const entry = this.store.get(key);
    return entry ? entry.value : null;
  }

  async set(key: string, value: string, ttlSeconds?: number): Promise<void> {
    this.store.set(key, { value, expiresAt: ttlSeconds ? Date.now() + ttlSeconds * 1000 : null });
  }

  async del(key: string): Promise<void> {
    this.store.delete(key);
  }

  async incr(key: string): Promise<number> {
    const current = Number(await this.get(key)) || 0;
    const next = current + 1;
    await this.set(key, String(next));
    return next;
  }

  async expire(key: string, seconds: number): Promise<void> {
    const entry = this.store.get(key);
    if (entry) entry.expiresAt = Date.now() + seconds * 1000;
  }

  async ping(): Promise<boolean> {
    return true;
  }
}
