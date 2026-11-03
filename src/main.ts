import http from "node:http";
import process from "node:process";
import { Pool } from "pg";
import { env, isDemo, warnIfInsecureDefaults } from "./config.js";
import { errorMessage, logger } from "./logger.js";
import { hashPassword } from "./auth.js";
import { MemoryCache, RedisCache } from "./cache.js";
import { MemoryQueue, RabbitQueue } from "./queue.js";
import { MemoryStore } from "./store/memory.js";
import { PostgresStore } from "./store/postgres.js";
import { JobService } from "./services/jobs.js";
import { OutboxDispatcher } from "./services/outbox.js";
import { WorkerService } from "./services/worker.js";
import { createApp } from "./app.js";
import type { ICache, IQueue, IStore } from "./types.js";

async function seedAdmin(store: IStore): Promise<void> {
  try {
    const existing = await store.findUserByEmail(env.ADMIN_EMAIL);
    if (existing) return;
    await store.createUser(env.ADMIN_EMAIL, hashPassword(env.ADMIN_PASSWORD), "admin");
    logger.info("admin user seeded", { email: env.ADMIN_EMAIL });
  } catch (error) {
    // api and worker may start in parallel against an empty database; the
    // loser of that race must not abort startup. createUser is idempotent
    // (Postgres ON CONFLICT), so this is a safety net for any other error too.
    logger.warn("admin seed skipped", { email: env.ADMIN_EMAIL, error: errorMessage(error) });
  }
}

export async function bootstrap(): Promise<void> {
  warnIfInsecureDefaults();

  let store: IStore;
  let cache: ICache;
  let queue: IQueue;
  let pool: Pool | null = null;

  if (isDemo) {
    store = new MemoryStore();
    cache = new MemoryCache();
    queue = new MemoryQueue();
  } else {
    pool = new Pool({ connectionString: env.DATABASE_URL, max: 10, idleTimeoutMillis: 30_000 });
    store = new PostgresStore(pool);
    cache = new RedisCache();
    queue = new RabbitQueue();
    await pool.query("SELECT 1");
    await (cache as RedisCache).ping();
    await (queue as RabbitQueue).connect();
  }

  try {
    await store.init();
    await seedAdmin(store);

    const jobService = new JobService(store, cache);
    const dispatcher = new OutboxDispatcher(store, queue);
    const worker = new WorkerService(store, queue, cache);
    const app = createApp({ store, cache, queue, jobService });
    const server = http.createServer(app);

    server.on("error", (err: Error) => {
      logger.error("listen failed", { error: errorMessage(err) });
      process.exit(1);
    });

    const role = process.argv[2] === "worker" ? "worker" : "api";
    const runApi = isDemo || role === "api";
    const runWorker = isDemo || role === "worker";

    if (runApi) dispatcher.start();
    if (runWorker) worker.start();

    if (runApi) {
      await new Promise<void>((resolve) => {
        server.listen(env.PORT, () => {
          logger.info("PulseOps ready", { url: `http://localhost:${env.PORT}`, mode: isDemo ? "demo" : "docker", role });
          resolve();
        });
      });
    }

    let shuttingDown = false;
    const shutdown = async (signal: string): Promise<void> => {
      if (shuttingDown) return;
      shuttingDown = true;
      logger.info("shutdown signal received", { signal });
      dispatcher.stop();
      await worker.stop();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      if (cache instanceof RedisCache) await cache.quit().catch(() => undefined);
      if (pool) await pool.end().catch(() => undefined);
      process.exit(0);
    };
    process.on("SIGINT", () => void shutdown("SIGINT"));
    process.on("SIGTERM", () => void shutdown("SIGTERM"));
  } catch (err) {
    logger.error("startup failed", { error: errorMessage(err) });
    process.exitCode = 1;
    if (pool) await pool.end().catch(() => undefined);
  }
}
