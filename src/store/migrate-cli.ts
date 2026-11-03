import process from "node:process";
import { Pool } from "pg";
import { env, warnIfInsecureDefaults } from "../config.js";
import { logger, errorMessage } from "../logger.js";
import { runMigrations } from "./migrate.js";

/**
 * CLI entry used by `npm run migrate`. The application also runs migrations
 * automatically at bootstrap (PostgresStore.init), so this is for explicit,
 * shared-schema control (e.g. before parallel deploy of api + worker).
 */
async function main(): Promise<void> {
  warnIfInsecureDefaults();
  const pool = new Pool({ connectionString: env.DATABASE_URL, max: 2, idleTimeoutMillis: 30_000 });
  try {
    const applied = await runMigrations(pool);
    logger.info("migrations applied", { count: applied.length, files: applied });
  } catch (error) {
    logger.error("migration failed", { error: errorMessage(error) });
    process.exitCode = 1;
  } finally {
    await pool.end().catch(() => undefined);
  }
}

void main();
