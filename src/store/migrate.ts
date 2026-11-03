import { readdir, readFile as readFileAsync } from "node:fs/promises";
import { join } from "node:path";

/**
 * Minimal query surface the runner needs — satisfied by a `pg` Pool, and by a
 * fake in tests so the runner logic can be verified without a live PostgreSQL.
 */
export interface MigrationRow {
  version: number;
}
export interface MigrationClient {
  query: (sql: string, params?: unknown[]) => Promise<{ rowCount: number | null; rows: MigrationRow[] }>;
  release: () => void;
}
export interface MigrationPool {
  connect: () => Promise<MigrationClient>;
}

const MIGRATIONS_DIR = () => join(__dirname, "..", "..", "migrations");
const MIGRATION_FILE = /^(\d{3})_[\w-]+\.sql$/;

export function parseMigrationFile(file: string): { version: number; name: string } | null {
  const match = MIGRATION_FILE.exec(file);
  return match ? { version: Number(match[1]), name: file } : null;
}

/**
 * Read and order migration files. Throws on duplicate versions so a renamed
 * migration cannot silently replace another: every version must be unique.
 */
export async function listMigrations(dir: string): Promise<Array<{ version: number; name: string }>> {
  const entries = await readdir(dir);
  const migrations = entries
    .map(parseMigrationFile)
    .filter((m): m is { version: number; name: string } => m !== null)
    .sort((a, b) => a.version - b.version);
  for (let i = 1; i < migrations.length; i++) {
    const current = migrations[i]!;
    const previous = migrations[i - 1]!;
    if (current.version === previous.version) {
      throw new Error(`duplicate migration version ${current.version}`);
    }
  }
  return migrations;
}

/**
 * Apply pending migrations in version order, each atomically (DDL + version
 * insert in one transaction). Already-applied migrations are skipped.
 *
 * `pg_advisory_lock` serialises concurrent boot of api + worker against the
 * same database; the lock is released in `finally`.
 */
export async function runMigrations(pool: MigrationPool, options: { dir?: string } = {}): Promise<string[]> {
  const dir = options.dir ?? MIGRATIONS_DIR();
  const migrations = await listMigrations(dir);
  const client = await pool.connect();
  const applied: string[] = [];
  try {
    await client.query("SELECT pg_advisory_lock(721001)");
    await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      version INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);
    for (const migration of migrations) {
      const existing = await client.query("SELECT version FROM schema_migrations WHERE version = $1", [migration.version]);
      if ((existing.rowCount ?? 0) > 0) continue;
      const sql = await readFileAsync(join(dir, migration.name), "utf8");
      await client.query("BEGIN");
      try {
        await client.query(sql);
        await client.query("INSERT INTO schema_migrations(version, name) VALUES ($1, $2)", [
          migration.version,
          migration.name,
        ]);
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK").catch(() => undefined);
        throw error;
      }
      applied.push(migration.name);
    }
  } finally {
    await client.query("SELECT pg_advisory_unlock(721001)").catch(() => undefined);
    await client.release();
  }
  return applied;
}
