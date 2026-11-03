import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseMigrationFile, listMigrations, runMigrations, type MigrationClient, type MigrationRow } from "../src/store/migrate.js";

// Unit tests for the migration runner. DDL itself needs a live PostgreSQL
// (Docker is not available here), so the runner's bookkeeping — version
// ordering, duplicate detection, skip-applied, per-file transaction — is
// verified against a scripted fake client instead of a real pool.

function makeTempDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "pulseops-migrate-"));
}

class FakeClient implements MigrationClient {
  readonly log: string[] = [];
  applied = new Set<number>();

  async query(sql: string, params?: unknown[]): Promise<{ rowCount: number; rows: MigrationRow[] }> {
    this.log.push(sql.trim().split(/\s+/)[0] ?? "");
    if (/^SELECT pg_advisory_lock/.test(sql)) return { rowCount: 1, rows: [] };
    if (/^SELECT pg_advisory_unlock/.test(sql)) return { rowCount: 1, rows: [] };
    if (/^CREATE TABLE IF NOT EXISTS schema_migrations/.test(sql)) return { rowCount: 0, rows: [] };
    if (/api.test|000_init|001_baseline|002_add|003_x|999_y/.test(sql)) return { rowCount: 0, rows: [] };
    const versionMatch = /SELECT version FROM schema_migrations WHERE version = \$1/.exec(sql);
    if (versionMatch) {
      const version = params?.[0] as number;
      return { rowCount: this.applied.has(version) ? 1 : 0, rows: this.applied.has(version) ? [{ version }] : [] };
    }
    const insertMatch = /^INSERT INTO schema_migrations/.test(sql);
    if (insertMatch) {
      const version = params?.[0] as number;
      this.applied.add(version);
      return { rowCount: 1, rows: [] };
    }
    return { rowCount: 0, rows: [] };
  }

  async release(): Promise<void> { /* no-op */ }
}

class FakePool {
  constructor(private readonly client: FakeClient) {}
  async connect(): Promise<FakeClient> {
    return this.client;
  }
}

interface RunResult {
  applied: string[];
  log: string[];
}

async function run(dirFiles: Record<string, string>): Promise<RunResult> {
  const dir = await makeTempDir();
  try {
    for (const [name, contents] of Object.entries(dirFiles)) {
      await writeFile(join(dir, name), contents);
    }
    const client = new FakeClient();
    const applied = await runMigrations(new FakePool(client) as never, { dir });
    return { applied, log: client.log };
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("parseMigrationFile accepts 3-digit numeric versions and rejects the rest", () => {
  assert.deepEqual(parseMigrationFile("001_init.sql"), { version: 1, name: "001_init.sql" });
  assert.deepEqual(parseMigrationFile("042_add_widget_columns.sql"), { version: 42, name: "042_add_widget_columns.sql" });
  assert.equal(parseMigrationFile("README.md"), null);
  assert.equal(parseMigrationFile("1_init.sql"), null, "must be zero-padded to 3 digits");
  assert.equal(parseMigrationFile("001.init.sql"), null, "dot is not an allowed separator");
  assert.deepEqual(parseMigrationFile("001_INIT.sql"), { version: 1, name: "001_INIT.sql" }, "\\w allows uppercase");
});

test("listMigrations returns files ordered by version", async () => {
  const dir = await makeTempDir();
  try {
    await writeFile(join(dir, "010_b.sql"), "");
    await writeFile(join(dir, "002_a.sql"), "");
    await writeFile(join(dir, "001_zero.sql"), "");
    const files = await listMigrations(dir);
    assert.deepEqual(files.map((f) => f.version), [1, 2, 10]);
    assert.deepEqual(files.map((f) => f.name), ["001_zero.sql", "002_a.sql", "010_b.sql"]);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("listMigrations throws on duplicate versions", async () => {
  const dir = await makeTempDir();
  try {
    await writeFile(join(dir, "001_a.sql"), "");
    await writeFile(join(dir, "001_b.sql"), "");
    await assert.rejects(listMigrations(dir), /duplicate migration version 1/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("runMigrations applies pending files in order and inserts version rows", async () => {
  const result = await run({
    "001_init.sql": "CREATE TABLE t1 (id int);",
    "003_third.sql": "CREATE TABLE t3 (id int);",
  });
  assert.deepEqual(result.applied, ["001_init.sql", "003_third.sql"]);
  const creates = result.log.filter((op) => op === "CREATE" || /schema_migrations/.test(op));
  assert.ok(result.log.includes("INSERT"));
  assert.ok([...result.log].filter((op) => op === "INSERT").length === 2, "one version row per applied migration");
});

test("runMigrations skips already-applied migrations", async () => {
  const result = await run({
    "001_init.sql": "CREATE TABLE t1 (id int);",
    "002_second.sql": "CREATE TABLE t2 (id int);",
  });
  // Re-run against the same logged fake client — applied versions are skipped.
  const dir = await makeTempDir();
  try {
    await writeFile(join(dir, "001_init.sql"), "CREATE TABLE t1 (id int);");
    await writeFile(join(dir, "002_second.sql"), "CREATE TABLE t2 (id int);");
    const client = new FakeClient();
    client.applied.add(1);
    const applied = await runMigrations(new FakePool(client) as never, { dir });
    assert.deepEqual(applied, ["002_second.sql"], "001 is already applied, only 002 runs");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("runMigrations rolls back and rethrows on a failing migration", async () => {
  class FailingClient extends FakeClient {
    override async query(sql: string, params?: unknown[]): Promise<{ rowCount: number; rows: MigrationRow[] }> {
      if (/CREATE TABLE t_bad/.test(sql)) throw new Error("syntax error near t_bad");
      if (sql.trim() === "ROLLBACK") {
        this.log.push("ROLLBACK");
        return { rowCount: 0, rows: [] };
      }
      return super.query(sql, params);
    }
  }
  const dir = await makeTempDir();
  try {
    await writeFile(join(dir, "001_ok.sql"), "CREATE TABLE t_ok (id int);");
    await writeFile(join(dir, "002_bad.sql"), "CREATE TABLE t_bad (id int) BROKEN;");
    const client = new FailingClient();
    await assert.rejects(runMigrations(new FakePool(client) as never, { dir }), /syntax error near t_bad/);
    assert.ok(client.log.includes("ROLLBACK"), "a failed migration must trigger ROLLBACK");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
