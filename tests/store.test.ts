import { test } from "node:test";
import assert from "node:assert/strict";
import { MemoryStore } from "../src/store/memory.js";
import { env } from "../src/config.js";
import type { CreateJobInput, JobRow, OutboxRow } from "../src/types.js";

const INPUT: CreateJobInput = { title: "store test job", type: "sync_catalog", priority: "normal", simulateFailure: false };

test("claim is atomic and sets a lease; second worker cannot claim", async () => {
  const store = new MemoryStore();
  await store.init();
  const job = await store.createJob(INPUT);
  const claimed = await store.claim(job.id, "w1", env.JOB_LEASE_TTL_S);
  assert.ok(claimed, "first claim must succeed");
  assert.equal(claimed.status, "processing");
  assert.equal(claimed.attempts, 1);
  assert.equal(claimed.locked_by, "w1");
  assert.ok(claimed.lease_expires_at, "lease_expires_at must be set");
  const second = await store.claim(job.id, "w2", env.JOB_LEASE_TTL_S);
  assert.equal(second, null, "second claim must fail while lease is held");
});

test("resetExpiredLeases recovers jobs of a crashed worker", async () => {
  const store = new MemoryStore();
  await store.init();
  const job = await store.createJob(INPUT);
  await store.claim(job.id, "w1", 1);
  const recovered = await store.resetExpiredLeases(new Date(Date.now() + 10_000).toISOString());
  assert.deepEqual(recovered, [job.id]);
  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "queued");
  assert.equal(reloaded?.locked_by, null);
  assert.equal(reloaded?.lease_expires_at, null);
  // another worker can now claim it (attempts incremented — at-least-once)
  const reclaimed = await store.claim(job.id, "w2", 60);
  assert.ok(reclaimed);
  assert.equal(reclaimed.attempts, 2);
});

test("finish is guarded by locked_by (cancel/lease race safe)", async () => {
  const store = new MemoryStore();
  await store.init();
  const job = await store.createJob(INPUT);
  await store.claim(job.id, "w1", 60);
  const wrongWorker = await store.finish(job.id, "w2", "succeeded", null);
  assert.equal(wrongWorker, false);
  const rightWorker = await store.finish(job.id, "w1", "succeeded", null);
  assert.equal(rightWorker, true);
  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "succeeded");
});

test("scheduleRetry requeues with future availability and a job.retry outbox row", async () => {
  const store = new MemoryStore();
  await store.init();
  const job = await store.createJob(INPUT);
  await store.claim(job.id, "w1", 60);
  const future = new Date(Date.now() + 30_000).toISOString();
  const ok = await store.scheduleRetry(job.id, future);
  assert.equal(ok, true);
  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "queued");
  assert.ok(new Date(reloaded?.available_at ?? 0).getTime() > Date.now(), "available_at must be in the future");
  const pending = await store.outboxPending();
  // the job.retry row is deferred (future) — not immediately pending
  assert.equal(pending.some((row: OutboxRow) => row.event_type === "job.retry" && row.aggregate_id === job.id), false);
  assert.ok((await store.outboxCount()) >= 1);
});

test("retryJob only works for failed jobs", async () => {
  const store = new MemoryStore();
  await store.init();
  const job = await store.createJob(INPUT);
  assert.equal(await store.retryJob(job.id), null, "queued job must not be retryable");
  await store.claim(job.id, "w1", 60);
  await store.finish(job.id, "w1", "failed", "boom");
  const retried = await store.retryJob(job.id);
  assert.ok(retried);
  assert.equal(retried?.status, "queued");
});

test("resetForReplay requeues a failed job with attempts reset", async () => {
  const store = new MemoryStore();
  await store.init();
  const job = await store.createJob(INPUT);
  await store.claim(job.id, "w1", 60);
  await store.finish(job.id, "w1", "failed", "boom");
  const ok = await store.resetForReplay(job.id);
  assert.equal(ok, true);
  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "queued");
  assert.equal(reloaded?.attempts, 0);
  assert.equal(reloaded?.error, null);
});

test("outbox: defer applies backoff (row leaves pending window), markOutboxSent completes", async () => {
  const store = new MemoryStore();
  await store.init();
  const job = await store.createJob(INPUT);
  const pending = await store.outboxPending();
  const row = pending.find((r) => r.aggregate_id === job.id);
  assert.ok(row, "createJob must enqueue a job.queued outbox row");
  await store.deferOutbox([row.id], 0, "test error");
  const afterDefer = await store.outboxPending();
  assert.ok(!afterDefer.some((r) => r.id === row.id), "deferred row must leave the pending window");
  await store.markOutboxSent([row.id]);
  const afterSent = await store.outboxPending();
  assert.ok(!afterSent.some((r) => r.id === row.id), "sent row must not be pending");
});

test("listJobs supports filters, search and pagination", async () => {
  const store = new MemoryStore();
  await store.init();
  await store.createJob({ ...INPUT, title: "lstest alpha campaign", type: "campaign_publish", priority: "high" });
  await store.createJob({ ...INPUT, title: "lstest beta digest", type: "send_digest", priority: "low" });
  await store.createJob({ ...INPUT, title: "lstest gamma campaign", type: "campaign_publish", priority: "low" });
  const all = await store.listJobs({ q: "lstest", limit: 100, offset: 0 });
  assert.equal(all.total, 3, "search must find all 3 created jobs");
  const filtered = await store.listJobs({ q: "lstest", type: "campaign_publish", limit: 100, offset: 0 });
  assert.equal(filtered.total, 2, "type filter must narrow to campaign jobs");
  const byPriority = await store.listJobs({ q: "lstest", priority: "low", limit: 100, offset: 0 });
  assert.equal(byPriority.total, 2, "priority filter must narrow to the two low-priority jobs");
  const page = await store.listJobs({ q: "lstest", limit: 1, offset: 1 });
  assert.equal(page.items.length, 1, "pagination must return exactly one item per page");
  assert.equal(page.total, 3);
});

test("createJob is atomic: job + outbox row + created event", async () => {
  const store = new MemoryStore();
  await store.init();
  const job: JobRow = await store.createJob(INPUT);
  const events = await store.getEvents(job.id);
  assert.ok(events.some((e) => e.event_type === "created"));
  assert.ok((await store.outboxCount()) >= 1);
});
