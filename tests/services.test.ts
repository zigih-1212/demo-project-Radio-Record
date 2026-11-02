import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { MemoryStore } from "../src/store/memory.js";
import { MemoryCache } from "../src/cache.js";
import { JobService } from "../src/services/jobs.js";
import { ConflictError, NotFoundError } from "../src/errors.js";
import type { CreateJobInput } from "../src/types.js";

const INPUT: CreateJobInput = { title: "service test job", type: "campaign_publish", priority: "high", simulateFailure: false };

function makeService() {
  const store = new MemoryStore();
  const cache = new MemoryCache();
  return { store, cache, service: new JobService(store, cache) };
}

test("jobs.create: idempotency key returns the same job on retry", async () => {
  const { service } = makeService();
  const first = await service.create(INPUT, "idem-key-1");
  const second = await service.create(INPUT, "idem-key-1");
  assert.equal(first.item.id, second.item.id);
  assert.equal(second.idempotent, true);
  const other = await service.create(INPUT, "idem-key-2");
  assert.notEqual(other.item.id, first.item.id);
  assert.equal(other.idempotent, undefined);
});

test("jobs.create without idempotency key creates a new job", async () => {
  const { service } = makeService();
  const a = await service.create(INPUT);
  const b = await service.create(INPUT);
  assert.notEqual(a.item.id, b.item.id);
  assert.equal(a.idempotent, undefined);
});

test("jobs.retry: only failed jobs below max attempts can be retried", async () => {
  const { store, service } = makeService();
  const created = await service.create(INPUT);
  await assert.rejects(() => service.retry(created.item.id), ConflictError, "queued job is not retryable");
  await store.claim(created.item.id, "w1", 60);
  await store.finish(created.item.id, "w1", "failed", "boom");
  const retried = await service.retry(created.item.id);
  assert.equal(retried.status, "queued");
  // exhausted attempts → conflict
  const exhausted = await service.create(INPUT);
  const mem = store as unknown as { jobs: Map<string, { attempts: number }> };
  mem.jobs.get(exhausted.item.id)!.attempts = 5;
  await assert.rejects(() => service.retry(exhausted.item.id), ConflictError);
});

test("jobs.cancel: queued job becomes cancelled; terminal jobs rejected", async () => {
  const { service } = makeService();
  const created = await service.create(INPUT);
  const cancelled = await service.cancel(created.item.id);
  assert.equal(cancelled.status, "cancelled");
  await assert.rejects(() => service.cancel(created.item.id), ConflictError);
});

test("jobs.get: unknown id → NotFoundError; detail includes events", async () => {
  const { service } = makeService();
  await assert.rejects(() => service.get(randomUUID()), NotFoundError);
  const created = await service.create(INPUT);
  const detail = await service.get(created.item.id);
  assert.equal(detail.job.id, created.item.id);
  assert.ok(detail.events.length >= 1);
});

test("jobs.dlqReplay: failed job is reset and republished via outbox", async () => {
  const { store, service } = makeService();
  const created = await service.create(INPUT);
  await store.claim(created.item.id, "w1", 60);
  await store.finish(created.item.id, "w1", "failed", "permanent");
  const replayed = await service.dlqReplay(created.item.id);
  assert.equal(replayed.status, "queued");
  assert.equal(replayed.attempts, 0);
  assert.equal(replayed.error, null);
  await assert.rejects(() => service.dlqReplay(created.item.id), ConflictError);
});

test("jobs.metricsText: prometheus exposition format", async () => {
  const { service } = makeService();
  await service.create(INPUT);
  const text = await service.metricsText();
  assert.ok(text.includes('pulseops_jobs{status="queued"}'));
  assert.ok(text.includes("pulseops_outbox_pending"));
  assert.ok(text.startsWith("# TYPE"));
});
