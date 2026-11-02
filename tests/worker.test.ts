import { test } from "node:test";
import assert from "node:assert/strict";
import { MemoryStore } from "../src/store/memory.js";
import { MemoryCache } from "../src/cache.js";
import { WorkerService } from "../src/services/worker.js";
import type { IQueue, JobRow, QueueMessage } from "../src/types.js";

class FakeQueue implements IQueue {
  published: unknown[] = [];
  dlq: unknown[] = [];
  publish(payload: unknown): void {
    this.published.push(payload);
  }
  publishDLQ(payload: unknown): void {
    this.dlq.push(payload);
  }
  async consume(): Promise<void> {}
  async consumeDLQ(): Promise<void> {}
  async close(): Promise<void> {}
  healthy(): boolean {
    return true;
  }
}

function fakeMessage(jobId: string): QueueMessage & { calls: string[] } {
  const calls: string[] = [];
  return {
    jobId,
    raw: JSON.stringify({ jobId }),
    ack: () => calls.push("ack"),
    nack: (requeue?: boolean) => calls.push("nack:" + String(requeue)),
    dlq: () => calls.push("dlq"),
    calls,
  };
}

function makeStore(): MemoryStore {
  return new MemoryStore();
}

test("worker: queued job is processed to succeeded", async () => {
  const store = makeStore();
  await store.init();
  const queue = new FakeQueue();
  const worker = new WorkerService(store, queue, new MemoryCache());
  const job = await store.createJob({ title: "ok job", type: "send_digest", priority: "normal", simulateFailure: false });
  const msg = fakeMessage(job.id);
  await worker.handleJobMessage(msg);
  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "succeeded");
  assert.ok(msg.calls.includes("ack"));
  const events = await store.getEvents(job.id);
  assert.ok(events.some((e) => e.event_type === "processing"));
  assert.ok(events.some((e) => e.event_type === "succeeded"));
});

test("worker: business failure schedules retry with backoff (attempts < max)", async () => {
  const store = makeStore();
  await store.init();
  const worker = new WorkerService(store, new FakeQueue(), new MemoryCache());
  const job = await store.createJob({ title: "flaky job", type: "import_orders", priority: "normal", simulateFailure: true });
  const msg = fakeMessage(job.id);
  await worker.handleJobMessage(msg);
  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "queued", "job must be requeued, not failed");
  assert.ok(new Date(reloaded?.available_at ?? 0).getTime() > Date.now(), "retry must be delayed (backoff)");
  assert.equal(reloaded?.attempts, 1);
  assert.ok(msg.calls.includes("ack"));
  assert.ok((await store.outboxCount()) >= 1, "job.retry outbox row must exist");
});

test("worker: max attempts exhausted → failed + DLQ publish", async () => {
  const store = makeStore();
  await store.init();
  const queue = new FakeQueue();
  const worker = new WorkerService(store, queue, new MemoryCache());
  const job = await store.createJob({ title: "poison job", type: "import_orders", priority: "low", simulateFailure: true });
  // simulate attempts = max_attempts - 1 (claim will increment to max_attempts)
  const mem = store as unknown as { jobs: Map<string, JobRow> };
  const row = mem.jobs.get(job.id);
  assert.ok(row);
  row.attempts = row.max_attempts - 1;
  const msg = fakeMessage(job.id);
  await worker.handleJobMessage(msg);
  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "failed");
  assert.equal(queue.dlq.length, 1, "message must be published to DLQ");
  const dlqPayload = queue.dlq[0] as { jobId: string; reason: string };
  assert.equal(dlqPayload.jobId, job.id);
  assert.equal(dlqPayload.reason, "max_attempts");
  assert.ok(msg.calls.includes("ack"));
});

test("worker: redelivery of a terminal job is a no-op (side-effect-once)", async () => {
  const store = makeStore();
  await store.init();
  const worker = new WorkerService(store, new FakeQueue(), new MemoryCache());
  const job = await store.createJob({ title: "once", type: "campaign_publish", priority: "high", simulateFailure: false });
  await worker.handleJobMessage(fakeMessage(job.id));
  const eventsAfterFirst = await store.getEvents(job.id);
  // simulate RabbitMQ redelivery of the same message
  await worker.handleJobMessage(fakeMessage(job.id));
  const eventsAfterSecond = await store.getEvents(job.id);
  assert.equal(eventsAfterSecond.length, eventsAfterFirst.length, "no duplicate processing events");
  assert.equal(eventsAfterSecond.filter((e) => e.event_type === "processing").length, 1);
});

test("worker: malformed message is moved to DLQ", async () => {
  const store = makeStore();
  await store.init();
  const queue = new FakeQueue();
  const worker = new WorkerService(store, queue, new MemoryCache());
  const msg = fakeMessage("");
  await worker.handleJobMessage(msg);
  assert.ok(msg.calls.includes("dlq"));
  assert.ok(!msg.calls.includes("ack"));
});

test("worker: transient store failure requeues the message (no loss)", async () => {
  class BrokenClaimStore extends MemoryStore {
    override async claim(): Promise<JobRow | null> {
      throw new Error("ECONNREFUSED");
    }
  }
  const store = new BrokenClaimStore();
  await store.init();
  const worker = new WorkerService(store, new FakeQueue(), new MemoryCache());
  const msg = fakeMessage("00000000-0000-0000-0000-000000000000");
  await worker.handleJobMessage(msg);
  assert.ok(msg.calls.includes("nack:true"), "message must be requeued, not dropped");
});

test("worker: crash mid-job is recovered by the lease reaper", async () => {
  const store = makeStore();
  await store.init();
  const worker = new WorkerService(store, new FakeQueue(), new MemoryCache());
  const job = await store.createJob({ title: "crash test", type: "sync_catalog", priority: "normal", simulateFailure: false });
  // worker w1 claims the job (lease 1s) and then "crashes" (never finishes)
  const claimed = await store.claim(job.id, "w1", 1);
  assert.ok(claimed);
  // reaper runs with a clock in the future → lease expired
  const recovered = await store.resetExpiredLeases(new Date(Date.now() + 10_000).toISOString());
  assert.deepEqual(recovered, [job.id]);
  // a redelivered message (or w2) can now claim and complete it
  await worker.handleJobMessage(fakeMessage(job.id));
  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "succeeded");
  assert.equal(reloaded?.attempts, 2, "at-least-once: attempts incremented");
});
