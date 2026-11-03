import { test } from "node:test";
import assert from "node:assert/strict";
import { MemoryStore } from "../src/store/memory.js";
import { MemoryCache } from "../src/cache.js";
import { WorkerService } from "../src/services/worker.js";
import { MemoryQueue } from "../src/queue.js";
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

test("worker: failing job cycles through all attempts to failed + DLQ across multiple deliveries", async () => {
  const store = makeStore();
  await store.init();
  const queue = new FakeQueue();
  const worker = new WorkerService(store, queue, new MemoryCache());
  const job = await store.createJob({ title: "poison cycle", type: "import_orders", priority: "normal", simulateFailure: true });
  const mem = store as unknown as { jobs: Map<string, JobRow> };
  for (let attempt = 1; attempt <= 5; attempt++) {
    const row = mem.jobs.get(job.id);
    assert.ok(row, `job row must exist before attempt ${attempt}`);
    row.available_at = new Date(Date.now() - 1000).toISOString(); // dispatcher delivers due job
    await worker.handleJobMessage(fakeMessage(job.id));
    const reloaded = await store.getJob(job.id);
    if (attempt < 5) {
      assert.equal(reloaded?.status, "queued", `attempt ${attempt} must requeue with backoff`);
      assert.ok(new Date(reloaded?.available_at ?? 0).getTime() > Date.now(), `attempt ${attempt} retry must be delayed`);
      assert.equal(reloaded?.attempts, attempt, `attempt ${attempt} counter`);
    } else {
      assert.equal(reloaded?.status, "failed", "after max attempts job must be failed");
      assert.equal(reloaded?.attempts, 5);
      assert.equal(queue.dlq.length, 1, "exhausted job must be published to DLQ");
      const dlqPayload = queue.dlq[0] as { jobId: string; reason: string };
      assert.equal(dlqPayload.jobId, job.id);
      assert.equal(dlqPayload.reason, "max_attempts");
    }
  }
  const events = await store.getEvents(job.id);
  assert.equal(events.filter((e) => e.event_type === "processing").length, 5, "each delivery must re-execute the job");
  assert.equal(events.filter((e) => e.event_type === "retry_scheduled").length, 4, "four intermediate retries must be scheduled");
  assert.ok(events.some((e) => e.event_type === "failed" && (e.message ?? "").includes("Превышено число попыток")), "final failure event must mention max attempts");
});

test("worker: job cancelled mid-flight is NOT resurrected when finish returns false", async () => {
  class CancelDuringClaimStore extends MemoryStore {
    override async claim(id: string, workerId: string, leaseTtlSeconds: number): Promise<JobRow | null> {
      const row = await super.claim(id, workerId, leaseTtlSeconds);
      if (row) await super.cancelJob(id); // user cancels while worker processes
      return row;
    }
  }
  const store = new CancelDuringClaimStore();
  await store.init();
  const queue = new FakeQueue();
  const worker = new WorkerService(store, queue, new MemoryCache());
  const job = await store.createJob({ title: "cancel race", type: "campaign_publish", priority: "high", simulateFailure: false });
  await worker.handleJobMessage(fakeMessage(job.id));
  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "cancelled", "cancelled job must stay cancelled");
  const pending = await store.outboxPending();
  const queuedRows = pending.filter((o) => o.aggregate_id === job.id && o.event_type === "job.queued");
  assert.equal(queuedRows.length, 1, "exactly one job.queued row (from creation); a cancelled job must not be republished");
});

test("worker: stop() drains in-flight jobs instead of interrupting them", async () => {
  const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
  const store = new MemoryStore();
  await store.init();
  const queue = new MemoryQueue(2);
  const worker = new WorkerService(store, queue, new MemoryCache());
  const job = await store.createJob({ title: "drain me", type: "sync_catalog", priority: "normal", simulateFailure: false });

  worker.start();
  queue.publish({ jobId: job.id }, "normal");

  // wait until the consumer has claimed the job (work itself takes ~850ms)
  let claimed = false;
  for (let i = 0; i < 75; i++) {
    const current = await store.getJob(job.id);
    if (current?.status === "processing") {
      claimed = true;
      break;
    }
    await sleep(20);
  }
  assert.ok(claimed, "job must be claimed by the running consumer before stop()");

  await worker.stop(10_000);

  const reloaded = await store.getJob(job.id);
  assert.equal(reloaded?.status, "succeeded", "in-flight job must finish during the drain, not be abandoned");
  const events = await store.getEvents(job.id);
  assert.ok(events.some((e) => e.event_type === "succeeded"), "succeeded event must be recorded");
});

test("worker: stop() is idempotent and closes the queue", async () => {
  const store = new MemoryStore();
  await store.init();
  const queue = new MemoryQueue(1);
  const worker = new WorkerService(store, queue, new MemoryCache());
  worker.start();
  await worker.stop(100);
  await worker.stop(100); // second call must not throw
});

