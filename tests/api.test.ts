import { test } from "node:test";
import assert from "node:assert/strict";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { MemoryStore } from "../src/store/memory.js";
import { MemoryCache } from "../src/cache.js";
import { MemoryQueue } from "../src/queue.js";
import { JobService } from "../src/services/jobs.js";
import { createApp } from "../src/app.js";
import { hashPassword } from "../src/auth.js";

// HTTP-level tests. They exercise the real Express app (middleware order,
// body parsing, error mapping, auth) instead of only unit-level pieces.
// Regression for M1: malformed JSON used to return 500 with requestId
// "unknown" because express.json ran before the requestId middleware.

interface Harness {
  server: Server;
  base: string;
  store: MemoryStore;
}

async function startHarness(): Promise<Harness> {
  const store = new MemoryStore();
  await store.init();
  const cache = new MemoryCache();
  const queue = new MemoryQueue(1);
  const jobService = new JobService(store, cache);
  const app = createApp({ store, cache, queue, jobService });
  const server = app.listen(0);
  await new Promise<void>((resolve) => server.once("listening", () => resolve()));
  const { port } = server.address() as AddressInfo;
  return { server, base: `http://127.0.0.1:${port}`, store };
}

async function stopHarness(harness: Harness): Promise<void> {
  await new Promise<void>((resolve) => harness.server.close(() => resolve()));
}

test("POST /api/jobs with malformed JSON → 400 invalid_json with a real requestId", async () => {
  const harness = await startHarness();
  try {
    const res = await fetch(`${harness.base}/api/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{not json",
    });
    assert.equal(res.status, 400, "malformed body is a client error, not 500");
    const headerRequestId = res.headers.get("x-request-id");
    assert.ok(headerRequestId && headerRequestId.length > 10, "X-Request-Id must be set");

    const body = (await res.json()) as { code?: string; requestId?: string };
    assert.equal(body.code, "invalid_json");
    assert.equal(body.requestId, headerRequestId, "body requestId must match the header (not 'unknown')");
  } finally {
    await stopHarness(harness);
  }
});

test("oversized JSON body → 413 payload_too_large", async () => {
  const harness = await startHarness();
  try {
    const big = JSON.stringify({ title: "x".repeat(40 * 1024) });
    const res = await fetch(`${harness.base}/api/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: big,
    });
    assert.equal(res.status, 413);
    const body = (await res.json()) as { code?: string };
    assert.equal(body.code, "payload_too_large");
  } finally {
    await stopHarness(harness);
  }
});

test("GET /api/jobs without a token → 401 (auth not weakened by middleware reorder)", async () => {
  const harness = await startHarness();
  try {
    const res = await fetch(`${harness.base}/api/jobs`);
    assert.equal(res.status, 401);
    const body = (await res.json()) as { code?: string };
    assert.equal(body.code, "unauthorized");
  } finally {
    await stopHarness(harness);
  }
});

test("login → Bearer list with empty query params → 200 (end-to-end API contract)", async () => {
  const harness = await startHarness();
  try {
    await harness.store.createUser("api-test@demo.local", hashPassword("api-test-password"), "admin");
    const loginRes = await fetch(`${harness.base}/api/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: "api-test@demo.local", password: "api-test-password" }),
    });
    assert.equal(loginRes.status, 200);
    const login = (await loginRes.json()) as { accessToken?: string };
    assert.ok(login.accessToken, "login must return an access token");

    const listRes = await fetch(`${harness.base}/api/jobs?status=&type=&priority=&q=`, {
      headers: { Authorization: `Bearer ${login.accessToken}` },
    });
    assert.equal(listRes.status, 200, "empty query parameters must be ignored, not rejected");
    const list = (await listRes.json()) as { items?: unknown[]; total?: number };
    assert.ok(Array.isArray(list.items));
    assert.equal(typeof list.total, "number");
  } finally {
    await stopHarness(harness);
  }
});
