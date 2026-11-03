import { test } from "node:test";
import assert from "node:assert/strict";
import { reconnectDelayMs } from "../src/queue.js";

// Regression for H2: the RabbitMQ adapter used to have no connection-level
// reconnect — after a broker restart the consumer retried createChannel() on a
// dead connection forever. Reconnect now backs off exponentially (capped) and
// is shared by publisher and consumers. The curve is pure and unit-testable
// without a live broker (Docker is not available in this environment).

test("reconnect backoff grows exponentially and is capped", () => {
  assert.equal(reconnectDelayMs(0), 1_000);
  assert.equal(reconnectDelayMs(1), 2_000);
  assert.equal(reconnectDelayMs(2), 4_000);
  assert.equal(reconnectDelayMs(3), 8_000);
  assert.equal(reconnectDelayMs(4), 16_000);
  assert.equal(reconnectDelayMs(5), 30_000, "cap at 30s");
  assert.equal(reconnectDelayMs(50), 30_000, "cap holds for large attempt counters");
});

test("reconnect backoff never returns a non-positive delay", () => {
  for (const attempt of [-5, -1, 0, 1, 2, 10, 100]) {
    assert.ok(reconnectDelayMs(attempt) > 0, `attempt ${attempt} must yield a positive delay`);
  }
});
