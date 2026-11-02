import { test } from "node:test";
import assert from "node:assert/strict";
import {
  hashPassword,
  hasPermission,
  issueTokens,
  requirePermission,
  revokeRefreshFamily,
  rotateRefreshToken,
  signJwt,
  verifyJwt,
  verifyPassword,
} from "../src/auth.js";
import { MemoryCache } from "../src/cache.js";
import { ForbiddenError, UnauthorizedError } from "../src/errors.js";
import type { Role, SessionUser } from "../src/types.js";

test("password hashing: scrypt with salt, timing-safe verify", async () => {
  const hash = hashPassword("secret-123");
  assert.ok(hash.startsWith("scrypt$"));
  assert.ok(verifyPassword("secret-123", hash));
  assert.ok(!verifyPassword("wrong", hash));
  assert.ok(!verifyPassword("secret-123", "garbage"));
});

test("jwt: sign → verify roundtrip", async () => {
  const token = signJwt({ sub: "user-1", email: "a@b.c", role: "admin" }, 60);
  const user = verifyJwt(token);
  assert.equal(user.id, "user-1");
  assert.equal(user.email, "a@b.c");
  assert.equal(user.role, "admin");
});

test("jwt: expired token rejected", async () => {
  const token = signJwt({ sub: "user-1", email: "a@b.c", role: "operator" }, -1);
  assert.throws(() => verifyJwt(token), UnauthorizedError);
});

test("jwt: tampered signature rejected", async () => {
  const token = signJwt({ sub: "user-1", email: "a@b.c", role: "operator" }, 60);
  const tampered = token.slice(0, -2) + (token.endsWith("aa") ? "bb" : "aa");
  assert.throws(() => verifyJwt(tampered), UnauthorizedError);
});

test("refresh rotation: reuse detection revokes the whole family", async () => {
  const cache = new MemoryCache();
  const user = { id: "user-1", email: "a@b.c", role: "admin" as Role };
  const first = await issueTokens(user, cache);
  const rotated = await rotateRefreshToken(first.refreshToken, cache);
  assert.notEqual(rotated.refreshToken, first.refreshToken);
  // replaying the OLD token is detected as theft
  await assert.rejects(() => rotateRefreshToken(first.refreshToken, cache), UnauthorizedError);
  // and the rest of the family is revoked
  await assert.rejects(() => rotateRefreshToken(rotated.refreshToken, cache), UnauthorizedError);
  // rotation works for a fresh family; logout then revokes it
  const fresh = await issueTokens(user, cache);
  const rotatedOk = await rotateRefreshToken(fresh.refreshToken, cache);
  assert.ok(rotatedOk.accessToken);
  await revokeRefreshFamily(rotatedOk.refreshToken, cache);
  await assert.rejects(() => rotateRefreshToken(rotatedOk.refreshToken, cache), UnauthorizedError);
});

test("RBAC: permission matrix enforced", async () => {
  assert.ok(hasPermission("admin", "metrics.read"));
  assert.ok(hasPermission("admin", "dlq.replay"));
  assert.ok(hasPermission("operator", "jobs.create"));
  assert.ok(hasPermission("operator", "jobs.read"));
  assert.ok(!hasPermission("operator", "metrics.read"));
  assert.ok(!hasPermission("operator", "dlq.replay"));
  assert.ok(!hasPermission("operator", "users.read"));

  const operator: SessionUser = { id: "u1", email: "op@x", role: "operator" };
  const admin: SessionUser = { id: "u2", email: "ad@x", role: "admin" };
  assert.throws(() => requirePermission("metrics.read")(operator), ForbiddenError);
  assert.throws(() => requirePermission("jobs.read")(undefined), UnauthorizedError);
  requirePermission("jobs.read")(operator); // must not throw
  requirePermission("metrics.read")(admin); // must not throw
});
