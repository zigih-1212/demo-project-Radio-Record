import { createHash, createHmac, randomBytes, randomUUID, scryptSync, timingSafeEqual } from "node:crypto";
import { UnauthorizedError, ForbiddenError } from "./errors.js";
import type { ICache } from "./types.js";
import type { Role, SessionUser } from "./types.js";
import { env } from "./config.js";

export function hashPassword(plain: string): string {
  const salt = randomBytes(16).toString("hex");
  const hash = scryptSync(plain, salt, 64).toString("hex");
  return `scrypt$${salt}$${hash}`;
}

export function verifyPassword(plain: string, stored: string): boolean {
  try {
    const parts = stored.split("$");
    if (parts.length !== 3 || parts[0] !== "scrypt") return false;
    const salt = parts[1];
    const expected = Buffer.from(parts[2], "hex");
    const candidate = scryptSync(plain, salt, 64);
    return candidate.length === expected.length && timingSafeEqual(candidate, expected);
  } catch {
    return false;
  }
}

function b64url(input: string | Buffer): string {
  return Buffer.from(input).toString("base64url");
}

function sha256Hex(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

export function signJwt(payload: Record<string, unknown>, ttlSeconds: number): string {
  const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const now = Math.floor(Date.now() / 1000);
  const body = b64url(
    JSON.stringify({ ...payload, iat: now, exp: now + ttlSeconds }),
  );
  const signature = createHmac("sha256", env.JWT_SECRET)
    .update(`${header}.${body}`)
    .digest("base64url");
  return `${header}.${body}.${signature}`;
}

export function verifyJwt(token: string): SessionUser {
  const parts = token.split(".");
  if (parts.length !== 3) throw new UnauthorizedError("Invalid token");
  const [headerB64, bodyB64, sigB64] = parts;
  const expected = createHmac("sha256", env.JWT_SECRET)
    .update(`${headerB64}.${bodyB64}`)
    .digest("base64url");
  const sigBuf = Buffer.from(sigB64 ?? "", "utf8");
  const expBuf = Buffer.from(expected, "utf8");
  if (sigBuf.length !== expBuf.length || !timingSafeEqual(sigBuf, expBuf)) {
    throw new UnauthorizedError("Invalid token");
  }
  let payload: { sub?: unknown; email?: unknown; role?: unknown; exp?: unknown };
  try {
    payload = JSON.parse(Buffer.from(bodyB64, "base64url").toString("utf8"));
  } catch {
    throw new UnauthorizedError("Invalid token");
  }
  if (typeof payload.exp === "number" && payload.exp < Math.floor(Date.now() / 1000)) {
    throw new UnauthorizedError("Token expired");
  }
  if (typeof payload.sub !== "string" || typeof payload.email !== "string" || typeof payload.role !== "string") {
    throw new UnauthorizedError("Invalid token");
  }
  return { id: payload.sub, email: payload.email, role: payload.role as Role };
}

const RT_PREFIX = "pulseops:rt:";
const RF_PREFIX = "pulseops:rf:";
const RU_PREFIX = "pulseops:ru:";

export interface IssuedTokens {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  userId: string;
}

export async function issueTokens(user: { id: string; email: string; role: Role }, cache: ICache): Promise<IssuedTokens> {
  const family = randomUUID();
  const refreshToken = randomUUID();
  const ttl = env.REFRESH_TOKEN_TTL_S;
  await cache.set(`${RT_PREFIX}${sha256Hex(refreshToken)}`, JSON.stringify({ sub: user.id, family }), ttl);
  await cache.set(`${RF_PREFIX}${family}`, "0", ttl);
  const accessToken = signJwt({ sub: user.id, email: user.email, role: user.role, family }, env.ACCESS_TOKEN_TTL_S);
  return { accessToken, refreshToken, expiresIn: env.ACCESS_TOKEN_TTL_S, userId: user.id };
}

export async function rotateRefreshToken(refreshToken: string, cache: ICache): Promise<IssuedTokens> {
  const hash = sha256Hex(refreshToken);
  const raw = await cache.get(`${RT_PREFIX}${hash}`);
  const session = raw ? (JSON.parse(raw) as { sub: string; family: string }) : null;
  if (session) {
    if ((await cache.get(`${RF_PREFIX}${session.family}`)) === "1") {
      throw new UnauthorizedError("Refresh token family revoked");
    }
  } else {
    const used = await cache.get(`${RU_PREFIX}${hash}`);
    if (used) {
      await cache.set(`${RF_PREFIX}${used}`, "1", env.REFRESH_TOKEN_TTL_S);
      throw new UnauthorizedError("Refresh token reuse detected — family revoked");
    }
    throw new UnauthorizedError("Invalid refresh token");
  }
  await cache.del(`${RT_PREFIX}${hash}`);
  await cache.set(`${RU_PREFIX}${hash}`, session.family, env.REFRESH_TOKEN_TTL_S);
  const newRefresh = randomUUID();
  await cache.set(`${RT_PREFIX}${sha256Hex(newRefresh)}`, JSON.stringify({ sub: session.sub, family: session.family }), env.REFRESH_TOKEN_TTL_S);
  const accessToken = signJwt({ sub: session.sub, family: session.family }, env.ACCESS_TOKEN_TTL_S);
  return { accessToken, refreshToken: newRefresh, expiresIn: env.ACCESS_TOKEN_TTL_S, userId: session.sub };
}

export async function revokeRefreshFamily(refreshToken: string, cache: ICache): Promise<void> {
  const raw = await cache.get(`${RT_PREFIX}${sha256Hex(refreshToken)}`);
  if (!raw) return;
  const session = JSON.parse(raw) as { family: string };
  await cache.set(`${RF_PREFIX}${session.family}`, "1", env.REFRESH_TOKEN_TTL_S);
  await cache.del(`${RT_PREFIX}${sha256Hex(refreshToken)}`);
}

const PERMISSIONS: Record<Role, string[]> = {
  admin: ["jobs.read", "jobs.create", "jobs.retry", "jobs.cancel", "metrics.read", "dlq.replay", "users.read"],
  operator: ["jobs.read", "jobs.create", "jobs.retry", "jobs.cancel"],
};

export function hasPermission(role: Role, permission: string): boolean {
  return PERMISSIONS[role]?.includes(permission) ?? false;
}

export function requirePermission(permission: string) {
  return (user: SessionUser | undefined): void => {
    if (!user) throw new UnauthorizedError("Authentication required");
    if (!hasPermission(user.role, permission)) throw new ForbiddenError("Insufficient permissions");
  };
}

export { randomUUID };
