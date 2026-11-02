import { Router, type NextFunction, type Request, type Response } from "express";
import { z } from "zod";
import { env, isDemo } from "./config.js";
import { BadRequestError, NotFoundError, UnauthorizedError } from "./errors.js";
import {
  issueTokens,
  requirePermission,
  revokeRefreshFamily,
  rotateRefreshToken,
  verifyJwt,
  verifyPassword,
} from "./auth.js";
import { JobService } from "./services/jobs.js";
import type { AuthedRequest, ICache, IQueue, IStore } from "./types.js";

const createJobSchema = z.object({
  title: z.string().trim().min(3).max(120),
  type: z.enum(["campaign_publish", "sync_catalog", "send_digest", "import_orders"]),
  priority: z.enum(["low", "normal", "high"]).default("normal"),
  simulateFailure: z.boolean().default(false),
});

const listJobsSchema = z.object({
  status: z.enum(["queued", "processing", "succeeded", "failed", "cancelled"]).optional(),
  type: z.enum(["campaign_publish", "sync_catalog", "send_digest", "import_orders"]).optional(),
  priority: z.enum(["low", "normal", "high"]).optional(),
  q: z.string().trim().max(120).default(""),
  limit: z.coerce.number().int().min(1).max(100).default(50),
  offset: z.coerce.number().int().min(0).default(0),
});

const loginSchema = z.object({
  email: z.string().email(),
  password: z.string().min(1),
});

const dlqReplaySchema = z.object({
  jobId: z.string().uuid(),
});

function zodIssues(error: z.ZodError): string {
  return error.issues.map((issue) => `${issue.path.join(".") || "body"}: ${issue.message}`).join("; ");
}

function parseCookies(header: string | undefined): Record<string, string> {
  const result: Record<string, string> = {};
  if (!header) return result;
  for (const part of header.split(";")) {
    const idx = part.indexOf("=");
    if (idx === -1) continue;
    result[part.slice(0, idx).trim().toLowerCase()] = decodeURIComponent(part.slice(idx + 1).trim());
  }
  return result;
}

function setRefreshCookie(res: Response, token: string): void {
  res.cookie("refresh_token", token, {
    httpOnly: true,
    secure: env.NODE_ENV === "production",
    sameSite: "lax",
    maxAge: env.REFRESH_TOKEN_TTL_S * 1000,
    path: "/api/auth",
  });
}

function paramId(req: Request): string {
  const p = req.params.id;
  return Array.isArray(p) ? (p[0] ?? "") : (p ?? "");
}

function extractAccessToken(req: Request): string | null {
  const auth = req.headers.authorization;
  if (auth && auth.startsWith("Bearer ")) return auth.slice(7).trim();
  const cookies = parseCookies(req.headers.cookie);
  return cookies["access_token"] ?? null;
}

function requireAuth(req: AuthedRequest, _res: Response, next: NextFunction): void {
  const token = extractAccessToken(req);
  if (!token) {
    next(new UnauthorizedError("Требуется авторизация"));
    return;
  }
  try {
    req.user = verifyJwt(token);
  } catch {
    next(new UnauthorizedError("Токен недействителен или истёк"));
    return;
  }
  next();
}

function requirePerm(permission: string) {
  return (req: AuthedRequest, _res: Response, next: NextFunction): void => {
    try {
      requirePermission(permission)(req.user);
      next();
    } catch (err) {
      next(err);
    }
  };
}

export interface RouteDeps {
  store: IStore;
  cache: ICache;
  queue: IQueue;
  jobService: JobService;
}

export function createRoutes(deps: RouteDeps): Router {
  const router = Router();
  const { store, cache, queue, jobService } = deps;

  const rateLimit = (limit: number, prefix: string, keyFn: (req: Request) => string) => {
    return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
      try {
        const key = `${prefix}:${keyFn(req)}`;
        const count = await cache.incr(key);
        if (count === 1) await cache.expire(key, 60);
        if (count > limit) {
          res.status(429).json({ error: "Слишком много запросов. Повторите позже." });
          return;
        }
      } catch {
        // cache unavailable — fail open (availability over strictness)
      }
      next();
    };
  };

  // ── public ─────────────────────────────────────────────────────────────
  router.get("/api/health", async (_req: Request, res: Response) => {
    const checks = { database: false, cache: false, queue: false, worker: false, outboxPending: 0 };
    try {
      await store.ping();
      checks.database = true;
    } catch {
      /* health only */
    }
    try {
      checks.cache = await cache.ping();
    } catch {
      /* health only */
    }
    checks.queue = queue.healthy();
    try {
      checks.worker = (await cache.get("pulseops:worker:heartbeat")) !== null;
    } catch {
      /* health only */
    }
    try {
      checks.outboxPending = await store.outboxCount();
    } catch {
      /* health only */
    }
    const ok = checks.database && checks.cache && checks.queue;
    res.status(ok ? 200 : 503).json({
      ok,
      mode: isDemo ? "demo" : "docker",
      checks,
      worker: checks.worker,
      timestamp: new Date().toISOString(),
    });
  });

  router.get("/api/stats", async (_req: Request, res: Response, next: NextFunction) => {
    try {
      const cached = await cache.get("pulseops:stats:v1").catch(() => null);
      if (cached) {
        res.json(JSON.parse(cached));
        return;
      }
      const stats = await jobService.stats();
      await cache.set("pulseops:stats:v1", JSON.stringify(stats), 3).catch(() => undefined);
      res.json(stats);
    } catch (err) {
      next(err);
    }
  });

  router.post("/api/auth/login", rateLimit(env.RATE_LIMIT_LOGIN, "pulseops:rate:login", (req) => `${req.ip ?? "unknown"}:${String(req.body?.email ?? "").toLowerCase()}`), async (req: Request, res: Response, next: NextFunction) => {
    try {
      const parsed = loginSchema.safeParse(req.body);
      if (!parsed.success) throw new BadRequestError(zodIssues(parsed.error));
      const email = parsed.data.email.toLowerCase();
      const user = await store.findUserByEmail(email);
      if (!user || !verifyPassword(parsed.data.password, user.password_hash)) {
        throw new UnauthorizedError("Неверная почта или пароль");
      }
      const tokens = await issueTokens(user, cache);
      setRefreshCookie(res, tokens.refreshToken);
      res.json({
        accessToken: tokens.accessToken,
        expiresIn: tokens.expiresIn,
        user: { id: user.id, email: user.email, role: user.role },
      });
    } catch (err) {
      next(err);
    }
  });

  router.post("/api/auth/refresh", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const cookies = parseCookies(req.headers.cookie);
      const token = cookies["refresh_token"] ?? (typeof req.body?.refreshToken === "string" ? req.body.refreshToken : undefined);
      if (!token) throw new UnauthorizedError("Refresh token required");
      const tokens = await rotateRefreshToken(token, cache);
      setRefreshCookie(res, tokens.refreshToken);
      res.json({ accessToken: tokens.accessToken, expiresIn: tokens.expiresIn });
    } catch (err) {
      next(err);
    }
  });

  router.post("/api/auth/logout", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const cookies = parseCookies(req.headers.cookie);
      const token = cookies["refresh_token"] ?? (typeof req.body?.refreshToken === "string" ? req.body.refreshToken : undefined);
      if (token) await revokeRefreshFamily(token, cache).catch(() => undefined);
      res.clearCookie("refresh_token", { path: "/api/auth" });
      res.json({ ok: true });
    } catch (err) {
      next(err);
    }
  });

  // ── authenticated ──────────────────────────────────────────────────────
  router.get("/api/auth/me", requireAuth, async (req: AuthedRequest, res: Response, next: NextFunction) => {
    try {
      const user = req.user ? await store.getUserById(req.user.id) : null;
      if (!user) throw new NotFoundError("Пользователь не найден");
      res.json({ user: { id: user.id, email: user.email, role: user.role } });
    } catch (err) {
      next(err);
    }
  });

  router.get("/api/jobs", requireAuth, requirePerm("jobs.read"), async (req: Request, res: Response, next: NextFunction) => {
    try {
      const query: Record<string, unknown> = { ...req.query };
      for (const key of ["status", "type", "priority", "q"]) {
        if (typeof query[key] === "string" && (query[key] as string).trim() === "") delete query[key];
      }
      const parsed = listJobsSchema.safeParse(query);
      if (!parsed.success) throw new BadRequestError(zodIssues(parsed.error));
      res.json(await jobService.list(parsed.data));
    } catch (err) {
      next(err);
    }
  });

  router.get("/api/jobs/:id", requireAuth, requirePerm("jobs.read"), async (req: Request, res: Response, next: NextFunction) => {
    try {
      res.json(await jobService.get(paramId(req)));
    } catch (err) {
      next(err);
    }
  });

  router.post(
    "/api/jobs",
    requireAuth,
    requirePerm("jobs.create"),
    rateLimit(env.RATE_LIMIT_GLOBAL, "pulseops:rate:ip", (req) => req.ip ?? "unknown"),
    async (req: AuthedRequest, res: Response, next: NextFunction) => {
      try {
        const parsed = createJobSchema.safeParse(req.body);
        if (!parsed.success) throw new BadRequestError(zodIssues(parsed.error));
        const idemKey = typeof req.headers["idempotency-key"] === "string" ? req.headers["idempotency-key"] : undefined;
        const result = await jobService.create(parsed.data, idemKey);
        res.status(result.idempotent ? 200 : 201).json(result);
      } catch (err) {
        next(err);
      }
    },
  );

  router.post("/api/jobs/:id/retry", requireAuth, requirePerm("jobs.retry"), async (req: Request, res: Response, next: NextFunction) => {
    try {
      res.json({ ok: true, item: await jobService.retry(paramId(req)) });
    } catch (err) {
      next(err);
    }
  });

  router.post("/api/jobs/:id/cancel", requireAuth, requirePerm("jobs.cancel"), async (req: Request, res: Response, next: NextFunction) => {
    try {
      res.json({ ok: true, item: await jobService.cancel(paramId(req)) });
    } catch (err) {
      next(err);
    }
  });

  // ── admin ──────────────────────────────────────────────────────────────
  router.get("/api/metrics", requireAuth, requirePerm("metrics.read"), async (_req: Request, res: Response, next: NextFunction) => {
    try {
      res.type("text/plain").send(await jobService.metricsText());
    } catch (err) {
      next(err);
    }
  });

  router.post("/api/dlq/replay", requireAuth, requirePerm("dlq.replay"), async (req: Request, res: Response, next: NextFunction) => {
    try {
      const parsed = dlqReplaySchema.safeParse(req.body);
      if (!parsed.success) throw new BadRequestError(zodIssues(parsed.error));
      const item = await jobService.dlqReplay(parsed.data.jobId);
      res.json({ ok: true, item });
    } catch (err) {
      next(err);
    }
  });

  return router;
}
