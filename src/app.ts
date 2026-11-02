import express, { type ErrorRequestHandler, type Express, type NextFunction, type Request, type Response } from "express";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import { env } from "./config.js";
import { AppError } from "./errors.js";
import { errorMessage, logger } from "./logger.js";
import { createRoutes } from "./routes.js";
import { APP_HTML } from "./ui.js";
import type { AuthedRequest, ICache, IQueue, IStore } from "./types.js";
import { JobService } from "./services/jobs.js";

export interface AppDeps {
  store: IStore;
  cache: ICache;
  queue: IQueue;
  jobService: JobService;
}

function corsMiddleware() {
  const raw = env.CORS_ORIGIN.trim();
  const allowAll = raw === "*";
  const allowed = allowAll ? null : new Set(raw.split(",").map((origin) => origin.trim().toLowerCase()).filter(Boolean));
  return (req: Request, res: Response, next: NextFunction): void => {
    const origin = req.headers.origin;
    if (origin) {
      const match = allowAll || (allowed !== null && allowed.has(origin.toLowerCase()));
      if (match) {
        res.setHeader("Access-Control-Allow-Origin", origin);
        res.setHeader("Access-Control-Allow-Credentials", "true");
        res.setHeader("Vary", "Origin");
      }
    } else if (allowAll) {
      res.setHeader("Access-Control-Allow-Origin", "*");
    }
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization,Idempotency-Key,X-Request-Id");
    if (req.method === "OPTIONS") {
      res.status(204).end();
      return;
    }
    next();
  };
}

function requestIdMiddleware(req: AuthedRequest, res: Response, next: NextFunction): void {
  const header = req.headers["x-request-id"];
  const id = typeof header === "string" && header.length > 0 ? header : randomUUID();
  req.requestId = id;
  res.setHeader("X-Request-Id", id);
  next();
}

function loggingMiddleware(req: AuthedRequest, res: Response, next: NextFunction): void {
  const started = Date.now();
  res.on("finish", () => {
    logger.info("http", {
      method: req.method,
      url: req.originalUrl,
      status: res.statusCode,
      ms: Date.now() - started,
      requestId: req.requestId,
      user: req.user?.email ?? null,
    });
  });
  next();
}

const errorMiddleware: ErrorRequestHandler = (err: unknown, req: Request, res: Response, _next: NextFunction): void => {
  const requestId = (req as AuthedRequest).requestId ?? "unknown";
  if (err instanceof AppError) {
    res.status(err.status).json({ error: err.message, code: err.code, requestId });
    return;
  }
  if (err instanceof z.ZodError) {
    res.status(400).json({
      error: err.issues.map((issue) => `${issue.path.join(".") || "body"}: ${issue.message}`).join("; "),
      code: "validation",
      requestId,
    });
    return;
  }
  logger.error("unhandled", { requestId, error: errorMessage(err) });
  if (res.headersSent) return;
  res.status(500).json({ error: "Внутренняя ошибка сервера", code: "internal", requestId });
};

export function createApp(deps: AppDeps): Express {
  const app = express();
  app.disable("x-powered-by");

  app.use((_req: Request, res: Response, next: NextFunction): void => {
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("X-Frame-Options", "DENY");
    res.setHeader("Referrer-Policy", "no-referrer");
    next();
  });
  app.use(corsMiddleware());
  app.use(express.json({ limit: "32kb" }));
  app.use(requestIdMiddleware);
  app.use(loggingMiddleware);
  app.use(createRoutes({ store: deps.store, cache: deps.cache, queue: deps.queue, jobService: deps.jobService }));
  app.get("/", (_req: Request, res: Response) => {
    res.type("html").send(APP_HTML);
  });
  app.use(errorMiddleware);
  return app;
}
