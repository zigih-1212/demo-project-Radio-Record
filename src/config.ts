import { z } from "zod";
import process from "node:process";

const envSchema = z.object({
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  DATABASE_URL: z.string().min(1).default("postgres://pulseops:pulseops@localhost:5432/pulseops"),
  REDIS_URL: z.string().min(1).default("redis://localhost:6379"),
  AMQP_URL: z.string().min(1).default("amqp://pulseops:pulseops@localhost:5672"),
  NODE_ENV: z.string().default("development"),
  CORS_ORIGIN: z.string().default("*"),
  WORKER_CONCURRENCY: z.coerce.number().int().min(1).max(50).default(5),
  DEMO_MODE: z.string().default("0"),
  JWT_SECRET: z.string().min(16).default("demo-secret-change-me-in-production!!"),
  ACCESS_TOKEN_TTL_S: z.coerce.number().int().min(60).default(900),
  REFRESH_TOKEN_TTL_S: z.coerce.number().int().min(300).default(604800),
  JOB_LEASE_TTL_S: z.coerce.number().int().min(5).default(120),
  REAPER_INTERVAL_S: z.coerce.number().int().min(5).default(15),
  OUTBOX_BATCH: z.coerce.number().int().min(1).max(100).default(25),
  RATE_LIMIT_GLOBAL: z.coerce.number().int().min(1).default(120),
  RATE_LIMIT_LOGIN: z.coerce.number().int().min(1).default(5),
  ADMIN_EMAIL: z.string().email().default("admin@demo.local"),
  ADMIN_PASSWORD: z.string().min(8).default("admin-demo-123"),
});

export const env = envSchema.parse(process.env);
export const isDemo = env.DEMO_MODE === "1";
export const QUEUE = "pulseops.jobs.v1";
export const DLQ = "pulseops.jobs.dlq";

export function warnIfInsecureDefaults(): void {
  if (env.JWT_SECRET === "demo-secret-change-me-in-production!!") {
    console.warn(
      JSON.stringify({
        level: "warn",
        msg: "JWT_SECRET is the demo default — set a strong secret via JWT_SECRET env var in production",
      }),
    );
  }
}
