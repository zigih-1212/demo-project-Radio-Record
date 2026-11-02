import process from "node:process";

type Level = "debug" | "info" | "warn" | "error";

const levelOrder: Record<Level, number> = { debug: 0, info: 1, warn: 2, error: 3 };
const current = levelOrder[(process.env.LOG_LEVEL as Level) ?? "info"] ?? levelOrder.info;

function emit(level: Level, msg: string, fields?: Record<string, unknown>): void {
  if (levelOrder[level] < current) return;
  const line = JSON.stringify({ level, msg, ts: new Date().toISOString(), ...fields });
  if (level === "error") console.error(line);
  else console.log(line);
}

export const logger = {
  debug: (msg: string, fields?: Record<string, unknown>) => emit("debug", msg, fields),
  info: (msg: string, fields?: Record<string, unknown>) => emit("info", msg, fields),
  warn: (msg: string, fields?: Record<string, unknown>) => emit("warn", msg, fields),
  error: (msg: string, fields?: Record<string, unknown>) => emit("error", msg, fields),
};

export function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    const code = (error as { code?: unknown }).code;
    return typeof code === "string" && code ? code : error.message;
  }
  return String(error);
}
