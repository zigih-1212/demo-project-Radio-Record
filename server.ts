import { bootstrap } from "./src/main.js";

void bootstrap().catch((err: unknown) => {
  console.error(JSON.stringify({ level: "error", msg: "fatal startup error", error: err instanceof Error ? err.message : String(err) }));
  process.exit(1);
});
