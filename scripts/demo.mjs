import { spawn } from "node:child_process";
import process from "node:process";

// One-command demo without Docker: runs the API with an in-memory store
// and an in-process worker. Open http://localhost:3000
process.env.DEMO_MODE = "1";

const child = spawn("npm", ["run", "dev"], { stdio: "inherit", shell: true, env: process.env });

const shutdown = () => {
  try { child.kill(); } catch { /* ignore */ }
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
child.on("exit", (code) => process.exit(code ?? 0));
