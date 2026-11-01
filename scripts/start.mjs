import { spawn, execSync } from "node:child_process";
import { existsSync } from "node:fs";
import process from "node:process";

const cwd = process.cwd();

function execOk(cmd, timeoutMs = 15000) {
  try {
    execSync(cmd, { cwd, stdio: "pipe", timeout: timeoutMs });
    return true;
  } catch {
    return false;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runChild(cmd, args, env = {}) {
  return spawn(cmd, args, { cwd, stdio: "inherit", shell: true, env: { ...process.env, ...env } });
}

async function waitForExit(child) {
  return new Promise((resolve) => child.on("exit", resolve));
}

async function installIfNeeded() {
  if (!existsSync("node_modules")) {
    console.log("📦 Устанавливаю npm-зависимости...");
    const code = await waitForExit(runChild("npm", ["install"]));
    if (code !== 0) {
      console.error("❌ npm install завершился с ошибкой");
      process.exit(1);
    }
  }
}

async function main() {
  const dockerOk = execOk("docker info");

  if (!dockerOk) {
    // ── Demo mode: no Docker, everything in-memory ──
    console.log("🐳 Docker недоступен — запускаю ДЕМО-РЕЖИМ (в памяти, без Docker).");
    await installIfNeeded();
    console.log("🚀 Запускаю PulseOps в демо-режиме...");
    console.log("   Сайт будет доступен на http://localhost:3000\n");
    const api = runChild("npm", ["run", "dev"], { DEMO_MODE: "1" });
    let shuttingDown = false;
    const shutdown = () => {
      if (shuttingDown) return;
      shuttingDown = true;
      console.log("\n🛑 Останавливаю...");
      try { api.kill(); } catch { /* ignore */ }
      process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
    api.on("exit", (code) => {
      if (!shuttingDown) {
        console.error(`API завершился (код ${code})`);
        shutdown();
      }
    });
    return;
  }

  // ── Docker mode ──
  console.log("🐳 Запускаю зависимости (Postgres, Redis, RabbitMQ)...");
  if (!execOk("docker compose up -d postgres redis rabbitmq")) {
    console.error("❌ Не удалось поднять контейнеры. Проверьте Docker Desktop и docker-compose.yml.");
    process.exit(1);
  }

  const deadline = Date.now() + 120_000;
  let ready = false;
  while (Date.now() < deadline) {
    const pg = execOk("docker compose exec -T postgres pg_isready -U pulseops -d pulseops");
    const rd = execOk("docker compose exec -T redis redis-cli ping");
    const rm = execOk("docker compose exec -T rabbitmq rabbitmq-diagnostics -q ping");
    if (pg && rd && rm) {
      ready = true;
      console.log("✅ Зависимости готовы");
      break;
    }
    await sleep(2000);
  }
  if (!ready) {
    console.error("❌ Зависимости не поднялись за 2 минуты. Проверьте Docker Desktop.");
    process.exit(1);
  }

  await installIfNeeded();

  console.log("🚀 Запускаю API и воркер...");
  console.log("   Сайт будет доступен на http://localhost:3000\n");

  const api = runChild("npm", ["run", "dev"]);
  const worker = runChild("npm", ["run", "dev:worker"]);

  let shuttingDown = false;
  const shutdown = () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log("\n🛑 Останавливаю...");
    try { api.kill(); } catch { /* ignore */ }
    try { worker.kill(); } catch { /* ignore */ }
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  api.on("exit", (code) => {
    if (!shuttingDown) {
      console.error(`API завершился (код ${code})`);
      shutdown();
    }
  });
  worker.on("exit", (code) => {
    if (!shuttingDown) {
      console.error(`Воркер завершился (код ${code})`);
      shutdown();
    }
  });
}

main().catch((err) => {
  console.error("❌", err.message || err);
  process.exit(1);
});
