<div align="center">

# ⚡ PulseOps

### Asynchronous job processing demo — transactional outbox + worker + RabbitMQ

[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178c6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-20-339933?logo=node.js&logoColor=white)](https://nodejs.org/)
[![Express](https://img.shields.io/badge/Express-5.x-888888?logo=express)](https://expressjs.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169e1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-dc382d?logo=redis&logoColor=white)](https://redis.io/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3-ff6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ed?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

**English** · [Русский](#-русский-раздел)

</div>

> Demo project prepared for the **Radio Record — fullstack web developer** vacancy.
> Shows a production-grade async job pipeline in a single repository, runnable in 30 seconds.

---

## 📖 Contents

1. [What it is](#-what-it-is)
2. [Features](#-features)
3. [Architecture](#-architecture)
4. [Quick start](#-quick-start)
5. [API reference](#-api-reference)
6. [Configuration](#%EF%B8%8F-configuration)
7. [Project structure](#-project-structure)
8. [Why this project matches the vacancy](#-why-this-project-matches-the-vacancy)
9. [Troubleshooting](#-troubleshooting)
10. [Roadmap](#-roadmap)
11. [🪪 Русский раздел](#-русский-раздел)

---

## 🎯 What it is

**PulseOps** is a small but realistic service that publishes background jobs through the
**transactional outbox pattern**:

1. Client creates a job via REST API.
2. API writes the job **and** the outbox event in **one PostgreSQL transaction** — no lost messages.
3. A dispatcher reads pending outbox rows and publishes them to **RabbitMQ** (priority queue, persistent messages).
4. A **worker** consumes messages, claims the job idempotently, does the work, updates the status and writes an event log.
5. **Redis** caches stats, holds the worker heartbeat and implements rate limiting.

Everything is observable: health checks, aggregated stats, Prometheus-style metrics, and a per-job event timeline.

---

## ✨ Features

- **Transactional outbox** — atomic job + event persistence, reliable publishing to RabbitMQ
- **Background worker** — at-least-once delivery, idempotent claim (`status='queued' → processing`), retry limits, **Dead-Letter Queue**
- **Priority queue** — `x-max-priority`, high/normal/low job priorities
- **REST API** — create / list / detail / retry / cancel jobs with pagination, filters and search
- **Idempotency** — `Idempotency-Key` header on job creation
- **Redis** — stats cache with TTL, worker heartbeat, rate limiter
- **Observability** — `/api/health` (DB/Redis/RabbitMQ/worker/outbox), `/api/stats`, `/api/metrics`
- **Production touches** — Zod env validation, request IDs, structured JSON logs, CORS + security headers, graceful shutdown
- **Inline dashboard** — single-page UI served from the API (no separate build step)
- **Demo mode** — run the whole system **in-memory, without Docker** (`npm run demo`)

---

## 🏗 Architecture

```
                 ┌─────────────┐
                 │   Browser   │  inline dashboard (served by API)
                 └──────┬──────┘
                        │ HTTP
                        ▼
                 ┌─────────────┐   ┌────────────────┐
                 │  Express API│──▶│   PostgreSQL   │ jobs, job_events, outbox
                 └──────┬──────┘   └───────┬────────┘
                        │                  │
          outbox dispatch│ (1s poll, SKIP LOCKED)
                        ▼                  │
                 ┌─────────────┐           │
                 │  RabbitMQ   │◀──────────┘ priority queue + DLQ
                 └──────┬──────┘
                        │ consume (prefetch)
                        ▼
                 ┌─────────────┐
                 │   Worker    │──▶ PostgreSQL (status + events)
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │    Redis    │ stats cache, heartbeat, rate limit
                 └─────────────┘
```

**Demo mode** (`DEMO_MODE=1`) runs the same interfaces with in-memory implementations
(MemoryStore / MemoryCache / MemoryQueue) in a **single process** — the outbox loop and the
worker still run, so the pattern stays visible without any infrastructure.

---

## 🚀 Quick start

### Option A — One command (recommended)

```bash
npm install
npm run start:all
```

or double-click **`start.bat`** (Windows) / run **`./start.sh`** (macOS/Linux).

The script auto-detects Docker:
- **Docker Desktop running** → starts Postgres + Redis + RabbitMQ, waits for readiness, launches API + worker
- **No Docker** → falls back to **demo mode** (in-memory, single process)

Then open **http://localhost:3000**

### Option B — Demo mode without Docker (fastest)

```bash
npm install
npm run demo
```

Open **http://localhost:3000** — create a job, watch it become `processing` → `succeeded` in ~1 second.

### Option C — Full stack in Docker

```bash
cp .env.example .env   # secrets (JWT_SECRET, admin creds) come from local .env — see .env.example
docker compose up --build
```

API on http://localhost:3000, RabbitMQ management on http://localhost:15672 (`pulseops`/`pulseops`).

### Option D — Manual (local PostgreSQL/Redis/RabbitMQ)

```bash
docker compose up -d postgres redis rabbitmq
npm install
npm run dev         # API        -> http://localhost:3000
npm run dev:worker  # worker     -> second terminal
```

---

## 📡 API reference

| Method | Path | Description | Auth |
|---|---|---|---|
| `GET` | `/` | Inline dashboard | public |
| `GET` | `/api/health` | Health: DB, Redis, RabbitMQ, worker heartbeat, outbox pending | public |
| `GET` | `/api/stats` | Aggregated counters by status (Redis-cached, 3 s TTL) | public |
| `POST` | `/api/auth/login` | Exchange email/password for a Bearer access token + HttpOnly refresh cookie | public |
| `POST` | `/api/auth/refresh` | Rotate the refresh cookie into a new token pair (reuse detection revokes the family) | cookie |
| `POST` | `/api/auth/logout` | Revoke the active refresh token and clear cookies | cookie |
| `GET` | `/api/auth/me` | Current user (email + role) from the access token | operator+ |
| `GET` | `/api/metrics` | Prometheus-style text metrics | admin |
| `GET` | `/api/jobs` | List: `status`, `type`, `priority`, `q`, `limit`, `offset` | operator+ |
| `GET` | `/api/jobs/:id` | Job detail + event timeline | operator+ |
| `POST` | `/api/jobs` | Create job (`Idempotency-Key` supported) | operator+ |
| `POST` | `/api/jobs/:id/retry` | Requeue a failed job (respects `max_attempts`) | operator+ |
| `POST` | `/api/jobs/:id/cancel` | Cancel a queued/processing job | operator+ |
| `POST` | `/api/dlq/replay` | Replay the whole DLQ back to the main queue (DB is the source of truth) | admin |

### Create a job

Every protected endpoint requires a Bearer access token. Get one via `POST /api/auth/login`
(demo admin: `admin@demo.local` / `admin-demo-123`):

```bash
# 1. Login → extract the access token
TOKEN=$(curl -s -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@demo.local","password":"admin-demo-123"}' \
  | python -c "import sys,json;print(json.load(sys.stdin)['accessToken'])")

# 2. Create a job using the token
curl -X POST http://localhost:3000/api/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: campaign-2026-11-01" \
  -d '{
    "title": "Публикация подборки выходного дня",
    "type": "campaign_publish",
    "priority": "high",
    "simulateFailure": false
  }'
```

```json
{
  "item": {
    "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "title": "Публикация подборки выходного дня",
    "job_type": "campaign_publish",
    "priority": "high",
    "status": "queued",
    "attempts": 0,
    "max_attempts": 5
  }
}
```

### Job lifecycle

```
queued ──▶ processing ──▶ succeeded
   │           │
   │           └──▶ failed ──(retry w/ exponential backoff + jitter, attempts < max)──▶ queued
   │                     │
   │                     └──(attempts ≥ max)──▶ failed (terminal) + published to DLQ
   └──(cancel)──▶ cancelled
```

---

## ⚙️ Configuration

All variables are validated with Zod at startup (defaults shown):

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | API port |
| `DATABASE_URL` | `postgres://pulseops:pulseops@localhost:5432/pulseops` | PostgreSQL DSN |
| `REDIS_URL` | `redis://localhost:6379` | Redis DSN |
| `AMQP_URL` | `amqp://pulseops:pulseops@localhost:5672` | RabbitMQ DSN |
| `NODE_ENV` | `development` | Environment |
| `CORS_ORIGIN` | `*` | Allowed origin |
| `WORKER_CONCURRENCY` | `5` | Worker prefetch / parallel jobs |
| `DEMO_MODE` | `0` | `1` = in-memory single-process demo |
| `JWT_SECRET` | `demo-secret-change-me-in-production!!` | HS256 signing secret (**set your own in production**) |
| `ACCESS_TOKEN_TTL_S` | `900` | Access token TTL, seconds (15 min) |
| `REFRESH_TOKEN_TTL_S` | `604800` | Refresh token / cookie TTL, seconds (7 d) |
| `JOB_LEASE_TTL_S` | `120` | Lease length, seconds before the reaper may claim a `processing` job |
| `REAPER_INTERVAL_S` | `15` | How often the worker scans for expired leases |
| `OUTBOX_BATCH` | `25` | Outbox rows dispatched per batch |
| `RATE_LIMIT_GLOBAL` | `120` | Global requests per minute per IP |
| `RATE_LIMIT_LOGIN` | `5` | Login attempts per minute per IP |
| `ADMIN_EMAIL` | `admin@demo.local` | Seeded admin email |
| `ADMIN_PASSWORD` | `admin-demo-123` | Seeded admin password |

---

## 📂 Project structure

```
pulseops-demo/
├── server.ts            # thin entry — bootstraps src/main.ts (keeps `tsx server.ts api|worker`)
├── src/
│   ├── config.ts        # Zod-validated env (JWT, lease, rate-limit settings)
│   ├── logger.ts        # structured JSON logs
│   ├── errors.ts        # AppError hierarchy + single error middleware input
│   ├── types.ts         # domain types + IStore/ICache/IQueue interfaces
│   ├── auth.ts          # scrypt hashing, HS256 JWT, refresh rotation + reuse detection, RBAC
│   ├── app.ts           # express factory: CORS, headers, requestId, logging, rate limit
│   ├── main.ts          # bootstrap (demo/docker), graceful shutdown with drain
│   ├── ui.ts            # inline dashboard (login, stats, filters, drawer, toasts)
│   ├── routes.ts        # auth + jobs + admin routes, real permission middleware
│   ├── store/           # PostgresStore (leases, outbox, transactions) + MemoryStore
│   ├── cache.ts         # RedisCache + MemoryCache
│   ├── queue.ts         # RabbitQueue (reconnect, DLQ) + MemoryQueue
│   └── services/        # outbox dispatcher, worker (leases/backoff/reaper), jobs service
├── tests/               # 43 node:test cases (7 files) — no new dependencies
├── scripts/             # start.mjs (Docker if available, else demo), demo.mjs
├── start.bat / start.sh # one-click launchers
├── docker-compose.yml, Dockerfile, tsconfig.json, .env.example, package.json
└── README.md
```

### 🔒 Auth, RBAC and reliability (v2 hardening)

- **Auth:** JWT access tokens (15 min) + rotating refresh tokens in HttpOnly `Secure` `SameSite=Lax` cookies (7 d); refresh reuse detection revokes the whole family; scrypt password hashing. Demo admin: `admin@demo.local` / `admin-demo-123` (override via `ADMIN_EMAIL` / `ADMIN_PASSWORD`).
- **RBAC (real server-side checks):** public — `GET /api/health`, `GET /api/stats`, `POST /api/auth/*`; operator+ — jobs read/create/retry/cancel; admin only — `GET /api/metrics`, `POST /api/dlq/replay`.
- **Worker:** lease-based claiming (`lease_expires_at`, `locked_by`) + reaper recovering stuck `processing` jobs after a crash; at-least-once with a side-effect-once guard; automatic retry with exponential backoff **+ jitter** to `max_attempts`, then DLQ; safe DLQ replay via admin endpoint (DB is the source of truth); outbox rows published/marked individually with per-row backoff.
- **Tests:** `npm test` — 43 node:test cases (7 files) — no new dependencies: leases/reaper, crash recovery, redelivery idempotency, DLQ at max attempts, malformed→DLQ, store-down requeue (no loss), outbox backoff, retry/cancel guards, create idempotency, JWT roundtrip/expiry/tamper, refresh rotation + reuse detection, RBAC matrix, HTTP-level API contract (malformed/oversized JSON → 400/413), worker stop drain, Rabbit reconnect backoff.

### 🔒 Авторизация, RBAC и надёжность (v2)

- **Auth:** JWT access (15 мин) + ротируемые refresh в HttpOnly `Secure` `SameSite=Lax` куке (7 д); повторное использование refresh отзывает всю семью; пароли — scrypt. Демо-админ: `admin@demo.local` / `admin-demo-123`.
- **RBAC (реальные серверные проверки):** публично — health/stats/auth; operator+ — чтение/создание/retry/cancel задач; admin — metrics и DLQ replay.
- **Воркер:** лизы (`lease_expires_at`, `locked_by`) + reaper восстановления «зависших» задач; at-least-once с защитой от повторных побочных эффектов; retry с exponential backoff **+ jitter** до `max_attempts`, затем DLQ; безопасный replay из DLQ через admin-эндпоинт; outbox-публикация покомандная с backoff.
- **Тесты:** `npm test` — 43 теста node:test (7 файлов, без новых зависимостей): лизы/reaper, восстановление после падения, идемпотентность повторной доставки, DLQ при исчерпании попыток, malformed→DLQ, requeue при недоступном сторе, отложенная публикация с backoff, защита retry/cancel, идемпотентность создания, JWT (круг/expiry/tamper), ротация refresh + детект повторного использования, RBAC-матрица, HTTP-контракт API (JSON → 400/413), дренаж задач при остановке воркера, backoff переподключения Rabbit.

The adapters (`PostgresStore`/`MemoryStore`, `RedisCache`/`MemoryCache`,
`RabbitQueue`/`MemoryQueue`) implement shared interfaces (`IStore`, `ICache`, `IQueue`),
so the business logic is completely infrastructure-agnostic.

---

## 🎯 Why this project matches the vacancy

| Vacancy requirement | Where to see it |
|---|---|
| React.js | *Honest note:* the dashboard is **inline vanilla TypeScript** (`src/ui.ts`), not React — no build step, served by the API itself. The layered routes → services → adapters design maps directly onto React/Angular feature modules. |
| Node.js + TypeScript | Strict TS, typed interfaces, Zod validation |
| NestJS-style architecture | Layered design: routes → services → store adapters, DI-friendly structure |
| PostgreSQL | Schema, transactions, indexes, `FOR UPDATE SKIP LOCKED`, event log |
| Redis | Stats cache, worker heartbeat, rate limiting |
| RabbitMQ | Priority durable queue, confirm channel, DLQ, at-least-once |
| Docker | `docker-compose.yml` (5 services) + `Dockerfile` |
| Microservices | API and worker are separate processes/roles with independent scaling |
| Legacy PHP/Bitrix (bonus) | README section below explains the migration approach |

**About PHP/Bitrix legacy:** the demo focuses on the modern stack, but the migration story is
straightforward — anti-corruption layer, parallel run, gradual cutover with rollback:
new NestJS services read/write their own PostgreSQL while syncing with Bitrix via events;
Bitrix modules become thin adapters; traffic is switched behind a router; old code is removed
module by module.

---

## 🛠 Troubleshooting

- **Port 3000 busy** — change `PORT` in `.env` or stop the conflicting process.
- **Docker not installed** — use `npm run demo`, it needs nothing but Node.js.
- **`ECONNREFUSED` on startup** — dependencies are down; run `docker compose up -d postgres redis rabbitmq` first.
- **Worker processes nothing** — check `/api/health` → `worker` field and RabbitMQ management UI (port 15672).
- **Jobs stuck in `processing`** — restart the worker; a stale heartbeat is visible in `/api/health`.

---

## 🗺 Roadmap (what I would add next)

- [x] JWT auth + role-based access to the API *(implemented — see Auth/RBAC section)*
- [ ] Scheduled jobs (cron-like) with pg_cron / bull-board
- [x] Retries with exponential backoff + jitter per job type *(implemented)*
- [ ] Docker Swarm / Compose deploy profiles for staging & prod
- [x] Unit tests (node:test) + HTTP-level API integration tests *(implemented — `npm test`)*
- [ ] Grafana dashboard for the Prometheus metrics

---

## 📄 License

MIT

---

# 🪪 Русский раздел

<div align="center">

**PulseOps** — демо асинхронной обработки фоновых задач на базе **transactional outbox**.
Сделано для вакансии **Radio Record — fullstack web-разработчик**.

</div>

## 🎯 Что это

**PulseOps** — компактный, но реалистичный сервис, который публикует фоновые задачи
через **паттерн транзакционного аутбокса**:

1. Клиент создаёт задачу через REST API.
2. API записывает задачу **и** событие аутбокса **в одной транзакции PostgreSQL** — сообщения не теряются.
3. Диспетчер читает отложенные события и публикует их в **RabbitMQ** (очередь с приоритетами, персистентные сообщения).
4. **Воркер** потребляет сообщения, идемпотентно захватывает задачу, выполняет работу, обновляет статус и пишет лог событий.
5. **Redis** кэширует статистику, хранит heartbeat воркера и реализует rate limit.

Всё наблюдаемо: health-check, агрегированная статистика, метрики в стиле Prometheus и таймлайн событий каждой задачи.

## ✨ Возможности

- **Transactional outbox** — атомарное сохранение задачи и события, надёжная публикация в RabbitMQ
- **Воркер** — at-least-once, идемпотентный захват, лимит попыток, **Dead-Letter Queue**
- **Приоритетная очередь** — `x-max-priority`, high/normal/low
- **REST API** — создание / список / детали / повтор / отмена задач, пагинация, фильтры, поиск
- **Идемпотентность** — заголовок `Idempotency-Key` при создании
- **Redis** — кэш статистики, heartbeat воркера, ограничение частоты запросов
- **Наблюдаемость** — `/api/health`, `/api/stats`, `/api/metrics`
- **Продакшн-детали** — Zod-валидация env, request ID, структурированные JSON-логи, CORS и security-заголовки, graceful shutdown
- **Встроенная панель** — SPA, отдаваемая самим API (без отдельной сборки)
- **Демо-режим** — вся система **в памяти, без Docker** (`npm run demo`)

## 🏗 Архитектура

```
                 ┌─────────────┐
                 │   Браузер   │  встроенная панель (отдаёт API)
                 └──────┬──────┘
                        │ HTTP
                        ▼
                 ┌─────────────┐   ┌────────────────┐
                 │ Express API │──▶│   PostgreSQL   │ jobs, job_events, outbox
                 └──────┬──────┘   └───────┬────────┘
                        │                  │
          dispatch outbox│ (опрос 1с, SKIP LOCKED)
                        ▼                  │
                 ┌─────────────┐           │
                 │  RabbitMQ   │◀──────────┘ очередь с приоритетами + DLQ
                 └──────┬──────┘
                        │ consume (prefetch)
                        ▼
                 ┌─────────────┐
                 │   Воркер    │──▶ PostgreSQL (статус + события)
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │    Redis    │ кэш статистики, heartbeat, rate limit
                 └─────────────┘
```

**Демо-режим** (`DEMO_MODE=1`) использует те же интерфейсы с in-memory-реализациями
(MemoryStore / MemoryCache / MemoryQueue) в **одном процессе** — цикл аутбокса и воркер
продолжают работать, так что паттерн остаётся наглядным без инфраструктуры.

## 🚀 Быстрый старт

### Вариант A — Одна команда (рекомендуется)

```bash
npm install
npm run start:all
```

или дважды кликните **`start.bat`** (Windows) / запустите **`./start.sh`** (macOS/Linux).

Скрипт сам определяет Docker:
- **Docker Desktop запущен** → поднимает Postgres + Redis + RabbitMQ, ждёт готовности, запускает API + воркер
- **Docker нет** → переключается в **демо-режим** (в памяти, один процесс)

Открыть **http://localhost:3000**

### Вариант B — Демо-режим без Docker (самый быстрый)

```bash
npm install
npm run demo
```

Открыть **http://localhost:3000** — создайте задачу и наблюдайте `processing` → `succeeded` за ~1 секунду.

### Вариант C — Полный стек в Docker

```bash
cp .env.example .env   # секреты (JWT_SECRET, данные админа) берутся из локального .env — см. .env.example
docker compose up --build
```

API на http://localhost:3000, панель RabbitMQ на http://localhost:15672 (`pulseops`/`pulseops`).

### Вариант D — Вручную (локальные PostgreSQL/Redis/RabbitMQ)

```bash
docker compose up -d postgres redis rabbitmq
npm install
npm run dev         # API        -> http://localhost:3000
npm run dev:worker  # воркер     -> второй терминал
```

## 📡 API

| Метод | Путь | Описание | Доступ |
|---|---|---|---|
| `GET` | `/` | Встроенная панель | public |
| `GET` | `/api/health` | Здоровье: БД, Redis, RabbitMQ, heartbeat воркера, отложенные события | public |
| `GET` | `/api/stats` | Счётчики по статусам (кэш в Redis, TTL 3 с) | public |
| `POST` | `/api/auth/login` | Вход: Bearer access token + HttpOnly refresh cookie | public |
| `POST` | `/api/auth/refresh` | Ротация refresh-куки (детект повторного использования отзывает семью) | cookie |
| `POST` | `/api/auth/logout` | Отзыв refresh-токена и очистка кук | cookie |
| `GET` | `/api/auth/me` | Текущий пользователь (email + роль) из access-токена | operator+ |
| `GET` | `/api/metrics` | Метрики в формате Prometheus | admin |
| `GET` | `/api/jobs` | Список: `status`, `type`, `priority`, `q`, `limit`, `offset` | operator+ |
| `GET` | `/api/jobs/:id` | Детали задачи + таймлайн событий | operator+ |
| `POST` | `/api/jobs` | Создать задачу (поддерживается `Idempotency-Key`) | operator+ |
| `POST` | `/api/jobs/:id/retry` | Вернуть failing-задачу в очередь (с учётом `max_attempts`) | operator+ |
| `POST` | `/api/jobs/:id/cancel` | Отменить queued/processing задачу | operator+ |
| `POST` | `/api/dlq/replay` | Replay всей DLQ в основную очередь (источник истины — БД) | admin |

### Создание задачи

Защищённые эндпоинты требуют Bearer access token. Получите его через `POST /api/auth/login`
(демо-админ: `admin@demo.local` / `admin-demo-123`):

```bash
# 1. Вход → извлекаем access token
TOKEN=$(curl -s -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@demo.local","password":"admin-demo-123"}' \
  | python -c "import sys,json;print(json.load(sys.stdin)['accessToken'])")

# 2. Создаём задачу с токеном
curl -X POST http://localhost:3000/api/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: campaign-2026-11-01" \
  -d '{
    "title": "Публикация подборки выходного дня",
    "type": "campaign_publish",
    "priority": "high",
    "simulateFailure": false
  }'
```

```json
{
  "item": {
    "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "title": "Публикация подборки выходного дня",
    "job_type": "campaign_publish",
    "priority": "high",
    "status": "queued",
    "attempts": 0,
    "max_attempts": 5
  }
}
```

### Жизненный цикл задачи

```
queued ──▶ processing ──▶ succeeded
   │           │
   │           └──▶ failed ──(retry c exponential backoff + jitter, attempts < max)──▶ queued
   │                     │
   │                     └──(attempts ≥ max)──▶ failed (терминально) + публикация в DLQ
   └──(cancel)──▶ cancelled
```

## ⚙️ Конфигурация

Все переменные валидируются через Zod при старте (значения по умолчанию):

| Переменная | По умолчанию | Описание |
|---|---|---|
| `PORT` | `3000` | Порт API |
| `DATABASE_URL` | `postgres://pulseops:pulseops@localhost:5432/pulseops` | DSN PostgreSQL |
| `REDIS_URL` | `redis://localhost:6379` | DSN Redis |
| `AMQP_URL` | `amqp://pulseops:pulseops@localhost:5672` | DSN RabbitMQ |
| `NODE_ENV` | `development` | Окружение |
| `CORS_ORIGIN` | `*` | Разрешённый origin |
| `WORKER_CONCURRENCY` | `5` | Prefetch воркера / параллельные задачи |
| `DEMO_MODE` | `0` | `1` = демо в памяти в одном процессе |
| `JWT_SECRET` | `demo-secret-change-me-in-production!!` | Секрет подписи HS256 (**установите свой в проде**) |
| `ACCESS_TOKEN_TTL_S` | `900` | TTL access-токена, секунды (15 мин) |
| `REFRESH_TOKEN_TTL_S` | `604800` | TTL refresh-токена / куки, секунды (7 д) |
| `JOB_LEASE_TTL_S` | `120` | Длина лиза, секунды, до захвата reaper'ом «зависшей» задачи |
| `REAPER_INTERVAL_S` | `15` | Как часто воркер сканирует истёкшие лизы |
| `OUTBOX_BATCH` | `25` | Отложенных событий на батч |
| `RATE_LIMIT_GLOBAL` | `120` | Запросов в минуту на IP |
| `RATE_LIMIT_LOGIN` | `5` | Попыток входа в минуту на IP |
| `ADMIN_EMAIL` | `admin@demo.local` | Email сидируемого админа |
| `ADMIN_PASSWORD` | `admin-demo-123` | Пароль сидируемого админа |

## 📂 Структура проекта

```
pulseops-demo/
├── server.ts            # тонкая точка входа — бутстрапит src/main.ts (сохраняет `tsx server.ts api|worker`)
├── src/
│   ├── config.ts        # env-валидация через Zod (JWT, lease, rate-limit)
│   ├── logger.ts        # структурированные JSON-логи
│   ├── errors.ts        # иерархия AppError + единый middleware ошибок
│   ├── types.ts         # доменные типы + интерфейсы IStore/ICache/IQueue
│   ├── auth.ts          # scrypt-хеши, HS256 JWT, ротация refresh + детект повторов, RBAC
│   ├── app.ts           # express-фабрика: CORS, заголовки, requestId, логирование, rate limit
│   ├── main.ts          # bootstrap (demo/docker), graceful shutdown с дренажем
│   ├── ui.ts            # встроенная панель (логин, статистика, фильтры, drawer, toasts)
│   ├── routes.ts        # auth + jobs + admin роуты, реальные permission-мидлвары
│   ├── store/           # PostgresStore (лизы, outbox, транзакции) + MemoryStore
│   ├── cache.ts         # RedisCache + MemoryCache
│   ├── queue.ts         # RabbitQueue (reconnect, DLQ) + MemoryQueue
│   └── services/        # outbox-диспетчер, воркер (лизы/backoff/reaper), jobs-сервис
├── tests/               # 43 теста node:test (7 файлов) — без новых зависимостей
├── scripts/             # start.mjs (Docker, если есть, иначе демо-режим), demo.mjs
├── start.bat / start.sh # запуск в один клик
├── docker-compose.yml, Dockerfile, tsconfig.json, .env.example, package.json
└── README.md
```

Адаптеры (`PostgresStore`/`MemoryStore`, `RedisCache`/`MemoryCache`,
`RabbitQueue`/`MemoryQueue`) реализуют общие интерфейсы (`IStore`, `ICache`, `IQueue`),
поэтому бизнес-логика полностью не зависит от инфраструктуры.

## 🎯 Почему этот проект подходит под вакансию

| Требование вакансии | Где посмотреть |
|---|---|
| React.js | *Честно:* панель — **inline-ваниль на TypeScript** (`src/ui.ts`), не React — без шага сборки, отдаётся самим API. Слоистая структура routes → services → adapters напрямую переносится на фичи в React/Angular. |
| Node.js + TypeScript | Строгий TS, типизированные интерфейсы, валидация Zod |
| Архитектура в стиле NestJS | Слоистость: routes → services → store-адаптеры, структура, готовая к DI |
| PostgreSQL | Схема, транзакции, индексы, `FOR UPDATE SKIP LOCKED`, лог событий |
| Redis | Кэш статистики, heartbeat воркера, rate limit |
| RabbitMQ | Приоритетная durable-очередь, confirm-канал, DLQ, at-least-once |
| Docker | `docker-compose.yml` (5 сервисов) + `Dockerfile` |
| Микросервисы | API и воркер — отдельные процессы/роли с независимым масштабированием |
| Легаси PHP/Bitrix (бонус) | Ниже — подход к миграции |

**Про легаси PHP/Bitrix:** демо сфокусировано на современном стеке, но история миграции простая —
слой антикоррупции, параллельный запуск, постепенное переключение с откатом: новые сервисы на
NestJS читают/пишут свой PostgreSQL, синхронизируясь с Bitrix через события; модули Bitrix
становятся тонкими адаптерами; трафик переключается за роутером; старый код удаляется по модулям.

## 🛠 Troubleshooting

- **Порт 3000 занят** — измените `PORT` в `.env` или остановите конфликтующий процесс.
- **Docker не установлен** — используйте `npm run demo`, нужен только Node.js.
- **`ECONNREFUSED` при старте** — зависимости не запущены; сначала `docker compose up -d postgres redis rabbitmq`.
- **Воркер ничего не обрабатывает** — проверьте `/api/health` (поле `worker`) и панель RabbitMQ (порт 15672).
- **Задачи застряли в `processing`** — перезапустите воркер; устаревший heartbeat виден в `/api/health`.

## 📄 Лицензия

MIT

---

<p align="center">
  <b>Сделано для отклика на вакансию Radio Record (fullstack web-разработчик).</b><br/>
  Stack: Node.js · TypeScript · NestJS-style · PostgreSQL · Redis · RabbitMQ · Docker · микросервисы · ванильная inline-панель (без React-сборки)
</p>
