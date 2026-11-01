<div align="center">

# ⚡ PulseOps

### Asynchronous job processing demo — transactional outbox + worker + RabbitMQ

[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178c6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-20-339933?logo=node.js&logoColor=white)](https://nodejs.org/)
[![Express](https://img.shields.io/badge/Express-4.x-888888?logo=express)](https://expressjs.com/)
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

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Inline dashboard |
| `GET` | `/api/health` | Health: DB, Redis, RabbitMQ, worker heartbeat, outbox pending |
| `GET` | `/api/stats` | Aggregated counters by status (Redis-cached, 3 s TTL) |
| `GET` | `/api/metrics` | Prometheus-style text metrics |
| `GET` | `/api/jobs` | List: `status`, `type`, `priority`, `q`, `limit`, `offset` |
| `GET` | `/api/jobs/:id` | Job detail + event timeline |
| `POST` | `/api/jobs` | Create job (`Idempotency-Key` supported) |
| `POST` | `/api/jobs/:id/retry` | Requeue a failed job (respects `max_attempts`) |
| `POST` | `/api/jobs/:id/cancel` | Cancel a queued/processing job |

### Create a job

```bash
curl -X POST http://localhost:3000/api/jobs \
  -H "Content-Type: application/json" \
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
   │           └──▶ failed ──(retry)──▶ queued
   │                     │
   │                     └──(attempts ≥ max)──▶ DLQ (dead_letter event)
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
| `DEMO_MODE` | `0` | `1` = in-memory single-process demo |
| `WORKER_CONCURRENCY` | `5` | Worker prefetch / parallel jobs |
| `CORS_ORIGIN` | `*` | Allowed origin |
| `NODE_ENV` | `development` | Environment |

---

## 📂 Project structure

```
pulseops-demo/
├── server.ts            # API + worker + inline dashboard (single file, adapter-based)
├── scripts/
│   ├── start.mjs        # one-click launcher: Docker if available, else demo mode
│   └── demo.mjs         # demo-mode launcher (DEMO_MODE=1)
├── start.bat            # Windows double-click entry point
├── start.sh             # macOS/Linux entry point
├── docker-compose.yml   # postgres + redis + rabbitmq + api + worker
├── Dockerfile
├── .env.example
├── package.json
└── README.md
```

The adapters (`PostgresStore`/`MemoryStore`, `RedisCache`/`MemoryCache`,
`RabbitQueue`/`MemoryQueue`) implement shared interfaces (`IStore`, `ICache`, `IQueue`),
so the business logic is completely infrastructure-agnostic.

---

## 🎯 Why this project matches the vacancy

| Vacancy requirement | Where to see it |
|---|---|
| React.js | Inline dashboard: list, filters, search, create/retry/cancel, live polling |
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

- [ ] JWT auth + role-based access to the API
- [ ] Scheduled jobs (cron-like) with pg_cron / bull-board
- [ ] Retries with exponential backoff per job type
- [ ] Docker Swarm / Compose deploy profiles for staging & prod
- [ ] Unit tests (Vitest) + API integration tests (supertest)
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

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/` | Встроенная панель |
| `GET` | `/api/health` | Здоровье: БД, Redis, RabbitMQ, heartbeat воркера, отложенные события |
| `GET` | `/api/stats` | Счётчики по статусам (кэш в Redis, TTL 3 с) |
| `GET` | `/api/metrics` | Метрики в формате Prometheus |
| `GET` | `/api/jobs` | Список: `status`, `type`, `priority`, `q`, `limit`, `offset` |
| `GET` | `/api/jobs/:id` | Детали задачи + таймлайн событий |
| `POST` | `/api/jobs` | Создать задачу (поддерживается `Idempotency-Key`) |
| `POST` | `/api/jobs/:id/retry` | Вернуть failing-задачу в очередь (с учётом `max_attempts`) |
| `POST` | `/api/jobs/:id/cancel` | Отменить queued/processing задачу |

### Создание задачи

```bash
curl -X POST http://localhost:3000/api/jobs \
  -H "Content-Type: application/json" \
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
   │           └──▶ failed ──(retry)──▶ queued
   │                     │
   │                     └──(attempts ≥ max)──▶ DLQ (событие dead_letter)
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
| `DEMO_MODE` | `0` | `1` = демо в памяти в одном процессе |
| `WORKER_CONCURRENCY` | `5` | Prefetch воркера / параллельные задачи |
| `CORS_ORIGIN` | `*` | Разрешённый origin |
| `NODE_ENV` | `development` | Окружение |

## 📂 Структура проекта

```
pulseops-demo/
├── server.ts            # API + воркер + встроенная панель (один файл, на адаптерах)
├── scripts/
│   ├── start.mjs        # запуск в один клик: Docker, если есть, иначе демо-режим
│   └── demo.mjs         # запуск демо-режима (DEMO_MODE=1)
├── start.bat            # точка входа в Windows (двойной клик)
├── start.sh             # точка входа в macOS/Linux
├── docker-compose.yml   # postgres + redis + rabbitmq + api + worker
├── Dockerfile
├── .env.example
├── package.json
└── README.md
```

Адаптеры (`PostgresStore`/`MemoryStore`, `RedisCache`/`MemoryCache`,
`RabbitQueue`/`MemoryQueue`) реализуют общие интерфейсы (`IStore`, `ICache`, `IQueue`),
поэтому бизнес-логика полностью не зависит от инфраструктуры.

## 🎯 Почему этот проект подходит под вакансию

| Требование вакансии | Где посмотреть |
|---|---|
| React.js | Встроенная панель: список, фильтры, поиск, создание/повтор/отмена, живое обновление |
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
  Stack: React · Node.js · TypeScript · NestJS-style · PostgreSQL · Redis · RabbitMQ · Docker · микросервисы
</p>
