# PulseOps — Asynchronous Job Processing Demo

[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178c6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-20-339933?logo=node.js&logoColor=white)](https://nodejs.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169e1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-dc382d?logo=redis&logoColor=white)](https://redis.io/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3-ff6600?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ed?logo=docker&logoColor=white)](https://docs.docker.com/compose/)

A full-stack demo of an asynchronous job-processing system built with the **transactional outbox pattern**.  
Designed to showcase middle-level fullstack skills: REST API, background worker, PostgreSQL, Redis cache, RabbitMQ messaging, Docker, and an inline React dashboard.

> Built for the **Radio Record** fullstack web developer vacancy (React.js, Node.js, TypeScript, NestJS-style services, PostgreSQL, Redis, RabbitMQ, Docker, microservices).

---

## ✨ Features

- **Transactional outbox** — jobs are written to Postgres and published to RabbitMQ reliably (no lost messages).
- **Background worker** — at-least-once delivery with idempotent claim, retry limits, and a Dead-Letter Queue.
- **Inline React dashboard** — create, filter, search, cancel, retry jobs; live status updates.
- **Redis cache** — stats cached with TTL, worker heartbeat.
- **Observability** — `/api/health`, `/api/stats`, `/api/metrics` (Prometheus-style).
- **Production touches** — Zod env validation, request IDs, structured logging, rate limiting, CORS/security headers, graceful shutdown.
- **Idempotency** — `Idempotency-Key` header support on job creation.

## 🧱 Stack

| Layer | Tech |
|---|---|
| Runtime | Node.js 20, TypeScript (strict) |
| HTTP | Express |
| Database | PostgreSQL 16 (pg) |
| Cache | Redis 7 (ioredis) |
| Messaging | RabbitMQ 3 (amqplib) |
| Validation | Zod |
| Frontend | React 18 (inline, no build step) |
| Infra | Docker, Docker Compose |

## 🏗 Architecture

```
┌────────────┐     ┌──────────────┐     ┌────────────┐
│  React UI  │────▶│  Express API │────▶│ PostgreSQL │
└────────────┘     └──────┬───────┘     └─────┬──────┘
                          │ outbox             │
                          ▼                    │
                   ┌──────────────┐            │
                   │  RabbitMQ    │◀───────────┘
                   └──────┬───────┘
                          │
                          ▼
                   ┌──────────────┐
                   │   Worker     │──▶ PostgreSQL (status update)
                   └──────────────┘
                          │
                          ▼
                   ┌──────────────┐
                   │    Redis     │ (cache, heartbeat, rate limit)
                   └──────────────┘
```

## 🚀 Quick start

### Option A — Docker (recommended)

```bash
docker compose up -d postgres redis rabbitmq
npm install
npm run dev        # API  -> http://localhost:3000
npm run dev:worker # worker (second terminal)
```

### Option B — run everything in containers

```bash
docker compose up --build
```

Then open **http://localhost:3000**.

## 📡 API

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Inline dashboard |
| `GET` | `/api/health` | Service health (DB, Redis, RabbitMQ, worker, outbox) |
| `GET` | `/api/stats` | Aggregated job stats (cached) |
| `GET` | `/api/metrics` | Prometheus-style metrics |
| `GET` | `/api/jobs` | List jobs (filters: status, type, priority, q, pagination) |
| `GET` | `/api/jobs/:id` | Job details + event log |
| `POST` | `/api/jobs` | Create job (`Idempotency-Key` supported) |
| `POST` | `/api/jobs/:id/retry` | Retry a failed job |
| `POST` | `/api/jobs/:id/cancel` | Cancel a queued/processing job |

## 📂 Project structure

```
pulseops-demo/
├── server.ts          # API + worker + inline UI (single file)
├── package.json
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

## 🖼 Screenshots

> _Add screenshots of the dashboard here (create job, status filters, job details)._

## 📝 Scripts

| Script | Description |
|---|---|
| `npm run dev` | API with watch mode |
| `npm run dev:worker` | Worker with watch mode |
| `npm run typecheck` | TypeScript check (no emit) |
| `npm start` | Production API (tsx) |
| `npm run start:worker` | Production worker (tsx) |

## 📄 License

MIT

---

## Русский

Полноценное демо асинхронной обработки фоновых задач на базе **transactional outbox**.  
Показывает уровень middle fullstack: REST API, фоновый воркер, PostgreSQL, Redis-кэш, RabbitMQ, Docker и встроенная React-панель.

### Запуск

1. `docker compose up -d postgres redis rabbitmq`
2. `npm install`
3. `npm run dev` (API) и `npm run dev:worker` (воркер) в разных терминалах
4. Открыть http://localhost:3000

### Что демонстрирует
- Outbox-паттерн (надёжная публикация событий в RabbitMQ)
- Воркер с at-least-once, лимитом попыток и DLQ
- Redis-кэш, heartbeat воркера, rate limit
- Health/stats/metrics эндпоинты
- Идемпотентность, отмена, повтор, пагинация, фильтры, поиск
- Graceful shutdown, структурированные логи, requestId, Zod-валидация env

### Стек
Node.js + TypeScript (Express), PostgreSQL, Redis, RabbitMQ, Docker, Zod, React (inline).

---

Сделано для отклика на вакансию **Radio Record** (fullstack web-разработчик).
