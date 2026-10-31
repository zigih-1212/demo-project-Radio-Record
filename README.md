# PulseOps Demo

Full-stack demo of an asynchronous job processing system with a transactional outbox, worker, Redis cache and RabbitMQ.

## Stack
- Node.js + TypeScript (Express)
- PostgreSQL
- Redis
- RabbitMQ
- Single-file full-stack app (API + worker + inline UI)

## Run locally
1. Start dependencies:
   `docker compose up -d postgres redis rabbitmq`
2. Install deps:
   `npm install`
3. Run API:
   `npm run dev`
4. Run worker (another terminal):
   `npm run dev:worker`
5. Open http://localhost:3000

## Scripts
- `npm run dev` — API with watch
- `npm run dev:worker` — worker with watch
- `npm run typecheck` — TypeScript check
- `npm start` / `npm run start:worker` — production run via tsx

## Notes
- Schema is created automatically on startup (demo-friendly).
- Outbox pattern ensures jobs are published to RabbitMQ reliably.
- Worker uses at-least-once delivery with idempotent claim.
