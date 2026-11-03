-- 001 baseline: schema extracted from PostgresStore.init().
-- The old inline DDL re-applied the status CHECK via ALTER TABLE on every
-- boot; here it is a single, properly named constraint inside the CREATE,
-- executed exactly once by the migration runner.
CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY,
  title VARCHAR(120) NOT NULL,
  job_type VARCHAR(40) NOT NULL CHECK (job_type IN ('campaign_publish','sync_catalog','send_digest','import_orders')),
  priority VARCHAR(10) NOT NULL CHECK (priority IN ('low','normal','high')),
  status VARCHAR(12) NOT NULL CONSTRAINT jobs_status_check CHECK (status IN ('queued','processing','succeeded','failed','cancelled')),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  lease_expires_at TIMESTAMPTZ,
  locked_by UUID
);
CREATE INDEX IF NOT EXISTS jobs_status_created_idx ON jobs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_available_idx ON jobs(status, available_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs(status, lease_expires_at) WHERE status = 'processing';

CREATE TABLE IF NOT EXISTS outbox (
  id UUID PRIMARY KEY,
  event_key UUID NOT NULL UNIQUE,
  aggregate_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  event_type VARCHAR(60) NOT NULL,
  payload JSONB NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  sent_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(available_at, id) WHERE sent_at IS NULL;

CREATE TABLE IF NOT EXISTS job_events (
  id BIGSERIAL PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  event_type VARCHAR(40) NOT NULL,
  message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events(job_id, created_at, id);

CREATE TABLE IF NOT EXISTS app_users (
  id UUID PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  role VARCHAR(20) NOT NULL CHECK (role IN ('admin','operator')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
