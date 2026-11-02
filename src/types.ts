import type { Request } from "express";

export type JobStatus = "queued" | "processing" | "succeeded" | "failed" | "cancelled";
export type JobType = "campaign_publish" | "sync_catalog" | "send_digest" | "import_orders";
export type JobPriority = "low" | "normal" | "high";
export type Role = "admin" | "operator";

export interface JobRow {
  id: string;
  title: string;
  job_type: JobType;
  priority: JobPriority;
  status: JobStatus;
  payload: Record<string, unknown>;
  attempts: number;
  max_attempts: number;
  error: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  available_at: string;
  lease_expires_at: string | null;
  locked_by: string | null;
}

export interface JobEventRow {
  id: number;
  job_id: string;
  event_type: string;
  message: string | null;
  created_at: string;
}

export interface OutboxRow {
  id: string;
  event_key: string;
  aggregate_id: string;
  event_type: string;
  payload: unknown;
  attempts: number;
  last_error: string | null;
  created_at: string;
  available_at: string;
  sent_at: string | null;
  job_priority?: JobPriority;
}

export interface UserRow {
  id: string;
  email: string;
  password_hash: string;
  role: Role;
  created_at: string;
}

export interface ListJobsFilter {
  status?: JobStatus;
  type?: JobType;
  priority?: JobPriority;
  q?: string;
  limit: number;
  offset: number;
}

export interface CreateJobInput {
  title: string;
  type: JobType;
  priority: JobPriority;
  simulateFailure: boolean;
}

export interface JobListResult {
  items: JobRow[];
  total: number;
  limit: number;
  offset: number;
}

export interface JobStats {
  total: number;
  queued: number;
  processing: number;
  succeeded: number;
  failed: number;
  cancelled: number;
}

export interface IStore {
  init(): Promise<void>;
  ping(): Promise<void>;
  listJobs(filter: ListJobsFilter): Promise<JobListResult>;
  getJob(id: string): Promise<JobRow | null>;
  getEvents(jobId: string): Promise<JobEventRow[]>;
  createJob(input: CreateJobInput): Promise<JobRow>;
  retryJob(id: string): Promise<JobRow | null>;
  cancelJob(id: string): Promise<JobRow | null>;
  claim(jobId: string, workerId: string, leaseTtlSeconds: number): Promise<JobRow | null>;
  finish(jobId: string, workerId: string, status: "succeeded" | "failed", error: string | null): Promise<boolean>;
  addEvent(jobId: string, eventType: string, message?: string): Promise<void>;
  resetExpiredLeases(nowIso: string): Promise<string[]>;
  resetForReplay(jobId: string): Promise<boolean>;
  scheduleRetry(jobId: string, availableAtIso: string): Promise<boolean>;
  outboxPending(): Promise<OutboxRow[]>;
  markOutboxSent(ids: string[]): Promise<void>;
  deferOutbox(ids: string[], delaySeconds: number, error?: string): Promise<void>;
  insertOutbox(eventKey: string, aggregateId: string, eventType: string, payload: unknown, availableAtIso?: string): Promise<void>;
  stats(): Promise<JobStats>;
  outboxCount(): Promise<number>;
  findUserByEmail(email: string): Promise<UserRow | null>;
  getUserById(id: string): Promise<UserRow | null>;
  createUser(email: string, passwordHash: string, role: Role): Promise<UserRow>;
}

export interface ICache {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlSeconds?: number): Promise<void>;
  del(key: string): Promise<void>;
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number): Promise<void>;
  ping(): Promise<boolean>;
}

export interface QueueMessage {
  jobId: string;
  raw: string;
  ack(): void;
  nack(requeue?: boolean): void;
  dlq(): void;
}

export type QueueHandler = (message: QueueMessage) => void;

export interface IQueue {
  publish(payload: unknown, priority?: JobPriority): void;
  publishDLQ(payload: unknown): void;
  consume(handler: QueueHandler): Promise<void>;
  consumeDLQ(handler: QueueHandler): Promise<void>;
  close(): Promise<void>;
  healthy(): boolean;
}

export interface SessionUser {
  id: string;
  email: string;
  role: Role;
}

export interface AuthedRequest extends Request {
  user?: SessionUser;
  requestId?: string;
}
