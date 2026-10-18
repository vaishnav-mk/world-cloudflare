import { OutboxStatus } from '../enums.js';

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY, data TEXT NOT NULL, created_at INTEGER NOT NULL,
    status TEXT NOT NULL, workflow_name TEXT NOT NULL);
  CREATE INDEX IF NOT EXISTS runs_created ON runs(created_at, id);

  CREATE TABLE IF NOT EXISTS steps (
    run_id TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, created_at INTEGER NOT NULL,
    PRIMARY KEY(run_id, id));
  CREATE INDEX IF NOT EXISTS steps_created ON steps(run_id, created_at, id);

  CREATE TABLE IF NOT EXISTS hooks (
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, token TEXT NOT NULL UNIQUE,
    data TEXT NOT NULL, created_at INTEGER NOT NULL);
  CREATE INDEX IF NOT EXISTS hooks_created ON hooks(run_id, created_at, id);

  CREATE TABLE IF NOT EXISTS waits (
    run_id TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(run_id, id));

  CREATE TABLE IF NOT EXISTS events (
    run_id TEXT NOT NULL, slot INTEGER NOT NULL, kind TEXT NOT NULL, correlation_id TEXT,
    spec_version INTEGER NOT NULL, created_at INTEGER NOT NULL, occurred_at INTEGER,
    resume_id TEXT, resume_digest TEXT, data TEXT, payload BLOB, PRIMARY KEY(run_id, slot));
  CREATE INDEX IF NOT EXISTS events_correlation ON events(run_id, correlation_id, slot);
  CREATE UNIQUE INDEX IF NOT EXISTS events_resume ON events(run_id, resume_id) WHERE resume_id IS NOT NULL;

  CREATE TABLE IF NOT EXISTS dispatch_outbox (
    run_id TEXT NOT NULL, slot INTEGER NOT NULL, queue_name TEXT NOT NULL, deployment_id TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0, redrive INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT '${OutboxStatus.Pending}', PRIMARY KEY(run_id, slot));
`;

export function migrate(sql: SqlStorage): void {
  sql.exec(SCHEMA);
  // Outboxes created before redrive keys existed lack the column.
  const columns = sql.exec<{ name: string }>('PRAGMA table_info(dispatch_outbox)').toArray();
  if (!columns.some(({ name }) => name === 'redrive')) {
    sql.exec('ALTER TABLE dispatch_outbox ADD COLUMN redrive INTEGER NOT NULL DEFAULT 0');
  }
  sql.exec('CREATE INDEX IF NOT EXISTS dispatch_outbox_pending ON dispatch_outbox(status, next_at)');
}
