import type { Hook, Step, Wait, WorkflowRun } from '@workflow/world';
import { decode, encode } from '../lib/codec.js';
import type { EventRow, RecordRow } from './rows.js';

export type RecordTable = 'runs' | 'steps' | 'hooks';

/** Typed reads and writes over the store's SQLite tables. Callers own transactions. */
export class Db {
  constructor(private readonly sql: SqlStorage) {}

  // Runs, steps, hooks and waits.

  run(id: string): WorkflowRun | undefined {
    return this.entity<WorkflowRun>('SELECT data FROM runs WHERE id = ?', id);
  }

  step(runId: string, stepId: string): Step | undefined {
    return this.entity<Step>('SELECT data FROM steps WHERE run_id = ? AND id = ?', runId, stepId);
  }

  hook(hookId: string): Hook | undefined {
    return this.entity<Hook>('SELECT data FROM hooks WHERE id = ?', hookId);
  }

  hookByToken(token: string): Hook | undefined {
    return this.entity<Hook>('SELECT data FROM hooks WHERE token = ?', token);
  }

  wait(runId: string, correlationId: string): Wait | undefined {
    return this.entity<Wait>('SELECT data FROM waits WHERE run_id = ? AND id = ?', runId, correlationId);
  }

  putRun(run: WorkflowRun): void {
    this.sql.exec(
      `INSERT INTO runs(id, data, created_at, status, workflow_name) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET data = excluded.data, status = excluded.status`,
      run.runId, encode(run), run.createdAt.getTime(), run.status, run.workflowName,
    );
  }

  putStep(step: Step): void {
    this.sql.exec(
      `INSERT INTO steps(run_id, id, data, created_at) VALUES (?, ?, ?, ?)
       ON CONFLICT(run_id, id) DO UPDATE SET data = excluded.data`,
      step.runId, step.stepId, encode(step), step.createdAt.getTime(),
    );
  }

  insertHook(hook: Hook): void {
    this.sql.exec(
      'INSERT INTO hooks(id, run_id, token, data, created_at) VALUES (?, ?, ?, ?, ?)',
      hook.hookId, hook.runId, hook.token, encode(hook), hook.createdAt.getTime(),
    );
  }

  deleteHook(hookId: string): void {
    this.sql.exec('DELETE FROM hooks WHERE id = ?', hookId);
  }

  deleteRunHook(runId: string, hookId: string): void {
    this.sql.exec('DELETE FROM hooks WHERE id = ? AND run_id = ?', hookId, runId);
  }

  /** Frees a finished run's hook tokens, except those still inside their retention window. */
  releaseHooks(runId: string, now: Date): void {
    const rows = this.sql.exec<{ id: string; data: string }>('SELECT id, data FROM hooks WHERE run_id = ?', runId).toArray();
    for (const row of rows) {
      const retainedUntil = decode<Hook>(row.data).tokenRetentionUntil;
      if (!retainedUntil || retainedUntil.getTime() <= now.getTime()) this.deleteHook(row.id);
    }
  }

  putWait(runId: string, correlationId: string, wait: Wait): void {
    this.sql.exec(
      `INSERT INTO waits(run_id, id, data) VALUES (?, ?, ?)
       ON CONFLICT(run_id, id) DO UPDATE SET data = excluded.data`,
      runId, correlationId, encode(wait),
    );
  }

  deleteWaits(runId: string): void {
    this.sql.exec('DELETE FROM waits WHERE run_id = ?', runId);
  }

  // Events.

  event(runId: string, slot: number): EventRow | undefined {
    return this.sql.exec<EventRow>('SELECT * FROM events WHERE run_id = ? AND slot = ?', runId, slot).toArray()[0];
  }

  eventByResumeId(runId: string, resumeId: string): (EventRow & { resume_digest: string }) | undefined {
    return this.sql.exec<EventRow & { resume_digest: string }>(
      'SELECT * FROM events WHERE run_id = ? AND resume_id = ?', runId, resumeId,
    ).toArray()[0];
  }

  /** Events after (or, descending, before) `slot`, optionally for one correlation ID. */
  events(runId: string, slot: number, limit: number, options: { correlationId?: string; descending?: boolean } = {}): EventRow[] {
    const { correlationId, descending = false } = options;
    const correlation = correlationId === undefined ? '' : ' AND correlation_id = ?';
    const query = `SELECT * FROM events WHERE run_id = ? AND slot ${descending ? '<' : '>'} ?${correlation}
                   ORDER BY slot ${descending ? 'DESC' : 'ASC'} LIMIT ?`;
    const values = correlationId === undefined ? [runId, slot, limit] : [runId, slot, correlationId, limit];
    return this.sql.exec<EventRow>(query, ...values).toArray();
  }

  eventsBetween(runId: string, afterSlot: number, beforeSlot: number): EventRow[] {
    return this.sql.exec<EventRow>(
      'SELECT * FROM events WHERE run_id = ? AND slot > ? AND slot < ? ORDER BY slot ASC', runId, afterSlot, beforeSlot,
    ).toArray();
  }

  lastSlot(runId: string): number {
    return this.sql.exec<{ slot: number }>(
      'SELECT COALESCE(MAX(slot), 0) AS slot FROM events WHERE run_id = ?', runId,
    ).one().slot;
  }

  insertEvent(row: EventRow, resumeDigest: string | null): void {
    this.sql.exec(
      `INSERT INTO events(run_id, slot, kind, correlation_id, spec_version, created_at, occurred_at,
                          resume_id, resume_digest, data, payload)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      row.run_id, row.slot, row.kind, row.correlation_id, row.spec_version, row.created_at, row.occurred_at,
      row.resume_id, resumeDigest, row.data, row.payload,
    );
  }

  // Lists.

  records(table: RecordTable, where: string[], values: (string | number)[], descending: boolean, limit: number): RecordRow[] {
    const filter = where.length ? ` WHERE ${where.join(' AND ')}` : '';
    const order = descending ? 'DESC' : 'ASC';
    return this.sql.exec<RecordRow>(
      `SELECT id, data, created_at FROM ${table}${filter} ORDER BY created_at ${order}, id ${order} LIMIT ?`,
      ...values, limit,
    ).toArray();
  }

  private entity<T>(query: string, ...values: string[]): T | undefined {
    const row = this.sql.exec<{ data: string }>(query, ...values).toArray()[0];
    return row ? decode<T>(row.data) : undefined;
  }
}
