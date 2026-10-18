import { EventType, OutboxStatus } from '../enums.js';
import type { DispatchKind } from '../types.js';
import type { OutboxRow } from './rows.js';

/** Restricts a query to outbox rows whose event has this kind, and optionally this correlation ID. */
function eventFilter(kind: DispatchKind, correlationId?: string): { clause: string; values: string[] } {
  return correlationId === undefined
    ? { clause: 'kind = ?', values: [kind] }
    : { clause: 'kind = ? AND correlation_id = ?', values: [kind, correlationId] };
}

function assertSlot(slot: number, min: number): void {
  if (!Number.isSafeInteger(slot) || slot < min) throw new RangeError('Invalid dispatch slot');
}

/**
 * Queue messages owed by committed events. A row is written in the same transaction as its event
 * and stays `pending` until the message is accepted by the queue or is no longer needed.
 */
export class Outbox {
  constructor(private readonly sql: SqlStorage) {}

  add(runId: string, slot: number, queueName: string, deploymentId: string, nextAt: number): void {
    this.sql.exec(
      'INSERT INTO dispatch_outbox (run_id, slot, queue_name, deployment_id, next_at) VALUES (?, ?, ?, ?, ?)',
      runId, slot, queueName, deploymentId, nextAt,
    );
  }

  /** Highest pending slot for this kind, or 0. */
  pendingSlot(runId: string, kind: DispatchKind, correlationId?: string): number {
    const { clause, values } = eventFilter(kind, correlationId);
    return this.sql.exec<{ slot: number }>(
      `SELECT COALESCE(MAX(slot), 0) AS slot FROM dispatch_outbox
       WHERE run_id = ? AND status = '${OutboxStatus.Pending}' AND slot IN (SELECT slot FROM events WHERE run_id = ? AND ${clause})`,
      runId, runId, ...values,
    ).one().slot;
  }

  /** Marks every pending row of this kind up to `upToSlot` published. */
  publishThrough(runId: string, upToSlot: number, kind: DispatchKind): void {
    assertSlot(upToSlot, 0);
    this.sql.exec(
      `UPDATE dispatch_outbox SET status = '${OutboxStatus.Published}'
       WHERE run_id = ? AND slot <= ? AND status = '${OutboxStatus.Pending}'
         AND slot IN (SELECT slot FROM events WHERE run_id = ? AND kind = ?)`,
      runId, upToSlot, runId, kind,
    );
  }

  /** Marks exactly one row published. */
  publishSlot(runId: string, slot: number, kind: DispatchKind): void {
    assertSlot(slot, 1);
    this.sql.exec(
      `UPDATE dispatch_outbox SET status = '${OutboxStatus.Published}'
       WHERE run_id = ? AND slot = ? AND status = '${OutboxStatus.Pending}'
         AND slot IN (SELECT slot FROM events WHERE run_id = ? AND kind = ?)`,
      runId, slot, runId, kind,
    );
  }

  /** Starts and wakes settle every earlier row too; steps and waits settle only their own row. */
  publishRow(row: OutboxRow): void {
    switch (row.kind) {
      case EventType.RunCreated:
      case EventType.HookReceived:
        return this.publishThrough(row.run_id, row.slot, row.kind);
      case EventType.StepCreated:
      case EventType.WaitCreated:
        return this.publishSlot(row.run_id, row.slot, row.kind);
    }
  }

  /** Marks an entity's rows published once it no longer needs a message, such as a step that started. */
  settle(runId: string, kind: DispatchKind, correlationId: string): void {
    this.sql.exec(
      `UPDATE dispatch_outbox SET status = '${OutboxStatus.Published}'
       WHERE run_id = ? AND slot IN (SELECT slot FROM events WHERE run_id = ? AND kind = ? AND correlation_id = ?)`,
      runId, runId, kind, correlationId,
    );
  }

  /** A finished run owes nothing. */
  settleRun(runId: string): void {
    this.sql.exec(
      `UPDATE dispatch_outbox SET status = '${OutboxStatus.Published}' WHERE run_id = ? AND status = '${OutboxStatus.Pending}'`,
      runId,
    );
  }

  /**
   * Reopens published rows after their message dead-lettered. Bumping `redrive` gives the next
   * publish a fresh idempotency key, because the queue keeps the spent one forever.
   */
  reopen(runId: string, nextAt: number, kind: DispatchKind, correlationId?: string): number {
    const { clause, values } = eventFilter(kind, correlationId);
    return this.sql.exec(
      `UPDATE dispatch_outbox SET status = '${OutboxStatus.Pending}', redrive = redrive + 1, next_at = ?
       WHERE run_id = ? AND status = '${OutboxStatus.Published}'
         AND slot IN (SELECT slot FROM events WHERE run_id = ? AND ${clause})
       RETURNING slot`,
      nextAt, runId, runId, ...values,
    ).toArray().length;
  }

  due(now: number, limit: number): OutboxRow[] {
    return this.sql.exec<OutboxRow>(
      `SELECT o.run_id, o.slot, o.queue_name, o.deployment_id, o.attempts, o.redrive, o.next_at, e.kind, e.correlation_id
       FROM dispatch_outbox o JOIN events e ON e.run_id = o.run_id AND e.slot = o.slot
       WHERE o.status = '${OutboxStatus.Pending}' AND o.next_at <= ? ORDER BY o.next_at LIMIT ?`,
      now, limit,
    ).toArray();
  }

  postpone(row: OutboxRow, nextAt: number): void {
    this.sql.exec(
      `UPDATE dispatch_outbox SET next_at = ? WHERE run_id = ? AND slot = ? AND status = '${OutboxStatus.Pending}'`,
      nextAt, row.run_id, row.slot,
    );
  }

  recordFailure(row: OutboxRow, nextAt: number): void {
    this.sql.exec(
      `UPDATE dispatch_outbox SET attempts = attempts + 1, next_at = ?
       WHERE run_id = ? AND slot = ? AND status = '${OutboxStatus.Pending}'`,
      nextAt, row.run_id, row.slot,
    );
  }

  nextDueAt(): number | null {
    return this.sql.exec<{ next_at: number | null }>(
      `SELECT MIN(next_at) AS next_at FROM dispatch_outbox WHERE status = '${OutboxStatus.Pending}'`,
    ).one().next_at;
  }
}
