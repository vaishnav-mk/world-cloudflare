import { DOQB, FetchTypes } from 'workers-qb';
import { EventType, OutboxStatus } from '../enums.js';
import { allRows, requireRow } from '../lib/qb.js';
import type { DispatchKind } from '../types.js';
import type { OutboxRow } from './rows.js';
import type { StoreSchema } from './tables.js';

/** Restricts outbox rows to those whose event has this kind, and optionally this correlation ID. */
function eventFilter(runId: string, kind: DispatchKind, correlationId?: string): { condition: string; params: string[] } {
  return correlationId === undefined
    ? { condition: 'slot IN (SELECT slot FROM events WHERE run_id = ? AND kind = ?)', params: [runId, kind] }
    : {
      condition: 'slot IN (SELECT slot FROM events WHERE run_id = ? AND kind = ? AND correlation_id = ?)',
      params: [runId, kind, correlationId],
    };
}

function assertSlot(slot: number, min: number): void {
  if (!Number.isSafeInteger(slot) || slot < min) throw new RangeError('Invalid dispatch slot');
}

/**
 * Queue messages owed by committed events. A row is written in the same transaction as its event
 * and stays pending until the queue accepts the message or the message is no longer needed.
 */
export class Outbox {
  readonly #qb: DOQB<StoreSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<StoreSchema>(sql);
  }

  add(runId: string, slot: number, queueName: string, deploymentId: string, nextAt: number): void {
    this.#qb.insert({
      tableName: 'dispatch_outbox',
      data: { run_id: runId, slot, queue_name: queueName, deployment_id: deploymentId, next_at: nextAt },
    }).execute();
  }

  /** Highest pending slot for this kind, or 0. */
  pendingSlot(runId: string, kind: DispatchKind, correlationId?: string): number {
    const filter = eventFilter(runId, kind, correlationId);
    return requireRow(this.#qb.raw<{ slot: number }>({
      query: `SELECT COALESCE(MAX(slot), 0) AS slot FROM dispatch_outbox
              WHERE run_id = ? AND status = ? AND ${filter.condition}`,
      args: [runId, OutboxStatus.Pending, ...filter.params],
      fetchType: FetchTypes.ONE,
    })).slot;
  }

  /** Marks every pending row of this kind up to `upToSlot` published. */
  publishThrough(runId: string, upToSlot: number, kind: DispatchKind): void {
    assertSlot(upToSlot, 0);
    this.#publish(runId, kind, ['slot <= ?'], [upToSlot]);
  }

  /** Marks exactly one row published. */
  publishSlot(runId: string, slot: number, kind: DispatchKind): void {
    assertSlot(slot, 1);
    this.#publish(runId, kind, ['slot = ?'], [slot]);
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
    const filter = eventFilter(runId, kind, correlationId);
    this.#qb.update({
      tableName: 'dispatch_outbox',
      data: { status: OutboxStatus.Published },
      where: { conditions: ['run_id = ?', filter.condition], params: [runId, ...filter.params] },
    }).execute();
  }

  /** A finished run owes nothing. */
  settleRun(runId: string): void {
    this.#qb.update({
      tableName: 'dispatch_outbox',
      data: { status: OutboxStatus.Published },
      where: { conditions: ['run_id = ?', 'status = ?'], params: [runId, OutboxStatus.Pending] },
    }).execute();
  }

  /**
   * Reopens published rows after their message dead-lettered. Bumping `redrive` gives the next
   * publish a fresh idempotency key, because the queue keeps the spent one forever.
   */
  reopen(runId: string, nextAt: number, kind: DispatchKind, correlationId?: string): number {
    const filter = eventFilter(runId, kind, correlationId);
    return allRows(this.#qb.raw<{ slot: number }>({
      query: `UPDATE dispatch_outbox SET status = ?, redrive = redrive + 1, next_at = ?
              WHERE run_id = ? AND status = ? AND ${filter.condition}
              RETURNING slot`,
      args: [OutboxStatus.Pending, nextAt, runId, OutboxStatus.Published, ...filter.params],
      fetchType: FetchTypes.ALL,
    })).length;
  }

  due(now: number, limit: number): OutboxRow[] {
    // SAFETY: only step and wait events, which always have a correlation ID, write those kinds.
    return allRows(this.#qb.raw<OutboxRow>({
      query: `SELECT o.run_id, o.slot, o.queue_name, o.deployment_id, o.attempts, o.redrive, o.next_at,
                     e.kind, e.correlation_id
              FROM dispatch_outbox o JOIN events e ON e.run_id = o.run_id AND e.slot = o.slot
              WHERE o.status = ? AND o.next_at <= ? ORDER BY o.next_at LIMIT ?`,
      args: [OutboxStatus.Pending, now, limit],
      fetchType: FetchTypes.ALL,
    }));
  }

  postpone(row: OutboxRow, nextAt: number): void {
    this.#qb.update({
      tableName: 'dispatch_outbox',
      data: { next_at: nextAt },
      where: { conditions: ['run_id = ?', 'slot = ?', 'status = ?'], params: [row.run_id, row.slot, OutboxStatus.Pending] },
    }).execute();
  }

  recordFailure(row: OutboxRow, nextAt: number): void {
    this.#qb.raw({
      query: 'UPDATE dispatch_outbox SET attempts = attempts + 1, next_at = ? WHERE run_id = ? AND slot = ? AND status = ?',
      args: [nextAt, row.run_id, row.slot, OutboxStatus.Pending],
    }).execute();
  }

  nextDueAt(): number | null {
    return requireRow(this.#qb.raw<{ next_at: number | null }>({
      query: 'SELECT MIN(next_at) AS next_at FROM dispatch_outbox WHERE status = ?',
      args: [OutboxStatus.Pending],
      fetchType: FetchTypes.ONE,
    })).next_at;
  }

  #publish(runId: string, kind: DispatchKind, conditions: string[], params: number[]): void {
    const filter = eventFilter(runId, kind);
    this.#qb.update({
      tableName: 'dispatch_outbox',
      data: { status: OutboxStatus.Published },
      where: {
        conditions: ['run_id = ?', ...conditions, 'status = ?', filter.condition],
        params: [runId, ...params, OutboxStatus.Pending, ...filter.params],
      },
    }).execute();
  }
}
