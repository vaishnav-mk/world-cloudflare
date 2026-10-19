import type { Hook, Step, Wait, WorkflowRun } from '@workflow/world';
import { DOQB, FetchTypes, OrderTypes, Raw } from 'workers-qb';
import { decode, encode } from '../lib/codec.js';
import type { EventRow, RecordRow } from './rows.js';
import type { StoreSchema } from './tables.js';

export type RecordTable = 'runs' | 'steps' | 'hooks';

/** Upserts replace the encoded entity and keep the row's identity columns. */
const replaceData = { data: new Raw('excluded.data') };

/** Typed reads and writes over the store's tables. Callers own transactions. */
export class Db {
  readonly #qb: DOQB<StoreSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<StoreSchema>(sql);
  }

  // Runs, steps, hooks and waits.

  run(id: string): WorkflowRun | undefined {
    const row = this.#qb.fetchOne({ tableName: 'runs', fields: ['data'], where: { conditions: 'id = ?', params: [id] } });
    return decodeRow(row.execute().results);
  }

  step(runId: string, stepId: string): Step | undefined {
    const row = this.#qb.fetchOne({
      tableName: 'steps', fields: ['data'],
      where: { conditions: ['run_id = ?', 'id = ?'], params: [runId, stepId] },
    });
    return decodeRow(row.execute().results);
  }

  hook(hookId: string): Hook | undefined {
    const row = this.#qb.fetchOne({ tableName: 'hooks', fields: ['data'], where: { conditions: 'id = ?', params: [hookId] } });
    return decodeRow(row.execute().results);
  }

  hookByToken(token: string): Hook | undefined {
    const row = this.#qb.fetchOne({ tableName: 'hooks', fields: ['data'], where: { conditions: 'token = ?', params: [token] } });
    return decodeRow(row.execute().results);
  }

  wait(runId: string, correlationId: string): Wait | undefined {
    const row = this.#qb.fetchOne({
      tableName: 'waits', fields: ['data'],
      where: { conditions: ['run_id = ?', 'id = ?'], params: [runId, correlationId] },
    });
    return decodeRow(row.execute().results);
  }

  putRun(run: WorkflowRun): void {
    this.#qb.insert({
      tableName: 'runs',
      data: {
        id: run.runId, data: encode(run), created_at: run.createdAt.getTime(),
        status: run.status, workflow_name: run.workflowName,
      },
      onConflict: { column: 'id', data: { ...replaceData, status: new Raw('excluded.status') } },
    }).execute();
  }

  putStep(step: Step): void {
    this.#qb.insert({
      tableName: 'steps',
      data: { run_id: step.runId, id: step.stepId, data: encode(step), created_at: step.createdAt.getTime() },
      onConflict: { column: ['run_id', 'id'], data: replaceData },
    }).execute();
  }

  insertHook(hook: Hook): void {
    this.#qb.insert({
      tableName: 'hooks',
      data: { id: hook.hookId, run_id: hook.runId, token: hook.token, data: encode(hook), created_at: hook.createdAt.getTime() },
    }).execute();
  }

  deleteHook(hookId: string): void {
    this.#qb.delete({ tableName: 'hooks', where: { conditions: 'id = ?', params: [hookId] } }).execute();
  }

  deleteRunHook(runId: string, hookId: string): void {
    this.#qb.delete({ tableName: 'hooks', where: { conditions: ['id = ?', 'run_id = ?'], params: [hookId, runId] } }).execute();
  }

  /** Frees a finished run's hook tokens, except those still inside their retention window. */
  releaseHooks(runId: string, now: Date): void {
    const rows = this.#qb.fetchAll<'hooks', 'id' | 'data'>({
      tableName: 'hooks', fields: ['id', 'data'], where: { conditions: 'run_id = ?', params: [runId] },
    }).execute().results ?? [];
    for (const row of rows) {
      const retainedUntil = decode<Hook>(row.data).tokenRetentionUntil;
      if (!retainedUntil || retainedUntil.getTime() <= now.getTime()) this.deleteHook(row.id);
    }
  }

  putWait(runId: string, correlationId: string, wait: Wait): void {
    this.#qb.insert({
      tableName: 'waits',
      data: { run_id: runId, id: correlationId, data: encode(wait) },
      onConflict: { column: ['run_id', 'id'], data: replaceData },
    }).execute();
  }

  deleteWaits(runId: string): void {
    this.#qb.delete({ tableName: 'waits', where: { conditions: 'run_id = ?', params: [runId] } }).execute();
  }

  // Events.

  event(runId: string, slot: number): EventRow | undefined {
    return this.#qb.fetchOne({
      tableName: 'events', where: { conditions: ['run_id = ?', 'slot = ?'], params: [runId, slot] },
    }).execute().results;
  }

  eventByResumeId(runId: string, resumeId: string): EventRow | undefined {
    return this.#qb.fetchOne({
      tableName: 'events', where: { conditions: ['run_id = ?', 'resume_id = ?'], params: [runId, resumeId] },
    }).execute().results;
  }

  /** Events after (or, descending, before) `slot`, optionally for one correlation ID. */
  events(runId: string, slot: number, limit: number, options: { correlationId?: string; descending?: boolean } = {}): EventRow[] {
    const { correlationId, descending = false } = options;
    const conditions = ['run_id = ?', descending ? 'slot < ?' : 'slot > ?'];
    const params: (string | number)[] = [runId, slot];
    if (correlationId !== undefined) {
      conditions.push('correlation_id = ?');
      params.push(correlationId);
    }
    return this.#qb.fetchAll<'events'>({
      tableName: 'events', where: { conditions, params },
      orderBy: { slot: descending ? OrderTypes.DESC : OrderTypes.ASC }, limit,
    }).execute().results ?? [];
  }

  eventsBetween(runId: string, afterSlot: number, beforeSlot: number): EventRow[] {
    return this.#qb.fetchAll<'events'>({
      tableName: 'events',
      where: { conditions: ['run_id = ?', 'slot > ?', 'slot < ?'], params: [runId, afterSlot, beforeSlot] },
      orderBy: { slot: OrderTypes.ASC },
    }).execute().results ?? [];
  }

  lastSlot(runId: string): number {
    const row = this.#qb.raw<{ slot: number }>({
      query: 'SELECT COALESCE(MAX(slot), 0) AS slot FROM events WHERE run_id = ?', args: [runId], fetchType: FetchTypes.ONE,
    }).execute().results;
    return row?.slot ?? 0;
  }

  insertEvent(row: EventRow): void {
    this.#qb.insert({ tableName: 'events', data: row }).execute();
  }

  // Lists.

  records(table: RecordTable, conditions: string[], params: (string | number)[], descending: boolean, limit: number): RecordRow[] {
    const order = descending ? OrderTypes.DESC : OrderTypes.ASC;
    return this.#qb.fetchAll<RecordTable, 'id' | 'data' | 'created_at'>({
      tableName: table, fields: ['id', 'data', 'created_at'],
      where: { conditions, params }, orderBy: { created_at: order, id: order }, limit,
    }).execute().results ?? [];
  }
}

function decodeRow<T>(row: { data: string } | undefined): T | undefined {
  return row ? decode<T>(row.data) : undefined;
}
