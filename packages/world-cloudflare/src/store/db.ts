import { HookNotFoundError, WorkflowRunNotFoundError } from '@workflow/errors';
import { slotToEventId, type Hook, type Step, type Wait, type WorkflowRun } from '@workflow/world';
import { DOQB, FetchTypes, OrderTypes, Raw } from 'workers-qb';
import { decode, encode } from '../lib/codec.js';
import { allRows, findRow, requireRow } from '../lib/qb.js';
import type { EventRow, RecordRow } from './rows.js';
import type { StoreSchema } from './tables.js';

export type RecordTable = 'runs' | 'steps' | 'hooks';

/** Upserts replace the encoded entity and keep the row's identity columns. */
const replaceData = { data: new Raw('excluded.data') };

/**
 * Typed reads and writes over the store's tables. Callers own transactions.
 *
 * `get*` lookups throw the contract's not-found error. `find*` lookups return `undefined` and are
 * only for callers where a missing record is a normal outcome.
 */
export class Db {
  readonly #qb: DOQB<StoreSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<StoreSchema>(sql);
  }

  // Runs.

  findRun(id: string): WorkflowRun | undefined {
    return this.#entity(this.#qb.fetchOne({ tableName: 'runs', fields: ['data'], where: { conditions: 'id = ?', params: [id] } }));
  }

  getRun(id: string): WorkflowRun {
    const run = this.findRun(id);
    if (run === undefined) throw new WorkflowRunNotFoundError(id);
    return run;
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

  // Steps.

  findStep(runId: string, stepId: string): Step | undefined {
    return this.#entity(this.#qb.fetchOne({
      tableName: 'steps', fields: ['data'], where: { conditions: ['run_id = ?', 'id = ?'], params: [runId, stepId] },
    }));
  }

  getStep(runId: string, stepId: string): Step {
    const step = this.findStep(runId, stepId);
    if (step === undefined) throw new Error(`Step ${stepId} in run ${runId} not found`);
    return step;
  }

  putStep(step: Step): void {
    this.#qb.insert({
      tableName: 'steps',
      data: { run_id: step.runId, id: step.stepId, data: encode(step), created_at: step.createdAt.getTime() },
      onConflict: { column: ['run_id', 'id'], data: replaceData },
    }).execute();
  }

  // Hooks.

  findHook(hookId: string): Hook | undefined {
    return this.#entity(this.#qb.fetchOne({ tableName: 'hooks', fields: ['data'], where: { conditions: 'id = ?', params: [hookId] } }));
  }

  getHook(hookId: string): Hook {
    const hook = this.findHook(hookId);
    if (hook === undefined) throw new HookNotFoundError(hookId);
    return hook;
  }

  findHookByToken(token: string): Hook | undefined {
    return this.#entity(this.#qb.fetchOne({ tableName: 'hooks', fields: ['data'], where: { conditions: 'token = ?', params: [token] } }));
  }

  getHookByToken(token: string): Hook {
    const hook = this.findHookByToken(token);
    if (hook === undefined) throw new HookNotFoundError(token);
    return hook;
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
    const rows = allRows(this.#qb.fetchAll<'hooks', 'id' | 'data'>({
      tableName: 'hooks', fields: ['id', 'data'], where: { conditions: 'run_id = ?', params: [runId] },
    }));
    for (const row of rows) {
      const retainedUntil = decode<Hook>(row.data).tokenRetentionUntil;
      if (retainedUntil === undefined || retainedUntil.getTime() <= now.getTime()) this.deleteHook(row.id);
    }
  }

  // Waits.

  findWait(runId: string, correlationId: string): Wait | undefined {
    return this.#entity(this.#qb.fetchOne({
      tableName: 'waits', fields: ['data'], where: { conditions: ['run_id = ?', 'id = ?'], params: [runId, correlationId] },
    }));
  }

  getWait(runId: string, correlationId: string): Wait {
    const wait = this.findWait(runId, correlationId);
    if (wait === undefined) throw new Error(`Wait ${correlationId} not found`);
    return wait;
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

  getEvent(runId: string, slot: number): EventRow {
    const row = findRow(this.#qb.fetchOne({
      tableName: 'events', where: { conditions: ['run_id = ?', 'slot = ?'], params: [runId, slot] },
    }));
    if (row === undefined) throw new Error(`Event ${slotToEventId(slot)} not found`);
    return row;
  }

  findEventByResumeId(runId: string, resumeId: string): EventRow | undefined {
    return findRow(this.#qb.fetchOne({
      tableName: 'events', where: { conditions: ['run_id = ?', 'resume_id = ?'], params: [runId, resumeId] },
    }));
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
    return allRows(this.#qb.fetchAll<'events'>({
      tableName: 'events', where: { conditions, params },
      orderBy: { slot: descending ? OrderTypes.DESC : OrderTypes.ASC }, limit,
    }));
  }

  eventsBetween(runId: string, afterSlot: number, beforeSlot: number, limit: number): EventRow[] {
    return allRows(this.#qb.fetchAll<'events'>({
      tableName: 'events',
      where: { conditions: ['run_id = ?', 'slot > ?', 'slot < ?'], params: [runId, afterSlot, beforeSlot] },
      orderBy: { slot: OrderTypes.ASC },
      limit,
    }));
  }

  lastSlot(runId: string): number {
    return requireRow(this.#qb.raw<{ slot: number }>({
      query: 'SELECT COALESCE(MAX(slot), 0) AS slot FROM events WHERE run_id = ?', args: [runId], fetchType: FetchTypes.ONE,
    })).slot;
  }

  insertEvent(row: EventRow): void {
    this.#qb.insert({ tableName: 'events', data: row }).execute();
  }

  // Lists.

  records(table: RecordTable, conditions: string[], params: (string | number)[], descending: boolean, limit: number): RecordRow[] {
    const order = descending ? OrderTypes.DESC : OrderTypes.ASC;
    return allRows(this.#qb.fetchAll<RecordTable, 'id' | 'data' | 'created_at'>({
      tableName: table, fields: ['id', 'data', 'created_at'],
      where: { conditions, params }, orderBy: { created_at: order, id: order }, limit,
    }));
  }

  #entity<T>(query: { execute(): { results?: { data: string } } }): T | undefined {
    const row = findRow(query);
    return row === undefined ? undefined : decode<T>(row.data);
  }
}
