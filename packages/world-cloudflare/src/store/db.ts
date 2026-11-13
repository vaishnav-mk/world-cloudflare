import { HookNotFoundError, WorkflowRunNotFoundError } from '@workflow/errors';
import {
  isTerminalWorkflowRunStatus, slotToEventId, type Hook, type HookClaimedFrom, type Step, type Wait, type WorkflowRun,
} from '@workflow/world';
import { DOQB, FetchTypes, OrderTypes, Raw } from 'workers-qb';
import { decode, encode } from '../lib/codec.js';
import { allRows, findRow, requireRow } from '../lib/qb.js';
import type { EventRow, RecordRow } from './rows.js';
import type { StoreSchema } from './tables.js';

export type RecordTable = 'runs' | 'steps' | 'hooks';

/** A hook another run took over: its token, the hook that took it, and what the takeover answered. */
export type ForcedDisposal = { token: string; claimedBy: { runId: string; hookId: string }; claimedFrom: HookClaimedFrom };

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

  /** A spec 9 run's in-band count, or null for a run without the fence. */
  seqInBand(runId: string): number | null {
    return findRow(this.#qb.fetchOne<'runs', 'seq_in_band'>({
      tableName: 'runs', fields: ['seq_in_band'], where: { conditions: 'id = ?', params: [runId] },
    }))?.seq_in_band ?? null;
  }

  setSeqInBand(runId: string, count: number): void {
    this.#qb.update({ tableName: 'runs', data: { seq_in_band: count }, where: { conditions: 'id = ?', params: [runId] } }).execute();
  }

  /** Legacy store: whether `id` is the old directory's index copy of a run that lives on a store shard. */
  isRunCopy(id: string): boolean {
    return findRow(this.#qb.fetchOne({
      tableName: 'runs', fields: ['id'], where: { conditions: ['id = ?', 'index_slot IS NOT NULL'], params: [id] },
    })) !== undefined;
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
    if (hook === undefined || this.isExpiredHook(hook, new Date())) throw new HookNotFoundError(hookId);
    return hook;
  }

  /**
   * Whether a hook's run has finished and its retention, if any, has ended. Such a hook is gone to
   * readers; its row stays until another run reclaims the token.
   */
  isExpiredHook(hook: Hook, now: Date): boolean {
    const run = this.findRun(hook.runId);
    const retained = hook.tokenRetentionUntil !== undefined && hook.tokenRetentionUntil.getTime() > now.getTime();
    return run !== undefined && isTerminalWorkflowRunStatus(run.status) && !retained;
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

  deleteHookByToken(token: string): void {
    this.#qb.delete({ tableName: 'hooks', where: { conditions: 'token = ?', params: [token] } }).execute();
  }

  /**
   * Removes a finished run's hooks, except those still inside their retention window. Returns every
   * hook the run held, whose token shards it owes a release.
   */
  releaseHooks(runId: string, now: Date): Hook[] {
    const hooks = allRows(this.#qb.fetchAll<'hooks', 'data'>({
      tableName: 'hooks', fields: ['data'], where: { conditions: 'run_id = ?', params: [runId] },
    })).map((row) => decode<Hook>(row.data));
    for (const hook of hooks) {
      const retainedUntil = hook.tokenRetentionUntil;
      if (retainedUntil === undefined || retainedUntil.getTime() <= now.getTime()) this.deleteHook(hook.hookId);
    }
    return hooks;
  }

  /** The takeover of a hook another run's forced claim disposed of, if any. */
  findForcedDisposal(hookId: string): ForcedDisposal | undefined {
    const row = findRow(this.#qb.fetchOne({
      tableName: 'forced_disposals', where: { conditions: 'hook_id = ?', params: [hookId] },
    }));
    return row === undefined ? undefined : {
      token: row.token, claimedBy: { runId: row.claimed_by_run_id, hookId: row.claimed_by_hook_id },
      claimedFrom: decode<HookClaimedFrom>(row.claimed_from),
    };
  }

  insertForcedDisposal(runId: string, hookId: string, { token, claimedBy, claimedFrom }: ForcedDisposal): void {
    this.#qb.insert({
      tableName: 'forced_disposals',
      data: {
        hook_id: hookId, run_id: runId, token, claimed_by_run_id: claimedBy.runId, claimed_by_hook_id: claimedBy.hookId,
        claimed_from: encode(claimedFrom),
      },
    }).execute();
  }

  /** Every hook whose token passes `keep`. Only the matches are decoded. */
  hooksWithTokens(keep: (token: string) => boolean): Hook[] {
    return allRows(this.#qb.fetchAll<'hooks', 'token' | 'data'>({ tableName: 'hooks', fields: ['token', 'data'] }))
      .filter((row) => keep(row.token)).map((row) => decode<Hook>(row.data));
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

  /**
   * Events after (or, descending, before) `slot`, optionally for one correlation ID, read from SQLite
   * as they are iterated, so a reader that stops early never loads the rest.
   */
  events(
    runId: string, slot: number, limit: number, options: { correlationId?: string; descending?: boolean } = {},
  ): Iterable<EventRow> {
    const { correlationId, descending = false } = options;
    const conditions = ['run_id = ?', descending ? 'slot < ?' : 'slot > ?'];
    const params: (string | number)[] = [runId, slot];
    if (correlationId !== undefined) {
      conditions.push('correlation_id = ?');
      params.push(correlationId);
    }
    // SAFETY: the rows of a SELECT on `events` are event rows. DOQB's lazy execute returns the SQLite
    // cursor itself, typed as rows of any shape, not the `{ results }` its `lazy: true` types declare.
    return this.#qb.lazyExecute(this.#qb.fetchAll<'events'>({
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
