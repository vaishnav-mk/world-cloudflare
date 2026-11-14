import { isTerminalWorkflowRunStatus } from '@workflow/world';
import { DOQB, FetchTypes, OrderTypes, Raw } from 'workers-qb';
import { INDEX_PUSH_BATCH, MAX_PUBLISH_BACKOFF_MS } from '../constants.js';
import { ResolveData } from '../enums.js';
import { allRows, findRow, requireRow } from '../lib/qb.js';
import { indexShardFor } from '../lib/shards.js';
import type { IndexBatch } from '../types.js';
import type { Db } from './db.js';
import type { StoreSchema } from './tables.js';
import { runView } from './views.js';

/** The method a store calls on an index shard. */
type IndexShard = { indexChanges(batch: IndexBatch): Promise<void> };

type Owed = StoreSchema['index_outbox'];

/** `hook_id` of an owe for the run's own row. */
const RUN_ROW = '';

/**
 * List rows a store owes the index shards: a run's row, or one of its hooks'. An owe commits with
 * the change that caused it and stays until the index shard accepts the push, so a failed push is
 * retried rather than lost. A push reads each row's state as it sends it, so the latest state wins.
 */
export class IndexOutbox {
  readonly #qb: DOQB<StoreSchema>;
  readonly #db: Db;

  constructor(sql: SqlStorage, db: Db) {
    this.#qb = new DOQB<StoreSchema>(sql);
    this.#db = db;
  }

  oweRun(runId: string): void {
    this.#owe(runId, RUN_ROW);
  }

  oweHook(runId: string, hookId: string): void {
    this.#owe(runId, hookId);
  }

  /**
   * Backfill: owes the rows of up to `limit` runs after `after` in ID order, with their hooks.
   * Returns the ID to go on after, or null once every run is owed.
   */
  oweRunsAfter(after: string, limit: number): string | null {
    const runIds = this.#db.runIdsAfter(after, limit);
    for (const runId of runIds) {
      this.oweRun(runId);
      for (const hookId of this.#db.hookIdsOf(runId)) this.oweHook(runId, hookId);
    }
    return runIds.length < limit ? null : runIds[runIds.length - 1];
  }

  /**
   * Sends one round of due rows, newest first so a write's own rows go in the round after it, with
   * one call per index shard. A shard that fails keeps its rows owed with backoff. Returns when to
   * retry after a failure.
   */
  async push(shard: (name: string) => IndexShard): Promise<number | undefined> {
    const due = allRows(this.#qb.fetchAll<'index_outbox'>({
      tableName: 'index_outbox', where: { conditions: 'next_at <= ?', params: [Date.now()] },
      orderBy: { next_at: OrderTypes.DESC }, limit: INDEX_PUSH_BATCH,
    }));
    const batches = new Map<string, { batch: IndexBatch; rows: Owed[] }>();
    const batchFor = (name: string, row: Owed) => {
      const entry = batches.get(name) ?? { batch: { runs: [], hooks: [], goneHooks: [] }, rows: [] };
      batches.set(name, entry);
      entry.rows.push(row);
      return entry.batch;
    };
    for (const row of due) {
      const run = this.#db.findRun(row.run_id);
      if (row.hook_id === RUN_ROW) {
        // A run is never deleted, so it is here; the legacy store's index copies are never owed.
        const batch = batchFor(indexShardFor(row.run_id), row);
        if (run !== undefined) batch.runs.push(runView(run, ResolveData.None));
        continue;
      }
      const batch = batchFor(indexShardFor(row.run_id, row.hook_id), row);
      const hook = this.#db.findHook(row.hook_id);
      if (hook?.runId === row.run_id) batch.hooks.push({ hook, finished: run !== undefined && isTerminalWorkflowRunStatus(run.status) });
      else batch.goneHooks.push({ runId: row.run_id, hookId: row.hook_id });
    }
    let retryAt: number | undefined;
    await Promise.all([...batches].map(async ([name, { batch, rows }]) => {
      try {
        await shard(name).indexChanges(batch);
      } catch (error) {
        console.error('Index push failed', { shard: name, error });
        for (const row of rows) {
          const at = Date.now() + Math.min(MAX_PUBLISH_BACKOFF_MS, (row.attempts + 1) ** 2 * 1_000);
          // A row owed again during the call is due now; it keeps that.
          this.#qb.update({
            tableName: 'index_outbox', data: { attempts: row.attempts + 1, next_at: at },
            where: { conditions: ['run_id = ?', 'hook_id = ?', 'seq = ?'], params: [row.run_id, row.hook_id, row.seq] },
          }).execute();
          retryAt = Math.min(retryAt ?? at, at);
        }
        return;
      }
      // A row owed again during the call carries a newer state; it stays owed.
      for (const row of rows) {
        this.#qb.delete({
          tableName: 'index_outbox',
          where: { conditions: ['run_id = ?', 'hook_id = ?', 'seq = ?'], params: [row.run_id, row.hook_id, row.seq] },
        }).execute();
      }
    }));
    return retryAt;
  }

  hasDue(): boolean {
    return findRow(this.#qb.fetchOne<'index_outbox', 'seq'>({
      tableName: 'index_outbox', fields: ['seq'], where: { conditions: 'next_at <= ?', params: [Date.now()] },
    })) !== undefined;
  }

  /** Whether a hook's row is due, as opposed to only runs' rows. */
  hasDueHook(): boolean {
    return findRow(this.#qb.fetchOne<'index_outbox', 'seq'>({
      tableName: 'index_outbox', fields: ['seq'],
      where: { conditions: ['next_at <= ?', 'hook_id != ?'], params: [Date.now(), RUN_ROW] },
    })) !== undefined;
  }

  nextDueAt(): number | null {
    return requireRow(this.#qb.raw<{ next_at: number | null }>({
      query: 'SELECT MIN(next_at) AS next_at FROM index_outbox', fetchType: FetchTypes.ONE,
    })).next_at;
  }

  #owe(runId: string, hookId: string): void {
    this.#qb.insert({
      tableName: 'index_outbox',
      data: { run_id: runId, hook_id: hookId, next_at: Date.now() },
      onConflict: { column: ['run_id', 'hook_id'], data: { seq: new Raw('seq + 1'), next_at: new Raw('excluded.next_at') } },
    }).execute();
  }
}
