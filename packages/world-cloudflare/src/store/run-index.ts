import type { Hook, ListHooksParams, ListWorkflowRunsParams, PaginatedResponse, WorkflowRunWithoutData } from '@workflow/world';
import { DOQB, FetchTypes } from 'workers-qb';
import { decode, encode } from '../lib/codec.js';
import { allRows, requireRow } from '../lib/qb.js';
import { indexSourceCount, indexSourceNames } from '../lib/shards.js';
import type { HeldToken, IndexAnswer, IndexBatch, ResolveDataMode } from '../types.js';
import type { Db } from './db.js';
import { listRecords } from './pages.js';
import type { StoreSchema } from './tables.js';
import { retentionLapsed } from './tokens.js';
import { hookView } from './views.js';

/** A finished run's hook past its retention is gone to readers, as on its run's store. */
const isLive = ({ hook, finished }: HeldToken, now: Date) => !finished || !retentionLapsed(hook, now);

/**
 * An index shard: the list rows of the runs that hash to it (`indexShardFor`) and of the hooks whose
 * IDs do, as their stores push them, so a cross-run list asks the index shards instead of every
 * store. Its answers count only once every store that feeds it has pushed it what it held before
 * the index (`indexSourceCount`); until then callers ask every store.
 */
export class RunIndex {
  readonly #qb: DOQB<StoreSchema>;
  readonly #db: Db;
  readonly #name: string;

  constructor(sql: SqlStorage, db: Db, name: string) {
    this.#qb = new DOQB<StoreSchema>(sql);
    this.#db = db;
    this.#name = name;
  }

  /** Applies a store's push. Call it inside `transactionSync`. */
  apply({ runs, hooks, goneHooks }: IndexBatch): void {
    for (const run of runs) {
      this.#qb.insert({
        tableName: 'index_runs',
        data: {
          id: run.runId, data: encode(run), created_at: run.createdAt.getTime(), status: run.status, workflow_name: run.workflowName,
        },
        onConflict: 'REPLACE',
      }).execute();
    }
    for (const held of hooks) {
      this.#qb.insert({
        tableName: 'index_hooks',
        data: { id: held.hook.hookId, run_id: held.hook.runId, data: encode(held), created_at: held.hook.createdAt.getTime() },
        onConflict: 'REPLACE',
      }).execute();
    }
    for (const { runId, hookId } of goneHooks) {
      this.#qb.delete({ tableName: 'index_hooks', where: { conditions: ['id = ?', 'run_id = ?'], params: [hookId, runId] } }).execute();
    }
  }

  /** Records that `store` has pushed this shard every run and hook it held before the index. */
  markIndexed(store: string): void {
    this.#qb.insert({ tableName: 'index_sources', data: { store, indexed_at: Date.now() }, onConflict: 'IGNORE' }).execute();
  }

  /** The stores that feed this shard and have yet to report it backfilled. */
  missingSources(): string[] {
    const reported = new Set(allRows(this.#qb.fetchAll<'index_sources', 'store'>({ tableName: 'index_sources', fields: ['store'] }))
      .map(({ store }) => store));
    return indexSourceNames(this.#name).filter((store) => !reported.has(store));
  }

  /** `read`'s answer once every store that feeds this shard has backfilled it. */
  answer<T>(read: () => T): IndexAnswer<T> {
    const sources = requireRow(this.#qb.raw<{ count: number }>({
      query: 'SELECT COUNT(*) AS count FROM index_sources', fetchType: FetchTypes.ONE,
    })).count;
    return sources < indexSourceCount(this.#name) ? { complete: false } : { complete: true, value: read() };
  }

  listRuns(params: ListWorkflowRunsParams): PaginatedResponse<WorkflowRunWithoutData> {
    return listRecords<WorkflowRunWithoutData>(this.#db, 'index_runs', params);
  }

  /** A live hook with this ID. IDs are unique within a store, so two runs on different stores may share one. */
  findHook(hookId: string, resolveData: ResolveDataMode | undefined, now: Date): Hook | undefined {
    const held = allRows(this.#qb.fetchAll<'index_hooks', 'data'>({
      tableName: 'index_hooks', fields: ['data'], where: { conditions: 'id = ?', params: [hookId] },
    })).map((row) => decode<HeldToken>(row.data)).find((each) => isLive(each, now));
    return held === undefined ? undefined : hookView(held.hook, resolveData);
  }

  /** A page of live hooks of every run, as a store lists its own. */
  listHooks({ pagination, resolveData }: ListHooksParams, now: Date): PaginatedResponse<Hook> {
    const page = listRecords<HeldToken>(this.#db, 'index_hooks', { pagination }, (held) => isLive(held, now));
    return { ...page, data: page.data.map(({ hook }) => hookView(hook, resolveData)) };
  }
}
