import { isTerminalWorkflowRunStatus } from '@workflow/world';
import { tokenShardFor } from '../lib/shards.js';
import type { HeldToken } from '../types.js';
import type { Db } from './db.js';

/**
 * The legacy store (`STORE_NAME`) holds the runs created before store shards, with their hooks. It
 * also still holds the old directory's index copies of sharded runs, which take no writes.
 */

/** The tokens this store's hooks hold that hash to `shard`, for that token shard's one-time import. */
export function legacyTokens(db: Db, shard: string): HeldToken[] {
  return db.hooksWithTokens((token) => tokenShardFor(token) === shard).map((hook) => {
    const run = db.findRun(hook.runId);
    return { hook, finished: run !== undefined && isTerminalWorkflowRunStatus(run.status) };
  });
}

/** Whether the legacy store holds this run itself, rather than the old directory's index copy. */
export function holdsRun(db: Db, runId: string): boolean {
  return db.findRun(runId) !== undefined && !db.isRunCopy(runId);
}

/** The old directory's copy of a sharded run was only an index entry: it takes no writes. */
export function refuseRunCopy(db: Db, runId: string): void {
  if (db.isRunCopy(runId)) throw new Error(`Run ${runId} lives on a store shard, not the legacy store`);
}
