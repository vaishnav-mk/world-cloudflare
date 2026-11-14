import { WorkflowRunNotFoundError } from '@workflow/errors';
import { FAN_OUT_CONCURRENCY, STORE_NAME } from '../constants.js';
import { storeShardFor, storeShardNames, tokenShardFor } from '../lib/shards.js';
import { transientAsWorldError } from '../lib/transient.js';
import type { WorldEnv } from '../types.js';

type Namespace = WorldEnv['WORLD_STORE'];
export type Store = ReturnType<Namespace['getByName']>;

/** Runs the legacy store holds, created before store shards. A run never moves, and there are finitely many. */
const legacyRuns = new Set<string>();


/** The legacy store: runs created before store shards, with their hooks. */
export function legacyStore(namespace: Namespace): Store {
  return transientAsWorldError(namespace.getByName(STORE_NAME));
}

/** A run's store shard. A new run's `run_created` goes straight to it. */
export function storeShard(namespace: Namespace, runId: string): Store {
  return transientAsWorldError(namespace.getByName(storeShardFor(runId)));
}

/** The store shard that registers a hook token. */
export function tokenShard(namespace: Namespace, token: string): Store {
  return transientAsWorldError(namespace.getByName(tokenShardFor(token)));
}

/**
 * Makes a run-scoped call on the run's shard, or on the legacy store for a run created before store
 * shards: a shard throws `WorkflowRunNotFoundError` for a run it does not hold. Remembers a run the
 * legacy store answered for, unless the call is one the legacy store also answers for a run it
 * lacks (`answersUnknown`), such as a list.
 */
export async function onRun<T>(
  namespace: Namespace, runId: string, call: (store: Store) => T, { answersUnknown = false } = {},
): Promise<Awaited<T>> {
  if (legacyRuns.has(runId)) return await call(legacyStore(namespace));
  try {
    return await call(storeShard(namespace, runId));
  } catch (error) {
    if (!WorkflowRunNotFoundError.is(error)) throw error;
  }
  const result = await call(legacyStore(namespace));
  if (!answersUnknown) legacyRuns.add(runId);
  return result;
}

/** Makes a call on each named store, `FAN_OUT_CONCURRENCY` at a time, answering in `names` order. */
export async function onStores<T>(namespace: Namespace, names: readonly string[], call: (store: Store) => T): Promise<Awaited<T>[]> {
  const results: Awaited<T>[] = [];
  let next = 0;
  const worker = async () => {
    for (let index = next++; index < names.length; index = next++) {
      results[index] = await call(transientAsWorldError(namespace.getByName(names[index])));
    }
  };
  await Promise.all(Array.from({ length: FAN_OUT_CONCURRENCY }, worker));
  return results;
}

/** Makes a cross-run read on every store shard, then the legacy store. */
export function onEveryStore<T>(namespace: Namespace, call: (store: Store) => T): Promise<Awaited<T>[]> {
  return onStores(namespace, [...storeShardNames(), STORE_NAME], call);
}
