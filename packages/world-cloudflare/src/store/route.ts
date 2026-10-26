import { WorkflowRunNotFoundError } from '@workflow/errors';
import { FAN_OUT_CONCURRENCY, STORE_NAME, STORE_SHARDS } from '../constants.js';
import { fnv1a32Hex } from '../lib/hash.js';
import { transientAsWorldError } from '../lib/transient.js';
import type { WorldEnv } from '../types.js';

type Namespace = WorldEnv['WORLD_STORE'];
type Store = ReturnType<Namespace['getByName']>;

/** Runs the legacy store holds, created before store shards. A run never moves, and there are finitely many. */
const legacyRuns = new Set<string>();

/** Every store: each shard, then the legacy store. */
const STORE_NAMES = [...Array.from({ length: STORE_SHARDS }, (_, index) => `store-${index}`), STORE_NAME];

/** The legacy store: runs created before store shards, with their hooks. */
export function legacyStore(namespace: Namespace): Store {
  return transientAsWorldError(namespace.getByName(STORE_NAME));
}

/** The store shard for a run ID or a hook token: a stable hash of it. */
export function storeShardFor(key: string): string {
  return `store-${Number.parseInt(fnv1a32Hex(key), 16) % STORE_SHARDS}`;
}

/** The store shard for a key. A new run's `run_created` goes straight to its shard, and a token's claims to its own. */
export function storeShard(namespace: Namespace, key: string): Store {
  return transientAsWorldError(namespace.getByName(storeShardFor(key)));
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

/** Makes a cross-run read on every store, `FAN_OUT_CONCURRENCY` at a time. Results are in `STORE_NAMES` order. */
export async function onEveryStore<T>(namespace: Namespace, call: (store: Store) => T): Promise<Awaited<T>[]> {
  const results: Awaited<T>[] = [];
  let next = 0;
  const worker = async () => {
    for (let index = next++; index < STORE_NAMES.length; index = next++) {
      results[index] = await call(transientAsWorldError(namespace.getByName(STORE_NAMES[index])));
    }
  };
  await Promise.all(Array.from({ length: FAN_OUT_CONCURRENCY }, worker));
  return results;
}
