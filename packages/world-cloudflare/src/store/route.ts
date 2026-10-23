import { WorkflowRunNotFoundError } from '@workflow/errors';
import { STORE_NAME } from '../constants.js';
import { transientAsWorldError } from '../lib/transient.js';
import type { WorldEnv } from '../types.js';

type Namespace = WorldEnv['WORLD_STORE'];
type Store = ReturnType<Namespace['getByName']>;

/** Runs the directory holds, created before per-run stores. A run never moves, and there are finitely many. */
const legacyRuns = new Set<string>();

/** The directory: the run index, hook tokens, and runs created before per-run stores. */
export function directory(namespace: Namespace): Store {
  return transientAsWorldError(namespace.getByName(STORE_NAME));
}

/** A run's own store. A new run's `run_created` goes straight here. */
export function runStore(namespace: Namespace, runId: string): Store {
  return transientAsWorldError(namespace.getByName(runId));
}

/**
 * Makes a run-scoped call on the run's own store, or on the directory for a run created before
 * per-run stores: a run's store throws `WorkflowRunNotFoundError` for a run it does not hold.
 * Remembers a run the directory answered for, unless the call is one the directory also answers
 * for a run it lacks (`answersUnknown`), such as a list.
 */
export async function onRun<T>(
  namespace: Namespace, runId: string, call: (store: Store) => T, { answersUnknown = false } = {},
): Promise<Awaited<T>> {
  if (legacyRuns.has(runId)) return await call(directory(namespace));
  try {
    return await call(runStore(namespace, runId));
  } catch (error) {
    if (!WorkflowRunNotFoundError.is(error)) throw error;
  }
  const result = await call(directory(namespace));
  if (!answersUnknown) legacyRuns.add(runId);
  return result;
}
