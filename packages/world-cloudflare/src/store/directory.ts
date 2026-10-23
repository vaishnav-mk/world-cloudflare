import { EntityConflictError } from '@workflow/errors';
import { isTerminalWorkflowRunStatus, type Hook, type WorkflowRun } from '@workflow/world';
import type { Db } from './db.js';
import { takeToken } from './events/hooks.js';

// The directory's side of a run that lives in its own store. Call these inside `transactionSync`.
// The directory keeps an index copy of the run, for `runs.list` and `runs.getMany`, and a copy of
// each hook whose token the run holds, for token uniqueness and `hooks.get*`.

/**
 * Records the run's row as of `slot` and releases the hooks it disposed of. A run that just
 * finished frees its tokens, except those inside their retention window.
 */
export function indexRun(db: Db, run: WorkflowRun, slot: number, released: readonly string[], now: Date): void {
  const finished = () => {
    const copy = db.findRun(run.runId);
    return copy !== undefined && isTerminalWorkflowRunStatus(copy.status);
  };
  const finishedBefore = finished();
  db.putRunCopy(run, slot);
  for (const hookId of released) db.deleteRunHook(run.runId, hookId);
  if (!finishedBefore && finished()) db.releaseHooks(run.runId, now);
}

/**
 * Claims `hook.token` for `hook`, under the same rule a run held by the directory follows, and
 * keeps a copy of the hook. Returns the run that holds the token, or null once granted. A repeated
 * claim for the same hook and token is granted again, so a write retried after a crash succeeds.
 */
export function claimHookToken(db: Db, hook: Hook, now: Date): string | null {
  const run = db.findRun(hook.runId);
  // A finished run cannot create hooks, and its token release may already have run.
  if (run !== undefined && isTerminalWorkflowRunStatus(run.status)) throw new EntityConflictError(`Run ${hook.runId} is terminal`);
  const existing = db.findHook(hook.hookId);
  if (existing === undefined) {
    const holderRunId = takeToken(db, hook.token, now);
    if (holderRunId !== undefined) return holderRunId;
  } else {
    if (existing.runId !== hook.runId || existing.token !== hook.token) {
      throw new EntityConflictError(`Hook ${hook.hookId} already exists`);
    }
    db.deleteHook(hook.hookId);
  }
  db.insertHook(hook);
  return null;
}
