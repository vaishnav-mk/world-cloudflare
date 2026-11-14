import { WorkflowRunNotFoundError, WorkflowWorldError } from '@workflow/errors';
import {
  isTerminalWorkflowRunStatus, SPEC_VERSION_LEGACY, SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM, type Hook, type HookClaimedFrom,
} from '@workflow/world';
import { DISPATCH_RECOVERY_DELAY_MS, STORE_NAME } from '../constants.js';
import { EventType, TokenClaim } from '../enums.js';
import { storeShardFor, tokenShardFor } from '../lib/shards.js';
import type { Peer, StoreContext } from './context.js';
import { appendEvent } from './events/journal.js';
import type { TokenAnswer } from './events/types.js';
import { refuseRunCopy } from './legacy.js';
import type { Takeover } from './takeovers.js';
import { importLegacyTokens } from './token-shard.js';

/**
 * Forced hook claims (`createHook({ experimental_force: true })`) move a token between runs across
 * three stores: the claimer's asks the token's shard, which records the takeover, has the holder's
 * store dispose of its hook, and only then gives the token to the claimer.
 */

/**
 * On the token's shard: claims a token for a forced `hook_created`, taking it over from a live
 * holder. The holder's store disposes of its hook before the token moves, so a resume that already
 * found the holder is refused there rather than landing in a run that no longer holds the token.
 * The answer is the grant, whose hook names what it took in `claimedFrom`; or a refusal, when the
 * holder's run predates forced disposals and would never read one.
 */
export async function claimHookTokenForced(store: StoreContext, hook: Hook): Promise<TokenAnswer> {
  const shard = tokenShardFor(hook.token);
  if (shard !== store.name) return store.peer(shard).claimHookTokenForced(hook);
  await importLegacyTokens(store, shard);
  // Before the takeover row commits, so a crash cannot strand it.
  await store.armAlarm(Date.now() + DISPATCH_RECOVERY_DELAY_MS, 'token takeover');
  const { tokens, takeovers } = store;
  const decided = store.ctx.storage.transactionSync((): TokenAnswer | Takeover => {
    const holder = tokens.find(hook.token);
    // Granted before: a retried write gets the same answer, with what the token was taken from.
    if (holder !== undefined && holder.hook.hookId === hook.hookId && holder.hook.runId === hook.runId) {
      return { kind: TokenClaim.Granted, hook: holder.hook };
    }
    if (tokens.claim(hook, new Date()) === null) return { kind: TokenClaim.Granted, hook };
    if (holder === undefined) throw new Error('A conflicting token needs a holder');
    const pending = takeovers.find(hook.token);
    if (pending !== undefined && pending.claimer.hookId !== hook.hookId) {
      throw new WorkflowWorldError(`Hook token ${hook.token} is being taken over by another run; retry`, { status: 503 });
    }
    if (pending !== undefined) return pending;
    takeovers.add(holder.hook, hook, Date.now() + DISPATCH_RECOVERY_DELAY_MS);
    return { holder: holder.hook, claimer: hook, attempts: 0 };
  });
  return 'kind' in decided ? decided : takeOver(store, decided);
}

/** On the token's shard: finishes a takeover a resume found half done, or the alarm found due. */
export async function finishTakeover(store: StoreContext, token: string): Promise<void> {
  const takeover = store.takeovers.find(token);
  if (takeover !== undefined) await takeOver(store, takeover);
}

/** The holder's store disposes of its hook, then the token moves to the claimer. A failure backs off for the alarm. */
async function takeOver(store: StoreContext, { holder, claimer, attempts }: Takeover): Promise<TokenAnswer> {
  const { token } = claimer;
  const { tokens, takeovers, ctx: { storage } } = store;
  let claimedFrom: HookClaimedFrom | null;
  try {
    claimedFrom = await onRun(store, holder.runId, (peer) => peer.disposeForcedHook(
      holder.runId, holder.hookId, token, { runId: claimer.runId, hookId: claimer.hookId },
    ));
  } catch (error) {
    await store.armAlarm(storage.transactionSync(() => takeovers.recordFailure(token, attempts)), 'token takeover retry');
    throw error;
  }
  return storage.transactionSync((): TokenAnswer => {
    // Another call finished this takeover first, and another claim may have started since.
    if (takeovers.find(token)?.claimer.hookId !== claimer.hookId) {
      const current = tokens.find(token);
      return current?.hook.hookId === claimer.hookId
        ? { kind: TokenClaim.Granted, hook: current.hook }
        : { kind: TokenClaim.Held, holderRunId: current?.hook.runId ?? holder.runId };
    }
    takeovers.remove(token);
    if (claimedFrom === null) return { kind: TokenClaim.Refused, holderRunId: holder.runId };
    const taken = { ...claimer, claimedFrom };
    tokens.give(taken);
    return { kind: TokenClaim.Granted, hook: taken };
  });
}

/** A run-scoped call on the store that holds the run: its shard, else the legacy store. */
async function onRun<T>(store: StoreContext, runId: string, call: (peer: Peer) => Promise<T>): Promise<T> {
  try {
    return await call(store.peer(storeShardFor(runId)));
  } catch (error) {
    if (!WorkflowRunNotFoundError.is(error)) throw error;
  }
  return call(store.peer(STORE_NAME));
}

/**
 * On the holder's store, for a token's shard taking a hook of this store's run for another run's
 * forced claim: journals `hook_disposed{forceClaimedBy}` while the run can still read it, and
 * removes the hook. The takeover is kept, so a later resume is refused naming the new holder, and a
 * hook not created yet is disposed of as it commits. Returns what the claimer journals as
 * `forceClaimedFrom`, or null for a running run that predates forced disposals, which keeps its hook.
 */
export async function disposeForcedHook(
  store: StoreContext, runId: string, hookId: string, token: string, claimedBy: { runId: string; hookId: string },
): Promise<HookClaimedFrom | null> {
  store.own(runId);
  const { db } = store;
  if (store.isLegacy) refuseRunCopy(db, runId);
  const outcome = store.ctx.storage.transactionSync(() => {
    const prior = db.findForcedDisposal(hookId);
    if (prior !== undefined) return { claimedFrom: prior.claimedFrom, journaled: false };
    const run = db.getRun(runId);
    const running = !isTerminalWorkflowRunStatus(run.status);
    const specVersion = run.specVersion ?? SPEC_VERSION_LEGACY;
    if (running && specVersion < SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM) return null;
    // A finished run has nothing to wake, so its takeover names no wake target.
    const claimedFrom: HookClaimedFrom = running
      ? { runId, hookId, workflowName: run.workflowName, deploymentId: run.deploymentId, runSpecVersion: specVersion }
      : { runId, hookId };
    const holds = db.findHook(hookId)?.runId === runId;
    if (holds && running) {
      appendEvent(db, runId, {
        eventType: EventType.HookDisposed, correlationId: hookId, specVersion, eventData: { token, forceClaimedBy: claimedBy },
      }, new Date());
    }
    if (holds) {
      db.deleteRunHook(runId, hookId);
      store.index.oweHook(runId, hookId);
    }
    db.insertForcedDisposal(runId, hookId, { token, claimedBy, claimedFrom });
    return { claimedFrom, journaled: holds && running };
  });
  if (outcome === null) return null;
  if (outcome.journaled) store.waiters.wake(runId);
  await store.feed.flush();
  return outcome.claimedFrom;
}
