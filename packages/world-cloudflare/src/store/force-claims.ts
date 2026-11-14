import { WorkflowRunNotFoundError, WorkflowWorldError } from '@workflow/errors';
import {
  isStartHookId, isTerminalWorkflowRunStatus, SPEC_VERSION_LEGACY, SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM, type Hook, type HookClaimedFrom,
} from '@workflow/world';
import { DISPATCH_RECOVERY_DELAY_MS, STORE_NAME } from '../constants.js';
import { EventType, TokenClaim } from '../enums.js';
import { storeShardFor, tokenShardFor } from '../lib/shards.js';
import { encode } from '../lib/codec.js';
import { sha256Hex } from '../lib/hash.js';
import type { HookClaimDecision, HookClaimOperation, HookClaimReceipt } from '../types.js';
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
  const pending = store.takeovers.find(hook.token);
  if (pending !== undefined) {
    if (pending.claimer.hookId === hook.hookId && pending.claimer.runId === hook.runId && pending.operation === null) {
      return takeOver(store, pending);
    }
    throw retryClaim(hook.token);
  }
  const holder = store.tokens.find(hook.token);
  if (holder !== undefined && holder.hook.hookId === hook.hookId && holder.hook.runId === hook.runId) {
    return { kind: TokenClaim.Granted, hook: holder.hook };
  }
  // Old stores cannot prove recoverable requester admission. Retained executors use createEvent instead.
  throw retryClaim(hook.token);
}

export function retryClaim(token: string): WorkflowWorldError {
  return new WorkflowWorldError(`Hook token ${token} needs an accepted operation or has a pending takeover; retry`, { status: 503 });
}

/** Validate RPC input against the immutable accepted operation before any registry side effect. */
async function acceptedOperation(store: StoreContext, input: unknown): Promise<{ operation: HookClaimOperation; digest: string }> {
  if (input === null || typeof input !== 'object' || !('id' in input) || typeof input.id !== 'string'
    || !('source' in input) || typeof input.source !== 'string') {
    throw new WorkflowWorldError('Invalid hook operation', { status: 400 });
  }
  // This status RPC never enters a run gate, including same-store and reciprocal takeovers.
  const evidence = await store.peer(input.source).hookClaimEvidence(input.id);
  if (evidence === undefined) throw new WorkflowWorldError('Hook operation has no accepted intent; retry', { status: 503 });
  const digest = await sha256Hex(encode(input));
  if (evidence !== digest) throw new WorkflowWorldError('Hook operation ID reused', { status: 400 });
  // SAFETY: the immutable admission digest authenticates the entire operation's parsed shape.
  return { operation: input as HookClaimOperation, digest };
}

/** Reconstruct grants from the caller's admitted Hook, not a second permanent metadata copy. */
function readDecision(store: StoreContext, operation: HookClaimOperation): HookClaimDecision | undefined {
  // SAFETY: only writeDecision writes this key.
  const receipt = store.ctx.storage.kv.get<HookClaimReceipt>(`hook-decision:${operation.id}`);
  if (receipt === undefined) return undefined;
  return receipt.kind === TokenClaim.Granted
    ? { kind: TokenClaim.Granted, hook: { ...operation.hook, ...(receipt.claimedFrom === undefined ? {} : { claimedFrom: receipt.claimedFrom }) } }
    : receipt;
}

function writeDecision(store: StoreContext, operation: HookClaimOperation, answer: HookClaimDecision): void {
  const receipt: HookClaimReceipt = answer.kind === TokenClaim.Granted
    ? { kind: TokenClaim.Granted, ...(answer.hook.claimedFrom === undefined ? {} : { claimedFrom: answer.hook.claimedFrom }) }
    : answer;
  store.ctx.storage.kv.put(`hook-decision:${operation.id}`, receipt);
}

/** Both ordinary and forced acquisitions use immutable, operation-bound receipts. */
export async function claimAcceptedHook(store: StoreContext, input: HookClaimOperation): Promise<HookClaimDecision> {
  const { operation, digest: binding } = await acceptedOperation(store, input);
  const { hook } = operation;
  const shard = tokenShardFor(hook.token);
  if (shard !== store.name) return store.peer(shard).claimAcceptedHook(operation);
  await importLegacyTokens(store, shard);
  const { storage } = store.ctx;
  const decision = () => {
    // SAFETY: only this protocol writes operation bindings and canonical answers.
    const prior = storage.kv.get<string>(`hook-operation:${operation.id}`);
    if (prior !== undefined && prior !== binding) throw new WorkflowWorldError('Hook operation ID reused', { status: 400 });
    if (storage.kv.get(`hook-cancelled:${operation.id}`) !== undefined) throw retryClaim(hook.token);
    return readDecision(store, operation);
  };
  const prior = decision();
  if (prior !== undefined) return prior;
  const decided = await storage.transaction(async () => {
    const outcome = storage.transactionSync((): HookClaimDecision | Takeover => {
      const answer = decision();
      if (answer !== undefined) return answer;
      const pending = store.takeovers.find(hook.token);
      if (pending !== undefined) {
        if (pending.operation?.id === operation.id) return pending;
        throw retryClaim(hook.token);
      }
      storage.kv.put(`hook-operation:${operation.id}`, binding);
      const holder = store.tokens.find(hook.token);
      if (holder !== undefined && holder.hook.hookId === hook.hookId && holder.hook.runId !== hook.runId) {
        const rejected = { kind: TokenClaim.Rejected } as const;
        writeDecision(store, operation, rejected);
        return rejected;
      }
      const heldBy = store.tokens.claim(hook, new Date());
      if (heldBy === null || !operation.force) {
        const result: TokenAnswer = heldBy === null ? { kind: TokenClaim.Granted, hook } : { kind: TokenClaim.Held, holderRunId: heldBy };
        writeDecision(store, operation, result);
        return result;
      }
      if (holder === undefined) throw new Error('A conflicting token needs a holder');
      store.takeovers.add(holder.hook, hook, Date.now() + DISPATCH_RECOVERY_DELAY_MS, operation);
      return { holder: holder.hook, claimer: hook, attempts: 0, operation };
    });
    // The reservation and its alarm commit together, without an RPC or an alarm-deletion gap.
    if (!('kind' in outcome)) await store.armAlarm(Date.now() + DISPATCH_RECOVERY_DELAY_MS, 'token takeover');
    return outcome;
  });
  return 'kind' in decided ? decided : takeOver(store, decided);
}

/** A durable cancellation closes delayed ordinary acquisitions before a release can run. */
export async function cancelAcceptedHook(store: StoreContext, input: HookClaimOperation): Promise<void> {
  const { operation, digest: binding } = await acceptedOperation(store, input);
  const shard = tokenShardFor(operation.hook.token);
  if (shard !== store.name) return store.peer(shard).cancelAcceptedHook(operation);
  if (operation.force) throw new WorkflowWorldError('Forced claims must roll forward', { status: 400 });
  store.ctx.storage.transactionSync(() => {
    const prior = store.ctx.storage.kv.get<string>(`hook-operation:${operation.id}`);
    if (prior !== undefined && prior !== binding) throw new WorkflowWorldError('Hook operation ID reused', { status: 400 });
    store.ctx.storage.kv.put(`hook-operation:${operation.id}`, binding);
    store.ctx.storage.kv.put(`hook-cancelled:${operation.id}`, true);
  });
}

/** On the token's shard: finishes a takeover a resume found half done, or the alarm found due. */
export async function finishTakeover(store: StoreContext, token: string): Promise<void> {
  const takeover = store.takeovers.find(token);
  if (takeover !== undefined) await takeOver(store, takeover);
}

/** The holder's store disposes of its hook, then the token moves to the claimer. A failure backs off for the alarm. */
async function takeOver(store: StoreContext, takeover: Takeover): Promise<TokenAnswer> {
  const { holder, claimer, operation } = takeover;
  const { token } = claimer;
  const { tokens, takeovers, ctx: { storage } } = store;
  let claimedFrom: HookClaimedFrom | null;
  try {
    claimedFrom = await onRun(store, holder.runId, (peer) => peer.disposeForcedHook(
      holder.runId, holder.hookId, token, { runId: claimer.runId, hookId: claimer.hookId },
    ));
  } catch (error) {
    await store.armAlarm(storage.transactionSync(() => takeovers.recordFailure(takeover)), 'token takeover retry');
    throw error;
  }
  return storage.transactionSync((): TokenAnswer => {
    // Another call finished this takeover first, and another claim may have started since.
    if (operation !== null) {
      // SAFETY: only claimAcceptedHook and this transaction write canonical decisions.
      const prior = readDecision(store, operation);
      if (prior !== undefined) {
        if (prior.kind === TokenClaim.Rejected) throw new Error('A reserved takeover cannot have an identity conflict');
        return prior;
      }
    }
    if (!takeovers.matches(takeover)) throw retryClaim(token);
    const current = tokens.find(token);
    if (current !== undefined && (current.hook.hookId !== holder.hookId || current.hook.runId !== holder.runId)) {
      throw retryClaim(token);
    }
    const answer: TokenAnswer = claimedFrom === null
      ? { kind: TokenClaim.Refused, holderRunId: holder.runId }
      : { kind: TokenClaim.Granted, hook: { ...claimer, claimedFrom } };
    if (answer.kind === TokenClaim.Granted) tokens.give(answer.hook);
    if (operation !== null) writeDecision(store, operation, answer);
    takeovers.remove(takeover);
    return answer;
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
  const { db } = store;
  if (store.isLegacy) refuseRunCopy(db, runId);
  const tokenDigest = isStartHookId(runId, hookId) ? await sha256Hex(token) : null;
  const outcome = store.ctx.storage.transactionSync(() => {
    const run = db.findRun(runId);
    if (run === undefined) {
      const pending = store.intents.find(runId);
      const candidate = pending?.expiresAt === undefined ? undefined : pending.operation.hook;
      // SAFETY: complete stores a token digest for admitted start candidates, true for other hooks.
      const completed = store.ctx.storage.kv.get<true | string>(`hook-completed:${hookId}`);
      if ((!store.isLegacy && storeShardFor(runId) !== store.name) || !isStartHookId(runId, hookId)
        || (!(candidate?.runId === runId && candidate.hookId === hookId && candidate.token === token) && completed !== tokenDigest)) {
        throw new WorkflowRunNotFoundError(runId);
      }
      if (candidate !== undefined && (candidate.specVersion ?? SPEC_VERSION_LEGACY) < SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM) return null;
    }
    const prior = db.findForcedDisposal(hookId);
    if (prior !== undefined) return { claimedFrom: prior.claimedFrom, journaled: false };
    const running = run !== undefined && !isTerminalWorkflowRunStatus(run.status);
    const specVersion = run?.specVersion ?? SPEC_VERSION_LEGACY;
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
  // The requester's run gate must not wait for the victim's eventual index update.
  store.ctx.waitUntil(store.feed.flush());
  return outcome.claimedFrom;
}
