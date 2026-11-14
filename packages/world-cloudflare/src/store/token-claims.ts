import { EntityConflictError, HookConflictError, WorkflowRunNotFoundError, WorkflowWorldError } from '@workflow/errors';
import {
  START_HOOK_ADMISSION_REJECTED_CODE, START_HOOK_CANDIDATE_MAX_AGE_MS, ulidToDate,
  type AnyEventRequest, type CreateEventParams, type EventResult, type Hook, type StartHook, type StartHookRejectionReason,
} from '@workflow/world';
import { DISPATCH_RECOVERY_DELAY_MS, MAX_START_HOOK_RETENTION_MS, STORE_NAME } from '../constants.js';
import { EventType, TokenClaim } from '../enums.js';
import { storeShardFor, tokenShardFor } from '../lib/shards.js';
import { mintSpecVersion } from '../lib/spec.js';
import type { HookClaimIntent } from '../types.js';
import type { StoreContext } from './context.js';
import { applyEvent } from './events/apply.js';
import { newHook, startHookOf } from './events/hooks.js';
import { refuseRunCopy } from './legacy.js';

/** Build the provisional grant. No registry mutation is permitted before reducer admission. */
export async function prepareHook(store: StoreContext, runId: string, data: AnyEventRequest): Promise<Hook | undefined> {
  if (store.isLegacy) refuseRunCopy(store.db, runId);
  else if (storeShardFor(runId) !== store.name) throw new WorkflowRunNotFoundError(runId);
  if (data.eventType === EventType.HookCreated) {
    const run = store.db.getRun(runId);
    return newHook(run, data, data.specVersion ?? run.specVersion ?? mintSpecVersion(), new Date());
  }
  const run = store.db.findRun(runId);
  if (data.eventType !== EventType.RunCreated && data.eventType !== EventType.RunStarted) return undefined;
  if (data.eventType === EventType.RunStarted && !store.isLegacy && run === undefined && await store.peer(STORE_NAME).holdsRun(runId)) {
    throw new WorkflowRunNotFoundError(runId);
  }
  const startHook = data.eventData?.startHook;
  if (startHook === undefined) return undefined;
  checkCandidate(store, runId, data.eventType, startHook);
  if (run !== undefined) return undefined;
  const { deploymentId, workflowName, encryptionPublicKey } = data.eventData ?? {};
  if (deploymentId === undefined || workflowName === undefined) throw new WorkflowRunNotFoundError(runId);
  return startHookOf(runId, { deploymentId, workflowName, encryptionPublicKey }, startHook, data.specVersion ?? mintSpecVersion(), new Date());
}

function rejected(message: string, reason: StartHookRejectionReason): WorkflowWorldError {
  return new WorkflowWorldError(message, { status: 400, code: START_HOOK_ADMISSION_REJECTED_CODE, reason });
}

function checkCandidate(store: StoreContext, runId: string, eventType: 'run_created' | 'run_started', startHook: StartHook): void {
  const { db } = store;
  const createdAt = runId.startsWith('wrun_') ? ulidToDate(runId.slice('wrun_'.length)) : null;
  if (createdAt === null) throw rejected(`A start Hook candidate needs a wrun_<ulid> run ID, not ${runId}`, 'invalid');
  const run = db.findRun(runId);
  if (Date.now() - createdAt.getTime() > START_HOOK_CANDIDATE_MAX_AGE_MS && (run === undefined || eventType === EventType.RunCreated)) {
    throw rejected(`Start Hook candidate ${runId} is too old to be created`, 'expired');
  }
  const until = startHook.tokenRetentionUntil;
  if (until !== undefined && until.getTime() - Date.now() > MAX_START_HOOK_RETENTION_MS) {
    throw rejected('The start Hook retention is beyond the World limit', 'invalid');
  }
  const refused = db.findStartHookRefusal(runId);
  const decidedToken = run?.startHook?.token ?? refused?.token;
  if (decidedToken !== undefined && decidedToken !== startHook.token) {
    throw rejected(`Start Hook candidate ${runId} was decided under another token`, 'token_mismatch');
  }
  if (refused !== undefined) throw new HookConflictError(startHook.token, refused.conflictingRunId);
  if (run !== undefined && run.startHook === undefined) throw rejected(`Run ${runId} was created without a start Hook`, 'ordinary_run');
}

/** The actual reducer is the validator; nested transactionSync rolls back its provisional writes. */
export function preflight(store: StoreContext, runId: string, data: AnyEventRequest, params: CreateEventParams, hook: Hook): void {
  const rollback = {};
  try {
    store.ctx.storage.transactionSync(() => {
      applyEvent(store.db, store.outbox, store.releases, store.index,
        { kind: TokenClaim.Granted, hook }, runId, data, params);
      throw rollback;
    });
  } catch (error) {
    if (error !== rollback) throw error;
  }
}

/** Runs only under the run gate. Acceptance is irreversible; reset/retry must roll it forward. */
export async function finishIntent(store: StoreContext, intent: HookClaimIntent): Promise<EventResult> {
  const { operation, request, params, expiresAt } = intent;
  const { hook } = operation;
  const shard = store.peer(tokenShardFor(hook.token));
  if (expiresAt !== undefined && Date.now() > expiresAt) {
    // A cancellation receipt precedes the conditional release, including claims delayed in transit.
    await shard.cancelAcceptedHook(operation);
    store.ctx.storage.transactionSync(() => {
      store.releases.owe(hook, true);
      store.intents.complete(intent);
    });
    throw rejected(`Start Hook candidate ${hook.runId} is too old to be created`, 'expired');
  }
  const tokens = await shard.claimAcceptedHook(operation);
  if (tokens.kind === TokenClaim.Rejected) {
    store.intents.complete(intent);
    throw new EntityConflictError(`Hook ${hook.hookId} already exists`);
  }
  if (request.eventType !== EventType.HookCreated && tokens.kind !== TokenClaim.Granted) {
    if (tokens.kind !== TokenClaim.Held && tokens.kind !== TokenClaim.Refused) throw new Error('Expected a token decision');
    store.ctx.storage.transactionSync(() => {
      store.db.insertStartHookRefusal(hook.runId, hook.token, tokens.holderRunId);
      store.intents.complete(intent);
    });
    throw new HookConflictError(hook.token, tokens.holderRunId);
  }
  return store.ctx.storage.transactionSync(() => {
    const result = applyEvent(store.db, store.outbox, store.releases, store.index, tokens, hook.runId, request, params);
    store.intents.complete(intent);
    return result;
  });
}

/** Recovery enters the same gate as writers, but never holds it while flushing release/index RPCs. */
export async function recoverIntents(store: StoreContext): Promise<void> {
  await Promise.all(store.intents.due().map((runId) => store.writeRun(runId, async () => {
    const intent = store.intents.find(runId);
    if (intent === undefined) return;
    try {
      const result = await finishIntent(store, intent);
      if (result.event !== undefined) store.waiters.wake(runId);
    } catch (error) {
      store.intents.retry(intent);
      console.error('Hook claim recovery failed', { runId, error });
    }
  })));
  if (store.intents.nextDueAt() !== null) await store.armAlarm(Date.now() + DISPATCH_RECOVERY_DELAY_MS, 'hook intent retry');
}
