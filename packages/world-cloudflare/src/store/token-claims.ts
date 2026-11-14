import { HookConflictError, WorkflowRunNotFoundError, WorkflowWorldError } from '@workflow/errors';
import {
  isTerminalWorkflowRunStatus, START_HOOK_ADMISSION_REJECTED_CODE, START_HOOK_CANDIDATE_MAX_AGE_MS, ulidToDate,
  type AnyEventRequest, type StartHook, type StartHookRejectionReason,
} from '@workflow/world';
import { MAX_START_HOOK_RETENTION_MS, STORE_NAME } from '../constants.js';
import { EventType, TokenClaim } from '../enums.js';
import { storeShardFor, tokenShardFor } from '../lib/shards.js';
import { mintSpecVersion } from '../lib/spec.js';
import type { EventOf } from '../types.js';
import type { StoreContext } from './context.js';
import { newHook, startHookOf } from './events/hooks.js';
import type { Tokens } from './events/types.js';
import { refuseRunCopy } from './legacy.js';

/**
 * Checks this store takes the write, and claims a new hook's token from the token's shard before
 * it. A store shard takes writes only for runs that hash to it; the legacy store refuses writes to
 * the old directory's index copies.
 */
export async function beforeWrite(store: StoreContext, runId: string | null, data: AnyEventRequest): Promise<Tokens> {
  if (store.isLegacy) {
    if (runId === null) return { kind: TokenClaim.None };
    refuseRunCopy(store.db, runId);
  } else if (runId === null || storeShardFor(runId) !== store.name) {
    throw new WorkflowRunNotFoundError(String(runId));
  }
  switch (data.eventType) {
    case EventType.HookCreated:
      return claimToken(store, runId, data);
    case EventType.RunCreated:
      return data.eventData.startHook === undefined ? { kind: TokenClaim.None } : admit(store, runId, data, data.eventData.startHook);
    case EventType.RunStarted: {
      // A lazy start creates a run it cannot find, so a shard first rules out a legacy run.
      if (!store.isLegacy && store.db.findRun(runId) === undefined && await store.peer(STORE_NAME).holdsRun(runId)) {
        throw new WorkflowRunNotFoundError(runId);
      }
      const startHook = data.eventData?.startHook;
      return startHook === undefined ? { kind: TokenClaim.None } : admit(store, runId, data, startHook);
    }
    default:
      return { kind: TokenClaim.None };
  }
}

/** Claims a new hook's token from the token's shard. A write the transaction rejects anyway claims nothing. */
async function claimToken(store: StoreContext, runId: string, data: EventOf<EventType.HookCreated>): Promise<Tokens> {
  const run = store.db.getRun(runId);
  if (isTerminalWorkflowRunStatus(run.status) || store.db.findHook(data.correlationId)) return { kind: TokenClaim.None };
  const hook = newHook(run, data, data.specVersion ?? run.specVersion ?? mintSpecVersion(), new Date());
  // Releases this store owes go first, so a run can claim again a token it just disposed of.
  await store.pushReleases();
  const shard = store.peer(tokenShardFor(hook.token));
  if (data.eventData.force) return shard.claimHookTokenForced(hook);
  const holderRunId = await shard.claimHookToken(hook);
  return holderRunId === null ? { kind: TokenClaim.Granted, hook } : { kind: TokenClaim.Held, holderRunId };
}

/**
 * Admits a start Hook candidate (`start({ experimental_hook })`), on its `run_created` or the
 * queued `run_started` that may create it: claims the token on its shard, and the write then
 * creates the run with its start Hook. A refusal is recorded, so every later copy of the candidate
 * is refused naming the same run; an admitted candidate is its run. Answers None for a candidate
 * already admitted, whose write replays.
 */
async function admit(
  store: StoreContext, runId: string, data: EventOf<EventType.RunCreated | EventType.RunStarted>, startHook: StartHook,
): Promise<Tokens> {
  const { db } = store;
  const rejected = (message: string, reason: StartHookRejectionReason) =>
    new WorkflowWorldError(message, { status: 400, code: START_HOOK_ADMISSION_REJECTED_CODE, reason });
  const createdAt = runId.startsWith('wrun_') ? ulidToDate(runId.slice('wrun_'.length)) : null;
  if (createdAt === null) throw rejected(`A start Hook candidate needs a wrun_<ulid> run ID, not ${runId}`, 'invalid');
  const run = db.findRun(runId);
  // Past the bound a candidate can never be created, so nothing waits on its decision either.
  if (Date.now() - createdAt.getTime() > START_HOOK_CANDIDATE_MAX_AGE_MS && (run === undefined || data.eventType === EventType.RunCreated)) {
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
  if (run !== undefined) {
    if (run.startHook === undefined) throw rejected(`Run ${runId} was created without a start Hook`, 'ordinary_run');
    return { kind: TokenClaim.None };
  }
  const { deploymentId, workflowName, encryptionPublicKey } = data.eventData ?? {};
  if (deploymentId === undefined || workflowName === undefined) throw new WorkflowRunNotFoundError(runId);
  const hook = startHookOf(runId, { deploymentId, workflowName, encryptionPublicKey }, startHook,
    data.specVersion ?? mintSpecVersion(), new Date());
  await store.pushReleases();
  const holderRunId = await store.peer(tokenShardFor(hook.token)).claimHookToken(hook);
  if (holderRunId === null) return { kind: TokenClaim.Granted, hook };
  // Another copy of this candidate may have been decided meanwhile; the first decision stands.
  const decided = store.ctx.storage.transactionSync(() => {
    const admitted = db.findRun(runId)?.startHook !== undefined;
    const prior = db.findStartHookRefusal(runId);
    if (!admitted && prior === undefined) db.insertStartHookRefusal(runId, hook.token, holderRunId);
    return admitted ? undefined : prior?.conflictingRunId ?? holderRunId;
  });
  if (decided === undefined) return { kind: TokenClaim.None };
  throw new HookConflictError(hook.token, decided);
}
