import { EntityConflictError, HookForceClaimedError, HookNotFoundError } from '@workflow/errors';
import type { Hook, WorkflowRun } from '@workflow/world';
import { ChangeKind, EventType, TokenClaim } from '../../enums.js';
import { unreachable } from '../../lib/unreachable.js';
import type { EventOf } from '../../types.js';
import type { Change, HookEvent, Tx } from './types.js';

/** Hooks created by this World report these fixed owner fields. */
const OWNER = { ownerId: 'cloudflare', projectId: 'cloudflare', environment: 'cloudflare' } as const;

export function changeHook(tx: Tx, run: WorkflowRun, data: HookEvent): Change {
  switch (data.eventType) {
    case EventType.HookCreated:
      return createHook(tx, run, data);
    case EventType.HookReceived:
      requireRunHook(tx, data);
      return { kind: ChangeKind.Run, run };
    case EventType.HookDisposed:
      return { kind: ChangeKind.HookDisposed, run, hook: requireRunHook(tx, data) };
    default:
      return unreachable(data);
  }
}

/**
 * Resumes and disposals must name a hook this run holds. A resume of one another run took over is
 * refused with `HookForceClaimedError` naming the new holder, so `resumeHook()` follows the token
 * there; a disposal of it finds no hook, which the SDK takes as already disposed.
 */
function requireRunHook(tx: Tx, data: EventOf<EventType.HookReceived | EventType.HookDisposed>): Hook {
  const hook = tx.db.findHook(data.correlationId);
  if (hook?.runId === tx.runId) return hook;
  const taken = data.eventType === EventType.HookReceived ? tx.db.findForcedDisposal(data.correlationId) : undefined;
  if (taken !== undefined && taken.claimedFrom.runId === tx.runId) {
    throw new HookForceClaimedError(taken.token, taken.claimedBy.runId, taken.claimedBy.hookId);
  }
  throw new HookNotFoundError(data.eventData?.token ?? data.correlationId);
}

/**
 * Creates the hook, or journals `hook_conflict` when another run holds its token, as the token's
 * shard answered beforehand. A forced creation that took the token over journals the hook it took
 * it from, whose run the token's shard already gave `hook_disposed{forceClaimedBy}`.
 */
function createHook(tx: Tx, run: WorkflowRun, data: EventOf<EventType.HookCreated>): Change {
  const { token } = data.eventData;
  if (tx.db.findHook(data.correlationId)) throw new EntityConflictError(`Hook ${data.correlationId} already exists`);
  const conflict = (holderRunId: string, refused: boolean): Change => ({ kind: ChangeKind.HookConflict, run, conflict: {
    eventType: EventType.HookConflict, correlationId: data.correlationId, specVersion: tx.specVersion,
    eventData: { token, conflictingRunId: holderRunId, ...(refused ? { forceRefusedReason: 'victim-spec-version' as const } : {}) },
  } });
  switch (tx.tokens.kind) {
    case TokenClaim.Granted:
      return { kind: ChangeKind.HookCreated, run, hook: tx.tokens.hook };
    case TokenClaim.Held:
      return conflict(tx.tokens.holderRunId, false);
    case TokenClaim.Refused:
      return conflict(tx.tokens.holderRunId, true);
    case TokenClaim.None:
      throw new Error('hook_created needs a token claim from the token shard');
    default:
      return unreachable(tx.tokens);
  }
}

/** The hook a `hook_created` makes for `run`. */
export function newHook(run: WorkflowRun, data: EventOf<EventType.HookCreated>, specVersion: number, now: Date): Hook {
  return {
    ...OWNER,
    runId: run.runId, hookId: data.correlationId, token: data.eventData.token,
    metadata: data.eventData.metadata, isWebhook: data.eventData.isWebhook ?? true,
    isSystem: data.eventData.isSystem ?? false, tokenRetentionUntil: data.eventData.tokenRetentionUntil,
    specVersion, createdAt: now,
    resumeContext: {
      deploymentId: run.deploymentId, workflowName: run.workflowName,
      runSpecVersion: run.specVersion, encryptionPublicKey: run.encryptionPublicKey,
    },
  };
}
