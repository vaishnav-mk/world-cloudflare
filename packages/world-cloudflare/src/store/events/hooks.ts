import { EntityConflictError } from '@workflow/errors';
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
      requireRunHook(tx, data.correlationId);
      return { kind: ChangeKind.Run, run };
    case EventType.HookDisposed:
      return { kind: ChangeKind.HookDisposed, run, hook: requireRunHook(tx, data.correlationId) };
    default:
      return unreachable(data);
  }
}

/** Resumes and disposals must name a hook this run created. */
function requireRunHook(tx: Tx, hookId: string): Hook {
  const hook = tx.db.findHook(hookId);
  if (hook?.runId !== tx.runId) throw new Error(`Hook ${hookId} not found`);
  return hook;
}

/** Creates the hook, or journals `hook_conflict` when another run holds its token, as the token's shard answered beforehand. */
function createHook(tx: Tx, run: WorkflowRun, data: EventOf<EventType.HookCreated>): Change {
  const { token } = data.eventData;
  if (data.eventData.force) throw new Error('Forced hook claims are not supported');
  if (tx.db.findHook(data.correlationId)) throw new EntityConflictError(`Hook ${data.correlationId} already exists`);
  const conflict = (holderRunId: string): Change => ({ kind: ChangeKind.HookConflict, run, conflict: {
    eventType: EventType.HookConflict, correlationId: data.correlationId, specVersion: tx.specVersion,
    eventData: { token, conflictingRunId: holderRunId },
  } });
  switch (tx.tokens.kind) {
    case TokenClaim.Granted:
      return { kind: ChangeKind.HookCreated, run, hook: tx.tokens.hook };
    case TokenClaim.Held:
      return conflict(tx.tokens.holderRunId);
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
