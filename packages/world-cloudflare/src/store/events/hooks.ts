import { EntityConflictError } from '@workflow/errors';
import { isTerminalWorkflowRunStatus, type Hook, type WorkflowRun } from '@workflow/world';
import { ChangeKind, EventType, TokenClaim } from '../../enums.js';
import { unreachable } from '../../lib/unreachable.js';
import type { EventOf } from '../../types.js';
import type { Db } from '../db.js';
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
      requireRunHook(tx, data.correlationId);
      return { kind: ChangeKind.HookDisposed, run, hookId: data.correlationId };
    default:
      return unreachable(data);
  }
}

/** Resumes and disposals must name a hook this run created. */
function requireRunHook(tx: Tx, hookId: string): void {
  if (tx.db.findHook(hookId)?.runId !== tx.runId) throw new Error(`Hook ${hookId} not found`);
}

/**
 * Creates the hook, or journals `hook_conflict` when another run holds its token. The directory
 * decides from its own hooks table; a run store brings the directory's answer, claimed beforehand.
 */
function createHook(tx: Tx, run: WorkflowRun, data: EventOf<EventType.HookCreated>): Change {
  const { token } = data.eventData;
  if (data.eventData.force) throw new Error('Forced hook claims are not supported');
  if (tx.db.findHook(data.correlationId)) throw new EntityConflictError(`Hook ${data.correlationId} already exists`);
  const conflict = (holderRunId: string): Change => ({ kind: ChangeKind.HookConflict, run, conflict: {
    eventType: EventType.HookConflict, correlationId: data.correlationId, specVersion: tx.specVersion,
    eventData: { token, conflictingRunId: holderRunId },
  } });
  switch (tx.tokens.kind) {
    case TokenClaim.Local: {
      const holderRunId = takeToken(tx.db, token, tx.now);
      if (holderRunId !== undefined) return conflict(holderRunId);
      return { kind: ChangeKind.HookCreated, run, hook: newHook(run, data, tx.specVersion, tx.now) };
    }
    case TokenClaim.Granted:
      return { kind: ChangeKind.HookCreated, run, hook: tx.tokens.hook };
    case TokenClaim.Held:
      return conflict(tx.tokens.holderRunId);
    case TokenClaim.None:
      throw new Error('hook_created needs a token claim from the directory');
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

/**
 * Takes `token` in this store's hooks table, reclaiming it from a finished run whose retention has
 * lapsed. Returns the run that holds it otherwise. The caller writes the new hook.
 */
export function takeToken(db: Db, token: string, now: Date): string | undefined {
  const holder = db.findHookByToken(token);
  if (holder === undefined) return undefined;
  const holderRun = db.findRun(holder.runId);
  const retentionLapsed = !holder.tokenRetentionUntil || holder.tokenRetentionUntil.getTime() <= now.getTime();
  if (holderRun === undefined || !isTerminalWorkflowRunStatus(holderRun.status) || !retentionLapsed) return holder.runId;
  db.deleteHook(holder.hookId);
  return undefined;
}
