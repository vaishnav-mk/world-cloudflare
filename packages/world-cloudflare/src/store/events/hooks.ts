import { EntityConflictError } from '@workflow/errors';
import { isTerminalWorkflowRunStatus, type Hook, type WorkflowRun } from '@workflow/world';
import { EventType } from '../../enums.js';
import { unreachable } from '../../lib/unreachable.js';
import type { EventOf } from '../../types.js';
import type { Change, HookEvent, Tx } from './types.js';

type HookChange = Pick<Change, 'createdHook' | 'hookConflict'>;

/** Hooks created by this World report these fixed owner fields. */
const OWNER = { ownerId: 'cloudflare', projectId: 'cloudflare', environment: 'cloudflare' } as const;

export function changeHook(tx: Tx, run: WorkflowRun, data: HookEvent): HookChange {
  switch (data.eventType) {
    case EventType.HookCreated:
      return createHook(tx, run, data);
    case EventType.HookReceived:
    case EventType.HookDisposed: {
      const hook = tx.db.hook(data.correlationId);
      if (!hook || hook.runId !== tx.runId) throw new Error(`Hook ${data.correlationId} not found`);
      return {};
    }
    default:
      return unreachable(data);
  }
}

/**
 * Claims the hook's token, which is unique across runs. A token held by a finished run whose
 * retention has lapsed is reclaimed; one held by any other run journals `hook_conflict` instead.
 */
function createHook(tx: Tx, run: WorkflowRun, data: EventOf<EventType.HookCreated>): HookChange {
  const { token } = data.eventData;
  if (data.eventData.force) throw new Error('Forced hook claims are not supported');
  if (tx.db.hook(data.correlationId)) throw new EntityConflictError(`Hook ${data.correlationId} already exists`);
  const holder = tx.db.hookByToken(token);
  if (holder) {
    if (!canReclaim(tx, holder)) {
      return { hookConflict: {
        eventType: EventType.HookConflict, correlationId: data.correlationId, specVersion: tx.specVersion,
        eventData: { token, conflictingRunId: holder.runId },
      } };
    }
    tx.db.deleteHook(holder.hookId);
  }
  const hook: Hook = {
    ...OWNER,
    runId: tx.runId, hookId: data.correlationId, token,
    metadata: data.eventData.metadata, isWebhook: data.eventData.isWebhook ?? true,
    isSystem: data.eventData.isSystem ?? false, tokenRetentionUntil: data.eventData.tokenRetentionUntil,
    specVersion: tx.specVersion, createdAt: tx.now,
    resumeContext: {
      deploymentId: run.deploymentId, workflowName: run.workflowName,
      runSpecVersion: run.specVersion, encryptionPublicKey: run.encryptionPublicKey,
    },
  };
  return { createdHook: hook };
}

function canReclaim(tx: Tx, holder: Hook): boolean {
  const holderRun = tx.db.run(holder.runId);
  const retentionLapsed = !holder.tokenRetentionUntil || holder.tokenRetentionUntil.getTime() <= tx.now.getTime();
  return Boolean(holderRun && isTerminalWorkflowRunStatus(holderRun.status) && retentionLapsed);
}
