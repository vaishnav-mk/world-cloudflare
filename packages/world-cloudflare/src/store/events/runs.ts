import { EntityConflictError, RunExpiredError } from '@workflow/errors';
import {
  applyAttributeChanges, isTerminalWorkflowRunStatus, validateAttributeChanges, validateAttributeEventDataSize,
  type AnyEventRequest, type EventResult, type WorkflowRun,
} from '@workflow/world';
import { EventType, RunStatus, StepStatus } from '../../enums.js';
import { unreachable } from '../../lib/unreachable.js';
import type { EventOf } from '../../types.js';
import type { Db } from '../db.js';
import type { RunUpdateEvent, Tx } from './types.js';

type NewRunFields = Pick<WorkflowRun, 'deploymentId' | 'workflowName' | 'input'> & Partial<Pick<WorkflowRun,
  'executionContext' | 'dynamicWorkflowCode' | 'encryptionPublicKey' | 'attributes'>> & {
  dynamicWorkflowCodeRef?: unknown;
  allowReservedAttributes?: boolean;
};

/**
 * The run an event creates, if any. `run_created` creates one; so does a lazy `run_started` that
 * carries the run's fields, and it also implies the `run_created` journaled before it.
 */
export function createRun(
  tx: Tx, data: AnyEventRequest, existing: WorkflowRun | undefined,
): { run: WorkflowRun; implied?: EventOf<EventType.RunCreated> } | undefined {
  switch (data.eventType) {
    case EventType.RunCreated:
      if (existing) throw new EntityConflictError(`Workflow run ${tx.runId} already exists`);
      return { run: newRun(tx, data.eventData) };
    case EventType.RunStarted: {
      const fields = data.eventData;
      if (existing || !fields?.deploymentId || !fields.workflowName || fields.input === undefined) return undefined;
      const eventData = {
        deploymentId: fields.deploymentId, workflowName: fields.workflowName, input: fields.input,
        executionContext: fields.executionContext, attributes: fields.attributes,
        allowReservedAttributes: fields.allowReservedAttributes, encryptionPublicKey: fields.encryptionPublicKey,
      };
      const run = newRun(tx, {
        ...eventData, dynamicWorkflowCode: fields.dynamicWorkflowCode, dynamicWorkflowCodeRef: fields.dynamicWorkflowCodeRef,
      });
      return { run, implied: { eventType: EventType.RunCreated, specVersion: tx.specVersion, eventData } };
    }
    default:
      return undefined;
  }
}

function newRun(tx: Tx, fields: NewRunFields): WorkflowRun {
  if (fields.dynamicWorkflowCodeRef) throw new Error('Dynamic workflow code refs are not supported');
  const changes = Object.entries(fields.attributes ?? {}).map(([key, value]) => ({ key, value }));
  validateAttributeChanges(changes, { allowReservedAttributes: fields.allowReservedAttributes === true });
  return {
    runId: tx.runId, status: RunStatus.Pending,
    deploymentId: fields.deploymentId, workflowName: fields.workflowName, input: fields.input,
    executionContext: fields.executionContext, dynamicWorkflowCode: fields.dynamicWorkflowCode,
    attributes: fields.attributes ?? {}, encryptionPublicKey: fields.encryptionPublicKey,
    specVersion: tx.specVersion, createdAt: tx.now, updatedAt: tx.now,
  };
}

/**
 * Whether the run accepts this event. Returns a result for an event that is already applied,
 * throws for one the run rejects, and returns undefined to proceed.
 */
export function checkRunAccepts(db: Db, run: WorkflowRun, data: AnyEventRequest): EventResult | undefined {
  if (!isTerminalWorkflowRunStatus(run.status)) {
    const alreadyStarted = data.eventType === EventType.RunStarted && run.status === RunStatus.Running;
    return alreadyStarted ? { run, allocated: 0 } : undefined;
  }
  switch (data.eventType) {
    case EventType.RunCancelled:
      if (run.status === RunStatus.Cancelled) return { run, allocated: 0 };
      break;
    case EventType.HookReceived:
      throw new RunExpiredError(`Run ${run.runId} no longer accepts hook events`, run.runId, run.status);
    case EventType.HookDisposed:
      // Retained hooks stay disposable after the run ends.
      return undefined;
    case EventType.StepCompleted:
    case EventType.StepFailed:
    case EventType.StepRetrying:
      // A step that was already running may still finish after cancellation.
      if (db.findStep(run.runId, data.correlationId)?.status === StepStatus.Running) return undefined;
      break;
  }
  throw new EntityConflictError(`Run ${run.runId} is terminal`);
}

/** The run after a lifecycle or attribute event. */
export function updateRun(run: WorkflowRun, data: RunUpdateEvent, now: Date): WorkflowRun {
  switch (data.eventType) {
    case EventType.RunStarted:
      return { ...run, status: RunStatus.Running, output: undefined, error: undefined, completedAt: undefined,
        startedAt: run.startedAt ?? now, updatedAt: now };
    case EventType.RunCompleted:
      return { ...run, status: RunStatus.Completed, error: undefined, output: data.eventData.output,
        completedAt: now, updatedAt: now };
    case EventType.RunFailed:
      return { ...run, status: RunStatus.Failed, output: undefined, error: data.eventData.error,
        errorCode: data.eventData.errorCode, completedAt: now, updatedAt: now };
    case EventType.RunCancelled:
      return { ...run, status: RunStatus.Cancelled, output: undefined, error: undefined, completedAt: now, updatedAt: now };
    case EventType.AttrSet:
      validateAttributeEventDataSize(data.eventData);
      validateAttributeChanges(data.eventData.changes, {
        existingKeys: Object.keys(run.attributes ?? {}),
        allowReservedAttributes: data.eventData.allowReservedAttributes === true,
      });
      return { ...run, attributes: applyAttributeChanges(run.attributes, data.eventData.changes), updatedAt: now };
    default:
      return unreachable(data);
  }
}
