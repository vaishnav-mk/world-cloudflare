import { EntityConflictError, WorkflowRunNotFoundError } from '@workflow/errors';
import {
  CreateEventSchema, getQueueTopicPrefix, getRunQueueNamespace, isTerminalWorkflowRunStatus, mintedSpecVersion,
  requireEventSlot, ValidQueueName,
  type AnyEventRequest, type CreateEventParams, type Event, type EventResult, type WorkflowRun,
} from '@workflow/world';
import { ulid } from 'ulid';
import { ChangeKind, EventType } from '../../enums.js';
import type { Db } from '../db.js';
import type { Outbox } from '../outbox.js';
import { toEvent } from '../rows.js';
import { changeHook } from './hooks.js';
import { appendEvent, eventResult, journalOf } from './journal.js';
import { checkRunAccepts, createRun, updateRun } from './runs.js';
import { changeStep } from './steps.js';
import type { Change, Tx } from './types.js';
import { changeWait } from './waits.js';

/**
 * Validates and writes one event with every state change it causes: the run, step, hook or wait,
 * and any queue message it owes. Call it inside `transactionSync`, so all of it commits together.
 */
export function applyEvent(
  db: Db, outbox: Outbox, requestedRunId: string | null, request: AnyEventRequest, params: CreateEventParams = {},
): EventResult {
  const data = CreateEventSchema.parse(request);
  checkParams(data, params);
  const runId = requestedRunId ?? (data.eventType === EventType.RunCreated ? `wrun_${ulid()}` : null);
  if (runId === null) throw new Error('runId is required');
  const replayed = replayedResume(db, runId, data, params);
  if (replayed) return replayed;

  const existing = db.findRun(runId);
  const specVersion = data.specVersion ?? existing?.specVersion ?? mintedSpecVersion();
  const tx: Tx = { db, outbox, runId, now: new Date(), specVersion };
  const created = createRun(tx, data, existing);
  const run = created?.run ?? existing;
  if (run === undefined) throw new WorkflowRunNotFoundError(runId);
  const noop = checkRunAccepts(db, run, data);
  if (noop) return noop;

  const change = changeFor(tx, run, data);
  if (change.kind === ChangeKind.HookConflict) {
    return eventResult(db, appendEvent(db, runId, change.conflict, tx.now), change, params);
  }
  if (created?.implied) appendEvent(db, runId, created.implied, tx.now);
  if (change.kind === ChangeKind.LazyStepStart) appendEvent(db, runId, change.implied, tx.now);
  const event = appendEvent(db, runId, { ...journalOf(data, change), specVersion }, tx.now, params);
  persist(tx, data, event, change);
  return eventResult(db, event, change, params);
}

function checkParams(data: AnyEventRequest, params: CreateEventParams): void {
  if (params.inBand || params.expectedSeqInBand !== undefined) {
    throw new Error('In-band writer fencing is not supported');
  }
  const resumesHook = data.eventType === EventType.HookReceived;
  if (!resumesHook && (params.resumeId || params.resumePayloadDigest)) {
    throw new Error('resumeId is valid only for hook_received');
  }
  if (resumesHook && params.resumeId && !params.resumePayloadDigest) {
    throw new Error('resumePayloadDigest is required with resumeId');
  }
}

/** A `hook_received` retried with the same resume ID returns the event it already wrote. */
function replayedResume(db: Db, runId: string, data: AnyEventRequest, params: CreateEventParams): EventResult | undefined {
  if (data.eventType !== EventType.HookReceived || !params.resumeId) return undefined;
  const existing = db.findEventByResumeId(runId, params.resumeId);
  if (existing === undefined) return undefined;
  if (existing.resume_digest !== params.resumePayloadDigest || existing.correlation_id !== data.correlationId) {
    throw new EntityConflictError('resumeId reused with different hook or payload');
  }
  return { event: toEvent(existing, params.resolveData), allocated: 0 };
}

function changeFor(tx: Tx, run: WorkflowRun, data: AnyEventRequest): Change {
  switch (data.eventType) {
    case EventType.RunStarted:
    case EventType.RunCompleted:
    case EventType.RunFailed:
    case EventType.RunCancelled:
    case EventType.AttrSet:
      return { kind: ChangeKind.Run, run: updateRun(run, data, tx.now) };
    case EventType.StepCreated:
    case EventType.StepStarted:
    case EventType.StepCompleted:
    case EventType.StepFailed:
    case EventType.StepRetrying:
      return changeStep(tx, run, data);
    case EventType.HookCreated:
    case EventType.HookReceived:
    case EventType.HookDisposed:
      return changeHook(tx, run, data);
    case EventType.WaitCreated:
    case EventType.WaitCompleted:
      return changeWait(tx, run, data);
    default:
      return { kind: ChangeKind.Run, run };
  }
}

function persist(tx: Tx, data: AnyEventRequest, event: Event, change: Change): void {
  const { db, outbox, runId, now } = tx;
  db.putRun(change.run);
  recordDispatch(tx, data, event, change.run);
  switch (change.kind) {
    case ChangeKind.Step:
    case ChangeKind.LazyStepStart:
      db.putStep(change.step);
      break;
    case ChangeKind.HookCreated:
      db.insertHook(change.hook);
      break;
    case ChangeKind.HookDisposed:
      db.deleteRunHook(runId, change.hookId);
      break;
    case ChangeKind.Wait:
      db.putWait(runId, change.correlationId, change.wait);
      break;
  }
  if (isTerminalWorkflowRunStatus(change.run.status)) {
    outbox.settleRun(runId);
    db.deleteWaits(runId);
    db.releaseHooks(runId, now);
  }
}

/** Records the queue message an event owes, or settles messages it makes unnecessary. */
function recordDispatch(tx: Tx, data: AnyEventRequest, event: Event, run: WorkflowRun): void {
  const { outbox, runId, now } = tx;
  const owe = (dueAt: Date) => {
    const prefix = getQueueTopicPrefix('workflow', getRunQueueNamespace(run.executionContext));
    const queueName = ValidQueueName.parse(`${prefix}${run.workflowName}`);
    outbox.add(runId, requireEventSlot(event.eventId), queueName, run.deploymentId, dueAt.getTime());
  };
  switch (data.eventType) {
    case EventType.RunCreated:
    case EventType.HookReceived:
    case EventType.StepCreated:
      return owe(now);
    case EventType.WaitCreated:
      return owe(data.eventData.resumeAt);
    case EventType.RunStarted:
      return outbox.publishThrough(runId, Number.MAX_SAFE_INTEGER, EventType.RunCreated);
    case EventType.StepStarted:
      return outbox.settle(runId, EventType.StepCreated, data.correlationId);
    case EventType.WaitCompleted:
      return outbox.settle(runId, EventType.WaitCreated, data.correlationId);
  }
}
