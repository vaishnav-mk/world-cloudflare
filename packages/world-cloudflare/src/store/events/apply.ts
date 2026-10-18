import { EntityConflictError } from '@workflow/errors';
import {
  CreateEventSchema, getQueueTopicPrefix, getRunQueueNamespace, isTerminalWorkflowRunStatus, mintedSpecVersion,
  requireEventSlot, ValidQueueName,
  type AnyEventRequest, type CreateEventParams, type Event, type EventResult, type WorkflowRun,
} from '@workflow/world';
import { ulid } from 'ulid';
import { EventType } from '../../enums.js';
import type { Db } from '../db.js';
import type { Outbox } from '../outbox.js';
import { toEvent } from '../rows.js';
import { changeHook } from './hooks.js';
import { appendEvent, eventResult, journalOf } from './journal.js';
import { checkRunAccepts, createRun, requireRun, updateRun } from './runs.js';
import { changeStep } from './steps.js';
import type { Change, Tx } from './types.js';
import { changeWait } from './waits.js';

/**
 * Validates and writes one event with every state change it causes: the run, step, hook or wait,
 * and any queue message it owes. Call it inside `transactionSync`, so all of it commits together.
 */
export function applyEvent(
  db: Db, outbox: Outbox, requestedRunId: string | null, request: AnyEventRequest, params?: CreateEventParams,
): EventResult {
  const data = CreateEventSchema.parse(request);
  checkParams(data, params);
  const runId = requestedRunId ?? (data.eventType === EventType.RunCreated ? `wrun_${ulid()}` : null);
  if (!runId) throw new Error('runId is required');
  const replayed = replayedResume(db, runId, data, params);
  if (replayed) return replayed;

  const existing = db.run(runId);
  const specVersion = data.specVersion ?? existing?.specVersion ?? mintedSpecVersion();
  const tx: Tx = { db, outbox, runId, now: new Date(), specVersion };
  const created = createRun(tx, data, existing);
  const run = requireRun(created?.run ?? existing, runId);
  const noop = checkRunAccepts(db, run, data);
  if (noop) return noop;

  const change = changeFor(tx, run, data);
  if (change.hookConflict) {
    return eventResult(db, appendEvent(db, runId, change.hookConflict, tx.now), change, params);
  }
  for (const implied of [created?.implied, change.implied]) {
    if (implied) appendEvent(db, runId, implied, tx.now);
  }
  const journaled = { ...journalOf(data, change.stepCreated === true), specVersion };
  const event = appendEvent(db, runId, journaled, tx.now, params);
  persist(tx, data, event, change);
  return eventResult(db, event, change, params);
}

function checkParams(data: AnyEventRequest, params?: CreateEventParams): void {
  if (params?.inBand || params?.expectedSeqInBand !== undefined) {
    throw new Error('In-band writer fencing is not supported');
  }
  const resumesHook = data.eventType === EventType.HookReceived;
  if (!resumesHook && (params?.resumeId || params?.resumePayloadDigest)) {
    throw new Error('resumeId is valid only for hook_received');
  }
  if (resumesHook && params?.resumeId && !params.resumePayloadDigest) {
    throw new Error('resumePayloadDigest is required with resumeId');
  }
}

/** A `hook_received` retried with the same resume ID returns the event it already wrote. */
function replayedResume(db: Db, runId: string, data: AnyEventRequest, params?: CreateEventParams): EventResult | undefined {
  if (data.eventType !== EventType.HookReceived || !params?.resumeId) return undefined;
  const existing = db.eventByResumeId(runId, params.resumeId);
  if (!existing) return undefined;
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
      return { run: updateRun(run, data, tx.now) };
    case EventType.StepCreated:
    case EventType.StepStarted:
    case EventType.StepCompleted:
    case EventType.StepFailed:
    case EventType.StepRetrying:
      return { run, ...changeStep(tx, data) };
    case EventType.HookCreated:
    case EventType.HookReceived:
    case EventType.HookDisposed:
      return { run, ...changeHook(tx, run, data) };
    case EventType.WaitCreated:
    case EventType.WaitCompleted:
      return { run, wait: changeWait(tx, data) };
    default:
      return { run };
  }
}

function persist(tx: Tx, data: AnyEventRequest, event: Event, change: Change): void {
  const { db, outbox, runId, now } = tx;
  db.putRun(change.run);
  recordDispatch(tx, data, event, change);
  if (change.step) db.putStep(change.step);
  if (change.createdHook) db.insertHook(change.createdHook);
  switch (data.eventType) {
    case EventType.HookDisposed:
      db.deleteRunHook(runId, data.correlationId);
      break;
    case EventType.WaitCreated:
    case EventType.WaitCompleted:
      db.putWait(runId, data.correlationId, change.wait!);
      break;
  }
  if (isTerminalWorkflowRunStatus(change.run.status)) {
    outbox.settleRun(runId);
    db.deleteWaits(runId);
    db.releaseHooks(runId, now);
  }
}

/** Records the queue message an event owes, or settles messages it makes unnecessary. */
function recordDispatch(tx: Tx, data: AnyEventRequest, event: Event, change: Change): void {
  const { outbox, runId, now } = tx;
  switch (data.eventType) {
    case EventType.RunCreated:
    case EventType.HookReceived:
    case EventType.StepCreated:
    case EventType.WaitCreated: {
      const { executionContext, workflowName, deploymentId } = change.run;
      const prefix = getQueueTopicPrefix('workflow', getRunQueueNamespace(executionContext));
      const queueName = ValidQueueName.parse(`${prefix}${workflowName}`);
      const dueAt = data.eventType === EventType.WaitCreated ? change.wait!.resumeAt!.getTime() : now.getTime();
      return outbox.add(runId, requireEventSlot(event.eventId), queueName, deploymentId, dueAt);
    }
    case EventType.RunStarted:
      return outbox.publishThrough(runId, Number.MAX_SAFE_INTEGER, EventType.RunCreated);
    case EventType.StepStarted:
      return outbox.settle(runId, EventType.StepCreated, data.correlationId);
    case EventType.WaitCompleted:
      return outbox.settle(runId, EventType.WaitCreated, data.correlationId);
  }
}
