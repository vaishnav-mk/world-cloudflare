import { EntityConflictError, WorkflowRunNotFoundError, WorkflowWorldError } from '@workflow/errors';
import {
  getQueueTopicPrefix, getRunQueueNamespace, isTerminalWorkflowRunStatus, requireEventSlot, ValidQueueName,
  type AnyEventRequest, type CreateEventParams, type Event, type EventResult, type Hook, type WorkflowRun,
} from '@workflow/world';
import { ulid } from 'ulid';
import { ChangeKind, EventType, TokenClaim } from '../../enums.js';
import { isSingleOrchestrator, mintSpecVersion } from '../../lib/spec.js';
import type { Db } from '../db.js';
import type { IndexOutbox } from '../index-outbox.js';
import type { Outbox } from '../outbox.js';
import { toEvent } from '../rows.js';
import type { TokenOutbox } from '../token-outbox.js';
import { advanceFence, checkFence } from './fence.js';
import { changeHook } from './hooks.js';
import { appendEvent, eventResult, journalOf } from './journal.js';
import { checkRunAccepts, createRun, updateRun } from './runs.js';
import { changeStep } from './steps.js';
import type { Change, Tokens, Tx } from './types.js';
import { changeWait } from './waits.js';

/**
 * Validates and writes one event, parsed with `CreateEventSchema`, with every state change it
 * causes: the run, step, hook or wait, and any queue message, token release or list row it owes.
 * Call it inside `transactionSync`, so all of it commits together.
 */
export function applyEvent(
  db: Db, outbox: Outbox, releases: TokenOutbox, index: IndexOutbox, tokens: Tokens, requestedRunId: string | null,
  data: AnyEventRequest, params: CreateEventParams = {},
): EventResult {
  checkParams(data, params);
  const runId = requestedRunId ?? (data.eventType === EventType.RunCreated ? `wrun_${ulid()}` : null);
  if (runId === null) throw new Error('runId is required');
  const existing = db.findRun(runId);
  const specVersion = data.specVersion ?? existing?.specVersion ?? mintSpecVersion();
  // The fence goes first: a stale writer touches nothing and hears 412, never another refusal.
  const fence = checkFence(db, runId, existing, data, specVersion, params);
  const replayed = replayedResume(db, runId, data, params);
  if (replayed) return replayed;

  // An admitted start Hook candidate's repeated run_created gets its run, as admission promises.
  if (data.eventType === EventType.RunCreated && existing?.startHook !== undefined && data.eventData.startHook !== undefined) {
    return { run: existing, allocated: 0 };
  }

  const tx: Tx = { db, outbox, releases, index, tokens, runId, now: new Date(), specVersion };
  const created = createRun(tx, data, existing);
  const run = created?.run ?? existing;
  if (run === undefined) throw new WorkflowRunNotFoundError(runId);
  const noop = checkRunAccepts(db, run, data);
  if (noop) return noop;

  const change = changeFor(tx, run, data);
  if (change.kind === ChangeKind.HookConflict) {
    const conflict = appendEvent(db, runId, change.conflict, tx.now, { occurredAt: params.occurredAt });
    advanceFence(db, runId, fence, 1);
    return eventResult(db, conflict, change, params, 1);
  }
  if (created?.implied) appendEvent(db, runId, created.implied, tx.now);
  if (change.kind === ChangeKind.LazyStepStart) appendEvent(db, runId, change.implied, tx.now);
  const event = appendEvent(db, runId, { ...journalOf(data, change), specVersion }, tx.now, params);
  persist(tx, data, event, change);
  // An admitted start Hook is made with its run; no hook_created stands for it.
  if (created !== undefined && tokens.kind === TokenClaim.Granted) insertHook(tx, tokens.hook);
  // Only run events change the run; the others pass it on as it was.
  if (created !== undefined || change.run !== run) index.oweRun(runId);
  // A lazy step start also writes its implied step_created. A lazy run start's implied run_created is
  // the creation, which the fence counted before this write.
  const allocated = change.kind === ChangeKind.LazyStepStart ? 2 : 1;
  advanceFence(db, runId, fence, allocated);
  return eventResult(db, event, change, params, allocated);
}

function checkParams(data: AnyEventRequest, params: CreateEventParams): void {
  const resumesHook = data.eventType === EventType.HookReceived;
  if (!resumesHook && (params.resumeId || params.resumePayloadDigest)) {
    throw new Error('resumeId is valid only for hook_received');
  }
  if (resumesHook && params.resumeId && !params.resumePayloadDigest) {
    throw new Error('resumePayloadDigest is required with resumeId');
  }
  // A cached hook's resume: refused before anything is written unless it can be deduplicated.
  if (params.requireHookResumeDedup && !(resumesHook && params.resumeId)) {
    throw new WorkflowWorldError('A deduplicated hook resume needs a hook_received with a resumeId', { status: 400 });
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
  const { db, outbox, releases, runId, now } = tx;
  db.putRun(change.run);
  recordDispatch(tx, data, event, change.run);
  switch (change.kind) {
    case ChangeKind.Step:
    case ChangeKind.LazyStepStart:
    case ChangeKind.StepAppend:
      db.putStep(change.step);
      break;
    case ChangeKind.HookCreated: {
      const taken = db.findForcedDisposal(change.hook.hookId);
      if (taken !== undefined) {
        // Another run's forced claim took the token before this creation committed: it is disposed at once.
        appendEvent(db, runId, {
          eventType: EventType.HookDisposed, correlationId: change.hook.hookId, specVersion: tx.specVersion,
          eventData: { token: change.hook.token, forceClaimedBy: taken.claimedBy },
        }, now);
        break;
      }
      insertHook(tx, change.hook);
      break;
    }
    case ChangeKind.HookDisposed:
      db.deleteRunHook(runId, change.hook.hookId);
      releases.owe(change.hook, true);
      tx.index.oweHook(runId, change.hook.hookId);
      break;
    case ChangeKind.Wait:
    case ChangeKind.WaitAppend:
      db.putWait(runId, change.correlationId, change.wait);
      break;
  }
  if (isTerminalWorkflowRunStatus(change.run.status)) {
    outbox.settleRun(runId);
    db.deleteWaits(runId);
    for (const hook of db.releaseHooks(runId, now)) {
      releases.owe(hook, false);
      tx.index.oweHook(runId, hook.hookId);
    }
  }
}

/** Stores a hook whose token its shard granted, and owes the index its row. */
function insertHook({ db, index, runId }: Tx, hook: Hook): void {
  // A hook here that still has the token belongs to a finished run past its retention.
  for (const gone of db.deleteHookByToken(hook.token)) index.oweHook(gone.run_id, gone.id);
  db.insertHook(hook);
  index.oweHook(runId, hook.hookId);
}

/** Records the queue message an event owes, or settles messages it makes unnecessary. */
function recordDispatch(tx: Tx, data: AnyEventRequest, event: Event, run: WorkflowRun): void {
  const { outbox, runId, now } = tx;
  const owe = (dueAt: Date) => {
    const prefix = getQueueTopicPrefix('workflow', getRunQueueNamespace(run.executionContext));
    const queueName = ValidQueueName.parse(`${prefix}${run.workflowName}`);
    outbox.add(runId, requireEventSlot(event.eventId), queueName, run.deploymentId, dueAt.getTime());
  };
  // A spec 9 run's runtime re-sends its step messages and arms its timers itself, under its own keys.
  const appends = isSingleOrchestrator(run.specVersion);
  switch (data.eventType) {
    case EventType.RunCreated:
    case EventType.HookReceived:
      return owe(now);
    case EventType.StepCreated:
      return appends ? undefined : owe(now);
    case EventType.WaitCreated:
      return appends ? undefined : owe(data.eventData.resumeAt);
    case EventType.RunStarted:
      return outbox.publishThrough(runId, Number.MAX_SAFE_INTEGER, EventType.RunCreated);
    case EventType.StepStarted:
      return outbox.settle(runId, EventType.StepCreated, data.correlationId);
    case EventType.WaitCompleted:
      return outbox.settle(runId, EventType.WaitCreated, data.correlationId);
  }
}
