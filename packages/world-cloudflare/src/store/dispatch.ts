import { isTerminalWorkflowRunStatus, type Step } from '@workflow/world';
import { stringify } from 'devalue';
import { MAX_PUBLISH_BACKOFF_MS, OUTBOX_DRAIN_BATCH, REDRIVE_DELAY_MS, WAIT_RETRY_DELAY_MS } from '../constants.js';
import { DispatchAction, EventType, MessageKind, RunStatus, StepStatus, WaitStatus } from '../enums.js';
import { fnv1a32Hex, messageIdFor, sha256Hex } from '../lib/hash.js';
import { unreachable } from '../lib/unreachable.js';
import type { QueueShard } from '../queue/queue-shard.js';
import type { DispatchKind, EnqueueResult, OutboxClaim, QueueDispatch } from '../types.js';
import type { StoreContext } from './context.js';
import type { Db } from './db.js';
import type { Outbox } from './outbox.js';
import type { OutboxRow } from './rows.js';

type Message = { key: string; body: unknown };
type ShardFor = (runId: string, payload: unknown, key: string) => DurableObjectStub<QueueShard>;

type Plan =
  | { action: DispatchAction.Settle }
  | { action: DispatchAction.Postpone }
  | {
    action: DispatchAction.Publish;
    message: Message;
    /** Rechecked after the hashing awaits, because other requests may run in between. */
    stillOwed: () => boolean;
  };

const SETTLE = { action: DispatchAction.Settle } as const;
const POSTPONE = { action: DispatchAction.Postpone } as const;

function isPendingStep(db: Db, runId: string, stepId: string, stepName: string): boolean {
  if (db.findRun(runId)?.status !== RunStatus.Running) return false;
  const step = db.findStep(runId, stepId);
  return step?.status === StepStatus.Pending && step.stepName === stepName;
}

/** Pending `step_created` slot for a step that still needs a delivery, or 0. */
function pendingStepSlot(db: Db, outbox: Outbox, runId: string, stepId: string, stepName: string): number {
  if (!isPendingStep(db, runId, stepId, stepName)) return 0;
  return outbox.pendingSlot(runId, EventType.StepCreated, stepId);
}

/** Pending `wait_created` slot for a wait that still needs a wake, or 0. */
function pendingWaitSlot(db: Db, outbox: Outbox, runId: string, correlationId: string): number {
  if (db.findRun(runId)?.status !== RunStatus.Running) return 0;
  if (db.findWait(runId, correlationId)?.status !== WaitStatus.Waiting) return 0;
  return outbox.pendingSlot(runId, EventType.WaitCreated, correlationId);
}

/**
 * The outbox slot a queue message would settle, or 0 where none is owed. Unlike other run-scoped
 * calls, a start is answered for a run this store does not hold yet: `start()` publishes the start
 * message while `run_created` is still in flight, and only new runs have one.
 */
export function pendingSlotFor(store: StoreContext, claim: OutboxClaim): number {
  const { db, outbox } = store;
  if (claim.kind !== MessageKind.Start) store.own(claim.runId);
  switch (claim.kind) {
    case MessageKind.Start:
      return outbox.pendingSlot(claim.runId, EventType.RunCreated);
    case MessageKind.HookWake:
      return outbox.pendingSlot(claim.runId, EventType.HookReceived);
    case MessageKind.Step:
      return pendingStepSlot(db, outbox, claim.runId, claim.stepId, claim.stepName);
    case MessageKind.WaitContinuation:
      return pendingWaitSlot(db, outbox, claim.runId, claim.correlationId);
    default:
      return unreachable(claim);
  }
}

/** Settles the outbox rows of a message the queue accepted: a start or hook wake every row up to `slot`, a step or wait its own. */
export function markPublished(store: StoreContext, kind: MessageKind, runId: string, slot: number): void {
  const { outbox } = store;
  if (kind !== MessageKind.Start) store.own(runId);
  switch (kind) {
    case MessageKind.Start:
      return outbox.publishThrough(runId, slot, EventType.RunCreated);
    case MessageKind.HookWake:
      return outbox.publishThrough(runId, slot, EventType.HookReceived);
    case MessageKind.Step:
      return outbox.publishSlot(runId, slot, EventType.StepCreated);
    case MessageKind.WaitContinuation:
      return outbox.publishSlot(runId, slot, EventType.WaitCreated);
    default:
      return unreachable(kind);
  }
}

/**
 * Publishes queue messages that settle this store's outbox rows: claims their slots, pushes them
 * to their queue shards, and settles the rows the queue accepted fresh, all in one call.
 */
export async function dispatch(store: StoreContext, entries: QueueDispatch[]): Promise<EnqueueResult[]> {
  const slots = entries.map(({ claim }) => pendingSlotFor(store, claim));
  const results = await store.pushQueue(entries);
  for (const [index, { claim }] of entries.entries()) {
    const slot = slots[index];
    const result = results[index];
    if (slot && 'fresh' in result && result.fresh) markPublished(store, claim.kind, claim.runId, slot);
  }
  return results;
}

/** After a run's hook wake dead-lettered: reopens its start if it never started, else its hook wakes. */
export async function reopenHookWakes(store: StoreContext, runId: string): Promise<number> {
  store.own(runId);
  const run = store.db.findRun(runId);
  if (run === undefined || isTerminalWorkflowRunStatus(run.status)) return 0;
  const kind = run.status === RunStatus.Pending ? EventType.RunCreated : EventType.HookReceived;
  return reopen(store, runId, Date.now() + REDRIVE_DELAY_MS, kind);
}

export async function reopenStepDispatch(store: StoreContext, runId: string, stepId: string, stepName: string): Promise<number> {
  store.own(runId);
  if (!isPendingStep(store.db, runId, stepId, stepName)) return 0;
  return reopen(store, runId, Date.now() + REDRIVE_DELAY_MS, EventType.StepCreated, stepId);
}

export async function reopenWaitDispatch(store: StoreContext, runId: string, correlationId: string): Promise<number> {
  store.own(runId);
  const wait = store.db.findWait(runId, correlationId);
  if (store.db.findRun(runId)?.status !== RunStatus.Running || wait?.status !== WaitStatus.Waiting) return 0;
  const at = Math.max(Date.now() + REDRIVE_DELAY_MS, wait.resumeAt?.getTime() ?? 0);
  return reopen(store, runId, at, EventType.WaitCreated, correlationId);
}

async function reopen(store: StoreContext, runId: string, at: number, kind: DispatchKind, correlationId?: string): Promise<number> {
  const reopened = store.outbox.reopen(runId, at, kind, correlationId);
  if (reopened) await store.armAlarm(at, 'redrive');
  return reopened;
}

/** Publishes due outbox rows. Returns how many rows were due. */
export async function drainOutbox(db: Db, outbox: Outbox, shardFor: ShardFor): Promise<number> {
  const due = outbox.due(Date.now(), OUTBOX_DRAIN_BATCH);
  for (const row of due) {
    const plan = planFor(db, outbox, row);
    switch (plan.action) {
      case DispatchAction.Settle:
        outbox.publishRow(row);
        break;
      case DispatchAction.Postpone:
        outbox.postpone(row, Date.now() + WAIT_RETRY_DELAY_MS);
        break;
      case DispatchAction.Publish:
        await publish(outbox, shardFor, row, plan.message, plan.stillOwed);
        break;
    }
  }
  return due.length;
}

/**
 * Decides what a due row still needs. A wait row is due no earlier than its `resumeAt`: it is
 * written with that time, and every reschedule only moves it later.
 */
function planFor(db: Db, outbox: Outbox, row: OutboxRow): Plan {
  const run = db.findRun(row.run_id);
  const runStatus = run?.status;
  switch (row.kind) {
    case EventType.RunCreated:
      if (runStatus !== RunStatus.Pending) return SETTLE;
      return { action: DispatchAction.Publish, message: wakeMessage(row, 'run-start', run?.specVersion), stillOwed: () => true };
    case EventType.HookReceived:
      return { action: DispatchAction.Publish, message: wakeMessage(row, 'hook-wake', run?.specVersion), stillOwed: () => true };
    case EventType.StepCreated: {
      const step = db.findStep(row.run_id, row.correlation_id);
      if (step?.status !== StepStatus.Pending || runStatus !== RunStatus.Running) return SETTLE;
      return {
        action: DispatchAction.Publish,
        message: stepMessage(row, step),
        stillOwed: () => pendingStepSlot(db, outbox, row.run_id, step.stepId, step.stepName) === row.slot,
      };
    }
    case EventType.WaitCreated: {
      const wait = db.findWait(row.run_id, row.correlation_id);
      if (wait?.status !== WaitStatus.Waiting || runStatus === undefined || isTerminalWorkflowRunStatus(runStatus)) {
        return SETTLE;
      }
      if (runStatus !== RunStatus.Running) return POSTPONE;
      return {
        action: DispatchAction.Publish,
        message: waitMessage(row, row.correlation_id),
        stillOwed: () => pendingWaitSlot(db, outbox, row.run_id, row.correlation_id) === row.slot,
      };
    }
  }
}

async function publish(
  outbox: Outbox, shardFor: ShardFor, row: OutboxRow, message: Message, stillOwed: () => boolean,
): Promise<void> {
  // A dead-lettered key is spent forever, so each redrive needs a new one.
  const key = row.redrive ? `${message.key}:redrive-${row.redrive}` : message.key;
  const body = stringify(message.body);
  const [id, digest] = await Promise.all([messageIdFor(key), sha256Hex(body)]);
  if (!stillOwed()) return;
  try {
    // The shard the SDK's own publish of this message reaches, which holds its key's deduplication.
    await shardFor(row.run_id, message.body, key).enqueue(id, row.queue_name, body, 0, row.deployment_id, key, digest);
    outbox.publishRow(row);
  } catch (error) {
    console.error('Workflow dispatch recovery failed', { runId: row.run_id, slot: row.slot, error });
    const delay = Math.min(MAX_PUBLISH_BACKOFF_MS, (row.attempts + 1) ** 2 * 1_000);
    outbox.recordFailure(row, Date.now() + delay);
  }
}

/** Matches the SDK's own step key, so an SDK publish and ours deduplicate. */
function stepMessage(row: OutboxRow, step: Step): Message {
  return {
    key: `${row.queue_name}:${step.stepId}:${fnv1a32Hex(step.stepName)}`,
    body: { runId: row.run_id, stepId: step.stepId, stepName: step.stepName },
  };
}

function waitMessage(row: OutboxRow, correlationId: string): Message {
  return {
    key: `${row.queue_name}:wait-backup:${row.run_id}:${row.slot}`,
    body: { runId: row.run_id, waitContinuation: { correlationId, attempt: row.redrive } },
  };
}

/** Stamped with the run's spec version, as the SDK stamps its own wakes, so the queue routes it by the run's model. */
function wakeMessage(row: OutboxRow, prefix: 'run-start' | 'hook-wake', runSpecVersion: number | undefined): Message {
  return {
    key: `${prefix}:${row.run_id}:${row.slot}`,
    body: { runId: row.run_id, ...(runSpecVersion === undefined ? {} : { runSpecVersion }) },
  };
}
