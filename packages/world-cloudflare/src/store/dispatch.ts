import { isTerminalWorkflowRunStatus, ROOT_RUN_ID_ATTRIBUTE, SPEC_VERSION_LEGACY, type Step, type WorkflowRun } from '@workflow/world';
import { stringify } from 'devalue';
import { MAX_PUBLISH_BACKOFF_MS, OUTBOX_DRAIN_BATCH, REDRIVE_DELAY_MS, WAIT_RETRY_DELAY_MS } from '../constants.js';
import { DispatchAction, EventType, MessageKind, RunStatus, StepStatus, WaitStatus } from '../enums.js';
import { fnv1a32Hex, messageIdFor, sha256Hex } from '../lib/hash.js';
import { queueShardFor } from '../lib/shards.js';
import { unreachable } from '../lib/unreachable.js';
import type { DispatchKind, EnqueueResult, OutboxClaim, QueueDispatch } from '../types.js';
import type { StoreContext } from './context.js';
import type { Db } from './db.js';
import type { Outbox } from './outbox.js';
import type { OutboxRow } from './rows.js';

/**
 * The dispatch outbox: the queue messages a store owes for committed events (`Outbox`). The queue
 * client claims and settles rows as it publishes, the alarm publishes the rows still due, and a
 * dead-lettered message reopens its row.
 */

type Message = { key: string; body: unknown };

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

/** Settles the outbox rows of a message the queue accepted (`Outbox.publish`). */
export function markPublished(store: StoreContext, kind: MessageKind, runId: string, slot: number): void {
  const { outbox } = store;
  if (kind !== MessageKind.Start) store.own(runId);
  switch (kind) {
    case MessageKind.Start:
      return outbox.publish(runId, slot, EventType.RunCreated);
    case MessageKind.HookWake:
      return outbox.publish(runId, slot, EventType.HookReceived);
    case MessageKind.Step:
      return outbox.publish(runId, slot, EventType.StepCreated);
    case MessageKind.WaitContinuation:
      return outbox.publish(runId, slot, EventType.WaitCreated);
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
  // A start is its run's first delivery.
  await Promise.all(entries.flatMap(({ claim }) => (claim.kind === MessageKind.Start ? [store.placeRunner(claim.runId)] : [])));
  const results = await store.pushQueue(entries);
  for (const [index, { claim }] of entries.entries()) {
    const slot = slots[index];
    const result = results[index];
    if (slot && 'fresh' in result && result.fresh) markPublished(store, claim.kind, claim.runId, slot);
  }
  return results;
}

/**
 * The run's steps whose message the queue has, or no longer needs: an SDK publish of one under its
 * dispatch key changes nothing, so the queue client answers it itself (`createQueue`).
 */
export function dispatchedSteps(store: StoreContext, runId: string): string[] {
  store.own(runId);
  return store.outbox.settledSteps(runId);
}

/** After a run's hook wake dead-lettered: reopens its start if it never started, else its hook wakes. */
export async function reopenHookWakes(store: StoreContext, runId: string): Promise<number> {
  store.own(runId);
  const run = store.db.findRun(runId);
  if (run === undefined || isTerminalWorkflowRunStatus(run.status)) return 0;
  const kind = run.status === RunStatus.Pending ? EventType.RunCreated : EventType.HookReceived;
  return reopen(store, runId, Date.now() + REDRIVE_DELAY_MS, kind);
}

/** After a step's message dead-lettered: reopens its row while the step still waits for a delivery. */
export async function reopenStepDispatch(store: StoreContext, runId: string, stepId: string, stepName: string): Promise<number> {
  store.own(runId);
  if (!isPendingStep(store.db, runId, stepId, stepName)) return 0;
  return reopen(store, runId, Date.now() + REDRIVE_DELAY_MS, EventType.StepCreated, stepId);
}

/** After a wait's wake dead-lettered: reopens its row, due no earlier than the wait's `resumeAt`. */
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

/**
 * Publishes due outbox rows together, one push per queue shard: a replay that created thousands of
 * steps and ended before it published them leaves that many due. Returns how many rows were due.
 */
export async function drainOutbox(store: StoreContext): Promise<number> {
  const { db, outbox } = store;
  const due = outbox.due(Date.now(), OUTBOX_DRAIN_BATCH);
  const sends: Send[] = [];
  for (const row of due) {
    const plan = planFor(db, outbox, row);
    switch (plan.action) {
      case DispatchAction.Settle:
        outbox.publish(row.run_id, row.slot, row.kind);
        break;
      case DispatchAction.Postpone:
        outbox.postpone(row, Date.now() + WAIT_RETRY_DELAY_MS);
        break;
      case DispatchAction.Publish:
        if (row.kind === EventType.RunCreated) await store.placeRunner(row.run_id);
        sends.push({ row, message: plan.message, stillOwed: plan.stillOwed });
        break;
    }
  }
  await publish(store, sends);
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
      if (step?.status !== StepStatus.Pending || run?.status !== RunStatus.Running) return SETTLE;
      return {
        action: DispatchAction.Publish,
        message: stepMessage(row, step, run),
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

type Send = { row: OutboxRow; message: Message; stillOwed: () => boolean };

async function publish(store: StoreContext, sends: Send[]): Promise<void> {
  const prepared = await Promise.all(sends.map(async ({ row, message, stillOwed }) => {
    // A dead-lettered key is spent forever, so each redrive needs a new one.
    const key = row.redrive ? `${message.key}:redrive-${row.redrive}` : message.key;
    const encoded = stringify(message.body);
    const [id, digest] = await Promise.all([messageIdFor(key), sha256Hex(encoded)]);
    return {
      row, stillOwed,
      // The shard the SDK's own publish of this message reaches, which holds its key's deduplication.
      shard: queueShardFor(row.run_id, message.body, key),
      request: { id, queueName: row.queue_name, encoded, delayMs: 0, targetDeployment: row.deployment_id, deduplicationKey: key, payloadDigest: digest },
    };
  }));
  const owed = prepared.filter(({ stillOwed }) => stillOwed());
  const results = await store.pushQueue(owed).catch((error: unknown) => owed.map((): EnqueueResult => ({ error: String(error) })));
  for (const [index, { row }] of owed.entries()) {
    const result = results[index];
    if ('id' in result) {
      store.outbox.publish(row.run_id, row.slot, row.kind);
      continue;
    }
    console.error('Workflow dispatch recovery failed', { runId: row.run_id, slot: row.slot, error: result.error });
    const delay = Math.min(MAX_PUBLISH_BACKOFF_MS, (row.attempts + 1) ** 2 * 1_000);
    store.outbox.recordFailure(row, Date.now() + delay);
  }
}

/**
 * Matches the SDK's own step key, so an SDK publish and ours deduplicate, and carries the run's
 * identity as the SDK's does, so the step starts without reading the run.
 */
function stepMessage(row: OutboxRow, step: Step, run: WorkflowRun): Message {
  const runContext = {
    deploymentId: run.deploymentId,
    specVersion: run.specVersion ?? SPEC_VERSION_LEGACY,
    ...(run.startedAt === undefined ? {} : { startedAt: run.startedAt.getTime() }),
    rootRunId: run.attributes?.[ROOT_RUN_ID_ATTRIBUTE] ?? run.runId,
  };
  return {
    key: `${row.queue_name}:${step.stepId}:${fnv1a32Hex(step.stepName)}`,
    body: { runId: row.run_id, stepId: step.stepId, stepName: step.stepName, runContext },
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
