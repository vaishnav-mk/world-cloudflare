import { isTerminalWorkflowRunStatus, type Step } from '@workflow/world';
import { stringify } from 'devalue';
import { MAX_PUBLISH_BACKOFF_MS, OUTBOX_DRAIN_BATCH, WAIT_RETRY_DELAY_MS } from '../constants.js';
import { DispatchAction, EventType, RunStatus, StepStatus, WaitStatus } from '../enums.js';
import { fnv1a32Hex, messageIdFor, sha256Hex } from '../lib/hash.js';
import type { QueueShard } from '../queue/queue-shard.js';
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

export function isPendingStep(db: Db, runId: string, stepId: string, stepName: string): boolean {
  if (db.findRun(runId)?.status !== RunStatus.Running) return false;
  const step = db.findStep(runId, stepId);
  return step?.status === StepStatus.Pending && step.stepName === stepName;
}

/** Pending `step_created` slot for a step that still needs a delivery, or 0. */
export function pendingStepSlot(db: Db, outbox: Outbox, runId: string, stepId: string, stepName: string): number {
  if (!isPendingStep(db, runId, stepId, stepName)) return 0;
  return outbox.pendingSlot(runId, EventType.StepCreated, stepId);
}

/** Pending `wait_created` slot for a wait that still needs a wake, or 0. */
export function pendingWaitSlot(db: Db, outbox: Outbox, runId: string, correlationId: string): number {
  if (db.findRun(runId)?.status !== RunStatus.Running) return 0;
  if (db.findWait(runId, correlationId)?.status !== WaitStatus.Waiting) return 0;
  return outbox.pendingSlot(runId, EventType.WaitCreated, correlationId);
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
  const runStatus = db.findRun(row.run_id)?.status;
  switch (row.kind) {
    case EventType.RunCreated:
      if (runStatus !== RunStatus.Pending) return SETTLE;
      return { action: DispatchAction.Publish, message: wakeMessage(row, 'run-start'), stillOwed: () => true };
    case EventType.HookReceived:
      return { action: DispatchAction.Publish, message: wakeMessage(row, 'hook-wake'), stillOwed: () => true };
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

function wakeMessage(row: OutboxRow, prefix: 'run-start' | 'hook-wake'): Message {
  return { key: `${prefix}:${row.run_id}:${row.slot}`, body: { runId: row.run_id } };
}
