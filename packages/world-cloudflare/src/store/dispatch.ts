import { isTerminalWorkflowRunStatus, type Step, type Wait } from '@workflow/world';
import { stringify } from 'devalue';
import { MAX_PUBLISH_BACKOFF_MS, OUTBOX_DRAIN_BATCH, WAIT_RETRY_DELAY_MS } from '../constants.js';
import { DispatchAction, EventType, RunStatus, StepStatus, WaitStatus } from '../enums.js';
import { fnv1a32Hex, messageIdFor, sha256Hex } from '../lib/hash.js';
import type { QueueShard } from '../queue/queue-shard.js';
import type { Db } from './db.js';
import type { Outbox } from './outbox.js';
import type { OutboxRow } from './rows.js';

type Plan =
  | { action: DispatchAction.Settle }
  | { action: DispatchAction.Postpone }
  | { action: DispatchAction.Publish; step?: Step; wait?: Wait };

export function isPendingStep(db: Db, runId: string, stepId: string, stepName: string): boolean {
  if (db.run(runId)?.status !== RunStatus.Running) return false;
  const step = db.step(runId, stepId);
  return step?.status === StepStatus.Pending && step.stepName === stepName;
}

/** Pending `step_created` slot for a step that still needs a delivery, or 0. */
export function pendingStepSlot(db: Db, outbox: Outbox, runId: string, stepId: string, stepName: string): number {
  if (!isPendingStep(db, runId, stepId, stepName)) return 0;
  return outbox.pendingSlot(runId, EventType.StepCreated, stepId);
}

/** Pending `wait_created` slot for a wait that still needs a wake, or 0. */
export function pendingWaitSlot(db: Db, outbox: Outbox, runId: string, correlationId: string): number {
  if (db.run(runId)?.status !== RunStatus.Running) return 0;
  if (db.wait(runId, correlationId)?.status !== WaitStatus.Waiting) return 0;
  return outbox.pendingSlot(runId, EventType.WaitCreated, correlationId);
}

/** Publishes due outbox rows. Returns how many rows were due. */
export async function drainOutbox(db: Db, outbox: Outbox, shard: DurableObjectStub<QueueShard>): Promise<number> {
  const due = outbox.due(Date.now(), OUTBOX_DRAIN_BATCH);
  for (const row of due) {
    const plan = planFor(db, row);
    switch (plan.action) {
      case DispatchAction.Settle:
        outbox.publishRow(row);
        break;
      case DispatchAction.Postpone:
        outbox.postpone(row, Date.now() + WAIT_RETRY_DELAY_MS);
        break;
      case DispatchAction.Publish:
        await publish(db, outbox, shard, row, plan.step, plan.wait);
        break;
    }
  }
  return due.length;
}

/** Decides what a due outbox row still needs. */
function planFor(db: Db, row: OutboxRow): Plan {
  const settle = { action: DispatchAction.Settle } as const;
  switch (row.kind) {
    case EventType.RunCreated:
      return db.run(row.run_id)?.status === RunStatus.Pending ? { action: DispatchAction.Publish } : settle;
    case EventType.HookReceived:
      return { action: DispatchAction.Publish };
    case EventType.StepCreated: {
      const step = row.correlation_id ? db.step(row.run_id, row.correlation_id) : undefined;
      const needed = step?.status === StepStatus.Pending && db.run(row.run_id)?.status === RunStatus.Running;
      return needed ? { action: DispatchAction.Publish, step } : settle;
    }
    case EventType.WaitCreated: {
      const wait = row.correlation_id ? db.wait(row.run_id, row.correlation_id) : undefined;
      const status = db.run(row.run_id)?.status;
      if (wait?.status !== WaitStatus.Waiting || !status || isTerminalWorkflowRunStatus(status)) return settle;
      return status === RunStatus.Running ? { action: DispatchAction.Publish, wait } : { action: DispatchAction.Postpone };
    }
  }
}

async function publish(
  db: Db, outbox: Outbox, shard: DurableObjectStub<QueueShard>, row: OutboxRow, step?: Step, wait?: Wait,
): Promise<void> {
  const { key, body } = messageFor(row, step);
  const [id, digest] = await Promise.all([messageIdFor(key), sha256Hex(body)]);
  // Other requests may have run during the awaits.
  if (!stillOwed(db, outbox, row, step, wait)) return;
  try {
    await shard.enqueue(id, row.queue_name, body, 0, row.deployment_id, key, digest);
    outbox.publishRow(row);
  } catch (error) {
    console.error('Workflow dispatch recovery failed', { runId: row.run_id, slot: row.slot, error });
    const delay = Math.min(MAX_PUBLISH_BACKOFF_MS, (row.attempts + 1) ** 2 * 1_000);
    outbox.recordFailure(row, Date.now() + delay);
  }
}

/** The queue message for a row. Step keys match the SDK's own, so an SDK publish and ours dedupe. */
function messageFor(row: OutboxRow, step?: Step): { key: string; body: string } {
  const { run_id: runId, queue_name: queueName, slot } = row;
  let key: string;
  let body: unknown;
  switch (row.kind) {
    case EventType.StepCreated:
      key = `${queueName}:${step!.stepId}:${fnv1a32Hex(step!.stepName)}`;
      body = { runId, stepId: step!.stepId, stepName: step!.stepName };
      break;
    case EventType.WaitCreated:
      key = `${queueName}:wait-backup:${runId}:${slot}`;
      body = { runId, waitContinuation: { correlationId: row.correlation_id, attempt: row.redrive } };
      break;
    case EventType.HookReceived:
      key = `hook-wake:${runId}:${slot}`;
      body = { runId };
      break;
    case EventType.RunCreated:
      key = `run-start:${runId}:${slot}`;
      body = { runId };
      break;
  }
  // A dead-lettered key is spent forever, so each redrive needs a new one.
  return { key: row.redrive ? `${key}:redrive-${row.redrive}` : key, body: stringify(body) };
}

function stillOwed(db: Db, outbox: Outbox, row: OutboxRow, step?: Step, wait?: Wait): boolean {
  if (step) return pendingStepSlot(db, outbox, row.run_id, step.stepId, step.stepName) === row.slot;
  if (!wait) return true;
  const due = Date.now() >= wait.resumeAt!.getTime();
  return due && pendingWaitSlot(db, outbox, row.run_id, row.correlation_id!) === row.slot;
}
