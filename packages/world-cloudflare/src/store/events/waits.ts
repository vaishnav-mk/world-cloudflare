import { EntityConflictError } from '@workflow/errors';
import type { Wait, WorkflowRun } from '@workflow/world';
import { ChangeKind, EventType, WaitStatus } from '../../enums.js';
import { isSingleOrchestrator } from '../../lib/spec.js';
import { unreachable } from '../../lib/unreachable.js';
import type { EventOf } from '../../types.js';
import type { Change, Tx, WaitEvent } from './types.js';

export function changeWait(tx: Tx, run: WorkflowRun, data: WaitEvent): Change {
  const { correlationId } = data;
  if (isSingleOrchestrator(run.specVersion)) {
    // Spec 9: an append. The wait record follows the events that fit it; nothing is refused.
    const wait = followWait(tx, tx.db.findWait(tx.runId, correlationId), data);
    return wait === undefined ? { kind: ChangeKind.Run, run } : { kind: ChangeKind.WaitAppend, run, correlationId, wait };
  }
  return { kind: ChangeKind.Wait, run, correlationId, wait: nextWait(tx, data) };
}

function newWait(tx: Tx, data: EventOf<EventType.WaitCreated>): Wait {
  return {
    runId: tx.runId, waitId: `${tx.runId}-${data.correlationId}`, status: WaitStatus.Waiting,
    resumeAt: data.eventData.resumeAt, createdAt: tx.now, updatedAt: tx.now, specVersion: tx.specVersion,
  };
}

function followWait(tx: Tx, wait: Wait | undefined, data: WaitEvent): Wait | undefined {
  switch (data.eventType) {
    case EventType.WaitCreated:
      return wait === undefined ? newWait(tx, data) : undefined;
    case EventType.WaitCompleted:
      return wait?.status === WaitStatus.Waiting ? { ...wait, status: WaitStatus.Completed, completedAt: tx.now, updatedAt: tx.now } : undefined;
    default:
      return unreachable(data);
  }
}

function nextWait(tx: Tx, data: WaitEvent): Wait {
  const id = data.correlationId;
  switch (data.eventType) {
    case EventType.WaitCreated:
      if (tx.db.findWait(tx.runId, id)) throw new EntityConflictError(`Wait ${id} already exists`);
      return newWait(tx, data);
    case EventType.WaitCompleted: {
      const wait = tx.db.getWait(tx.runId, id);
      if (wait.status === WaitStatus.Completed) throw new EntityConflictError(`Wait ${id} already completed`);
      return { ...wait, status: WaitStatus.Completed, completedAt: tx.now, updatedAt: tx.now };
    }
    default:
      return unreachable(data);
  }
}
