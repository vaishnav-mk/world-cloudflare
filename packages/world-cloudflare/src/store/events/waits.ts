import { EntityConflictError } from '@workflow/errors';
import type { Wait, WorkflowRun } from '@workflow/world';
import { ChangeKind, EventType, WaitStatus } from '../../enums.js';
import { unreachable } from '../../lib/unreachable.js';
import type { Change, Tx, WaitEvent } from './types.js';

export function changeWait(tx: Tx, run: WorkflowRun, data: WaitEvent): Change {
  return { kind: ChangeKind.Wait, run, correlationId: data.correlationId, wait: nextWait(tx, data) };
}

function nextWait(tx: Tx, data: WaitEvent): Wait {
  const id = data.correlationId;
  switch (data.eventType) {
    case EventType.WaitCreated:
      if (tx.db.findWait(tx.runId, id)) throw new EntityConflictError(`Wait ${id} already exists`);
      return {
        runId: tx.runId, waitId: `${tx.runId}-${id}`, status: WaitStatus.Waiting, resumeAt: data.eventData.resumeAt,
        createdAt: tx.now, updatedAt: tx.now, specVersion: tx.specVersion,
      };
    case EventType.WaitCompleted: {
      const wait = tx.db.getWait(tx.runId, id);
      if (wait.status === WaitStatus.Completed) throw new EntityConflictError(`Wait ${id} already completed`);
      return { ...wait, status: WaitStatus.Completed, completedAt: tx.now, updatedAt: tx.now };
    }
    default:
      return unreachable(data);
  }
}
