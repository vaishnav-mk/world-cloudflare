import { EntityConflictError } from '@workflow/errors';
import type { Wait } from '@workflow/world';
import { EventType, WaitStatus } from '../../enums.js';
import { unreachable } from '../../lib/unreachable.js';
import type { Tx, WaitEvent } from './types.js';

export function changeWait(tx: Tx, data: WaitEvent): Wait {
  const id = data.correlationId;
  const existing = tx.db.wait(tx.runId, id);
  switch (data.eventType) {
    case EventType.WaitCreated:
      if (existing) throw new EntityConflictError(`Wait ${id} already exists`);
      return {
        runId: tx.runId, waitId: `${tx.runId}-${id}`, status: WaitStatus.Waiting, resumeAt: data.eventData.resumeAt,
        createdAt: tx.now, updatedAt: tx.now, specVersion: tx.specVersion,
      };
    case EventType.WaitCompleted:
      if (!existing) throw new Error(`Wait ${id} not found`);
      if (existing.status === WaitStatus.Completed) throw new EntityConflictError(`Wait ${id} already completed`);
      return { ...existing, status: WaitStatus.Completed, completedAt: tx.now, updatedAt: tx.now };
    default:
      return unreachable(data);
  }
}
