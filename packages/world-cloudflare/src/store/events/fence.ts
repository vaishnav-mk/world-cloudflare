import { InBandSupersededError, WorkflowWorldError } from '@workflow/errors';
import { IN_BAND_SEQ_AT_RUN_CREATION, type AnyEventRequest, type CreateEventParams, type WorkflowRun } from '@workflow/world';
import { EventType } from '../../enums.js';
import { isSingleOrchestrator } from '../../lib/spec.js';
import type { Db } from '../db.js';

/** A spec 9 run's in-band count before a write, and whether the write is in-band. */
type Fence = { count: number; inBand: boolean; creates: boolean };

/**
 * The in-band fence of spec 9 runs. An in-band write must name the run's current in-band count,
 * or it is refused with `InBandSupersededError` before anything is written. `run_created` counts as
 * the first in-band position and is never fenced itself; a lazy `run_started` that creates the run
 * is checked against that position. Undefined for a run without the fence: below spec 9, or missing.
 */
export function checkFence(
  db: Db, runId: string, existing: WorkflowRun | undefined, data: AnyEventRequest, specVersion: number,
  params: CreateEventParams,
): Fence | undefined {
  const inBand = params.inBand === true;
  let fence: Fence | undefined;
  if (existing !== undefined) {
    if (data.eventType === EventType.RunCreated) return undefined;
    // Only a spec 9 run has a count, so an earlier run's writes skip the read.
    const count = isSingleOrchestrator(existing.specVersion) ? db.seqInBand(runId) : null;
    fence = count === null ? undefined : { count, inBand, creates: false };
  } else if (!isSingleOrchestrator(specVersion)) {
    return undefined;
  } else if (data.eventType === EventType.RunCreated) {
    return { count: 0, inBand: true, creates: true };
  } else if (data.eventType === EventType.RunStarted) {
    fence = { count: IN_BAND_SEQ_AT_RUN_CREATION, inBand, creates: true };
  } else {
    // A missing run: the write fails as not found.
    return undefined;
  }
  if (!inBand) return fence;
  if (params.expectedSeqInBand === undefined) {
    throw new WorkflowWorldError('An in-band write needs expectedSeqInBand', { status: 400 });
  }
  if (fence === undefined) throw new WorkflowWorldError(`Run ${runId} has no in-band fence`, { status: 400 });
  if (params.expectedSeqInBand !== fence.count) {
    throw new InBandSupersededError(`Run ${runId} has another in-band writer`, { seq: db.lastSlot(runId), seqInBand: fence.count });
  }
  return fence;
}

/** Counts the positions an in-band write allocated, and stores a new run's count. */
export function advanceFence(db: Db, runId: string, fence: Fence | undefined, allocated: number): void {
  if (fence === undefined || (!fence.inBand && !fence.creates)) return;
  db.setSeqInBand(runId, fence.count + (fence.inBand ? allocated : 0));
}
