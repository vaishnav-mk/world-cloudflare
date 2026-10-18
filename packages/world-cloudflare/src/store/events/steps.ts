import { EntityConflictError } from '@workflow/errors';
import type { Step } from '@workflow/world';
import { EventType, StepStatus } from '../../enums.js';
import { unreachable } from '../../lib/unreachable.js';
import type { EventOf } from '../../types.js';
import type { Change, StepEvent, Tx } from './types.js';

type StepChange = Pick<Change, 'step' | 'implied' | 'stepCreated'>;
type StepUpdateEvent = Exclude<StepEvent, EventOf<EventType.StepCreated>>;

/**
 * Applies a step event. `step_created` makes a pending step. A lazy `step_started` carries the
 * input itself: it creates the step and starts it in one write, implying a `step_created`.
 */
export function changeStep(tx: Tx, data: StepEvent): StepChange {
  switch (data.eventType) {
    case EventType.StepCreated:
      return { step: createStep(tx, data) };
    case EventType.StepStarted:
      if (data.eventData?.input !== undefined) return startLazily(tx, data);
      break;
  }
  return { step: advanceStep(existingStep(tx, data.correlationId), data, tx.now) };
}

function startLazily(tx: Tx, data: EventOf<EventType.StepStarted>): StepChange {
  const step = createStep(tx, data);
  const implied: EventOf<EventType.StepCreated> = {
    eventType: EventType.StepCreated, correlationId: step.stepId, specVersion: tx.specVersion,
    eventData: { stepName: step.stepName, input: step.input! },
  };
  return { step: advanceStep(step, data, tx.now), implied, stepCreated: true };
}

function createStep(tx: Tx, data: EventOf<EventType.StepCreated | EventType.StepStarted>): Step {
  const id = data.correlationId;
  if (tx.db.step(tx.runId, id)) throw new EntityConflictError(`Step ${id} already created`);
  const stepName = data.eventData?.stepName;
  if (!stepName) throw new Error('Lazy step_started requires stepName');
  return {
    runId: tx.runId, stepId: id, stepName, status: StepStatus.Pending, input: data.eventData!.input, attempt: 0,
    specVersion: tx.specVersion, createdAt: tx.now, updatedAt: tx.now,
  };
}

function existingStep(tx: Tx, id: string): Step {
  const step = tx.db.step(tx.runId, id);
  if (!step) throw new Error(`Step ${id} not found`);
  return step;
}

function advanceStep(step: Step, data: StepUpdateEvent, now: Date): Step {
  const id = step.stepId;
  switch (step.status) {
    case StepStatus.Completed:
    case StepStatus.Failed:
    case StepStatus.Cancelled:
      throw new EntityConflictError(`Step ${id} is terminal`);
  }
  if (data.eventType === EventType.StepStarted) {
    if (step.retryAfter && step.retryAfter.getTime() > now.getTime()) {
      throw new Error(`Step ${id} retryAfter has not elapsed`);
    }
    return { ...step, status: StepStatus.Running, attempt: step.attempt + 1, startedAt: step.startedAt ?? now,
      retryAfter: undefined, updatedAt: now };
  }
  if (step.status !== StepStatus.Running) throw new EntityConflictError(`Step ${id} is not running`);
  switch (data.eventType) {
    case EventType.StepCompleted:
      return { ...step, status: StepStatus.Completed, output: data.eventData.result, completedAt: now, updatedAt: now };
    case EventType.StepFailed:
      return { ...step, status: StepStatus.Failed, error: data.eventData.error, completedAt: now, updatedAt: now };
    case EventType.StepRetrying:
      return { ...step, status: StepStatus.Pending, error: data.eventData.error, retryAfter: data.eventData.retryAfter,
        updatedAt: now };
    default:
      return unreachable(data);
  }
}
