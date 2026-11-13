import { EntityConflictError } from '@workflow/errors';
import type { Step, WorkflowRun } from '@workflow/world';
import { ChangeKind, EventType, StepStatus } from '../../enums.js';
import { isSingleOrchestrator } from '../../lib/spec.js';
import { unreachable } from '../../lib/unreachable.js';
import type { EventOf } from '../../types.js';
import type { Change, StepEvent, Tx } from './types.js';

type StepUpdateEvent = Exclude<StepEvent, EventOf<EventType.StepCreated>>;
type LazyStartFields = Omit<NonNullable<EventOf<EventType.StepStarted>['eventData']>, 'input'>;

/**
 * Applies a step event. `step_created` makes a pending step. A lazy `step_started` carries the
 * input itself: it creates the step and starts it in one write, implying a `step_created`.
 */
export function changeStep(tx: Tx, run: WorkflowRun, data: StepEvent): Change {
  if (isSingleOrchestrator(run.specVersion)) {
    const step = followStep(tx, tx.db.findStep(tx.runId, data.correlationId), data);
    return step === undefined ? { kind: ChangeKind.Run, run } : { kind: ChangeKind.StepAppend, run, step };
  }
  switch (data.eventType) {
    case EventType.StepCreated: {
      const { stepName, input } = data.eventData;
      return { kind: ChangeKind.Step, run, step: createStep(tx, data.correlationId, stepName, input) };
    }
    case EventType.StepStarted: {
      const { input, ...fields } = data.eventData ?? {};
      if (input !== undefined) return startLazily(tx, run, data, input, fields);
      break;
    }
  }
  return { kind: ChangeKind.Step, run, step: advanceStep(tx.db.getStep(tx.runId, data.correlationId), data, tx.now) };
}

function startLazily(
  tx: Tx, run: WorkflowRun, data: EventOf<EventType.StepStarted>, input: unknown, { stepName, ...rest }: LazyStartFields,
): Change {
  if (!stepName) throw new Error('Lazy step_started requires stepName');
  const step = createStep(tx, data.correlationId, stepName, input);
  return {
    kind: ChangeKind.LazyStepStart,
    run,
    step: advanceStep(step, data, tx.now),
    implied: {
      eventType: EventType.StepCreated, correlationId: step.stepId, specVersion: tx.specVersion,
      eventData: { stepName, input },
    },
    journal: { ...data, eventData: { stepName, ...rest } },
  };
}

function createStep(tx: Tx, stepId: string, stepName: string, input: unknown): Step {
  if (tx.db.findStep(tx.runId, stepId)) throw new EntityConflictError(`Step ${stepId} already created`);
  return {
    runId: tx.runId, stepId, stepName, status: StepStatus.Pending, input, attempt: 0,
    specVersion: tx.specVersion, createdAt: tx.now, updatedAt: tx.now,
  };
}

const isTerminalStep = (step: Step) =>
  step.status === StepStatus.Completed || step.status === StepStatus.Failed || step.status === StepStatus.Cancelled;

/**
 * Spec 9: step events are appends and the runtime keeps the lifecycle itself, so nothing is
 * refused. The step record follows the events that fit it, for `steps.get` and `steps.list`, and
 * is left alone by the rest. Undefined when the record does not change. An inline step has no
 * `step_created`: its first `step_started` carries the input and creates it.
 */
function followStep(tx: Tx, step: Step | undefined, data: StepEvent): Step | undefined {
  const { now } = tx;
  if (data.eventType === EventType.StepCreated) {
    return step ?? createStep(tx, data.correlationId, data.eventData.stepName, data.eventData.input);
  }
  if (step === undefined) {
    if (data.eventType !== EventType.StepStarted) return undefined;
    const { stepName, input, attempt = 1 } = data.eventData ?? {};
    if (!stepName || input === undefined) return undefined;
    return { ...createStep(tx, data.correlationId, stepName, input), status: StepStatus.Running, attempt, startedAt: now };
  }
  if (isTerminalStep(step)) return undefined;
  switch (data.eventType) {
    case EventType.StepStarted:
      return { ...step, status: StepStatus.Running, attempt: data.eventData?.attempt ?? step.attempt + 1,
        startedAt: step.startedAt ?? now, retryAfter: undefined, updatedAt: now };
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

function advanceStep(step: Step, data: StepUpdateEvent, now: Date): Step {
  const id = step.stepId;
  if (isTerminalStep(step)) throw new EntityConflictError(`Step ${id} is terminal`);
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
