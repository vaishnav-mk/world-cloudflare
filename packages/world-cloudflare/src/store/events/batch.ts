import type { BatchEventRequest } from '@workflow/world';
import { EventType } from '../../enums.js';

/** Event types the contract keeps out of a batch; they take the single `create` path. */
const UNBATCHABLE: ReadonlySet<string> = new Set<string>([
  EventType.RunCreated, EventType.RunStarted, EventType.RunCancelled,
  EventType.HookCreated, EventType.HookDisposed, EventType.AttrSet,
]);

/** What a spec 9 batch may hold: step and wait events, any number per entity. */
const APPENDS: ReadonlySet<string> = new Set<string>([
  EventType.StepCreated, EventType.StepStarted, EventType.StepCompleted, EventType.StepFailed, EventType.StepRetrying,
  EventType.WaitCreated, EventType.WaitCompleted,
]);

/**
 * Rejects a batch the contract does not allow, before any of it is applied. A spec 9 run's batch
 * holds step and wait events only. An earlier run's batch holds no unbatchable event type, and one
 * event per entity unless it is `step_created` then `step_started`, which creates the step already
 * running, with its input on the `step_created`.
 */
export function checkBatch(events: readonly BatchEventRequest[], appends: boolean): void {
  if (appends) {
    for (const { event } of events) {
      if (!APPENDS.has(event.eventType)) throw new Error(`${event.eventType} cannot be batched on a spec 9 run`);
    }
    return;
  }
  const byEntity = new Map<string, BatchEventRequest['event'][]>();
  for (const { event } of events) {
    if (UNBATCHABLE.has(event.eventType)) throw new Error(`${event.eventType} cannot be batched`);
    if (!('correlationId' in event) || event.correlationId === undefined) continue;
    const entity = byEntity.get(event.correlationId);
    if (entity === undefined) byEntity.set(event.correlationId, [event]);
    else entity.push(event);
  }
  for (const [correlationId, [first, second, ...rest]] of byEntity) {
    if (second === undefined) continue;
    const bornRunning = rest.length === 0 && first.eventType === EventType.StepCreated
      && second.eventType === EventType.StepStarted;
    if (!bornRunning) throw new Error(`A batch holds one event per entity, except a new running step (${correlationId})`);
    if ('eventData' in second && second.eventData !== undefined && 'input' in second.eventData) {
      throw new Error(`A batched step_started cannot carry the step input (${correlationId})`);
    }
  }
}
