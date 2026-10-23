import type { BatchEventRequest } from '@workflow/world';
import { EventType } from '../../enums.js';

/** Event types the contract keeps out of a batch; they take the single `create` path. */
const UNBATCHABLE: ReadonlySet<string> = new Set<string>([
  EventType.RunCreated, EventType.RunStarted, EventType.RunCancelled,
  EventType.HookCreated, EventType.HookDisposed, EventType.AttrSet,
]);

/**
 * Rejects a batch the contract does not allow, before any of it is applied: an unbatchable event
 * type, or more than one event for an entity unless it is `step_created` then `step_started`, which
 * creates the step already running, with its input on the `step_created`.
 */
export function checkBatch(events: readonly BatchEventRequest[]): void {
  const byEntity = new Map<string, BatchEventRequest['event'][]>();
  for (const { event } of events) {
    if (UNBATCHABLE.has(event.eventType)) throw new Error(`${event.eventType} cannot be batched`);
    if (!('correlationId' in event) || event.correlationId === undefined) continue;
    byEntity.set(event.correlationId, [...(byEntity.get(event.correlationId) ?? []), event]);
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
