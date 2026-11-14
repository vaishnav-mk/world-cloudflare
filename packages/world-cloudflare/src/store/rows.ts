import { getEventDataPayloadField, slotToEventId, stripEventDataRefs, type Event } from '@workflow/world';
import { EventType, ResolveData } from '../enums.js';
import { decode } from '../lib/codec.js';
import type { StoreSchema } from './tables.js';

/** A run, step or hook row: the entity is encoded in `data`. */
export type RecordRow = Pick<StoreSchema['runs'], 'id' | 'data' | 'created_at'>;

/** One event. The event's payload field (input, output, result...) is a BLOB beside its metadata. */
export type EventRow = StoreSchema['events'];

type OutboxColumns = Omit<StoreSchema['dispatch_outbox'], 'status'>;

/**
 * A due outbox row joined with its event. Step and wait events always carry a correlation ID,
 * so their rows have one; starts and hook wakes are identified by run and slot alone.
 */
export type OutboxRow = OutboxColumns & (
  | { kind: EventType.RunCreated | EventType.HookReceived; correlation_id: string | null }
  | { kind: EventType.StepCreated | EventType.WaitCreated; correlation_id: string }
);

/** `resolveData` is the SDK's events mode; only `none` changes the result. */
export function toEvent(row: EventRow, resolveData?: string): Event {
  // SAFETY: `kind` is only ever written from a validated event's `eventType` (`CreateEventSchema`).
  const eventType = row.kind as Event['eventType'];
  const eventData = row.data === null ? undefined : decode<Record<string, unknown>>(row.data);
  const field = getEventDataPayloadField(eventType);
  // Replay never reads recorded step inputs, so it skips loading them, from R2 too.
  const skipped = resolveData === ResolveData.SkipStepInputs && eventType === EventType.StepCreated;
  if (field && row.payload !== null && !skipped) {
    if (!eventData) throw new Error('Corrupt event: payload without metadata');
    eventData[field] = new Uint8Array(row.payload);
  }
  // SAFETY: these are the fields of the stored event of type `eventType`, written from a validated request.
  const event = {
    eventType,
    runId: row.run_id,
    eventId: slotToEventId(row.slot),
    createdAt: new Date(row.created_at),
    ...(row.occurred_at === null ? {} : { occurredAt: new Date(row.occurred_at) }),
    specVersion: row.spec_version,
    ...(row.correlation_id === null ? {} : { correlationId: row.correlation_id }),
    ...(row.resume_id === null ? {} : { resumeId: row.resume_id }),
    ...(eventData === undefined ? {} : { eventData }),
  } as Event;
  return resolveData === ResolveData.None ? stripEventDataRefs(event, ResolveData.None) : event;
}
