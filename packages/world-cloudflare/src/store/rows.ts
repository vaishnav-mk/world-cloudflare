import { getEventDataPayloadField, slotToEventId, stripEventDataRefs, type Event } from '@workflow/world';
import { ResolveData } from '../enums.js';
import { decode } from '../lib/codec.js';
import type { DispatchKind } from '../types.js';

/** A run, step or hook row: the entity is encoded in `data`. */
export type RecordRow = { id: string; data: string; created_at: number };

/** One event. The event's payload field (input, output, result...) is a BLOB beside its metadata. */
export type EventRow = {
  run_id: string;
  slot: number;
  kind: Event['eventType'];
  correlation_id: string | null;
  spec_version: number;
  created_at: number;
  occurred_at: number | null;
  resume_id: string | null;
  data: string | null;
  payload: ArrayBuffer | null;
};

/** A due outbox row joined with its event. */
export type OutboxRow = {
  run_id: string;
  slot: number;
  kind: DispatchKind;
  correlation_id: string | null;
  queue_name: string;
  deployment_id: string;
  attempts: number;
  redrive: number;
  next_at: number;
};

/** `resolveData` is the SDK's events mode; only `none` changes the result. */
export function toEvent(row: EventRow, resolveData?: string): Event {
  const eventData = row.data === null ? undefined : decode<Record<string, unknown>>(row.data);
  const field = getEventDataPayloadField(row.kind);
  if (field && row.payload !== null) {
    if (!eventData) throw new Error('Corrupt event: payload without metadata');
    eventData[field] = new Uint8Array(row.payload);
  }
  const event = {
    eventType: row.kind,
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
