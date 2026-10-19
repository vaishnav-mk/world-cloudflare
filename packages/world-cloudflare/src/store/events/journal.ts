import {
  getEventDataPayloadField, mintedSpecVersion, requireEventSlot,
  type AnyEventRequest, type CreateEventParams, type Event, type EventResult,
} from '@workflow/world';
import { SINCE_CURSOR_LIMIT } from '../../constants.js';
import { ChangeKind, EventType, SortOrder } from '../../enums.js';
import { encode } from '../../lib/codec.js';
import type { Db } from '../db.js';
import { listEvents } from '../pages.js';
import { toEvent, type EventRow } from '../rows.js';
import type { Change, HookConflictEvent } from './types.js';

/** Appends at the run's next dense slot. The event's payload field is stored as a BLOB. */
export function appendEvent(
  db: Db, runId: string, data: AnyEventRequest | HookConflictEvent, now: Date, params: CreateEventParams = {},
): Event {
  const field = getEventDataPayloadField(data.eventType);
  const metadata = 'eventData' in data ? { ...data.eventData } as Record<string, unknown> : undefined;
  const payload = field && metadata ? metadata[field] : undefined;
  if (payload !== undefined && !(payload instanceof Uint8Array)) {
    throw new Error(`Expected Uint8Array for ${data.eventType}.${field}`);
  }
  if (field && metadata) delete metadata[field];
  const row: EventRow = {
    run_id: runId,
    slot: db.lastSlot(runId) + 1,
    kind: data.eventType,
    correlation_id: 'correlationId' in data ? data.correlationId ?? null : null,
    spec_version: data.specVersion ?? mintedSpecVersion(),
    created_at: (params.occurredAt ?? now).getTime(),
    occurred_at: params.occurredAt?.getTime() ?? null,
    resume_id: params.resumeId ?? null,
    resume_digest: params.resumePayloadDigest ?? null,
    data: metadata === undefined ? null : encode(metadata),
    payload: payload === undefined ? null : new Uint8Array(payload).buffer,
  };
  db.insertEvent(row);
  return toEvent(row);
}

/** What the log keeps of a request. Data that lives only on an entity is left out. */
export function journalOf(data: AnyEventRequest, change: Change): AnyEventRequest {
  if (change.kind === ChangeKind.LazyStepStart) return change.journal;
  switch (data.eventType) {
    case EventType.RunCreated: {
      // Dynamic workflow source lives only on the run.
      const { dynamicWorkflowCode: _code, dynamicWorkflowCodeRef: _ref, ...eventData } = data.eventData;
      return { ...data, eventData };
    }
    case EventType.RunStarted:
      // A lazy start's run fields are journaled in the implied run_created.
      return { eventType: EventType.RunStarted, specVersion: data.specVersion };
    default:
      return data;
  }
}

/** The `events.create` result: the event, the entities it touched, and any events the writer missed. */
export function eventResult(db: Db, event: Event, change: Change, params: CreateEventParams): EventResult {
  const reportsRun = event.eventType.startsWith('run_') || event.eventType === EventType.AttrSet;
  const result: EventResult = {
    event,
    allocated: change.kind === ChangeKind.LazyStepStart ? 2 : 1,
    ...(reportsRun ? { run: change.run } : {}),
    ...entitiesOf(change),
  };
  if (params.sinceCursor) {
    const page = listEvents(db, {
      runId: event.runId, resolveData: params.resolveData,
      pagination: { cursor: params.sinceCursor, sortOrder: SortOrder.Asc, limit: SINCE_CURSOR_LIMIT },
    });
    return { ...result, events: page.data, cursor: page.cursor, hasMore: page.hasMore };
  }
  // A stale writer gets the events it skipped.
  const seen = params.eventCount;
  const slot = requireEventSlot(event.eventId);
  if (seen !== undefined && Number.isSafeInteger(seen) && seen >= 0 && slot > seen + 1) {
    const missed = db.eventsBetween(event.runId, seen, slot);
    return { ...result, events: missed.map((row) => toEvent(row, params.resolveData)), cursor: null, hasMore: false };
  }
  return result;
}

function entitiesOf(change: Change): Pick<EventResult, 'step' | 'hook' | 'wait' | 'stepCreated'> {
  switch (change.kind) {
    case ChangeKind.Step:
      return { step: change.step };
    case ChangeKind.LazyStepStart:
      return { step: change.step, stepCreated: true };
    case ChangeKind.HookCreated:
      return { hook: change.hook };
    case ChangeKind.Wait:
      return { wait: change.wait };
    case ChangeKind.Run:
    case ChangeKind.HookConflict:
    case ChangeKind.HookDisposed:
      return {};
  }
}
