import {
  getEventDataPayloadField, requireEventSlot,
  type AnyEventRequest, type CreateEventParams, type Event, type EventResult,
} from '@workflow/world';
import { SINCE_CURSOR_LIMIT } from '../../constants.js';
import { ChangeKind, EventType, ResolveData, SortOrder } from '../../enums.js';
import { encode } from '../../lib/codec.js';
import { mintSpecVersion } from '../../lib/spec.js';
import type { Db } from '../db.js';
import { listEvents } from '../pages.js';
import { toEvent, type EventRow } from '../rows.js';
import { journaledStartHook } from './runs.js';
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
    spec_version: data.specVersion ?? mintSpecVersion(),
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
      const { dynamicWorkflowCode: _code, dynamicWorkflowCodeRef: _ref, startHook, ...eventData } = data.eventData;
      if (startHook === undefined || change.run.startHook === undefined) return { ...data, eventData };
      // Replay binds the workflow's `createHook({ token })` to the start Hook from this.
      return { ...data, eventData: { ...eventData, startHook: journaledStartHook(change.run.runId, startHook) } };
    }
    case EventType.RunStarted:
      // A lazy start's run fields are journaled in the implied run_created.
      return { eventType: EventType.RunStarted, specVersion: data.specVersion };
    case EventType.HookCreated:
      // A forced creation that took the token over names the hook it took, so a replay can wake that run.
      return change.kind === ChangeKind.HookCreated && change.hook.claimedFrom !== undefined
        ? { ...data, eventData: { ...data.eventData, forceClaimedFrom: change.hook.claimedFrom } }
        : data;
    default:
      return data;
  }
}

/**
 * The `events.create` result: the event, the entities it touched, how many positions it took, and
 * any events the writer missed.
 */
export function eventResult(db: Db, event: Event, change: Change, params: CreateEventParams, allocated: number): EventResult {
  const reportsRun = event.eventType.startsWith('run_') || event.eventType === EventType.AttrSet;
  const result: EventResult = {
    event,
    // Where an ascending list resumes after this event. Only where the list would return this very
    // event: `none` and `skip-step-inputs` reads leave payloads out.
    ...(params.resolveData === undefined || params.resolveData === ResolveData.All ? { eventCursor: `eid:${event.eventId}` } : {}),
    allocated,
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
  const skipped = skippedEvents(db, event.runId, params.eventCount, requireEventSlot(event.eventId), params.resolveData);
  return skipped === undefined ? result : { ...result, ...skipped };
}

/**
 * The events a stale writer skipped: those after the `seen` events it held and before `slot`,
 * where its write landed. One page at most: a writer far behind would otherwise get the whole log,
 * past the 32 MiB RPC limit. The SDK re-reads a truncated report.
 */
export function skippedEvents(
  db: Db, runId: string, seen: number | undefined, slot: number, resolveData: CreateEventParams['resolveData'],
): { events: Event[]; cursor: string | null; hasMore: boolean } | undefined {
  if (seen === undefined || !Number.isSafeInteger(seen) || seen < 0 || slot <= seen + 1) return undefined;
  const rows = db.eventsBetween(runId, seen, slot, SINCE_CURSOR_LIMIT + 1);
  const events = rows.slice(0, SINCE_CURSOR_LIMIT).map((row) => toEvent(row, resolveData));
  const hasMore = rows.length > SINCE_CURSOR_LIMIT;
  const last = events.at(-1);
  return { events, cursor: hasMore && last ? `eid:${last.eventId}` : null, hasMore };
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
    // Spec 9 writes return no step or wait: the runtime folds them from the log.
    case ChangeKind.StepAppend:
    case ChangeKind.WaitAppend:
      return {};
  }
}
