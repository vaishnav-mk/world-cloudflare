import {
  isTerminalWorkflowRunStatus, slotToEventId,
  type Event, type EventListResponse, type Hook, type ListEventsByCorrelationIdParams, type ListEventsParams,
  type ListHooksParams, type ListWorkflowRunStepsParams, type ListWorkflowRunsParams, type PaginatedResponse, type Step,
  type StepWithoutData, type WorkflowRun, type WorkflowRunWithoutData,
} from '@workflow/world';
import { EVENT_POLL_BATCH, EVENT_POLL_MS, MAX_TERMINAL_WAIT_MS } from '../constants.js';
import { toJson } from '../lib/json.js';
import type { ResolveDataMode } from '../types.js';
import type { StoreContext } from './context.js';
import { listRecords, readEventPage } from './pages.js';
import { toEvent } from './rows.js';
import { hookView, runView, stepView } from './views.js';

type AnyRun = WorkflowRun | WorkflowRunWithoutData;

export function getRun({ db }: StoreContext, id: string, resolveData?: ResolveDataMode): AnyRun {
  return runView(db.getRun(id), resolveData);
}

export function getManyRuns({ db }: StoreContext, ids: readonly string[], resolveData?: ResolveDataMode): (AnyRun | null)[] {
  return ids.map((id) => {
    const run = db.findRun(id);
    return run === undefined ? null : runView(run, resolveData);
  });
}

export async function waitForTerminalRun(
  store: StoreContext, id: string, timeoutMs: number, resolveData?: ResolveDataMode,
): Promise<AnyRun> {
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw new RangeError('Invalid status timeout');
  const current = getRun(store, id, resolveData);
  if (timeoutMs === 0 || isTerminalWorkflowRunStatus(current.status)) return current;
  await store.waiters.wait(id, Math.min(timeoutMs, MAX_TERMINAL_WAIT_MS), () => isTerminalWorkflowRunStatus(store.db.getRun(id).status));
  return getRun(store, id, resolveData);
}

export function listRuns({ db }: StoreContext, params: ListWorkflowRunsParams): PaginatedResponse<AnyRun> {
  const page = listRecords<WorkflowRun>(db, 'runs', params);
  return { ...page, data: page.data.map((run) => runView(run, params.resolveData)) };
}

export function getStep(store: StoreContext, runId: string, stepId: string, resolveData?: ResolveDataMode): Step | StepWithoutData {
  store.own(runId);
  return stepView(store.db.getStep(runId, stepId), resolveData);
}

export function listSteps(store: StoreContext, params: ListWorkflowRunStepsParams): PaginatedResponse<Step | StepWithoutData> {
  store.own(params.runId);
  const page = listRecords<Step>(store.db, 'steps', params);
  return { ...page, data: page.data.map((step) => stepView(step, params.resolveData)) };
}

export function getHook({ db }: StoreContext, hookId: string, resolveData?: ResolveDataMode): Hook {
  return hookView(db.getHook(hookId), resolveData);
}

/**
 * For `hooks.get`, which asks every store. The legacy store skips the old directory's copies of
 * sharded hooks, and a finished run's hook past its retention is gone, as with `getHook`.
 */
export function findHook({ db, isLegacy }: StoreContext, hookId: string, resolveData?: ResolveDataMode): Hook | undefined {
  const hook = db.findHook(hookId);
  if (hook === undefined || (isLegacy && db.isRunCopy(hook.runId)) || db.isExpiredHook(hook, new Date())) return undefined;
  return hookView(hook, resolveData);
}

export function listHooks(store: StoreContext, params: ListHooksParams): PaginatedResponse<Hook> {
  if (params.runId !== undefined) store.own(params.runId);
  const page = listRecords<Hook>(store.db, 'hooks', params);
  const now = new Date();
  const live = page.data.filter((hook) => !store.db.isExpiredHook(hook, now));
  return { ...page, data: live.map((hook) => hookView(hook, params.resolveData)) };
}

export function getEvent(store: StoreContext, runId: string, eventId: string, resolveData?: ResolveDataMode): Event {
  store.own(runId);
  const slot = Number(eventId.replace(/^evnt_/, ''));
  if (!Number.isSafeInteger(slot) || slot < 1 || slotToEventId(slot) !== eventId) throw new Error('Invalid event ID');
  return toEvent(store.db.getEvent(runId, slot), resolveData);
}

/** A page of the run's events, with a spec 9 run's fence counts. */
export function listEvents(store: StoreContext, params: ListEventsParams): Promise<EventListResponse> {
  store.own(params.runId);
  const { db } = store;
  return store.pageReads.run(async () => {
    const page = await readEventPage(db, params);
    // Read right after the page's last read: nothing is awaited between, so no write lands in between.
    const seqInBand = db.seqInBand(params.runId);
    const snapshot = seqInBand === null ? undefined : { seq: db.lastSlot(params.runId), seqInBand };
    return { ...page, ...(snapshot === undefined ? {} : { snapshot }) };
  });
}

export function listEventsByCorrelationId(store: StoreContext, params: ListEventsByCorrelationIdParams): Promise<PaginatedResponse<Event>> {
  store.own(params.runId);
  return store.pageReads.run(() => readEventPage(store.db, params));
}

/** Replays events after `cursor` (a slot), or waits for the next commit. `done` once a finished run is drained. */
export async function nextEvents({ db, waiters }: StoreContext, runId: string, cursor: number, filter?: string[]) {
  if (!Number.isSafeInteger(cursor) || cursor < 0) throw new RangeError('Invalid subscription cursor');
  const read = () => {
    const rows = [...db.events(runId, cursor, EVENT_POLL_BATCH)];
    const nextCursor = rows.at(-1)?.slot ?? cursor;
    return {
      events: rows.filter((row) => !filter || filter.includes(row.kind)).map((row) => ({
        eventId: slotToEventId(row.slot), eventType: row.kind, data: toJson(toEvent(row)),
      })),
      cursor: nextCursor,
      done: isTerminalWorkflowRunStatus(db.getRun(runId).status) && nextCursor >= db.lastSlot(runId),
    };
  };
  const page = read();
  if (page.cursor !== cursor || page.done) return page;
  await waiters.wait(runId, EVENT_POLL_MS, () => db.lastSlot(runId) > cursor);
  return read();
}

/**
 * For `events.subscribe`: the run's events after `afterSlot`, as `events.list` returns them, or
 * the next ones to commit within `EVENT_POLL_MS`. `done` once a finished run has none left.
 */
export async function eventsAfter(store: StoreContext, runId: string, afterSlot: number): Promise<{ events: Event[]; done: boolean }> {
  if (!Number.isSafeInteger(afterSlot) || afterSlot < 0) throw new RangeError('Invalid subscription slot');
  store.own(runId);
  const { db } = store;
  const read = () => {
    const events = Array.from(db.events(runId, afterSlot, EVENT_POLL_BATCH), (row) => toEvent(row));
    const done = events.length === 0 && isTerminalWorkflowRunStatus(db.getRun(runId).status);
    return { events, done };
  };
  const page = read();
  if (page.events.length > 0 || page.done) return page;
  await store.waiters.wait(runId, EVENT_POLL_MS, () => db.lastSlot(runId) > afterSlot);
  return read();
}
