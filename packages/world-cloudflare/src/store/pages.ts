import type {
  Event, ListEventsByCorrelationIdParams, ListEventsParams, PaginatedResponse, PaginationOptions,
} from '@workflow/world';
import {
  DEFAULT_PAGE_LIMIT, EVENT_PAGE_MAX_BYTES, EVENT_READ_BYTES, FULL_EVENT_LOG_LIMIT, MAX_PAGE_LIMIT,
} from '../constants.js';
import { SortOrder } from '../enums.js';
import { decode, encode } from '../lib/codec.js';
import type { Db, RecordTable } from './db.js';
import { toEvent } from './rows.js';

type RecordFilter = {
  pagination?: PaginationOptions;
  runId?: string;
  workflowName?: string;
  status?: string | string[];
};

function pageLimit(limit = DEFAULT_PAGE_LIMIT): number {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_PAGE_LIMIT) {
    throw new RangeError(`Pagination limit must be between 1 and ${MAX_PAGE_LIMIT}`);
  }
  return limit;
}

/** Runs, steps or hooks, newest first by default. The cursor is the encoded `[createdAt, id]` of the last row. */
export function listRecords<T>(db: Db, table: RecordTable, filter: RecordFilter): PaginatedResponse<T> {
  const { limit: requestedLimit, sortOrder, cursor: rawCursor } = filter.pagination ?? {};
  const limit = pageLimit(requestedLimit);
  const descending = sortOrder !== SortOrder.Asc;
  const where: string[] = [];
  const values: (string | number)[] = [];
  // The legacy store still has the old directory's copies of sharded runs and their hooks; the shards list those.
  if (table === 'runs') where.push('index_slot IS NULL');
  if (table === 'hooks') where.push('NOT EXISTS (SELECT 1 FROM runs WHERE runs.id = run_id AND runs.index_slot IS NOT NULL)');
  if (filter.runId !== undefined) {
    where.push('run_id = ?');
    values.push(filter.runId);
  }
  if (filter.workflowName !== undefined) {
    where.push('workflow_name = ?');
    values.push(filter.workflowName);
  }
  if (filter.status !== undefined) {
    const statuses = Array.isArray(filter.status) ? filter.status : [filter.status];
    // An empty status list matches nothing, like SQL `IN ()`.
    where.push(statuses.length ? `status IN (${statuses.map(() => '?').join(',')})` : '0');
    values.push(...statuses);
  }
  const cursor = parseRecordCursor(rawCursor);
  if (cursor) {
    where.push(`(created_at, id) ${descending ? '<' : '>'} (?, ?)`);
    values.push(...cursor);
  }
  const rows = db.records(table, where, values, descending, limit + 1);
  const page = rows.slice(0, limit);
  const last = page.at(-1);
  const hasMore = rows.length > limit;
  return {
    data: page.map((row) => decode<T>(row.data)),
    cursor: hasMore && last ? encode([last.created_at, last.id]) : null,
    hasMore,
  };
}

/**
 * One page of a list that every store answered with the same filter and cursor, as `listRecords`
 * would give over all their rows: each store's page holds every row of its own that could come
 * first. The cursor is the last returned row's `[createdAt, id]`, so the next page asks every store
 * for rows strictly after it.
 */
export function mergePages<T>(
  pages: readonly PaginatedResponse<T>[], keyOf: (item: T) => [number, string], pagination: PaginationOptions = {},
): PaginatedResponse<T> {
  const limit = pageLimit(pagination.limit);
  const sign = pagination.sortOrder === SortOrder.Asc ? 1 : -1;
  const rows = pages.flatMap((page) => page.data.map((item) => ({ item, key: keyOf(item) })));
  // Ordered as SQLite orders `created_at, id`: by number, then by string.
  rows.sort(({ key: [atA, idA] }, { key: [atB, idB] }) => sign * (atA - atB || (idA < idB ? -1 : idA > idB ? 1 : 0)));
  const page = rows.slice(0, limit);
  const last = page.at(-1);
  const hasMore = rows.length > limit || pages.some((each) => each.hasMore);
  return { data: page.map(({ item }) => item), cursor: hasMore && last ? encode(last.key) : null, hasMore };
}

type EventQuery = {
  runId: string;
  correlationId?: string;
  descending: boolean;
  resolveData?: string;
  /** The slot the page starts after (descending: before). */
  after: number;
  limit: number;
  cursor?: string;
};

function eventQuery(params: ListEventsParams | ListEventsByCorrelationIdParams): EventQuery {
  const { runId, resolveData } = params;
  const { limit: requestedLimit, sortOrder, cursor } = params.pagination ?? {};
  const correlationId = 'correlationId' in params ? params.correlationId : undefined;
  const descending = sortOrder === SortOrder.Desc;
  // The SDK replays a run by listing its events without a limit.
  const limit = requestedLimit === undefined && correlationId === undefined ? FULL_EVENT_LOG_LIMIT : pageLimit(requestedLimit);
  return { runId, correlationId, descending, resolveData, after: parseEventCursor(cursor, descending), limit, cursor };
}

/**
 * Appends the events after slot `from` to `page` until it holds `query.limit` events or the rows read
 * pass `maxBytes`. Always reads at least one event when there is one.
 */
function readEvents(db: Db, query: EventQuery, page: Event[], from: number, maxBytes: number) {
  let bytes = 0;
  let last = from;
  for (const row of db.events(query.runId, from, query.limit - page.length + 1, query)) {
    if (page.length === query.limit || bytes >= maxBytes) return { bytes, last, more: true };
    page.push(toEvent(row, query.resolveData));
    bytes += (row.data?.length ?? 0) + (row.payload?.byteLength ?? 0);
    last = row.slot;
  }
  return { bytes, last, more: false };
}

function eventPage(data: Event[], hasMore: boolean, cursor: string | undefined): PaginatedResponse<Event> {
  const last = data.at(-1);
  return { data, cursor: last ? `eid:${last.eventId}` : cursor ?? null, hasMore };
}

/**
 * Events in slot order, in one read, for a caller that cannot wait, such as a write's transaction.
 * The cursor is `eid:` plus the last event ID. A page ends at its count limit, or once its rows pass
 * `EVENT_PAGE_MAX_BYTES`, with `hasMore` and the cursor to read on from.
 */
export function listEvents(db: Db, params: ListEventsParams | ListEventsByCorrelationIdParams): PaginatedResponse<Event> {
  const query = eventQuery(params);
  const page: Event[] = [];
  const { more } = readEvents(db, query, page, query.after, EVENT_PAGE_MAX_BYTES);
  return eventPage(page, more, query.cursor);
}

/**
 * `listEvents`, read `EVENT_READ_BYTES` at a time with a pause between reads, so the object serves
 * other requests while it reads a long page.
 */
export async function readEventPage(db: Db, params: ListEventsParams | ListEventsByCorrelationIdParams): Promise<PaginatedResponse<Event>> {
  const query = eventQuery(params);
  const page: Event[] = [];
  let bytes = 0;
  let from = query.after;
  let more = true;
  while (more && page.length < query.limit && bytes < EVENT_PAGE_MAX_BYTES) {
    if (page.length) await scheduler.wait(0);
    const read = readEvents(db, query, page, from, Math.min(EVENT_READ_BYTES, EVENT_PAGE_MAX_BYTES - bytes));
    bytes += read.bytes;
    from = read.last;
    more = read.more;
  }
  return eventPage(page, more, query.cursor);
}

function parseRecordCursor(cursor: string | undefined): [number, string] | null {
  if (cursor === undefined) return null;
  const value = decode<unknown>(cursor);
  if (!Array.isArray(value) || !Number.isFinite(value[0]) || typeof value[1] !== 'string') {
    throw new Error('Invalid pagination cursor');
  }
  return value as [number, string];
}

function parseEventCursor(raw: string | undefined, descending: boolean): number {
  const cursor = raw?.startsWith('eid:') ? raw.slice(4) : raw;
  const slot = cursor ? Number(cursor.replace(/^evnt_/, '')) : descending ? Number.MAX_SAFE_INTEGER : 0;
  if (!Number.isSafeInteger(slot) || slot < 0) throw new Error('Invalid event cursor');
  return slot;
}
