import type {
  Event, ListEventsByCorrelationIdParams, ListEventsParams, PaginatedResponse, PaginationOptions,
} from '@workflow/world';
import { DEFAULT_PAGE_LIMIT, FULL_EVENT_LOG_LIMIT, MAX_PAGE_LIMIT } from '../constants.js';
import { SortOrder } from '../enums.js';
import { decode, encode } from '../lib/codec.js';
import type { Db, RecordTable } from './db.js';
import { toEvent } from './rows.js';

export type RecordFilter = {
  pagination?: PaginationOptions;
  runId?: string;
  workflowName?: string;
  status?: string | string[];
};

export function pageLimit(options?: PaginationOptions): number {
  const limit = options?.limit ?? DEFAULT_PAGE_LIMIT;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_PAGE_LIMIT) {
    throw new RangeError(`Pagination limit must be between 1 and ${MAX_PAGE_LIMIT}`);
  }
  return limit;
}

/** Runs, steps or hooks, newest first by default. The cursor is the encoded `[createdAt, id]` of the last row. */
export function listRecords<T>(db: Db, table: RecordTable, filter: RecordFilter): PaginatedResponse<T> {
  const { pagination } = filter;
  const limit = pageLimit(pagination);
  const descending = pagination?.sortOrder !== SortOrder.Asc;
  const where: string[] = [];
  const values: (string | number)[] = [];
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
  const cursor = parseRecordCursor(pagination?.cursor);
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

/** Events in slot order. The cursor is `eid:` plus the last event ID. */
export function listEvents(
  db: Db, params: ListEventsParams | ListEventsByCorrelationIdParams,
): PaginatedResponse<Event> {
  const { runId, pagination, resolveData } = params;
  const correlationId = 'correlationId' in params ? params.correlationId : undefined;
  const descending = pagination?.sortOrder === SortOrder.Desc;
  const raw = pagination?.cursor;
  const after = parseEventCursor(raw, descending);
  // The SDK replays a run by listing its events without a limit.
  const limit = pagination?.limit === undefined && correlationId === undefined ? FULL_EVENT_LOG_LIMIT : pageLimit(pagination);
  const rows = db.events(runId, after, limit + 1, { correlationId, descending });
  const data = rows.slice(0, limit).map((row) => toEvent(row, resolveData));
  const last = data.at(-1);
  return {
    data,
    cursor: last ? `eid:${last.eventId}` : raw ?? null,
    hasMore: rows.length > limit,
  };
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
