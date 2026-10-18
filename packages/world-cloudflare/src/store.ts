import { DurableObject } from 'cloudflare:workers';
import { EntityConflictError, HookNotFoundError, RunExpiredError, WorkflowRunNotFoundError } from '@workflow/errors';
import { parse, stringify } from 'devalue';
import { ulid } from 'ulid';
import type { QueueShard } from './queue.js';
import {
  applyAttributeChanges,
  CreateEventSchema,
  getEventDataPayloadField,
  getQueueTopicPrefix,
  getRunQueueNamespace,
  isTerminalWorkflowRunStatus,
  ValidQueueName,
  slotToEventId,
  mintedSpecVersion,
  stripEventDataRefs,
  validateAttributeChanges,
  validateAttributeEventDataSize,
} from '@workflow/world';
import type {
  AnyEventRequest,
  CreateEventParams,
  Event,
  EventResult,
  GetWorkflowRunParams,
  Hook,
  ListEventsByCorrelationIdParams,
  ListEventsParams,
  ListHooksParams,
  ListWorkflowRunStepsParams,
  ListWorkflowRunsParams,
  PaginatedResponse,
  PaginationOptions,
  Step,
  StepWithoutData,
  Wait,
  WorkflowRun,
  WorkflowRunWithoutData,
} from '@workflow/world';

export interface Env {
  WORLD_STORE: DurableObjectNamespace<WorldStore>;
  QUEUE_SHARD: DurableObjectNamespace<QueueShard>;
}

type RecordRow = { data: string; created_at: number; id: string };
type DispatchKind = 'run_created' | 'hook_received' | 'step_created' | 'wait_created';
type OutboxRow = {
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
type HookConflictRequest = {
  eventType: 'hook_conflict';
  correlationId: string;
  specVersion: number;
  eventData: { token: string; conflictingRunId: string };
};
type EventRow = {
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

const dateKeys = new Set([
  'createdAt', 'updatedAt', 'startedAt', 'completedAt', 'expiredAt',
  'retryAfter', 'resumeAt', 'tokenRetentionUntil',
]);

// Prefix distinguishes this lossless encoding from rows written by the old JSON codec.
function encode(value: unknown): string {
  return `dv1:${stringify(value)}`;
}

function decode<T>(value: string): T {
  if (value.startsWith('dv1:')) return parse(value.slice(4)) as T;
  return JSON.parse(value, (key, item: unknown) => {
    if (item && typeof item === 'object' && '__workflowBytes' in item) {
      return new Uint8Array((item as { __workflowBytes: number[] }).__workflowBytes);
    }
    return dateKeys.has(key) && typeof item === 'string' ? new Date(item) : item;
  }) as T;
}

function withoutRunData(run: WorkflowRun): WorkflowRunWithoutData {
  const { dynamicWorkflowCode: _code, ...rest } = run;
  return { ...rest, input: undefined, output: undefined };
}

function withoutStepData(step: Step): StepWithoutData {
  return { ...step, input: undefined, output: undefined };
}

function pageLimit(options?: PaginationOptions): number {
  const limit = options?.limit ?? 100;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
    throw new RangeError('Pagination limit must be between 1 and 1000');
  }
  return limit;
}

function conflict(message: string): never {
  throw new EntityConflictError(message);
}

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY, data TEXT NOT NULL, created_at INTEGER NOT NULL,
    status TEXT NOT NULL, workflow_name TEXT NOT NULL);
  CREATE INDEX IF NOT EXISTS runs_created ON runs(created_at, id);
  CREATE TABLE IF NOT EXISTS steps (
    run_id TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, created_at INTEGER NOT NULL,
    PRIMARY KEY(run_id, id));
  CREATE INDEX IF NOT EXISTS steps_created ON steps(run_id, created_at, id);
  CREATE TABLE IF NOT EXISTS hooks (
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, token TEXT NOT NULL UNIQUE,
    data TEXT NOT NULL, created_at INTEGER NOT NULL);
  CREATE INDEX IF NOT EXISTS hooks_created ON hooks(run_id, created_at, id);
  CREATE TABLE IF NOT EXISTS waits (
    run_id TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(run_id, id));
  CREATE TABLE IF NOT EXISTS events (
    run_id TEXT NOT NULL, slot INTEGER NOT NULL, kind TEXT NOT NULL, correlation_id TEXT,
    spec_version INTEGER NOT NULL, created_at INTEGER NOT NULL, occurred_at INTEGER,
    resume_id TEXT, resume_digest TEXT, data TEXT, payload BLOB, PRIMARY KEY(run_id, slot));
  CREATE INDEX IF NOT EXISTS events_correlation ON events(run_id, correlation_id, slot);
  CREATE UNIQUE INDEX IF NOT EXISTS events_resume ON events(run_id, resume_id) WHERE resume_id IS NOT NULL;
  CREATE TABLE IF NOT EXISTS dispatch_outbox (
    run_id TEXT NOT NULL, slot INTEGER NOT NULL, queue_name TEXT NOT NULL, deployment_id TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0, redrive INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', PRIMARY KEY(run_id, slot));
`;

const DISPATCH_KINDS: ReadonlySet<string> = new Set<DispatchKind>(['run_created', 'hook_received', 'step_created', 'wait_created']);

function attributeChanges(attributes: Record<string, string> | undefined) {
  return Object.entries(attributes ?? {}).map(([key, value]) => ({ key, value }));
}

/** Restricts an outbox query to rows whose event has this kind, and optionally this correlation ID. */
function eventFilter(kind: DispatchKind, correlationId?: string): { clause: string; values: string[] } {
  return correlationId === undefined
    ? { clause: 'kind = ?', values: [kind] }
    : { clause: 'kind = ? AND correlation_id = ?', values: [kind, correlationId] };
}

export class WorldStore extends DurableObject<Env> {
  private readonly waiting = new Map<string, Set<() => void>>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      const sql = ctx.storage.sql;
      sql.exec(SCHEMA);
      // Outboxes created before redrive keys existed lack the column.
      if (!sql.exec<{ name: string }>('PRAGMA table_info(dispatch_outbox)').toArray().some(({ name }) => name === 'redrive')) {
        sql.exec('ALTER TABLE dispatch_outbox ADD COLUMN redrive INTEGER NOT NULL DEFAULT 0');
      }
      sql.exec('CREATE INDEX IF NOT EXISTS dispatch_outbox_pending ON dispatch_outbox(status, next_at)');
    });
  }

  private run(id: string): WorkflowRun | undefined {
    const row = this.ctx.storage.sql.exec<{ data: string }>('SELECT data FROM runs WHERE id = ?', id).toArray()[0];
    return row ? decode<WorkflowRun>(row.data) : undefined;
  }

  private step(runId: string, stepId: string): Step | undefined {
    const row = this.ctx.storage.sql.exec<{ data: string }>('SELECT data FROM steps WHERE run_id = ? AND id = ?', runId, stepId).toArray()[0];
    return row ? decode<Step>(row.data) : undefined;
  }

  private hook(hookId: string): Hook | undefined {
    const row = this.ctx.storage.sql.exec<{ data: string }>('SELECT data FROM hooks WHERE id = ?', hookId).toArray()[0];
    return row ? decode<Hook>(row.data) : undefined;
  }

  private wait(runId: string, correlationId: string): Wait | undefined {
    const row = this.ctx.storage.sql.exec<{ data: string }>('SELECT data FROM waits WHERE run_id = ? AND id = ?', runId, correlationId).toArray()[0];
    return row ? decode<Wait>(row.data) : undefined;
  }

  private persistRun(run: WorkflowRun): void {
    this.ctx.storage.sql.exec(
      `INSERT INTO runs(id, data, created_at, status, workflow_name) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET data = excluded.data, status = excluded.status`,
      run.runId, encode(run), run.createdAt.getTime(), run.status, run.workflowName
    );
  }

  /** Moves the alarm earlier when needed. Callers have already committed their outbox row, so the cron covers a failure. */
  private async armAlarm(at: number, purpose: string): Promise<void> {
    try {
      const alarm = await this.ctx.storage.getAlarm();
      if (alarm === null || alarm > at) await this.ctx.storage.setAlarm(at);
    } catch (error) { console.error(`Failed to schedule ${purpose}`, error); }
  }

  private event(row: EventRow, resolveData?: string): Event {
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
    return resolveData === 'none' ? stripEventDataRefs(event, 'none') : event;
  }

  private eventRows(runId: string, after: number, limit: number, correlationId?: string, descending = false): EventRow[] {
    const sql = this.ctx.storage.sql;
    const direction = descending ? 'DESC' : 'ASC';
    const comparison = descending ? '<' : '>';
    const condition = correlationId === undefined ? '' : ' AND correlation_id = ?';
    const query = `SELECT * FROM events WHERE run_id = ? AND slot ${comparison} ?${condition} ORDER BY slot ${direction} LIMIT ?`;
    return correlationId === undefined
      ? sql.exec<EventRow>(query, runId, after, limit).toArray()
      : sql.exec<EventRow>(query, runId, after, correlationId, limit).toArray();
  }

  private eventsPage(params: ListEventsParams | ListEventsByCorrelationIdParams): PaginatedResponse<Event> {
    const { runId, pagination, resolveData } = params;
    const descending = pagination?.sortOrder === 'desc';
    const raw = pagination?.cursor;
    const cursor = raw?.startsWith('eid:') ? raw.slice(4) : raw;
    const after = cursor ? Number(cursor.replace(/^evnt_/, '')) : descending ? Number.MAX_SAFE_INTEGER : 0;
    if (!Number.isSafeInteger(after) || after < 0) throw new Error('Invalid event cursor');
    const limit = pagination?.limit === undefined && !('correlationId' in params)
      ? 100000 : pageLimit(pagination);
    const rows = this.eventRows(runId, after, limit + 1, 'correlationId' in params ? params.correlationId : undefined, descending);
    const data = rows.slice(0, limit).map((row) => this.event(row, resolveData));
    return {
      data,
      cursor: data.length ? `eid:${data[data.length - 1].eventId}` : raw ?? null,
      hasMore: rows.length > limit,
    };
  }

  private records<T>(table: 'runs' | 'steps' | 'hooks', params: {
    pagination?: PaginationOptions; resolveData?: string; runId?: string;
    workflowName?: string; status?: string | string[];
  }): PaginatedResponse<T> {
    const { pagination } = params;
    const limit = pageLimit(pagination);
    const descending = pagination?.sortOrder !== 'asc';
    const cursor = pagination?.cursor === undefined ? null : decode<[number, string]>(pagination.cursor);
    if (cursor && (!Array.isArray(cursor) || !Number.isFinite(cursor[0]) || typeof cursor[1] !== 'string')) {
      throw new Error('Invalid pagination cursor');
    }
    const clauses: string[] = [];
    const values: (string | number)[] = [];
    if (params.runId !== undefined) { clauses.push('run_id = ?'); values.push(params.runId); }
    if (params.workflowName !== undefined) { clauses.push('workflow_name = ?'); values.push(params.workflowName); }
    if (params.status !== undefined) {
      const statuses = Array.isArray(params.status) ? params.status : [params.status];
      clauses.push(statuses.length ? `status IN (${statuses.map(() => '?').join(',')})` : '0');
      values.push(...statuses);
    }
    if (cursor) {
      clauses.push(`(created_at, id) ${descending ? '<' : '>'} (?, ?)`);
      values.push(...cursor);
    }
    const where = clauses.length ? ` WHERE ${clauses.join(' AND ')}` : '';
    const order = descending ? 'DESC' : 'ASC';
    const rows = this.ctx.storage.sql.exec<RecordRow>(
      `SELECT id, data, created_at FROM ${table}${where} ORDER BY created_at ${order}, id ${order} LIMIT ?`,
      ...values, limit + 1
    ).toArray();
    const selected = rows.slice(0, limit);
    const last = selected.at(-1);
    return {
      data: selected.map((row) => decode<T>(row.data)),
      cursor: rows.length > limit && last ? encode([last.created_at, last.id]) : null,
      hasMore: rows.length > limit,
    };
  }

  async getRun(id: string, params: GetWorkflowRunParams & { resolveData: 'none' }): Promise<WorkflowRunWithoutData>;
  async getRun(id: string, params?: GetWorkflowRunParams & { resolveData?: 'all' }): Promise<WorkflowRun>;
  async getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData>;
  async getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData> {
    const run = this.run(id);
    if (!run) throw new WorkflowRunNotFoundError(id);
    return params?.resolveData === 'none' ? withoutRunData(run) : run;
  }

  getManyRuns(ids: readonly string[], resolveData?: 'none' | 'all'): (WorkflowRun | WorkflowRunWithoutData | null)[] {
    return ids.map((id) => {
      const run = this.run(id);
      return run ? (resolveData === 'none' ? withoutRunData(run) : run) : null;
    });
  }

  async waitForTerminalRun(id: string, timeoutMs: number, resolveData?: 'none' | 'all'): Promise<WorkflowRun | WorkflowRunWithoutData> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw new RangeError('Invalid status timeout');
    const current = await this.getRun(id, { resolveData });
    if (timeoutMs === 0 || isTerminalWorkflowRunStatus(current.status)) return current;
    await new Promise<void>((resolve) => {
      const waiters = this.waiting.get(id) ?? new Set<() => void>();
      this.waiting.set(id, waiters);
      const timer = setTimeout(() => { waiters.delete(wake); resolve(); }, Math.min(timeoutMs, 30_000));
      const wake = () => { clearTimeout(timer); waiters.delete(wake); resolve(); };
      waiters.add(wake);
      const latest = this.run(id)?.status;
      if (latest && isTerminalWorkflowRunStatus(latest)) wake();
    });
    return this.getRun(id, { resolveData });
  }

  async listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>> {
    const page = this.records<WorkflowRun>('runs', params ?? {});
    return params?.resolveData === 'none' ? { ...page, data: page.data.map(withoutRunData) } : page;
  }

  async getStep(runId: string, stepId: string, params: { resolveData: 'none' }): Promise<StepWithoutData>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: 'all' }): Promise<Step>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<Step | StepWithoutData>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<Step | StepWithoutData> {
    const step = this.step(runId, stepId);
    if (!step) throw new Error(`Step ${stepId} in run ${runId} not found`);
    return params?.resolveData === 'none' ? withoutStepData(step) : step;
  }

  async listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>> {
    const page = this.records<Step>('steps', params);
    return params.resolveData === 'none' ? { ...page, data: page.data.map(withoutStepData) } : page;
  }

  async getHook(hookId: string, resolveData?: 'none' | 'all'): Promise<Hook> {
    const hook = this.hook(hookId);
    if (!hook) throw new HookNotFoundError(hookId);
    if (resolveData === 'none') { const { metadata: _metadata, ...rest } = hook; return rest; }
    return hook;
  }

  async getHookByToken(token: string, resolveData?: 'none' | 'all'): Promise<Hook> {
    const row = this.ctx.storage.sql.exec<{ data: string }>('SELECT data FROM hooks WHERE token = ?', token).toArray()[0];
    if (!row) throw new HookNotFoundError(token);
    const hook = decode<Hook>(row.data);
    if (resolveData === 'none') { const { metadata: _metadata, ...rest } = hook; return rest; }
    return hook;
  }

  async listHooks(params: ListHooksParams): Promise<PaginatedResponse<Hook>> {
    const page = this.records<Hook>('hooks', params);
    return params.resolveData === 'none'
      ? { ...page, data: page.data.map(({ metadata: _metadata, ...hook }) => hook) } : page;
  }

  async getEvent(runId: string, eventId: string, resolveData?: 'none' | 'all'): Promise<Event> {
    const slot = Number(eventId.replace(/^evnt_/, ''));
    if (!Number.isSafeInteger(slot) || slot < 1 || slotToEventId(slot) !== eventId) throw new Error('Invalid event ID');
    const row = this.ctx.storage.sql.exec<EventRow>(
      'SELECT * FROM events WHERE run_id = ? AND slot = ?', runId, slot,
    ).toArray()[0];
    if (!row) throw new Error(`Event ${eventId} not found`);
    return this.event(row, resolveData);
  }

  async listEvents(params: ListEventsParams): Promise<PaginatedResponse<Event>> {
    return this.eventsPage(params);
  }

  async listEventsByCorrelationId(params: ListEventsByCorrelationIdParams): Promise<PaginatedResponse<Event>> {
    return this.eventsPage(params);
  }

  /** Replay retained events first, then wait for the next commit. Cursor is the last seen slot. */
  async nextEvents(runId: string, cursor: number, filter?: string[]) {
    if (!Number.isSafeInteger(cursor) || cursor < 0) throw new RangeError('Invalid subscription cursor');
    if (!this.run(runId)) throw new WorkflowRunNotFoundError(runId);
    const lastSlot = () => this.ctx.storage.sql.exec<{ slot: number }>(
      'SELECT COALESCE(MAX(slot), 0) AS slot FROM events WHERE run_id = ?', runId,
    ).one().slot;
    const read = () => {
      const rows = this.eventRows(runId, cursor, 100);
      const status = this.run(runId)!.status;
      const nextCursor = rows.at(-1)?.slot ?? cursor;
      return {
        events: rows.filter((row) => !filter || filter.includes(row.kind)).map((row) => ({
          eventId: slotToEventId(row.slot), eventType: row.kind, data: JSON.stringify(this.event(row)),
        })),
        cursor: nextCursor,
        done: isTerminalWorkflowRunStatus(status) && nextCursor >= lastSlot(),
      };
    };
    let result = read();
    if (result.cursor !== cursor || result.done) return result;
    await new Promise<void>((resolve) => {
      const waiters = this.waiting.get(runId) ?? new Set<() => void>();
      this.waiting.set(runId, waiters);
      const timer = setTimeout(() => { waiters.delete(wake); resolve(); }, 15_000);
      const wake = () => { clearTimeout(timer); waiters.delete(wake); resolve(); };
      waiters.add(wake);
      if (lastSlot() > cursor) wake();
    });
    result = read();
    return result;
  }

  private pendingDispatchSlot(runId: string, kind: DispatchKind, correlationId?: string): number {
    const { clause, values } = eventFilter(kind, correlationId);
    return this.ctx.storage.sql.exec<{ slot: number }>(
      `SELECT COALESCE(MAX(slot), 0) AS slot FROM dispatch_outbox
       WHERE run_id = ? AND status = 'pending' AND slot IN (SELECT slot FROM events WHERE run_id = ? AND ${clause})`,
      runId, runId, ...values,
    ).one().slot;
  }

  private markDispatchPublished(runId: string, upToSlot: number, kind: DispatchKind): void {
    if (!Number.isSafeInteger(upToSlot) || upToSlot < 0) throw new RangeError('Invalid dispatch slot');
    this.ctx.storage.sql.exec(
      `UPDATE dispatch_outbox SET status = 'published'
       WHERE run_id = ? AND slot <= ? AND status = 'pending' AND slot IN (SELECT slot FROM events WHERE run_id = ? AND kind = ?)`,
      runId, upToSlot, runId, kind,
    );
  }

  private markDispatchSlotPublished(runId: string, slot: number, kind: DispatchKind): void {
    if (!Number.isSafeInteger(slot) || slot < 1) throw new RangeError('Invalid dispatch slot');
    this.ctx.storage.sql.exec(
      `UPDATE dispatch_outbox SET status = 'published'
       WHERE run_id = ? AND slot = ? AND status = 'pending' AND slot IN (SELECT slot FROM events WHERE run_id = ? AND kind = ?)`,
      runId, slot, runId, kind,
    );
  }

  /** After a DLQ, reopens published dispatches. A redrive bump gives the next publish a fresh queue key. */
  private reopenDispatch(runId: string, nextAt: number, kind: DispatchKind, correlationId?: string): number {
    const { clause, values } = eventFilter(kind, correlationId);
    return this.ctx.storage.sql.exec(
      `UPDATE dispatch_outbox SET status = 'pending', redrive = redrive + 1, next_at = ?
       WHERE run_id = ? AND status = 'published' AND slot IN (SELECT slot FROM events WHERE run_id = ? AND ${clause})
       RETURNING slot`,
      nextAt, runId, runId, ...values,
    ).toArray().length;
  }

  /** Marks a dispatch published once its entity no longer needs it, for example a step that already started. */
  private settleDispatches(runId: string, kind: DispatchKind, correlationId: string): void {
    this.ctx.storage.sql.exec(
      `UPDATE dispatch_outbox SET status = 'published'
       WHERE run_id = ? AND slot IN (SELECT slot FROM events WHERE run_id = ? AND kind = ? AND correlation_id = ?)`,
      runId, runId, kind, correlationId,
    );
  }

  private isPendingStep(runId: string, stepId: string, stepName: string): boolean {
    if (this.run(runId)?.status !== 'running') return false;
    const step = this.step(runId, stepId);
    return step?.status === 'pending' && step.stepName === stepName;
  }

  async pendingRunStartSlot(runId: string): Promise<number> {
    return this.pendingDispatchSlot(runId, 'run_created');
  }

  async markRunStartsPublished(runId: string, upToSlot: number): Promise<void> {
    this.markDispatchPublished(runId, upToSlot, 'run_created');
  }

  async pendingHookWakeSlot(runId: string): Promise<number> {
    return this.pendingDispatchSlot(runId, 'hook_received');
  }

  async markHookWakesPublished(runId: string, upToSlot: number): Promise<void> {
    this.markDispatchPublished(runId, upToSlot, 'hook_received');
  }

  async pendingStepDispatchSlot(runId: string, stepId: string, stepName: string): Promise<number> {
    return this.isPendingStep(runId, stepId, stepName) ? this.pendingDispatchSlot(runId, 'step_created', stepId) : 0;
  }

  async markStepDispatchPublished(runId: string, slot: number): Promise<void> {
    this.markDispatchSlotPublished(runId, slot, 'step_created');
  }

  async pendingWaitDispatchSlot(runId: string, correlationId: string): Promise<number> {
    if (this.run(runId)?.status !== 'running' || this.wait(runId, correlationId)?.status !== 'waiting') return 0;
    return this.pendingDispatchSlot(runId, 'wait_created', correlationId);
  }

  async markWaitDispatchPublished(runId: string, slot: number): Promise<void> {
    this.markDispatchSlotPublished(runId, slot, 'wait_created');
  }

  async reopenPublishedHookWakes(runId: string): Promise<number> {
    const run = this.run(runId);
    if (!run || isTerminalWorkflowRunStatus(run.status)) return 0;
    const nextAt = Date.now() + 10_000;
    const reopened = this.reopenDispatch(runId, nextAt, run.status === 'pending' ? 'run_created' : 'hook_received');
    if (reopened) await this.armAlarm(nextAt, 'DLQ wake recovery');
    return reopened;
  }

  async reopenPublishedStepDispatch(runId: string, stepId: string, stepName: string): Promise<number> {
    if (!this.isPendingStep(runId, stepId, stepName)) return 0;
    const nextAt = Date.now() + 10_000;
    const reopened = this.reopenDispatch(runId, nextAt, 'step_created', stepId);
    if (reopened) await this.armAlarm(nextAt, 'DLQ step recovery');
    return reopened;
  }

  async reopenPublishedWaitDispatch(runId: string, correlationId: string): Promise<number> {
    if (this.run(runId)?.status !== 'running') return 0;
    const wait = this.wait(runId, correlationId);
    if (!wait || wait.status !== 'waiting') return 0;
    const nextAt = Math.max(Date.now() + 10_000, wait.resumeAt?.getTime() ?? 0);
    const reopened = this.reopenDispatch(runId, nextAt, 'wait_created', correlationId);
    if (reopened) await this.armAlarm(nextAt, 'DLQ wait recovery');
    return reopened;
  }

  async drainOutbox(): Promise<number> {
    const sql = this.ctx.storage.sql;
    const due = sql.exec<OutboxRow>(
      `SELECT o.run_id, o.slot, o.queue_name, o.deployment_id, o.attempts, o.redrive, o.next_at, e.kind, e.correlation_id
       FROM dispatch_outbox o JOIN events e ON e.run_id = o.run_id AND e.slot = o.slot
       WHERE o.status = 'pending' AND o.next_at <= ? ORDER BY o.next_at LIMIT 50`,
      Date.now(),
    ).toArray();
    for (const row of due) {
      if (row.kind === 'run_created' && this.run(row.run_id)?.status !== 'pending') {
        await this.markRunStartsPublished(row.run_id, row.slot);
        continue;
      }
      const step = row.kind === 'step_created' && row.correlation_id
        ? this.step(row.run_id, row.correlation_id) : undefined;
      if (row.kind === 'step_created' &&
          (!step || step.status !== 'pending' || this.run(row.run_id)?.status !== 'running')) {
        await this.markStepDispatchPublished(row.run_id, row.slot);
        continue;
      }
      const wait = row.kind === 'wait_created' && row.correlation_id
        ? this.wait(row.run_id, row.correlation_id) : undefined;
      const runStatus = wait ? this.run(row.run_id)?.status : undefined;
      if (row.kind === 'wait_created' &&
          (!wait || wait.status !== 'waiting' || !runStatus || isTerminalWorkflowRunStatus(runStatus))) {
        await this.markWaitDispatchPublished(row.run_id, row.slot);
        continue;
      }
      if (wait && runStatus !== 'running') {
        sql.exec(`UPDATE dispatch_outbox SET next_at = ? WHERE run_id = ? AND slot = ? AND status = 'pending'`,
          Date.now() + 1_000, row.run_id, row.slot);
        continue;
      }
      // Step dispatch must use the SDK key, including the queue-name prefix.
      let hash = 0x811c9dc5;
      if (step) for (let i = 0; i < step.stepName.length; i++) {
        hash ^= step.stepName.charCodeAt(i);
        hash = Math.imul(hash, 0x01000193);
      }
      const baseKey = step
        ? `${row.queue_name}:${step.stepId}:${(hash >>> 0).toString(16).padStart(8, '0')}`
        : wait ? `${row.queue_name}:wait-backup:${row.run_id}:${row.slot}`
        : `${row.kind === 'hook_received' ? 'hook-wake' : 'run-start'}:${row.run_id}:${row.slot}`;
      // Once a message reached the DLQ, its original key is spent forever.
      const key = row.redrive ? `${baseKey}:redrive-${row.redrive}` : baseKey;
      const encoded = stringify(step
        ? { runId: row.run_id, stepId: step.stepId, stepName: step.stepName }
        : wait ? { runId: row.run_id, waitContinuation: { correlationId: row.correlation_id, attempt: row.redrive } }
        : { runId: row.run_id });
      const [idHash, bodyHash] = await Promise.all([
        crypto.subtle.digest('SHA-256', new TextEncoder().encode(key)),
        crypto.subtle.digest('SHA-256', new TextEncoder().encode(encoded)),
      ]);
      if (step && await this.pendingStepDispatchSlot(row.run_id, step.stepId, step.stepName) !== row.slot) continue;
      if (wait && (Date.now() < wait.resumeAt!.getTime() ||
          await this.pendingWaitDispatchSlot(row.run_id, row.correlation_id!) !== row.slot)) continue;
      try {
        await this.env.QUEUE_SHARD.getByName('flow').enqueue(
          `qmsg_${Buffer.from(idHash).toString('hex')}`, row.queue_name, encoded, 0,
          row.deployment_id, key, Buffer.from(bodyHash).toString('hex'),
        );
        if (step) await this.markStepDispatchPublished(row.run_id, row.slot);
        else if (wait) await this.markWaitDispatchPublished(row.run_id, row.slot);
        else if (row.kind === 'run_created') await this.markRunStartsPublished(row.run_id, row.slot);
        else await this.markHookWakesPublished(row.run_id, row.slot);
      } catch (error) {
        console.error('Workflow dispatch recovery failed', { runId: row.run_id, slot: row.slot, error });
        const delay = Math.min(60_000, (row.attempts + 1) ** 2 * 1_000);
        sql.exec(
          `UPDATE dispatch_outbox SET attempts = attempts + 1, next_at = ?
           WHERE run_id = ? AND slot = ? AND status = 'pending'`,
          Date.now() + delay, row.run_id, row.slot,
        );
      }
    }
    const next = sql.exec<{ next_at: number | null }>(
      `SELECT MIN(next_at) AS next_at FROM dispatch_outbox WHERE status = 'pending'`,
    ).one().next_at;
    if (next !== null) await this.ctx.storage.setAlarm(Math.max(Date.now() + 1_000, next));
    return due.length;
  }

  async alarm(): Promise<void> {
    await this.drainOutbox();
  }

  async createEvent(runId: string | null, request: AnyEventRequest, params?: CreateEventParams): Promise<EventResult> {
    // No awaits inside this transaction: slot, event, and materialized state commit together.
    const result = this.ctx.storage.transactionSync(() => {
      const data = CreateEventSchema.parse(request);
      if (params?.inBand || params?.expectedSeqInBand !== undefined) {
        throw new Error('In-band writer fencing is not supported');
      }
      if (data.eventType === 'run_created' && runId === null) runId = `wrun_${ulid()}`;
      if (!runId) throw new Error('runId is required');
      if (data.eventType !== 'hook_received' && (params?.resumeId || params?.resumePayloadDigest)) {
        throw new Error('resumeId is valid only for hook_received');
      }
      if (data.eventType === 'hook_received' && params?.resumeId && !params.resumePayloadDigest) {
        throw new Error('resumePayloadDigest is required with resumeId');
      }
      if (data.eventType === 'hook_received' && params?.resumeId) {
        const existing = this.ctx.storage.sql.exec<EventRow & { resume_digest: string }>(
          'SELECT * FROM events WHERE run_id = ? AND resume_id = ?', runId, params.resumeId
        ).toArray()[0];
        if (existing) {
          if (existing.resume_digest !== params.resumePayloadDigest || existing.correlation_id !== data.correlationId) {
            conflict('resumeId reused with different hook or payload');
          }
          return { event: this.event(existing, params.resolveData), allocated: 0 };
        }
      }
      const sql = this.ctx.storage.sql;
      const now = new Date();
      let run = this.run(runId);
      const specVersion = data.specVersion ?? run?.specVersion ?? mintedSpecVersion();
      let step: Step | undefined;
      let hook: Hook | undefined;
      let wait: Wait | undefined;
      let stepCreated: true | undefined;
      let synthetic: AnyEventRequest | undefined;

      // A lazy run_started carries the run itself; journal the run_created it implies.
      if (data.eventType === 'run_started' && !run && data.eventData?.deploymentId &&
          data.eventData.workflowName && data.eventData.input !== undefined) {
        const eventData = data.eventData;
        if (eventData.dynamicWorkflowCodeRef) throw new Error('Dynamic workflow code refs are not supported');
        validateAttributeChanges(attributeChanges(eventData.attributes),
          { allowReservedAttributes: eventData.allowReservedAttributes === true });
        synthetic = { eventType: 'run_created', specVersion, eventData: {
          deploymentId: eventData.deploymentId!, workflowName: eventData.workflowName!, input: eventData.input,
          executionContext: eventData.executionContext, attributes: eventData.attributes,
          allowReservedAttributes: eventData.allowReservedAttributes,
          encryptionPublicKey: eventData.encryptionPublicKey,
        } };
        run = {
          runId, status: 'pending', deploymentId: eventData.deploymentId!, workflowName: eventData.workflowName!,
          input: eventData.input, dynamicWorkflowCode: eventData.dynamicWorkflowCode,
          executionContext: eventData.executionContext, attributes: eventData.attributes ?? {},
          encryptionPublicKey: eventData.encryptionPublicKey, specVersion, createdAt: now, updatedAt: now,
        };
      } else if (data.eventType === 'run_created') {
        const eventData = data.eventData;
        if (run) conflict(`Workflow run ${runId} already exists`);
        if (eventData.dynamicWorkflowCodeRef) throw new Error('Dynamic workflow code refs are not supported');
        validateAttributeChanges(attributeChanges(eventData.attributes),
          { allowReservedAttributes: eventData.allowReservedAttributes === true });
        run = {
          runId, status: 'pending', deploymentId: eventData.deploymentId, workflowName: eventData.workflowName,
          input: eventData.input, executionContext: eventData.executionContext,
          dynamicWorkflowCode: eventData.dynamicWorkflowCode, attributes: eventData.attributes ?? {},
          encryptionPublicKey: eventData.encryptionPublicKey, specVersion, createdAt: now, updatedAt: now,
        };
      }
      if (!run) throw new WorkflowRunNotFoundError(runId);
      if (isTerminalWorkflowRunStatus(run.status)) {
        if (data.eventType === 'run_cancelled' && run.status === 'cancelled') return { run, allocated: 0 };
        if (data.eventType === 'hook_received') {
          throw new RunExpiredError(`Run ${runId} no longer accepts hook events`, runId, run.status);
        }
        // Retained hooks remain disposable; in-flight steps may still finish after cancellation.
        const finishesRunningStep = (data.eventType === 'step_completed' || data.eventType === 'step_failed' ||
          data.eventType === 'step_retrying') && this.step(runId, data.correlationId)?.status === 'running';
        if (data.eventType !== 'hook_disposed' && !finishesRunningStep) conflict(`Run ${runId} is terminal`);
      }
      if (data.eventType === 'run_started' && run.status === 'running') return { run, allocated: 0 };
      let nextRun: WorkflowRun = run;
      if (data.eventType === 'run_started') {
        nextRun = { ...run, status: 'running', output: undefined, error: undefined, completedAt: undefined,
          startedAt: run.startedAt ?? now, updatedAt: now };
      }
      if (data.eventType === 'run_completed') {
        nextRun = { ...run, status: 'completed', error: undefined, output: data.eventData.output,
          completedAt: now, updatedAt: now };
      }
      if (data.eventType === 'run_failed') {
        nextRun = { ...run, status: 'failed', output: undefined, error: data.eventData.error,
          errorCode: data.eventData.errorCode, completedAt: now, updatedAt: now };
      }
      if (data.eventType === 'run_cancelled') {
        nextRun = { ...run, status: 'cancelled', output: undefined, error: undefined, completedAt: now, updatedAt: now };
      }
      if (data.eventType === 'attr_set') {
        validateAttributeEventDataSize(data.eventData);
        validateAttributeChanges(data.eventData.changes, {
          existingKeys: Object.keys(run.attributes ?? {}),
          allowReservedAttributes: data.eventData.allowReservedAttributes === true,
        });
        nextRun = { ...run, attributes: applyAttributeChanges(run.attributes, data.eventData.changes), updatedAt: now };
      }

      if (data.eventType === 'step_created' || data.eventType === 'step_started' ||
          data.eventType === 'step_completed' || data.eventType === 'step_failed' || data.eventType === 'step_retrying') {
        const id = data.correlationId;
        const existing = this.step(runId, id);
        // A lazy step_started carries the step's input and creates the step in the same write.
        if (data.eventType === 'step_created' || (data.eventType === 'step_started' && data.eventData?.input !== undefined)) {
          if (existing) conflict(`Step ${id} already created`);
          const input = data.eventData!.input;
          const stepName = data.eventData!.stepName;
          if (!stepName) throw new Error('Lazy step_started requires stepName');
          step = { runId, stepId: id, stepName, status: 'pending', input, attempt: 0, specVersion,
            createdAt: now, updatedAt: now };
          if (data.eventType === 'step_started') {
            synthetic = { eventType: 'step_created', correlationId: id, specVersion,
              eventData: { stepName, input: input! } };
            stepCreated = true;
          }
        } else {
          if (!existing) throw new Error(`Step ${id} not found`);
          step = existing;
        }
        if (!step) throw new Error(`Step ${id} not found`);
        if (data.eventType !== 'step_created') {
          if (step.status === 'completed' || step.status === 'failed' || step.status === 'cancelled') {
            conflict(`Step ${id} is terminal`);
          }
          if (data.eventType === 'step_started') {
            if (step.retryAfter && step.retryAfter.getTime() > now.getTime()) {
              throw new Error(`Step ${id} retryAfter has not elapsed`);
            }
            step = { ...step, status: 'running', attempt: step.attempt + 1, startedAt: step.startedAt ?? now,
              retryAfter: undefined, updatedAt: now };
          } else if (data.eventType === 'step_completed') {
            if (step.status !== 'running') conflict(`Step ${id} is not running`);
            step = { ...step, status: 'completed', output: data.eventData.result, completedAt: now, updatedAt: now };
          } else if (data.eventType === 'step_failed') {
            if (step.status !== 'running') conflict(`Step ${id} is not running`);
            step = { ...step, status: 'failed', error: data.eventData.error, completedAt: now, updatedAt: now };
          } else if (data.eventType === 'step_retrying') {
            if (step.status !== 'running') conflict(`Step ${id} is not running`);
            step = { ...step, status: 'pending', error: data.eventData.error, retryAfter: data.eventData.retryAfter,
              updatedAt: now };
          }
        }
      }
      if (data.eventType === 'hook_created') {
        const eventData = data.eventData;
        if (eventData.force) throw new Error('Forced hook claims are not supported');
        if (this.hook(data.correlationId)) conflict(`Hook ${data.correlationId} already exists`);
        // Reclaim an expired retention before testing the globally unique token.
        const owner = sql.exec<{ data: string }>('SELECT data FROM hooks WHERE token = ?', eventData.token).toArray()[0];
        if (owner) {
          const claimed = decode<Hook>(owner.data);
          const owningRun = this.run(claimed.runId);
          if (owningRun && isTerminalWorkflowRunStatus(owningRun.status) &&
            (!claimed.tokenRetentionUntil || claimed.tokenRetentionUntil.getTime() <= now.getTime())) {
            sql.exec('DELETE FROM hooks WHERE id = ?', claimed.hookId);
          } else {
            const result = this.append(runId, {
              eventType: 'hook_conflict', correlationId: data.correlationId, specVersion,
              eventData: { token: eventData.token, conflictingRunId: claimed.runId },
            }, now);
            return this.decorate(result, nextRun, undefined, undefined, undefined, params);
          }
        }
        hook = {
          runId, hookId: data.correlationId, token: eventData.token,
          ownerId: 'cloudflare', projectId: 'cloudflare', environment: 'cloudflare',
          metadata: eventData.metadata, isWebhook: eventData.isWebhook ?? true,
          isSystem: eventData.isSystem ?? false, tokenRetentionUntil: eventData.tokenRetentionUntil,
          specVersion, createdAt: now,
          resumeContext: {
            deploymentId: nextRun.deploymentId, workflowName: nextRun.workflowName,
            runSpecVersion: nextRun.specVersion, encryptionPublicKey: nextRun.encryptionPublicKey,
          },
        };
      }
      if (data.eventType === 'hook_received' || data.eventType === 'hook_disposed') {
        hook = this.hook(data.correlationId);
        if (!hook || hook.runId !== runId) throw new Error(`Hook ${data.correlationId} not found`);
      }
      if (data.eventType === 'wait_created') {
        if (this.wait(runId, data.correlationId)) conflict(`Wait ${data.correlationId} already exists`);
        wait = { runId, waitId: `${runId}-${data.correlationId}`, status: 'waiting', resumeAt: data.eventData.resumeAt,
          createdAt: now, updatedAt: now, specVersion };
      }
      if (data.eventType === 'wait_completed') {
        wait = this.wait(runId, data.correlationId);
        if (!wait) throw new Error(`Wait ${data.correlationId} not found`);
        if (wait.status === 'completed') conflict(`Wait ${data.correlationId} already completed`);
        wait = { ...wait, status: 'completed', completedAt: now, updatedAt: now };
      }

      if (synthetic) this.append(runId, synthetic, now);
      // Dynamic source lives only on the run, not in the event log.
      let journal: AnyEventRequest = data;
      if (data.eventType === 'run_created') {
        const { dynamicWorkflowCode: _code, dynamicWorkflowCodeRef: _ref, ...rest } = data.eventData;
        journal = { ...data, eventData: rest };
      } else if (data.eventType === 'run_started') {
        journal = { eventType: 'run_started', specVersion: data.specVersion };
      } else if (data.eventType === 'step_started' && stepCreated) {
        const { input: _input, ...rest } = data.eventData!;
        journal = { ...data, eventData: rest };
      }
      const inserted = this.append(runId, { ...journal, specVersion }, now, params);
      this.persistRun(nextRun);
      // Dispatch debt commits with its event, so a crash before the queue publish cannot strand the run.
      if (DISPATCH_KINDS.has(data.eventType)) {
        const queueName = ValidQueueName.parse(
          `${getQueueTopicPrefix('workflow', getRunQueueNamespace(nextRun.executionContext))}${nextRun.workflowName}`,
        );
        sql.exec('INSERT INTO dispatch_outbox (run_id, slot, queue_name, deployment_id, next_at) VALUES (?, ?, ?, ?, ?)',
          runId, Number(inserted.eventId.slice(5)), queueName, nextRun.deploymentId,
          data.eventType === 'wait_created' ? wait!.resumeAt!.getTime() : now.getTime());
      }
      if (data.eventType === 'run_started') this.markRunStartsPublished(runId, Number.MAX_SAFE_INTEGER);
      if (data.eventType === 'step_started') this.settleDispatches(runId, 'step_created', data.correlationId);
      if (data.eventType === 'wait_completed') this.settleDispatches(runId, 'wait_created', data.correlationId);
      if (step) {
        sql.exec(`INSERT INTO steps(run_id, id, data, created_at) VALUES (?, ?, ?, ?)
                  ON CONFLICT(run_id, id) DO UPDATE SET data = excluded.data`,
          runId, step.stepId, encode(step), step.createdAt.getTime());
      }
      if (hook && data.eventType === 'hook_created') {
        sql.exec('INSERT INTO hooks(id, run_id, token, data, created_at) VALUES (?, ?, ?, ?, ?)',
          hook.hookId, runId, hook.token, encode(hook), now.getTime());
      }
      if (data.eventType === 'hook_disposed') {
        sql.exec('DELETE FROM hooks WHERE id = ? AND run_id = ?', data.correlationId, runId);
      }
      if (wait) {
        sql.exec(`INSERT INTO waits(run_id, id, data) VALUES (?, ?, ?)
                  ON CONFLICT(run_id, id) DO UPDATE SET data = excluded.data`,
          runId, data.correlationId, encode(wait));
      }
      if (isTerminalWorkflowRunStatus(nextRun.status)) {
        sql.exec(`UPDATE dispatch_outbox SET status = 'published' WHERE run_id = ? AND status = 'pending'`, runId);
        sql.exec('DELETE FROM waits WHERE run_id = ?', runId);
        const hooks = sql.exec<{ id: string; data: string }>('SELECT id, data FROM hooks WHERE run_id = ?', runId).toArray();
        for (const entry of hooks) {
          const retained = decode<Hook>(entry.data).tokenRetentionUntil;
          if (!retained || retained.getTime() <= now.getTime()) sql.exec('DELETE FROM hooks WHERE id = ?', entry.id);
        }
      }
      const createdHook = data.eventType === 'hook_created' ? hook : undefined;
      return this.decorate(inserted, nextRun, step, createdHook, wait, params, stepCreated);
    });
    if (result.event && runId) for (const wake of this.waiting.get(runId) ?? []) wake();
    if (result.event && DISPATCH_KINDS.has(request.eventType)) {
      const when = request.eventType === 'wait_created' && result.wait?.resumeAt
        ? Math.max(Date.now() + 1, result.wait.resumeAt.getTime()) : Date.now() + 5_000;
      await this.armAlarm(when, 'dispatch recovery');
    }
    return result;
  }

  /** Appends at the next dense slot. The payload field is stored as a BLOB beside its metadata. */
  private append(runId: string, data: AnyEventRequest | HookConflictRequest, now: Date, params?: CreateEventParams): Event {
    const sql = this.ctx.storage.sql;
    const slot = sql.exec<{ slot: number }>(
      'SELECT COALESCE(MAX(slot), 0) + 1 AS slot FROM events WHERE run_id = ?', runId,
    ).one().slot;
    const field = getEventDataPayloadField(data.eventType);
    const metadata = 'eventData' in data ? { ...data.eventData } as Record<string, unknown> : undefined;
    const payload = field && metadata ? metadata[field] : undefined;
    if (payload !== undefined && !(payload instanceof Uint8Array)) {
      throw new Error(`Expected Uint8Array for ${data.eventType}.${field}`);
    }
    if (field && metadata) delete metadata[field];
    const event: EventRow = {
      run_id: runId, slot, kind: data.eventType,
      correlation_id: 'correlationId' in data ? data.correlationId ?? null : null,
      spec_version: data.specVersion ?? mintedSpecVersion(),
      created_at: (params?.occurredAt ?? now).getTime(), occurred_at: params?.occurredAt?.getTime() ?? null,
      resume_id: params?.resumeId ?? null,
      data: metadata === undefined ? null : encode(metadata),
      payload: payload instanceof Uint8Array ? new Uint8Array(payload).buffer : null,
    };
    sql.exec(
      `INSERT INTO events(run_id, slot, kind, correlation_id, spec_version, created_at, occurred_at,
                          resume_id, resume_digest, data, payload)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      runId, slot, event.kind, event.correlation_id, event.spec_version, event.created_at, event.occurred_at,
      event.resume_id, params?.resumePayloadDigest ?? null, event.data, event.payload,
    );
    return this.event(event);
  }

  private decorate(
    event: Event, run: WorkflowRun, step?: Step, hook?: Hook, wait?: Wait,
    params?: CreateEventParams, stepCreated?: true,
  ): EventResult {
    const result: EventResult = {
      event, allocated: stepCreated ? 2 : 1,
      ...(event.eventType.startsWith('run_') || event.eventType === 'attr_set' ? { run } : {}),
      ...(step ? { step } : {}), ...(hook ? { hook } : {}), ...(wait ? { wait } : {}),
      ...(stepCreated ? { stepCreated } : {}),
    };
    if (params?.sinceCursor) {
      const page = this.eventsPage({
        runId: event.runId, resolveData: params.resolveData,
        pagination: { cursor: params.sinceCursor, sortOrder: 'asc', limit: 1000 },
      });
      return { ...result, events: page.data, cursor: page.cursor, hasMore: page.hasMore };
    }
    // A stale writer gets the events it skipped.
    if (params?.eventCount !== undefined && Number.isSafeInteger(params.eventCount) && params.eventCount >= 0) {
      const slot = Number(event.eventId.slice(5));
      if (slot > params.eventCount + 1) {
        const missed = this.ctx.storage.sql.exec<EventRow>(
          'SELECT * FROM events WHERE run_id = ? AND slot > ? AND slot < ? ORDER BY slot ASC',
          event.runId, params.eventCount, slot,
        ).toArray();
        return { ...result, events: missed.map((row) => this.event(row, params.resolveData)), cursor: null, hasMore: false };
      }
    }
    return result;
  }
}
