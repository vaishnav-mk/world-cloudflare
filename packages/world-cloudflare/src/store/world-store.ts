import { DurableObject } from 'cloudflare:workers';
import { HookNotFoundError, WorkflowRunNotFoundError } from '@workflow/errors';
import {
  isTerminalWorkflowRunStatus, slotToEventId,
  type AnyEventRequest, type CreateEventParams, type Event, type EventResult, type GetWorkflowRunParams, type Hook,
  type ListEventsByCorrelationIdParams, type ListEventsParams, type ListHooksParams, type ListWorkflowRunStepsParams,
  type ListWorkflowRunsParams, type PaginatedResponse, type Step, type StepWithoutData, type WorkflowRun,
  type WorkflowRunWithoutData,
} from '@workflow/world';
import {
  DISPATCH_KINDS, DISPATCH_RECOVERY_DELAY_MS, EVENT_POLL_BATCH, EVENT_POLL_MS, MAX_TERMINAL_WAIT_MS, MIN_ALARM_DELAY_MS,
  QUEUE_SHARD_NAME, REDRIVE_DELAY_MS,
} from '../constants.js';
import { EventType, RunStatus, WaitStatus } from '../enums.js';
import { Waiters } from '../lib/waiters.js';
import type { DispatchKind, ResolveDataMode, WorldEnv } from '../types.js';
import { Db } from './db.js';
import { drainOutbox, isPendingStep, pendingStepSlot, pendingWaitSlot } from './dispatch.js';
import { applyEvent } from './events/apply.js';
import { Outbox } from './outbox.js';
import { listEvents, listRecords } from './pages.js';
import { toEvent } from './rows.js';
import { migrate } from './schema.js';
import { hookView, runView, stepView } from './views.js';

// shortcut: one object for all runs; shard per run with a token index once writes bottleneck.
/**
 * Every run's event log and materialized state, in one SQLite Durable Object.
 *
 * One object makes event slots, state changes and cross-run hook-token uniqueness transactional.
 * Retained executors call these methods over RPC, so keep their names and signatures stable.
 */
export class WorldStore extends DurableObject<WorldEnv> {
  readonly #db: Db;
  readonly #outbox: Outbox;
  readonly #waiters = new Waiters();

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    this.#db = new Db(ctx.storage.sql);
    this.#outbox = new Outbox(ctx.storage.sql);
    ctx.blockConcurrencyWhile(async () => migrate(ctx.storage.sql));
  }

  // Runs.

  async getRun(id: string, params: GetWorkflowRunParams & { resolveData: 'none' }): Promise<WorkflowRunWithoutData>;
  async getRun(id: string, params?: GetWorkflowRunParams & { resolveData?: 'all' }): Promise<WorkflowRun>;
  async getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData>;
  async getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData> {
    const run = this.#db.run(id);
    if (!run) throw new WorkflowRunNotFoundError(id);
    return runView(run, params?.resolveData);
  }

  getManyRuns(ids: readonly string[], resolveData?: ResolveDataMode): (WorkflowRun | WorkflowRunWithoutData | null)[] {
    return ids.map((id) => {
      const run = this.#db.run(id);
      return run ? runView(run, resolveData) : null;
    });
  }

  async waitForTerminalRun(id: string, timeoutMs: number, resolveData?: ResolveDataMode): Promise<WorkflowRun | WorkflowRunWithoutData> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw new RangeError('Invalid status timeout');
    const current = await this.getRun(id, { resolveData });
    if (timeoutMs === 0 || isTerminalWorkflowRunStatus(current.status)) return current;
    await this.#waiters.wait(id, Math.min(timeoutMs, MAX_TERMINAL_WAIT_MS), () => {
      const status = this.#db.run(id)?.status;
      return Boolean(status && isTerminalWorkflowRunStatus(status));
    });
    return this.getRun(id, { resolveData });
  }

  async listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>> {
    const page = listRecords<WorkflowRun>(this.#db, 'runs', params ?? {});
    return { ...page, data: page.data.map((run) => runView(run, params?.resolveData)) };
  }

  // Steps.

  async getStep(runId: string, stepId: string, params: { resolveData: 'none' }): Promise<StepWithoutData>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: 'all' }): Promise<Step>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: ResolveDataMode }): Promise<Step | StepWithoutData>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: ResolveDataMode }): Promise<Step | StepWithoutData> {
    const step = this.#db.step(runId, stepId);
    if (!step) throw new Error(`Step ${stepId} in run ${runId} not found`);
    return stepView(step, params?.resolveData);
  }

  async listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>> {
    const page = listRecords<Step>(this.#db, 'steps', params);
    return { ...page, data: page.data.map((step) => stepView(step, params.resolveData)) };
  }

  // Hooks.

  async getHook(hookId: string, resolveData?: ResolveDataMode): Promise<Hook> {
    const hook = this.#db.hook(hookId);
    if (!hook) throw new HookNotFoundError(hookId);
    return hookView(hook, resolveData);
  }

  async getHookByToken(token: string, resolveData?: ResolveDataMode): Promise<Hook> {
    const hook = this.#db.hookByToken(token);
    if (!hook) throw new HookNotFoundError(token);
    return hookView(hook, resolveData);
  }

  async listHooks(params: ListHooksParams): Promise<PaginatedResponse<Hook>> {
    const page = listRecords<Hook>(this.#db, 'hooks', params);
    return { ...page, data: page.data.map((hook) => hookView(hook, params.resolveData)) };
  }

  // Events.

  async getEvent(runId: string, eventId: string, resolveData?: ResolveDataMode): Promise<Event> {
    const slot = Number(eventId.replace(/^evnt_/, ''));
    if (!Number.isSafeInteger(slot) || slot < 1 || slotToEventId(slot) !== eventId) throw new Error('Invalid event ID');
    const row = this.#db.event(runId, slot);
    if (!row) throw new Error(`Event ${eventId} not found`);
    return toEvent(row, resolveData);
  }

  async listEvents(params: ListEventsParams): Promise<PaginatedResponse<Event>> {
    return listEvents(this.#db, params);
  }

  async listEventsByCorrelationId(params: ListEventsByCorrelationIdParams): Promise<PaginatedResponse<Event>> {
    return listEvents(this.#db, params);
  }

  async createEvent(runId: string | null, request: AnyEventRequest, params?: CreateEventParams): Promise<EventResult> {
    // No awaits inside: the slot, event, state and outbox row commit together.
    const result = this.ctx.storage.transactionSync(() => applyEvent(this.#db, this.#outbox, runId, request, params));
    if (!result.event) return result;
    this.#waiters.wake(result.event.runId);
    if (DISPATCH_KINDS.has(request.eventType)) {
      const resumeAt = request.eventType === EventType.WaitCreated ? result.wait?.resumeAt : undefined;
      const at = resumeAt ? Math.max(Date.now() + 1, resumeAt.getTime()) : Date.now() + DISPATCH_RECOVERY_DELAY_MS;
      await this.#armAlarm(at, 'dispatch recovery');
    }
    return result;
  }

  /** Replays events after `cursor` (a slot), or waits for the next commit. `done` once a finished run is drained. */
  async nextEvents(runId: string, cursor: number, filter?: string[]) {
    if (!Number.isSafeInteger(cursor) || cursor < 0) throw new RangeError('Invalid subscription cursor');
    if (!this.#db.run(runId)) throw new WorkflowRunNotFoundError(runId);
    const read = () => {
      const rows = this.#db.events(runId, cursor, EVENT_POLL_BATCH);
      const nextCursor = rows.at(-1)?.slot ?? cursor;
      return {
        events: rows.filter((row) => !filter || filter.includes(row.kind)).map((row) => ({
          eventId: slotToEventId(row.slot), eventType: row.kind, data: JSON.stringify(toEvent(row)),
        })),
        cursor: nextCursor,
        done: isTerminalWorkflowRunStatus(this.#db.run(runId)!.status) && nextCursor >= this.#db.lastSlot(runId),
      };
    };
    const page = read();
    if (page.cursor !== cursor || page.done) return page;
    await this.#waiters.wait(runId, EVENT_POLL_MS, () => this.#db.lastSlot(runId) > cursor);
    return read();
  }

  // Dispatch outbox, called by the queue client and the queue shard's dead-letter handler.

  async pendingRunStartSlot(runId: string): Promise<number> {
    return this.#outbox.pendingSlot(runId, EventType.RunCreated);
  }

  async markRunStartsPublished(runId: string, upToSlot: number): Promise<void> {
    this.#outbox.publishThrough(runId, upToSlot, EventType.RunCreated);
  }

  async pendingHookWakeSlot(runId: string): Promise<number> {
    return this.#outbox.pendingSlot(runId, EventType.HookReceived);
  }

  async markHookWakesPublished(runId: string, upToSlot: number): Promise<void> {
    this.#outbox.publishThrough(runId, upToSlot, EventType.HookReceived);
  }

  async pendingStepDispatchSlot(runId: string, stepId: string, stepName: string): Promise<number> {
    return pendingStepSlot(this.#db, this.#outbox, runId, stepId, stepName);
  }

  async markStepDispatchPublished(runId: string, slot: number): Promise<void> {
    this.#outbox.publishSlot(runId, slot, EventType.StepCreated);
  }

  async pendingWaitDispatchSlot(runId: string, correlationId: string): Promise<number> {
    return pendingWaitSlot(this.#db, this.#outbox, runId, correlationId);
  }

  async markWaitDispatchPublished(runId: string, slot: number): Promise<void> {
    this.#outbox.publishSlot(runId, slot, EventType.WaitCreated);
  }

  async reopenPublishedHookWakes(runId: string): Promise<number> {
    const run = this.#db.run(runId);
    if (!run || isTerminalWorkflowRunStatus(run.status)) return 0;
    // A run that never started lost its start message; a started one lost a hook wake.
    const kind = run.status === RunStatus.Pending ? EventType.RunCreated : EventType.HookReceived;
    return this.#reopen(runId, Date.now() + REDRIVE_DELAY_MS, kind);
  }

  async reopenPublishedStepDispatch(runId: string, stepId: string, stepName: string): Promise<number> {
    if (!isPendingStep(this.#db, runId, stepId, stepName)) return 0;
    return this.#reopen(runId, Date.now() + REDRIVE_DELAY_MS, EventType.StepCreated, stepId);
  }

  async reopenPublishedWaitDispatch(runId: string, correlationId: string): Promise<number> {
    const wait = this.#db.wait(runId, correlationId);
    if (this.#db.run(runId)?.status !== RunStatus.Running || wait?.status !== WaitStatus.Waiting) return 0;
    const at = Math.max(Date.now() + REDRIVE_DELAY_MS, wait.resumeAt?.getTime() ?? 0);
    return this.#reopen(runId, at, EventType.WaitCreated, correlationId);
  }

  async drainOutbox(): Promise<number> {
    const drained = await drainOutbox(this.#db, this.#outbox, this.env.QUEUE_SHARD.getByName(QUEUE_SHARD_NAME));
    const next = this.#outbox.nextDueAt();
    if (next !== null) await this.ctx.storage.setAlarm(Math.max(Date.now() + MIN_ALARM_DELAY_MS, next));
    return drained;
  }

  async alarm(): Promise<void> {
    await this.drainOutbox();
  }

  async #reopen(runId: string, at: number, kind: DispatchKind, correlationId?: string): Promise<number> {
    const reopened = this.#outbox.reopen(runId, at, kind, correlationId);
    if (reopened) await this.#armAlarm(at, 'redrive');
    return reopened;
  }

  /** Moves the alarm earlier when needed. The outbox row is already committed, so the cron covers a failure. */
  async #armAlarm(at: number, purpose: string): Promise<void> {
    try {
      const alarm = await this.ctx.storage.getAlarm();
      if (alarm === null || alarm > at) await this.ctx.storage.setAlarm(at);
    } catch (error) {
      console.error(`Failed to schedule ${purpose}`, error);
    }
  }
}
