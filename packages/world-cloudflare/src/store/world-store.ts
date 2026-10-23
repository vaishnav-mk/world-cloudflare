import { DurableObject } from 'cloudflare:workers';
import { EntityConflictError } from '@workflow/errors';
import {
  isTerminalWorkflowRunStatus, slotToEventId,
  type AnyEventRequest, type BatchEventItemResult, type BatchEventRequest, type CreateEventBatchParams,
  type CreateEventParams, type Event, type EventBatchResult, type EventResult, type GetWorkflowRunParams, type Hook,
  type ListEventsByCorrelationIdParams, type ListEventsParams, type ListHooksParams, type ListWorkflowRunStepsParams,
  type ListWorkflowRunsParams, type PaginatedResponse, type Step, type StepWithoutData, type WorkflowRun,
  type WorkflowRunWithoutData,
} from '@workflow/world';
import migrations from '../../drizzle/store/migrations.js';
import {
  DISPATCH_KINDS, DISPATCH_RECOVERY_DELAY_MS, EVENT_POLL_BATCH, EVENT_POLL_MS, MAX_TERMINAL_WAIT_MS, MIN_ALARM_DELAY_MS,
  QUEUE_SHARD_NAME, REDRIVE_DELAY_MS,
} from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { EventType, MessageKind, RunStatus, WaitStatus } from '../enums.js';
import { toJson } from '../lib/json.js';
import { unreachable } from '../lib/unreachable.js';
import { Waiters } from '../lib/waiters.js';
import type { DispatchKind, OutboxClaim, ResolveDataMode, WorldEnv } from '../types.js';
import { Db } from './db.js';
import { drainOutbox, isPendingStep, pendingStepSlot, pendingWaitSlot } from './dispatch.js';
import { applyEvent } from './events/apply.js';
import { checkBatch } from './events/batch.js';
import { Outbox } from './outbox.js';
import { listEvents, listRecords } from './pages.js';
import { toEvent } from './rows.js';
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
    migrateOnStart(ctx, migrations);
  }

  // Runs.

  async getRun(id: string, params: GetWorkflowRunParams & { resolveData: 'none' }): Promise<WorkflowRunWithoutData>;
  async getRun(id: string, params?: GetWorkflowRunParams & { resolveData?: 'all' }): Promise<WorkflowRun>;
  async getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData>;
  async getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData> {
    return runView(this.#db.getRun(id), params?.resolveData);
  }

  getManyRuns(ids: readonly string[], resolveData?: ResolveDataMode): (WorkflowRun | WorkflowRunWithoutData | null)[] {
    return ids.map((id) => {
      const run = this.#db.findRun(id);
      return run === undefined ? null : runView(run, resolveData);
    });
  }

  async waitForTerminalRun(id: string, timeoutMs: number, resolveData?: ResolveDataMode): Promise<WorkflowRun | WorkflowRunWithoutData> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw new RangeError('Invalid status timeout');
    const current = await this.getRun(id, { resolveData });
    if (timeoutMs === 0 || isTerminalWorkflowRunStatus(current.status)) return current;
    await this.#waiters.wait(id, Math.min(timeoutMs, MAX_TERMINAL_WAIT_MS), () => {
      return isTerminalWorkflowRunStatus(this.#db.getRun(id).status);
    });
    return this.getRun(id, { resolveData });
  }

  async listRuns(params: ListWorkflowRunsParams = {}): Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>> {
    const page = listRecords<WorkflowRun>(this.#db, 'runs', params);
    return { ...page, data: page.data.map((run) => runView(run, params.resolveData)) };
  }

  // Steps.

  async getStep(runId: string, stepId: string, params: { resolveData: 'none' }): Promise<StepWithoutData>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: 'all' }): Promise<Step>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: ResolveDataMode }): Promise<Step | StepWithoutData>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: ResolveDataMode }): Promise<Step | StepWithoutData> {
    return stepView(this.#db.getStep(runId, stepId), params?.resolveData);
  }

  async listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>> {
    const page = listRecords<Step>(this.#db, 'steps', params);
    return { ...page, data: page.data.map((step) => stepView(step, params.resolveData)) };
  }

  // Hooks.

  async getHook(hookId: string, resolveData?: ResolveDataMode): Promise<Hook> {
    return hookView(this.#db.getHook(hookId), resolveData);
  }

  async getHookByToken(token: string, resolveData?: ResolveDataMode): Promise<Hook> {
    return hookView(this.#db.getHookByToken(token), resolveData);
  }

  async listHooks(params: ListHooksParams): Promise<PaginatedResponse<Hook>> {
    const page = listRecords<Hook>(this.#db, 'hooks', params);
    return { ...page, data: page.data.map((hook) => hookView(hook, params.resolveData)) };
  }

  // Events.

  async getEvent(runId: string, eventId: string, resolveData?: ResolveDataMode): Promise<Event> {
    const slot = Number(eventId.replace(/^evnt_/, ''));
    if (!Number.isSafeInteger(slot) || slot < 1 || slotToEventId(slot) !== eventId) throw new Error('Invalid event ID');
    return toEvent(this.#db.getEvent(runId, slot), resolveData);
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
    await this.#afterCommit([{ request, result }]);
    return result;
  }

  /**
   * Appends events in order in one transaction. Each event commits or is rejected on its own, with
   * the outcome its single `createEvent` would have had; a conflict is a 409 item.
   */
  async createEventBatch(
    runId: string, events: BatchEventRequest[], params: CreateEventBatchParams = {},
  ): Promise<EventBatchResult> {
    checkBatch(events);
    const storage = this.ctx.storage;
    const outcomes = storage.transactionSync(() => events.map(({ event, occurredAt, computeInstanceId }) => {
      try {
        // A nested transaction is a savepoint, so a rejected event leaves nothing behind.
        return storage.transactionSync(() => applyEvent(this.#db, this.#outbox, runId, event, {
          resolveData: params.resolveData, requestId: params.requestId, occurredAt, computeInstanceId,
        }));
      } catch (error) {
        if (EntityConflictError.is(error)) return error;
        throw error;
      }
    }));
    const results: BatchEventItemResult[] = [];
    const committed: { request: AnyEventRequest; result: EventResult }[] = [];
    for (const [index, outcome] of outcomes.entries()) {
      if (outcome instanceof Error || outcome.event === undefined) {
        const message = outcome instanceof Error ? outcome.message : 'The run is finished';
        results.push({ status: 409, error: 'conflict', message });
        continue;
      }
      committed.push({ request: events[index].event, result: outcome });
      const { event, run, step, wait } = outcome;
      results.push({ status: 200, event, run, step, wait });
    }
    await this.#afterCommit(committed);
    return { results };
  }

  /**
   * Wakes event subscribers and makes sure the queue messages the events owe get published. One
   * alarm check covers all of them: each check is a storage round trip.
   */
  async #afterCommit(committed: { request: AnyEventRequest; result: EventResult }[]): Promise<void> {
    let at = Number.POSITIVE_INFINITY;
    for (const { request, result } of committed) {
      if (result.event === undefined) continue;
      this.#waiters.wake(result.event.runId);
      if (!DISPATCH_KINDS.has(request.eventType)) continue;
      const resumeAt = request.eventType === EventType.WaitCreated ? result.wait?.resumeAt : undefined;
      at = Math.min(at, resumeAt ? Math.max(Date.now() + 1, resumeAt.getTime()) : Date.now() + DISPATCH_RECOVERY_DELAY_MS);
    }
    if (at !== Number.POSITIVE_INFINITY) await this.#armAlarm(at, 'dispatch recovery');
  }

  /** Replays events after `cursor` (a slot), or waits for the next commit. `done` once a finished run is drained. */
  async nextEvents(runId: string, cursor: number, filter?: string[]) {
    if (!Number.isSafeInteger(cursor) || cursor < 0) throw new RangeError('Invalid subscription cursor');
    const read = () => {
      const rows = this.#db.events(runId, cursor, EVENT_POLL_BATCH);
      const nextCursor = rows.at(-1)?.slot ?? cursor;
      return {
        events: rows.filter((row) => !filter || filter.includes(row.kind)).map((row) => ({
          eventId: slotToEventId(row.slot), eventType: row.kind, data: toJson(toEvent(row)),
        })),
        cursor: nextCursor,
        done: isTerminalWorkflowRunStatus(this.#db.getRun(runId).status) && nextCursor >= this.#db.lastSlot(runId),
      };
    };
    const page = read();
    if (page.cursor !== cursor || page.done) return page;
    await this.#waiters.wait(runId, EVENT_POLL_MS, () => this.#db.lastSlot(runId) > cursor);
    return read();
  }

  // Dispatch outbox, called by the queue client and the queue shard's dead-letter handler.

  /** The outbox slot each queue message would settle, 0 where none is owed, in one call. */
  async pendingDispatchSlots(claims: OutboxClaim[]): Promise<number[]> {
    return Promise.all(claims.map((claim) => {
      switch (claim.kind) {
        case MessageKind.Step:
          return this.pendingStepDispatchSlot(claim.runId, claim.stepId, claim.stepName);
        case MessageKind.Start:
          return this.pendingRunStartSlot(claim.runId);
        case MessageKind.HookWake:
          return this.pendingHookWakeSlot(claim.runId);
        case MessageKind.WaitContinuation:
          return this.pendingWaitDispatchSlot(claim.runId, claim.correlationId);
        default:
          return unreachable(claim);
      }
    }));
  }

  /** Settles the outbox rows of messages the queue accepted, in one call. */
  async markDispatchesPublished(published: { claim: OutboxClaim; slot: number }[]): Promise<void> {
    for (const { claim, slot } of published) {
      switch (claim.kind) {
        case MessageKind.Step:
          await this.markStepDispatchPublished(claim.runId, slot);
          break;
        case MessageKind.Start:
          await this.markRunStartsPublished(claim.runId, slot);
          break;
        case MessageKind.HookWake:
          await this.markHookWakesPublished(claim.runId, slot);
          break;
        case MessageKind.WaitContinuation:
          await this.markWaitDispatchPublished(claim.runId, slot);
          break;
        default:
          unreachable(claim);
      }
    }
  }

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
    const run = this.#db.findRun(runId);
    if (run === undefined || isTerminalWorkflowRunStatus(run.status)) return 0;
    // A run that never started lost its start message; a started one lost a hook wake.
    const kind = run.status === RunStatus.Pending ? EventType.RunCreated : EventType.HookReceived;
    return this.#reopen(runId, Date.now() + REDRIVE_DELAY_MS, kind);
  }

  async reopenPublishedStepDispatch(runId: string, stepId: string, stepName: string): Promise<number> {
    if (!isPendingStep(this.#db, runId, stepId, stepName)) return 0;
    return this.#reopen(runId, Date.now() + REDRIVE_DELAY_MS, EventType.StepCreated, stepId);
  }

  async reopenPublishedWaitDispatch(runId: string, correlationId: string): Promise<number> {
    const wait = this.#db.findWait(runId, correlationId);
    if (this.#db.findRun(runId)?.status !== RunStatus.Running || wait?.status !== WaitStatus.Waiting) return 0;
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
