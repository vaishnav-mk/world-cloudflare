import { DurableObject } from 'cloudflare:workers';
import { EntityConflictError, WorkflowRunNotFoundError } from '@workflow/errors';
import {
  CreateEventSchema, isTerminalWorkflowRunStatus, mintedSpecVersion, slotToEventId,
  type AnyEventRequest, type BatchEventItemResult, type BatchEventRequest, type CreateEventBatchParams,
  type CreateEventParams, type Event, type EventBatchResult, type EventResult, type GetWorkflowRunParams, type Hook,
  type ListEventsByCorrelationIdParams, type ListEventsParams, type ListHooksParams, type ListWorkflowRunStepsParams,
  type ListWorkflowRunsParams, type PaginatedResponse, type Step, type StepWithoutData, type WorkflowRun,
  type WorkflowRunWithoutData,
} from '@workflow/world';
import migrations from '../../drizzle/store/migrations.js';
import {
  DIRECTORY_KINDS, DISPATCH_KINDS, DISPATCH_RECOVERY_DELAY_MS, EVENT_POLL_BATCH, EVENT_POLL_MS, MAX_TERMINAL_WAIT_MS,
  MIN_ALARM_DELAY_MS, QUEUE_SHARD_NAME, REDRIVE_DELAY_MS, STORE_NAME,
} from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { EventType, MessageKind, RunStatus, TokenClaim, WaitStatus } from '../enums.js';
import { toJson } from '../lib/json.js';
import { transientAsWorldError } from '../lib/transient.js';
import { unreachable } from '../lib/unreachable.js';
import { Waiters } from '../lib/waiters.js';
import type { DispatchKind, EventOf, OutboxClaim, ResolveDataMode, WorldEnv } from '../types.js';
import { Db } from './db.js';
import { claimHookToken, indexRun } from './directory.js';
import { DirectoryOutbox } from './directory-outbox.js';
import { drainOutbox, isPendingStep, pendingStepSlot, pendingWaitSlot } from './dispatch.js';
import { applyEvent } from './events/apply.js';
import { checkBatch } from './events/batch.js';
import { newHook } from './events/hooks.js';
import type { Tokens } from './events/types.js';
import { Outbox } from './outbox.js';
import { listEvents, listRecords } from './pages.js';
import { toEvent } from './rows.js';
import { hookView, runView, stepView } from './views.js';

/** A write that frees hook tokens: a disposal, or the run finishing. */
function freesTokens(data: AnyEventRequest, result: EventResult): boolean {
  if (result.event === undefined) return false;
  return data.eventType === EventType.HookDisposed || (result.run !== undefined && isTerminalWorkflowRunStatus(result.run.status));
}

// shortcut: the directory is one object; it takes about three index pushes per run plus hook
// claims, so partition the index by run ID if it ever bottlenecks.
/**
 * A run's event log and materialized state, in a SQLite Durable Object named by the run ID. Each
 * run's store makes its event slots, state changes and outbox rows transactional.
 *
 * The store named `STORE_NAME` is the directory: it keeps an index copy of every run for
 * `runs.list` and `runs.getMany`, and the hook-token registry that keeps tokens unique across
 * runs. It also still holds every run created before per-run stores, with their own logic.
 * Retained executors call these methods over RPC on the directory, so keep names and signatures stable.
 */
export class WorldStore extends DurableObject<WorldEnv> {
  readonly #db: Db;
  readonly #outbox: Outbox;
  readonly #directoryOutbox: DirectoryOutbox;
  readonly #waiters = new Waiters();
  readonly #isDirectory: boolean;
  /** The push to the directory in flight, if any. */
  #pushing: Promise<void> | undefined;

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    this.#db = new Db(ctx.storage.sql);
    this.#outbox = new Outbox(ctx.storage.sql);
    this.#directoryOutbox = new DirectoryOutbox(ctx.storage.sql);
    this.#isDirectory = ctx.id.name === STORE_NAME;
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
    this.#own(runId);
    return stepView(this.#db.getStep(runId, stepId), params?.resolveData);
  }

  async listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>> {
    this.#own(params.runId);
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
    if (params.runId !== undefined) this.#own(params.runId);
    const page = listRecords<Hook>(this.#db, 'hooks', params);
    return { ...page, data: page.data.map((hook) => hookView(hook, params.resolveData)) };
  }

  // Events.

  async getEvent(runId: string, eventId: string, resolveData?: ResolveDataMode): Promise<Event> {
    this.#own(runId);
    const slot = Number(eventId.replace(/^evnt_/, ''));
    if (!Number.isSafeInteger(slot) || slot < 1 || slotToEventId(slot) !== eventId) throw new Error('Invalid event ID');
    return toEvent(this.#db.getEvent(runId, slot), resolveData);
  }

  async listEvents(params: ListEventsParams): Promise<PaginatedResponse<Event>> {
    this.#own(params.runId);
    return listEvents(this.#db, params);
  }

  async listEventsByCorrelationId(params: ListEventsByCorrelationIdParams): Promise<PaginatedResponse<Event>> {
    this.#own(params.runId);
    return listEvents(this.#db, params);
  }

  async createEvent(runId: string | null, request: AnyEventRequest, params?: CreateEventParams): Promise<EventResult> {
    const data = CreateEventSchema.parse(request);
    const tokens = await this.#beforeWrite(runId, data);
    await this.#armFor([data]);
    // No awaits inside: the slot, event, state and outbox rows commit together.
    const result = this.ctx.storage.transactionSync(() => {
      const result = applyEvent(this.#db, this.#outbox, tokens, runId, data, params);
      this.#oweDirectory(data, result);
      return result;
    });
    this.#wake([result]);
    await this.#afterWrite(freesTokens(data, result));
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
    this.#own(runId);
    if (this.#isDirectory) this.#refuseRunCopy(runId);
    const parsed = events.map((item) => ({ ...item, event: CreateEventSchema.parse(item.event) }));
    await this.#armFor(parsed.map(({ event }) => event));
    const storage = this.ctx.storage;
    // A batch cannot hold hook_created, so it never needs a token claim.
    const tokens: Tokens = this.#isDirectory ? { kind: TokenClaim.Local } : { kind: TokenClaim.None };
    const outcomes = storage.transactionSync(() => parsed.map(({ event, occurredAt, computeInstanceId }) => {
      try {
        // A nested transaction is a savepoint, so a rejected event leaves nothing behind.
        return storage.transactionSync(() => {
          const result = applyEvent(this.#db, this.#outbox, tokens, runId, event, {
            resolveData: params.resolveData, requestId: params.requestId, occurredAt, computeInstanceId,
          });
          this.#oweDirectory(event, result);
          return result;
        });
      } catch (error) {
        if (EntityConflictError.is(error)) return error;
        throw error;
      }
    }));
    const results: BatchEventItemResult[] = [];
    const committed: EventResult[] = [];
    let frees = false;
    for (const [index, outcome] of outcomes.entries()) {
      if (outcome instanceof Error || outcome.event === undefined) {
        const message = outcome instanceof Error ? outcome.message : 'The run is finished';
        results.push({ status: 409, error: 'conflict', message });
        continue;
      }
      committed.push(outcome);
      frees ||= freesTokens(parsed[index].event, outcome);
      const { event, run, step, wait } = outcome;
      results.push({ status: 200, event, run, step, wait });
    }
    this.#wake(committed);
    await this.#afterWrite(frees);
    return { results };
  }

  #wake(results: EventResult[]): void {
    for (const { event } of results) if (event !== undefined) this.#waiters.wake(event.runId);
  }

  /**
   * Arms the alarm for what these events will owe, before they commit, so a crash just after the
   * commit cannot strand an outbox row: a queue message per dispatch event, and a run store's push
   * to the directory. One alarm check covers all of them: each check is a storage round trip.
   */
  async #armFor(events: AnyEventRequest[]): Promise<void> {
    let at = Number.POSITIVE_INFINITY;
    for (const data of events) {
      if (data.eventType === EventType.WaitCreated) {
        at = Math.min(at, Math.max(Date.now() + 1, data.eventData.resumeAt.getTime()));
      } else if (DISPATCH_KINDS.has(data.eventType) || (!this.#isDirectory && DIRECTORY_KINDS.has(data.eventType))) {
        at = Math.min(at, Date.now() + DISPATCH_RECOVERY_DELAY_MS);
      }
    }
    if (at !== Number.POSITIVE_INFINITY) await this.#armAlarm(at, 'dispatch recovery');
  }

  // Per-run stores and the directory.

  /** A run store answers only for its own run. The directory answers for any run, as before. */
  #own(runId: string): void {
    if (!this.#isDirectory) this.#db.getRun(runId);
  }

  #directory() {
    return transientAsWorldError(this.env.WORLD_STORE.getByName(STORE_NAME));
  }

  /**
   * Checks this store takes the write, and how it learns about hook tokens. The directory decides
   * tokens itself, and refuses writes to runs it only indexes. A run store takes writes for its
   * one run, and claims a new hook's token from the directory before the write.
   */
  async #beforeWrite(runId: string | null, data: AnyEventRequest): Promise<Tokens> {
    if (this.#isDirectory) {
      if (runId !== null) this.#refuseRunCopy(runId);
      return { kind: TokenClaim.Local };
    }
    if (runId === null || runId !== this.ctx.id.name) throw new WorkflowRunNotFoundError(String(runId));
    switch (data.eventType) {
      case EventType.HookCreated:
        return this.#claimToken(runId, data);
      case EventType.RunStarted:
        // A lazy start creates a run it cannot find, so first rule out a run held by the directory.
        if (this.#db.findRun(runId) === undefined && await this.#directory().holdsRun(runId)) {
          throw new WorkflowRunNotFoundError(runId);
        }
        return { kind: TokenClaim.None };
      default:
        return { kind: TokenClaim.None };
    }
  }

  /** The directory's copy of a run that lives in its own store is only an index entry: it takes no writes. */
  #refuseRunCopy(runId: string): void {
    if (this.#db.isRunCopy(runId)) throw new Error(`Run ${runId} lives in its own store, not the directory`);
  }

  /** Claims a new hook's token. A write the transaction rejects anyway claims nothing. */
  async #claimToken(runId: string, data: EventOf<EventType.HookCreated>): Promise<Tokens> {
    const run = this.#db.getRun(runId);
    if (isTerminalWorkflowRunStatus(run.status) || data.eventData.force || this.#db.findHook(data.correlationId)) {
      return { kind: TokenClaim.None };
    }
    const hook = newHook(run, data, data.specVersion ?? run.specVersion ?? mintedSpecVersion(), new Date());
    const holderRunId = await this.#directory().claimHookToken(hook);
    return holderRunId === null ? { kind: TokenClaim.Granted, hook } : { kind: TokenClaim.Held, holderRunId };
  }

  /** Inside the write's transaction: a run store owes the directory its run row, and disposed hooks. */
  #oweDirectory(data: AnyEventRequest, result: EventResult): void {
    if (!this.#isDirectory) this.#directoryOutbox.owe(data, result);
  }

  /**
   * After a run store's write: a write that frees hook tokens waits for its push, so a freed token
   * is free once the write returns. The run index alone updates in the background, off the write's path.
   */
  async #afterWrite(freesTokens: boolean): Promise<void> {
    if (this.#isDirectory) return;
    if (freesTokens) await this.#push();
    else this.ctx.waitUntil(this.#push());
  }

  /**
   * One push at a time; a write that commits during a push rides its loop. A failed push stays
   * owed and the alarm retries it with backoff, so a stale index heals itself.
   */
  #push(): Promise<void> {
    this.#pushing ??= this.#directoryOutbox.push(this.#db, this.#directory())
      .then((retryAt) => (retryAt === undefined ? undefined : this.#armAlarm(retryAt, 'run index retry')))
      .finally(() => { this.#pushing = undefined; });
    return this.#pushing;
  }

  // Directory only, called by run stores.

  /** Records a run store's latest run row in the run index, and releases the tokens it gave up. */
  async indexRun(run: WorkflowRun, slot: number, released: string[]): Promise<void> {
    this.#requireDirectory();
    if (!Number.isSafeInteger(slot) || slot < 1) throw new RangeError('Invalid index slot');
    this.ctx.storage.transactionSync(() => indexRun(this.#db, run, slot, released, new Date()));
  }

  /** Claims a token for a run store's new hook. Returns the run that holds it, or null once granted. */
  async claimHookToken(hook: Hook): Promise<string | null> {
    this.#requireDirectory();
    return this.ctx.storage.transactionSync(() => claimHookToken(this.#db, hook, new Date()));
  }

  /** Whether the directory holds this run itself, rather than an index copy: a run created before per-run stores. */
  async holdsRun(runId: string): Promise<boolean> {
    this.#requireDirectory();
    return this.#db.findRun(runId) !== undefined && !this.#db.isRunCopy(runId);
  }

  #requireDirectory(): void {
    if (!this.#isDirectory) throw new Error('Only the directory keeps the run index and hook tokens');
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

  /**
   * Unlike other run-scoped calls, a run store answers 0 for a run it does not hold yet: `start()`
   * publishes the start message while `run_created` is still in flight, and only new runs have one.
   */
  async pendingRunStartSlot(runId: string): Promise<number> {
    return this.#outbox.pendingSlot(runId, EventType.RunCreated);
  }

  async markRunStartsPublished(runId: string, upToSlot: number): Promise<void> {
    this.#outbox.publishThrough(runId, upToSlot, EventType.RunCreated);
  }

  async pendingHookWakeSlot(runId: string): Promise<number> {
    this.#own(runId);
    return this.#outbox.pendingSlot(runId, EventType.HookReceived);
  }

  async markHookWakesPublished(runId: string, upToSlot: number): Promise<void> {
    this.#own(runId);
    this.#outbox.publishThrough(runId, upToSlot, EventType.HookReceived);
  }

  async pendingStepDispatchSlot(runId: string, stepId: string, stepName: string): Promise<number> {
    this.#own(runId);
    return pendingStepSlot(this.#db, this.#outbox, runId, stepId, stepName);
  }

  async markStepDispatchPublished(runId: string, slot: number): Promise<void> {
    this.#own(runId);
    this.#outbox.publishSlot(runId, slot, EventType.StepCreated);
  }

  async pendingWaitDispatchSlot(runId: string, correlationId: string): Promise<number> {
    this.#own(runId);
    return pendingWaitSlot(this.#db, this.#outbox, runId, correlationId);
  }

  async markWaitDispatchPublished(runId: string, slot: number): Promise<void> {
    this.#own(runId);
    this.#outbox.publishSlot(runId, slot, EventType.WaitCreated);
  }

  async reopenPublishedHookWakes(runId: string): Promise<number> {
    this.#own(runId);
    const run = this.#db.findRun(runId);
    if (run === undefined || isTerminalWorkflowRunStatus(run.status)) return 0;
    // A run that never started lost its start message; a started one lost a hook wake.
    const kind = run.status === RunStatus.Pending ? EventType.RunCreated : EventType.HookReceived;
    return this.#reopen(runId, Date.now() + REDRIVE_DELAY_MS, kind);
  }

  async reopenPublishedStepDispatch(runId: string, stepId: string, stepName: string): Promise<number> {
    this.#own(runId);
    if (!isPendingStep(this.#db, runId, stepId, stepName)) return 0;
    return this.#reopen(runId, Date.now() + REDRIVE_DELAY_MS, EventType.StepCreated, stepId);
  }

  async reopenPublishedWaitDispatch(runId: string, correlationId: string): Promise<number> {
    this.#own(runId);
    const wait = this.#db.findWait(runId, correlationId);
    if (this.#db.findRun(runId)?.status !== RunStatus.Running || wait?.status !== WaitStatus.Waiting) return 0;
    const at = Math.max(Date.now() + REDRIVE_DELAY_MS, wait.resumeAt?.getTime() ?? 0);
    return this.#reopen(runId, at, EventType.WaitCreated, correlationId);
  }

  async drainOutbox(): Promise<number> {
    const drained = await drainOutbox(this.#db, this.#outbox, this.env.QUEUE_SHARD.getByName(QUEUE_SHARD_NAME));
    if (!this.#isDirectory) await this.#push();
    const due = [this.#outbox.nextDueAt(), this.#directoryOutbox.nextDueAt()].filter((at) => at !== null);
    if (due.length) await this.ctx.storage.setAlarm(Math.max(Date.now() + MIN_ALARM_DELAY_MS, Math.min(...due)));
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

  /** Moves the alarm earlier when needed. On failure the cron covers the directory's rows, the next write a run store's. */
  async #armAlarm(at: number, purpose: string): Promise<void> {
    try {
      const alarm = await this.ctx.storage.getAlarm();
      if (alarm === null || alarm > at) await this.ctx.storage.setAlarm(at);
    } catch (error) {
      console.error(`Failed to schedule ${purpose}`, error);
    }
  }
}
