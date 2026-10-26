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
  DISPATCH_KINDS, DISPATCH_RECOVERY_DELAY_MS, EVENT_POLL_BATCH, EVENT_POLL_MS, MAX_TERMINAL_WAIT_MS,
  MIN_ALARM_DELAY_MS, REDRIVE_DELAY_MS, RELEASE_KINDS, STORE_NAME,
} from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { EventType, MessageKind, RunStatus, TokenClaim, WaitStatus } from '../enums.js';
import { toJson } from '../lib/json.js';
import { transientAsWorldError } from '../lib/transient.js';
import { unreachable } from '../lib/unreachable.js';
import { Waiters } from '../lib/waiters.js';
import { shardPusher } from '../queue/push.js';
import { queueShardFor } from '../queue/shards.js';
import { storeShardFor } from './route.js';
import type {
  DispatchKind, EnqueueResult, EventOf, HeldToken, OutboxClaim, QueueDispatch, ResolveDataMode, TokenRelease, WorldEnv,
} from '../types.js';
import { Db } from './db.js';
import { drainOutbox, isPendingStep, pendingStepSlot, pendingWaitSlot } from './dispatch.js';
import { applyEvent } from './events/apply.js';
import { checkBatch } from './events/batch.js';
import { newHook } from './events/hooks.js';
import type { Tokens } from './events/types.js';
import { Outbox } from './outbox.js';
import { listEvents, listRecords } from './pages.js';
import { toEvent } from './rows.js';
import { TokenOutbox } from './token-outbox.js';
import { TokenRegistry } from './tokens.js';
import { hookView, runView, stepView } from './views.js';

/** Storage key a token shard sets once it has imported the legacy store's tokens. */
const LEGACY_TOKENS_IMPORTED = 'legacy-tokens-imported';

/** A write that frees hook tokens: a disposal, or the run finishing. */
function freesTokens(data: AnyEventRequest, result: EventResult): boolean {
  if (result.event === undefined) return false;
  return data.eventType === EventType.HookDisposed || (result.run !== undefined && isTerminalWorkflowRunStatus(result.run.status));
}

/**
 * A store shard, a SQLite Durable Object holding the runs that hash to it (`storeShardFor(runId)`):
 * their event logs and materialized state, whose event slots, state changes and outbox rows commit
 * in one transaction. Each shard is also the registry for the hook tokens that hash to it
 * (`storeShardFor(token)`), which keeps a token unique across runs.
 *
 * The store named `STORE_NAME` is the legacy store: it still holds every run created before store
 * shards, with their hooks. Retained executors call these methods over RPC, so keep names and
 * signatures stable.
 */
export class WorldStore extends DurableObject<WorldEnv> {
  readonly #db: Db;
  readonly #outbox: Outbox;
  readonly #pushQueue: ReturnType<typeof shardPusher>;
  readonly #releases: TokenOutbox;
  readonly #tokens: TokenRegistry;
  readonly #waiters = new Waiters();
  readonly #isLegacy: boolean;
  /** The token release push in flight, if any. */
  #pushing: Promise<void> | undefined;

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    this.#db = new Db(ctx.storage.sql);
    this.#outbox = new Outbox(ctx.storage.sql);
    this.#pushQueue = shardPusher(env.QUEUE_SHARD);
    this.#releases = new TokenOutbox(ctx.storage.sql);
    this.#tokens = new TokenRegistry(ctx.storage.sql);
    this.#isLegacy = ctx.id.name === STORE_NAME;
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

  /** For `hooks.get`, which asks every store. The legacy store skips the old directory's copies of sharded hooks. */
  async findHook(hookId: string, resolveData?: ResolveDataMode): Promise<Hook | undefined> {
    const hook = this.#db.findHook(hookId);
    if (hook === undefined || (this.#isLegacy && this.#db.isRunCopy(hook.runId))) return undefined;
    return hookView(hook, resolveData);
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
    let result: EventResult;
    try {
      // No awaits inside: the slot, event, state and outbox rows commit together.
      result = this.ctx.storage.transactionSync(() => applyEvent(this.#db, this.#outbox, this.#releases, tokens, runId, data, params));
    } catch (error) {
      // A token granted for a write that did not commit, such as one racing the run's end, goes back,
      // unless a concurrent write of the same hook committed with it.
      if (tokens.kind === TokenClaim.Granted && this.#db.findHook(tokens.hook.hookId) === undefined) {
        this.#releases.owe(tokens.hook, true);
        this.ctx.waitUntil(this.#push());
      }
      throw error;
    }
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
    if (this.#isLegacy) this.#refuseRunCopy(runId);
    const parsed = events.map((item) => ({ ...item, event: CreateEventSchema.parse(item.event) }));
    await this.#armFor(parsed.map(({ event }) => event));
    const storage = this.ctx.storage;
    // A batch cannot hold hook_created, so it never needs a token claim.
    const tokens: Tokens = { kind: TokenClaim.None };
    const outcomes = storage.transactionSync(() => parsed.map(({ event, occurredAt, computeInstanceId }) => {
      try {
        // A nested transaction is a savepoint, so a rejected event leaves nothing behind.
        return storage.transactionSync(() => applyEvent(this.#db, this.#outbox, this.#releases, tokens, runId, event, {
          resolveData: params.resolveData, requestId: params.requestId, occurredAt, computeInstanceId,
        }));
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
   * commit cannot strand an outbox row: a queue message per dispatch event, and token releases.
   * One alarm check covers all of them: each check is a storage round trip.
   */
  async #armFor(events: AnyEventRequest[]): Promise<void> {
    let at = Number.POSITIVE_INFINITY;
    for (const data of events) {
      if (data.eventType === EventType.WaitCreated) {
        at = Math.min(at, Math.max(Date.now() + 1, data.eventData.resumeAt.getTime()));
      } else if (DISPATCH_KINDS.has(data.eventType) || RELEASE_KINDS.has(data.eventType)) {
        at = Math.min(at, Date.now() + DISPATCH_RECOVERY_DELAY_MS);
      }
    }
    if (at !== Number.POSITIVE_INFINITY) await this.#armAlarm(at, 'dispatch recovery');
  }

  // Store shards and the legacy store.

  /** A store shard answers only for its own runs. The legacy store answers for any run, as before. */
  #own(runId: string): void {
    if (!this.#isLegacy) this.#db.getRun(runId);
  }

  /** Another store, or this one called directly. */
  #store(name: string) {
    return name === this.ctx.id.name ? this : transientAsWorldError(this.env.WORLD_STORE.getByName(name));
  }

  /**
   * Checks this store takes the write, and claims a new hook's token from the token's shard before
   * it. A store shard takes writes only for runs that hash to it; the legacy store refuses writes to
   * the old directory's index copies.
   */
  async #beforeWrite(runId: string | null, data: AnyEventRequest): Promise<Tokens> {
    if (this.#isLegacy) {
      if (runId === null) return { kind: TokenClaim.None };
      this.#refuseRunCopy(runId);
    } else if (runId === null || storeShardFor(runId) !== this.ctx.id.name) {
      throw new WorkflowRunNotFoundError(String(runId));
    }
    switch (data.eventType) {
      case EventType.HookCreated:
        return this.#claimToken(runId, data);
      case EventType.RunStarted:
        // A lazy start creates a run it cannot find, so a shard first rules out a legacy run.
        if (!this.#isLegacy && this.#db.findRun(runId) === undefined && await this.#store(STORE_NAME).holdsRun(runId)) {
          throw new WorkflowRunNotFoundError(runId);
        }
        return { kind: TokenClaim.None };
      default:
        return { kind: TokenClaim.None };
    }
  }

  /** The old directory's copy of a sharded run was only an index entry: it takes no writes. */
  #refuseRunCopy(runId: string): void {
    if (this.#db.isRunCopy(runId)) throw new Error(`Run ${runId} lives on a store shard, not the legacy store`);
  }

  /** Claims a new hook's token from the token's shard. A write the transaction rejects anyway claims nothing. */
  async #claimToken(runId: string, data: EventOf<EventType.HookCreated>): Promise<Tokens> {
    const run = this.#db.getRun(runId);
    if (isTerminalWorkflowRunStatus(run.status) || data.eventData.force || this.#db.findHook(data.correlationId)) {
      return { kind: TokenClaim.None };
    }
    const hook = newHook(run, data, data.specVersion ?? run.specVersion ?? mintedSpecVersion(), new Date());
    // Releases this store owes go first, so a run can claim again a token it just disposed of.
    await this.#push();
    const holderRunId = await this.#store(storeShardFor(hook.token)).claimHookToken(hook);
    return holderRunId === null ? { kind: TokenClaim.Granted, hook } : { kind: TokenClaim.Held, holderRunId };
  }

  /**
   * After a write: owed token releases go to their shards in the background, so a freed token is
   * free shortly after the write returns. A failed release stays owed and the alarm retries it.
   */
  async #afterWrite(freesTokens: boolean): Promise<void> {
    if (this.#releases.nextDueAt() !== null) {
      this.ctx.waitUntil(this.#push());
      return;
    }
    // A finished run usually owes nothing now, so its store need not wake for the recovery alarm.
    if (freesTokens && this.#outbox.nextDueAt() === null) await this.ctx.storage.deleteAlarm();
  }

  /** One push at a time; a release owed during a push rides its loop. A failed one arms the alarm. */
  #push(): Promise<void> {
    this.#pushing ??= this.#releases.push((name) => this.#store(name))
      .then((retryAt) => (retryAt === undefined ? undefined : this.#armAlarm(retryAt, 'token release retry')))
      .finally(() => { this.#pushing = undefined; });
    return this.#pushing;
  }

  // Token shard: the registry for the tokens that hash to this store, called by every store.

  /**
   * Claims a token for a store's new hook. Returns the run that holds it, or null once granted. A
   * store that is not the token's shard, such as the legacy store called by an older store, passes
   * the claim on.
   */
  async claimHookToken(hook: Hook): Promise<string | null> {
    const shard = storeShardFor(hook.token);
    if (shard !== this.ctx.id.name) return this.#store(shard).claimHookToken(hook);
    await this.#importLegacyTokens(shard);
    return this.ctx.storage.transactionSync(() => this.#tokens.claim(hook, new Date()));
  }

  /** Applies releases from the stores whose hooks held these tokens. */
  async releaseTokens(releases: TokenRelease[]): Promise<void> {
    const shards = new Set(releases.map(({ token }) => storeShardFor(token)));
    for (const shard of shards) {
      if (shard !== this.ctx.id.name) throw new Error('Token released on the wrong shard');
      await this.#importLegacyTokens(shard);
    }
    const now = new Date();
    this.ctx.storage.transactionSync(() => {
      for (const release of releases) this.#tokens.release(release, now);
    });
  }

  /** The hook holding `token`, read from the token's shard. */
  async getHookByToken(token: string, resolveData?: ResolveDataMode): Promise<Hook> {
    const shard = storeShardFor(token);
    if (shard !== this.ctx.id.name) return this.#store(shard).getHookByToken(token, resolveData);
    await this.#importLegacyTokens(shard);
    return hookView(this.#tokens.get(token), resolveData);
  }

  /**
   * Once per token shard, before its registry first answers: copies in the tokens the legacy store's
   * hooks hold, so they still conflict. Legacy runs claim new tokens on the shards too, so one copy
   * is enough. Other requests wait meanwhile, so a release that arrives during the copy applies after it.
   */
  async #importLegacyTokens(shard: string): Promise<void> {
    const imported = () => this.ctx.storage.kv.get(LEGACY_TOKENS_IMPORTED) !== undefined;
    if (imported()) return;
    let failure: { error: unknown } | undefined;
    await this.ctx.blockConcurrencyWhile(async () => {
      if (imported()) return;
      try {
        const held = await this.#store(STORE_NAME).legacyTokens(shard);
        this.ctx.storage.transactionSync(() => {
          this.#tokens.import(held);
          this.ctx.storage.kv.put(LEGACY_TOKENS_IMPORTED, Date.now());
        });
      } catch (error) {
        // Thrown from here it would reset the object; the request that needed the import fails instead.
        failure = { error };
      }
    });
    if (failure !== undefined) throw failure.error;
  }

  // Legacy store, called by store shards.

  /** The tokens this store's hooks hold that hash to `shard`, for that token shard's one-time import. */
  async legacyTokens(shard: string): Promise<HeldToken[]> {
    return this.#db.hooksWithTokens((token) => storeShardFor(token) === shard).map((hook) => {
      const run = this.#db.findRun(hook.runId);
      return { hook, finished: run !== undefined && isTerminalWorkflowRunStatus(run.status) };
    });
  }

  /** Whether the legacy store holds this run itself, rather than the old directory's index copy. */
  async holdsRun(runId: string): Promise<boolean> {
    return this.#db.findRun(runId) !== undefined && !this.#db.isRunCopy(runId);
  }

  /** Stores still running the version with a run index push their runs here. Runs have no index now, so it only accepts them. */
  async indexRun(_run: WorkflowRun, _slot: number, _released: string[]): Promise<void> {}

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

  /**
   * Publishes queue messages that settle this store's outbox rows: claims their slots, pushes them
   * to their queue shards, and settles the rows the queue accepted fresh, all in one call.
   */
  async dispatch(entries: QueueDispatch[]): Promise<EnqueueResult[]> {
    const slots = await this.pendingDispatchSlots(entries.map(({ claim }) => claim));
    const results = await this.#pushQueue(entries);
    await this.markDispatchesPublished(entries.flatMap(({ claim }, index) => {
      const slot = slots[index] ?? 0;
      const result = results[index];
      return slot && 'fresh' in result && result.fresh ? [{ claim, slot }] : [];
    }));
    return results;
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
    const drained = await drainOutbox(this.#db, this.#outbox, (key) => this.env.QUEUE_SHARD.getByName(queueShardFor(key)));
    await this.#push();
    const due = [this.#outbox.nextDueAt(), this.#releases.nextDueAt()].filter((at) => at !== null);
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

  /** Moves the alarm earlier when needed. On failure the cron covers the legacy store's rows, the next write a shard's. */
  async #armAlarm(at: number, purpose: string): Promise<void> {
    try {
      const alarm = await this.ctx.storage.getAlarm();
      if (alarm === null || alarm > at) await this.ctx.storage.setAlarm(at);
    } catch (error) {
      console.error(`Failed to schedule ${purpose}`, error);
    }
  }
}
