import { DurableObject } from 'cloudflare:workers';
import type {
  AnyEventRequest, BatchEventRequest, CreateEventBatchParams, CreateEventParams, Event, EventBatchResult, EventListResponse,
  EventResult, GetWorkflowRunParams, Hook, HookClaimedFrom, ListEventsByCorrelationIdParams, ListEventsParams, ListHooksParams,
  ListWorkflowRunStepsParams, ListWorkflowRunsParams, PaginatedResponse, Step, StepWithoutData, WorkflowRun,
  WorkflowRunWithoutData,
} from '@workflow/world';
import migrations from '../../drizzle/store/migrations.js';
import { MIN_ALARM_DELAY_MS } from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { MessageKind } from '../enums.js';
import { holdsRuns, objectOptions } from '../lib/shards.js';
import { transientAsWorldError } from '../lib/transient.js';
import type {
  EnqueueResult, HeldToken, HookClaimDecision, HookClaimOperation, IndexAnswer, IndexBatch, IndexPage, OutboxClaim, QueueDispatch, ResolveDataMode, TokenRelease, WorldEnv,
} from '../types.js';
import { StoreContext } from './context.js';
import * as dispatch from './dispatch.js';
import type { TokenAnswer } from './events/types.js';
import * as forceClaims from './force-claims.js';
import * as legacy from './legacy.js';
import * as reads from './reads.js';
import { recoverIntents } from './token-claims.js';
import * as tokenShard from './token-shard.js';
import * as writes from './writes.js';

/**
 * A store shard, a SQLite Durable Object holding the runs that hash to it (`storeShardFor(runId)`):
 * their event logs and materialized state, whose event slots, state changes and outbox rows commit
 * in one transaction. Each shard is also the registry for the hook tokens that hash to it
 * (`tokenShardFor(token)`), which keeps a token unique across runs.
 *
 * The store named `STORE_NAME` is the legacy store: it still holds every run created before store
 * shards, with their hooks. Every store owes the index shards the list rows of its runs and hooks.
 * A store named `index-*` is an index shard (`RunIndex`), which holds those rows and answers
 * cross-run lists.
 *
 * Each method is RPC and delegates to the module for its role. Retained executors and stores on
 * older code call these methods, so keep names and signatures stable.
 */
export class WorldStore extends DurableObject<WorldEnv> {
  readonly #store: StoreContext;

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    // Reads `this.env` at call time, so a test can swap the bindings of a live object.
    this.#store = new StoreContext(ctx, env.QUEUE_SHARD, env.RUNNER, (name) =>
      (name === ctx.id.name ? this : transientAsWorldError(this.env.WORLD_STORE.getByName(name, objectOptions()))));
    migrateOnStart(ctx, migrations);
    ctx.blockConcurrencyWhile(async () => {
      const store = this.#store;
      if (holdsRuns(store.name)) await store.feed.resume();
      const due = [store.intents.nextDueAt(), store.takeovers.nextDueAt()].filter((at) => at !== null);
      // No network recovery under the constructor gate. The alarm replays accepted work.
      if (due.length !== 0) await store.armAlarm(Math.min(...due), 'hook recovery startup');
    });
  }

  // Reads, from the World client (`reads.ts`).

  async getRun(id: string, params: GetWorkflowRunParams & { resolveData: 'none' }): Promise<WorkflowRunWithoutData>;
  async getRun(id: string, params?: GetWorkflowRunParams & { resolveData?: 'all' }): Promise<WorkflowRun>;
  async getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData>;
  async getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData> {
    return reads.getRun(this.#store, id, params?.resolveData);
  }

  getManyRuns(ids: readonly string[], resolveData?: ResolveDataMode): (WorkflowRun | WorkflowRunWithoutData | null)[] {
    return reads.getManyRuns(this.#store, ids, resolveData);
  }

  async waitForTerminalRun(id: string, timeoutMs: number, resolveData?: ResolveDataMode): Promise<WorkflowRun | WorkflowRunWithoutData> {
    return reads.waitForTerminalRun(this.#store, id, timeoutMs, resolveData);
  }

  async listRuns(params: ListWorkflowRunsParams = {}): Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>> {
    return reads.listRuns(this.#store, params);
  }

  async getStep(runId: string, stepId: string, params: { resolveData: 'none' }): Promise<StepWithoutData>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: 'all' }): Promise<Step>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: ResolveDataMode }): Promise<Step | StepWithoutData>;
  async getStep(runId: string, stepId: string, params?: { resolveData?: ResolveDataMode }): Promise<Step | StepWithoutData> {
    return reads.getStep(this.#store, runId, stepId, params?.resolveData);
  }

  async listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>> {
    return reads.listSteps(this.#store, params);
  }

  async getHook(hookId: string, resolveData?: ResolveDataMode): Promise<Hook> {
    return reads.getHook(this.#store, hookId, resolveData);
  }

  async findHook(hookId: string, resolveData?: ResolveDataMode): Promise<Hook | undefined> {
    return reads.findHook(this.#store, hookId, resolveData);
  }

  async listHooks(params: ListHooksParams): Promise<PaginatedResponse<Hook>> {
    return reads.listHooks(this.#store, params);
  }

  async getEvent(runId: string, eventId: string, resolveData?: ResolveDataMode): Promise<Event> {
    return reads.getEvent(this.#store, runId, eventId, resolveData);
  }

  async listEvents(params: ListEventsParams): Promise<EventListResponse> {
    return reads.listEvents(this.#store, params);
  }

  async listEventsByCorrelationId(params: ListEventsByCorrelationIdParams): Promise<PaginatedResponse<Event>> {
    return reads.listEventsByCorrelationId(this.#store, params);
  }

  async nextEvents(runId: string, cursor: number, filter?: string[]) {
    return reads.nextEvents(this.#store, runId, cursor, filter);
  }

  async eventsAfter(runId: string, afterSlot: number): Promise<{ events: Event[]; done: boolean }> {
    return reads.eventsAfter(this.#store, runId, afterSlot);
  }

  // The write path (`writes.ts`, `token-claims.ts`).

  async createEvent(runId: string | null, request: AnyEventRequest, params?: CreateEventParams): Promise<EventResult> {
    return writes.createEvent(this.#store, runId, request, params);
  }

  async createEvents(runId: string, events: { request: AnyEventRequest; params?: CreateEventParams }[]): Promise<(EventResult | Error)[]> {
    return writes.createEvents(this.#store, runId, events);
  }

  async createEventBatch(runId: string, events: BatchEventRequest[], params: CreateEventBatchParams = {}): Promise<EventBatchResult> {
    return writes.createEventBatch(this.#store, runId, events, params);
  }

  // Index shard (`RunIndex`): list rows from every store of its epoch, read by cross-run lists.

  /** Applies list rows a store pushed. */
  async indexChanges(batch: IndexBatch): Promise<void> {
    this.ctx.storage.transactionSync(() => this.#store.runIndex.apply(batch));
  }

  /** Records that `store` has pushed this shard every run and hook it held before the index. */
  async markIndexed(store: string): Promise<void> {
    this.#store.runIndex.markIndexed(store);
  }

  /** An index shard asks this store to report to it; waking the store starts its own report too. */
  async reportIndex(shard: string): Promise<void> {
    await this.#store.feed.reportTo(shard);
  }

  async listIndexedRuns(params: ListWorkflowRunsParams = {}): Promise<IndexPage<WorkflowRunWithoutData>> {
    const { runIndex } = this.#store;
    return this.#store.answerFromIndex(() => runIndex.listRuns(params));
  }

  async findIndexedHook(hookId: string, resolveData?: ResolveDataMode): Promise<IndexAnswer<Hook | undefined>> {
    const { runIndex } = this.#store;
    return this.#store.answerFromIndex(() => runIndex.findHook(hookId, resolveData, new Date()));
  }

  async listIndexedHooks(params: ListHooksParams): Promise<IndexPage<Hook>> {
    const { runIndex } = this.#store;
    return this.#store.answerFromIndex(() => runIndex.listHooks(params, new Date()));
  }

  // Token shard (`token-shard.ts`, `force-claims.ts`), called by every store.

  async claimHookToken(hook: Hook): Promise<string | null> {
    return tokenShard.claimHookToken(this.#store, hook);
  }

  async claimHookTokenForced(hook: Hook): Promise<TokenAnswer> {
    return forceClaims.claimHookTokenForced(this.#store, hook);
  }

  async claimAcceptedHook(operation: HookClaimOperation): Promise<HookClaimDecision> {
    return forceClaims.claimAcceptedHook(this.#store, operation);
  }

  async cancelAcceptedHook(operation: HookClaimOperation): Promise<void> {
    return forceClaims.cancelAcceptedHook(this.#store, operation);
  }

  async hookClaimEvidence(id: string): Promise<string | undefined> {
    return this.#store.intents.evidence(id);
  }

  async finishTakeover(token: string): Promise<void> {
    return forceClaims.finishTakeover(this.#store, token);
  }

  /** On the store of the run that held a token another run's forced claim takes. */
  async disposeForcedHook(
    runId: string, hookId: string, token: string, claimedBy: { runId: string; hookId: string },
  ): Promise<HookClaimedFrom | null> {
    return forceClaims.disposeForcedHook(this.#store, runId, hookId, token, claimedBy);
  }

  async releaseTokens(releases: TokenRelease[]): Promise<void> {
    return tokenShard.releaseTokens(this.#store, releases);
  }

  async getHookByToken(token: string, resolveData?: ResolveDataMode): Promise<Hook> {
    return tokenShard.getHookByToken(this.#store, token, resolveData);
  }

  // Legacy store (`legacy.ts`), called by store shards.

  async legacyTokens(shard: string): Promise<HeldToken[]> {
    return legacy.legacyTokens(this.#store.db, shard);
  }

  async holdsRun(runId: string): Promise<boolean> {
    return legacy.holdsRun(this.#store.db, runId);
  }

  // Dispatch outbox (`dispatch.ts`). The queue client calls `dispatch` and `dispatchedSteps`, and the
  // dead-letter handler the `reopenPublished*` calls. The per-message `pending*` and `mark*` calls
  // are what queue clients made before `dispatch`; they stay for executors retained from then, since
  // RPC names are frozen.

  /** The outbox slot each queue message would settle, 0 where none is owed, in one call. */
  async pendingDispatchSlots(claims: OutboxClaim[]): Promise<number[]> {
    return claims.map((claim) => dispatch.pendingSlotFor(this.#store, claim));
  }

  async dispatch(entries: QueueDispatch[]): Promise<EnqueueResult[]> {
    return dispatch.dispatch(this.#store, entries);
  }

  async dispatchedSteps(runId: string): Promise<string[]> {
    return dispatch.dispatchedSteps(this.#store, runId);
  }

  /** Settles the outbox rows of messages the queue accepted, in one call. */
  async markDispatchesPublished(published: { claim: OutboxClaim; slot: number }[]): Promise<void> {
    for (const { claim, slot } of published) dispatch.markPublished(this.#store, claim.kind, claim.runId, slot);
  }

  async pendingRunStartSlot(runId: string): Promise<number> {
    return dispatch.pendingSlotFor(this.#store, { kind: MessageKind.Start, runId });
  }

  async markRunStartsPublished(runId: string, upToSlot: number): Promise<void> {
    dispatch.markPublished(this.#store, MessageKind.Start, runId, upToSlot);
  }

  async pendingHookWakeSlot(runId: string): Promise<number> {
    return dispatch.pendingSlotFor(this.#store, { kind: MessageKind.HookWake, runId });
  }

  async markHookWakesPublished(runId: string, upToSlot: number): Promise<void> {
    dispatch.markPublished(this.#store, MessageKind.HookWake, runId, upToSlot);
  }

  async pendingStepDispatchSlot(runId: string, stepId: string, stepName: string): Promise<number> {
    return dispatch.pendingSlotFor(this.#store, { kind: MessageKind.Step, runId, stepId, stepName });
  }

  async markStepDispatchPublished(runId: string, slot: number): Promise<void> {
    dispatch.markPublished(this.#store, MessageKind.Step, runId, slot);
  }

  async pendingWaitDispatchSlot(runId: string, correlationId: string): Promise<number> {
    return dispatch.pendingSlotFor(this.#store, { kind: MessageKind.WaitContinuation, runId, correlationId });
  }

  async markWaitDispatchPublished(runId: string, slot: number): Promise<void> {
    dispatch.markPublished(this.#store, MessageKind.WaitContinuation, runId, slot);
  }

  async reopenPublishedHookWakes(runId: string): Promise<number> {
    return dispatch.reopenHookWakes(this.#store, runId);
  }

  async reopenPublishedStepDispatch(runId: string, stepId: string, stepName: string): Promise<number> {
    return dispatch.reopenStepDispatch(this.#store, runId, stepId, stepName);
  }

  async reopenPublishedWaitDispatch(runId: string, correlationId: string): Promise<number> {
    return dispatch.reopenWaitDispatch(this.#store, runId, correlationId);
  }

  /**
   * The recovery pass, run by the alarm and, for the legacy store, the cron: publishes due outbox
   * rows, sends owed token releases, finishes due takeovers and does the index work, then re-arms
   * the alarm for whatever is still owed. Returns how many outbox rows were due.
   */
  async drainOutbox(): Promise<number> {
    const store = this.#store;
    // Persist the next recovery attempt before doing network work, even if this alarm resets.
    if (store.intents.nextDueAt() !== null || store.takeovers.nextDueAt() !== null) {
      await store.armAlarm(Date.now() + MIN_ALARM_DELAY_MS, 'hook recovery');
    }
    await recoverIntents(store);
    const drained = await dispatch.drainOutbox(store);
    await store.pushReleases();
    for (const token of store.takeovers.due(Date.now())) {
      await forceClaims.finishTakeover(store, token).catch((error: unknown) => console.error('Token takeover failed', { token, error }));
    }
    const indexAt = holdsRuns(store.name) ? await store.feed.alarm() : null;
    const due = [store.outbox.nextDueAt(), store.releases.nextDueAt(), store.takeovers.nextDueAt(), store.intents.nextDueAt(), indexAt].filter((at) => at !== null);
    if (due.length) await this.ctx.storage.setAlarm(Math.max(Date.now() + MIN_ALARM_DELAY_MS, Math.min(...due)));
    return drained;
  }

  async alarm(): Promise<void> {
    await this.drainOutbox();
  }
}
