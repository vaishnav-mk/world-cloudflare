import { DurableObject } from 'cloudflare:workers';
import {
  EntityConflictError, HookConflictError, HookForceClaimedError, WorkflowRunNotFoundError, WorkflowWorldError,
} from '@workflow/errors';
import {
  CreateEventSchema, isTerminalWorkflowRunStatus, slotToEventId, SPEC_VERSION_LEGACY, SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM,
  START_HOOK_ADMISSION_REJECTED_CODE, START_HOOK_CANDIDATE_MAX_AGE_MS, ulidToDate,
  type HookClaimedFrom, type StartHook, type StartHookRejectionReason,
  type AnyEventRequest, type BatchEventItemResult, type BatchEventRequest, type CreateEventBatchParams,
  type CreateEventParams, type Event, type EventBatchResult, type EventListResponse, type EventResult, type GetWorkflowRunParams, type Hook,
  type ListEventsByCorrelationIdParams, type ListEventsParams, type ListHooksParams, type ListWorkflowRunStepsParams,
  type ListWorkflowRunsParams, type PaginatedResponse, type Step, type StepWithoutData, type WorkflowRun,
  type WorkflowRunWithoutData,
} from '@workflow/world';
import migrations from '../../drizzle/store/migrations.js';
import {
  DISPATCH_KINDS, DISPATCH_RECOVERY_DELAY_MS, EVENT_PAGE_READS, EVENT_POLL_BATCH, EVENT_POLL_MS, MAX_START_HOOK_RETENTION_MS,
  MAX_TERMINAL_WAIT_MS,
  MIN_ALARM_DELAY_MS, REDRIVE_DELAY_MS, RELEASE_KINDS, STORE_NAME,
} from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { isSingleOrchestrator, mintSpecVersion } from '../lib/spec.js';
import { EventType, MessageKind, RunStatus, TokenClaim, WaitStatus } from '../enums.js';
import { toJson } from '../lib/json.js';
import { Semaphore } from '../lib/semaphore.js';
import { transientAsWorldError } from '../lib/transient.js';
import { unreachable } from '../lib/unreachable.js';
import { Waiters } from '../lib/waiters.js';
import { shardPusher } from '../queue/push.js';
import { queueShardFor, storeShardFor, tokenShardFor } from '../lib/shards.js';
import type {
  DispatchKind, EnqueueResult, EventOf, HeldToken, OutboxClaim, QueueDispatch, ResolveDataMode, TokenRelease, WorldEnv,
} from '../types.js';
import { Db } from './db.js';
import { drainOutbox, isPendingStep, pendingStepSlot, pendingWaitSlot } from './dispatch.js';
import { advanceFence, applyEvent, checkFence } from './events/apply.js';
import { checkBatch } from './events/batch.js';
import { newHook, startHookOf } from './events/hooks.js';
import { appendEvent, skippedEvents } from './events/journal.js';
import type { TokenAnswer, Tokens } from './events/types.js';
import { Outbox } from './outbox.js';
import { listRecords, readEventPage } from './pages.js';
import { toEvent } from './rows.js';
import { TokenOutbox } from './token-outbox.js';
import { TokenRegistry } from './tokens.js';
import { TokenTakeovers, type Takeover } from './takeovers.js';
import { hookView, runView, stepView } from './views.js';

/** Storage key a token shard sets once it has imported the legacy store's tokens. */
const LEGACY_TOKENS_IMPORTED = 'legacy-tokens-imported';

/** A batch item the World refuses on its own, as its single create would be: a 4xx, not a failure of the batch. */
function isItemRefusal(error: unknown): error is WorkflowWorldError {
  // `WorkflowWorldError.is` matches the base name only; these errors were made in this object.
  if (!(error instanceof WorkflowWorldError)) return false;
  const { status } = error;
  return status === undefined ? EntityConflictError.is(error) : status >= 400 && status < 500;
}

function itemRefusal({ status, code, name, message }: WorkflowWorldError): BatchEventItemResult {
  // An EntityConflictError carries no status: it is the 409.
  return status === undefined ? { status: 409, error: 'conflict', message } : { status, error: code ?? name, message };
}

/** A write that frees hook tokens: a disposal, or the run finishing. */
function freesTokens(data: AnyEventRequest, result: EventResult): boolean {
  if (result.event === undefined) return false;
  return data.eventType === EventType.HookDisposed || (result.run !== undefined && isTerminalWorkflowRunStatus(result.run.status));
}

/**
 * A store shard, a SQLite Durable Object holding the runs that hash to it (`storeShardFor(runId)`):
 * their event logs and materialized state, whose event slots, state changes and outbox rows commit
 * in one transaction. Each shard is also the registry for the hook tokens that hash to it
 * (`tokenShardFor(token)`), which keeps a token unique across runs.
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
  readonly #takeovers: TokenTakeovers;
  readonly #waiters = new Waiters();
  readonly #pageReads = new Semaphore(EVENT_PAGE_READS);
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
    this.#takeovers = new TokenTakeovers(ctx.storage.sql);
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

  /**
   * For `hooks.get`, which asks every store. The legacy store skips the old directory's copies of
   * sharded hooks, and a finished run's hook past its retention is gone, as with `getHook`.
   */
  async findHook(hookId: string, resolveData?: ResolveDataMode): Promise<Hook | undefined> {
    const hook = this.#db.findHook(hookId);
    if (hook === undefined || (this.#isLegacy && this.#db.isRunCopy(hook.runId)) || this.#db.isExpiredHook(hook, new Date())) {
      return undefined;
    }
    return hookView(hook, resolveData);
  }

  async listHooks(params: ListHooksParams): Promise<PaginatedResponse<Hook>> {
    if (params.runId !== undefined) this.#own(params.runId);
    const page = listRecords<Hook>(this.#db, 'hooks', params);
    const now = new Date();
    const live = page.data.filter((hook) => !this.#db.isExpiredHook(hook, now));
    return { ...page, data: live.map((hook) => hookView(hook, params.resolveData)) };
  }

  // Events.

  async getEvent(runId: string, eventId: string, resolveData?: ResolveDataMode): Promise<Event> {
    this.#own(runId);
    const slot = Number(eventId.replace(/^evnt_/, ''));
    if (!Number.isSafeInteger(slot) || slot < 1 || slotToEventId(slot) !== eventId) throw new Error('Invalid event ID');
    return toEvent(this.#db.getEvent(runId, slot), resolveData);
  }

  async listEvents(params: ListEventsParams): Promise<EventListResponse> {
    this.#own(params.runId);
    return this.#pageReads.run(async () => {
      const page = await readEventPage(this.#db, params);
      // A spec 9 run's fence counts, read right after the page's last read: nothing is awaited
      // between, so no write lands in between.
      const seqInBand = this.#db.seqInBand(params.runId);
      const snapshot = seqInBand === null ? undefined : { seq: this.#db.lastSlot(params.runId), seqInBand };
      return { ...page, ...(snapshot === undefined ? {} : { snapshot }) };
    });
  }

  async listEventsByCorrelationId(params: ListEventsByCorrelationIdParams): Promise<PaginatedResponse<Event>> {
    this.#own(params.runId);
    return this.#pageReads.run(() => readEventPage(this.#db, params));
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
      // A resume of a hook another run took: make sure the token names its new holder before the
      // resumer looks it up again. The alarm on the token's shard covers a failure here.
      if (HookForceClaimedError.is(error)) {
        await this.#store(tokenShardFor(error.token)).finishTakeover(error.token)
          .catch((cause: unknown) => console.error('Could not finish a token takeover', cause));
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
    this.#own(runId);
    if (this.#isLegacy) this.#refuseRunCopy(runId);
    const run = this.#db.getRun(runId);
    const appends = isSingleOrchestrator(run.specVersion);
    checkBatch(events, appends);
    const parsed = events.map((item) => ({ ...item, event: CreateEventSchema.parse(item.event) }));
    await this.#armFor(parsed.map(({ event }) => event));
    const storage = this.ctx.storage;
    // A batch cannot hold hook_created, so it never needs a token claim.
    const tokens: Tokens = { kind: TokenClaim.None };
    const db = this.#db;
    const { outcomes, blockStart } = storage.transactionSync(() => {
      // The whole block is one fenced allocation.
      const fence = parsed.length === 0 ? undefined
        : checkFence(db, runId, run, parsed[0].event, run.specVersion ?? mintSpecVersion(), params);
      const start = db.lastSlot(runId) + 1;
      // On a spec 9 run, an entity's later events are refused behind its refused one.
      const refused = new Set<string>();
      const applied = parsed.map(({ event, occurredAt, computeInstanceId }): EventResult | WorkflowWorldError => {
        const entity = 'correlationId' in event ? event.correlationId : undefined;
        if (appends && entity !== undefined && refused.has(entity)) {
          return new EntityConflictError(`An earlier event for ${entity} in this batch was refused`);
        }
        try {
          // A nested transaction is a savepoint, so a rejected event leaves nothing behind.
          return storage.transactionSync(() => applyEvent(db, this.#outbox, this.#releases, tokens, runId, event, {
            resolveData: params.resolveData, requestId: params.requestId, occurredAt, computeInstanceId,
          }));
        } catch (error) {
          if (!isItemRefusal(error)) throw error;
          if (entity !== undefined) refused.add(entity);
          return error;
        }
      });
      advanceFence(db, runId, fence, applied.reduce((sum, outcome) => sum + (outcome instanceof Error ? 0 : outcome.allocated ?? 0), 0));
      return { outcomes: applied, blockStart: start };
    });
    const results: BatchEventItemResult[] = [];
    const committed: EventResult[] = [];
    let frees = false;
    for (const [index, outcome] of outcomes.entries()) {
      if (outcome instanceof Error || outcome.event === undefined) {
        results.push(outcome instanceof Error ? itemRefusal(outcome) : { status: 409, error: 'conflict', message: 'The run is finished' });
        continue;
      }
      committed.push(outcome);
      frees ||= freesTokens(parsed[index].event, outcome);
      const { event, run: changedRun, step, wait } = outcome;
      results.push({ status: 200, event, run: changedRun, step, wait });
    }
    this.#wake(committed);
    await this.#afterWrite(frees);
    const allocated = committed.reduce((sum, outcome) => sum + (outcome.allocated ?? 0), 0);
    const skipped = skippedEvents(db, runId, params.eventCount, blockStart, params.resolveData);
    return { results, allocated, ...(skipped === undefined ? {} : { events: skipped.events, hasMore: skipped.hasMore }) };
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
      // A spec 9 run's steps and waits owe the outbox nothing (see `recordDispatch`).
      const append = data.eventType === EventType.StepCreated || data.eventType === EventType.WaitCreated;
      if (append && isSingleOrchestrator(data.specVersion)) continue;
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
      case EventType.RunCreated:
        return data.eventData.startHook === undefined ? { kind: TokenClaim.None } : this.#admit(runId, data, data.eventData.startHook);
      case EventType.RunStarted: {
        // A lazy start creates a run it cannot find, so a shard first rules out a legacy run.
        if (!this.#isLegacy && this.#db.findRun(runId) === undefined && await this.#store(STORE_NAME).holdsRun(runId)) {
          throw new WorkflowRunNotFoundError(runId);
        }
        const startHook = data.eventData?.startHook;
        return startHook === undefined ? { kind: TokenClaim.None } : this.#admit(runId, data, startHook);
      }
      default:
        return { kind: TokenClaim.None };
    }
  }

  /**
   * Admits a start Hook candidate (`start({ experimental_hook })`), on its `run_created` or the
   * queued `run_started` that may create it: claims the token on its shard, and the write then
   * creates the run with its start Hook. A refusal is recorded, so every later copy of the candidate
   * is refused naming the same run; an admitted candidate is its run. Answers None for a candidate
   * already admitted, whose write replays.
   */
  async #admit(runId: string, data: EventOf<EventType.RunCreated | EventType.RunStarted>, startHook: StartHook): Promise<Tokens> {
    const rejected = (message: string, reason: StartHookRejectionReason) =>
      new WorkflowWorldError(message, { status: 400, code: START_HOOK_ADMISSION_REJECTED_CODE, reason });
    const createdAt = runId.startsWith('wrun_') ? ulidToDate(runId.slice('wrun_'.length)) : null;
    if (createdAt === null) throw rejected(`A start Hook candidate needs a wrun_<ulid> run ID, not ${runId}`, 'invalid');
    const run = this.#db.findRun(runId);
    // Past the bound a candidate can never be created, so nothing waits on its decision either.
    if (Date.now() - createdAt.getTime() > START_HOOK_CANDIDATE_MAX_AGE_MS && (run === undefined || data.eventType === EventType.RunCreated)) {
      throw rejected(`Start Hook candidate ${runId} is too old to be created`, 'expired');
    }
    const until = startHook.tokenRetentionUntil;
    if (until !== undefined && until.getTime() - Date.now() > MAX_START_HOOK_RETENTION_MS) {
      throw rejected('The start Hook retention is beyond the World limit', 'invalid');
    }
    const refused = this.#db.findStartHookRefusal(runId);
    const decidedToken = run?.startHook?.token ?? refused?.token;
    if (decidedToken !== undefined && decidedToken !== startHook.token) {
      throw rejected(`Start Hook candidate ${runId} was decided under another token`, 'token_mismatch');
    }
    if (refused !== undefined) throw new HookConflictError(startHook.token, refused.conflictingRunId);
    if (run !== undefined) {
      if (run.startHook === undefined) throw rejected(`Run ${runId} was created without a start Hook`, 'ordinary_run');
      return { kind: TokenClaim.None };
    }
    const { deploymentId, workflowName, encryptionPublicKey } = data.eventData ?? {};
    if (deploymentId === undefined || workflowName === undefined) throw new WorkflowRunNotFoundError(runId);
    const hook = startHookOf(runId, { deploymentId, workflowName, encryptionPublicKey }, startHook,
      data.specVersion ?? mintSpecVersion(), new Date());
    await this.#push();
    const holderRunId = await this.#store(tokenShardFor(hook.token)).claimHookToken(hook);
    if (holderRunId === null) return { kind: TokenClaim.Granted, hook };
    // Another copy of this candidate may have been decided meanwhile; the first decision stands.
    const decided = this.ctx.storage.transactionSync(() => {
      const admitted = this.#db.findRun(runId)?.startHook !== undefined;
      const prior = this.#db.findStartHookRefusal(runId);
      if (!admitted && prior === undefined) this.#db.insertStartHookRefusal(runId, hook.token, holderRunId);
      return admitted ? undefined : prior?.conflictingRunId ?? holderRunId;
    });
    if (decided === undefined) return { kind: TokenClaim.None };
    throw new HookConflictError(hook.token, decided);
  }

  /** The old directory's copy of a sharded run was only an index entry: it takes no writes. */
  #refuseRunCopy(runId: string): void {
    if (this.#db.isRunCopy(runId)) throw new Error(`Run ${runId} lives on a store shard, not the legacy store`);
  }

  /** Claims a new hook's token from the token's shard. A write the transaction rejects anyway claims nothing. */
  async #claimToken(runId: string, data: EventOf<EventType.HookCreated>): Promise<Tokens> {
    const run = this.#db.getRun(runId);
    if (isTerminalWorkflowRunStatus(run.status) || this.#db.findHook(data.correlationId)) return { kind: TokenClaim.None };
    const hook = newHook(run, data, data.specVersion ?? run.specVersion ?? mintSpecVersion(), new Date());
    // Releases this store owes go first, so a run can claim again a token it just disposed of.
    await this.#push();
    const shard = this.#store(tokenShardFor(hook.token));
    if (data.eventData.force) return shard.claimHookTokenForced(hook);
    const holderRunId = await shard.claimHookToken(hook);
    return holderRunId === null ? { kind: TokenClaim.Granted, hook } : { kind: TokenClaim.Held, holderRunId };
  }

  /** A run-scoped call on the store that holds the run: its shard, else the legacy store. */
  async #onRun<T>(runId: string, call: (store: WorldStore | DurableObjectStub<WorldStore>) => Promise<T>): Promise<T> {
    try {
      return await call(this.#store(storeShardFor(runId)));
    } catch (error) {
      if (!WorkflowRunNotFoundError.is(error)) throw error;
    }
    return call(this.#store(STORE_NAME));
  }

  /**
   * After a write: owed token releases go to their shards in the background, so a freed token is
   * free shortly after the write returns. A failed release stays owed and the alarm retries it.
   */
  async #afterWrite(freesTokens: boolean): Promise<void> {
    if (this.#releases.nextDueAt() !== null) {
      // A write that frees a token answers once the token's shard has it, so the token is free
      // when the write returns. A failed release stays owed for the alarm and fails nothing here.
      if (freesTokens) await this.#push();
      else this.ctx.waitUntil(this.#push());
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
    const shard = tokenShardFor(hook.token);
    if (shard !== this.ctx.id.name) return this.#store(shard).claimHookToken(hook);
    await this.#importLegacyTokens(shard);
    return this.ctx.storage.transactionSync(() => this.#tokens.claim(hook, new Date()));
  }

  /**
   * Claims a token for a forced `hook_created`, taking it over from a live holder. The holder's
   * store disposes of its hook before the token moves, so a resume that already found the holder is
   * refused there rather than landing in a run that no longer holds the token. The answer is the
   * grant, whose hook names what it took in `claimedFrom`; or a refusal, when the holder's run
   * predates forced disposals and would never read one.
   */
  async claimHookTokenForced(hook: Hook): Promise<TokenAnswer> {
    const shard = tokenShardFor(hook.token);
    if (shard !== this.ctx.id.name) return this.#store(shard).claimHookTokenForced(hook);
    await this.#importLegacyTokens(shard);
    // Before the takeover row commits, so a crash cannot strand it.
    await this.#armAlarm(Date.now() + DISPATCH_RECOVERY_DELAY_MS, 'token takeover');
    const decided = this.ctx.storage.transactionSync((): TokenAnswer | Takeover => {
      const holder = this.#tokens.find(hook.token);
      // Granted before: a retried write gets the same answer, with what the token was taken from.
      if (holder !== undefined && holder.hook.hookId === hook.hookId && holder.hook.runId === hook.runId) {
        return { kind: TokenClaim.Granted, hook: holder.hook };
      }
      if (holder === undefined || this.#tokens.claim(hook, new Date()) === null) return { kind: TokenClaim.Granted, hook };
      const pending = this.#takeovers.find(hook.token);
      if (pending !== undefined && pending.claimer.hookId !== hook.hookId) {
        throw new WorkflowWorldError(`Hook token ${hook.token} is being taken over by another run; retry`, { status: 503 });
      }
      if (pending !== undefined) return pending;
      this.#takeovers.add(holder.hook, hook, Date.now() + DISPATCH_RECOVERY_DELAY_MS);
      return { holder: holder.hook, claimer: hook, attempts: 0 };
    });
    return 'kind' in decided ? decided : this.#takeOver(decided);
  }

  /** Finishes a takeover a resume found half done; see `createEvent`. */
  async finishTakeover(token: string): Promise<void> {
    const takeover = this.#takeovers.find(token);
    if (takeover !== undefined) await this.#takeOver(takeover);
  }

  /** The holder's store disposes of its hook, then the token moves to the claimer. A failure backs off for the alarm. */
  async #takeOver({ holder, claimer, attempts }: Takeover): Promise<TokenAnswer> {
    const { token } = claimer;
    let claimedFrom: HookClaimedFrom | null;
    try {
      claimedFrom = await this.#onRun(holder.runId, (store) => store.disposeForcedHook(
        holder.runId, holder.hookId, token, { runId: claimer.runId, hookId: claimer.hookId },
      ));
    } catch (error) {
      await this.#armAlarm(this.ctx.storage.transactionSync(() => this.#takeovers.recordFailure(token, attempts)), 'token takeover retry');
      throw error;
    }
    return this.ctx.storage.transactionSync((): TokenAnswer => {
      // Another call finished this takeover first, and another claim may have started since.
      if (this.#takeovers.find(token)?.claimer.hookId !== claimer.hookId) {
        const current = this.#tokens.find(token);
        return current?.hook.hookId === claimer.hookId
          ? { kind: TokenClaim.Granted, hook: current.hook }
          : { kind: TokenClaim.Held, holderRunId: current?.hook.runId ?? holder.runId };
      }
      this.#takeovers.remove(token);
      if (claimedFrom === null) return { kind: TokenClaim.Refused, holderRunId: holder.runId };
      const taken = { ...claimer, claimedFrom };
      this.#tokens.give(taken);
      return { kind: TokenClaim.Granted, hook: taken };
    });
  }

  /**
   * For a token's shard taking a hook of this store's run for another run's forced claim: journals
   * `hook_disposed{forceClaimedBy}` while the run can still read it, and removes the hook. The
   * takeover is kept, so a later resume is refused naming the new holder, and a hook not created yet
   * is disposed of as it commits. Returns what the claimer journals as `forceClaimedFrom`, or null
   * for a running run that predates forced disposals, which keeps its hook.
   */
  async disposeForcedHook(
    runId: string, hookId: string, token: string, claimedBy: { runId: string; hookId: string },
  ): Promise<HookClaimedFrom | null> {
    this.#own(runId);
    if (this.#isLegacy) this.#refuseRunCopy(runId);
    const db = this.#db;
    const outcome = this.ctx.storage.transactionSync(() => {
      const prior = db.findForcedDisposal(hookId);
      if (prior !== undefined) return { claimedFrom: prior.claimedFrom, journaled: false };
      const run = db.getRun(runId);
      const running = !isTerminalWorkflowRunStatus(run.status);
      const specVersion = run.specVersion ?? SPEC_VERSION_LEGACY;
      if (running && specVersion < SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM) return null;
      // A finished run has nothing to wake, so its takeover names no wake target.
      const claimedFrom: HookClaimedFrom = running
        ? { runId, hookId, workflowName: run.workflowName, deploymentId: run.deploymentId, runSpecVersion: specVersion }
        : { runId, hookId };
      const holds = db.findHook(hookId)?.runId === runId;
      if (holds && running) {
        appendEvent(db, runId, {
          eventType: EventType.HookDisposed, correlationId: hookId, specVersion, eventData: { token, forceClaimedBy: claimedBy },
        }, new Date());
      }
      if (holds) db.deleteRunHook(runId, hookId);
      db.insertForcedDisposal(runId, hookId, { token, claimedBy, claimedFrom });
      return { claimedFrom, journaled: holds && running };
    });
    if (outcome === null) return null;
    if (outcome.journaled) this.#waiters.wake(runId);
    return outcome.claimedFrom;
  }

  /** Applies releases from the stores whose hooks held these tokens. */
  async releaseTokens(releases: TokenRelease[]): Promise<void> {
    const shards = new Set(releases.map(({ token }) => tokenShardFor(token)));
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
    const shard = tokenShardFor(token);
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
    return this.#db.hooksWithTokens((token) => tokenShardFor(token) === shard).map((hook) => {
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
      const rows = [...this.#db.events(runId, cursor, EVENT_POLL_BATCH)];
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
    const drained = await drainOutbox(this.#db, this.#outbox,
      (runId, payload, key) => this.env.QUEUE_SHARD.getByName(queueShardFor(runId, payload, key)));
    await this.#push();
    for (const token of this.#takeovers.due(Date.now())) await this.finishTakeover(token).catch((error: unknown) => {
      console.error('Token takeover failed', { token, error });
    });
    const due = [this.#outbox.nextDueAt(), this.#releases.nextDueAt(), this.#takeovers.nextDueAt()].filter((at) => at !== null);
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
