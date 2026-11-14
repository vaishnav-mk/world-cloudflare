import { EntityConflictError, HookForceClaimedError, WorkflowRunNotFoundError, WorkflowWorldError } from '@workflow/errors';
import {
  CreateEventSchema, isTerminalWorkflowRunStatus, START_HOOK_CANDIDATE_MAX_AGE_MS, ulidToDate,
  type AnyEventRequest, type BatchEventItemResult, type BatchEventRequest, type CreateEventBatchParams, type CreateEventParams,
  type EventBatchResult, type EventResult,
} from '@workflow/world';
import { ulid } from 'ulid';
import { DISPATCH_KINDS, DISPATCH_RECOVERY_DELAY_MS, RELEASE_KINDS } from '../constants.js';
import { BackfillStage, EventType, TokenClaim } from '../enums.js';
import { encode } from '../lib/codec.js';
import { sha256Hex } from '../lib/hash.js';
import { tokenShardFor } from '../lib/shards.js';
import { isSingleOrchestrator, mintSpecVersion } from '../lib/spec.js';
import type { HookClaimIntent } from '../types.js';
import type { StoreContext } from './context.js';
import { applyEvent } from './events/apply.js';
import { checkBatch } from './events/batch.js';
import { advanceFence, checkFence } from './events/fence.js';
import { skippedEvents } from './events/journal.js';
import type { Tokens } from './events/types.js';
import { refuseRunCopy } from './legacy.js';
import { finishIntent, preflight, prepareHook } from './token-claims.js';

/** Writes one event and everything it changes in one transaction; see `applyEvent`. */
export async function createEvent(
  store: StoreContext, runId: string | null, request: AnyEventRequest, params: CreateEventParams = {},
): Promise<EventResult> {
  const data = CreateEventSchema.parse(request);
  if (runId === null && !store.isLegacy) throw new WorkflowRunNotFoundError('null');
  const id = runId ?? (data.eventType === EventType.RunCreated ? `wrun_${ulid()}` : null);
  if (id === null) throw new Error('runId is required');
  // Reject already-stale writers before even flushing existing debt; recheck after the awaits.
  checkFence(store.db, id, store.db.findRun(id), data, data.specVersion ?? mintSpecVersion(), params);
  // Never hold a run gate across release delivery or index RPCs.
  await store.pushReleases();
  let result: EventResult;
  try {
    result = await store.writeRun(id, async () => {
      await resumeRunIntent(store, id);
      checkFence(store.db, id, store.db.findRun(id), data, data.specVersion ?? mintSpecVersion(), params);
      const hook = await prepareHook(store, id, data);
      if (hook === undefined) {
        await armFor(store, [data]);
        return store.ctx.storage.transactionSync(() => applyEvent(
          store.db, store.outbox, store.releases, store.index, { kind: TokenClaim.None }, id, data, params,
        ));
      }
      const createdAt = data.eventType === EventType.HookCreated ? null : ulidToDate(id.slice('wrun_'.length));
      const operation = { id: crypto.randomUUID(), source: store.name, hook, force: data.eventType === EventType.HookCreated && data.eventData.force === true };
      const intent: HookClaimIntent = {
        operation, digest: await sha256Hex(encode(operation)),
        request: { ...data, specVersion: hook.specVersion }, params,
        ...(createdAt === null ? {} : { expiresAt: createdAt.getTime() + START_HOOK_CANDIDATE_MAX_AGE_MS }),
      };
      // Only local storage inside this transaction. A failed alarm rolls admission back too.
      await store.ctx.storage.transaction(async () => {
        preflight(store, id, data, params, hook);
        store.intents.add(intent);
        await store.armAlarm(Date.now() + DISPATCH_RECOVERY_DELAY_MS, 'hook intent');
      });
      return finishIntent(store, intent);
    });
  } catch (error) {
    store.ctx.waitUntil(store.pushReleases());
    // A resume of a hook another run took: make sure the token names its new holder before the
    // resumer looks it up again. The alarm on the token's shard covers a failure here.
    if (HookForceClaimedError.is(error)) {
      await store.peer(tokenShardFor(error.token)).finishTakeover(error.token)
        .catch((cause: unknown) => console.error('Could not finish a token takeover', cause));
    }
    throw error;
  }
  wake(store, [result]);
  await afterWrite(store, freesTokens(data, result));
  return result;
}

/**
 * Writes events a flow request created at once, in order, each as its own `createEvent`, and
 * answers each with its result or its error: one call for a replay's thousands of `step_created`.
 */
export async function createEvents(
  store: StoreContext, runId: string, events: readonly { request: AnyEventRequest; params?: CreateEventParams }[],
): Promise<(EventResult | Error)[]> {
  // A shard that does not hold the run refuses the whole call, so the caller asks the legacy store.
  store.own(runId);
  const results: (EventResult | Error)[] = [];
  for (const [index, { request, params }] of events.entries()) {
    // One skipped-slot report per call: a later write's skipped slots are mostly this batch's own,
    // and a report each made a fan-out's results hold thousands of events.
    const { eventCount: _eventCount, ...unreported } = params ?? {};
    const written = createEvent(store, runId, request, index === 0 ? params : unreported);
    results.push(await written.catch((error: unknown) => (error instanceof Error ? error : new Error(String(error)))));
  }
  return results;
}

/**
 * Appends events in order in one transaction. Each event commits or is rejected on its own, with
 * the outcome its single `createEvent` would have had; a conflict is a 409 item.
 */
export async function createEventBatch(
  store: StoreContext, runId: string, events: BatchEventRequest[], params: CreateEventBatchParams,
): Promise<EventBatchResult> {
  const { db, ctx: { storage } } = store;
  const { outcomes, blockStart, parsed } = await store.writeRun(runId, async () => {
    await resumeRunIntent(store, runId);
    store.own(runId);
    if (store.isLegacy) refuseRunCopy(db, runId);
    const run = db.getRun(runId);
    const appends = isSingleOrchestrator(run.specVersion);
    checkBatch(events, appends);
    const parsed = events.map((item) => ({ ...item, event: CreateEventSchema.parse(item.event) }));
    if (parsed.length !== 0) checkFence(db, runId, run, parsed[0].event, run.specVersion ?? mintSpecVersion(), params);
    await armFor(store, parsed.map(({ event }) => event));
    // A batch cannot hold hook_created, so it never needs a token claim.
    const tokens: Tokens = { kind: TokenClaim.None };
    return { ...storage.transactionSync(() => {
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
          return storage.transactionSync(() => applyEvent(db, store.outbox, store.releases, store.index, tokens, runId, event, {
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
    }), parsed };
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
  wake(store, committed);
  await afterWrite(store, frees);
  const allocated = committed.reduce((sum, outcome) => sum + (outcome.allocated ?? 0), 0);
  const skipped = skippedEvents(db, runId, params.eventCount, blockStart, params.resolveData);
  return { results, allocated, ...(skipped === undefined ? {} : { events: skipped.events, hasMore: skipped.hasMore }) };
}

/** After reset, accepted work precedes every later mutation of this run. Caller holds its gate. */
async function resumeRunIntent(store: StoreContext, runId: string): Promise<void> {
  const pending = store.intents.find(runId);
  if (pending === undefined) return;
  try {
    const result = await finishIntent(store, pending);
    wake(store, [result]);
  } catch (error) {
    // Expiration/conflict completed the intent. A transient failure still blocks later mutations.
    if (store.intents.find(runId) !== undefined) throw error;
  }
}

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

/** Wakes the long polls of the runs these writes appended to. */
function wake({ waiters }: StoreContext, results: EventResult[]): void {
  for (const { event } of results) if (event !== undefined) waiters.wake(event.runId);
}

/**
 * Arms the alarm for what these events will owe, before they commit, so a crash just after the
 * commit cannot strand an outbox row: a queue message per dispatch event, and token releases.
 * One alarm check covers all of them: each check is a storage round trip.
 */
async function armFor(store: StoreContext, events: AnyEventRequest[]): Promise<void> {
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
  if (at !== Number.POSITIVE_INFINITY) await store.armAlarm(at, 'dispatch recovery');
}

/**
 * After a write: owed token releases go to their shards, and owed list rows to the index shards
 * before the write answers (see `IndexFeed.flush`). A failed release stays owed and the alarm
 * retries it.
 */
async function afterWrite(store: StoreContext, frees: boolean): Promise<void> {
  const indexed = store.feed.flush(frees);
  if (store.releases.nextDueAt() !== null) {
    // A write that frees a token answers once the token's shard has it, so the token is free
    // when the write returns. A failed release stays owed for the alarm and fails nothing here.
    if (frees) await store.pushReleases();
    else store.ctx.waitUntil(store.pushReleases());
    await indexed;
    return;
  }
  await indexed;
  // A finished run usually owes nothing now, so its store need not wake for the recovery alarm.
  if (frees && store.releases.nextDueAt() === null && store.outbox.nextDueAt() === null && store.index.nextDueAt() === null
    && store.intents.nextDueAt() === null && store.takeovers.nextDueAt() === null
    && store.feed.backfill().stage === BackfillStage.Reported) {
    await store.ctx.storage.deleteAlarm();
  }
}
