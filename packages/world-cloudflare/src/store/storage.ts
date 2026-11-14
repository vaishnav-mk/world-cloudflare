import { EntityConflictError, FatalError, HookNotFoundError } from '@workflow/errors';
import { requireEventSlot } from '@workflow/world';
import type {
  AnyEventRequest,
  CreateEventParams,
  Event,
  CreateEventRequest,
  EventResult,
  GetWorkflowRunParams,
  Hook,
  ListWorkflowRunStepsParams,
  ListWorkflowRunsParams,
  PaginatedResponse,
  RunCreatedEventRequest,
  Step,
  StepWithoutData,
  Storage,
  WaitForTerminalRunStatusParams,
  WorkflowRun,
  WorkflowRunWithoutData,
} from '@workflow/world';
import { stringify } from 'devalue';
import { ulid } from 'ulid';
import { FAN_OUT_CONCURRENCY, PAYLOAD_MAX_BYTES } from '../constants.js';
import { EventType, ResolveData } from '../enums.js';
import { groupBy } from '../lib/group.js';
import { createPayloads } from '../lib/payloads.js';
import { Semaphore } from '../lib/semaphore.js';
import { hookIndexShards, indexShardNames, storeShardFor } from '../lib/shards.js';
import { isSingleOrchestrator } from '../lib/spec.js';
import { toWorldError } from '../lib/transient.js';
import { gatherTurns } from '../lib/turns.js';
import type { EventOf, IndexAnswer, IndexPage, WorldEnv } from '../types.js';
import { cancelMany } from './cancel-many.js';
import { mergePages } from './pages.js';
import { answersUnknown, legacyStore, onEveryStore, onRun, onStores, storeShard, tokenShard, type Store } from './route.js';

type AnyRun = WorkflowRun | WorkflowRunWithoutData;
type AnyStep = Step | StepWithoutData;
type WithoutData = { resolveData: 'none' };
type WithData = { resolveData?: 'all' };

const encoder = new TextEncoder();
const runKey = (run: AnyRun): [number, string] => [run.createdAt.getTime(), run.runId];
const hookKey = (hook: Hook): [number, string] => [hook.createdAt.getTime(), hook.hookId];
const isComplete = <T>(answer: IndexAnswer<T>): answer is { complete: true; value: T } => answer.complete;

/**
 * The World's `Storage`. Every call is an RPC to a store Durable Object: the run's shard for a
 * run-scoped call, the token's shard for a token, and the index shards for a cross-run list or
 * `hooks.get` (every store while a store has still to backfill the index). Large payloads go to R2
 * before the call and come back from R2 after it, so the objects only see refs to them.
 */
export function createStorage(env: Pick<WorldEnv, 'WORLD_STORE' | 'PAYLOADS'>): Storage {
  // A stub belongs to one request, so the route helpers make one per call; never cache it in the World singleton.
  const stores = env.WORLD_STORE;
  const { offloadEvent, resolveAll } = createPayloads(env.PAYLOADS);

  // A replay that starts thousands of steps creates them all at once, which as a call each
  // overloaded the run's store and ran it out of memory (`WorldStore.createEvents`).
  const createStep = gatherTurns(async (runId: string, writes: { request: AnyEventRequest; params?: CreateEventParams }[]) => {
    // RPC stubs type Event-bearing results as never; this is `WorldStore.createEvents`' own type.
    const results: (EventResult | Error)[] = await onRun(stores, runId, (store) => store.createEvents(runId, writes));
    return results;
  });
  async function gathered(runId: string, request: AnyEventRequest, params?: CreateEventParams): Promise<EventResult> {
    const result = await createStep(runId, { request, params });
    if (result instanceof Error) throw toWorldError(result);
    return result;
  }

  function getRun(id: string, params: GetWorkflowRunParams & WithoutData): Promise<WorkflowRunWithoutData>;
  function getRun(id: string, params?: GetWorkflowRunParams & WithData): Promise<WorkflowRun>;
  function getRun(id: string, params?: GetWorkflowRunParams): Promise<AnyRun>;
  async function getRun(id: string, params?: GetWorkflowRunParams): Promise<AnyRun> {
    return resolveAll(await onRun(stores, id, (store) => store.getRun(id, params)));
  }

  function getMany(ids: readonly string[], params: GetWorkflowRunParams & WithoutData): Promise<(WorkflowRunWithoutData | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams & WithData): Promise<(WorkflowRun | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams): Promise<(AnyRun | null)[]>;
  async function getMany(ids: readonly string[], params?: GetWorkflowRunParams): Promise<(AnyRun | null)[]> {
    const found = new Map<string, AnyRun>();
    const record = (asked: readonly string[], runs: (AnyRun | null)[]) => {
      for (const [index, run] of runs.entries()) if (run !== null) found.set(asked[index], run);
    };
    // One call per shard, then one to the legacy store for the rest.
    const calls = new Semaphore(FAN_OUT_CONCURRENCY);
    await Promise.all([...groupBy(new Set(ids), storeShardFor)].map(([, group]) => calls.run(async () =>
      record(group, await storeShard(stores, group[0]).getManyRuns(group, params?.resolveData)))));
    const rest = [...new Set(ids)].filter((id) => !found.has(id));
    if (rest.length) record(rest, await legacyStore(stores).getManyRuns(rest, params?.resolveData));
    return resolveAll(ids.map((id) => found.get(id) ?? null));
  }

  function waitForTerminalStatus(id: string, params: WaitForTerminalRunStatusParams & WithoutData): Promise<WorkflowRunWithoutData>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams & WithData): Promise<WorkflowRun>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams): Promise<AnyRun>;
  async function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams): Promise<AnyRun> {
    // An AbortSignal cannot cross RPC; the World contract lets us ignore it.
    return resolveAll(await onRun(stores, id, (store) => store.waitForTerminalRun(id, params?.timeoutMs ?? 0, params?.resolveData)));
  }

  function listRuns(params: ListWorkflowRunsParams & WithoutData): Promise<PaginatedResponse<WorkflowRunWithoutData>>;
  function listRuns(params?: ListWorkflowRunsParams & WithData): Promise<PaginatedResponse<WorkflowRun>>;
  function listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<AnyRun>>;
  async function listRuns(params: ListWorkflowRunsParams = {}): Promise<PaginatedResponse<AnyRun>> {
    const indexed = await fromIndex<WorkflowRunWithoutData>((shard) => shard.listIndexedRuns(params));
    if (indexed === undefined) return resolveAll(mergePages(await onEveryStore(stores, (store) => store.listRuns(params)), runKey, params.pagination));
    const page = mergePages<AnyRun>(indexed, runKey, params.pagination);
    if (params.resolveData === ResolveData.None) return resolveAll(page);
    // The index keeps runs without their payloads, so the page's runs come whole from their stores.
    const runs = await getMany(page.data.map(({ runId }) => runId), { resolveData: params.resolveData });
    return { ...page, data: runs.filter((run) => run !== null) };
  }

  /**
   * Every index shard's page of a cross-run list, or undefined while a store has still to backfill
   * them, when the caller asks every store instead.
   */
  async function fromIndex<T>(read: (shard: Store) => Promise<IndexPage<T>>): Promise<PaginatedResponse<T>[] | undefined> {
    const answers = await onStores(stores, indexShardNames(), read);
    return answers.every(isComplete) ? answers.map(({ value }) => value) : undefined;
  }

  function getStep(runId: string, stepId: string, params: WithoutData): Promise<StepWithoutData>;
  function getStep(runId: string, stepId: string, params?: WithData): Promise<Step>;
  function getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<AnyStep>;
  async function getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<AnyStep> {
    return resolveAll(await onRun(stores, runId, (store) => store.getStep(runId, stepId, params)));
  }

  function listSteps(params: ListWorkflowRunStepsParams & WithoutData): Promise<PaginatedResponse<StepWithoutData>>;
  function listSteps(params: ListWorkflowRunStepsParams & WithData): Promise<PaginatedResponse<Step>>;
  function listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<AnyStep>>;
  async function listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<AnyStep>> {
    return resolveAll(await onRun(stores, params.runId, (store) => store.listSteps(params), answersUnknown));
  }

  async function create<T extends RunCreatedEventRequest>(
    runId: string | null, data: T, params?: CreateEventParams): Promise<EventResult<T['eventType']>>;
  async function create<T extends CreateEventRequest>(
    runId: string, data: T, params?: CreateEventParams): Promise<EventResult<T['eventType']>>;
  async function create<T extends RunCreatedEventRequest | CreateEventRequest>(
    runId: string | null, data: T, params?: CreateEventParams,
  ): Promise<EventResult<T['eventType']>> {
    // The store mints run IDs too, but a payload's R2 key needs the run ID before the write.
    const id = runId ?? (data.eventType === EventType.RunCreated ? `wrun_${ulid()}` : null);
    if (id === null) throw new Error('runId is required');
    if (data.eventType === EventType.StepCompleted && !isSingleOrchestrator(data.specVersion)) await failOversizedResult(id, data, params);
    // Payloads reach R2 before the event commits, so a committed ref never points at nothing.
    const written = new Map<string, Uint8Array>();
    const request = await offloadEvent(id, data, written);
    // A new run lives in its shard from its first event.
    const result = data.eventType === EventType.RunCreated
      ? await storeShard(stores, id).createEvent(id, request, params)
      : data.eventType === EventType.StepCreated && params?.inBand !== true
        ? await gathered(id, request, params)
        : await onRun(stores, id, (store) => store.createEvent(id, request, params));
    // SAFETY: the store answers with the result for the request's own event type; RPC types it as the union.
    return resolveAll(result, written) as Promise<EventResult<T['eventType']>>;
  }

  /**
   * Fails a spec 8 step whose result is over the payload limit, then rejects its completion as a
   * conflict. The SDK redelivers any other rejected completion, and on the node engine the
   * redelivery re-runs the step until it exceeds its retries, hiding why. A conflict it skips, then
   * replays the step's failure, on the first attempt with either engine. A spec 9 orchestrator
   * fails the run on the payload error itself, and fences writes it did not make.
   */
  async function failOversizedResult(runId: string, data: EventOf<EventType.StepCompleted>, params?: CreateEventParams) {
    const { result, stepName } = data.eventData;
    if (!(result instanceof Uint8Array) || result.byteLength <= PAYLOAD_MAX_BYTES) return;
    const message = `A ${result.byteLength}-byte result is over the ${PAYLOAD_MAX_BYTES}-byte payload limit`;
    // How the SDK serializes `new FatalError(message)`: a format prefix, then devalue.
    const error = encoder.encode(`devl${stringify(new FatalError(message), {
      FatalError: (value: unknown) => value instanceof FatalError && { message: value.message },
    })}`);
    await onRun(stores, runId, (store) => store.createEvent(runId, {
      eventType: EventType.StepFailed, specVersion: data.specVersion, correlationId: data.correlationId, eventData: { stepName, error },
    }, params));
    throw new EntityConflictError(`Step ${data.correlationId} failed: ${message}`);
  }

  return {
    runs: { get: getRun, getMany, list: listRuns, waitForTerminalStatus, cancelMany: (request) => cancelMany(request, getRun, create) },
    steps: { get: getStep, list: listSteps },
    events: {
      create,
      createBatch: async (runId, events, params) => {
        const written = new Map<string, Uint8Array>();
        const offloaded = await Promise.all(events.map(async (item) => ({ ...item, event: await offloadEvent(runId, item.event, written) })));
        return resolveAll(await onRun(stores, runId, (store) => store.createEventBatch(runId, offloaded, params)), written);
      },
      get: async (runId, eventId, params) =>
        resolveAll(await onRun(stores, runId, (store) => store.getEvent(runId, eventId, params?.resolveData))),
      // An AbortSignal cannot cross RPC; the contract lets a World ignore it.
      list: async ({ signal: _signal, ...params }) =>
        resolveAll(await onRun(stores, params.runId, (store) => store.listEvents(params), answersUnknown)),
      listByCorrelationId: async (params) =>
        resolveAll(await onRun(stores, params.runId, (store) => store.listEventsByCorrelationId(params), answersUnknown)),
      // A long poll per page on the run's store, which answers as soon as an event commits.
      subscribe: (runId, afterSlot, onEvent, { onError } = {}) => {
        let live = true;
        const follow = async () => {
          for (let slot = afterSlot; live;) {
            // RPC stubs type Event-bearing results as never; this is `WorldStore.eventsAfter`'s own type.
            const page: { events: Event[]; done: boolean } = await onRun(stores, runId, (store) => store.eventsAfter(runId, slot));
            for (const event of await resolveAll(page.events)) {
              if (!live) return;
              onEvent(event);
              slot = requireEventSlot(event.eventId);
            }
            if (page.done) throw new Error(`Run ${runId} is finished; its events have all been delivered`);
          }
        };
        follow().catch((error: unknown) => {
          if (live) onError?.(error);
        });
        return () => {
          live = false;
        };
      },
    },
    // A hook lives with its run, its token's copy on the token's shard, and its list row on an index
    // shard of its run's epoch. A hook ID does not name its run, so `hooks.get` asks one index shard per epoch.
    hooks: {
      get: async (hookId, params) => {
        // RPC stubs widen the answer's type; this is `WorldStore.findIndexedHook`'s own.
        const indexed: IndexAnswer<Hook | undefined>[] = await onStores(stores, hookIndexShards(hookId),
          (shard) => shard.findIndexedHook(hookId, params?.resolveData));
        const found = indexed.every(isComplete)
          ? indexed.find(({ value }) => value !== undefined)?.value
          : (await onEveryStore(stores, (store) => store.findHook(hookId, params?.resolveData))).find((hook) => hook !== undefined);
        if (found === undefined) throw new HookNotFoundError(hookId);
        return resolveAll(found);
      },
      getByToken: async (token, params) => resolveAll(await tokenShard(stores, token).getHookByToken(token, params?.resolveData)),
      list: async (params) => {
        if (params.runId !== undefined) return resolveAll(await onRun(stores, params.runId, (store) => store.listHooks(params), answersUnknown));
        const pages = await fromIndex<Hook>((shard) => shard.listIndexedHooks(params));
        return resolveAll(mergePages(pages ?? await onEveryStore(stores, (store) => store.listHooks(params)), hookKey, params.pagination));
      },
    },
  };
}
