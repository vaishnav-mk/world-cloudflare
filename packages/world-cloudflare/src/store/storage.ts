import type {
  CreateEventParams,
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
import { ulid } from 'ulid';
import { HookNotFoundError } from '@workflow/errors';
import { EventType } from '../enums.js';
import { groupBy } from '../lib/group.js';
import { createPayloads } from '../lib/payloads.js';
import type { WorldEnv } from '../types.js';
import { mergePages } from './pages.js';
import { legacyStore, onEveryStore, onRun, storeShard, storeShardFor } from './route.js';

type AnyRun = WorkflowRun | WorkflowRunWithoutData;
type AnyStep = Step | StepWithoutData;
type WithoutData = { resolveData: 'none' };
type WithData = { resolveData?: 'all' };

const runKey = (run: AnyRun): [number, string] => [run.createdAt.getTime(), run.runId];
const hookKey = (hook: Hook): [number, string] => [hook.createdAt.getTime(), hook.hookId];

/**
 * The World's `Storage`. Every call is an RPC to a store Durable Object: the run's shard for a
 * run-scoped call, the token's shard for a token, and every store for a cross-run list. Large
 * payloads go to R2 before the call and come back from R2 after it, so the objects only see refs to them.
 */
export function createStorage(env: Pick<WorldEnv, 'WORLD_STORE' | 'PAYLOADS'>): Storage {
  // A stub belongs to one request, so the route helpers make one per call; never cache it in the World singleton.
  const stores = env.WORLD_STORE;
  const answersUnknown = { answersUnknown: true };
  const { offloadEvent, resolveAll } = createPayloads(env.PAYLOADS);

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
    await Promise.all([...groupBy(new Set(ids), storeShardFor)].map(async ([, group]) =>
      record(group, await storeShard(stores, group[0]).getManyRuns(group, params?.resolveData))));
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
    const pages = await onEveryStore(stores, (store) => store.listRuns(params));
    return resolveAll(mergePages(pages, runKey, params.pagination));
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
    // Payloads reach R2 before the event commits, so a committed ref never points at nothing.
    const written = new Map<string, Uint8Array>();
    const request = await offloadEvent(id, data, written);
    // A new run lives in its shard from its first event.
    const result = data.eventType === EventType.RunCreated
      ? await storeShard(stores, id).createEvent(id, request, params)
      : await onRun(stores, id, (store) => store.createEvent(id, request, params));
    // The store returns the union result; narrow it to the request's event type.
    return resolveAll(result, written) as Promise<EventResult<T['eventType']>>;
  }

  return {
    runs: { get: getRun, getMany, list: listRuns, waitForTerminalStatus },
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
      list: async (params) => resolveAll(await onRun(stores, params.runId, (store) => store.listEvents(params), answersUnknown)),
      listByCorrelationId: async (params) =>
        resolveAll(await onRun(stores, params.runId, (store) => store.listEventsByCorrelationId(params), answersUnknown)),
    },
    // A hook lives with its run, and its token's copy on the token's shard. The SDK runtime calls
    // `hooks.get` only for invocations, which this World does not declare, so it asks every store.
    hooks: {
      get: async (hookId, params) => {
        const found = (await onEveryStore(stores, (store) => store.findHook(hookId, params?.resolveData))).find((hook) => hook !== undefined);
        if (found === undefined) throw new HookNotFoundError(hookId);
        return resolveAll(found);
      },
      getByToken: async (token, params) => resolveAll(await storeShard(stores, token).getHookByToken(token, params?.resolveData)),
      list: async (params) => {
        if (params.runId !== undefined) return resolveAll(await onRun(stores, params.runId, (store) => store.listHooks(params), answersUnknown));
        return resolveAll(mergePages(await onEveryStore(stores, (store) => store.listHooks(params)), hookKey, params.pagination));
      },
    },
  };
}
