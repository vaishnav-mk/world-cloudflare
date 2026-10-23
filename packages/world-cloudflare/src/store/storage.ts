import type {
  CreateEventParams,
  CreateEventRequest,
  EventResult,
  GetWorkflowRunParams,
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
import { EventType } from '../enums.js';
import { createPayloads } from '../lib/payloads.js';
import type { WorldEnv } from '../types.js';
import { directory, onRun, runStore } from './route.js';

type AnyRun = WorkflowRun | WorkflowRunWithoutData;
type AnyStep = Step | StepWithoutData;
type WithoutData = { resolveData: 'none' };
type WithData = { resolveData?: 'all' };

/**
 * The World's `Storage`. Every call is an RPC to a store Durable Object: the run's own store for a
 * run-scoped call, the directory for cross-run reads. Large payloads go to R2 before the call and
 * come back from R2 after it, so the objects only see refs to them.
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
    return resolveAll(await directory(stores).getManyRuns([...ids], params?.resolveData));
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
  async function listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<AnyRun>> {
    return resolveAll(await directory(stores).listRuns(params));
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
    // A new run lives in its own store from its first event.
    const result = data.eventType === EventType.RunCreated
      ? await runStore(stores, id).createEvent(id, request, params)
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
    // The directory keeps a copy of every hook, so only a run's own list goes to the run's store.
    hooks: {
      get: async (hookId, params) => resolveAll(await directory(stores).getHook(hookId, params?.resolveData)),
      getByToken: async (token, params) => resolveAll(await directory(stores).getHookByToken(token, params?.resolveData)),
      list: async (params) => resolveAll(await (params.runId === undefined
        ? directory(stores).listHooks(params)
        : onRun(stores, params.runId, (store) => store.listHooks(params), answersUnknown))),
    },
  };
}
