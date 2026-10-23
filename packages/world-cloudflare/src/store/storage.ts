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
import { STORE_NAME } from '../constants.js';
import { EventType } from '../enums.js';
import { createPayloads, type Written } from '../lib/payloads.js';
import { transientAsWorldError } from '../lib/transient.js';
import type { WorldEnv } from '../types.js';

type AnyRun = WorkflowRun | WorkflowRunWithoutData;
type AnyStep = Step | StepWithoutData;
type WithoutData = { resolveData: 'none' };
type WithData = { resolveData?: 'all' };

/**
 * The World's `Storage`. Every call is an RPC to the store Durable Object. Large payloads go to R2
 * before the call and come back from R2 after it, so the object only sees refs to them.
 */
export function createStorage(env: Pick<WorldEnv, 'WORLD_STORE' | 'PAYLOADS'>): Storage {
  // A stub belongs to one request, so never cache it in the World singleton.
  const store = () => transientAsWorldError(env.WORLD_STORE.getByName(STORE_NAME));
  const { offloadEvent, resolveAll } = createPayloads(env.PAYLOADS);

  function getRun(id: string, params: GetWorkflowRunParams & WithoutData): Promise<WorkflowRunWithoutData>;
  function getRun(id: string, params?: GetWorkflowRunParams & WithData): Promise<WorkflowRun>;
  function getRun(id: string, params?: GetWorkflowRunParams): Promise<AnyRun>;
  async function getRun(id: string, params?: GetWorkflowRunParams): Promise<AnyRun> {
    return resolveAll(await store().getRun(id, params));
  }

  function getMany(ids: readonly string[], params: GetWorkflowRunParams & WithoutData): Promise<(WorkflowRunWithoutData | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams & WithData): Promise<(WorkflowRun | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams): Promise<(AnyRun | null)[]>;
  async function getMany(ids: readonly string[], params?: GetWorkflowRunParams): Promise<(AnyRun | null)[]> {
    return resolveAll(await store().getManyRuns([...ids], params?.resolveData));
  }

  function waitForTerminalStatus(id: string, params: WaitForTerminalRunStatusParams & WithoutData): Promise<WorkflowRunWithoutData>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams & WithData): Promise<WorkflowRun>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams): Promise<AnyRun>;
  async function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams): Promise<AnyRun> {
    // An AbortSignal cannot cross RPC; the World contract lets us ignore it.
    return resolveAll(await store().waitForTerminalRun(id, params?.timeoutMs ?? 0, params?.resolveData));
  }

  function listRuns(params: ListWorkflowRunsParams & WithoutData): Promise<PaginatedResponse<WorkflowRunWithoutData>>;
  function listRuns(params?: ListWorkflowRunsParams & WithData): Promise<PaginatedResponse<WorkflowRun>>;
  function listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<AnyRun>>;
  async function listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<AnyRun>> {
    return resolveAll(await store().listRuns(params));
  }

  function getStep(runId: string, stepId: string, params: WithoutData): Promise<StepWithoutData>;
  function getStep(runId: string, stepId: string, params?: WithData): Promise<Step>;
  function getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<AnyStep>;
  async function getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<AnyStep> {
    return resolveAll(await store().getStep(runId, stepId, params));
  }

  function listSteps(params: ListWorkflowRunStepsParams & WithoutData): Promise<PaginatedResponse<StepWithoutData>>;
  function listSteps(params: ListWorkflowRunStepsParams & WithData): Promise<PaginatedResponse<Step>>;
  function listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<AnyStep>>;
  async function listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<AnyStep>> {
    return resolveAll(await store().listSteps(params));
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
    const written: Written = new Map();
    const result = await store().createEvent(id, await offloadEvent(id, data, written), params);
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
        return resolveAll(await store().createEventBatch(runId, offloaded, params), written);
      },
      get: async (runId, eventId, params) => resolveAll(await store().getEvent(runId, eventId, params?.resolveData)),
      list: async (params) => resolveAll(await store().listEvents(params)),
      listByCorrelationId: async (params) => resolveAll(await store().listEventsByCorrelationId(params)),
    },
    hooks: {
      get: async (hookId, params) => resolveAll(await store().getHook(hookId, params?.resolveData)),
      getByToken: async (token, params) => resolveAll(await store().getHookByToken(token, params?.resolveData)),
      list: async (params) => resolveAll(await store().listHooks(params)),
    },
  };
}
