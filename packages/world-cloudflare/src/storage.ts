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
import type { Env } from './store.js';

export function createStorage(env: Env): Storage {
  // A Durable Object stub belongs to one request. Never cache it in the World singleton.
  // shortcut: one DO makes cross-run hook tokens atomic; shard after a measured write bottleneck.
  const getStore = () => env.WORLD_STORE.getByName('global');

  function getRun(id: string, params: GetWorkflowRunParams & { resolveData: 'none' }): Promise<WorkflowRunWithoutData>;
  function getRun(id: string, params?: GetWorkflowRunParams & { resolveData?: 'all' }): Promise<WorkflowRun>;
  function getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData>;
  function getRun(id: string, params?: GetWorkflowRunParams): Promise<WorkflowRun | WorkflowRunWithoutData> {
    return getStore().getRun(id, params);
  }

  function getMany(ids: readonly string[], params: GetWorkflowRunParams & { resolveData: 'none' }): Promise<(WorkflowRunWithoutData | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams & { resolveData?: 'all' }): Promise<(WorkflowRun | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams): Promise<(WorkflowRun | WorkflowRunWithoutData | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams): Promise<(WorkflowRun | WorkflowRunWithoutData | null)[]> {
    return getStore().getManyRuns([...ids], params?.resolveData);
  }

  function waitForTerminalStatus(id: string, params: WaitForTerminalRunStatusParams & { resolveData: 'none' }): Promise<WorkflowRunWithoutData>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams & { resolveData?: 'all' }): Promise<WorkflowRun>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams): Promise<WorkflowRun | WorkflowRunWithoutData>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams): Promise<WorkflowRun | WorkflowRunWithoutData> {
    // AbortSignal cannot cross DO RPC; the World contract permits ignoring it.
    return getStore().waitForTerminalRun(id, params?.timeoutMs ?? 0, params?.resolveData);
  }

  function listRuns(params: ListWorkflowRunsParams & { resolveData: 'none' }): Promise<PaginatedResponse<WorkflowRunWithoutData>>;
  function listRuns(params?: ListWorkflowRunsParams & { resolveData?: 'all' }): Promise<PaginatedResponse<WorkflowRun>>;
  function listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>>;
  function listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>> {
    return getStore().listRuns(params);
  }

  function getStep(runId: string, stepId: string, params: { resolveData: 'none' }): Promise<StepWithoutData>;
  function getStep(runId: string, stepId: string, params?: { resolveData?: 'all' }): Promise<Step>;
  function getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<Step | StepWithoutData>;
  function getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<Step | StepWithoutData> {
    return getStore().getStep(runId, stepId, params);
  }

  function listSteps(params: ListWorkflowRunStepsParams & { resolveData: 'none' }): Promise<PaginatedResponse<StepWithoutData>>;
  function listSteps(params: ListWorkflowRunStepsParams & { resolveData?: 'all' }): Promise<PaginatedResponse<Step>>;
  function listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>>;
  function listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>> {
    return getStore().listSteps(params);
  }

  async function create<T extends RunCreatedEventRequest>(runId: string | null, data: T, params?: CreateEventParams): Promise<EventResult<T['eventType']>>;
  async function create<T extends CreateEventRequest>(runId: string, data: T, params?: CreateEventParams): Promise<EventResult<T['eventType']>>;
  async function create<T extends RunCreatedEventRequest | CreateEventRequest>(runId: string | null, data: T, params?: CreateEventParams): Promise<EventResult<T['eventType']>> {
    // The DO dispatches on eventType; this narrows its union result to the request's discriminant.
    return getStore().createEvent(runId, data, params) as Promise<EventResult<T['eventType']>>;
  }

  return {
    runs: { get: getRun, getMany, list: listRuns, waitForTerminalStatus },
    steps: { get: getStep, list: listSteps },
    events: {
      create,
      get: (runId, eventId, params) => getStore().getEvent(runId, eventId, params?.resolveData),
      list: (params) => getStore().listEvents(params),
      listByCorrelationId: (params) => getStore().listEventsByCorrelationId(params),
    },
    hooks: {
      get: (hookId, params) => getStore().getHook(hookId, params?.resolveData),
      getByToken: (token, params) => getStore().getHookByToken(token, params?.resolveData),
      list: (params) => getStore().listHooks(params),
    },
  };
}
