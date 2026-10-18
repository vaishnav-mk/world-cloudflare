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
import { STORE_NAME } from '../constants.js';
import type { WorldEnv } from '../types.js';

type AnyRun = WorkflowRun | WorkflowRunWithoutData;
type AnyStep = Step | StepWithoutData;
type WithoutData = { resolveData: 'none' };
type WithData = { resolveData?: 'all' };

/** The World's `Storage`. Every call is an RPC to the store Durable Object. */
export function createStorage(env: Pick<WorldEnv, 'WORLD_STORE'>): Storage {
  // A stub belongs to one request, so never cache it in the World singleton.
  const store = () => env.WORLD_STORE.getByName(STORE_NAME);

  function getRun(id: string, params: GetWorkflowRunParams & WithoutData): Promise<WorkflowRunWithoutData>;
  function getRun(id: string, params?: GetWorkflowRunParams & WithData): Promise<WorkflowRun>;
  function getRun(id: string, params?: GetWorkflowRunParams): Promise<AnyRun>;
  function getRun(id: string, params?: GetWorkflowRunParams): Promise<AnyRun> {
    return store().getRun(id, params);
  }

  function getMany(ids: readonly string[], params: GetWorkflowRunParams & WithoutData): Promise<(WorkflowRunWithoutData | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams & WithData): Promise<(WorkflowRun | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams): Promise<(AnyRun | null)[]>;
  function getMany(ids: readonly string[], params?: GetWorkflowRunParams): Promise<(AnyRun | null)[]> {
    return store().getManyRuns([...ids], params?.resolveData);
  }

  function waitForTerminalStatus(id: string, params: WaitForTerminalRunStatusParams & WithoutData): Promise<WorkflowRunWithoutData>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams & WithData): Promise<WorkflowRun>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams): Promise<AnyRun>;
  function waitForTerminalStatus(id: string, params?: WaitForTerminalRunStatusParams): Promise<AnyRun> {
    // An AbortSignal cannot cross RPC; the World contract lets us ignore it.
    return store().waitForTerminalRun(id, params?.timeoutMs ?? 0, params?.resolveData);
  }

  function listRuns(params: ListWorkflowRunsParams & WithoutData): Promise<PaginatedResponse<WorkflowRunWithoutData>>;
  function listRuns(params?: ListWorkflowRunsParams & WithData): Promise<PaginatedResponse<WorkflowRun>>;
  function listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<AnyRun>>;
  function listRuns(params?: ListWorkflowRunsParams): Promise<PaginatedResponse<AnyRun>> {
    return store().listRuns(params);
  }

  function getStep(runId: string, stepId: string, params: WithoutData): Promise<StepWithoutData>;
  function getStep(runId: string, stepId: string, params?: WithData): Promise<Step>;
  function getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<AnyStep>;
  function getStep(runId: string, stepId: string, params?: { resolveData?: 'none' | 'all' }): Promise<AnyStep> {
    return store().getStep(runId, stepId, params);
  }

  function listSteps(params: ListWorkflowRunStepsParams & WithoutData): Promise<PaginatedResponse<StepWithoutData>>;
  function listSteps(params: ListWorkflowRunStepsParams & WithData): Promise<PaginatedResponse<Step>>;
  function listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<AnyStep>>;
  function listSteps(params: ListWorkflowRunStepsParams): Promise<PaginatedResponse<AnyStep>> {
    return store().listSteps(params);
  }

  async function create<T extends RunCreatedEventRequest>(
    runId: string | null, data: T, params?: CreateEventParams): Promise<EventResult<T['eventType']>>;
  async function create<T extends CreateEventRequest>(
    runId: string, data: T, params?: CreateEventParams): Promise<EventResult<T['eventType']>>;
  async function create<T extends RunCreatedEventRequest | CreateEventRequest>(
    runId: string | null, data: T, params?: CreateEventParams,
  ): Promise<EventResult<T['eventType']>> {
    // The store returns the union result; narrow it to the request's event type.
    return store().createEvent(runId, data, params) as Promise<EventResult<T['eventType']>>;
  }

  return {
    runs: { get: getRun, getMany, list: listRuns, waitForTerminalStatus },
    steps: { get: getStep, list: listSteps },
    events: {
      create,
      get: (runId, eventId, params) => store().getEvent(runId, eventId, params?.resolveData),
      list: (params) => store().listEvents(params),
      listByCorrelationId: (params) => store().listEventsByCorrelationId(params),
    },
    hooks: {
      get: (hookId, params) => store().getHook(hookId, params?.resolveData),
      getByToken: (token, params) => store().getHookByToken(token, params?.resolveData),
      list: (params) => store().listHooks(params),
    },
  };
}
