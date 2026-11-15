import { getRun, start } from 'workflow/api';
import { toJson } from 'world-cloudflare';
import { HttpError, readJson } from '../http.js';
import type { RouteContext } from '../router.js';
import { world } from '../world.js';
import { DEFAULT_WORKFLOW, isWorkflowName, workflows } from '../workflows.js';

const MAX_ARGS = 4;
const isFiniteNumber = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);

/** `POST /runs` with `{ "name"?: string, "value": number | number[] }`; an array is the workflow's arguments. */
export async function startRun({ request }: RouteContext): Promise<Response> {
  const body = await readJson(request);
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new HttpError(400, 'Invalid JSON');
  // SAFETY: checked to be a plain object above; each field stays `unknown` until it is checked.
  const { value, name } = body as { value?: unknown; name?: unknown };
  const args = Array.isArray(value) ? value : [value];
  if (args.length === 0 || args.length > MAX_ARGS || !args.every(isFiniteNumber)) throw new HttpError(400, 'Invalid value');
  const workflow = name ?? DEFAULT_WORKFLOW;
  if (!isWorkflowName(workflow)) throw new HttpError(400, 'Unknown workflow');
  // By ID, because the workflows' argument lists differ.
  const { workflowId } = workflows[workflow];
  const run = await start({ workflowId }, args);
  return Response.json({ runId: run.runId }, { status: 202 });
}

const json = (value: unknown) => new Response(toJson(value), { headers: { 'content-type': 'application/json' } });

/** The run, with binary input and output as base64. */
export async function getRunStatus({ params }: RouteContext): Promise<Response> {
  return json(await world.runs.get(params.runId));
}

/** Waits for the run to finish, then returns its decoded result, with binary values as base64. */
export async function getRunResult({ params }: RouteContext): Promise<Response> {
  return json({ value: await getRun(params.runId).returnValue });
}

export async function listRunHooks({ params }: RouteContext): Promise<Response> {
  return json(await world.hooks.list({ runId: params.runId }));
}
