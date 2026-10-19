import { getRun, start } from 'workflow/api';
import { toJson } from '@workflow/world-cloudflare';
import { HttpError, readJson } from '../http.js';
import type { RouteContext } from '../router.js';
import { world } from '../world.js';
import { DEFAULT_WORKFLOW, isWorkflowName, workflows } from '../workflows.js';

/** `POST /runs` with `{ "name"?: string, "value": number }`. */
export async function startRun({ request }: RouteContext): Promise<Response> {
  const body = await readJson(request);
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new HttpError(400, 'Invalid JSON');
  const { value, name } = body as { value?: unknown; name?: unknown };
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new HttpError(400, 'Invalid value');
  const workflow = name ?? DEFAULT_WORKFLOW;
  if (!isWorkflowName(workflow)) throw new HttpError(400, 'Unknown workflow');
  const run = await start(workflows[workflow], [value]);
  return Response.json({ runId: run.runId }, { status: 202 });
}

const json = (value: unknown) => new Response(toJson(value), { headers: { 'content-type': 'application/json' } });

/** The run, with binary input and output as base64. */
export async function getRunStatus({ params }: RouteContext): Promise<Response> {
  return json(await world.runs.get(params.runId));
}

/** Waits for the run to finish, then returns its decoded result. */
export async function getRunResult({ params }: RouteContext): Promise<Response> {
  return Response.json({ value: await getRun(params.runId).returnValue });
}

export async function listRunHooks({ params }: RouteContext): Promise<Response> {
  return json(await world.hooks.list({ runId: params.runId }));
}
