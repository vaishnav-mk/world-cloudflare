import { HookNotFoundError, WorkflowRunNotFoundError, WorkflowWorldError } from '@workflow/errors';
import type { WorldEnv } from '@workflow/world-cloudflare';
import { HttpError } from './http.js';
import { subscribeEvents } from './routes/events.js';
import { resumeHookByToken } from './routes/hooks.js';
import { getRunResult, getRunStatus, listRunHooks, startRun } from './routes/runs.js';
import { listStreams, readStream } from './routes/streams.js';
import { worldRpc } from './routes/world.js';

export type RouteContext = {
  request: Request;
  url: URL;
  env: WorldEnv & { WORLD_RPC_TOKEN?: string };
  /** Named path segments, such as `runId`. */
  params: Record<string, string>;
};

type Route = {
  method: 'GET' | 'POST';
  pattern: URLPattern;
  handle: (context: RouteContext) => Promise<Response>;
};

const route = (method: Route['method'], pathname: string, handle: Route['handle']): Route =>
  ({ method, pattern: new URLPattern({ pathname }), handle });

const routes: Route[] = [
  route('POST', '/runs', startRun),
  route('GET', '/runs/:runId', getRunStatus),
  route('GET', '/runs/:runId/result', getRunResult),
  route('GET', '/runs/:runId/hooks', listRunHooks),
  route('GET', '/runs/:runId/events', subscribeEvents),
  route('GET', '/runs/:runId/streams', listStreams),
  route('GET', '/runs/:runId/streams/:namespace', readStream),
  route('POST', '/hooks/:token', resumeHookByToken),
  route('POST', '/world/rpc', worldRpc),
];

export async function handleRequest(request: Request, env: WorldEnv): Promise<Response> {
  const url = new URL(request.url);
  for (const { method, pattern, handle } of routes) {
    const match = method === request.method ? pattern.exec(url) : null;
    if (!match) continue;
    const params = Object.fromEntries(
      Object.entries(match.pathname.groups).filter((entry): entry is [string, string] => entry[1] !== undefined),
    );
    try {
      return await handle({ request, url, env, params });
    } catch (error) {
      return errorResponse(error);
    }
  }
  return new Response('Not found', { status: 404 });
}

function errorResponse(error: unknown): Response {
  if (error instanceof HttpError) return new Response(error.message, { status: error.status });
  if (WorkflowRunNotFoundError.is(error)) return new Response('Run not found', { status: 404 });
  if (HookNotFoundError.is(error)) return new Response('Hook not found', { status: 404 });
  // A World error carries its status, such as 503 for a Durable Object that is overloaded or resetting.
  if (WorkflowWorldError.is(error) && error.status !== undefined) {
    return new Response(error.message, { status: error.status, headers: error.status === 503 ? { 'retry-after': '1' } : {} });
  }
  console.error('Request failed', error);
  return new Response('Internal error', { status: 500 });
}
