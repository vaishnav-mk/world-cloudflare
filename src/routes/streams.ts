import { getRun } from 'workflow/api';
import { ResolveData } from '@workflow/world-cloudflare';
import type { RouteContext } from '../router.js';
import { world } from '../world.js';

export async function listStreams({ params }: RouteContext): Promise<Response> {
  return Response.json({ names: await world.streams.list(params.runId) });
}

/** `GET /runs/:runId/streams/:namespace`: the named stream's bytes, following live writes. */
export async function readStream({ params }: RouteContext): Promise<Response> {
  await world.runs.get(params.runId, { resolveData: ResolveData.None });
  const stream = getRun(params.runId).getReadable<Uint8Array>({ namespace: params.namespace });
  return new Response(stream, { headers: { 'content-type': 'application/octet-stream' } });
}
