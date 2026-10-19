import type { World } from '@workflow/world';
import { decodeRequest, encode, RPC_CONTENT_TYPE, RpcMethod, type RpcError, type RpcResult } from './protocol.js';

const encoder = new TextEncoder();

/**
 * Serves World calls from a Node process, such as `workflow web` running the remote client.
 * Every call needs `Authorization: Bearer <token>`, because the endpoint can write events. With
 * no token configured the endpoint does not exist.
 */
export async function handleWorldRpc(request: Request, world: World, token: string | undefined): Promise<Response> {
  if (!token) return new Response('Not found', { status: 404 });
  if (!(await hasToken(request, token))) return new Response('Unauthorized', { status: 401 });
  let call;
  try {
    call = decodeRequest(await request.text());
  } catch {
    return new Response('Invalid World RPC request', { status: 400 });
  }
  try {
    if (call.method === RpcMethod.StreamsGet) {
      const [runId, name, startIndex] = call.args as Parameters<World['streams']['get']>;
      return new Response(await world.streams.get(runId, name, startIndex), {
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    return respond({ ok: true, value: await invoke(world, call.method, call.args) });
  } catch (error) {
    return respond({ ok: false, error: describe(error) });
  }
}

function invoke(world: World, method: RpcMethod, args: unknown[]): unknown {
  const [scope, name] = method.split('.');
  // SAFETY: `method` is one of the RpcMethod paths, each naming a World function or an optional
  // group of them, such as `experimental_snapshots`.
  const owner = (name === undefined ? world : world[scope as keyof World]) as Record<string, unknown> | undefined;
  const fn = owner?.[name ?? scope];
  if (typeof fn !== 'function') throw new Error(`This World does not implement ${method}`);
  return fn.apply(owner, args);
}

function respond(result: RpcResult): Response {
  return new Response(encode(result), { headers: { 'content-type': RPC_CONTENT_TYPE } });
}

/** Keeps the class name and fields (`runId`, `token`...) so the client can rebuild the error. */
function describe(error: unknown): RpcError {
  if (!(error instanceof Error)) return { name: 'Error', message: String(error), props: {} };
  const { name, message, stack: _stack, cause: _cause, ...props } = error as Error & Record<string, unknown>;
  return { name, message, props: JSON.parse(JSON.stringify(props)) };
}

/** Compares digests so the check takes the same time whatever the token's length or content. */
async function hasToken(request: Request, token: string): Promise<boolean> {
  const given = request.headers.get('authorization')?.replace(/^Bearer /, '') ?? '';
  const [a, b] = await Promise.all([given, token].map(async (value) =>
    new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(value)))));
  let difference = 0;
  for (const [index, byte] of a.entries()) difference |= byte ^ b[index];
  return difference === 0;
}
