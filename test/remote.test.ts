import { env } from 'cloudflare:test';
import { WorkflowRunNotFoundError } from '@workflow/errors';
import { afterEach, expect, it, vi } from 'vitest';
import { createWorld } from '@workflow/world-cloudflare';
import { handleWorldRpc } from '../packages/world-cloudflare/src/remote/server.js';
import { createWorld as createRemoteWorld } from '../packages/world-cloudflare/src/remote/client.js';

const TOKEN = 'test-token';
const world = createWorld(env);
const bytes = (value: string) => new TextEncoder().encode(value);

/** Routes the remote client's fetches straight into the RPC handler. */
function remoteWorld(token = TOKEN) {
  vi.stubGlobal('fetch', (input: string | URL, init?: RequestInit) => handleWorldRpc(new Request(input, init), world, TOKEN));
  return createRemoteWorld({ url: 'https://world.test', token });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

it('does not exist without a token and rejects a wrong one', async () => {
  const request = () => new Request('https://world.test/world/rpc', { method: 'POST', headers: { authorization: 'Bearer nope' }, body: '' });
  expect((await handleWorldRpc(request(), world, undefined)).status).toBe(404);
  expect((await handleWorldRpc(request(), world, TOKEN)).status).toBe(401);
  await expect(remoteWorld('wrong').runs.get('wrun_x')).rejects.toThrow('HTTP 401');
});

it('round-trips World calls with dates, bytes and SDK errors intact', async () => {
  const remote = remoteWorld();
  const created = await remote.events.create(null, {
    eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName: 'remote', input: bytes('in') },
  });
  const runId = created.run!.runId;
  const run = await remote.runs.get(runId);
  expect(run.createdAt).toBeInstanceOf(Date);
  expect(run.input).toEqual(bytes('in'));
  expect((await remote.events.list({ runId })).data.map((event) => event.eventType)).toEqual(['run_created']);

  const missing = await remote.runs.get('wrun_missing').catch((error: unknown) => error);
  expect(WorkflowRunNotFoundError.is(missing)).toBe(true);
  expect(missing).toBeInstanceOf(WorkflowRunNotFoundError);
});

it('streams bytes through streams.get', async () => {
  const remote = remoteWorld();
  const runId = `wrun_${crypto.randomUUID()}`;
  await remote.streams.writeMulti!(runId, 'out', [bytes('a'), bytes('b')]);
  await remote.streams.close(runId, 'out');
  const text = await new Response(await remote.streams.get(runId, 'out')).text();
  expect(text).toBe('ab');
});
