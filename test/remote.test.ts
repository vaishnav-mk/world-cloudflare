import { env } from 'cloudflare:test';
import { HookConflictError, WorkflowRunNotFoundError } from '@workflow/errors';
import { startHookId } from '@workflow/world';
import { afterEach, expect, it, vi } from 'vitest';
import { start } from 'workflow/api';
import { createWorld } from '@workflow/world-cloudflare';
import { handleWorldRpc } from '../packages/world-cloudflare/src/remote/server.js';
import { connectWorld, createWorld as createRemoteWorld } from '../packages/world-cloudflare/src/remote/client.js';
import { encode } from '../packages/world-cloudflare/src/remote/protocol.js';

const TOKEN = 'test-token';
const world = createWorld(env);
const bytes = (value: string) => new TextEncoder().encode(value);

/** Routes the remote client's fetches straight into the RPC handler. */
function remoteWorld(token = TOKEN, backend = world) {
  vi.stubGlobal('fetch', (input: string | URL, init?: RequestInit) => handleWorldRpc(new Request(input, init), backend, TOKEN));
  return createRemoteWorld({ url: 'https://world.test', token });
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

it('discovers the backend fence before SDK start creates a spec 9 run', async () => {
  vi.stubEnv('WORKFLOW_SPEC_VERSION', '9');
  const backend = createWorld(env);
  const remote = remoteWorld(TOKEN, backend);
  await remote.start?.();
  const run = await start({ workflowId: 'workflow//remote//spec9' }, [], { world: remote });
  expect((await backend.runs.get(run.runId)).specVersion).toBe(9);
  expect(remote.specVersion).toBe(backend.specVersion);
  expect(remote.capabilities?.inBandFence).toBe(true);
});

it('keeps the synchronous factory conservative without making a discovery request', () => {
  vi.stubEnv('WORKFLOW_SPEC_VERSION', '9');
  const fetch = vi.fn();
  vi.stubGlobal('fetch', fetch);
  const remote = createRemoteWorld({ url: 'https://world.test', token: TOKEN });
  expect(remote.specVersion).toBe(8);
  expect(remote.capabilities).toBeUndefined();
  expect(fetch).not.toHaveBeenCalled();
});

it('connects with the server version rather than the local environment, without advertising invocation', async () => {
  const backend = { ...world, specVersion: 9 };
  remoteWorld(TOKEN, backend);
  vi.stubEnv('WORKFLOW_SPEC_VERSION', '8');
  const fetch = vi.fn(globalThis.fetch);
  vi.stubGlobal('fetch', fetch);
  const remote = await connectWorld({ url: 'https://world.test', token: TOKEN });
  await Promise.all([remote.start?.(), remote.start?.()]);
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(remote.specVersion).toBe(9);
  const { invoke: _invoke, ...capabilities } = backend.capabilities ?? {};
  expect(remote.capabilities).toEqual(capabilities);
  expect(remote.capabilities?.invoke).toBeUndefined();
});

it('does not infer missing backend capabilities', async () => {
  const remote = remoteWorld(TOKEN, { ...world, capabilities: undefined });
  await remote.start?.();
  expect(remote.capabilities).toEqual({});
  await expect(start({ workflowId: 'workflow//remote//hook' }, [], {
    world: remote, experimental_hook: { token: crypto.randomUUID() },
  })).rejects.toThrow('requires a World that supports atomic start Hooks');
});

it('rejects malformed discovery and retries initialization without keeping partial capabilities', async () => {
  const remote = remoteWorld();
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(encode({
    ok: true, value: { specVersion: '9', capabilities: { inBandFence: true } },
  }))));
  await expect(remote.start?.()).rejects.toThrow('Invalid World RPC discovery response');
  expect(remote.capabilities).toBeUndefined();
  vi.stubGlobal('fetch', (input: string | URL, init?: RequestInit) => handleWorldRpc(new Request(input, init), world, TOKEN));
  await Promise.all([remote.start?.(), remote.start?.()]);
  expect(remote.capabilities?.inBandFence).toBe(true);
});

it('refuses discovery on an older RPC server without guessing capabilities', async () => {
  const remote = remoteWorld();
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('Invalid World RPC request', { status: 400 })));
  await expect(remote.start?.()).rejects.toThrow('HTTP 400');
  expect(remote.capabilities).toBeUndefined();
});

it('round-trips SDK atomic start Hook admission and rejects a duplicate with its owning run', async () => {
  const remote = remoteWorld();
  await remote.start?.();
  const token = crypto.randomUUID();
  const workflow = { workflowId: 'workflow//remote//hook' };
  const options = { world: remote, experimental_hook: { token } };
  const run = await start(workflow, [], options);
  expect((await remote.runs.get(run.runId)).startHook).toMatchObject({ token, hookId: startHookId(run.runId) });
  expect(await remote.hooks.getByToken(token)).toMatchObject({ runId: run.runId, hookId: startHookId(run.runId) });
  const duplicate = await start(workflow, [], options).catch((error: unknown) => error);
  expect(HookConflictError.is(duplicate)).toBe(true);
  expect(duplicate).toBeInstanceOf(HookConflictError);
  expect(duplicate).toMatchObject({ conflictingRunId: run.runId });
});

it('does not exist without a token and rejects a wrong one', async () => {
  const request = () => new Request('https://world.test/world/rpc', { method: 'POST', headers: { authorization: 'Bearer nope' }, body: '' });
  expect((await handleWorldRpc(request(), world, undefined)).status).toBe(404);
  expect((await handleWorldRpc(request(), world, TOKEN)).status).toBe(401);
  const unauthorized = remoteWorld('wrong');
  await expect(unauthorized.runs.get('wrun_x')).rejects.toThrow('HTTP 401');
  await expect(unauthorized.start?.()).rejects.toThrow('HTTP 401');
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

  expect((await remote.runs.cancelMany!({ runIds: [runId] })).results).toEqual([{ runId, outcome: 'cancelled' }]);
});

it('round-trips a payload kept in R2', async () => {
  const remote = remoteWorld();
  const input = new Uint8Array(3 * 1024 * 1024).map((_, index) => index % 251);
  const created = await remote.events.create(null, {
    eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName: 'remote', input },
  });
  const runId = created.run!.runId;
  expect((await env.PAYLOADS.list({ prefix: `runs/${runId}/` })).objects).toHaveLength(1);
  expect(Buffer.from((await remote.runs.get(runId)).input as Uint8Array).equals(input)).toBe(true);
});

it('streams bytes through streams.get', async () => {
  const remote = remoteWorld();
  const runId = `wrun_${crypto.randomUUID()}`;
  await remote.streams.writeMulti!(runId, 'out', [bytes('a'), bytes('b')]);
  await remote.streams.close(runId, 'out');
  const text = await new Response(await remote.streams.get(runId, 'out')).text();
  expect(text).toBe('ab');
});

it('saves, loads and deletes snapshots with their metadata', async () => {
  const remote = remoteWorld();
  const runId = `wrun_${crypto.randomUUID()}`;
  const metadata = { eventsCursor: 'eid:7', createdAt: new Date(1_700_000_000_000), eventCount: 7, formatVersion: 3 };
  expect(await remote.experimental_snapshots!.load(runId)).toBeNull();
  await remote.experimental_snapshots!.save(runId, new Uint8Array([0, 255, 1]), metadata);
  expect(await remote.experimental_snapshots!.load(runId)).toEqual({ data: new Uint8Array([0, 255, 1]), metadata });
  await remote.experimental_snapshots!.delete(runId);
  expect(await world.experimental_snapshots!.load(runId)).toBeNull();
});
