import { SELF } from 'cloudflare:test';
import { expect, it, vi } from 'vitest';
import { world } from '../src/world.js';

// Runs with WORKFLOW_ENCRYPTION_SECRET set (wrangler.encrypted.test.jsonc), on QuickJS with snapshots.
const json = { 'content-type': 'application/json' };
const prefix = (bytes: Uint8Array) => new TextDecoder().decode(bytes.subarray(0, 4));

async function startRun(name: string, value: number): Promise<string> {
  const response = await SELF.fetch('https://local/runs', { method: 'POST', headers: json, body: JSON.stringify({ name, value }) });
  expect(response.status).toBe(202);
  return (await response.json() as { runId: string }).runId;
}

const result = async (runId: string) => (await (await SELF.fetch(`https://local/runs/${runId}/result`)).json() as { value: unknown }).value;

/** Every binary field the World holds for the run: its input and output, and each event payload. */
async function storedPayloads(runId: string): Promise<Uint8Array[]> {
  const run = await world.runs.get(runId);
  const events = (await world.events.list({ runId })).data;
  return [run.input, run.output, ...events.flatMap((event) => Object.values('eventData' in event ? event.eventData ?? {} : {}))]
    .filter((value) => value instanceof Uint8Array);
}

it('keeps payloads and VM snapshots encrypted at rest, and the run restores from its snapshot', async () => {
  const snapshots = world.experimental_snapshots!;
  using saves = vi.spyOn(snapshots, 'save');
  using loads = vi.spyOn(snapshots, 'load');
  using lists = vi.spyOn(world.events, 'list');
  const runId = await startRun('delayed', 15);
  expect(await result(runId)).toBe(30);

  const payloads = await storedPayloads(runId);
  expect(payloads.length).toBeGreaterThanOrEqual(4);
  expect(payloads.map(prefix)).toEqual(payloads.map(() => 'encr'));

  // Without a key the SDK would not snapshot at all: this config leaves WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED unset.
  const saved = saves.mock.calls.find(([id]) => id === runId);
  expect(prefix(saved?.[1] ?? new Uint8Array())).toBe('encr');
  // The next invocation decrypted it and read only the events after its cursor.
  const loadIndex = loads.mock.calls.findIndex(([id]) => id === runId);
  expect(await loads.mock.results[loadIndex]?.value).not.toBeNull();
  const readsAfterLoad = lists.mock.calls.flatMap(([params], index) =>
    params.runId === runId && lists.mock.invocationCallOrder[index] > loads.mock.invocationCallOrder[loadIndex]
      ? [params.pagination?.cursor] : []);
  expect(readsAfterLoad[0]).toBe(saved?.[2].eventsCursor);
});

it('encrypts a hook payload and a stream chunk', async () => {
  const runId = await startRun('approval', 11);
  const token = await vi.waitFor(async () => {
    const hooks = await world.hooks.list({ runId });
    expect(hooks.data[0]?.token).toBeTruthy();
    return hooks.data[0].token;
  }, { interval: 100, timeout: 10_000 });
  expect((await SELF.fetch(`https://local/hooks/${token}`, { method: 'POST', headers: json, body: '7' })).status).toBe(200);
  expect(await result(runId)).toBe(18);
  const received = (await world.events.list({ runId })).data.find((event) => event.eventType === 'hook_received');
  expect(received?.eventType === 'hook_received' && received.eventData.payload).toBeInstanceOf(Uint8Array);
  if (received?.eventType === 'hook_received') expect(['encr', 'encp']).toContain(prefix(received.eventData.payload as Uint8Array));

  const streamed = await startRun('progress', 13);
  expect(await result(streamed)).toBe(13);
  const names = await world.streams.list(streamed);
  const chunks = (await Promise.all(names.map((name) => world.streams.getChunks(streamed, name)))).flatMap((page) => page.data);
  expect(chunks.length).toBeGreaterThan(0);
  expect(chunks.some(({ data }) => Buffer.from(data).includes('value=13'))).toBe(false);
  // The reader decrypts it.
  const text = new TextDecoder().decode(await (await SELF.fetch(`https://local/runs/${streamed}/streams/progress`)).arrayBuffer());
  expect(text).toBe('value=13');
});
