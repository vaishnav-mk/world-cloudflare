import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { storeShardFor } from '../packages/world-cloudflare/src/lib/shards.js';
import { PAYLOAD_INLINE_MAX_BYTES, PAYLOAD_MAX_BYTES, PAYLOAD_REF_PREFIX } from '../packages/world-cloudflare/src/constants.js';
import { toJson } from '../packages/world-cloudflare/src/lib/json.js';
import { createStorage } from '../packages/world-cloudflare/src/store/storage.js';
import type { WorldStore } from '../packages/world-cloudflare/src/store/world-store.js';
import { createStreamer } from '../packages/world-cloudflare/src/streams/streamer.js';

const store = createStorage(env);
const { streams } = createStreamer(env);
// SAFETY: the stub serves WorldStore's methods; its generated RPC types reduce entities to `never`.
const raw = (runId: string) => env.WORLD_STORE.getByName(storeShardFor(runId)) as unknown as WorldStore;
/** The run and step as the run's store keeps them, before the World client resolves refs. */
const storedRun = (runId: string) => raw(runId).getRun(runId);
const storedStep = (runId: string) => raw(runId).getStep(runId, 'big');

function random(length: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(length);
  for (let offset = 0; offset < length; offset += 65_536) {
    crypto.getRandomValues(bytes.subarray(offset, Math.min(offset + 65_536, length)));
  }
  return bytes;
}
/** Compares without handing vitest megabytes to diff. */
const same = (actual: unknown, expected: Uint8Array) =>
  actual instanceof Uint8Array && Buffer.from(actual).equals(Buffer.from(expected));
const objects = async (runId: string) => (await env.PAYLOADS.list({ prefix: `runs/${runId}/` })).objects;
const isRef = (bytes: unknown) => bytes instanceof Uint8Array && PAYLOAD_REF_PREFIX.every((byte, index) => bytes[index] === byte);

async function startedRun(input = new Uint8Array([1])) {
  const { run } = await store.events.create(null, {
    eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName: 'example', input },
  });
  const runId = run!.runId;
  await store.events.create(runId, { eventType: 'run_started' });
  return runId;
}

describe('payloads in R2', () => {
  it('rejects a payload over the limit before writing anything', async () => {
    const runId = await startedRun();
    const write = store.events.create(runId, {
      eventType: 'step_created', correlationId: 'huge', eventData: { stepName: 'work', input: new Uint8Array(PAYLOAD_MAX_BYTES + 1) },
    });
    await expect(write).rejects.toMatchObject({ status: 413 });
    expect(await objects(runId)).toHaveLength(0);
    expect((await store.events.list({ runId })).data).toHaveLength(2);
  });

  it('offloads a large step input written through a batch', async () => {
    const runId = await startedRun();
    const input = random(PAYLOAD_INLINE_MAX_BYTES + 1);
    const { results } = await store.events.createBatch!(runId, [
      { event: { eventType: 'step_created', correlationId: 'big', eventData: { stepName: 'work', input } } },
    ]);
    expect(results[0]?.status).toBe(200);
    expect(await objects(runId)).toHaveLength(1);
    expect(isRef((await storedStep(runId)).input)).toBe(true);
    expect(same((await store.steps.get(runId, 'big')).input, input)).toBe(true);
  });

  it('keeps a payload at the threshold inline', async () => {
    const input = random(PAYLOAD_INLINE_MAX_BYTES);
    const runId = await startedRun(input);
    expect(await objects(runId)).toHaveLength(0);
    expect(same((await storedRun(runId)).input, input)).toBe(true);
    expect(same((await store.runs.get(runId)).input, input)).toBe(true);
  });

  it('offloads larger payloads and round-trips them byte for byte', async () => {
    const runInput = random(PAYLOAD_INLINE_MAX_BYTES + 1);
    const runId = await startedRun(runInput);
    const stepInput = random(3 * 1024 * 1024);
    await store.events.create(runId, { eventType: 'step_created', correlationId: 'big', eventData: { stepName: 'big', input: stepInput } });
    const started = await store.events.create(runId, { eventType: 'step_started', correlationId: 'big' });
    expect(same(started.step?.input, stepInput)).toBe(true);
    const output = random(PAYLOAD_INLINE_MAX_BYTES + 1);
    const completed = await store.events.create(runId, {
      eventType: 'step_completed', correlationId: 'big', eventData: { stepName: 'big', result: output },
    });
    // The write's own result reuses the bytes it wrote instead of reading them back.
    expect(completed.step?.output).toBe(output);

    // R2 holds the three payloads, and the store holds only refs to them.
    expect(await objects(runId)).toHaveLength(3);
    const stored = await storedStep(runId);
    expect(isRef(stored.input) && isRef(stored.output)).toBe(true);
    expect(isRef((await storedRun(runId)).input)).toBe(true);
    expect((stored.output as Uint8Array).byteLength).toBeLessThan(200);

    const events = (await store.events.list({ runId })).data;
    const byType = (type: string) => events.find((event) => event.eventType === type) as { eventId: string; eventData: Record<string, unknown> };
    expect(same(byType('run_created').eventData.input, runInput)).toBe(true);
    expect(same(byType('step_created').eventData.input, stepInput)).toBe(true);
    expect(same(byType('step_completed').eventData.result, output)).toBe(true);
    const event = await store.events.get(runId, byType('step_completed').eventId) as { eventData: { result: unknown } };
    expect(same(event.eventData.result, output)).toBe(true);
    const step = await store.steps.get(runId, 'big');
    expect(same(step.input, stepInput) && same(step.output, output)).toBe(true);
    expect(same((await store.runs.get(runId)).input, runInput)).toBe(true);
    const byCorrelation = (await store.events.listByCorrelationId({ runId, correlationId: 'big' })).data;
    expect(byCorrelation.some((item) => 'eventData' in item && same((item.eventData as { result?: unknown }).result, output))).toBe(true);

    // `resolveData: 'none'` drops payloads without touching R2, and the event subscription shows the ref.
    expect((await store.steps.get(runId, 'big', { resolveData: 'none' })).output).toBeUndefined();
    expect(toJson(stored)).toContain(`"$ref":"runs/${runId}/`);
  });

  it('offloads small bytes that look like a ref, so stored refs are never user data', async () => {
    const runId = await startedRun();
    const lookalike = Uint8Array.of(...PAYLOAD_REF_PREFIX, ...new TextEncoder().encode('runs/other/secret'));
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'hook', eventData: { token: crypto.randomUUID() } });
    await store.events.create(runId, { eventType: 'hook_received', correlationId: 'hook', eventData: { payload: lookalike } });
    expect(await objects(runId)).toHaveLength(1);
    const received = (await store.events.list({ runId })).data.find((event) => event.eventType === 'hook_received');
    expect(same((received as { eventData: { payload: unknown } }).eventData.payload, lookalike)).toBe(true);
  });

  it('round-trips stream chunks above the threshold', async () => {
    const runId = crypto.randomUUID();
    const small = random(10);
    const large = random(PAYLOAD_INLINE_MAX_BYTES + 1);
    const larger = random(12 * 1024 * 1024);
    await streams.writeMulti!(runId, 'out', [small, large]);
    await streams.write(runId, 'out', larger);
    await streams.close(runId, 'out');
    expect(await objects(runId)).toHaveLength(2);
    const { data } = await streams.getChunks(runId, 'out');
    expect(data.map(({ index }) => index)).toEqual([0, 1, 2]);
    expect(same(data[0].data, small) && same(data[1].data, large) && same(data[2].data, larger)).toBe(true);
    const read = new Uint8Array(await new Response(await streams.get(runId, 'out')).arrayBuffer());
    expect(same(read, Buffer.concat([small, large, larger]))).toBe(true);
  });
});
