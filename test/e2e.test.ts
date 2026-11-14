import { env, SELF } from 'cloudflare:test';
import { expect, it, vi } from 'vitest';
import { HookConflictError } from '@workflow/errors';
import { resumeHook, start } from 'workflow/api';
import { PAYLOAD_MAX_BYTES } from '../packages/world-cloudflare/src/constants.js';
import { workflows } from '../src/workflows.js';
import { MAX_BODY_BYTES } from '../src/http.js';
import { world } from '../src/world.js';

const headers = {};
const MiB = 1024 * 1024;

async function startRun(name: string, value: number | number[]): Promise<string> {
  const response = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name, value }),
  });
  expect(response.status).toBe(202);
  return (await response.json() as { runId: string }).runId;
}

const payloadObjects = async (runId: string) => (await env.PAYLOADS.list({ prefix: `runs/${runId}/` })).objects;
const runStatus = async (runId: string) => (await (await SELF.fetch(`https://local/runs/${runId}`)).json() as { status: string }).status;
const runEvents = async (runId: string) => (await SELF.fetch(`https://local/runs/${runId}/events`)).text();

/** The run's error as the SDK serialized it, decompressed, as text. */
async function runError(runId: string): Promise<string> {
  const run = await (await SELF.fetch(`https://local/runs/${runId}`)).json() as { error: { $bytes: string } };
  const bytes = Buffer.from(run.error.$bytes, 'base64');
  if (bytes.subarray(0, 4).toString() !== 'gzip') return bytes.toString();
  return new Response(new Blob([bytes.subarray(4)]).stream().pipeThrough(new DecompressionStream('gzip'))).text();
}

it('rejects oversized streaming JSON without Content-Length', async () => {
  let sent = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      const chunk = new Uint8Array(Math.min(MiB, MAX_BODY_BYTES + 1 - sent));
      sent += chunk.byteLength;
      controller.enqueue(chunk);
      if (sent > MAX_BODY_BYTES) controller.close();
    },
  });
  const response = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' }, body,
  });
  expect(response.status).toBe(413);
});

it('returns not-found errors for unknown runs and hooks', async () => {
  expect((await SELF.fetch('https://local/runs/not-real', { headers })).status).toBe(404);
  const missingHook = await SELF.fetch('https://local/hooks/not-real', {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' }, body: '7',
  });
  expect(missingHook.status).toBe(404);
});

it('retries a transient step and reports a permanent step failure', async () => {
  const create = async (name: string) => {
    const response = await SELF.fetch('https://local/runs', {
      method: 'POST', headers: { ...headers, 'content-type': 'application/json' },
      body: JSON.stringify({ name, value: 11 }),
    });
    expect(response.status).toBe(202);
    return (await response.json() as { runId: string }).runId;
  };
  const retry = await create('retryOnce');
  await expect.poll(async () => (await (await SELF.fetch(`https://local/runs/${retry}`, { headers })).json() as { status: string }).status,
    { interval: 200, timeout: 15_000 }).toBe('completed');
  expect(await (await SELF.fetch(`https://local/runs/${retry}/result`, { headers })).json()).toEqual({ value: 22 });
  expect(await (await SELF.fetch(`https://local/runs/${retry}/events`, { headers })).text()).toContain('event: step_retrying');

  const fatal = await create('failPermanently');
  await expect.poll(async () => (await (await SELF.fetch(`https://local/runs/${fatal}`, { headers })).json() as { status: string }).status,
    { interval: 200, timeout: 15_000 }).toBe('failed');
  const events = await (await SELF.fetch(`https://local/runs/${fatal}/events`, { headers })).text();
  expect(events).toContain('event: step_failed');
  expect(events).toContain('event: run_failed');
});

it('isolates overlapping requests and reuses no request-scoped RPC stub', async () => {
  const started = await Promise.all([3, 5, 7, 11].map((value) => SELF.fetch('https://local/runs', {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' },
    body: JSON.stringify({ value }),
  })));
  const ids = await Promise.all(started.map(async (response) => {
    expect(response.status).toBe(202);
    return (await response.json() as { runId: string }).runId;
  }));
  await expect.poll(async () => Promise.all(ids.map(async (runId) => {
    const status = await SELF.fetch(`https://local/runs/${runId}`, { headers });
    return (await status.json() as { status: string }).status;
  })), { interval: 100, timeout: 15_000 }).toEqual(['completed', 'completed', 'completed', 'completed']);
  const results = await Promise.all(ids.map(async (id) => {
    const response = await SELF.fetch(`https://local/runs/${id}/result`, { headers });
    return (await response.json() as { value: number }).value;
  }));
  expect(results).toEqual([6, 10, 14, 22]);
});

it('persists a sleep and streams live events through completion', async () => {
  const response = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' },
    body: JSON.stringify({ name: 'delayed', value: 9 }),
  });
  expect(response.status).toBe(202);
  const { runId } = await response.json() as { runId: string };
  const stream = await SELF.fetch(`https://local/runs/${runId}/events`, { headers });
  expect(stream.status).toBe(200);
  const text = await stream.text();
  expect(text).toContain('event: wait_created');
  expect(text).toContain('event: wait_completed');
  expect(text).toContain('event: run_completed');
  expect(await (await SELF.fetch(`https://local/runs/${runId}/result`, { headers })).json()).toEqual({ value: 18 });
});

it('streams step output to a reader that connects before the step', async () => {
  const started = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' },
    body: JSON.stringify({ name: 'progress', value: 13 }),
  });
  expect(started.status).toBe(202);
  const { runId } = await started.json() as { runId: string };
  const stream = await SELF.fetch(`https://local/runs/${runId}/streams/progress`, { headers });
  expect(stream.status).toBe(200);
  expect(new TextDecoder().decode(await stream.arrayBuffer())).toBe('value=13');
  await expect.poll(async () => {
    const response = await SELF.fetch(`https://local/runs/${runId}`, { headers });
    return (await response.json() as { status: string }).status;
  }, { interval: 100, timeout: 15_000 }).toBe('completed');
});

it('resumes a durable hook and continues replay', async () => {
  const started = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' },
    body: JSON.stringify({ name: 'approval', value: 11 }),
  });
  expect(started.status).toBe(202);
  const { runId } = await started.json() as { runId: string };
  const token = await vi.waitFor(async () => {
    const response = await SELF.fetch(`https://local/runs/${runId}/hooks`, { headers });
    const hooks = await response.json() as { data: { token: string }[] };
    expect(hooks.data[0]?.token).toBeTruthy();
    return hooks.data[0].token;
  }, { interval: 100, timeout: 10_000 });
  const resumed = await SELF.fetch(`https://local/hooks/${token}`, {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' }, body: JSON.stringify(7),
  });
  expect(resumed.status).toBe(200);
  await expect.poll(async () => {
    const response = await SELF.fetch(`https://local/runs/${runId}`, { headers });
    return (await response.json() as { status: string }).status;
  }, { interval: 100, timeout: 15_000 }).toBe('completed');
  expect(await (await SELF.fetch(`https://local/runs/${runId}/result`, { headers })).json()).toEqual({ value: 18 });
});

it("takes a hook token over from another run, which reads the takeover and the resume goes to the claimer", async () => {
  const value = Math.floor(Math.random() * 1e9);
  const statusOf = async (runId: string) => (await (await SELF.fetch(`https://local/runs/${runId}`, { headers })).json() as { status: string }).status;
  const hookOf = (runId: string) => vi.waitFor(async () => {
    const hooks = await (await SELF.fetch(`https://local/runs/${runId}/hooks`, { headers })).json() as { data: { token: string }[] };
    expect(hooks.data[0]?.token).toBe(`force-${value}`);
  }, { interval: 100, timeout: 10_000 });
  const owner = await startRun('forceOwner', value);
  await hookOf(owner);
  const claimer = await startRun('forceClaimer', value);
  await hookOf(claimer);
  // The token's shard disposed of the owner's hook, and the claimer woke it to read that.
  await expect.poll(() => statusOf(owner), { interval: 100, timeout: 15_000 }).toBe('completed');
  expect(await (await SELF.fetch(`https://local/runs/${owner}/result`, { headers })).json()).toEqual({ value: -1 });
  const resumed = await SELF.fetch(`https://local/hooks/force-${value}`, {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' }, body: JSON.stringify(1),
  });
  expect(resumed.status).toBe(200);
  await expect.poll(() => statusOf(claimer), { interval: 100, timeout: 15_000 }).toBe('completed');
  expect(await (await SELF.fetch(`https://local/runs/${claimer}/result`, { headers })).json()).toEqual({ value: value + 1 });
});

it('admits one start per start Hook token, and resumes the Hook before the run opens it', async () => {
  const value = Math.floor(Math.random() * 1e9);
  const token = `start-${value}`;
  const run = await start(workflows.startHooked, [value], { experimental_hook: { token } });
  // The owner holds the token until it ends, so a second start with it is refused.
  const duplicate = await start(workflows.startHooked, [value], { experimental_hook: { token } }).catch((error: unknown) => error);
  expect(HookConflictError.is(duplicate)).toBe(true);
  expect(duplicate).toMatchObject({ conflictingRunId: run.runId });
  // Resumable as soon as start() returns, whether or not the workflow has opened the Hook yet.
  await resumeHook(token, 1);
  expect(await run.returnValue).toBe(value + 1);
});

it.skipIf(process.env.WORKFLOW_EXPERIMENTAL_DYNAMIC_WORKFLOWS !== '1')('runs dynamic workflows, with their code inline or uploaded ahead', async () => {
  const steps = { double: { stepId: 'step//./workflows/demo//double' } };
  const source = (padding: string) => `async function workflow(value) {\n  "use workflow";\n  // ${padding}\n  return steps.double(value);\n}`;
  const inline = await start(source(''), [5], { experimental_dynamic: { steps } });
  // Random text that compresses past the inline limit, so start() uploads it first.
  const noise = Array.from(crypto.getRandomValues(new Uint8Array(60_000)), (byte) => (byte % 36).toString(36)).join('');
  const uploaded = await start(source(noise), [7], { experimental_dynamic: { steps } });
  expect([await inline.returnValue, await uploaded.returnValue]).toEqual([10, 14]);
  const code = await world.runs.get(uploaded.runId);
  expect(code.dynamicWorkflowCode).toBeInstanceOf(Uint8Array);
  expect((await env.PAYLOADS.list({ prefix: `runs/${uploaded.runId}/dynamic-code/` })).objects).toHaveLength(1);
});

it('starts a v5 run, executes a real step, returns its result, and replays the subscription', async () => {
  const start = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' }, body: JSON.stringify({ value: 21 }),
  });
  expect(start.status).toBe(202);
  const { runId } = await start.json() as { runId: string };
  await expect.poll(async () => {
    const response = await SELF.fetch(`https://local/runs/${runId}`, { headers });
    return (await response.json() as { status: string }).status;
  }, { interval: 100, timeout: 15_000 }).toBe('completed');
  const result = await SELF.fetch(`https://local/runs/${runId}/result`, { headers });
  expect(result.status).toBe(200);
  expect(await result.json()).toEqual({ value: 42 });
  const subscription = await SELF.fetch(`https://local/runs/${runId}/events`, { headers });
  expect(subscription.status).toBe(200);
  const events = await subscription.text();
  expect(events).toContain('event: run_created');
  expect(events).toContain('event: step_completed');
  expect(events).toContain('event: run_completed');
  const firstId = events.match(/id: (evnt_\d+)/)?.[1];
  expect(firstId).toBeTruthy();
  const resumed = await SELF.fetch(`https://local/runs/${runId}/events?cursor=${firstId}&types=run_completed`, { headers });
  const resumedEvents = await resumed.text();
  expect(resumedEvents).toContain('event: run_completed');
  expect(resumedEvents).not.toContain('event: run_created');
});

it('runs 200 parallel steps to completion', async () => {
  const started = Date.now();
  const runId = await startRun('parallelSteps', 200);
  await expect.poll(async () => (await (await SELF.fetch(`https://local/runs/${runId}`, { headers })).json() as { status: string }).status,
    { interval: 250, timeout: 60_000 }).toBe('completed');
  expect(await (await SELF.fetch(`https://local/runs/${runId}/result`, { headers })).json()).toEqual({ value: 200 });
  // Spec 9 runs take about 30 s for this here today; the tighter bound is for spec 8.
  expect(Date.now() - started).toBeLessThan(process.env.WORKFLOW_SPEC_VERSION === '9' ? 60_000 : 30_000);
}, 90_000);

it('stores incompressible step output larger than 64 KiB', async () => {
  const response = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' },
    body: JSON.stringify({ name: 'stepOutput', value: 256 * 1024 }),
  });
  const { runId } = await response.json() as { runId: string };
  await expect.poll(async () => (await (await SELF.fetch(`https://local/runs/${runId}`, { headers })).json() as { status: string }).status,
    { interval: 200, timeout: 20_000 }).toBe('completed');
  expect(await (await SELF.fetch(`https://local/runs/${runId}/result`, { headers })).json()).toEqual({ value: 256 * 1024 });
  // Binary payloads stream as base64, not as JSON index maps about 12 times their size.
  const events = await (await SELF.fetch(`https://local/runs/${runId}/events`, { headers })).text();
  expect(events.length).toBeLessThan(2 * 256 * 1024);
});

// VM snapshots belong to the QuickJS engine.
it.skipIf(process.env.WORKFLOW_VM !== 'quickjs')('saves a VM snapshot and restores the next invocation from it', async () => {
  const snapshots = world.experimental_snapshots!;
  using saves = vi.spyOn(snapshots, 'save');
  using loads = vi.spyOn(snapshots, 'load');
  using lists = vi.spyOn(world.events, 'list');
  const response = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { ...headers, 'content-type': 'application/json' },
    body: JSON.stringify({ name: 'delayed', value: 15 }),
  });
  const { runId } = await response.json() as { runId: string };
  await expect.poll(async () => (await (await SELF.fetch(`https://local/runs/${runId}`, { headers })).json() as { status: string }).status,
    { interval: 100, timeout: 15_000 }).toBe('completed');
  expect(await (await SELF.fetch(`https://local/runs/${runId}/result`, { headers })).json()).toEqual({ value: 30 });

  // The first invocation saved a snapshot when it suspended on the sleep.
  const saved = saves.mock.calls.find(([id]) => id === runId);
  expect(saved?.[1].length).toBeGreaterThan(0);
  expect(saved?.[2].eventCount).toBeGreaterThanOrEqual(2);
  // The second invocation loaded it back, metadata intact.
  const loadIndex = loads.mock.calls.findIndex(([id]) => id === runId);
  const loaded = await loads.mock.results[loadIndex]?.value;
  expect(loaded?.metadata).toEqual(saved?.[2]);
  // Then it read only the events after the snapshot's cursor and never replayed the whole log.
  const readsAfterLoad = lists.mock.calls.flatMap(([params], index) =>
    params.runId === runId && lists.mock.invocationCallOrder[index] > loads.mock.invocationCallOrder[loadIndex]
      ? [params.pagination?.cursor] : []);
  expect(readsAfterLoad[0]).toBe(saved?.[2].eventsCursor);
  expect(readsAfterLoad).not.toContain(undefined);
  // The finished run's snapshot is deleted.
  await expect.poll(() => snapshots.load(runId), { interval: 100, timeout: 5_000 }).toBeNull();
});

it('passes a 12 MiB step output to the next step through R2', async () => {
  const runId = await startRun('stepInput', 12 * MiB);
  const result = await SELF.fetch(`https://local/runs/${runId}/result`, { headers });
  expect(await result.json()).toEqual({ value: 12 * MiB });
  // The step's output and the next step's input are each an R2 object.
  expect((await payloadObjects(runId)).length).toBe(2);
  // The event subscription reads the store directly, so it shows refs instead of 12 MiB of base64.
  const events = await (await SELF.fetch(`https://local/runs/${runId}/events`, { headers })).text();
  expect(events).toContain(`"$ref":"runs/${runId}/`);
  expect(events.length).toBeLessThan(MiB);
}, 60_000);

it('returns a 12 MiB run output', async () => {
  const runId = await startRun('runOutput', 12 * MiB);
  const result = await (await SELF.fetch(`https://local/runs/${runId}/result`, { headers })).json() as { value: { $bytes: string } };
  expect(Buffer.from(result.value.$bytes, 'base64').byteLength).toBe(12 * MiB);
  expect((await payloadObjects(runId)).length).toBeGreaterThanOrEqual(1);
}, 60_000);

it('fails a step whose output is over the payload limit on its first attempt', async () => {
  const runId = await startRun('stepOutput', 20 * MiB);
  await expect.poll(() => runStatus(runId), { interval: 250, timeout: 30_000 }).toBe('failed');
  expect(await runError(runId)).toContain(`byte result is over the ${PAYLOAD_MAX_BYTES}-byte payload limit`);
  expect((await runEvents(runId)).match(/event: step_started/g)).toHaveLength(1);
  expect(await payloadObjects(runId)).toHaveLength(0);
}, 60_000);

it('streams a 12 MiB chunk', async () => {
  const runId = await startRun('streamChunk', 12 * MiB);
  const stream = await SELF.fetch(`https://local/runs/${runId}/streams/limits`, { headers });
  expect((await stream.arrayBuffer()).byteLength).toBe(12 * MiB);
  expect(await (await SELF.fetch(`https://local/runs/${runId}/result`, { headers })).json()).toEqual({ value: 12 * MiB });
}, 60_000);

it('passes a 256 MiB byte stream in 64 MiB chunks from one step to the next', async () => {
  const runId = await startRun('streamBytes', [256, 64]);
  const result = await (await SELF.fetch(`https://local/runs/${runId}/result`, { headers })).json() as { value: unknown };
  // The reading step checked every byte against the pattern the writing step made.
  expect(result.value).toMatchObject({ bytes: 256 * MiB, firstMismatch: -1 });
  // Each chunk is an R2 object, and the steps' payloads carry only the stream's name.
  expect((await payloadObjects(runId)).map(({ size }) => size)).toEqual([64 * MiB, 64 * MiB, 64 * MiB, 64 * MiB]);
}, 120_000);

it('resumes a hook with a 12 MB payload', async () => {
  const runId = await startRun('hookPayload', 0);
  const token = await vi.waitFor(async () => {
    const hooks = await (await SELF.fetch(`https://local/runs/${runId}/hooks`, { headers })).json() as { data: { token: string }[] };
    expect(hooks.data[0]?.token).toBeTruthy();
    return hooks.data[0].token;
  }, { interval: 100, timeout: 10_000 });
  // Random base64, so the SDK's gzip cannot shrink it under the inline threshold.
  const random = new Uint8Array(9_000_000);
  for (let offset = 0; offset < random.length; offset += 65_536) crypto.getRandomValues(random.subarray(offset, offset + 65_536));
  const body = JSON.stringify(Buffer.from(random).toString('base64'));
  const resumed = await SELF.fetch(`https://local/hooks/${token}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body });
  expect(resumed.status).toBe(200);
  expect(await (await SELF.fetch(`https://local/runs/${runId}/result`, { headers })).json()).toEqual({ value: body.length });
  expect((await payloadObjects(runId)).length).toBe(1);
}, 60_000);
