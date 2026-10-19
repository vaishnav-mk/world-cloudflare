import { SELF } from 'cloudflare:test';
import { expect, it, vi } from 'vitest';

const headers = {};

it('rejects oversized streaming JSON without Content-Length', async () => {
  const body = new ReadableStream<Uint8Array>({
    start(controller) { controller.enqueue(new Uint8Array(1_048_577)); controller.close(); },
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
  const retry = await create('retry');
  await expect.poll(async () => (await (await SELF.fetch(`https://local/runs/${retry}`, { headers })).json() as { status: string }).status,
    { interval: 200, timeout: 15_000 }).toBe('completed');
  expect(await (await SELF.fetch(`https://local/runs/${retry}/result`, { headers })).json()).toEqual({ value: 22 });
  expect(await (await SELF.fetch(`https://local/runs/${retry}/events`, { headers })).text()).toContain('event: step_retrying');

  const fatal = await create('fatal');
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
