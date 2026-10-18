import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';

const base = process.env.WORKFLOW_BASE_URL?.replace(/\/$/, '');
if (!base) throw new Error('WORKFLOW_BASE_URL is required');

async function request(path, { stream = false, ...options } = {}) {
  return fetch(`${base}${path}`, {
    ...options,
    headers: options.headers ?? {},
    signal: AbortSignal.timeout(stream ? 60_000 : 15_000),
  });
}

async function json(path, options = {}) {
  const response = await request(path, options);
  const text = await response.text();
  if (!response.ok) throw new Error(`${options.method ?? 'GET'} ${path}: HTTP ${response.status} ${text.slice(0, 160)}`);
  return JSON.parse(text);
}

async function start(name, value) {
  const response = await request('/runs', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name, value }),
  });
  const text = await response.text();
  assert.equal(response.status, 202, `start ${name}: ${text.slice(0, 160)}`);
  return JSON.parse(text).runId;
}

async function waitFor(id, condition, timeoutMs = 60_000) {
  const until = Date.now() + timeoutMs;
  do {
    const run = await json(`/runs/${id}`);
    if (condition(run)) return run;
    if (run.status === 'failed' || run.status === 'cancelled') throw new Error(`${id} ended ${run.status} (${run.errorCode ?? 'unknown'})`);
    await sleep(250);
  } while (Date.now() < until);
  throw new Error(`Timed out waiting for ${id}`);
}

const publicRead = await fetch(`${base}/runs`, { signal: AbortSignal.timeout(15_000) });
assert.equal(publicRead.status, 404);
for (const body of ['not-json', 'null', '{"name":"__proto__","value":1}', '{"value":"bad"}']) {
  const response = await request('/runs', { method: 'POST', headers: { 'content-type': 'application/json' }, body });
  assert.equal(response.status, 400, `invalid input ${body}: ${response.status}`);
}
const oversized = await request('/runs', {
  method: 'POST', headers: { 'content-type': 'application/json' }, duplex: 'half',
  body: new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(1_048_577)); controller.close(); } }),
});
assert.equal(oversized.status, 413, 'streaming body limit');
assert.equal((await request('/runs/not-real')).status, 404);
assert.equal((await request('/hooks/not-real', {
  method: 'POST', headers: { 'content-type': 'application/json' }, body: '7',
})).status, 404);
console.log('public access, validation, and not-found handling: ok');

const id = await start('demo', 21);
await waitFor(id, (run) => run.status === 'completed');
assert.deepEqual(await json(`/runs/${id}/result`), { value: 42 });
const all = await (await request(`/runs/${id}/events`, { stream: true })).text();
for (const type of ['run_created', 'step_started', 'step_completed', 'run_completed']) {
  assert.match(all, new RegExp(`event: ${type}`));
}
const first = all.match(/id: (evnt_\d+)/)?.[1];
assert.ok(first);
const filtered = await (await request(`/runs/${id}/events?cursor=${first}&types=run_completed`, { stream: true })).text();
assert.match(filtered, /event: run_completed/);
assert.doesNotMatch(filtered, /event: run_created/);
const resumedByHeader = await (await request(`/runs/${id}/events?types=run_completed`, {
  stream: true, headers: { 'last-event-id': first },
})).text();
assert.match(resumedByHeader, /event: run_completed/);
console.log('step, result, subscription history/cursor/filter: ok');

const parallel = await Promise.all([2, 3, 5, 7].map((value) => start('demo', value)));
await Promise.all(parallel.map((runId) => waitFor(runId, (run) => run.status === 'completed')));
assert.deepEqual(await Promise.all(parallel.map(async (runId) => (await json(`/runs/${runId}/result`)).value)), [4, 6, 10, 14]);
console.log('four concurrent runs: ok');

const retry = await start('retry', 11);
await waitFor(retry, (run) => run.status === 'completed');
assert.deepEqual(await json(`/runs/${retry}/result`), { value: 22 });
assert.match(await (await request(`/runs/${retry}/events`, { stream: true })).text(), /event: step_retrying/);
const fatal = await start('fatal', 11);
await waitFor(fatal, (run) => run.status === 'failed');
const fatalEvents = await (await request(`/runs/${fatal}/events`, { stream: true })).text();
assert.match(fatalEvents, /event: step_failed/);
assert.match(fatalEvents, /event: run_failed/);
assert.equal(await (await request(`/runs/${fatal}/events?types=step_completed`, { stream: true })).text(), '');
console.log('transient retry and terminal failure: ok');

const delayed = await start('delayed', 9);
const delayedEvents = (await request(`/runs/${delayed}/events`, { stream: true })).text();
const delayLog = await delayedEvents;
assert.match(delayLog, /event: wait_created/);
assert.match(delayLog, /event: wait_completed/);
assert.match(delayLog, /event: run_completed/);
assert.deepEqual(await json(`/runs/${delayed}/result`), { value: 18 });
console.log('durable sleep and live subscription: ok');

const approval = await start('approval', 11);
const approvalEvents = (await request(`/runs/${approval}/events`, { stream: true })).text();
let hook;
const hookDeadline = Date.now() + 30_000;
do {
  hook = (await json(`/runs/${approval}/hooks`)).data[0];
  if (hook) break;
  await sleep(250);
} while (Date.now() < hookDeadline);
assert.ok(hook?.token, 'hook was not registered');
await json(`/hooks/${encodeURIComponent(hook.token)}`, {
  method: 'POST', headers: { 'content-type': 'application/json' }, body: '7',
});
assert.match(await approvalEvents, /event: hook_received/);
await waitFor(approval, (run) => run.status === 'completed');
assert.deepEqual(await json(`/runs/${approval}/result`), { value: 18 });
console.log('hook registration and external resume: ok');

const progress = await start('progress', 13);
const stream = await request(`/runs/${progress}/streams/progress`, { stream: true });
assert.equal(stream.status, 200);
assert.equal(new TextDecoder().decode(await stream.arrayBuffer()), 'value=13');
await waitFor(progress, (run) => run.status === 'completed');
console.log('named stream, including early reader: ok');

console.log('remote E2E passed');
