import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';

const base = process.env.WORKFLOW_BASE_URL?.replace(/\/$/, '');
const count = Number(process.env.WORKFLOW_LOAD_COUNT ?? 50);
if (!base || !Number.isSafeInteger(count) || count < 1 || count > 100) {
  throw new Error('Set WORKFLOW_BASE_URL and 1..100 WORKFLOW_LOAD_COUNT');
}
const headers = { 'content-type': 'application/json' };
const startedAt = performance.now();
const ids = [];
for (let offset = 0; offset < count; offset += 10) {
  const batch = await Promise.all(Array.from({ length: Math.min(10, count - offset) }, async (_, index) => {
    const value = offset + index + 1;
    const response = await fetch(`${base}/runs`, {
      method: 'POST', headers, body: JSON.stringify({ value }), signal: AbortSignal.timeout(20_000),
    });
    assert.equal(response.status, 202, `create #${value}: ${response.status}`);
    return { runId: (await response.json()).runId, value };
  }));
  ids.push(...batch);
}
const createdMs = Math.round(performance.now() - startedAt);
const pending = new Set(ids.map((item) => item.runId));
const completedAt = new Map();
const deadline = performance.now() + 90_000;
while (pending.size && performance.now() < deadline) {
  await Promise.all([...pending].map(async (id) => {
    const response = await fetch(`${base}/runs/${id}`, {
      headers, signal: AbortSignal.timeout(20_000),
    });
    assert.equal(response.status, 200);
    const run = await response.json();
    if (run.status === 'completed') { pending.delete(id); completedAt.set(id, performance.now() - startedAt); }
    else if (run.status === 'failed' || run.status === 'cancelled') throw new Error(`${id}: ${run.status}/${run.errorCode ?? 'unknown'}`);
  }));
  if (pending.size) await sleep(350);
}
assert.equal(pending.size, 0, `Still pending after 90s: ${pending.size}`);
const output = await Promise.all(ids.map(async ({ runId, value }) => {
  const response = await fetch(`${base}/runs/${runId}/result`, { headers, signal: AbortSignal.timeout(20_000) });
  assert.equal(response.status, 200);
  assert.equal((await response.json()).value, value * 2);
  return completedAt.get(runId);
}));
output.sort((a, b) => a - b);
console.log(JSON.stringify({
  runs: count,
  createdMs,
  totalMs: Math.round(performance.now() - startedAt),
  p50Ms: Math.round(output[Math.floor(count * 0.5)]),
  p95Ms: Math.round(output[Math.floor(count * 0.95)]),
}));
