import { SELF } from 'cloudflare:test';
import { afterAll, expect, it } from 'vitest';
import { world } from '../src/world.js';

/**
 * Measures how a wide fan-out replays: `parallelSteps(n)`'s wall time, its workflow deliveries (each
 * one replays the run), its full log reads (an `events.list` of the run from the start, which a
 * replay without a VM snapshot makes, and which a step delivery makes to look for the run's last
 * step) and every `events.list` of the run. Run it with `vitest.replays.config.ts`; it prints one
 * line per size.
 */
const counts = new Map<string, { replays: number; fullReads: number; reads: number }>();
function countOf(runId: string) {
  let count = counts.get(runId);
  if (count === undefined) counts.set(runId, count = { replays: 0, fullReads: 0, reads: 0 });
  return count;
}
const { list } = world.events;
const { createQueueHandler } = world;
// Counts without keeping the pages, which a spy would hold on to.
world.events.list = (params) => {
  const count = countOf(params.runId);
  count.reads++;
  if (params.pagination?.cursor === undefined) count.fullReads++;
  return list(params);
};
// The SDK builds its handler with this before its first delivery.
world.createQueueHandler = (prefix, handler) => createQueueHandler(prefix, (message, meta) => {
  const fields = message as { runId?: unknown; stepId?: unknown; invoke?: unknown };
  if (typeof fields.runId === 'string' && fields.stepId === undefined && fields.invoke === undefined) countOf(fields.runId).replays++;
  return handler(message, meta);
});
afterAll(() => {
  Object.assign(world.events, { list });
  Object.assign(world, { createQueueHandler });
});

for (const count of (process.env.REPLAY_COUNTS ?? '200,1000').split(',').map(Number)) {
  it(`replays parallelSteps(${count})`, async () => {
    const started = Date.now();
    const response = await SELF.fetch('https://local/runs', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name: 'parallelSteps', value: count }),
    });
    const { runId } = await response.json() as { runId: string };
    await expect.poll(async () => (await world.runs.get(runId, { resolveData: 'none' })).status,
      { interval: 250, timeout: 600_000 }).toBe('completed');
    const ms = Date.now() - started;
    console.log(`REPLAYS ${JSON.stringify({ engine: process.env.WORKFLOW_VM, count, ms, ...countOf(runId) })}`);
  }, 660_000);
}
