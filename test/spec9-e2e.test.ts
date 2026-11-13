import { SELF } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { SPEC_VERSION_SINGLE_ORCHESTRATOR } from '@workflow/world';
import { world } from '../src/world.js';

it('creates spec 9 runs and drives them to completion with the in-band fence', async () => {
  expect(world.specVersion).toBe(SPEC_VERSION_SINGLE_ORCHESTRATOR);
  const started = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ value: 4 }),
  });
  expect(started.status).toBe(202);
  const { runId } = await started.json() as { runId: string };
  await expect.poll(async () => (await world.runs.get(runId)).status, { interval: 100, timeout: 15_000 }).toBe('completed');
  expect((await world.runs.get(runId)).specVersion).toBe(SPEC_VERSION_SINGLE_ORCHESTRATOR);
  const { data, snapshot } = await world.events.list({ runId });
  // Every event of the run is counted, and the orchestrator's own writes are in-band.
  expect(snapshot?.seq).toBe(data.length);
  expect(snapshot?.seqInBand).toBeGreaterThan(1);
  expect(data.every((event) => event.specVersion === SPEC_VERSION_SINGLE_ORCHESTRATOR)).toBe(true);
});
