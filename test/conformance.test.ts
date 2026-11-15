import { env } from 'cloudflare:test';
import { vi } from 'vitest';
import { createWorld } from 'world-cloudflare';
// The SDK's own conformance suites, by relative path as their docs ask: the package exports no test-support entry.
import { atomicStartHookConformance } from '../node_modules/@workflow/world/dist/test-support/atomic-start-hook-conformance.js';
import { inBandFenceConformance } from '../node_modules/@workflow/world/dist/test-support/in-band-fence-conformance.js';
import { newRunId } from './ids.js';

const world = createWorld(env);

inBandFenceConformance({
  name: 'world-cloudflare',
  events: () => createWorld(env).events,
  capabilities: () => world.capabilities,
  newRunId,
  atRunCreation: 1,
  // Two World instances over the same Durable Objects share one fence.
  secondEvents: () => createWorld(env).events,
});

// Without `advanceClock` the orphan-bound cases skip: the World reads the Durable Objects' clock.
// `test/start-hook.test.ts` crosses the bound with a candidate minted in the past instead. The
// retention cases wait out their 10 s retention in real time.
vi.setConfig({ testTimeout: 30_000 });
atomicStartHookConformance({
  name: 'world-cloudflare',
  storage: () => createWorld(env),
  capabilities: () => world.capabilities,
  newRunId,
  secondEvents: () => createWorld(env).events,
});
