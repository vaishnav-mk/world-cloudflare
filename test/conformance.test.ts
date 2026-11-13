import { env } from 'cloudflare:test';
import { createWorld } from '@workflow/world-cloudflare';
// The SDK's own conformance suites, by relative path as their docs ask: the package exports no test-support entry.
import { inBandFenceConformance } from '../node_modules/@workflow/world/dist/test-support/in-band-fence-conformance.js';

const world = createWorld(env);

inBandFenceConformance({
  name: 'world-cloudflare',
  events: () => createWorld(env).events,
  capabilities: () => world.capabilities,
  newRunId: () => `wrun_${crypto.randomUUID()}`,
  atRunCreation: 1,
  // Two World instances over the same Durable Objects share one fence.
  secondEvents: () => createWorld(env).events,
});
