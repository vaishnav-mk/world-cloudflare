import { describe, expect, it } from 'vitest';
import { DEFAULT_SHARD_EPOCHS } from '../packages/world-cloudflare/src/constants.js';
import {
  hookIndexShards, indexShardFor, indexShardNames, indexShardsOf, indexSourceCount, parseEpochs, queueShardFor, queueShardNames,
  queueShardsToWake, runnerShardFor, stepRunnerFor, storeShardFor, storeShardNames, tokenShardFor,
} from '../packages/world-cloudflare/src/lib/shards.js';

// wrangler.test.jsonc sets two epochs: { from: 0, store: 4, queue: 4, index: 2 } and
// { from: 1700000000000, store: 8, queue: 8, index: 3 }.
const SECOND = (1_700_000_000_000).toString(36);

/** A run ID whose ULID time is `ms`: 10 Crockford base32 time characters, then a fixed random part. */
function runIdAt(ms: number): string {
  const alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  let time = '';
  for (let rest = ms, index = 0; index < 10; index++, rest = Math.floor(rest / 32)) time = alphabet[rest % 32] + time;
  return `wrun_${time}${'0'.repeat(16)}`;
}

describe('shard epochs', () => {
  it('routes a run by the epoch its ULID time falls in', () => {
    const old = runIdAt(1_600_000_000_000);
    const current = runIdAt(Date.now());
    expect(storeShardFor(old)).toMatch(/^store-[0-3]$/);
    expect(storeShardFor(current)).toMatch(new RegExp(`^store-e${SECOND}-[0-7]$`));
    expect(queueShardFor(old, { runId: old })).toMatch(/^flow-[0-3]$/);
    expect(queueShardFor(current, { runId: current })).toMatch(new RegExp(`^flow-e${SECOND}-[0-7]$`));
    expect(storeShardFor('wrun_not-a-ulid')).toMatch(/^store-[0-3]$/);
    expect(storeShardFor(current)).toBe(storeShardFor(current));
  });

  it("routes a run's workflow messages by the run and its other messages by key", () => {
    const runId = runIdAt(Date.now());
    const run = queueShardFor(runId, { runId });
    const workflow = [
      { runId, runInput: { input: new Uint8Array(1), deploymentId: 'd', workflowName: 'w', specVersion: 8 } },
      { runId, runSpecVersion: 8, hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 1 } },
      { runId, runSpecVersion: 9, waitContinuation: { correlationId: 'wait_1', attempt: 0 } },
      { runId, runSpecVersion: 9, completeWaits: ['wait_1'] },
    ];
    for (const [index, payload] of workflow.entries()) expect(queueShardFor(runId, payload, `q:key-${index}`)).toBe(run);
    // Steps, and a long wait's intermediate timer hops, spread by key; every repeat of a key reaches one shard.
    const others = Array.from({ length: 32 }, (_, index) => index % 2
      ? { payload: { runId, stepId: `step_${index}`, stepName: 'double' }, key: `q:step_${index}:a0eb0f08` }
      : { payload: { runId, waitContinuation: { correlationId: `wait_${index}`, attempt: 0, resumeAtMs: 9, hopAtMs: 5 } }, key: `q:wait:${index}` });
    const shards = others.map(({ payload, key }) => {
      expect(queueShardFor(runId, { ...payload, requestedAt: new Date() }, key)).toBe(queueShardFor(runId, payload, key));
      return queueShardFor(runId, payload, key);
    });
    expect(new Set(shards).size).toBeGreaterThan(1);
  });

  it("gives a run the runner with its queue shard's index, and each queue shard one step runner", () => {
    for (const runId of [runIdAt(1_600_000_000_000), runIdAt(Date.now())]) {
      expect(runnerShardFor(runId)).toBe(queueShardFor(runId, { runId }).replace(/^flow-/, 'runner-'));
    }
    expect(stepRunnerFor(`flow-e${SECOND}-3`)).toBe(`runner-steps-e${SECOND}-3`);
    expect(stepRunnerFor('flow-5')).toBe('runner-steps-5');
    expect(stepRunnerFor('flow')).toBe('runner-steps');
  });

  it('keeps hook tokens on the first epoch', () => {
    expect(tokenShardFor(`token-${crypto.randomUUID()}`)).toMatch(/^store-[0-3]$/);
  });

  it('lists every shard of every epoch', () => {
    expect(storeShardNames()).toHaveLength(12);
    expect(queueShardNames()).toEqual(['flow', 'flow-0', 'flow-1', 'flow-2', 'flow-3', ...Array.from({ length: 8 }, (_, n) => `flow-e${SECOND}-${n}`)]);
  });

  it("puts a run's list row, and its hooks', on an index shard of the run's epoch", () => {
    const old = runIdAt(1_600_000_000_000);
    const current = runIdAt(Date.now());
    expect(indexShardFor(old)).toMatch(/^index-[0-1]$/);
    expect(indexShardFor(current)).toMatch(new RegExp(`^index-e${SECOND}-[0-2]$`));
    // A hook hashes by its own ID within its run's epoch, so hooks.get asks one shard per epoch.
    const hookId = `hook_${crypto.randomUUID()}`;
    expect(hookIndexShards(hookId)).toHaveLength(2);
    expect(hookIndexShards(hookId)).toContain(indexShardFor(old, hookId));
    expect(hookIndexShards(hookId)).toContain(indexShardFor(current, hookId));
    expect(indexShardNames()).toEqual(['index-0', 'index-1', `index-e${SECOND}-0`, `index-e${SECOND}-1`, `index-e${SECOND}-2`]);
  });

  it('has each store report to the index shards it feeds, and each index shard count its stores', () => {
    expect(indexShardsOf('store-3')).toEqual(['index-0', 'index-1']);
    expect(indexShardsOf(`store-e${SECOND}-7`)).toEqual([`index-e${SECOND}-0`, `index-e${SECOND}-1`, `index-e${SECOND}-2`]);
    // The legacy store's runs predate every epoch added later.
    expect(indexShardsOf('global')).toEqual(['index-0', 'index-1']);
    // An epoch's store shards, and for the first epoch the legacy store.
    expect(indexSourceCount('index-1')).toBe(5);
    expect(indexSourceCount(`index-e${SECOND}-2`)).toBe(8);
    expect(() => indexShardsOf('store-x')).toThrow();
  });

  it('wakes every queue shard once per cycle of cron ticks', () => {
    // 13 shards fit in one tick, so each tick wakes all of them.
    expect(queueShardsToWake(0)).toEqual(queueShardNames());
    expect(queueShardsToWake(7)).toEqual(queueShardNames());
    // 5,377 shards take 11 ticks of at most 512 each, and each shard is woken in exactly one.
    const many = Array.from({ length: 5377 }, (_, index) => `flow-${index}`);
    const ticks = Array.from({ length: 11 }, (_, tick) => queueShardsToWake(tick, many));
    expect(Math.max(...ticks.map((woken) => woken.length))).toBeLessThanOrEqual(512);
    expect(ticks.flat().sort()).toEqual([...many].sort());
    expect(queueShardsToWake(11, many)).toEqual(ticks[0]);
  });

  it('parses and validates SHARD_EPOCHS', () => {
    expect(parseEpochs(undefined)).toBe(DEFAULT_SHARD_EPOCHS);
    expect(parseEpochs('[{"from":0,"store":2,"queue":3}]')).toEqual([{ from: 0, store: 2, queue: 3 }]);
    expect(parseEpochs('[{"from":0,"store":2,"queue":3,"index":4}]')).toEqual([{ from: 0, store: 2, queue: 3, index: 4 }]);
    for (const bad of ['[]', '[{"from":5,"store":1,"queue":1}]', '[{"from":0,"store":0,"queue":1}]', '[{"from":0,"store":1,"queue":1,"index":0}]',
      '[{"from":0,"store":1,"queue":1},{"from":0,"store":2,"queue":2}]']) {
      expect(() => parseEpochs(bad), bad).toThrow();
    }
  });
});
