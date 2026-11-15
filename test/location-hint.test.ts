import { env } from 'cloudflare:test';
import { afterAll, describe, expect, it } from 'vitest';
import { getQueueTopicPrefix, type ValidQueueName } from '@workflow/world';
import { createWorld, drainOutbox } from 'world-cloudflare';
import { parseLocationHint, runnerShardFor } from '../packages/world-cloudflare/src/lib/shards.js';

// wrangler.test.jsonc sets LOCATION_HINT to weur. The Durable Objects run in this isolate, so
// patching the namespaces' shared prototype sees their calls too.
const calls: { name: string; options: unknown }[] = [];
const prototype = Object.getPrototypeOf(env.WORLD_STORE) as { getByName: (name: string, options?: unknown) => unknown };
const { getByName } = prototype;
prototype.getByName = function (this: unknown, name: string, options?: unknown) {
  calls.push({ name, options });
  return getByName.call(this, name, options);
};
afterAll(() => {
  prototype.getByName = getByName;
});

const world = createWorld(env);
const bytes = (text: string) => new TextEncoder().encode(text);

describe('LOCATION_HINT', () => {
  it('parses a known hint, and nothing when unset', () => {
    expect(parseLocationHint(undefined)).toEqual({});
    expect(parseLocationHint('')).toEqual({});
    expect(parseLocationHint('apac-ne')).toEqual({ locationHint: 'apac-ne' });
    expect(() => parseLocationHint('mars')).toThrow(/LOCATION_HINT must be one of/);
  });

  it('reaches every store, index, token, queue, runner and streams object the World gets', async () => {
    const { run: { runId } } = await world.events.create(null, {
      eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName: 'hinted', input: bytes('input') },
    });
    await world.events.create(runId, { eventType: 'run_started' });
    // A hook claims its token on the token's store shard, from the run's store.
    await world.events.create(runId, { eventType: 'hook_created', correlationId: 'hook', eventData: { token: crypto.randomUUID() } });
    await world.runs.list({ workflowName: 'hinted' });
    await world.streams.write(runId, 'stdout', 'a');
    await world.experimental_snapshots!.save(runId, bytes('vm'), {
      eventsCursor: 'eid:3', createdAt: new Date(), eventCount: 3, rngDraws: 0, serdeRootPtr: 0,
      lastUlid: '01J0000000000000000000000', engineVersion: '1', clockMs: 0, formatVersion: 3,
    });
    // A workflow message is pushed to its queue shard, which hands it to the run's runner.
    const queueName = `${getQueueTopicPrefix('workflow')}hinted` as ValidQueueName;
    const { messageId } = await world.queue(queueName, { runId });
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toContain(messageId);
    // An invocation goes to the run's runner, which finds no such run.
    await expect(world.invoke!(`wrun_${'0'.repeat(26)}`, { type: 'hook_resume' })).rejects.toThrow();
    await drainOutbox(env);

    const ours = calls.filter(({ name }) => name !== 'sink');
    for (const prefix of ['store-', 'index-', 'flow-', 'runner-', 'wrun_', 'global']) {
      expect(ours.some(({ name }) => name.startsWith(prefix)), prefix).toBe(true);
    }
    expect(ours.filter(({ options }) => (options as { locationHint?: string } | undefined)?.locationHint !== 'weur')).toEqual([]);
  });

  it("creates a run's runner from the run's store without the hint, before the run's start reaches it", async () => {
    const { run: { runId } } = await world.events.create(null, {
      eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName: 'placed', input: bytes('input') },
    });
    const from = calls.length;
    const queueName = `${getQueueTopicPrefix('workflow')}placed` as ValidQueueName;
    const { messageId } = await world.queue(queueName, {
      runId, runInput: { input: bytes('input'), deploymentId: 'test-v1', workflowName: 'placed', specVersion: 8 },
    });
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toContain(messageId);

    const runner = calls.slice(from).filter(({ name }) => name === runnerShardFor(runId));
    // The store's call, then the queue shard's hand-off, which finds the runner where it is.
    expect(runner.length).toBeGreaterThan(1);
    expect(runner[0]?.options).toBeUndefined();
  });
});
