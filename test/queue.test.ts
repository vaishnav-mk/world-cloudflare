import { createExecutionContext, env, runDurableObjectAlarm, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { getQueueTopicPrefix, type ValidQueueName } from '@workflow/world';
import { createQueueConsumer } from '../packages/world-cloudflare/src/queue/consumer.js';
import { queueShardFor } from '../packages/world-cloudflare/src/lib/shards.js';
import { createQueue } from '../packages/world-cloudflare/src/queue/queue.js';
import { executorFor } from '../packages/world-cloudflare/src/queue/routes.js';

const queue = createQueue(env);
const queueName = `${getQueueTopicPrefix('workflow')}demo` as ValidQueueName;

describe('durability fanout', () => {
  it('delivers once to the consumer entrypoint', async () => {
    const id = (await queue.queue(queueName, { runId: 'run-1' })).messageId;
    await runDurableObjectAlarm(env.QUEUE_SHARD.getByName(queueShardFor('run-1', { runId: 'run-1' })));
    await expect.poll(() => env.SINK.getByName('sink').received()).toContain(id);
  });

  it('queues a batch with one result per message, in order, failing only the bad entry', async () => {
    const key = crypto.randomUUID();
    const results = await queue.queueBatch!(queueName, [
      { message: { runId: 'batch-1' }, opts: { idempotencyKey: `${key}-1` } },
      { message: { runId: 'batch-2' }, opts: { deploymentId: 'nowhere' } },
      { message: { runId: 'batch-3' }, opts: { idempotencyKey: `${key}-3` } },
    ]);
    expect(results.map((result) => result.error === undefined)).toEqual([true, false, true]);
    expect(results[1]).toMatchObject({ messageId: null, retryable: false, error: 'Deployment nowhere is unavailable' });
    for (const { messageId } of [results[0], results[2]]) {
      await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toContain(messageId);
    }
  });

  it('runs a batch of deliveries together and returns each its own outcome', async () => {
    let running = 0;
    let peak = 0;
    const Consumer = createQueueConsumer(async (request) => {
      peak = Math.max(peak, ++running);
      await new Promise((resolve) => setTimeout(resolve, 20));
      running--;
      switch (request.headers.get('x-vqs-message-id')) {
        case 'fails': return new Response('boom', { status: 500 });
        case 'sleeps': return Response.json({ timeoutSeconds: 3 });
        default: return Response.json({});
      }
    });
    const consumer = new Consumer(createExecutionContext(), env);
    const delivery = (id: string) => ({ id, attempt: 1, enqueuedAt: Date.now(), body: { queueName, encoded: '[]' } });
    expect(await consumer.consume([delivery('done'), delivery('fails'), delivery('sleeps')])).toEqual([
      { kind: 'done' }, { kind: 'failed', error: 'Flow handler returned 500: boom' }, { kind: 'wake', seconds: 3 },
    ]);
    expect(peak).toBe(3);
  });

  it('keeps a slow run from holding up a run on another shard', async () => {
    const sink = env.SINK.getByName('sink');
    const slowRun = `shard-slow-${crypto.randomUUID()}`;
    const fastRun = Array.from({ length: 64 }, (_, index) => `shard-fast-${index}`)
      .find((id) => queueShardFor(id, { runId: id }) !== queueShardFor(slowRun, { runId: slowRun }));
    const slowQueue = `${getQueueTopicPrefix('workflow')}lane_slow` as ValidQueueName;
    const slow = (await queue.queue(slowQueue, { runId: slowRun })).messageId;
    await new Promise((resolve) => setTimeout(resolve, 200));
    const fast = (await queue.queue(queueName, { runId: fastRun ?? 'unreachable' })).messageId;
    await expect.poll(() => sink.received(), { timeout: 1_000 }).toContain(fast);
    expect(await sink.received()).not.toContain(slow);
    await expect.poll(() => sink.received(), { timeout: 5_000 }).toContain(slow);
  });

  it('settles its in-flight record once the consumer returns', async () => {
    const id = (await queue.queue(queueName, { runId: 'settled' })).messageId;
    await expect.poll(() => env.SINK.getByName('sink').received()).toContain(id);
    await expect.poll(() => runInDurableObject(env.QUEUE_SHARD.getByName(queueShardFor('settled', { runId: 'settled' })), (_shard, state) =>
      state.storage.sql.exec('SELECT id FROM inflight WHERE id = ?', id).toArray())).toEqual([]);
  });

  it('redelivers a delivery that a reset cut off', async () => {
    const before = (await env.SINK.getByName('sink').received()).length;
    await runInDurableObject(env.QUEUE_SHARD.getByName('orphans'), (_shard, state) => {
      state.storage.sql.exec(
        'INSERT INTO inflight (id, queue_name, encoded) VALUES (?, ?, ?)', 'cut-off', queueName, '[{"runId":1},"orphan"]',
      );
    });
    // Resets the object, so the next call starts a new instance over the same storage, as after a deploy.
    await runInDurableObject(env.QUEUE_SHARD.getByName('orphans'), (_shard, state) => state.abort('reset')).catch(() => {});
    await env.QUEUE_SHARD.getByName('orphans').wake();
    await expect.poll(async () => (await env.SINK.getByName('sink').received()).length, { timeout: 5_000 }).toBe(before + 1);
    expect(await runInDurableObject(env.QUEUE_SHARD.getByName('orphans'), (_shard, state) =>
      state.storage.sql.exec('SELECT id FROM inflight').toArray())).toEqual([]);
  });

  it('deduplicates a repeated enqueue key while delivery is pending', async () => {
    const idempotencyKey = crypto.randomUUID();
    const opts = { idempotencyKey, delaySeconds: 0.4 };
    const [first, second] = await Promise.all([
      queue.queue(queueName, { runId: 'dedup' }, opts),
      queue.queue(queueName, { runId: 'dedup' }, opts),
    ]);
    expect(second.messageId).toBe(first.messageId);
    await expect.poll(async () => (await env.SINK.getByName('sink').received()).filter((id) => id === first.messageId),
      { timeout: 5_000 }).toHaveLength(1);
    const afterDelivery = await queue.queue(queueName, { runId: 'dedup' }, opts);
    expect(afterDelivery.messageId).toBe(first.messageId);
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect((await env.SINK.getByName('sink').received()).filter((id) => id === first.messageId)).toHaveLength(1);
    // Vercel accepts a repeated key even when its retry payload differs; only the first is delivered.
    expect((await queue.queue(queueName, { runId: 'dedup', requestedAt: new Date() }, opts)).messageId).toBe(first.messageId);
    expect((await env.SINK.getByName('sink').received()).filter((id) => id === first.messageId)).toHaveLength(1);
  });

  it('redelivers after the consumer requests a retry', async () => {
    const retryName = `${getQueueTopicPrefix('workflow')}retry` as ValidQueueName;
    const id = (await queue.queue(retryName, { runId: 'run-retry' })).messageId;
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 6_000 }).toContain(id);
  });

  it('accepts a retained executor but refuses unregistered and prototype names', async () => {
    const routes = '{"old-build":"executor-old"}';
    expect(executorFor({ EXECUTOR_ROUTES: routes }, 'old-build')).toBe('executor-old');
    expect(executorFor({ EXECUTOR_ROUTES: routes }, '__proto__')).toBeUndefined();
    const routed = createQueue({ ...env, EXECUTOR_ROUTES: routes });
    const id = (await routed.queue(queueName, { runId: 'old-run' }, { deploymentId: 'old-build' })).messageId;
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5000 }).toContain(id);
    await expect(routed.queue(queueName, { runId: 'bad-run' }, { deploymentId: 'unknown' })).rejects.toThrow('unavailable');
  });

  it("delivers a run's replays one at a time, and settles pure wakes the replay before them covers", async () => {
    const runId = `turns-${crypto.randomUUID()}`;
    const slow = `${getQueueTopicPrefix('workflow')}turns_slow` as ValidQueueName;
    // One push, so one batch: a wake that carries a retry count must be delivered; the two pure wakes need not.
    const [named, ...pure] = await queue.queueBatch!(slow, [
      { message: { runId, serverErrorRetryCount: 1 } }, { message: { runId } }, { message: { runId, requestedAt: new Date() } },
    ]);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toContain(named.messageId);
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    const received = await env.SINK.getByName('sink').received();
    for (const { messageId } of pure) expect(received).not.toContain(messageId);
  }, 15_000);

  it('folds any number of wakes queued during a replay into one more replay', async () => {
    const sink = env.SINK.getByName('sink');
    const runId = `fold-${crypto.randomUUID()}`;
    const slow = `${getQueueTopicPrefix('workflow')}fold_slow` as ValidQueueName;
    const first = (await queue.queue(slow, { runId })).messageId;
    const shard = env.QUEUE_SHARD.getByName(queueShardFor(runId, { runId }));
    await expect.poll(() => runInDurableObject(shard, (_shard, state) =>
      state.storage.sql.exec('SELECT id FROM inflight WHERE id = ?', first).toArray().length)).toBe(1);
    // More than one batch of wakes, all queued while the first replay runs.
    const wakes = await Promise.all(Array.from({ length: 25 }, () => queue.queue(slow, { runId, requestedAt: new Date() })));
    const ids = new Set([first, ...wakes.map(({ messageId }) => messageId)]);
    const delivered = async () => (await sink.received()).filter((id) => ids.has(id as typeof first)).length;
    await expect.poll(delivered, { timeout: 10_000 }).toBe(2);
    // Long enough for a third slow delivery to land, had there been one.
    await new Promise((resolve) => setTimeout(resolve, 3_000));
    expect(await delivered()).toBe(2);
  }, 25_000);

  it('redelivers a failed replay instead of settling the wakes it would have covered', async () => {
    const runId = `flaky-${crypto.randomUUID()}`;
    const flaky = `${getQueueTopicPrefix('workflow')}replay_flaky` as ValidQueueName;
    const [failed, other] = await queue.queueBatch!(flaky, [{ message: { runId } }, { message: { runId, requestedAt: new Date() } }]);
    // The first replay fails, so the second wake is delivered too; the failed one is redelivered.
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 8_000 })
      .toEqual(expect.arrayContaining([`${failed.messageId}#1`, `${other.messageId}#1`, `${failed.messageId}#2`]));
  }, 15_000);

  it('retains delayed delivery and rejects another deployment', async () => {
    await expect(queue.queue(queueName, { runId: 'run-2' }, { deploymentId: 'old-v0' })).rejects.toThrow('unavailable');
    const id = (await queue.queue(queueName, { runId: 'run-2' }, { delaySeconds: 1 })).messageId;
    await runDurableObjectAlarm(env.QUEUE_SHARD.getByName(queueShardFor('run-2', { runId: 'run-2' })));
    expect(await env.SINK.getByName('sink').received()).not.toContain(id);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 4000 }).toContain(id);
  });
});
