import { env, runDurableObjectAlarm } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { getQueueTopicPrefix, type ValidQueueName } from '@workflow/world';
import { createQueue, executorFor } from '../packages/world-cloudflare/src/queue.js';

const queue = createQueue(env);
const queueName = `${getQueueTopicPrefix('workflow')}demo` as ValidQueueName;

describe('durability fanout', () => {
  it('delivers once to the consumer entrypoint', async () => {
    const id = (await queue.queue(queueName, { runId: 'run-1' })).messageId;
    await runDurableObjectAlarm(env.QUEUE_SHARD.getByName('flow'));
    await expect.poll(() => env.SINK.getByName('sink').received()).toContain(id);
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
    expect((await queue.queue(queueName, { runId: 'different-work' }, opts)).messageId).toBe(first.messageId);
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

  it('retains delayed delivery and rejects another deployment', async () => {
    await expect(queue.queue(queueName, { runId: 'run-2' }, { deploymentId: 'old-v0' })).rejects.toThrow('unavailable');
    const id = (await queue.queue(queueName, { runId: 'run-2' }, { delaySeconds: 1 })).messageId;
    await runDurableObjectAlarm(env.QUEUE_SHARD.getByName('flow'));
    expect(await env.SINK.getByName('sink').received()).not.toContain(id);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 4000 }).toContain(id);
  });
});
