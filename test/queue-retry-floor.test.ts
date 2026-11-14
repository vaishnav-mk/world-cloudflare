import { createExecutionContext, env, runInDurableObject } from 'cloudflare:test';
import { WorkflowWorldError } from '@workflow/errors';
import { getQueueTopicPrefix } from '@workflow/world';
import { stringify } from 'devalue';
import { expect, it, vi } from 'vitest';
import { MAX_WAKE_SECONDS } from '../packages/world-cloudflare/src/constants.js';
import { DeliveryResult } from '../packages/world-cloudflare/src/enums.js';
import { createQueueConsumer } from '../packages/world-cloudflare/src/queue/consumer.js';
import { createQueueHandler } from '../packages/world-cloudflare/src/queue/handler.js';
import { Deliverer, type RunnerStubs } from '../packages/world-cloudflare/src/queue/deliver.js';
import { DeliveryCounts } from '../packages/world-cloudflare/src/queue/delivery-counts.js';
import { Holds } from '../packages/world-cloudflare/src/queue/holds.js';
import { InflightTable } from '../packages/world-cloudflare/src/queue/inflight.js';
import type { Delivery } from '../packages/world-cloudflare/src/types.js';

const prefix = getQueueTopicPrefix('workflow');
const delivery: Delivery = {
  id: 'msg_retry_floor', attempt: 1, enqueuedAt: 42,
  body: { queueName: `${prefix}retry_floor`, encoded: stringify({ runId: 'retry-floor', runSpecVersion: 8 }), delayMs: 0 },
};

it.each([{ retryAfter: 120 }, new WorkflowWorldError('throttled', { status: 429, retryAfter: 120 })])('carries a numeric retryAfter seconds floor from handler to consumer %#', async (error) => {
  const handle = createQueueHandler(prefix, async () => { throw error; });
  const Consumer = createQueueConsumer(handle);
  expect(await new Consumer(createExecutionContext(), env).consume([delivery])).toEqual([
    { kind: DeliveryResult.Failed, error: expect.stringContaining('Flow handler returned 500:'), retryAfter: 120 },
  ]);
});

it.each([Number.NaN, Infinity, -Infinity, -1, 0, '120', new Date(120_000), undefined])('ignores an invalid handler retryAfter %s', async (retryAfter) => {
  const Consumer = createQueueConsumer(createQueueHandler(prefix, async () => { throw { retryAfter }; }));
  expect(await new Consumer(createExecutionContext(), env).consume([delivery])).toEqual([
    { kind: DeliveryResult.Failed, error: 'Flow handler returned 500: {"error":"Delivery failed"}' },
  ]);
});

it.each([
  '{"retryAfter":"120"}', '{"retryAfter":-1}', '{"retryAfter":0}', '{"retryAfter":1e400}', '{"retryAfter":-1e400}',
  '{"retryAfter":null}', 'null', '[]', 'boom', '{"error":"old handler"}',
])('ignores invalid or absent retryAfter in the untrusted failure body %s', async (body) => {
  const Consumer = createQueueConsumer(async () => new Response(body, { status: 500 }));
  expect(await new Consumer(createExecutionContext(), env).consume([delivery])).toEqual([
    { kind: DeliveryResult.Failed, error: `Flow handler returned 500: ${body}` },
  ]);
});

it.each([
  { retryAfter: 120, attempts: 1, delay: 120_000 },
  { retryAfter: 120.125, attempts: 1, delay: 120_125 },
  { retryAfter: 600, attempts: 1, delay: 600_000 },
  { retryAfter: 120, attempts: 12, delay: 144_000 },
  { retryAfter: 0.125, attempts: 1, delay: 1_000 },
  { retryAfter: undefined, attempts: 2, delay: 4_000 },
  { retryAfter: Number.MAX_VALUE, attempts: 1, delay: MAX_WAKE_SECONDS * 1_000 },
])('retries failures with floor $retryAfter at attempt $attempts after $delay ms', async ({ retryAfter, attempts, delay }) => {
  await runInDurableObject(env.QUEUE_SHARD.getByName(`retry-floor-${crypto.randomUUID()}`), async (_shard, state) => {
    const Consumer = createQueueConsumer(createQueueHandler(prefix, async () => { throw { retryAfter }; }));
    const consumer = new Consumer(createExecutionContext(), env);
    // SAFETY: Deliverer only calls consume/replay on these runner stubs.
    const runners: RunnerStubs = { get: () => ({
      consume: (deliveries: Delivery[]) => consumer.consume(deliveries),
      replay: async (_run: string, current: Delivery) => (await consumer.consume([current]))[0],
    }) as unknown as ReturnType<RunnerStubs['get']>, forget: () => {} };
    const enqueue = vi.fn(async () => {});
    const worker = new Deliverer({}, runners, 'steps', new DeliveryCounts(state.storage.sql), new Holds(state.storage.sql), new InflightTable(state.storage.sql), enqueue);
    const retry = vi.fn(async () => {});
    const deadLetter = vi.fn(async () => {});
    // SAFETY: Deliverer only reads these fanout fields and settlement methods.
    const message = { id: delivery.id, body: delivery.body, enqueuedAt: Date.now(), retry, deadLetter } as unknown as Parameters<Deliverer['deliver']>[0][number];
    for (let index = 0; index < attempts; index++) await worker.deliver([message]);
    expect(retry).toHaveBeenLastCalledWith(delay);
    expect(deadLetter).not.toHaveBeenCalled();
    expect(enqueue).not.toHaveBeenCalled();
  });
});
