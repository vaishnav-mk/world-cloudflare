import { env, runInDurableObject } from 'cloudflare:test';
import { stringify } from 'devalue';
import { expect, it, vi } from 'vitest';
import { DeliveryResult } from '../packages/world-cloudflare/src/enums.js';
import type { Delivery, DeliveryOutcome, EnqueueRequest, Envelope } from '../packages/world-cloudflare/src/types.js';
import { Deliverer, type RunnerStubs } from '../packages/world-cloudflare/src/queue/deliver.js';
import { DeliveryCounts } from '../packages/world-cloudflare/src/queue/delivery-counts.js';
import { Holds } from '../packages/world-cloudflare/src/queue/holds.js';
import { InflightTable } from '../packages/world-cloudflare/src/queue/inflight.js';

type Message = Parameters<Deliverer['deliver']>[0][number];

function message(id: string, body: Envelope) {
  // SAFETY: Deliverer uses only these fanout fields and settlement methods.
  return { id, body, enqueuedAt: Date.now(), retry: vi.fn(async (_delay?: number) => {}),
    ack: vi.fn(async () => {}), deadLetter: vi.fn(async () => {}) } as unknown as Message;
}

function deliverer(sql: SqlStorage, outcome: (delivery: Delivery) => DeliveryOutcome | Promise<DeliveryOutcome>, enqueue: (request: EnqueueRequest) => Promise<unknown>) {
  // SAFETY: Deliverer calls only replay/consume on these runner stubs.
  const runners: RunnerStubs = { get: () => ({
    replay: async (_run: string, delivery: Delivery) => outcome(delivery),
    consume: async (deliveries: Delivery[]) => Promise.all(deliveries.map(outcome)),
  }) as unknown as ReturnType<RunnerStubs['get']>, forget: () => {} };
  return new Deliverer({ QUEUE_MAX_ATTEMPTS: '2' }, runners, 'steps', new DeliveryCounts(sql), new Holds(sql), new InflightTable(sql), enqueue);
}

it.each([0, 0.125])('continues successful spec 8 wakes with delay %s as fresh messages beyond the attempt cap', async (seconds) => {
  await runInDurableObject(env.QUEUE_SHARD.getByName(`continue-${crypto.randomUUID()}`), async (_shard, state) => {
    const delivered: Delivery[] = [];
    const enqueued: EnqueueRequest[] = [];
    const encoded = stringify({ runId: 'spec8', runSpecVersion: 8, requestedAt: new Date(42) });
    const worker = deliverer(state.storage.sql, (delivery) => {
      delivered.push(delivery);
      return { kind: DeliveryResult.Wake, seconds };
    }, async (request) => { enqueued.push(request); });
    let current = message('transport-copy', { queueName: 'workflow-test', encoded, targetDeployment: 'pinned', delayMs: 0, messageId: 'logical-original' });
    for (let index = 0; index < 5; index++) {
      await worker.deliver([current]);
      expect(current.retry).not.toHaveBeenCalled();
      expect(current.ack).not.toHaveBeenCalled();
      expect(current.deadLetter).not.toHaveBeenCalled();
      const next = enqueued[index];
      expect(next).toEqual({ id: expect.any(String), queueName: 'workflow-test', encoded, targetDeployment: 'pinned', delayMs: seconds * 1000 });
      expect(next.id).not.toBe(current.id);
      // The fanout waits the requested delay before this next delivery.
      const { id, delayMs: _delay, ...body } = next;
      current = message(id, { ...body, delayMs: 0 });
    }
    expect(new Set(enqueued.map(({ id }) => id)).size).toBe(5);
    expect(delivered.map(({ attempt, deliveryCount }) => [attempt, deliveryCount])).toEqual(Array(5).fill([1, 1]));
  });
});

it.each(['positive wake', 'failure', 'spec8 failure', 'handoff'] as const)('keeps IDs and counts for %s', async (kind) => {
  await runInDurableObject(env.QUEUE_SHARD.getByName(`spec9-counts-${crypto.randomUUID()}`), async (_shard, state) => {
    const delivered: Delivery[] = [];
    const enqueue = vi.fn(async (_request: EnqueueRequest) => {});
    const worker = deliverer(state.storage.sql, (delivery) => {
      delivered.push(delivery);
      return kind.includes('failure') ? { kind: DeliveryResult.Failed, error: 'failed' } : { kind: DeliveryResult.Wake, seconds: kind === 'handoff' ? 0 : 0.25 };
    }, enqueue);
    const current = message('stable', { queueName: 'workflow-test', encoded: stringify({ runId: 'counts', runSpecVersion: kind === 'spec8 failure' ? 8 : 9 }), delayMs: 0 });
    for (let index = 0; index < 5; index++) await worker.deliver([current]);
    expect(enqueue).not.toHaveBeenCalled();
    expect(delivered.map(({ id }) => id)).toEqual(Array(kind === 'handoff' ? 5 : 2).fill('stable'));
    expect(delivered.map(({ attempt, deliveryCount }) => [attempt, deliveryCount]))
      .toEqual(kind === 'handoff' ? [[1, 1], [1, 2], [1, 3], [1, 4], [1, 5]] : [[1, 1], [2, 2]]);
    expect(current.retry).toHaveBeenNthCalledWith(1, kind.includes('failure') ? 1000 : kind === 'handoff' ? 0 : 250);
    expect(current.retry).toHaveBeenNthCalledWith(2, kind.includes('failure') ? 4000 : kind === 'handoff' ? 0 : 250);
    expect(current.deadLetter).toHaveBeenCalledTimes(kind === 'handoff' ? 0 : 3);
  });
});

it('keeps a failed spec 8 continuation enqueue retryable and recoverable', async () => {
  await runInDurableObject(env.QUEUE_SHARD.getByName(`enqueue-fails-${crypto.randomUUID()}`), async (_shard, state) => {
    const enqueue = vi.fn(async (_request: EnqueueRequest): Promise<void> => { throw new Error('enqueue unavailable'); });
    const worker = deliverer(state.storage.sql, () => ({ kind: DeliveryResult.Wake, seconds: 0 }), enqueue);
    const current = message('original', { queueName: 'workflow-test', encoded: stringify({ runId: 'spec8', runSpecVersion: 8 }), delayMs: 0 });
    await expect(worker.deliver([current])).rejects.toThrow('enqueue unavailable');
    expect(current.ack).not.toHaveBeenCalled();
    expect(current.retry).not.toHaveBeenCalled();
    expect(current.deadLetter).not.toHaveBeenCalled();
    expect(new InflightTable(state.storage.sql).all().map(({ id }) => id)).toEqual(['original']);
    enqueue.mockImplementationOnce(async () => {});
    await worker.deliver([current]);
    expect(new InflightTable(state.storage.sql).all()).toEqual([]);
  });
});

it.each([8, 9])('recovers an original spec %s delivery after a reset during settlement', async (runSpecVersion) => {
  const shard = `settlement-reset-${crypto.randomUUID()}`;
  const id = crypto.randomUUID();
  await runInDurableObject(env.QUEUE_SHARD.getByName(shard), async (_shard, state) => {
    const reset = async () => {
      await state.storage.sync();
      state.abort('reset during settlement');
    };
    const worker = deliverer(state.storage.sql, () => ({ kind: DeliveryResult.Wake, seconds: 1 }), reset);
    const current = message(id, { queueName: '__workflow_workflow_test', encoded: stringify({ runId: id, runSpecVersion }), delayMs: 0 });
    current.retry = reset;
    await worker.deliver([current]);
  }).catch(() => {});
  await env.QUEUE_SHARD.getByName(shard).wake();
  await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 1500 }).toContain(id);
  expect(await runInDurableObject(env.QUEUE_SHARD.getByName(shard), (_shard, state) => new InflightTable(state.storage.sql).all())).toEqual([]);
});
