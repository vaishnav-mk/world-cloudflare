import { env, runInDurableObject } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { DeliveryCounts } from '../packages/world-cloudflare/src/queue/delivery-counts.js';
import { Holds } from '../packages/world-cloudflare/src/queue/holds.js';

it.each(['missing', 'done', 'expired'])('refuses a %s hold without counting a delivery', async (status) => {
  const shard = env.QUEUE_SHARD.getByName(`begin-held-${status}-${crypto.randomUUID()}`);
  await runInDurableObject(shard, (_shard, state) => {
    const holds = new Holds(state.storage.sql);
    if (status !== 'missing') {
      holds.add({ id: 'held', queueName: 'workflow-test', encoded: '[]', delayMs: 0 }, Date.now() + (status === 'expired' ? -1_000 : 60_000));
      if (status === 'done') holds.settle('held');
    }
  });
  await runInDurableObject(shard, async (object) => {
    await expect(object.beginHeld('held')).rejects.toThrow('lease');
  });
  expect(await runInDurableObject(shard, (_shard, state) => new DeliveryCounts(state.storage.sql).get('held')))
    .toEqual({ deliveries: 0, handoffs: 0 });
});

it.each(['missing', 'done', 'expired', 'live'])('renews a %s hold only while its lease is live', async (status) => {
  const shard = env.QUEUE_SHARD.getByName(`renew-held-${status}-${crypto.randomUUID()}`);
  const leaseUntil = Date.now() + (status === 'expired' ? -1_000 : 60_000);
  await runInDurableObject(shard, (_shard, state) => {
    const holds = new Holds(state.storage.sql);
    if (status !== 'missing') {
      holds.add({ id: 'held', queueName: 'workflow-test', encoded: '[]', delayMs: 0 }, leaseUntil);
      if (status === 'done') holds.settle('held');
    }
  });
  expect(await shard.renewHold('held', 120_000)).toBe(status === 'live');
  const held = await runInDurableObject(shard, (_shard, state) => new Holds(state.storage.sql).find('held'));
  if (status === 'missing') {
    expect(held).toBeUndefined();
  } else if (status === 'live') {
    expect(held?.leaseUntil).toBeGreaterThan(leaseUntil);
    expect(await shard.beginHeld('held')).toEqual({ deliveries: 1, handoffs: 0 });
  } else {
    expect(held?.leaseUntil).toBe(leaseUntil);
  }
});
