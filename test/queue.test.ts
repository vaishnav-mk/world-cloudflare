import { createExecutionContext, env, runDurableObjectAlarm, runInDurableObject } from 'cloudflare:test';
import { stringify } from 'devalue';
import { describe, expect, it } from 'vitest';
import { getQueueTopicPrefix, type ValidQueueName } from '@workflow/world';
import type { WorldEnv } from 'world-cloudflare';
import type { EnqueueRequest } from '../packages/world-cloudflare/src/types.js';
import { PAYLOAD_MAX_BYTES, TURN_BATCH_MAX, TURN_CALLS_IN_FLIGHT } from '../packages/world-cloudflare/src/constants.js';
import { withRequestContext } from '../packages/world-cloudflare/src/lib/request-context.js';
import { createQueueConsumer } from '../packages/world-cloudflare/src/queue/consumer.js';
import { queueShardFor, runnerShardFor } from '../packages/world-cloudflare/src/lib/shards.js';
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

  it("gathers a flow request's publishes in one turn into a few calls to the run's store, a couple at once", async () => {
    const calls: number[] = [];
    let active = 0;
    let peak = 0;
    const stores = { getByName: () => ({
      dispatch: async (entries: { request: { id: string } }[]) => {
        calls.push(entries.length);
        peak = Math.max(peak, ++active);
        await new Promise((resolve) => setTimeout(resolve, 5));
        active--;
        return entries.map(({ request }) => ({ id: request.id, fresh: true }));
      },
    }) } as unknown as WorldEnv['WORLD_STORE'];
    const gathering = createQueue({ ...env, WORLD_STORE: stores });
    const runId = `wrun_${'0'.repeat(26)}`;
    const step = (stepId: string) => gathering.queue(queueName, { runId, stepId, stepName: 'work' }, { idempotencyKey: stepId });
    const count = 3 * TURN_BATCH_MAX + 1;
    const published = await withRequestContext(createExecutionContext(), {}, () =>
      Promise.all(Array.from({ length: count }, (_, index) => step(`step-${index}`))));
    expect(new Set(published.map(({ messageId }) => messageId)).size).toBe(count);
    expect(calls).toEqual([TURN_BATCH_MAX, TURN_BATCH_MAX, TURN_BATCH_MAX, 1]);
    expect(peak).toBe(TURN_CALLS_IN_FLIGHT);
    // Outside a flow request, each publish is its own call.
    await Promise.all([step('alone-1'), step('alone-2')]);
    expect(calls).toEqual([TURN_BATCH_MAX, TURN_BATCH_MAX, TURN_BATCH_MAX, 1, 1, 1]);
  });

  it("answers a flow request's publish of a dispatched step itself, reading the run's dispatched steps once", async () => {
    const calls = { reads: 0, dispatched: [] as string[] };
    const stores = { getByName: () => ({
      dispatchedSteps: async () => {
        calls.reads++;
        return ['done'];
      },
      dispatch: async (entries: { request: { id: string; encoded: string } }[]) => entries.map(({ request }) => {
        calls.dispatched.push(request.id);
        return { id: request.id, fresh: true };
      }),
    }) } as unknown as WorldEnv['WORLD_STORE'];
    const answering = createQueue({ ...env, WORLD_STORE: stores });
    const runId = `wrun_${'0'.repeat(26)}`;
    // The SDK's dispatch key: the step's ID and the FNV-1a hash of its name.
    const step = (stepId: string, idempotencyKey = `${stepId}:a0eb0f08`) =>
      answering.queue(queueName, { runId, stepId, stepName: 'double' }, { idempotencyKey });
    const id = async (key: string) => `qmsg_${Buffer.from(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(`${queueName}:${key}`))).toString('hex')}`;
    await withRequestContext(createExecutionContext(), {}, async () => {
      expect((await step('done')).messageId).toBe(await id('done:a0eb0f08'));
      expect(calls.dispatched).toEqual([]);
      await step('pending');
      await step('done', 'done:retry-1');
    });
    expect(calls.reads).toBe(1);
    expect(calls.dispatched).toEqual([await id('pending:a0eb0f08'), await id('done:retry-1')]);
  });

  it('warms its flow route ahead of a delivery, unless deliveries run on executors', async () => {
    let warmed = 0;
    const flow = Object.assign(async () => Response.json({}), { warm: async () => { warmed++; } });
    const Consumer = createQueueConsumer(flow);
    await new Consumer(createExecutionContext(), env).warm();
    expect(warmed).toBe(1);
    // SAFETY: warm only checks whether the binding exists.
    await new Consumer(createExecutionContext(), { ...env, EXECUTORS: {} as DispatchNamespace }).warm();
    expect(warmed).toBe(1);
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

  it("delivers a step while its run's long replay holds the same shard", async () => {
    const sink = env.SINK.getByName('sink');
    const runId = `step-beside-${crypto.randomUUID()}`;
    const slowQueue = `${getQueueTopicPrefix('workflow')}lane_slow` as ValidQueueName;
    // A replay that holds its delivery, as a spec 9 orchestrator waiting on its steps does.
    const slow = (await queue.queue(slowQueue, { runId })).messageId;
    await new Promise((resolve) => setTimeout(resolve, 200));
    // Without an idempotency key, a step goes to its run's shard.
    const step = (await queue.queue(queueName, { runId, stepId: 'step-1' })).messageId;
    expect(queueShardFor(runId, { runId, stepId: 'step-1' })).toBe(queueShardFor(runId, { runId }));
    await expect.poll(() => sink.received(), { timeout: 1_000 }).toContain(step);
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
    // A repeated key is accepted even when its retry payload differs, matching world-vercel's queue; only the first is delivered.
    expect((await queue.queue(queueName, { runId: 'dedup', requestedAt: new Date() }, opts)).messageId).toBe(first.messageId);
    expect((await env.SINK.getByName('sink').received()).filter((id) => id === first.messageId)).toHaveLength(1);
  });

  it('redelivers after the consumer requests a retry', async () => {
    const retryName = `${getQueueTopicPrefix('workflow')}retry` as ValidQueueName;
    const id = (await queue.queue(retryName, { runId: 'run-retry', runSpecVersion: 9 })).messageId;
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 6_000 }).toContain(id);
  });

  it('pins delayed messages to their sending deployment when a newer deployment delivers them', async () => {
    const persisted: EnqueueRequest[] = [];
    // SAFETY: The queue only calls dispatch on the store for these step messages.
    const stores = { getByName: () => ({
      dispatch: async (entries: { request: EnqueueRequest }[]) => entries.map(({ request }) => {
        persisted.push(request);
        return { id: request.id, fresh: true };
      }),
    }) } as unknown as WorldEnv['WORLD_STORE'];
    const sender = createQueue({ ...env, WORLD_STORE: stores, DEPLOYMENT_ID: 'A', EXECUTOR_ROUTES: '{"B":"executor-B"}' });
    const runId = `wrun_${'0'.repeat(26)}`;
    await sender.queue(queueName, { runId, stepId: 'default', stepName: 'work' }, { delaySeconds: 60 });
    await sender.queue(queueName, { runId, stepId: 'explicit', stepName: 'work' }, { delaySeconds: 60, deploymentId: 'B' });
    const targets: string[] = [];
    // SAFETY: The consumer only gets an executor by script name and calls fetch.
    const executors = { get: (script: string) => ({ fetch: async () => {
      targets.push(script);
      return Response.json({});
    } }) } as unknown as DispatchNamespace;
    const Consumer = createQueueConsumer(async () => { throw new Error('must use retained executor'); });
    const consumer = new Consumer(createExecutionContext(), {
      ...env, DEPLOYMENT_ID: 'B', EXECUTOR_ROUTES: '{"A":"executor-A","B":"executor-B"}', EXECUTORS: executors,
    });
    expect(await consumer.consume(persisted.map(({ id, ...body }) => ({ id, body, attempt: 1, enqueuedAt: Date.now() })))).toEqual([
      { kind: 'done' }, { kind: 'done' },
    ]);
    expect(targets).toEqual(['executor-A', 'executor-B']);
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
  it("delivers a run's workflow messages one at a time, whatever its spec version", async () => {
    const timed = `${getQueueTopicPrefix('workflow')}order_timed` as ValidQueueName;
    const spec9 = `wrun_${crypto.randomUUID()}`;
    const spec8 = `wrun_${crypto.randomUUID()}`;
    // Each second message carries more than a replay, so none is settled without its own delivery. A
    // keyed timer shares its run's shard, so it waits its turn too.
    await queue.queueBatch!(timed, [
      { message: { runId: spec9, runSpecVersion: 9 } },
      { message: { runId: spec9, runSpecVersion: 9, completeWaits: ['wait_1'] }, opts: { idempotencyKey: `wait:${crypto.randomUUID()}` } },
      { message: { runId: spec8, runSpecVersion: 8 } },
      { message: { runId: spec8, runSpecVersion: 8, serverErrorRetryCount: 1 } },
    ]);
    const runOf = async (runId: string) => (await env.SINK.getByName('sink').logged())
      .filter((entry) => entry.runId === runId).sort((a, b) => a.start - b.start);
    await expect.poll(async () => (await runOf(spec9)).length + (await runOf(spec8)).length, { timeout: 5_000 }).toBe(4);
    // Two replays of one run at once would let the SDK's in-process step single-flight hand one
    // request's promise to another, which Workers reject.
    for (const runId of [spec9, spec8]) {
      const [first, second] = await runOf(runId);
      expect(second.start).toBeGreaterThanOrEqual(first.end);
      // Waiting behind the run's other delivery is not a delivery.
      expect([first, second].map(({ deliveryCount, attempt }) => [deliveryCount, attempt])).toEqual([[1, 1], [1, 1]]);
    }
  });

  it("runs a run's replays one at a time on its runner even when two shards deliver them, beside other runs'", async () => {
    const timed = `${getQueueTopicPrefix('workflow')}runner_timed` as ValidQueueName;
    const [busy, other] = [`wrun_${crypto.randomUUID()}`, `wrun_${crypto.randomUUID()}`];
    const delivery = (runId: string) => ({
      id: crypto.randomUUID(), attempt: 1, deliveryCount: 1, enqueuedAt: Date.now(), body: { queueName: timed, encoded: stringify({ runId }) },
    });
    // One runner, as a run's legacy and sharded queue shards would both reach it.
    const runner = env.RUNNER.getByName(runnerShardFor(busy));
    await Promise.all([busy, busy, busy, other].map((runId) => runner.replay(runId, delivery(runId))));
    const runOf = async (runId: string) => (await env.SINK.getByName('sink').logged())
      .filter((entry) => entry.runId === runId).sort((a, b) => a.start - b.start);
    const [first, second, third] = await runOf(busy);
    expect(second.start).toBeGreaterThanOrEqual(first.end);
    expect(third.start).toBeGreaterThanOrEqual(second.end);
    const [alone] = await runOf(other);
    expect(alone.start).toBeLessThan(first.end);
  });

  it("counts every redelivery, and leaves a spec 9 handoff out of the attempt", async () => {
    const handoff = `${getQueueTopicPrefix('workflow')}h_handoff` as ValidQueueName;
    const spec9 = `wrun_${crypto.randomUUID()}`;
    const spec8 = `wrun_${crypto.randomUUID()}`;
    await queue.queue(handoff, { runId: spec9, runSpecVersion: 9 });
    // A delay is waited out, not delivered, so the first delivery after it is still the first.
    await queue.queue(handoff, { runId: spec8, runSpecVersion: 8 }, { delaySeconds: 0.5 });
    const counts = async (runId: string) => (await env.SINK.getByName('sink').logged())
      .filter((entry) => entry.runId === runId).map(({ deliveryCount, attempt }) => [deliveryCount, attempt]);
    await expect.poll(() => counts(spec9), { timeout: 5_000 }).toEqual([[1, 1], [2, 1]]);
    // An earlier run's successful timeout starts a new message, with fresh counts.
    await expect.poll(() => counts(spec8), { timeout: 5_000 }).toEqual([[1, 1], [1, 1]]);
  });

  it.each([false, true])('continues spec 8 successful wakes beyond the cap with hold=%s', async (held) => {
    const sink = env.SINK.getByName('sink');
    const runId = `continuation-${crypto.randomUUID()}`;
    const name = `${getQueueTopicPrefix('workflow')}fresh_continuation` as ValidQueueName;
    const payload = { runId, runSpecVersion: 8, requestedAt: new Date(42) };
    const opts = { idempotencyKey: crypto.randomUUID(), deploymentId: 'test-v1', ...(held ? { experimental_hold: true as const } : {}) };
    const first = await queue.queue(name, payload, opts);
    if (held) {
      if (first.experimental_hold?.status !== 'held') throw new Error('expected a held message');
      await first.experimental_hold.consume(async (_payload, meta) => {
        expect(meta).toMatchObject({ messageId: first.messageId, attempt: 1, deliveryCount: 1 });
        return { timeoutSeconds: 0.05 };
      });
    }
    const logged = async () => (await sink.logged()).filter((entry) => entry.runId === runId);
    await expect.poll(async () => (await logged()).length, { timeout: 4_000 }).toBe(5);
    const entries = await logged();
    expect(new Set(entries.map(({ id }) => id)).size).toBe(5);
    expect(entries[0].id === first.messageId).toBe(!held);
    expect(entries.map(({ attempt, deliveryCount }) => [attempt, deliveryCount])).toEqual(Array(5).fill([1, 1]));
    for (const { body } of entries) expect(body).toMatchObject({ queueName: name, encoded: stringify(payload), targetDeployment: 'test-v1' });
    for (const [index, entry] of entries.entries()) {
      if (index > 0) expect(entry.start - entries[index - 1].end).toBeGreaterThanOrEqual(45);
    }
    // The original dedup key still names the first message, never a continuation.
    expect((await queue.queue(name, payload, opts)).messageId).toBe(first.messageId);
  });

  it('passes delivery counts and the invocation start to the handler, and bounds an unbounded wake', async () => {
    const metas: unknown[] = [];
    const handle = queue.createQueueHandler(getQueueTopicPrefix('workflow'), async (_message, meta) => {
      metas.push(meta);
      return { timeoutSeconds: Number.POSITIVE_INFINITY };
    });
    const deliver = async (payload: object, extra: Record<string, string>) => (await handle(new Request('https://internal/flow', {
      method: 'POST', body: stringify(payload),
      headers: { 'x-vqs-queue-name': queueName, 'x-vqs-message-id': 'msg_1', 'x-vqs-message-attempt': '1', ...extra },
    }))).json();
    expect(await deliver({ runId: 'r', runSpecVersion: 9 }, { 'x-vqs-delivery-count': '3', 'x-workflow-invocation-started-at': '42' }))
      .toEqual({ timeoutSeconds: 900 });
    expect(await deliver({ runId: 'r' }, {})).toEqual({ timeoutSeconds: 82_800 });
    expect(metas[0]).toMatchObject({ attempt: 1, deliveryCount: 3, invocationStartedAt: 42 });
    expect(metas[1]).toMatchObject({ attempt: 1, deliveryCount: undefined });
  });

  it('holds a message for its sender, who runs it as its first delivery, with the queue as backstop', async () => {
    const sink = env.SINK.getByName('sink');
    const metas: unknown[] = [];
    const consumed = await queue.queue(queueName, { runId: crypto.randomUUID() }, { experimental_hold: { visibilityTimeoutSeconds: 1 } });
    const held = consumed.experimental_hold;
    if (held?.status !== 'held') throw new Error('expected a held message');
    await held.consume(async (_message, meta) => { metas.push(meta); });
    expect(metas).toMatchObject([{ attempt: 1, deliveryCount: 1, messageId: consumed.messageId }]);
    await expect(held.consume(async () => {})).rejects.toThrow('once');
    // A sender that never runs its message: the queue delivers it once the lease lapses.
    const dropped = await queue.queue(queueName, { runId: crypto.randomUUID() }, { experimental_hold: { visibilityTimeoutSeconds: 1 } });
    await expect.poll(() => sink.received(), { timeout: 5_000 }).toContain(dropped.messageId);
    expect(await sink.received()).not.toContain(consumed.messageId);
  });

  it('refuses a late inline consume after the expired hold starts its queued backstop', async () => {
    const runId = crypto.randomUUID();
    const slow = `${getQueueTopicPrefix('workflow')}held_slow` as ValidQueueName;
    const queued = await queue.queue(slow, { runId }, { experimental_hold: { visibilityTimeoutSeconds: 1 } });
    const held = queued.experimental_hold;
    if (held?.status !== 'held') throw new Error('expected a held message');
    const shard = env.QUEUE_SHARD.getByName(queueShardFor(runId, { runId }));
    await expect.poll(() => runInDurableObject(shard, (_shard, state) =>
      state.storage.sql.exec('SELECT id FROM inflight WHERE id = ?', queued.messageId).toArray().length), { timeout: 3_000 }).toBe(1);
    let calls = 0;
    await expect(held.consume(async () => { calls++; })).rejects.toThrow('lease');
    expect(calls).toBe(0);
    expect(await runInDurableObject(shard, (_shard, state) =>
      state.storage.sql.exec('SELECT deliveries FROM delivery_counts WHERE id = ?', queued.messageId).one().deliveries)).toBe(1);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 3_000 }).toContain(queued.messageId);
  });

  it('delivers a released or handed-off held message at once, still under its ID', async () => {
    const sink = env.SINK.getByName('sink');
    const released = await queue.queue(queueName, { runId: crypto.randomUUID() }, { experimental_hold: true });
    if (released.experimental_hold?.status !== 'held') throw new Error('expected a held message');
    await released.experimental_hold.release();
    // Well inside the 30 s lease.
    await expect.poll(() => sink.received(), { timeout: 3_000 }).toContain(released.messageId);
    const handoff = `${getQueueTopicPrefix('workflow')}held_handoff` as ValidQueueName;
    const runId = `wrun_${crypto.randomUUID()}`;
    const handedOff = await queue.queue(handoff, { runId, runSpecVersion: 9 }, { experimental_hold: true });
    if (handedOff.experimental_hold?.status !== 'held') throw new Error('expected a held message');
    await handedOff.experimental_hold.consume(async () => ({ timeoutSeconds: 0 }));
    await expect.poll(async () => (await sink.logged()).filter((entry) => entry.runId === runId)
      .map(({ id, deliveryCount, attempt }) => [id, deliveryCount, attempt]), { timeout: 3_000 })
      .toEqual([[handedOff.messageId, 2, 1]]);
  });

  it('does not hold a message for another deployment, or a repeat of an accepted key', async () => {
    const routed = createQueue({ ...env, EXECUTOR_ROUTES: '{"old-build":"executor-old"}' });
    const other = await routed.queue(queueName, { runId: crypto.randomUUID() }, { deploymentId: 'old-build', experimental_hold: true });
    expect(other.experimental_hold).toEqual({ status: 'not_held', reason: 'unsupported' });
    const opts = { idempotencyKey: crypto.randomUUID(), experimental_hold: true } as const;
    const first = await queue.queue(queueName, { runId: 'held-key' }, opts);
    expect(first.experimental_hold?.status).toBe('held');
    expect((await queue.queue(queueName, { runId: 'held-key' }, opts)).experimental_hold).toEqual({ status: 'not_held', reason: 'duplicate' });
    await expect(queue.queue(queueName, { runId: 'x' }, { delaySeconds: 1, experimental_hold: true })).rejects.toThrow('delaySeconds');
  });

  it('retains delayed delivery and rejects another deployment', async () => {
    const unreachable = await queue.queue(queueName, { runId: 'run-2' }, { deploymentId: 'old-v0' }).catch((error: unknown) => error);
    expect(queue.isDeploymentUnavailableError!(unreachable)).toBe(true);
    expect(queue.isDeploymentUnavailableError!(new Error('Deployment old-v0 is unavailable'))).toBe(false);
    const id = (await queue.queue(queueName, { runId: 'run-2' }, { delaySeconds: 1 })).messageId;
    await runDurableObjectAlarm(env.QUEUE_SHARD.getByName(queueShardFor('run-2', { runId: 'run-2' })));
    expect(await env.SINK.getByName('sink').received()).not.toContain(id);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 4000 }).toContain(id);
  });
});

describe('invocations', () => {
  it('refuses a hook payload over the payload limit before reaching the run', async () => {
    const payload = { type: 'hook_resume', version: 1, hookId: 'hook_big', token: 'token-big', payload: new Uint8Array(PAYLOAD_MAX_BYTES + 1) };
    await expect(queue.invoke?.('wrun_big', payload)).rejects.toMatchObject({ status: 413, fatal: true });
  });
});
