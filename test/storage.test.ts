import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { storeShardFor } from '../packages/world-cloudflare/src/store/route.js';
import { parse } from 'devalue';
import { getQueueTopicPrefix, slotToEventId, SPEC_VERSION_SUPPORTS_SLOT_IDENTITY } from '@workflow/world';
import { createStorage } from '../packages/world-cloudflare/src/store/storage.js';
import { createQueue } from '../packages/world-cloudflare/src/queue/queue.js';

const store = createStorage(env);
const bytes = (value: string) => new TextEncoder().encode(value);
async function newRun(id: string) {
  return store.events.create(id, {
    eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName: 'example', input: bytes('input') },
  });
}

describe('v5 event storage in SQLite DO', () => {
  it('recovers a committed hook event when no SDK queue publish follows', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'recover-hook', eventData: { token: crypto.randomUUID() } });
    const received = await store.events.create(runId, {
      eventType: 'hook_received', correlationId: 'recover-hook', eventData: { payload: bytes('7') },
    });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    expect(await stub.pendingHookWakeSlot(runId)).toBe(Number(received.event!.eventId.slice(5)));
    const before = (await env.SINK.getByName('sink').received()).length;
    expect(await stub.drainOutbox()).toBe(1);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toHaveLength(before + 1);
    expect(await stub.pendingHookWakeSlot(runId)).toBe(0);
    expect(await stub.drainOutbox()).toBe(0);
  });

  it('reopens a published wake when its queue delivery reaches the DLQ', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'dlq-hook', eventData: { token: crypto.randomUUID() } });
    const received = await store.events.create(runId, {
      eventType: 'hook_received', correlationId: 'dlq-hook', eventData: { payload: bytes('1') },
    });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const slot = Number(received.event!.eventId.slice(5));
    await stub.markHookWakesPublished(runId, slot);
    expect(await stub.pendingHookWakeSlot(runId)).toBe(0);
    expect(await stub.reopenPublishedHookWakes(runId)).toBe(1);
    expect(await stub.pendingHookWakeSlot(runId)).toBe(slot);
    await store.events.create(runId, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    expect(await stub.reopenPublishedHookWakes(runId)).toBe(0);
  });

  it('reopens a hook wake after the actual fanout message exhausts into R2', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'poison-hook', eventData: { token: crypto.randomUUID() } });
    const received = await store.events.create(runId, {
      eventType: 'hook_received', correlationId: 'poison-hook', eventData: { payload: bytes('7') },
    });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const slot = Number(received.event!.eventId.slice(5));
    await stub.markHookWakesPublished(runId, slot);
    expect(await stub.pendingHookWakeSlot(runId)).toBe(0);
    const queue = createQueue(env);
    const id = (await queue.queue(`${getQueueTopicPrefix('workflow')}poison`, { runId }, { deploymentId: 'test-v1' })).messageId;
    await expect.poll(async () => (await env.DLQ.list({ prefix: 'workflow/' })).objects.length,
      { timeout: 10_000 }).toBeGreaterThan(0);
    const [object] = (await env.DLQ.list({ prefix: 'workflow/' })).objects;
    expect((await (await env.DLQ.get(object.key))!.json() as { messageId: string }).messageId).toBe(id);
    expect(await stub.pendingHookWakeSlot(runId)).toBe(slot);
  }, 15_000);

  it('uses a fresh key when an outbox backup itself reaches the DLQ', async () => {
    const runId = crypto.randomUUID();
    await store.events.create(runId, {
      eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName: 'poison', input: bytes('in') },
    });
    await store.events.create(runId, { eventType: 'run_started' });
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'backup-hook', eventData: { token: crypto.randomUUID() } });
    const received = await store.events.create(runId, {
      eventType: 'hook_received', correlationId: 'backup-hook', eventData: { payload: bytes('1') },
    });
    const slot = Number(received.event!.eventId.slice(5));
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    // Earlier tests leave their own dead letters behind, so count only this run's.
    const deadIds = async () => {
      const ids = new Set<string>();
      for (const object of (await env.DLQ.list({ prefix: 'workflow/' })).objects) {
        const entry = await (await env.DLQ.get(object.key))!.json() as { messageId: string; body: { encoded: string } };
        if ((parse(entry.body.encoded) as { runId?: unknown }).runId === runId) ids.add(entry.messageId);
      }
      return ids.size;
    };
    await stub.drainOutbox();
    await expect.poll(deadIds, { timeout: 5_000 }).toBe(1);
    await expect.poll(() => stub.pendingHookWakeSlot(runId), { timeout: 5_000 }).toBe(slot);
    await new Promise((resolve) => setTimeout(resolve, 10_100));
    await stub.drainOutbox();
    await expect.poll(deadIds, { timeout: 5_000 }).toBe(2);
  }, 20_000);

  it('does not republish a hook wake accepted by the SDK queue', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'normal-hook', eventData: { token: crypto.randomUUID() } });
    await store.events.create(runId, { eventType: 'hook_received', correlationId: 'normal-hook', eventData: { payload: bytes('9') } });
    const queue = createQueue(env);
    await queue.queue(`${getQueueTopicPrefix('workflow')}example`, {
      runId, hookResumeTiming: { resumeRequestedAtMs: Date.now(), queuePublishRequestedAtMs: Date.now() },
    });
    expect(await env.WORLD_STORE.getByName(storeShardFor(runId)).pendingHookWakeSlot(runId)).toBe(0);
    expect(await env.WORLD_STORE.getByName(storeShardFor(runId)).drainOutbox()).toBe(0);
  });

  it('does not clear a later hook wake when a queue key reuses an earlier delivery', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    const queue = createQueue(env);
    const queueName = `${getQueueTopicPrefix('workflow')}example`;
    const message = { runId, hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 1 } };
    const opts = { deploymentId: 'test-v1', idempotencyKey: `wake-${runId}` };
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'first', eventData: { token: crypto.randomUUID() } });
    await store.events.create(runId, { eventType: 'hook_received', correlationId: 'first', eventData: { payload: bytes('1') } });
    await queue.queue(queueName, message, opts);
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'second', eventData: { token: crypto.randomUUID() } });
    const later = await store.events.create(runId, { eventType: 'hook_received', correlationId: 'second', eventData: { payload: bytes('2') } });
    await queue.queue(queueName, message, opts);
    expect(await env.WORLD_STORE.getByName(storeShardFor(runId)).pendingHookWakeSlot(runId)).toBe(Number(later.event!.eventId.slice(5)));
  });

  it('recovers a committed run_created when its initial queue publish is lost', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    expect(await stub.pendingRunStartSlot(runId)).toBe(1);
    const key = `run-start:${runId}:1`;
    const expectedId = `qmsg_${Buffer.from(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(key))).toString('hex')}`;
    await stub.drainOutbox();
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toContain(expectedId);
    expect(await stub.pendingRunStartSlot(runId)).toBe(0);
  });

  it('clears initial start debt when run_started commits', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    expect(await stub.pendingRunStartSlot(runId)).toBe(1);
    await store.events.create(runId, { eventType: 'run_started' });
    expect(await stub.pendingRunStartSlot(runId)).toBe(0);
    expect(await stub.drainOutbox()).toBe(0);
  });

  it('does not clear step debt when the initial run start publishes', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    const step = await store.events.create(runId, {
      eventType: 'step_created', correlationId: 'early-step', eventData: { stepName: 'double', input: bytes('2') },
    });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    await createQueue(env).queue(`${getQueueTopicPrefix('workflow')}example`, {
      runId, runInput: { input: bytes('input'), deploymentId: 'test-v1', workflowName: 'example', specVersion: 5 },
    });
    expect(await stub.pendingRunStartSlot(runId)).toBe(0);
    await store.events.create(runId, { eventType: 'run_started' });
    expect(await stub.pendingStepDispatchSlot(runId, 'early-step', 'double')).toBe(Number(step.event!.eventId.slice(5)));
    await store.events.create(runId, { eventType: 'step_started', correlationId: 'early-step' });
  });

  it('recovers a committed eager step using the SDK queue key and run namespace', async () => {
    const runId = crypto.randomUUID();
    await store.events.create(runId, { eventType: 'run_created', eventData: {
      deploymentId: 'test-v1', workflowName: 'example', input: bytes('input'),
      executionContext: { queueNamespace: 'blue' },
    } });
    await store.events.create(runId, { eventType: 'run_started' });
    const created = await store.events.create(runId, {
      eventType: 'step_created', correlationId: 'lost-step', eventData: { stepName: 'double', input: bytes('2') },
    });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const slot = Number(created.event!.eventId.slice(5));
    expect(await stub.pendingStepDispatchSlot(runId, 'lost-step', 'double')).toBe(slot);
    expect(await stub.pendingHookWakeSlot(runId)).toBe(0);
    const key = `${getQueueTopicPrefix('workflow', 'blue')}example:lost-step:a0eb0f08`;
    const expectedId = `qmsg_${Buffer.from(await crypto.subtle.digest('SHA-256', bytes(key))).toString('hex')}`;
    expect(await stub.drainOutbox()).toBe(1);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toContain(expectedId);
    expect(await stub.pendingStepDispatchSlot(runId, 'lost-step', 'double')).toBe(0);
    expect(await stub.drainOutbox()).toBe(0);
  });

  it('settles only its own pending step when the SDK publishes normally', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    await store.events.create(runId, {
      eventType: 'step_created', correlationId: 'first-step', eventData: { stepName: 'double', input: bytes('2') },
    });
    const second = await store.events.create(runId, {
      eventType: 'step_created', correlationId: 'second-step', eventData: { stepName: 'double', input: bytes('3') },
    });
    const queueName = `${getQueueTopicPrefix('workflow')}example`;
    const queue = createQueue(env);
    const message = { runId, stepId: 'first-step', stepName: 'double' };
    const opts = { deploymentId: 'test-v1', idempotencyKey: 'first-step:a0eb0f08' };
    const result = await queue.queue(queueName, message, opts);
    const key = `${queueName}:${opts.idempotencyKey}`;
    expect(result.messageId).toBe(`qmsg_${Buffer.from(await crypto.subtle.digest('SHA-256', bytes(key))).toString('hex')}`);
    expect(await stub.pendingStepDispatchSlot(runId, 'first-step', 'double')).toBe(0);
    expect(await stub.pendingStepDispatchSlot(runId, 'second-step', 'double')).toBe(Number(second.event!.eventId.slice(5)));
    expect(await stub.pendingStepDispatchSlot(runId, 'first-step', 'other')).toBe(0);
    expect(await stub.drainOutbox()).toBe(1);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toContain(result.messageId);
    expect((await env.SINK.getByName('sink').received()).filter((id) => id === result.messageId)).toHaveLength(1);
  });

  it('keeps later step debt when a repeated SDK publish deduplicates', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const queue = createQueue(env);
    const queueName = `${getQueueTopicPrefix('workflow')}example`;
    const message = { runId, stepId: 'race-step', stepName: 'double' };
    const opts = { deploymentId: 'test-v1', idempotencyKey: 'race-step:a0eb0f08' };
    // A claim accepted before step_created must not settle a later debt with the same key.
    await queue.queue(queueName, message, opts);
    const created = await store.events.create(runId, {
      eventType: 'step_created', correlationId: 'race-step', eventData: { stepName: 'double', input: bytes('2') },
    });
    await Promise.all([queue.queue(queueName, message, opts), queue.queue(queueName, message, opts)]);
    expect(await stub.pendingStepDispatchSlot(runId, 'race-step', 'double')).toBe(Number(created.event!.eventId.slice(5)));
    // Once a step starts, stale dispatch debt cannot start another body.
    await store.events.create(runId, { eventType: 'step_started', correlationId: 'race-step' });
    expect(await stub.pendingStepDispatchSlot(runId, 'race-step', 'double')).toBe(0);
    expect(await stub.drainOutbox()).toBe(0);
  });

  it('deduplicates a racing SDK publish and outbox drain by step identity', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    await store.events.create(runId, {
      eventType: 'step_created', correlationId: 'same-step', eventData: { stepName: 'double', input: bytes('2') },
    });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const queueName = `${getQueueTopicPrefix('workflow')}example`;
    const key = `${queueName}:same-step:a0eb0f08`;
    const id = `qmsg_${Buffer.from(await crypto.subtle.digest('SHA-256', bytes(key))).toString('hex')}`;
    await Promise.all([
      createQueue(env).queue(queueName, { runId, stepId: 'same-step', stepName: 'double' },
        { deploymentId: 'test-v1', idempotencyKey: 'same-step:a0eb0f08' }),
      stub.drainOutbox(),
    ]);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toContain(id);
    expect((await env.SINK.getByName('sink').received()).filter((entry) => entry === id)).toHaveLength(1);
    expect(await stub.pendingStepDispatchSlot(runId, 'same-step', 'double')).toBe(0);
  });

  it('keeps step debt when a hook wake publishes and never creates synthetic step debt', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const step = await store.events.create(runId, {
      eventType: 'step_created', correlationId: 'eager', eventData: { stepName: 'double', input: bytes('2') },
    });
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'wake', eventData: { token: crypto.randomUUID() } });
    const hook = await store.events.create(runId, {
      eventType: 'hook_received', correlationId: 'wake', eventData: { payload: bytes('yes') },
    });
    await createQueue(env).queue(`${getQueueTopicPrefix('workflow')}example`, {
      runId, hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 1 },
    });
    expect(await stub.pendingHookWakeSlot(runId)).toBe(0);
    expect(await stub.pendingStepDispatchSlot(runId, 'eager', 'double')).toBe(Number(step.event!.eventId.slice(5)));
    await stub.markHookWakesPublished(runId, Number(hook.event!.eventId.slice(5)));
    expect(await stub.pendingStepDispatchSlot(runId, 'eager', 'double')).toBe(Number(step.event!.eventId.slice(5)));
    await store.events.create(runId, { eventType: 'step_started', correlationId: 'lazy', eventData: { stepName: 'double', input: bytes('4') } });
    expect(await stub.pendingStepDispatchSlot(runId, 'lazy', 'double')).toBe(0);
    await store.events.create(runId, { eventType: 'step_started', correlationId: 'eager' });
  });

  it('reopens a real DLQ step message with the SDK payload only while pending', async () => {
    const runId = crypto.randomUUID();
    await store.events.create(runId, { eventType: 'run_created', eventData: {
      deploymentId: 'test-v1', workflowName: 'poison', input: bytes('input'),
    } });
    await store.events.create(runId, { eventType: 'run_started' });
    const created = await store.events.create(runId, {
      eventType: 'step_created', correlationId: 'poison-step', eventData: { stepName: 'double', input: bytes('2') },
    });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const slot = Number(created.event!.eventId.slice(5));
    const key = `${getQueueTopicPrefix('workflow')}poison:poison-step:a0eb0f08`;
    const id = `qmsg_${Buffer.from(await crypto.subtle.digest('SHA-256', bytes(key))).toString('hex')}`;
    await stub.drainOutbox();
    let dead: { messageId: string; body: { encoded: string; queueName: string; targetDeployment: string } } | undefined;
    await expect.poll(async () => {
      for (const object of (await env.DLQ.list({ prefix: 'workflow/' })).objects) {
        const entry = await (await env.DLQ.get(object.key))!.json() as typeof dead;
        if (entry?.messageId === id) { dead = entry; return true; }
      }
      return false;
    }, { timeout: 10_000 }).toBe(true);
    expect(parse(dead!.body.encoded)).toEqual({ runId, stepId: 'poison-step', stepName: 'double' });
    expect(dead!.body.queueName).toBe(`${getQueueTopicPrefix('workflow')}poison`);
    expect(dead!.body.targetDeployment).toBe('test-v1');
    expect(await stub.pendingStepDispatchSlot(runId, 'poison-step', 'double')).toBe(slot);
    await store.events.create(runId, { eventType: 'step_started', correlationId: 'poison-step' });
    expect(await stub.reopenPublishedStepDispatch(runId, 'poison-step', 'double')).toBe(0);
  }, 15_000);

  it('reopens DLQ step debt only while that same step is pending', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const created = await store.events.create(runId, {
      eventType: 'step_created', correlationId: 'dlq-step', eventData: { stepName: 'double', input: bytes('2') },
    });
    const slot = Number(created.event!.eventId.slice(5));
    await stub.markStepDispatchPublished(runId, slot);
    expect(await stub.reopenPublishedStepDispatch(runId, 'dlq-step', 'other')).toBe(0);
    expect(await stub.reopenPublishedStepDispatch(runId, 'dlq-step', 'double')).toBe(1);
    expect(await stub.pendingStepDispatchSlot(runId, 'dlq-step', 'double')).toBe(slot);
    await store.events.create(runId, { eventType: 'step_started', correlationId: 'dlq-step' });
    expect(await stub.reopenPublishedStepDispatch(runId, 'dlq-step', 'double')).toBe(0);
    expect(await stub.pendingStepDispatchSlot(runId, 'dlq-step', 'double')).toBe(0);
  });

  it('recovers a lost wait timer only at or after resumeAt', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    const resumeAt = new Date(Date.now() + 1_500);
    const created = await store.events.create(runId, {
      eventType: 'wait_created', correlationId: 'lost-wait', eventData: { resumeAt },
    });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const slot = Number(created.event!.eventId.slice(5));
    expect(await stub.pendingWaitDispatchSlot(runId, 'lost-wait')).toBe(slot);
    const key = `${getQueueTopicPrefix('workflow')}example:wait-backup:${runId}:${slot}`;
    const id = `qmsg_${Buffer.from(await crypto.subtle.digest('SHA-256', bytes(key))).toString('hex')}`;
    await stub.drainOutbox();
    expect((await env.SINK.getByName('sink').received())).not.toContain(id);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 6_000 }).toContain(id);
    expect(await stub.pendingWaitDispatchSlot(runId, 'lost-wait')).toBe(0);
    await store.events.create(runId, { eventType: 'wait_completed', correlationId: 'lost-wait' });
  }, 10_000);

  it('marks only the matching wait debt when the SDK timer is accepted', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    const resumeAt = new Date(Date.now() + 30_000);
    await store.events.create(runId, { eventType: 'wait_created', correlationId: 'first-wait', eventData: { resumeAt } });
    const second = await store.events.create(runId, { eventType: 'wait_created', correlationId: 'second-wait', eventData: { resumeAt } });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    await createQueue(env).queue(`${getQueueTopicPrefix('workflow')}example`, {
      runId, waitContinuation: { correlationId: 'first-wait', attempt: 0 },
    }, { deploymentId: 'test-v1', delaySeconds: 30, idempotencyKey: 'first-wait' });
    expect(await stub.pendingWaitDispatchSlot(runId, 'first-wait')).toBe(0);
    expect(await stub.pendingWaitDispatchSlot(runId, 'second-wait')).toBe(Number(second.event!.eventId.slice(5)));
    await stub.drainOutbox();
    expect(await stub.pendingWaitDispatchSlot(runId, 'second-wait')).toBe(Number(second.event!.eventId.slice(5)));
    await store.events.create(runId, { eventType: 'wait_completed', correlationId: 'first-wait' });
    await store.events.create(runId, { eventType: 'wait_completed', correlationId: 'second-wait' });
  });

  it('does not settle a wait when publishing unrelated hook or step messages', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    await store.events.create(runId, { eventType: 'run_started' });
    const created = await store.events.create(runId, {
      eventType: 'wait_created', correlationId: 'sleep', eventData: { resumeAt: new Date(Date.now() + 30_000) },
    });
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const slot = Number(created.event!.eventId.slice(5));
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'unrelated-hook', eventData: { token: crypto.randomUUID() } });
    await store.events.create(runId, { eventType: 'hook_received', correlationId: 'unrelated-hook', eventData: { payload: bytes('yes') } });
    await store.events.create(runId, { eventType: 'step_created', correlationId: 'unrelated-step', eventData: { stepName: 'double', input: bytes('2') } });
    const queue = createQueue(env);
    const queueName = `${getQueueTopicPrefix('workflow')}example`;
    await queue.queue(queueName, { runId, hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 1 } });
    await queue.queue(queueName, { runId, stepId: 'unrelated-step', stepName: 'double' },
      { idempotencyKey: 'unrelated-step:a0eb0f08' });
    expect(await stub.pendingWaitDispatchSlot(runId, 'sleep')).toBe(slot);
    await store.events.create(runId, { eventType: 'step_started', correlationId: 'unrelated-step' });
    await store.events.create(runId, { eventType: 'wait_completed', correlationId: 'sleep' });
  });

  it('skips completed waits and terminal runs without sending continuations', async () => {
    const completedRun = crypto.randomUUID();
    await newRun(completedRun);
    await store.events.create(completedRun, { eventType: 'run_started' });
    await store.events.create(completedRun, { eventType: 'wait_created', correlationId: 'done', eventData: { resumeAt: new Date(Date.now() - 1) } });
    await store.events.create(completedRun, { eventType: 'wait_completed', correlationId: 'done' });
    expect(await env.WORLD_STORE.getByName(storeShardFor(completedRun)).pendingWaitDispatchSlot(completedRun, 'done')).toBe(0);
    const terminalRun = crypto.randomUUID();
    await newRun(terminalRun);
    await store.events.create(terminalRun, { eventType: 'run_started' });
    await store.events.create(terminalRun, { eventType: 'wait_created', correlationId: 'cancelled', eventData: { resumeAt: new Date(Date.now() + 30_000) } });
    await store.events.create(terminalRun, { eventType: 'run_cancelled' });
    const stub = env.WORLD_STORE.getByName(storeShardFor(terminalRun));
    expect(await stub.pendingWaitDispatchSlot(terminalRun, 'cancelled')).toBe(0);
    expect(await stub.reopenPublishedWaitDispatch(terminalRun, 'cancelled')).toBe(0);
  });

  it('reopens DLQ wait debt for normal timers and backup messages only while waiting', async () => {
    const deadMessage = async (id: string) => {
      for (const object of (await env.DLQ.list({ prefix: 'workflow/' })).objects) {
        const entry = await (await env.DLQ.get(object.key))!.json() as {
          messageId: string; body: { encoded: string; queueName: string; targetDeployment?: string };
        };
        if (entry.messageId === id) return entry;
      }
    };
    const runId = crypto.randomUUID();
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    await store.events.create(runId, { eventType: 'run_created', eventData: {
      deploymentId: 'test-v1', workflowName: 'poison', input: bytes('input'),
    } });
    await store.events.create(runId, { eventType: 'run_started' });
    const created = await store.events.create(runId, {
      eventType: 'wait_created', correlationId: 'poison-wait', eventData: { resumeAt: new Date(Date.now() + 30_000) },
    });
    const slot = Number(created.event!.eventId.slice(5));
    const normalId = (await createQueue(env).queue(`${getQueueTopicPrefix('workflow')}poison`, {
      runId, waitContinuation: { correlationId: 'poison-wait', attempt: 0 },
    }, { deploymentId: 'test-v1', idempotencyKey: 'poison-wait' })).messageId;
    expect(await stub.pendingWaitDispatchSlot(runId, 'poison-wait')).toBe(0);
    await expect.poll(() => deadMessage(normalId!), { timeout: 10_000 }).toBeDefined();
    expect(await stub.pendingWaitDispatchSlot(runId, 'poison-wait')).toBe(slot);
    await stub.drainOutbox();
    expect(await stub.pendingWaitDispatchSlot(runId, 'poison-wait')).toBe(slot);
    await store.events.create(runId, { eventType: 'wait_completed', correlationId: 'poison-wait' });
    expect(await stub.reopenPublishedWaitDispatch(runId, 'poison-wait')).toBe(0);

    const backupRun = crypto.randomUUID();
    await store.events.create(backupRun, { eventType: 'run_created', eventData: {
      deploymentId: 'test-v1', workflowName: 'poison', input: bytes('input'),
    } });
    await store.events.create(backupRun, { eventType: 'run_started' });
    const backup = await store.events.create(backupRun, {
      eventType: 'wait_created', correlationId: 'backup-wait', eventData: { resumeAt: new Date(Date.now() - 1) },
    });
    const backupSlot = Number(backup.event!.eventId.slice(5));
    const key = `${getQueueTopicPrefix('workflow')}poison:wait-backup:${backupRun}:${backupSlot}`;
    const backupId = `qmsg_${Buffer.from(await crypto.subtle.digest('SHA-256', bytes(key))).toString('hex')}`;
    const backupStub = env.WORLD_STORE.getByName(storeShardFor(backupRun));
    await backupStub.drainOutbox();
    await expect.poll(() => deadMessage(backupId), { timeout: 10_000 }).toBeDefined();
    expect(parse((await deadMessage(backupId))!.body.encoded)).toEqual({
      runId: backupRun, waitContinuation: { correlationId: 'backup-wait', attempt: 0 },
    });
    expect((await deadMessage(backupId))!.body.targetDeployment).toBe('test-v1');
    expect(await backupStub.pendingWaitDispatchSlot(backupRun, 'backup-wait')).toBe(backupSlot);
    await store.events.create(backupRun, { eventType: 'wait_completed', correlationId: 'backup-wait' });
    expect(await backupStub.reopenPublishedWaitDispatch(backupRun, 'backup-wait')).toBe(0);
  }, 15_000);

  it('gets many runs in input order with nulls and resolved-data selection', async () => {
    const first = crypto.randomUUID(), second = crypto.randomUUID();
    await Promise.all([newRun(first), newRun(second)]);
    const runs = await store.runs.getMany!([second, 'missing-run', first, second]);
    expect(runs.map((run) => run?.runId ?? null)).toEqual([second, null, first, second]);
    expect(runs[0]?.input).toEqual(bytes('input'));
    const withoutData = await store.runs.getMany!([first, 'missing-run'], { resolveData: 'none' });
    expect(withoutData[0]?.input).toBeUndefined();
    expect(withoutData[1]).toBeNull();
  });

  it('allocates dense event IDs and atomically materializes run and step state', async () => {
    const runId = crypto.randomUUID();
    expect((await newRun(runId)).event?.eventId).toBe(slotToEventId(1));
    await store.events.create(runId, { eventType: 'run_started' });
    await store.events.create(runId, { eventType: 'step_created', correlationId: 'step-1', eventData: { stepName: 'double', input: bytes('2') } });
    await store.events.create(runId, { eventType: 'step_started', correlationId: 'step-1' });
    await store.events.create(runId, { eventType: 'step_completed', correlationId: 'step-1', eventData: { result: bytes('4') } });
    await store.events.create(runId, { eventType: 'run_completed', eventData: { output: bytes('4') } });
    const events = await store.events.list({ runId });
    expect(events.data.map((event) => event.eventId)).toEqual(Array.from({ length: 6 }, (_, i) => slotToEventId(i + 1)));
    expect((await store.runs.get(runId)).status).toBe('completed');
    expect((await store.steps.get(runId, 'step-1')).status).toBe('completed');
    expect((await store.events.list({ runId, resolveData: 'none' })).data[0].eventData).not.toHaveProperty('input');
  });

  it('long-polls for terminal state and returns a pending snapshot on timeout', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    const pending = await store.runs.waitForTerminalStatus!(runId, { timeoutMs: 50, resolveData: 'none' });
    expect(pending.status).toBe('pending');
    expect(pending.input).toBeUndefined();
    const waiting = store.runs.waitForTerminalStatus!(runId, { timeoutMs: 5_000 });
    await store.events.create(runId, { eventType: 'run_started' });
    // An early nonterminal wake is permitted; the next wait observes completion.
    await store.events.create(runId, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    const result = await waiting;
    expect(['running', 'completed']).toContain(result.status);
    expect((await store.runs.waitForTerminalStatus!(runId, { timeoutMs: 5_000 })).status).toBe('completed');
  });

  it('keeps an existing run’s protocol version on later events', async () => {
    const runId = crypto.randomUUID();
    await store.events.create(runId, {
      eventType: 'run_created', specVersion: SPEC_VERSION_SUPPORTS_SLOT_IDENTITY,
      eventData: { deploymentId: 'test-v1', workflowName: 'example', input: bytes('input') },
    });
    const started = await store.events.create(runId, { eventType: 'run_started' });
    expect(started.event?.specVersion).toBe(SPEC_VERSION_SUPPORTS_SLOT_IDENTITY);
    expect((await store.runs.get(runId)).specVersion).toBe(SPEC_VERSION_SUPPORTS_SLOT_IDENTITY);
  });

  it('preserves user data that looks like internal date or byte fields', async () => {
    const runId = crypto.randomUUID();
    await store.events.create(runId, { eventType: 'run_created', eventData: {
      deploymentId: 'test-v1', workflowName: 'example', input: bytes('input'),
      attributes: { createdAt: 'customer text' },
      executionContext: { payload: { updatedAt: 'not a date', __workflowBytes: [1, 2, 3] } },
    } });
    const run = await store.runs.get(runId);
    expect(run.attributes?.createdAt).toBe('customer text');
    expect(run.executionContext?.payload).toEqual({ updatedAt: 'not a date', __workflowBytes: [1, 2, 3] });
    await store.events.create(runId, { eventType: 'hook_created', correlationId: 'hook-meta', eventData: {
      token: crypto.randomUUID(), metadata: bytes('meta'),
    } });
    expect((await store.hooks.get('hook-meta')).metadata).toEqual(bytes('meta'));
  });

  it('commits attribute events with the run view', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    const result = await store.events.create(runId, { eventType: 'attr_set', eventData: {
      changes: [{ key: 'phase', value: 'started' }], writer: { type: 'workflow' },
    } });
    expect(result.event?.eventType).toBe('attr_set');
    expect(result.run?.attributes?.phase).toBe('started');
    expect((await store.runs.get(runId)).attributes?.phase).toBe('started');
  });

  it('enforces the SDK attribute count and write-byte limits across merges', async () => {
    const runId = crypto.randomUUID();
    const attributes = Object.fromEntries(Array.from({ length: 64 }, (_, index) => [`k${index}`, 'x']));
    await store.events.create(runId, { eventType: 'run_created', eventData: {
      deploymentId: 'test-v1', workflowName: 'example', input: bytes('in'), attributes,
    } });
    await expect(store.events.create(runId, { eventType: 'attr_set', eventData: {
      changes: [{ key: 'overflow', value: 'x' }], writer: { type: 'workflow' },
    } })).rejects.toThrow('64');
    await store.events.create(runId, { eventType: 'attr_set', eventData: {
      changes: [{ key: 'k0', value: null }, { key: 'replacement', value: 'x' }], writer: { type: 'workflow' },
    } });
    expect(Object.keys((await store.runs.get(runId)).attributes ?? {})).toHaveLength(64);
    const oversized = Array.from({ length: 40 }, (_, index) => ({ key: `long${index}`, value: 'y'.repeat(256) }));
    await expect(store.events.create(runId, { eventType: 'attr_set', eventData: {
      changes: oversized, writer: { type: 'workflow' },
    } })).rejects.toThrow('8192');
  });

  it('replays retained events then wakes a live subscriber, including terminal filtering', async () => {
    const runId = crypto.randomUUID();
    await newRun(runId);
    const stub = env.WORLD_STORE.getByName(storeShardFor(runId));
    const pending = stub.nextEvents(runId, 1);
    await store.events.create(runId, { eventType: 'run_started' });
    const next = await pending;
    expect(next.events.map((event) => event.eventType)).toEqual(['run_started']);
    expect(next.cursor).toBe(2);
    await store.events.create(runId, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    const filtered = await stub.nextEvents(runId, 2, ['step_completed']);
    expect(filtered.events).toEqual([]);
    expect(filtered.done).toBe(true);
  });
});
