import { env, runDurableObjectAlarm } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import {
  getQueueTopicPrefix, mintedSpecVersion, slotToEventId, SPEC_VERSION_MAX_SUPPORTED,
  ValidQueueName, workflowRunIdSchema,
} from '@workflow/world';
import { EntityConflictError, RunExpiredError } from '@workflow/errors';
import { createWorld } from '@workflow/world-cloudflare';

const world = createWorld(env);
const bytes = (text: string) => new TextEncoder().encode(text);

async function newRun(workflowName = `contract-${crypto.randomUUID()}`) {
  const created = await world.events.create(null, {
    eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName, input: bytes('input') },
  });
  return created.run.runId;
}

describe('World v5 contract (workerd bindings)', () => {
  it('mints a run ID and stamps the declared protocol version on run_created(null)', async () => {
    const created = await world.events.create(null, {
      eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName: 'contract', input: bytes('input') },
    });
    const runId = workflowRunIdSchema.parse(created.run.runId);
    expect(created.event).toMatchObject({ runId, eventId: slotToEventId(1), eventType: 'run_created' });
    expect(created.run).toMatchObject({ runId, status: 'pending', input: bytes('input') });
    expect(world.specVersion).toBe(mintedSpecVersion());
    expect(created.run.specVersion).toBeGreaterThanOrEqual(mintedSpecVersion());
    expect(created.run.specVersion).toBeLessThanOrEqual(SPEC_VERSION_MAX_SUPPORTED);
  });

  it('filters runs by one status, several statuses, or an empty set', async () => {
    const name = `contract-status-${crypto.randomUUID()}`;
    const pending = await newRun(name);
    const running = await newRun(name);
    const completed = await newRun(name);
    await world.events.create(running, { eventType: 'run_started' });
    await world.events.create(completed, { eventType: 'run_started' });
    await world.events.create(completed, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    const ids = async (status: 'pending' | 'running' | 'completed' | ('pending' | 'completed')[]) =>
      (await world.runs.list({ workflowName: name, status })).data.map((run) => run.runId).sort();
    expect(await ids('running')).toEqual([running]);
    expect(await ids('completed')).toEqual([completed]);
    expect(await ids(['pending', 'completed'])).toEqual([pending, completed].sort());
    expect((await world.runs.list({ workflowName: name, status: [] })).data).toEqual([]);
  });

  it('allocates dense slots, reports skipped events to a stale writer, and returns the sinceCursor delta', async () => {
    const runId = await newRun();
    const first = await world.events.list({ runId });
    await world.events.create(runId, { eventType: 'run_started' });
    const stale = await world.events.create(runId, {
      eventType: 'step_created', correlationId: 'step', eventData: { stepName: 'work', input: bytes('argument') },
    }, { eventCount: 1 });
    expect(stale.event?.eventId).toBe(slotToEventId(3));
    expect(stale.events?.map((event) => event.eventId)).toEqual([slotToEventId(2)]);
    const since = await world.events.create(runId, { eventType: 'step_started', correlationId: 'step' }, {
      sinceCursor: first.cursor!,
    });
    expect(since.events?.map((event) => event.eventId)).toEqual([slotToEventId(2), slotToEventId(3), slotToEventId(4)]);
    expect(since.hasMore).toBe(false);
    expect(since.cursor).toBe((await world.events.list({ runId })).cursor);
    expect((await world.events.list({ runId })).data.map((event) => event.eventId)).toEqual(
      [1, 2, 3, 4].map(slotToEventId),
    );
  });

  it("treats skip-step-inputs as all except possibly step inputs, unlike none", async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    await world.events.create(runId, {
      eventType: 'step_created', correlationId: 'step', eventData: { stepName: 'work', input: bytes('argument') },
    });
    await world.events.create(runId, { eventType: 'step_started', correlationId: 'step' });
    await world.events.create(runId, {
      eventType: 'step_completed', correlationId: 'step', eventData: { result: bytes('result') },
    });
    const all = (await world.events.list({ runId, resolveData: 'all' })).data;
    const skipped = (await world.events.list({ runId, resolveData: 'skip-step-inputs' })).data;
    const none = (await world.events.list({ runId, resolveData: 'none' })).data;
    expect(skipped.map((event) => event.eventId)).toEqual(all.map((event) => event.eventId));
    expect(skipped.filter((event) => !['step_created', 'step_started'].includes(event.eventType))).toEqual(
      all.filter((event) => !['step_created', 'step_started'].includes(event.eventType)),
    );
    const step = skipped.find((event) => event.eventType === 'step_created');
    expect(step?.eventType).toBe('step_created');
    if (step?.eventType === 'step_created') expect([undefined, bytes('argument')]).toContainEqual(step.eventData?.input);
    expect(none.find((event) => event.eventType === 'run_created')).not.toHaveProperty('eventData.input');
    expect(none.find((event) => event.eventType === 'step_completed')).not.toHaveProperty('eventData.result');
    expect(none.find((event) => event.eventType === 'step_created')).toHaveProperty('eventData.stepName', 'work');
  });

  it('allows one concurrent lazy step_started claim and refuses a start after terminal run state', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    const claim = { eventType: 'step_started', correlationId: 'race', eventData: { stepName: 'work', input: bytes('argument') } } as const;
    const claims = await Promise.allSettled([world.events.create(runId, claim), world.events.create(runId, claim)]);
    expect(claims.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
    const loser = claims.find((result) => result.status === 'rejected');
    expect(loser?.status === 'rejected' && EntityConflictError.is(loser.reason)).toBe(true);
    expect((await world.steps.get(runId, 'race')).status).toBe('running');
    expect((await world.events.list({ runId })).data.map((event) => event.eventId)).toEqual(
      [1, 2, 3, 4].map(slotToEventId),
    );
    await world.events.create(runId, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    const restart = await world.events.create(runId, { eventType: 'step_started', correlationId: 'race' })
      .then(() => null, (error: unknown) => error);
    expect(EntityConflictError.is(restart)).toBe(true);
    const late = await world.events.create(runId, {
      eventType: 'step_started', correlationId: 'late', eventData: { stepName: 'work', input: bytes('late') },
    }).then(() => null, (error: unknown) => error);
    expect(EntityConflictError.is(late)).toBe(true);
    expect((await world.steps.list({ runId })).data.map((step) => step.stepId)).toEqual(['race']);
  });

  it('reports the token owner on collision, retains it after completion, and frees it on disposal', async () => {
    const owner = await newRun();
    const contender = await newRun();
    const token = crypto.randomUUID();
    await world.events.create(owner, { eventType: 'hook_created', correlationId: 'owned', eventData: {
      token, tokenRetentionUntil: new Date(Date.now() + 60_000),
    } });
    const conflict = await world.events.create(contender, {
      eventType: 'hook_created', correlationId: 'collision', eventData: { token },
    });
    expect(conflict.event).toMatchObject({ eventType: 'hook_conflict', eventData: { token, conflictingRunId: owner } });
    expect((await world.hooks.getByToken(token)).runId).toBe(owner);
    await world.events.create(owner, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    expect((await world.hooks.getByToken(token)).runId).toBe(owner);
    const retainedConflict = await world.events.create(contender, {
      eventType: 'hook_created', correlationId: 'retained-collision', eventData: { token },
    });
    expect(retainedConflict.event).toMatchObject({ eventType: 'hook_conflict', eventData: { conflictingRunId: owner } });
    const late = await world.events.create(owner, {
      eventType: 'hook_received', correlationId: 'owned', eventData: { payload: bytes('late') },
    }).then(() => null, (error: unknown) => error);
    expect(RunExpiredError.is(late)).toBe(true);
    await world.events.create(owner, { eventType: 'hook_disposed', correlationId: 'owned' });
    expect((await world.hooks.list({ runId: owner })).data).toEqual([]);
    expect((await world.events.create(contender, {
      eventType: 'hook_created', correlationId: 'new-owner', eventData: { token },
    })).hook?.runId).toBe(contender);
  });

  it('validates queue names and delays, keeps keyed message IDs stable, and delays delivery', async () => {
    const name = ValidQueueName.parse(`${getQueueTopicPrefix('workflow')}contract-${crypto.randomUUID()}`);
    const message = { runId: crypto.randomUUID() };
    await expect(world.queue('not-a-workflow-queue' as typeof name, message)).rejects.toThrow();
    await expect(world.queue(name, message, { delaySeconds: -1 })).rejects.toThrow();
    const options = { delaySeconds: 2, idempotencyKey: crypto.randomUUID() };
    const first = await world.queue(name, message, options);
    expect(first.messageId).toMatch(/^qmsg_[a-f0-9]{64}$/);
    expect((await world.queue(name, message, options)).messageId).toBe(first.messageId);
    expect((await world.queue(name, { runId: 'different' }, { ...options, delaySeconds: 3 })).messageId)
      .toBe(first.messageId);
    await runDurableObjectAlarm(env.QUEUE_SHARD.getByName('flow'));
    expect(await env.SINK.getByName('sink').received()).not.toContain(first.messageId);
    await expect.poll(() => env.SINK.getByName('sink').received(), { timeout: 5_000 }).toContain(first.messageId);
    expect((await env.SINK.getByName('sink').received()).filter((id) => id === first.messageId)).toHaveLength(1);
  }, 10_000);

  it('supports negative stream start, chunk cursors, done, run isolation, and close', async () => {
    const runId = crypto.randomUUID();
    const otherRun = crypto.randomUUID();
    expect(await world.streams.getInfo(runId, 'stdout')).toEqual({ tailIndex: -1, done: false });
    await world.streams.write(runId, 'stdout', 'a');
    await world.streams.write(runId, 'stdout', 'b');
    await world.streams.write(runId, 'stdout', 'c');
    await world.streams.write(otherRun, 'stdout', 'other');
    const first = await world.streams.getChunks(runId, 'stdout', { limit: 2 });
    expect(first.data).toEqual([{ index: 0, data: bytes('a') }, { index: 1, data: bytes('b') }]);
    expect(first.hasMore).toBe(true);
    expect(first.done).toBe(false);
    expect((await world.streams.getChunks(runId, 'stdout', { cursor: first.cursor!, limit: 2 })).data).toEqual([
      { index: 2, data: bytes('c') },
    ]);
    expect(await world.streams.getInfo(runId, 'stdout')).toEqual({ tailIndex: 2, done: false });
    expect(await world.streams.list(otherRun)).toEqual(['stdout']);
    expect((await world.streams.getChunks(otherRun, 'stdout')).data).toEqual([{ index: 0, data: bytes('other') }]);
    await world.streams.close(runId, 'stdout');
    expect(await world.streams.getInfo(runId, 'stdout')).toEqual({ tailIndex: 2, done: true });
    expect((await world.streams.getChunks(runId, 'stdout', { cursor: first.cursor! })).done).toBe(true);
    expect(await new Response(await world.streams.get(runId, 'stdout', -2)).text()).toBe('bc');
    expect(await world.streams.getInfo(otherRun, 'stdout')).toEqual({ tailIndex: 0, done: false });
    await expect(world.streams.write(runId, 'stdout', 'late')).rejects.toThrow();
  });
});
