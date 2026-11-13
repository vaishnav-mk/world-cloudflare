import { env, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { EntityConflictError, InBandSupersededError, RunExpiredError } from '@workflow/errors';
import { SPEC_VERSION_SINGLE_ORCHESTRATOR, slotToEventId, type CreateEventParams, type CreateEventRequest } from '@workflow/world';
import { createWorld } from '@workflow/world-cloudflare';
import { storeShardFor } from '../packages/world-cloudflare/src/lib/shards.js';

const world = createWorld(env);
const bytes = (text: string) => new TextEncoder().encode(text);
const specVersion = SPEC_VERSION_SINGLE_ORCHESTRATOR;
const create = (runId: string, data: object, params?: CreateEventParams) =>
  // SAFETY: each test passes a valid event request; the World parses it with the SDK's schema.
  world.events.create(runId, { specVersion, ...data } as unknown as CreateEventRequest, params);

async function newRun(): Promise<string> {
  const created = await world.events.create(null, {
    eventType: 'run_created', specVersion, eventData: { deploymentId: 'test-v1', workflowName: 'spec9', input: bytes('in') },
  });
  await create(created.run.runId, { eventType: 'run_started' }, { inBand: true, expectedSeqInBand: 1 });
  return created.run.runId;
}

const step = (correlationId: string) => ({
  eventType: 'step_created', correlationId, eventData: { stepName: 'work', input: bytes('arg'), inline: false },
});

describe('spec 9 runs', () => {
  it('appends step and wait events without refusing them, and returns no step or wait', async () => {
    const runId = await newRun();
    const first = await create(runId, step('s'));
    expect(first.step).toBeUndefined();
    // A second creation and an outcome for a step that never existed are appends too.
    expect((await create(runId, step('s'))).event?.eventType).toBe('step_created');
    await create(runId, { eventType: 'step_completed', correlationId: 'ghost', eventData: { result: bytes('r') } });
    const wait = await create(runId, { eventType: 'wait_created', correlationId: 'w', eventData: { resumeAt: new Date(Date.now() + 60_000) } });
    expect(wait.wait).toBeUndefined();
    await create(runId, { eventType: 'wait_completed', correlationId: 'w' });
    await create(runId, { eventType: 'wait_completed', correlationId: 'w' });
    expect((await world.events.list({ runId })).data.map((event) => event.eventType)).toEqual([
      'run_created', 'run_started', 'step_created', 'step_created', 'step_completed', 'wait_created', 'wait_completed', 'wait_completed',
    ]);
    // The step record still follows the events that fit it.
    expect((await world.steps.get(runId, 's')).status).toBe('pending');
    await expect(world.steps.get(runId, 'ghost')).rejects.toThrow('not found');
  });

  it('creates an inline step from its first step_started, as one event', async () => {
    const runId = await newRun();
    await create(runId, {
      eventType: 'step_started', correlationId: 'inline',
      eventData: { stepName: 'work', input: bytes('arg'), inline: true, attempt: 1 },
    }, { inBand: true, expectedSeqInBand: 2 });
    await create(runId, { eventType: 'step_completed', correlationId: 'inline', eventData: { result: bytes('r') } }, { inBand: true, expectedSeqInBand: 3 });
    expect((await world.events.list({ runId })).data.map((event) => event.eventType)).toEqual(
      ['run_created', 'run_started', 'step_started', 'step_completed'],
    );
    expect(await world.steps.get(runId, 'inline')).toMatchObject({ status: 'completed', attempt: 1, output: bytes('r') });
  });

  it('refuses new step and wait work on a finished run but keeps taking outcomes', async () => {
    const runId = await newRun();
    await create(runId, step('open'));
    await create(runId, { eventType: 'step_started', correlationId: 'open', eventData: { attempt: 1 } });
    // Only the in-band writes moved the count: run_created and run_started.
    await create(runId, { eventType: 'run_completed', eventData: { output: bytes('done') } }, { inBand: true, expectedSeqInBand: 2 });
    const refusals = await Promise.all([
      step('late'),
      { eventType: 'step_started', correlationId: 'open', eventData: { attempt: 2 } },
      { eventType: 'step_retrying', correlationId: 'open', eventData: { error: bytes('e') } },
      { eventType: 'wait_created', correlationId: 'w', eventData: { resumeAt: new Date() } },
    ].map((data) => create(runId, data).then(() => null, (error: unknown) => error)));
    expect(refusals.map((error) => RunExpiredError.is(error))).toEqual([true, true, true, true]);
    await create(runId, { eventType: 'step_completed', correlationId: 'open', eventData: { result: bytes('r') } });
    await create(runId, { eventType: 'step_failed', correlationId: 'other', eventData: { error: bytes('e') } });
    await create(runId, { eventType: 'wait_completed', correlationId: 'w' });
  });

  it('owes the outbox only the run start, never a step message or timer', async () => {
    const runId = await newRun();
    await create(runId, step('s'));
    await create(runId, { eventType: 'wait_created', correlationId: 'w', eventData: { resumeAt: new Date() } });
    const kinds = await runInDurableObject(env.WORLD_STORE.getByName(storeShardFor(runId)), (_store, state) => state.storage.sql.exec(
      'SELECT e.kind FROM dispatch_outbox o JOIN events e ON e.run_id = o.run_id AND e.slot = o.slot WHERE o.run_id = ?', runId,
    ).toArray().map((row) => row.kind));
    expect(kinds).toEqual(['run_created']);
  });

  it('fences a batch as one block, orders it per entity, and reports what the writer skipped', async () => {
    const runId = await newRun();
    // Another writer's event the batch's writer has not seen.
    await create(runId, {
      eventType: 'attr_set', eventData: { changes: [{ key: 'k', value: 'v' }], writer: { type: 'workflow' } },
    }, { inBand: false });
    const stale = await world.events.createBatch!(runId, [{ event: step('a') as never }], {
      inBand: true, expectedSeqInBand: 1, eventCount: 2,
    }).catch((error: unknown) => error);
    expect(InBandSupersededError.is(stale)).toBe(true);
    const batch = await world.events.createBatch!(runId, [
      { event: step('a') as never },
      { event: { eventType: 'step_started', correlationId: 'a', specVersion, eventData: { attempt: 1 } } },
      { event: { eventType: 'step_completed', correlationId: 'a', specVersion, eventData: { result: bytes('r') } } },
    ], { inBand: true, expectedSeqInBand: 2, eventCount: 2 });
    expect(batch.results.map((item) => item.status)).toEqual([200, 200, 200]);
    expect(batch.allocated).toBe(3);
    expect(batch.events?.map((event) => event.eventType)).toEqual(['attr_set']);
    expect((await world.events.list({ runId })).snapshot).toEqual({ seq: 6, seqInBand: 5 });
    // On a finished run the creation is refused, and so is every later event of its step.
    await create(runId, { eventType: 'run_completed', eventData: { output: bytes('done') } }, { inBand: true, expectedSeqInBand: 5 });
    const late = await world.events.createBatch!(runId, [
      { event: step('b') as never },
      { event: { eventType: 'step_completed', correlationId: 'b', specVersion, eventData: { result: bytes('r') } } },
      { event: { eventType: 'step_completed', correlationId: 'c', specVersion, eventData: { result: bytes('r') } } },
    ]);
    expect(late.results.map((item) => item.status)).toEqual([410, 409, 200]);
    expect(late.results[2]?.event?.eventId).toBe(slotToEventId(8));
  });

  it('refuses an in-band write on a run below spec 9, which has no fence', async () => {
    const created = await world.events.create(null, {
      eventType: 'run_created', specVersion: 8, eventData: { deploymentId: 'test-v1', workflowName: 'spec8', input: bytes('in') },
    });
    const runId = created.run.runId;
    const error = await world.events.create(runId, { eventType: 'run_started' }, { inBand: true, expectedSeqInBand: 1 })
      .then(() => null, (caught: unknown) => caught);
    expect(error).toMatchObject({ status: 400 });
    expect((await world.events.list({ runId })).snapshot).toBeUndefined();
    // A spec 8 run keeps its step rules.
    await world.events.create(runId, { eventType: 'run_started' });
    await world.events.create(runId, step('s') as never);
    expect(EntityConflictError.is(await world.events.create(runId, step('s') as never).catch((caught: unknown) => caught))).toBe(true);
  });

  it('counts a lazy run_started that creates the run after the creation position', async () => {
    const runId = `wrun_${crypto.randomUUID()}`;
    const started = await create(runId, {
      eventType: 'run_started', eventData: { deploymentId: 'test-v1', workflowName: 'spec9', input: bytes('in') },
    }, { inBand: true, expectedSeqInBand: 1 });
    expect(started.allocated).toBe(1);
    expect((await world.events.list({ runId })).snapshot).toEqual({ seq: 2, seqInBand: 2 });
  });
});
