import { env, runDurableObjectAlarm, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { OUT_OF_BAND_REPORT_LIMIT } from '../packages/world-cloudflare/src/constants.js';
import { queueShardFor, storeShardFor } from '../packages/world-cloudflare/src/lib/shards.js';
import {
  getQueueTopicPrefix, mintedSpecVersion, slotToEventId, SPEC_VERSION_MAX_SUPPORTED,
  ValidQueueName, workflowRunIdSchema,
} from '@workflow/world';
import { EntityConflictError, HookNotFoundError, RunExpiredError } from '@workflow/errors';
import { createWorld } from 'world-cloudflare';

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

  it('lists events for a caller that passes an AbortSignal, which cannot cross RPC', async () => {
    const runId = await newRun();
    const listed = await world.events.list({ runId, signal: new AbortController().signal, specVersion: world.specVersion });
    expect(listed.data.map((event) => event.eventType)).toEqual(['run_created']);
  });

  it('declares that only its own queue reaches the flow route', () => {
    expect(world.queueAuthMode).toBe('skip');
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
    // A write's eventCursor resumes an ascending list just after its event, which the list returns as written.
    expect(since.eventCursor).toBe(since.cursor);
    const fromStale = await world.events.list({ runId, pagination: { cursor: stale.eventCursor } });
    expect(fromStale.data.map((event) => event.eventId)).toEqual([slotToEventId(4)]);
    expect((await world.events.list({ runId })).data[2]).toEqual(stale.event);
    expect((await world.events.create(runId, { eventType: 'attr_set', eventData: {
      changes: [{ key: 'k', value: 'v' }], writer: { type: 'workflow' },
    } }, { resolveData: 'none' })).eventCursor).toBeUndefined();
  });

  it('pushes committed events to a subscriber in slot order, stops on unsubscribe, and reports a finished run', async () => {
    const runId = await newRun();
    const seen: string[] = [];
    let ended: unknown;
    const unsubscribe = world.events.subscribe!(runId, 1, (event) => seen.push(event.eventType), {
      onError: (error) => { ended = error; },
    });
    await world.events.create(runId, { eventType: 'run_started' });
    await world.events.create(runId, {
      eventType: 'step_created', correlationId: 'step', eventData: { stepName: 'work', input: bytes('argument') },
    });
    await expect.poll(() => seen).toEqual(['run_started', 'step_created']);
    unsubscribe();
    await world.events.create(runId, { eventType: 'step_started', correlationId: 'step' });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(seen).toEqual(['run_started', 'step_created']);
    expect(ended).toBeUndefined();
    // A subscription to a finished run delivers what is left, then says it has ended.
    await world.events.create(runId, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    const rest: string[] = [];
    const finished = new Promise((resolve) => world.events.subscribe!(runId, 3, (event) => rest.push(event.eventType), { onError: resolve }));
    expect(await finished).toBeInstanceOf(Error);
    expect(rest).toEqual(['step_started', 'run_completed']);
  });

  it('gets one event by ID, with or without its payload', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    await world.events.create(runId, {
      eventType: 'step_created', correlationId: 'step', eventData: { stepName: 'work', input: bytes('argument') },
    });
    expect(await world.events.get(runId, slotToEventId(3))).toMatchObject({
      runId, eventId: slotToEventId(3), eventType: 'step_created', correlationId: 'step',
      eventData: { stepName: 'work', input: bytes('argument') },
    });
    const bare = await world.events.get(runId, slotToEventId(3), { resolveData: 'none' });
    expect(bare).toHaveProperty('eventData.stepName', 'work');
    expect(bare).not.toHaveProperty('eventData.input');
    await expect(world.events.get(runId, slotToEventId(4))).rejects.toThrow('not found');
    await expect(world.events.get(await newRun(), slotToEventId(3))).rejects.toThrow('not found');
    await expect(world.events.get(runId, 'evnt_3')).rejects.toThrow('Invalid event ID');
  });

  it("lists one correlation ID's events within its run, in slot order and a page at a time", async () => {
    const runId = await newRun();
    const other = await newRun();
    const step = (correlationId: string) => ({
      eventType: 'step_created' as const, correlationId, eventData: { stepName: 'work', input: bytes('argument') },
    });
    await world.events.create(runId, { eventType: 'run_started' });
    await world.events.create(other, { eventType: 'run_started' });
    await world.events.create(runId, step('a'));
    await world.events.create(runId, step('b'));
    await world.events.create(other, step('a'));
    await world.events.create(runId, { eventType: 'step_started', correlationId: 'a' });
    await world.events.create(runId, { eventType: 'step_completed', correlationId: 'a', eventData: { result: bytes('result') } });
    const page = (pagination?: { limit?: number; cursor?: string; sortOrder?: 'asc' | 'desc' }) =>
      world.events.listByCorrelationId({ runId, correlationId: 'a', pagination });
    const first = await page({ limit: 2 });
    expect(first.data.map((event) => [event.eventId, event.eventType])).toEqual([
      [slotToEventId(3), 'step_created'], [slotToEventId(5), 'step_started'],
    ]);
    expect(first.hasMore).toBe(true);
    const second = await page({ limit: 2, cursor: first.cursor! });
    expect(second.data.map((event) => event.eventId)).toEqual([slotToEventId(6)]);
    expect(second.data[0]).toHaveProperty('eventData.result', bytes('result'));
    expect(second.hasMore).toBe(false);
    expect((await page({ sortOrder: 'desc' })).data.map((event) => event.eventId)).toEqual([6, 5, 3].map(slotToEventId));
    const bare = await world.events.listByCorrelationId({ runId, correlationId: 'a', resolveData: 'none' });
    expect(bare.data[2]).not.toHaveProperty('eventData.result');
  });

  it('ends an event page once its rows pass the byte cap, so a large log comes back in several pages', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    // Under the R2 threshold, so each input stays in the store: ten of them pass the 4 MiB page cap.
    const input = new Uint8Array(600 * 1024);
    for (let index = 0; index < 10; index++) {
      await world.events.create(runId, { eventType: 'step_created', correlationId: `step-${index}`, eventData: { stepName: 'work', input } });
    }
    const sizes: number[] = [];
    const ids: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await world.events.list({ runId, pagination: { cursor } });
      sizes.push(page.data.length);
      ids.push(...page.data.map((event) => event.eventId));
      cursor = page.hasMore ? page.cursor! : undefined;
    } while (cursor);
    // The first page stops at the step row that crosses 4 MiB.
    expect(sizes).toEqual([9, 3]);
    expect(ids).toEqual(Array.from({ length: 12 }, (_, index) => slotToEventId(index + 1)));
  });

  it('collapses retries of one hook resume onto its first event and refuses a resumeId reused for another payload', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    await world.events.create(runId, { eventType: 'hook_created', correlationId: 'hook', eventData: { token: crypto.randomUUID() } });
    const resume = (payload: string) => ({
      eventType: 'hook_received' as const, correlationId: 'hook', eventData: { payload: bytes(payload) },
    });
    const params = { resumeId: 'resume-1', resumePayloadDigest: 'digest-a' };
    const [first, concurrent] = await Promise.all([
      world.events.create(runId, resume('a'), params), world.events.create(runId, resume('a'), params),
    ]);
    expect(concurrent.event).toEqual(first.event);
    expect(first.event?.resumeId).toBe('resume-1');
    expect((await world.events.create(runId, resume('a'), params)).event).toEqual(first.event);
    const reused = await world.events.create(runId, resume('b'), { resumeId: 'resume-1', resumePayloadDigest: 'digest-b' })
      .then(() => null, (error: unknown) => error);
    expect(EntityConflictError.is(reused)).toBe(true);
    // Another resume ID is another resume of the same hook.
    await world.events.create(runId, resume('b'), { resumeId: 'resume-2', resumePayloadDigest: 'digest-b' });
    const received = (await world.events.list({ runId })).data.filter((event) => event.eventType === 'hook_received');
    expect(received.map((event) => event.resumeId)).toEqual(['resume-1', 'resume-2']);
    // A retry that lands after the run finished still gets the committed event.
    await world.events.create(runId, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    expect((await world.events.create(runId, resume('a'), params)).event).toEqual(first.event);
  });

  it('takes a cached hook resume only with its dedup key, and refuses a missing hook before writing', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    const hookId = `hook_${crypto.randomUUID()}`;
    await world.events.create(runId, { eventType: 'hook_created', correlationId: hookId, eventData: { token: crypto.randomUUID() } });
    const resume = (correlationId: string) => ({
      eventType: 'hook_received' as const, correlationId, eventData: { payload: bytes('p') },
    });
    const before = (await world.events.list({ runId })).data.length;
    const keyless = await world.events.create(runId, resume(hookId), { requireHookResumeDedup: true }).catch((error: unknown) => error);
    expect(keyless).toMatchObject({ status: 400 });
    const missing = await world.events.create(runId, resume('hook_gone'), {
      requireHookResumeDedup: true, resumeId: 'r1', resumePayloadDigest: 'd',
    }).catch((error: unknown) => error);
    expect(HookNotFoundError.is(missing)).toBe(true);
    expect((await world.events.list({ runId })).data).toHaveLength(before);
    const params = { requireHookResumeDedup: true, resumeId: 'r1', resumePayloadDigest: 'd' };
    const first = await world.events.create(runId, resume(hookId), params);
    expect((await world.events.create(runId, resume(hookId), params)).event).toEqual(first.event);
  });

  it('appends a batch in order, each event with the outcome its own create would have had', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    const step = (correlationId: string) => ({ event: {
      eventType: 'step_created' as const, correlationId, eventData: { stepName: 'work', input: bytes('argument') },
    } });
    const first = await world.events.createBatch!(runId, [step('a'), step('b')]);
    expect(first.results.map((item) => [item.status, item.event?.eventId])).toEqual([[200, slotToEventId(3)], [200, slotToEventId(4)]]);
    expect(first.results[0]?.step?.status).toBe('pending');
    // `a` exists already: its item conflicts and takes no slot, and `c` still commits right after `b`.
    const second = await world.events.createBatch!(runId, [step('a'), step('c')]);
    expect(second.results.map((item) => item.status)).toEqual([409, 200]);
    expect(second.results[1]?.event?.eventId).toBe(slotToEventId(5));
    expect((await world.events.list({ runId })).data).toHaveLength(5);
  });

  it('creates a running step from a batched pair without dispatching it', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    const { results } = await world.events.createBatch!(runId, [
      { event: { eventType: 'step_created', correlationId: 'pair', eventData: { stepName: 'work', input: bytes('argument') } } },
      { event: { eventType: 'step_started', correlationId: 'pair' } },
    ]);
    expect(results.map((item) => item.status)).toEqual([200, 200]);
    expect((await world.steps.get(runId, 'pair')).status).toBe('running');
    const pending = await runInDurableObject(env.WORLD_STORE.getByName(storeShardFor(runId)), (_store, state) => state.storage.sql.exec(
      `SELECT slot FROM dispatch_outbox WHERE run_id = ? AND status = 'pending' AND slot = 3`, runId).toArray());
    expect(pending).toEqual([]);
  });

  it('rejects a batch the contract does not allow, writing none of it', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    const created = { event: { eventType: 'step_created' as const, correlationId: 'x', eventData: { stepName: 'work', input: bytes('a') } } };
    await expect(world.events.createBatch!(runId, [created, { event: { eventType: 'run_cancelled' } }])).rejects.toThrow('cannot be batched');
    await expect(world.events.createBatch!(runId, [created, created])).rejects.toThrow('one event per entity');
    expect((await world.events.list({ runId })).data).toHaveLength(2);
  });

  it('reports skipped events only to a writer at most OUT_OF_BAND_REPORT_LIMIT slots behind', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    const step = (correlationId: string) => ({
      eventType: 'step_created' as const, correlationId, eventData: { stepName: 'work', input: bytes('argument') },
    });
    for (let index = 1; index < OUT_OF_BAND_REPORT_LIMIT; index++) await world.events.create(runId, step(`step-${index}`));
    // Slots 2 to OUT_OF_BAND_REPORT_LIMIT + 1 were written after the writer's view.
    const behind = await world.events.create(runId, step('behind'), { eventCount: 1 });
    expect(behind.events).toHaveLength(OUT_OF_BAND_REPORT_LIMIT);
    expect(behind.events?.[0]?.eventId).toBe(slotToEventId(2));
    expect(behind.hasMore).toBe(false);
    // One slot further behind, the SDK would read the log anyway, so the write reads nothing for it.
    const further = await world.events.create(runId, step('further'), { eventCount: 1 });
    expect(further.event?.eventId).toBe(slotToEventId(OUT_OF_BAND_REPORT_LIMIT + 3));
    expect(further.events).toBeUndefined();
  });

  it("treats skip-step-inputs as all except step inputs, unlike none", async () => {
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
    if (step?.eventType === 'step_created') expect(step.eventData).toEqual({ stepName: 'work' });
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
    // The release reaches the token's shard just after the disposal, in the background.
    await expect.poll(() => world.hooks.getByToken(token).then(() => 'held', () => 'free')).toBe('free');
    expect((await world.events.create(contender, {
      eventType: 'hook_created', correlationId: 'new-owner', eventData: { token },
    })).hook?.runId).toBe(contender);
  });

  it('hides a retained hook once its retention ends and lets another run take its token', async () => {
    const owner = await newRun();
    const token = crypto.randomUUID();
    const hookId = `hook_${crypto.randomUUID()}`;
    await world.events.create(owner, { eventType: 'hook_created', correlationId: hookId, eventData: {
      token, tokenRetentionUntil: new Date(Date.now() + 500),
    } });
    await world.events.create(owner, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    expect((await world.hooks.get(hookId)).token).toBe(token);
    await new Promise((resolve) => setTimeout(resolve, 600));
    expect(HookNotFoundError.is(await world.hooks.get(hookId).catch((error: unknown) => error))).toBe(true);
    expect(HookNotFoundError.is(await world.hooks.getByToken(token).catch((error: unknown) => error))).toBe(true);
    expect((await world.hooks.list({ runId: owner })).data).toEqual([]);
    const claimer = await newRun();
    expect((await world.events.create(claimer, {
      eventType: 'hook_created', correlationId: `hook_${crypto.randomUUID()}`, eventData: { token },
    })).hook?.runId).toBe(claimer);
    expect((await world.hooks.getByToken(token)).runId).toBe(claimer);
  });

  it('cancels many runs with one outcome per run, in request order', async () => {
    const [pending, running, cancelled, completed] = await Promise.all([newRun(), newRun(), newRun(), newRun()]);
    await world.events.create(running, { eventType: 'run_started' });
    await world.events.create(cancelled, { eventType: 'run_cancelled' });
    await world.events.create(completed, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    const missing = `wrun_${crypto.randomUUID()}`;
    const result = await world.runs.cancelMany!({ runIds: [completed, pending, missing, cancelled, running], cancelReason: 'cleanup' });
    expect(result.results).toEqual([
      { runId: completed, outcome: 'not_cancellable', status: 'completed' },
      { runId: pending, outcome: 'cancelled' },
      { runId: missing, outcome: 'not_found' },
      { runId: cancelled, outcome: 'already_cancelled' },
      { runId: running, outcome: 'cancelled' },
    ]);
    expect(result.summary).toEqual({ requested: 5, cancelled: 2, alreadyCancelled: 1, notCancellable: 1, notFound: 1, failed: 0 });
    expect((await world.runs.get(running)).status).toBe('cancelled');
    expect((await world.events.list({ runId: running })).data.at(-1)).toMatchObject({
      eventType: 'run_cancelled', eventData: { cancelReason: 'cleanup' },
    });
    // More runs than are cancelled at once keep their order.
    const many = await Promise.all(Array.from({ length: 25 }, () => newRun()));
    expect((await world.runs.cancelMany!({ runIds: many })).results).toEqual(many.map((runId) => ({ runId, outcome: 'cancelled' })));
    await expect(world.runs.cancelMany!({ runIds: [] })).rejects.toThrow();
    await expect(world.runs.cancelMany!({ runIds: [pending, pending] })).rejects.toThrow('duplicates');
  });

  it("stores a dynamic run's uploaded code with the run, and refuses a ref outside the run", async () => {
    const runId = `wrun_${crypto.randomUUID()}`;
    const ref = await world.uploadDynamicWorkflowCode!(runId, { workflowName: 'dynamic', code: bytes('code') });
    const eventData = { deploymentId: 'test-v1', workflowName: 'dynamic', input: bytes('in') };
    const stolen = await world.events.create(`wrun_${crypto.randomUUID()}`, {
      eventType: 'run_created', eventData: { ...eventData, dynamicWorkflowCodeRef: ref },
    }).catch((error: unknown) => error);
    expect(stolen).toMatchObject({ status: 400 });
    const created = await world.events.create(runId, { eventType: 'run_created', eventData: { ...eventData, dynamicWorkflowCodeRef: ref } });
    expect(created.run.dynamicWorkflowCode).toEqual(bytes('code'));
    expect((await world.runs.get(runId)).dynamicWorkflowCode).toEqual(bytes('code'));
    expect((await world.events.list({ runId })).data[0]).not.toHaveProperty('eventData.dynamicWorkflowCodeRef');
    expect(() => world.validateRunExecutionContext!({ big: 'x'.repeat(70_000) })).toThrow('limit');
  });

  it('advertises exactly the capabilities it implements', () => {
    expect(world.capabilities).toEqual({
      hookRetention: { active: true }, atomicStartHook: { active: true }, hookResumeDedup: true, resolvedHookResume: true, inBandFence: true, maxConcurrency: true,
      hookForceClaim: true, dynamicWorkflowCode: true, deploymentAffinity: true, invoke: true,
    });
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
    // A repeat may carry a different payload; a key always belongs to one run.
    expect((await world.queue(name, { ...message, requestedAt: new Date() }, { ...options, delaySeconds: 3 })).messageId)
      .toBe(first.messageId);
    await runDurableObjectAlarm(env.QUEUE_SHARD.getByName(queueShardFor(message.runId, message)));
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
