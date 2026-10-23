import { env, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { WorkflowRunNotFoundError } from '@workflow/errors';
import { createWorld, type WorldEnv } from '@workflow/world-cloudflare';
import type { WorldStore } from '../packages/world-cloudflare/src/store/world-store.js';

const world = createWorld(env);
const bytes = (text: string) => new TextEncoder().encode(text);
const directory = () => env.WORLD_STORE.getByName('global');
const runCreated = (workflowName: string) => ({
  eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName, input: bytes('input') },
} as const);

async function newRun(workflowName = `shard-${crypto.randomUUID()}`) {
  return (await world.events.create(null, runCreated(workflowName))).run.runId;
}

const countRows = (stub: DurableObjectStub<WorldStore>, table: string, runId: string) =>
  runInDurableObject(stub, (_store, state) =>
    state.storage.sql.exec<{ count: number }>(`SELECT COUNT(*) AS count FROM ${table} WHERE run_id = ?`, runId).one().count);

const indexedStatus = async (workflowName: string) => (await world.runs.list({ workflowName })).data.map((run) => run.status);

describe('per-run stores', () => {
  it('keeps a run in its own store, with only an index copy in the directory', async () => {
    const runId = await newRun();
    await world.events.create(runId, { eventType: 'run_started' });
    await world.events.createBatch!(runId, [
      { event: { eventType: 'step_created', correlationId: 'a', eventData: { stepName: 'work', input: bytes('a') } } },
      { event: { eventType: 'step_started', correlationId: 'a' } },
    ]);
    expect(await countRows(env.WORLD_STORE.getByName(runId), 'events', runId)).toBe(4);
    expect(await countRows(env.WORLD_STORE.getByName(runId), 'steps', runId)).toBe(1);
    expect(await countRows(directory(), 'events', runId)).toBe(0);
    expect(await countRows(directory(), 'steps', runId)).toBe(0);
    // Step events leave the run row alone, so the copy is as of run_started.
    const copy = await runInDurableObject(directory(), (_store, state) =>
      state.storage.sql.exec('SELECT status, index_slot FROM runs WHERE id = ?', runId).one());
    expect(copy).toEqual({ status: 'running', index_slot: 2 });
    // The index copy takes no writes.
    const completed = { eventType: 'run_completed', eventData: { output: bytes('x') } } as const;
    const refused = await Promise.resolve(directory().createEvent(runId, completed)).then(() => null, (error: unknown) => error);
    expect(String(refused)).toContain('lives in its own store');
  });

  it('lists sharded runs and gets many with their current status', async () => {
    const name = `shard-list-${crypto.randomUUID()}`;
    const runId = await newRun(name);
    expect(await indexedStatus(name)).toEqual(['pending']);
    await world.events.create(runId, { eventType: 'run_started' });
    expect(await indexedStatus(name)).toEqual(['running']);
    await world.events.create(runId, { eventType: 'attr_set', eventData: {
      changes: [{ key: 'phase', value: 'late' }], writer: { type: 'workflow' },
    } });
    await world.events.create(runId, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    expect(await indexedStatus(name)).toEqual(['completed']);
    const [run, missing] = await world.runs.getMany!([runId, 'wrun_missing']);
    expect(run).toMatchObject({ runId, status: 'completed', output: bytes('done'), attributes: { phase: 'late' } });
    expect(missing).toBeNull();
  });

  it('keeps hook tokens unique across run stores and reads hooks from any run', async () => {
    const [first, second] = await Promise.all([newRun(), newRun()]);
    const token = crypto.randomUUID();
    const hookId = `hook-${crypto.randomUUID()}`;
    const created = await world.events.create(first, { eventType: 'hook_created', correlationId: hookId, eventData: { token } });
    expect(created.event?.eventType).toBe('hook_created');
    const conflict = await world.events.create(second, {
      eventType: 'hook_created', correlationId: `hook-${crypto.randomUUID()}`, eventData: { token },
    });
    expect(conflict.event).toMatchObject({ eventType: 'hook_conflict', eventData: { token, conflictingRunId: first } });
    expect(await world.hooks.getByToken(token)).toEqual(created.hook);
    expect(await world.hooks.get(hookId)).toEqual(created.hook);
    expect((await world.hooks.list({ runId: first })).data).toEqual([created.hook]);
    expect((await world.hooks.list({ runId: second })).data).toEqual([]);
    // A retry of the same hook after a crash between claim and write is granted again.
    expect(await directory().claimHookToken(created.hook!)).toBeNull();
  });

  it('reclaims a token once its holder finishes and its retention lapses', async () => {
    const [holder, claimant] = await Promise.all([newRun(), newRun()]);
    const token = crypto.randomUUID();
    await world.events.create(holder, { eventType: 'hook_created', correlationId: `hook-${crypto.randomUUID()}`, eventData: {
      token, tokenRetentionUntil: new Date(Date.now() + 500),
    } });
    await world.events.create(holder, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    const early = await world.events.create(claimant, {
      eventType: 'hook_created', correlationId: `hook-${crypto.randomUUID()}`, eventData: { token },
    });
    expect(early.event).toMatchObject({ eventType: 'hook_conflict', eventData: { conflictingRunId: holder } });
    await new Promise((resolve) => setTimeout(resolve, 600));
    const late = await world.events.create(claimant, {
      eventType: 'hook_created', correlationId: `hook-${crypto.randomUUID()}`, eventData: { token },
    });
    expect(late.event?.eventType).toBe('hook_created');
    expect((await world.hooks.getByToken(token)).runId).toBe(claimant);
  });

  it('still reads and writes a legacy run the directory holds', async () => {
    const name = `legacy-${crypto.randomUUID()}`;
    const runId = `wrun_legacy_${crypto.randomUUID()}`;
    await directory().createEvent(runId, runCreated(name));
    expect((await world.runs.get(runId)).status).toBe('pending');
    // A lazy start that carries the run's fields must not create a second copy in a run store.
    await world.events.create(runId, { eventType: 'run_started', eventData: {
      deploymentId: 'test-v1', workflowName: name, input: bytes('input'),
    } });
    await world.events.create(runId, {
      eventType: 'step_created', correlationId: 'step', eventData: { stepName: 'work', input: bytes('a') },
    });
    const token = crypto.randomUUID();
    await world.events.create(runId, {
      eventType: 'hook_created', correlationId: `hook-${crypto.randomUUID()}`, eventData: { token },
    });
    expect((await world.events.list({ runId })).data.map((event) => event.eventType))
      .toEqual(['run_created', 'run_started', 'step_created', 'hook_created']);
    expect((await world.steps.get(runId, 'step')).status).toBe('pending');
    expect(await indexedStatus(name)).toEqual(['running']);
    expect(await countRows(env.WORLD_STORE.getByName(runId), 'events', runId)).toBe(0);
    // Tokens are unique across legacy and sharded runs.
    const sharded = await newRun();
    const conflict = await world.events.create(sharded, {
      eventType: 'hook_created', correlationId: `hook-${crypto.randomUUID()}`, eventData: { token },
    });
    expect(conflict.event).toMatchObject({ eventType: 'hook_conflict', eventData: { conflictingRunId: runId } });
  });

  it('refuses run-scoped calls in a run store for a run it does not hold', async () => {
    const stub = env.WORLD_STORE.getByName('wrun_elsewhere');
    const calls = [
      () => stub.getRun('wrun_elsewhere'), () => stub.getStep('wrun_elsewhere', 'step'),
      () => stub.listSteps({ runId: 'wrun_elsewhere' }), () => stub.listEvents({ runId: 'wrun_elsewhere' }),
      () => stub.listHooks({ runId: 'wrun_elsewhere' }), () => stub.pendingRunStartSlot('wrun_elsewhere'),
      () => stub.reopenPublishedHookWakes('wrun_elsewhere'), () => stub.createEvent('wrun_elsewhere', { eventType: 'run_started' }),
      () => stub.createEvent('wrun_other', runCreated('misrouted')),
    ];
    for (const call of calls) {
      const error = await Promise.resolve(call()).then(() => null, (reason: unknown) => reason);
      expect(WorkflowRunNotFoundError.is(error)).toBe(true);
    }
  });

  it('heals a stale run index after a failed push', async () => {
    const name = `shard-heal-${crypto.randomUUID()}`;
    const runId = await newRun(name);
    const stub = env.WORLD_STORE.getByName(runId);
    await runInDurableObject(stub, async (store: WorldStore) => {
      // SAFETY: `env` is the Durable Object's own binding record; the test swaps in a failing directory.
      const self = store as unknown as { env: WorldEnv };
      const real = self.env;
      const down = { getByName: () => ({ indexRun: async () => { throw new Error('directory down'); } }) };
      self.env = { ...real, WORLD_STORE: down as unknown as WorldEnv['WORLD_STORE'] };
      try {
        expect((await store.createEvent(runId, { eventType: 'run_started' })).run?.status).toBe('running');
      } finally {
        self.env = real;
      }
    });
    expect(await indexedStatus(name)).toEqual(['pending']);
    expect(await countRows(stub, 'directory_outbox', runId)).toBe(1);
    // The run store's alarm retries the push after its backoff.
    await expect.poll(() => indexedStatus(name), { timeout: 5_000 }).toEqual(['running']);
    expect(await countRows(stub, 'directory_outbox', runId)).toBe(0);
  });
});
