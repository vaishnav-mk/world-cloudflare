import { env, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { HookNotFoundError, WorkflowRunNotFoundError } from '@workflow/errors';
import type { Hook } from '@workflow/world';
import { createWorld, type WorldEnv } from '@workflow/world-cloudflare';
import { encode } from '../packages/world-cloudflare/src/lib/codec.js';
import { storeShardFor } from '../packages/world-cloudflare/src/store/route.js';
import type { WorldStore } from '../packages/world-cloudflare/src/store/world-store.js';

const world = createWorld(env);
const bytes = (text: string) => new TextEncoder().encode(text);
const legacy = () => env.WORLD_STORE.getByName('global');
const shard = (key: string) => env.WORLD_STORE.getByName(storeShardFor(key));
const runCreated = (workflowName: string) => ({
  eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName, input: bytes('input') },
} as const);
const completed = { eventType: 'run_completed', eventData: { output: bytes('done') } } as const;
const hookCreated = (token: string, extra: { tokenRetentionUntil?: Date } = {}) => ({
  eventType: 'hook_created', correlationId: `hook-${crypto.randomUUID()}`, eventData: { token, ...extra },
} as const);

async function newRun(workflowName = `shard-${crypto.randomUUID()}`) {
  return (await world.events.create(null, runCreated(workflowName))).run;
}

/** Two new runs on different shards. */
async function runsOnTwoShards() {
  const first = (await newRun()).runId;
  for (;;) {
    const second = (await newRun()).runId;
    if (storeShardFor(second) !== storeShardFor(first)) return [first, second];
  }
}

/** A token whose shard is not `key`'s. */
function tokenAwayFrom(key: string): string {
  for (;;) {
    const token = crypto.randomUUID();
    if (storeShardFor(token) !== storeShardFor(key)) return token;
  }
}

const count = (stub: DurableObjectStub<WorldStore>, query: string, ...args: string[]) =>
  runInDurableObject(stub, (_store, state) => state.storage.sql.exec<{ count: number }>(query, ...args).one().count);

const isFree = (token: string) => world.hooks.getByToken(token).then(() => false, (error: unknown) => HookNotFoundError.is(error));

/** Every page of a list, following its cursor. */
async function allPages<T>(list: (cursor: string | undefined) => Promise<{ data: T[]; cursor: string | null; hasMore: boolean }>) {
  const pages: { data: T[]; hasMore: boolean }[] = [];
  let cursor: string | undefined;
  do {
    const page = await list(cursor);
    pages.push(page);
    cursor = page.cursor ?? undefined;
    expect(page.cursor === null).toBe(!page.hasMore);
  } while (cursor !== undefined);
  return pages;
}

describe('store shards', () => {
  it('keeps a run on its shard, with nothing in the legacy store', async () => {
    const name = `shard-${crypto.randomUUID()}`;
    const { runId } = await newRun(name);
    await world.events.create(runId, { eventType: 'run_started' });
    await world.events.createBatch!(runId, [
      { event: { eventType: 'step_created', correlationId: 'a', eventData: { stepName: 'work', input: bytes('a') } } },
      { event: { eventType: 'step_started', correlationId: 'a' } },
    ]);
    expect(await count(shard(runId), 'SELECT COUNT(*) AS count FROM events WHERE run_id = ?', runId)).toBe(4);
    expect(await count(shard(runId), 'SELECT COUNT(*) AS count FROM steps WHERE run_id = ?', runId)).toBe(1);
    expect(await count(legacy(), 'SELECT COUNT(*) AS count FROM runs WHERE id = ?', runId)).toBe(0);
    // Lists read the shards, so they show a change as soon as it commits.
    expect((await world.runs.list({ workflowName: name })).data.map((run) => run.status)).toEqual(['running']);
    await world.events.create(runId, completed);
    expect((await world.runs.list({ workflowName: name })).data.map((run) => run.status)).toEqual(['completed']);
    // It owes nothing more, so its store drops the recovery alarm instead of waking for it.
    expect(await runInDurableObject(shard(runId), (_store, state) => state.storage.getAlarm())).toBeNull();
  });

  it('pages runs across shards in sort order, with status filters', async () => {
    const name = `shard-page-${crypto.randomUUID()}`;
    const runs = [];
    for (let index = 0; index < 7; index++) runs.push(await newRun(name));
    expect(new Set(runs.map(({ runId }) => storeShardFor(runId))).size).toBeGreaterThan(1);
    const started = [runs[1], runs[4], runs[6]].map(({ runId }) => runId);
    for (const runId of started) await world.events.create(runId, { eventType: 'run_started' });
    const ascending = [...runs].sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime() || (a.runId < b.runId ? -1 : 1))
      .map(({ runId }) => runId);

    const newestFirst = await allPages((cursor) => world.runs.list({ workflowName: name, pagination: { limit: 3, cursor } }));
    expect(newestFirst.map((page) => [page.data.length, page.hasMore])).toEqual([[3, true], [3, true], [1, false]]);
    expect(newestFirst.flatMap((page) => page.data.map((run) => run.runId))).toEqual([...ascending].reverse());

    const oldestFirst = await allPages((cursor) =>
      world.runs.list({ workflowName: name, pagination: { limit: 4, cursor, sortOrder: 'asc' } }));
    expect(oldestFirst.flatMap((page) => page.data.map((run) => run.runId))).toEqual(ascending);

    const running = await allPages((cursor) =>
      world.runs.list({ workflowName: name, status: 'running', pagination: { limit: 2, cursor }, resolveData: 'none' }));
    expect(running.map((page) => page.data.length)).toEqual([2, 1]);
    expect(running.flatMap((page) => page.data.map((run) => run.runId)))
      .toEqual(ascending.filter((runId) => started.includes(runId)).reverse());
    expect(running[0].data[0].input).toBeUndefined();
    const pending = await world.runs.list({ workflowName: name, status: ['pending'], pagination: { limit: 10 } });
    expect(pending.data.map((run) => run.runId).sort()).toEqual(ascending.filter((runId) => !started.includes(runId)).sort());
    expect(pending.hasMore).toBe(false);
  });

  it('gets many runs across shards and the legacy store', async () => {
    const [first, second] = await runsOnTwoShards();
    const legacyRun = `wrun_legacy_${crypto.randomUUID()}`;
    await legacy().createEvent(legacyRun, runCreated('legacy'));
    const runs = await world.runs.getMany!([second, 'wrun_missing', legacyRun, first, second]);
    expect(runs.map((run) => run?.runId ?? null)).toEqual([second, null, legacyRun, first, second]);
  });

  it('keeps hook tokens unique on their token shard, and reads them from any store', async () => {
    const [first, second] = await runsOnTwoShards();
    const token = crypto.randomUUID();
    const created = await world.events.create(first, hookCreated(token));
    const hook = created.hook!;
    expect(created.event?.eventType).toBe('hook_created');
    const conflict = await world.events.create(second, hookCreated(token));
    expect(conflict.event).toMatchObject({ eventType: 'hook_conflict', eventData: { token, conflictingRunId: first } });
    // The registry row lives on the token's shard; the legacy store keeps nothing.
    expect(await count(shard(token), 'SELECT COUNT(*) AS count FROM tokens WHERE token = ? AND hook_id = ?', token, hook.hookId)).toBe(1);
    expect(await count(legacy(), 'SELECT COUNT(*) AS count FROM hooks WHERE token = ?', token)).toBe(0);

    expect(await world.hooks.getByToken(token)).toEqual(hook);
    const elsewhere = env.WORLD_STORE.getByName(storeShardFor(tokenAwayFrom(token)));
    expect(await elsewhere.getHookByToken(token)).toEqual(hook);
    expect(await legacy().getHookByToken(token)).toEqual(hook);
    expect(await world.hooks.get(hook.hookId)).toEqual(hook);
    expect((await world.hooks.list({ runId: first })).data).toEqual([hook]);
    expect((await world.hooks.list({ runId: second })).data).toEqual([]);
    // A retry of the same hook after a crash between claim and write is granted again, also through an older caller's route.
    expect(await shard(token).claimHookToken(hook)).toBeNull();
    expect(await legacy().claimHookToken(hook)).toBeNull();
  });

  it('frees a token after disposal or its run finishing, and a retained one once it lapses', async () => {
    const [holder, claimant] = await runsOnTwoShards();
    const disposed = crypto.randomUUID();
    const finished = crypto.randomUUID();
    const retained = crypto.randomUUID();
    const disposable = await world.events.create(holder, hookCreated(disposed));
    await world.events.create(holder, hookCreated(finished));
    await world.events.create(holder, hookCreated(retained, { tokenRetentionUntil: new Date(Date.now() + 1_000) }));
    await world.events.create(holder, { eventType: 'hook_disposed', correlationId: disposable.hook!.hookId });
    await world.events.create(holder, completed);
    // Releases reach the token shards just after the writes, in the background.
    await expect.poll(() => isFree(disposed)).toBe(true);
    await expect.poll(() => isFree(finished)).toBe(true);
    expect((await world.events.create(claimant, hookCreated(disposed))).event?.eventType).toBe('hook_created');
    expect((await world.events.create(claimant, hookCreated(finished))).event?.eventType).toBe('hook_created');
    const early = await world.events.create(claimant, hookCreated(retained));
    expect(early.event).toMatchObject({ eventType: 'hook_conflict', eventData: { conflictingRunId: holder } });
    expect((await world.hooks.getByToken(retained)).runId).toBe(holder);
    await new Promise((resolve) => setTimeout(resolve, 1_100));
    const late = await world.events.create(claimant, hookCreated(retained));
    expect(late.event?.eventType).toBe('hook_created');
    expect((await world.hooks.getByToken(retained)).runId).toBe(claimant);
  });

  it('lets a run claim again a token it just disposed of', async () => {
    const { runId } = await newRun();
    const token = tokenAwayFrom(runId);
    const first = await world.events.create(runId, hookCreated(token));
    await world.events.create(runId, { eventType: 'hook_disposed', correlationId: first.hook!.hookId });
    const again = await world.events.create(runId, hookCreated(token));
    expect(again.event?.eventType).toBe('hook_created');
    expect(await world.hooks.getByToken(token)).toEqual(again.hook);
  });

  it('lists hooks across every store, page by page', async () => {
    const [first, second] = await runsOnTwoShards();
    const third = (await newRun()).runId;
    const ours = new Set<string>();
    for (const runId of [first, second, third, first]) ours.add((await world.events.create(runId, hookCreated(crypto.randomUUID()))).hook!.hookId);
    const pages = await allPages((cursor) => world.hooks.list({ pagination: { limit: 2, cursor, sortOrder: 'asc' } }));
    const listed = pages.flatMap((page) => page.data);
    expect(pages.length).toBeGreaterThan(1);
    expect(new Set(listed.map((hook) => hook.hookId)).size).toBe(listed.length);
    const keys = listed.map((hook) => [hook.createdAt.getTime(), hook.hookId] as const);
    expect(keys).toEqual([...keys].sort((a, b) => a[0] - b[0] || (a[1] < b[1] ? -1 : 1)));
    expect(listed.filter((hook) => ours.has(hook.hookId))).toHaveLength(4);
  });

  it('still lists, reads and holds the token of a legacy run', async () => {
    const name = `legacy-${crypto.randomUUID()}`;
    const runId = `wrun_legacy_${crypto.randomUUID()}`;
    await legacy().createEvent(runId, runCreated(name));
    // A hook the legacy store granted itself before token shards, which no token shard has seen yet.
    const token = tokenAwayFrom(runId);
    const createdAt = new Date();
    const oldHook: Hook = {
      runId, hookId: `hook-${crypto.randomUUID()}`, token, ownerId: 'cloudflare', projectId: 'cloudflare',
      environment: 'cloudflare', isWebhook: true, isSystem: false, specVersion: 2, createdAt,
    };
    await runInDurableObject(legacy(), (_store, state) => {
      state.storage.sql.exec('INSERT INTO hooks (id, run_id, token, data, created_at) VALUES (?, ?, ?, ?, ?)',
        oldHook.hookId, runId, token, encode(oldHook), createdAt.getTime());
    });
    await runInDurableObject(shard(token), (_store, state) => { state.storage.kv.delete('legacy-tokens-imported'); });

    expect((await world.runs.list({ workflowName: name })).data.map((run) => run.runId)).toEqual([runId]);
    expect((await world.runs.get(runId)).status).toBe('pending');
    // A lazy start that carries the run's fields must not create a second copy on a shard.
    await world.events.create(runId, { eventType: 'run_started', eventData: {
      deploymentId: 'test-v1', workflowName: name, input: bytes('input'),
    } });
    await world.events.create(runId, {
      eventType: 'step_created', correlationId: 'step', eventData: { stepName: 'work', input: bytes('a') },
    });
    expect((await world.steps.get(runId, 'step')).status).toBe('pending');
    expect(await count(shard(runId), 'SELECT COUNT(*) AS count FROM events WHERE run_id = ?', runId)).toBe(0);

    // The old hook still holds its token against a sharded run, and reads from its token's shard.
    expect(await world.hooks.getByToken(token)).toEqual(oldHook);
    const sharded = (await newRun()).runId;
    expect((await world.events.create(sharded, hookCreated(token))).event)
      .toMatchObject({ eventType: 'hook_conflict', eventData: { conflictingRunId: runId } });
    // A new legacy hook claims its token on the token's shard, like any other.
    const newToken = crypto.randomUUID();
    const newHook = (await world.events.create(runId, hookCreated(newToken))).hook!;
    expect(await count(shard(newToken), 'SELECT COUNT(*) AS count FROM tokens WHERE token = ?', newToken)).toBe(1);
    expect((await world.events.create(sharded, hookCreated(newToken))).event)
      .toMatchObject({ eventType: 'hook_conflict', eventData: { conflictingRunId: runId } });
    expect((await world.hooks.list({ runId, pagination: { sortOrder: 'asc' } })).data.map((hook) => hook.hookId))
      .toEqual([oldHook.hookId, newHook.hookId]);
    expect((await world.runs.list({ workflowName: name })).data.map((run) => run.status)).toEqual(['running']);
    const listed = await allPages((cursor) => world.hooks.list({ pagination: { limit: 1000, cursor } }));
    expect(listed.flatMap((page) => page.data).map((hook) => hook.hookId)).toEqual(expect.arrayContaining([oldHook.hookId]));

    // The legacy run finishing releases both tokens on their shards.
    await world.events.create(runId, completed);
    await expect.poll(() => isFree(token)).toBe(true);
    await expect.poll(() => isFree(newToken)).toBe(true);
    expect((await world.events.create(sharded, hookCreated(token))).event?.eventType).toBe('hook_created');
  });

  it('refuses run-scoped calls in a shard for a run it does not hold', async () => {
    const stub = shard('wrun_elsewhere');
    const calls = [
      () => stub.getRun('wrun_elsewhere'), () => stub.getStep('wrun_elsewhere', 'step'),
      () => stub.listSteps({ runId: 'wrun_elsewhere' }), () => stub.listEvents({ runId: 'wrun_elsewhere' }),
      () => stub.listHooks({ runId: 'wrun_elsewhere' }), () => stub.pendingHookWakeSlot('wrun_elsewhere'),
      () => stub.reopenPublishedHookWakes('wrun_elsewhere'), () => stub.createEvent('wrun_elsewhere', { eventType: 'run_started' }),
      () => stub.createEvent('wrun_other', runCreated('misrouted')),
    ];
    for (const call of calls) {
      const error = await Promise.resolve(call()).then(() => null, (reason: unknown) => reason);
      expect(WorkflowRunNotFoundError.is(error)).toBe(true);
    }
    // `start()` publishes the start message while `run_created` may still be in flight.
    expect(await stub.pendingRunStartSlot('wrun_elsewhere')).toBe(0);
  });

  it('retries a token release after a failed push', async () => {
    const { runId } = await newRun();
    const token = tokenAwayFrom(runId);
    const { hook } = await world.events.create(runId, hookCreated(token));
    const stub = shard(runId);
    await runInDurableObject(stub, async (store: WorldStore) => {
      // SAFETY: `env` is the Durable Object's own binding record; the test swaps in a failing token shard.
      const self = store as unknown as { env: WorldEnv };
      const real = self.env;
      const down = { getByName: () => ({ releaseTokens: async () => { throw new Error('token shard down'); } }) };
      self.env = { ...real, WORLD_STORE: down as unknown as WorldEnv['WORLD_STORE'] };
      try {
        await store.createEvent(runId, { eventType: 'hook_disposed', correlationId: hook!.hookId });
      } finally {
        self.env = real;
      }
    });
    await expect.poll(() => count(stub, 'SELECT COUNT(*) AS count FROM token_releases WHERE token = ? AND attempts = 1', token)).toBe(1);
    expect(await isFree(token)).toBe(false);
    // The shard's alarm retries the release after its backoff.
    await expect.poll(() => isFree(token), { timeout: 5_000 }).toBe(true);
    expect(await count(stub, 'SELECT COUNT(*) AS count FROM token_releases WHERE token = ?', token)).toBe(0);
  });
});
