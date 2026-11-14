import { env, runInDurableObject } from 'cloudflare:test';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { HookNotFoundError } from '@workflow/errors';
import { startHookId, type Hook, type WorkflowRun } from '@workflow/world';
import { createWorld, type WorldEnv } from '@workflow/world-cloudflare';
import { INDEX_BACKFILL_BATCH } from '../packages/world-cloudflare/src/constants.js';
import { encode } from '../packages/world-cloudflare/src/lib/codec.js';
import { hookIndexShards, indexShardFor, indexShardNames, storeShardFor } from '../packages/world-cloudflare/src/lib/shards.js';
import type { WorldStore } from '../packages/world-cloudflare/src/store/world-store.js';
import { predateIndex } from './backfill.js';
import { newRunId } from './ids.js';

const world = createWorld(env);
const bytes = (text: string) => new TextEncoder().encode(text);
const store = (name: string) => env.WORLD_STORE.getByName(name);
/** Before the second epoch in wrangler.test.jsonc, so a run with this ULID time lives in the first. */
const FIRST_EPOCH = 1_600_000_000_000;
const runCreated = (workflowName: string) => ({
  eventType: 'run_created', eventData: { deploymentId: 'test-v1', workflowName, input: bytes('input') },
} as const);
const hookCreated = (hookId: string, extra: { tokenRetentionUntil?: Date } = {}) => ({
  eventType: 'hook_created', correlationId: hookId, eventData: { token: crypto.randomUUID(), ...extra },
} as const);
const completed = { eventType: 'run_completed', eventData: { output: bytes('done') } } as const;

/** A World whose store calls are recorded by object name. */
function watched() {
  const touched = new Set<string>();
  // SAFETY: the World's storage only calls `getByName` on its namespace.
  const stores = { getByName: (name: string) => {
    touched.add(name);
    return env.WORLD_STORE.getByName(name);
  } } as unknown as WorldEnv['WORLD_STORE'];
  return { world: createWorld({ ...env, WORLD_STORE: stores }), touched };
}

/** A run in the first epoch (an old ULID time), or in the second (minted now). */
async function newRun(workflowName: string, epoch: 'first' | 'second'): Promise<WorkflowRun> {
  const runId = epoch === 'first' ? newRunId(FIRST_EPOCH) : null;
  return (await world.events.create(runId, runCreated(workflowName))).run;
}

/** Every page of a list, following its cursor. */
async function allPages<T>(list: (cursor: string | undefined) => Promise<{ data: T[]; cursor: string | null; hasMore: boolean }>) {
  const pages: T[][] = [];
  let cursor: string | undefined;
  do {
    const page = await list(cursor);
    pages.push(page.data);
    cursor = page.cursor ?? undefined;
  } while (cursor !== undefined);
  return pages;
}

const newestFirst = (runs: { runId: string; createdAt: Date }[]) => [...runs]
  .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime() || (a.runId < b.runId ? 1 : -1)).map(({ runId }) => runId);

const count = (name: string, query: string, ...args: (string | number)[]) =>
  runInDurableObject(store(name), (_store, state) => state.storage.sql.exec<{ count: number }>(query, ...args).one().count);

/** How many of this workflow's runs the index shards hold. */
async function indexed(workflowName: string): Promise<number> {
  let total = 0;
  for (const name of indexShardNames()) total += await count(name, 'SELECT COUNT(*) AS count FROM index_runs WHERE workflow_name = ?', workflowName);
  return total;
}

describe('list index', () => {
  beforeAll(async () => {
    // A fresh deployment's first list asks every store, which wakes each to report to its index shards.
    await world.runs.list({ pagination: { limit: 1 } });
    await vi.waitFor(async () => {
      const { world: reader, touched } = watched();
      await reader.runs.list({ pagination: { limit: 1 }, resolveData: 'none' });
      expect([...touched].every((name) => name.startsWith('index-'))).toBe(true);
    });
  });

  it('lists runs of both epochs from the index shards, in order, by page and filter', async () => {
    const name = `index-${crypto.randomUUID()}`;
    const runs: WorkflowRun[] = [];
    for (let index = 0; index < 14; index++) runs.push(await newRun(name, index % 2 ? 'first' : 'second'));
    expect(new Set(runs.map(({ runId }) => indexShardFor(runId))).size).toBeGreaterThan(2);
    expect(new Set(runs.map(({ runId }) => storeShardFor(runId).startsWith('store-e'))).size).toBe(2);
    const started = runs.filter((_, index) => index % 3 === 0).map(({ runId }) => runId);
    const finished = started.slice(0, 2);
    for (const runId of started) await world.events.create(runId, { eventType: 'run_started' });
    for (const runId of finished) await world.events.create(runId, completed);
    const order = newestFirst(runs);
    const statusOf = (runId: string) => finished.includes(runId) ? 'completed' : started.includes(runId) ? 'running' : 'pending';

    const { world: reader, touched } = watched();
    const pages = await allPages((cursor) => reader.runs.list({ workflowName: name, pagination: { limit: 4, cursor }, resolveData: 'none' }));
    expect(pages.map((page) => page.length)).toEqual([4, 4, 4, 2]);
    expect(pages.flat().map(({ runId }) => runId)).toEqual(order);
    expect(pages.flat().every(({ input }) => input === undefined)).toBe(true);
    // Without payloads, the index answers alone.
    expect([...touched].sort()).toEqual(indexShardNames().sort());

    const ascending = await allPages((cursor) => world.runs.list({ workflowName: name, pagination: { limit: 5, cursor, sortOrder: 'asc' } }));
    expect(ascending.flat().map(({ runId }) => runId)).toEqual([...order].reverse());
    expect(ascending.flat().every(({ input }) => input instanceof Uint8Array)).toBe(true);

    const ids = async (status: 'running' | 'completed' | ('pending' | 'completed')[]) =>
      (await world.runs.list({ workflowName: name, status })).data.map(({ runId }) => runId);
    expect(await ids('running')).toEqual(order.filter((runId) => statusOf(runId) === 'running'));
    expect(await ids('completed')).toEqual(order.filter((runId) => statusOf(runId) === 'completed'));
    expect(await ids(['pending', 'completed'])).toEqual(order.filter((runId) => statusOf(runId) !== 'running'));
    expect((await world.runs.list({ workflowName: name, status: [] })).data).toEqual([]);

    // With payloads, the page's runs come from their own stores, and only theirs.
    touched.clear();
    const page = await reader.runs.list({ workflowName: name, pagination: { limit: 3 } });
    expect(page.data.map(({ runId }) => runId)).toEqual(order.slice(0, 3));
    expect(page.data.map((run) => run.status)).toEqual(order.slice(0, 3).map(statusOf));
    const stores = [...touched].filter((touchedName) => !touchedName.startsWith('index-'));
    expect(new Set(stores)).toEqual(new Set(order.slice(0, 3).map(storeShardFor)));
  });

  it('shows a run whose index push failed once the alarm pushes it again', async () => {
    const name = `index-lag-${crypto.randomUUID()}`;
    const runId = newRunId();
    await runInDurableObject(store(storeShardFor(runId)), async (instance: WorldStore) => {
      // SAFETY: `env` is the Durable Object's own binding record; the test swaps in failing index shards.
      const self = instance as unknown as { env: WorldEnv };
      const real = self.env;
      const stores = { getByName: (shard: string) => shard.startsWith('index-')
        ? { indexChanges: async () => { throw new Error('index shard down'); } }
        : real.WORLD_STORE.getByName(shard) };
      self.env = { ...real, WORLD_STORE: stores as unknown as WorldEnv['WORLD_STORE'] };
      try {
        await instance.createEvent(runId, runCreated(name));
      } finally {
        self.env = real;
      }
    });
    // The write committed, and its list row is owed with backoff.
    expect((await world.runs.get(runId)).workflowName).toBe(name);
    expect(await count(storeShardFor(runId), 'SELECT COUNT(*) AS count FROM index_outbox WHERE run_id = ? AND attempts = 1', runId)).toBe(1);
    expect((await world.runs.list({ workflowName: name })).data).toEqual([]);
    // The alarm retries it after its backoff.
    await expect.poll(async () => (await world.runs.list({ workflowName: name })).data.map((run) => run.runId), { timeout: 5_000 })
      .toEqual([runId]);
    expect(await count(storeShardFor(runId), 'SELECT COUNT(*) AS count FROM index_outbox WHERE run_id = ?', runId)).toBe(0);
    // A later change goes out with the write again.
    await world.events.create(runId, { eventType: 'run_started' });
    expect((await world.runs.list({ workflowName: name, status: 'running', resolveData: 'none' })).data.map((run) => run.runId)).toEqual([runId]);
  });

  it('gets hooks by ID and lists them across stores from the index', async () => {
    const [first, second] = [await newRun('index-hooks', 'first'), await newRun('index-hooks', 'second')];
    const kept = `hook_${crypto.randomUUID()}`;
    const disposed = `hook_${crypto.randomUUID()}`;
    const retained = `hook_${crypto.randomUUID()}`;
    const keptHook = (await world.events.create(first.runId, hookCreated(kept))).hook as Hook;
    await world.events.create(second.runId, hookCreated(disposed));
    await world.events.create(second.runId, hookCreated(retained, { tokenRetentionUntil: new Date(Date.now() + 1_000) }));

    const { world: reader, touched } = watched();
    expect(await reader.hooks.get(kept)).toEqual(keptHook);
    // A hook ID does not name its run: one index shard per epoch.
    expect([...touched].sort()).toEqual(hookIndexShards(kept).sort());
    expect(hookIndexShards(kept)).toHaveLength(2);

    await world.events.create(second.runId, { eventType: 'hook_disposed', correlationId: disposed });
    expect(HookNotFoundError.is(await world.hooks.get(disposed).catch((error: unknown) => error))).toBe(true);
    await world.events.create(second.runId, completed);
    expect((await world.hooks.get(retained)).runId).toBe(second.runId);

    touched.clear();
    const listed = (await allPages((cursor) => reader.hooks.list({ pagination: { limit: 2, cursor, sortOrder: 'asc' } }))).flat();
    expect([...touched].every((touchedName) => touchedName.startsWith('index-'))).toBe(true);
    const ids = listed.map(({ hookId }) => hookId);
    expect(ids).toEqual(expect.arrayContaining([kept, retained]));
    expect(ids).not.toContain(disposed);
    expect(new Set(ids).size).toBe(ids.length);

    // A finished run's retained hook goes once its retention ends.
    await new Promise((resolve) => setTimeout(resolve, 1_100));
    expect(HookNotFoundError.is(await world.hooks.get(retained).catch((error: unknown) => error))).toBe(true);
    expect((await world.hooks.list({ pagination: { limit: 1000 } })).data.map(({ hookId }) => hookId)).not.toContain(retained);

    // A start Hook is there as soon as its run_created returns.
    const runId = newRunId();
    const token = crypto.randomUUID();
    await world.events.create(runId, { eventType: 'run_created', eventData: {
      deploymentId: 'test-v1', workflowName: 'index-start-hook', input: bytes('input'), startHook: { token },
    } });
    expect(await world.hooks.get(startHookId(runId))).toMatchObject({ runId, token });
  });

  it('backfills runs and hooks a store held before the index, through its alarm, in batches', async () => {
    const name = `index-backfill-${crypto.randomUUID()}`;
    const target = storeShardFor(newRunId());
    const total = INDEX_BACKFILL_BATCH * 2 + 50;
    const runIds: string[] = [];
    while (runIds.length < total) {
      const runId = newRunId();
      if (storeShardFor(runId) === target) runIds.push(runId);
    }
    const hookId = `hook_${crypto.randomUUID()}`;
    const now = Date.now();
    // Rows an older version wrote: nothing owes them to the index.
    await runInDurableObject(store(target), (_store, state) => {
      for (const [index, runId] of runIds.entries()) {
        const run: WorkflowRun = {
          runId, status: 'pending', deploymentId: 'test-v1', workflowName: name, input: bytes('input'),
          attributes: {}, specVersion: 2, createdAt: new Date(now + index), updatedAt: new Date(now + index),
        };
        state.storage.sql.exec('INSERT INTO runs (id, data, created_at, status, workflow_name) VALUES (?, ?, ?, ?, ?)',
          runId, encode(run), now + index, 'pending', name);
      }
      const hook: Hook = {
        runId: runIds[0], hookId, token: crypto.randomUUID(), ownerId: 'cloudflare', projectId: 'cloudflare',
        environment: 'cloudflare', isWebhook: true, isSystem: false, specVersion: 2, createdAt: new Date(now),
      };
      state.storage.sql.exec('INSERT INTO hooks (id, run_id, token, data, created_at) VALUES (?, ?, ?, ?, ?)',
        hookId, hook.runId, hook.token, encode(hook), now);
    });
    await predateIndex(target);

    // Until the store has backfilled, lists ask every store, so they still see its runs.
    const { world: reader, touched } = watched();
    const early = await reader.runs.list({ workflowName: name, pagination: { limit: 1000 }, resolveData: 'none' });
    expect(early.data).toHaveLength(total);
    expect(touched.has(target)).toBe(true);
    expect((await reader.hooks.get(hookId)).runId).toBe(runIds[0]);

    // Its alarm owes a batch at a time and keeps its place across a reset.
    const stage = () => runInDurableObject(store(target), (_store, state) => state.storage.kv.get<{ stage: string }>('index-backfill'));
    await expect.poll(async () => (await stage())?.stage, { timeout: 10_000 }).toBe('scanning');
    await expect.poll(() => indexed(name), { timeout: 10_000 }).toBeGreaterThan(0);
    expect(await indexed(name)).toBeLessThan(total);
    await runInDurableObject(store(target), (_store, state) => state.abort('reset')).catch(() => {});
    await expect.poll(async () => (await stage())?.stage, { timeout: 15_000, interval: 200 }).toBe('reported');
    expect(await indexed(name)).toBe(total);

    // Now the index answers alone.
    touched.clear();
    const pages = await allPages((cursor) => reader.runs.list({ workflowName: name, pagination: { limit: 200, cursor }, resolveData: 'none' }));
    expect(pages.flat().map(({ runId }) => runId)).toEqual([...runIds].reverse());
    expect(await reader.hooks.get(hookId)).toMatchObject({ runId: runIds[0], hookId });
    expect([...touched].every((touchedName) => touchedName.startsWith('index-'))).toBe(true);
  }, 30_000);

  it('reads many runs on many shards through the index shards alone', async () => {
    const name = `index-many-${crypto.randomUUID()}`;
    const runs: WorkflowRun[] = [];
    for (let batch = 0; batch < 6; batch++) {
      runs.push(...await Promise.all(Array.from({ length: 50 }, (_, index) => newRun(name, index % 2 ? 'first' : 'second'))));
    }
    const stores = new Set(runs.map(({ runId }) => storeShardFor(runId)));
    expect(stores.size).toBe(12);
    const { world: reader, touched } = watched();
    const pages = await allPages((cursor) => reader.runs.list({ workflowName: name, pagination: { limit: 40, cursor }, resolveData: 'none' }));
    expect(pages).toHaveLength(Math.ceil(runs.length / 40));
    expect(pages.flat().map(({ runId }) => runId)).toEqual(newestFirst(runs));
    // Eight pages over 300 runs on 12 store shards asked the 5 index shards and nothing else.
    expect([...touched].sort()).toEqual(indexShardNames().sort());
    expect(indexShardNames().length).toBeLessThan(stores.size);
  }, 30_000);
});
