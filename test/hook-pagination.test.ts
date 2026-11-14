import { env, runInDurableObject } from 'cloudflare:test';
import { beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { Hook } from '@workflow/world';
import { createWorld, type WorldEnv } from '@workflow/world-cloudflare';
import { indexShardNames, indexSourceNames, storeShardNames } from '../packages/world-cloudflare/src/lib/shards.js';
import { Db } from '../packages/world-cloudflare/src/store/db.js';

const hooks: Hook[] = Array.from({ length: 5 }, (_, index) => ({
  hookId: `hook_pagination_${index}`, runId: `wrun_pagination_${index}`, token: `pagination-${index}`,
  ownerId: 'cloudflare', projectId: 'cloudflare', environment: 'cloudflare', isWebhook: true, isSystem: false,
  specVersion: 2, createdAt: new Date(1_600_000_000_000 + index),
  // The outer hooks have expired. An unfinished run's retention does not hide its hook.
  tokenRetentionUntil: new Date(index === 2 ? 4_000_000_000_000 : 1_600_000_000_000),
}));
const finished = (index: number) => index !== 1 && index !== 3;
const shard = (index: number) => index === 2 ? 'index-1' : 'index-0';

describe.each(['ready index', 'fallback'])('hook pagination from %s', (source) => {
  beforeAll(async () => {
    // These fixtures are already indexed, with every source reported, so the real index answers.
    for (const name of indexShardNames()) {
      for (const source of indexSourceNames(name)) await env.WORLD_STORE.getByName(name).markIndexed(source);
    }
  });

  beforeEach(async () => {
    for (const name of ['index-0', 'index-1']) {
      await env.WORLD_STORE.getByName(name).indexChanges({
        runs: [], hooks: [], goneHooks: hooks.map(({ runId, hookId }) => ({ runId, hookId })),
      });
    }
    for (const name of ['store-0', 'store-1']) {
      await runInDurableObject(env.WORLD_STORE.getByName(name), (_store, state) => {
        const db = new Db(state.storage.sql);
        for (const hook of hooks) db.deleteHook(hook.hookId);
      });
    }
  });

  for (const sortOrder of ['asc', 'desc'] as const) {
    for (const secondShard of [false, true]) {
      it(`pages live hooks ${sortOrder}, second shard ${secondShard}`, async () => {
        const selected = hooks.filter((_, index) => secondShard || index !== 2);
        for (const hook of selected) {
          const index = hooks.indexOf(hook);
          await env.WORLD_STORE.getByName(shard(index)).indexChanges({
            runs: [], hooks: [{ hook, finished: finished(index) }], goneHooks: [],
          });
          await runInDurableObject(env.WORLD_STORE.getByName(index === 2 ? 'store-1' : 'store-0'), (_store, state) => {
            const db = new Db(state.storage.sql);
            db.putRun({
              runId: hook.runId, deploymentId: 'test-v1', workflowName: 'hook-pagination',
              input: new Uint8Array(), attributes: {}, specVersion: 2, createdAt: hook.createdAt, updatedAt: hook.createdAt,
              ...(finished(index) ? { status: 'completed', completedAt: hook.createdAt } : { status: 'running' }),
            });
            db.insertHook(hook);
          });
        }
        const touched = new Set<string>();
        // SAFETY: hooks.list only uses these namespace methods. Simulate an incomplete index for
        // every fallback page; authoritative store reads still reach real Durable Objects.
        const stores = { getByName: (name: string) => {
          touched.add(name);
          if (source === 'fallback' && name.startsWith('index-')) return { listIndexedHooks: async () => ({ complete: false }) };
          return env.WORLD_STORE.getByName(name);
        } } as unknown as WorldEnv['WORLD_STORE'];
        const world = createWorld({ ...env, WORLD_STORE: stores });
        const expected = selected.filter((hook) => hooks.indexOf(hook) > 0 && hooks.indexOf(hook) < 4).map(({ hookId }) => hookId);
        if (sortOrder === 'desc') expected.reverse();
        const seen: string[] = [];
        const cursors = new Set<string>();
        let cursor: string | undefined;
        for (let pageNumber = 0; ; pageNumber++) {
          // A broken cursor must fail rather than loop forever.
          expect(pageNumber).toBeLessThanOrEqual(expected.length);
          const page = await world.hooks.list({ pagination: { limit: 1, cursor, sortOrder } });
          expect(page.data.map(({ hookId }) => hookId)).toEqual([expected[pageNumber]]);
          seen.push(...page.data.map(({ hookId }) => hookId));
          expect(page.hasMore).toBe(seen.length < expected.length);
          if (!page.hasMore) {
            expect(page.cursor).toBeNull();
            break;
          }
          expect(page.cursor).not.toBeNull();
          if (page.cursor === null) throw new Error('Missing continuation cursor');
          expect(cursors.has(page.cursor)).toBe(false);
          cursors.add(page.cursor);
          cursor = page.cursor;
        }
        expect(seen).toEqual(expected);
        expect(new Set(seen).size).toBe(seen.length);
        expect([...touched].sort()).toEqual([
          ...indexShardNames(), ...(source === 'fallback' ? [...storeShardNames(), 'global'] : []),
        ].sort());
      });
    }
  }
});
