import { env, runInDurableObject } from 'cloudflare:test';
import { indexShardNames } from '../packages/world-cloudflare/src/lib/shards.js';

/**
 * Puts store `name` back to before the index, as data an older version wrote: not backfilled, so
 * lists ask every store until it has. It restarts, so it backfills on its next wake, as after a deploy.
 */
export async function predateIndex(name: string): Promise<void> {
  for (const index of indexShardNames()) {
    await runInDurableObject(env.WORLD_STORE.getByName(index), (_store, state) => {
      state.storage.sql.exec('DELETE FROM index_sources WHERE store = ?', name);
    });
  }
  await runInDurableObject(env.WORLD_STORE.getByName(name), (_store, state) => { state.storage.kv.delete('index-backfill'); });
  await runInDurableObject(env.WORLD_STORE.getByName(name), (_store, state) => state.abort('deploy')).catch(() => {});
}
