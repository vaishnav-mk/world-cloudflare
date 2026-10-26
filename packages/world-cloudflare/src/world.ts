import { env as platformEnv } from 'cloudflare:workers';
import { mintedSpecVersion, type World } from '@workflow/world';
import { createQueue } from './queue/queue.js';
import { createStorage } from './store/storage.js';
import { createSnapshots } from './streams/snapshots.js';
import { createStreamer } from './streams/streamer.js';
import type { WorldEnv } from './types.js';

/** The Cloudflare World. Defaults to the Worker's own bindings. */
export function createWorld(env: WorldEnv = platformEnv): World {
  return {
    specVersion: mintedSpecVersion(),
    capabilities: {
      // Hooks keep `tokenRetentionUntil` past their run's end; see `Db.isExpiredHook`.
      hookRetention: { active: true },
      // A unique `(run_id, resume_id)` index, and the store returns the committed event to a retry.
      hookResumeDedup: true,
    },
    ...createStorage(env),
    ...createQueue(env),
    ...createStreamer(env),
    ...createSnapshots(env),
  };
}
