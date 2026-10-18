import { env as platformEnv } from 'cloudflare:workers';
import { mintedSpecVersion, type World } from '@workflow/world';
import { createQueue } from './queue/queue.js';
import { createStorage } from './store/storage.js';
import { createStreamer } from './streams/streamer.js';
import type { WorldEnv } from './types.js';

/** The Cloudflare World. Defaults to the Worker's own bindings. */
export function createWorld(env: WorldEnv = platformEnv): World {
  return {
    specVersion: mintedSpecVersion(),
    ...createStorage(env),
    ...createQueue(env),
    ...createStreamer(env),
  };
}
