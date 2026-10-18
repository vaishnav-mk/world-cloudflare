import { env as platformEnv } from 'cloudflare:workers';
import { mintedSpecVersion, type World } from '@workflow/world';
import { createQueue, type QueueEnv } from './queue.js';
import { createStorage } from './storage.js';
import type { Env as StoreEnv } from './store.js';
import { createStreamer, type RunStreams } from './streams.js';

export type WorldEnv = StoreEnv & QueueEnv & { RUN_STREAMS: DurableObjectNamespace<RunStreams> };

/** The `POST` export of the app's generated `.well-known/workflow/v1/flow.mjs`. */
export type FlowHandler = (request: Request) => Promise<Response>;

export function createWorld(env: WorldEnv = platformEnv): World {
  return {
    specVersion: mintedSpecVersion(),
    ...createStorage(env),
    ...createQueue(env),
    ...createStreamer(env),
  };
}
