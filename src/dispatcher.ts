import { createQueueConsumer, drainOutbox, type WorldEnv } from 'world-cloudflare';
import { handleRequest } from './router.js';
import './world.js';

/**
 * The main Worker for a deployment with retained executors (`EXECUTORS`): every delivery runs on
 * an executor, so this entry leaves the workflow bundle out and starts faster. `index.ts` adds the
 * bundle for a Worker that runs its own flows.
 */
export { QueueShard, Runner, RunStreams, WorldStore } from 'world-cloudflare';
export const QueueConsumer = createQueueConsumer(() => {
  throw new Error('This Worker runs no flows: bind EXECUTORS, or deploy src/index.ts');
});

export default {
  async scheduled(_controller: ScheduledController, env: WorldEnv): Promise<void> {
    await drainOutbox(env);
  },
  fetch: handleRequest,
};
