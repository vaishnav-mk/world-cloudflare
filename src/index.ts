import { createQueueConsumer, drainOutbox, type WorldEnv } from '@workflow/world-cloudflare';
import { POST as flow } from '../.well-known/workflow/v1/flow.mjs';
import { handleRequest } from './router.js';
import './world.js';

export { QueueShard, RunStreams, WorldStore } from '@workflow/world-cloudflare';
export const QueueConsumer = createQueueConsumer(flow);

export default {
  async scheduled(_controller: ScheduledController, env: WorldEnv): Promise<void> {
    await drainOutbox(env);
  },
  fetch: handleRequest,
};
