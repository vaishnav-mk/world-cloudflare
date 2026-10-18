import { createWorld, type WorldEnv } from './world.js';

export { createWorld, type FlowHandler, type WorldEnv } from './world.js';
export { WorldStore } from './store.js';
export { QueueShard, type Delivery } from './queue.js';
export { RunStreams } from './streams.js';
export { createQueueConsumer } from './consumer.js';
export { createExecutor } from './executor.js';

/** Publishes dispatches whose alarm was lost. Call it from a `scheduled` handler. */
export function drainOutbox(env: Pick<WorldEnv, 'WORLD_STORE'>): Promise<number> {
  return env.WORLD_STORE.getByName('global').drainOutbox();
}

export default createWorld;
