import { STORE_NAME } from './constants.js';
import type { WorldEnv } from './types.js';
import { createWorld } from './world.js';

export { createWorld } from './world.js';
export { createExecutor } from './executor.js';
export { createQueueConsumer } from './queue/consumer.js';
export { QueueShard } from './queue/queue-shard.js';
export { WorldStore } from './store/world-store.js';
export { RunStreams } from './streams/run-streams.js';
export { EventType, ResolveData, RunStatus, StepStatus, WaitStatus } from './enums.js';
export { toJson } from './lib/json.js';
export { handleWorldRpc } from './remote/server.js';
export type { Delivery, FlowHandler, WorldEnv } from './types.js';

/** Publishes dispatches whose alarm was lost. Call it from a `scheduled` handler. */
export function drainOutbox(env: Pick<WorldEnv, 'WORLD_STORE'>): Promise<number> {
  return env.WORLD_STORE.getByName(STORE_NAME).drainOutbox();
}

/**
 * A run's events after `cursor` (an event slot, 0 for the start), optionally only of `types`.
 * Waits up to 15 s for the next commit when there are none. Backs a live event subscription.
 */
export function nextEvents(env: Pick<WorldEnv, 'WORLD_STORE'>, runId: string, cursor: number, types?: string[]) {
  return env.WORLD_STORE.getByName(STORE_NAME).nextEvents(runId, cursor, types);
}

export default createWorld;
