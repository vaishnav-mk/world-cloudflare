import { QUEUE_SHARD_NAME } from './constants.js';
import { directory, onRun } from './store/route.js';
import type { WorldEnv } from './types.js';
import { createWorld } from './world.js';

export { createWorld } from './world.js';
export { createExecutor } from './executor.js';
export { createQueueConsumer } from './queue/consumer.js';
export { QueueShard } from './queue/queue-shard.js';
export { WorldStore } from './store/world-store.js';
export { RunStreams } from './streams/run-streams.js';
export { DeliveryResult, EventType, ResolveData, RunStatus, StepStatus, WaitStatus } from './enums.js';
export { toJson } from './lib/json.js';
export { handleWorldRpc } from './remote/server.js';
export type { Delivery, DeliveryOutcome, FlowHandler, WorldEnv } from './types.js';

/**
 * Publishes the directory's dispatches whose alarm was lost, and wakes the queue shard so it
 * redelivers what a reset cut off. Call it from a `scheduled` handler. It reaches only the
 * directory, which holds runs created before per-run stores; a run's own store arms its alarm
 * before each write that owes something, and re-arms it on the run's next write.
 */
export async function drainOutbox(env: Pick<WorldEnv, 'WORLD_STORE' | 'QUEUE_SHARD'>): Promise<number> {
  const [published] = await Promise.all([
    directory(env.WORLD_STORE).drainOutbox(),
    env.QUEUE_SHARD.getByName(QUEUE_SHARD_NAME).wake(),
  ]);
  return published;
}

/**
 * A run's events after `cursor` (an event slot, 0 for the start), optionally only of `types`.
 * Waits up to 15 s for the next commit when there are none. Backs a live event subscription.
 */
export function nextEvents(env: Pick<WorldEnv, 'WORLD_STORE'>, runId: string, cursor: number, types?: string[]) {
  return onRun(env.WORLD_STORE, runId, (store) => store.nextEvents(runId, cursor, types));
}

export default createWorld;
