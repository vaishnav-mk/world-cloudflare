import { queueShardsToWake } from './lib/shards.js';
import { legacyStore, onRun } from './store/route.js';
import type { WorldEnv } from './types.js';
import { createWorld } from './world.js';

export { createWorld, registerWorld } from './world.js';
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
 * Publishes the legacy store's dispatches whose alarm was lost, and wakes a slice of the queue
 * shards (`queueShardsToWake`) so each redelivers what a reset cut off. Call it from a `scheduled`
 * handler every minute. It reaches only the legacy store, which holds runs created before store
 * shards; a shard arms its alarm before each write that owes something, and re-arms it on its next write.
 */
export async function drainOutbox(env: Pick<WorldEnv, 'WORLD_STORE' | 'QUEUE_SHARD'>): Promise<number> {
  const [published] = await Promise.all([
    legacyStore(env.WORLD_STORE).drainOutbox(),
    ...queueShardsToWake(Math.floor(Date.now() / 60_000)).map((name) => env.QUEUE_SHARD.getByName(name).wake()),
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
