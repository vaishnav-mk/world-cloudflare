import { QUEUE_SHARD_NAME, QUEUE_SHARDS } from '../constants.js';
import { fnv1a32Hex } from '../lib/hash.js';

/**
 * The queue shard for a message, by a stable hash of its idempotency key, or of its run ID when it
 * has none. Deduplication lives in each shard, so every repeat of a key must reach the same one.
 */
export function queueShardFor(routingKey: string): string {
  return `${QUEUE_SHARD_NAME}-${Number.parseInt(fnv1a32Hex(routingKey), 16) % QUEUE_SHARDS}`;
}

/** Every shard, plus the single `flow` shard that served messages queued before sharding. */
export const QUEUE_SHARD_NAMES = [
  QUEUE_SHARD_NAME, ...Array.from({ length: QUEUE_SHARDS }, (_, index) => `${QUEUE_SHARD_NAME}-${index}`),
];
