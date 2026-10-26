import { DurabilityRouting } from 'durability/routing';
import { toWorldError } from '../lib/transient.js';
import type { EnqueueRequest, EnqueueResult, WorldEnv } from '../types.js';
import { QueueShard } from './queue-shard.js';

type Push = { shard: string; requests: EnqueueRequest[] };

/**
 * Returns a function that sends each queue shard its messages in one call and answers each
 * message's result in input order. The queue client and the store shards both publish with it.
 */
export function shardPusher(namespace: WorldEnv['QUEUE_SHARD']) {
  const shards = DurabilityRouting.client({
    target: QueueShard,
    namespace: () => namespace,
    invoke: (stub, { requests }: Push) => stub.push(requests),
    sharding: ({ shard }) => shard,
  });
  return async function push(entries: { request: EnqueueRequest; shard: string }[]): Promise<EnqueueResult[]> {
    const byShard = new Map<string, number[]>();
    for (const [index, { shard }] of entries.entries()) byShard.set(shard, [...(byShard.get(shard) ?? []), index]);
    const results: EnqueueResult[] = entries.map(() => ({ error: 'Message was not enqueued' }));
    await Promise.all([...byShard].map(async ([shard, indexes]) => {
      const pushed = await shards.push({ shard, requests: indexes.map((index) => entries[index].request) }).catch((error: unknown) => {
        throw toWorldError(error);
      });
      for (const [position, index] of indexes.entries()) {
        results[index] = pushed.success ? pushed.value[position] ?? { error: 'Incomplete enqueue response' } : { error: pushed.error.message };
      }
    }));
    return results;
  };
}
