import { isSingleOrchestratorMessage, MessageId, type QueuePayload, type ValidQueueName } from '@workflow/world';
import { currentInvocationStart } from '../lib/request-context.js';
import type { EnqueueRequest, QueueHandler, QueueResult } from '../types.js';
import { wakeSeconds } from './handler.js';
import type { QueueShard } from './queue-shard.js';

/** How long the queue waits before it redelivers a held message whose handler rejected. */
const HELD_FAILURE_RETRY_MS = 1_000;

/**
 * `queue()` with `experimental_hold`: enqueues the message on its shard, held for this sender. The
 * sender runs it with `consume` in this request, renewing the lease, and the queue delivers it as
 * usual only if the lease lapses first. A key the queue already accepted is not held.
 */
export async function holdMessage(
  shard: () => DurableObjectStub<QueueShard>, queueName: ValidQueueName, payload: QueuePayload, request: EnqueueRequest,
  leaseMs: number,
): Promise<QueueResult> {
  const result = await shard().hold(request);
  if ('error' in result) throw new Error(result.error);
  const messageId = MessageId.parse(result.id);
  if (!result.fresh) return { messageId, experimental_hold: { status: 'not_held', reason: 'duplicate' } };
  let used = false;
  const once = () => {
    if (used) throw new Error('A held message is consumed or released once');
    used = true;
  };
  const createdAt = new Date();
  return { messageId, experimental_hold: {
    status: 'held', messageId, leaseExpiresAt: new Date(createdAt.getTime() + leaseMs),
    consume: async (handler: QueueHandler) => {
      once();
      const renewal = setInterval(() => {
        shard().renewHold(messageId, leaseMs).catch((error: unknown) => console.error('Hold renewal failed', error));
      }, leaseMs / 2);
      try {
        const { deliveries, handoffs } = await shard().beginHeld(messageId);
        let outcome: unknown;
        try {
          outcome = await handler(payload, {
            queueName, messageId, attempt: deliveries - handoffs, deliveryCount: deliveries, createdAt,
            invocationStartedAt: currentInvocationStart() ?? Date.now(),
          });
        } catch (error) {
          // As for a delivery that rejects: the queue retries it.
          console.error('Held message failed', error);
          await shard().settleHold(messageId, HELD_FAILURE_RETRY_MS, false);
          return;
        }
        const seconds = wakeSeconds(outcome, payload);
        const appends = isSingleOrchestratorMessage(payload);
        await shard().settleHold(messageId, seconds === undefined ? null : seconds * 1000,
          seconds === 0 && appends, seconds !== undefined && !appends);
      } finally {
        clearInterval(renewal);
      }
    },
    release: async (delaySeconds = 0) => {
      once();
      await shard().settleHold(messageId, delaySeconds * 1000, false);
    },
  } };
}
