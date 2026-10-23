import { parse, stringify } from 'devalue';
import {
  MessageId, QueuePrefix, ValidQueueName, type Queue, type QueueOptions, type QueuePayload,
} from '@workflow/world';
import { QUEUE_SHARD_NAME, STORE_NAME } from '../constants.js';
import { MessageKind } from '../enums.js';
import { messageIdFor, sha256Hex } from '../lib/hash.js';
import { transientAsWorldError } from '../lib/transient.js';
import type { EnqueueRequest, EnqueueResult, OutboxClaim, WorldEnv } from '../types.js';
import { executorFor } from './routes.js';

type QueueEnv = Pick<WorldEnv, 'WORLD_STORE' | 'QUEUE_SHARD' | 'DEPLOYMENT_ID' | 'EXECUTOR_ROUTES'>;
type Ready = { request: EnqueueRequest; claim?: OutboxClaim };

/** The World's `Queue`, backed by the queue shard Durable Object. */
export function createQueue(env: QueueEnv): Queue {
  // A batch costs three round trips however many messages it holds: outbox slots, enqueue, settle.
  const queueBatch: NonNullable<Queue['queueBatch']> = async (queueName, messages) => {
    ValidQueueName.parse(queueName);
    const prepared = await Promise.all(messages.map(({ message, opts = {} }) => prepare(env, queueName, message, opts)));
    const ready = prepared.filter((entry): entry is Ready => !('error' in entry));
    const store = transientAsWorldError(env.WORLD_STORE.getByName(STORE_NAME));
    const claims = ready.flatMap(({ claim }) => (claim ? [claim] : []));
    const slots = claims.length ? await store.pendingDispatchSlots(claims) : [];
    const slotOf = new Map(claims.map((claim, index) => [claim, slots[index] ?? 0]));
    const enqueued = ready.length
      ? await transientAsWorldError(env.QUEUE_SHARD.getByName(QUEUE_SHARD_NAME)).enqueueMany(ready.map(({ request }) => request))
      : [];
    if (enqueued.length !== ready.length) throw new Error('Incomplete enqueue response');
    const results = new Map<Ready, EnqueueResult>(ready.map((entry, index) => [entry, enqueued[index]]));
    const published = ready.flatMap((entry) => {
      const slot = entry.claim ? slotOf.get(entry.claim) : 0;
      const result = results.get(entry);
      return entry.claim && slot && result && 'fresh' in result && result.fresh ? [{ claim: entry.claim, slot }] : [];
    });
    if (published.length) await store.markDispatchesPublished(published);
    return prepared.map((entry) => {
      if ('error' in entry) return { messageId: null, error: entry.error, retryable: false };
      const result = results.get(entry);
      if (result === undefined || 'error' in result) {
        return { messageId: null, error: result?.error ?? 'Message was not enqueued', retryable: true };
      }
      return { messageId: MessageId.parse(result.id) };
    });
  };

  return {
    getDeploymentId: async () => env.DEPLOYMENT_ID,

    queue: async (queueName, message, opts) => {
      const [result] = await queueBatch(queueName, [{ message, opts }]);
      if (result === undefined || result.error !== undefined) throw new Error(result?.error ?? 'Message was not enqueued');
      return { messageId: result.messageId };
    },

    queueBatch,

    createQueueHandler: (prefix, handler) => async (request) => {
      const queueName = request.headers.get('x-vqs-queue-name');
      const messageId = request.headers.get('x-vqs-message-id');
      const attempt = Number(request.headers.get('x-vqs-message-attempt'));
      const validQueue = queueName !== null && ValidQueueName.safeParse(queueName).success &&
        queueName.startsWith(QueuePrefix.parse(prefix));
      if (!validQueue || !messageId || !Number.isSafeInteger(attempt) || attempt < 1) {
        return Response.json({ error: 'Invalid queue delivery' }, { status: 400 });
      }
      try {
        const payload = parse(await request.text()) as QueuePayload;
        const result = await handler(payload, {
          queueName: ValidQueueName.parse(queueName), messageId: MessageId.parse(messageId), attempt,
          deliveryCount: attempt, createdAt: new Date(Number(request.headers.get('x-vqs-created-at'))),
        });
        if (typeof result === 'object' && result !== null && 'timeoutSeconds' in result) {
          return Response.json({ timeoutSeconds: result.timeoutSeconds });
        }
        return Response.json({ ok: true });
      } catch (error) {
        console.error('Workflow delivery failed', error);
        return Response.json({ error: 'Delivery failed' }, { status: 500 });
      }
    },
  };
}

/** Validates one message and computes what the shard stores, or why it cannot be queued. */
async function prepare(
  env: QueueEnv, queueName: string, message: QueuePayload,
  { deploymentId: target, delaySeconds: delay = 0, idempotencyKey }: QueueOptions,
): Promise<Ready | { error: string }> {
  if (target && target !== env.DEPLOYMENT_ID && !executorFor(env, target)) return { error: `Deployment ${target} is unavailable` };
  if (!Number.isFinite(delay) || delay < 0) return { error: 'Invalid queue delay' };
  const key = idempotencyKey ? `${queueName}:${idempotencyKey}` : undefined;
  const encoded = stringify(message);
  const [id, digest] = key ? await Promise.all([messageIdFor(key), sha256Hex(encoded)]) : [crypto.randomUUID(), undefined];
  return {
    request: {
      id, queueName, encoded, delayMs: Math.ceil(delay * 1000), targetDeployment: target,
      deduplicationKey: key, payloadDigest: digest,
    },
    claim: outboxClaim(message),
  };
}

/** Which outbox row an SDK message would settle, if any. */
function outboxClaim(message: QueuePayload): OutboxClaim | undefined {
  if ('__healthCheck' in message) return undefined;
  const { runId } = message;
  if (message.stepId) {
    return message.stepName ? { kind: MessageKind.Step, runId, stepId: message.stepId, stepName: message.stepName } : undefined;
  }
  if (message.runInput) return { kind: MessageKind.Start, runId };
  if (message.hookResumeTiming) return { kind: MessageKind.HookWake, runId };
  if (message.waitContinuation) {
    return { kind: MessageKind.WaitContinuation, runId, correlationId: message.waitContinuation.correlationId };
  }
  return undefined;
}

