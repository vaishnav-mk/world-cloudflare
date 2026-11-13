import { parse, stringify } from 'devalue';
import {
  isSingleOrchestratorMessage, MessageId, QueuePrefix, ValidQueueName,
  type Queue, type QueueOptions, type QueuePayload,
} from '@workflow/world';
import { MessageKind } from '../enums.js';
import { groupBy } from '../lib/group.js';
import { messageIdFor, sha256Hex } from '../lib/hash.js';
import { onRun } from '../store/route.js';
import type { EnqueueRequest, EnqueueResult, OutboxClaim, QueueDispatch, WorldEnv } from '../types.js';
import { shardPusher } from './push.js';
import { executorFor } from './routes.js';
import { queueShardFor } from '../lib/shards.js';
import { MAX_SPEC9_REDELIVERY_SECONDS, MAX_WAKE_SECONDS } from '../constants.js';

type QueueEnv = Pick<WorldEnv, 'WORLD_STORE' | 'QUEUE_SHARD' | 'DEPLOYMENT_ID' | 'EXECUTOR_ROUTES'>;
type Ready = { request: EnqueueRequest; claim?: OutboxClaim; shard: string };

/** The World's `Queue`, backed by the queue shard Durable Objects. */
export function createQueue(env: QueueEnv): Queue {
  const push = shardPusher(env.QUEUE_SHARD);

  // A message that settles an outbox row goes through its run's store, which claims the slot,
  // pushes to the queue shard and settles the row, so the caller waits on one round trip.
  // Other messages go straight to their queue shard.
  const queueBatch: NonNullable<Queue['queueBatch']> = async (queueName, messages) => {
    ValidQueueName.parse(queueName);
    const prepared = await Promise.all(messages.map(({ message, opts = {} }) => prepare(env, queueName, message, opts)));
    const ready = prepared.filter((entry): entry is Ready => !('error' in entry));
    const claimed = ready.filter((entry): entry is QueueDispatch => entry.claim !== undefined);
    const direct = ready.filter((entry) => entry.claim === undefined);
    const results = new Map<Ready, EnqueueResult>();
    await Promise.all([
      push(direct).then((pushed) => direct.forEach((entry, index) => results.set(entry, pushed[index]))),
      ...[...groupBy(claimed, (entry) => entry.claim.runId)].map(async ([runId, group]) => {
        const pushed = await onRun(env.WORLD_STORE, runId, (store) => store.dispatch(group), answersUnknown);
        group.forEach((entry, index) => results.set(entry, pushed[index] ?? { error: 'Incomplete enqueue response' }));
      }),
    ]);
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
    // Only the queue consumer and private executor dispatch reach the flow route; no public route serves it.
    queueAuthMode: 'skip',

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
        // Absent from a consumer that predates them: then every delivery may be a redelivery.
        const deliveryCount = Number(request.headers.get('x-vqs-delivery-count') ?? Number.NaN);
        const startedAt = Number(request.headers.get('x-workflow-invocation-started-at') ?? Number.NaN);
        const result = await handler(payload, {
          queueName: ValidQueueName.parse(queueName), messageId: MessageId.parse(messageId), attempt,
          deliveryCount: Number.isSafeInteger(deliveryCount) && deliveryCount >= 1 ? deliveryCount : undefined,
          createdAt: new Date(Number(request.headers.get('x-vqs-created-at'))),
          invocationStartedAt: Number.isSafeInteger(startedAt) ? startedAt : Date.now(),
        });
        if (typeof result === 'object' && result !== null && 'timeoutSeconds' in result) {
          // JSON has no Infinity. An unbounded or negative wait is redelivered after the longest delay
          // instead, never read as 0, which on a spec 9 run is a handoff that is not an attempt.
          const seconds = Number(result.timeoutSeconds);
          const longest = isSingleOrchestratorMessage(payload) ? MAX_SPEC9_REDELIVERY_SECONDS : MAX_WAKE_SECONDS;
          return Response.json({ timeoutSeconds: Number.isFinite(seconds) && seconds >= 0 ? seconds : longest });
        }
        return Response.json({ ok: true });
      } catch (error) {
        console.error('Workflow delivery failed', error);
        return Response.json({ error: 'Delivery failed' }, { status: 500 });
      }
    },
  };
}

/** The legacy store answers outbox calls for a run it lacks, such as one whose `run_created` is still in flight. */
const answersUnknown = { answersUnknown: true };

/** Validates one message and computes what the shard stores, or why it cannot be queued. */
async function prepare(
  env: QueueEnv, queueName: string, message: QueuePayload,
  { deploymentId: target, delaySeconds: delay = 0, idempotencyKey }: QueueOptions,
): Promise<Ready | { error: string }> {
  if (target && target !== env.DEPLOYMENT_ID && !executorFor(env, target)) return { error: `Deployment ${target} is unavailable` };
  if (!Number.isFinite(delay) || delay < 0) return { error: 'Invalid queue delay' };
  const key = idempotencyKey ? `${queueName}:${idempotencyKey}` : undefined;
  const encoded = stringify(message);
  const runId = 'runId' in message && typeof message.runId === 'string' ? message.runId : '';
  const [id, digest] = key ? await Promise.all([messageIdFor(key), sha256Hex(encoded)]) : [crypto.randomUUID(), undefined];
  return {
    request: {
      id, queueName, encoded, delayMs: Math.ceil(delay * 1000), targetDeployment: target,
      deduplicationKey: key, payloadDigest: digest,
    },
    claim: outboxClaim(message),
    shard: queueShardFor(runId, message, key),
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

