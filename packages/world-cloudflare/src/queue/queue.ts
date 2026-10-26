import { parse, stringify } from 'devalue';
import {
  MessageId, QueuePrefix, ValidQueueName, type Queue, type QueueOptions, type QueuePayload,
} from '@workflow/world';
import { DurabilityRouting } from 'durability/routing';
import { MessageKind } from '../enums.js';
import { messageIdFor, sha256Hex } from '../lib/hash.js';
import { toWorldError } from '../lib/transient.js';
import { onRun } from '../store/route.js';
import type { EnqueueRequest, EnqueueResult, OutboxClaim, WorldEnv } from '../types.js';
import { QueueShard } from './queue-shard.js';
import { executorFor } from './routes.js';
import { queueShardFor } from './shards.js';

type QueueEnv = Pick<WorldEnv, 'WORLD_STORE' | 'QUEUE_SHARD' | 'DEPLOYMENT_ID' | 'EXECUTOR_ROUTES'>;
type Ready = { request: EnqueueRequest; claim?: OutboxClaim; shard: string };
type Push = { shard: string; requests: EnqueueRequest[] };

/** The World's `Queue`, backed by the queue shard Durable Object. */
export function createQueue(env: QueueEnv): Queue {
  const shards = DurabilityRouting.client({
    target: QueueShard,
    namespace: () => env.QUEUE_SHARD,
    invoke: (stub, { requests }: Push) => stub.push(requests),
    sharding: ({ shard }) => shard,
  });
  /** Sends each shard its messages in one call, returning each message's result in input order. */
  async function enqueue(ready: Ready[]): Promise<EnqueueResult[]> {
    const byShard = new Map<string, Ready[]>();
    for (const entry of ready) byShard.set(entry.shard, [...(byShard.get(entry.shard) ?? []), entry]);
    const results = new Map<Ready, EnqueueResult>();
    await Promise.all([...byShard].map(async ([shard, group]) => {
      const pushed = await shards.push({ shard, requests: group.map(({ request }) => request) }).catch((error: unknown) => {
        throw toWorldError(error);
      });
      for (const [index, entry] of group.entries()) {
        results.set(entry, pushed.success ? pushed.value[index] ?? { error: 'Incomplete enqueue response' } : { error: pushed.error.message });
      }
    }));
    return ready.map((entry) => results.get(entry) ?? { error: 'Message was not enqueued' });
  }

  // A batch costs three round trips per shard however many messages it holds: outbox slots, enqueue, settle.
  // The outbox calls go to each run's store, in parallel; a batch usually holds one run's messages.
  const queueBatch: NonNullable<Queue['queueBatch']> = async (queueName, messages) => {
    ValidQueueName.parse(queueName);
    const prepared = await Promise.all(messages.map(({ message, opts = {} }) => prepare(env, queueName, message, opts)));
    const ready = prepared.filter((entry): entry is Ready => !('error' in entry));
    const claims = ready.flatMap(({ claim }) => (claim ? [claim] : []));
    const slotOf = new Map<OutboxClaim, number>();
    await Promise.all([...byRun(claims, (claim) => claim.runId)].map(async ([runId, group]) => {
      const slots = await onRun(env.WORLD_STORE, runId, (store) => store.pendingDispatchSlots(group), answersUnknown);
      for (const [index, claim] of group.entries()) slotOf.set(claim, slots[index] ?? 0);
    }));
    const enqueued = await enqueue(ready);
    const results = new Map<Ready, EnqueueResult>(ready.map((entry, index) => [entry, enqueued[index]]));
    const published = ready.flatMap((entry) => {
      const slot = entry.claim ? slotOf.get(entry.claim) : 0;
      const result = results.get(entry);
      return entry.claim && slot && result && 'fresh' in result && result.fresh ? [{ claim: entry.claim, slot }] : [];
    });
    await Promise.all([...byRun(published, ({ claim }) => claim.runId)].map(([runId, group]) =>
      onRun(env.WORLD_STORE, runId, (store) => store.markDispatchesPublished(group), answersUnknown)));
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

/** The directory answers outbox calls for a run it lacks, such as one whose `run_created` is still in flight. */
const answersUnknown = { answersUnknown: true };

function byRun<T>(items: T[], runIdOf: (item: T) => string): Map<string, T[]> {
  const groups = new Map<string, T[]>();
  for (const item of items) groups.set(runIdOf(item), [...(groups.get(runIdOf(item)) ?? []), item]);
  return groups;
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
    shard: queueShardFor(key ?? ('runId' in message && typeof message.runId === 'string' ? message.runId : '')),
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

