import { parse, stringify } from 'devalue';
import { MessageId, QueuePrefix, ValidQueueName, type Queue, type QueuePayload } from '@workflow/world';
import { QUEUE_SHARD_NAME, STORE_NAME } from '../constants.js';
import { MessageKind } from '../enums.js';
import { messageIdFor, sha256Hex } from '../lib/hash.js';
import type { WorldStore } from '../store/world-store.js';
import type { WorldEnv } from '../types.js';
import { executorFor } from './routes.js';

type QueueEnv = Pick<WorldEnv, 'WORLD_STORE' | 'QUEUE_SHARD' | 'DEPLOYMENT_ID' | 'EXECUTOR_ROUTES'>;
type Store = DurableObjectStub<WorldStore>;

/** The outbox row a message settles once the queue accepts it. */
type OutboxClaim =
  | { kind: MessageKind.Step; runId: string; stepId: string; stepName: string }
  | { kind: MessageKind.Start; runId: string }
  | { kind: MessageKind.HookWake; runId: string }
  | { kind: MessageKind.WaitContinuation; runId: string; correlationId: string };

/** The World's `Queue`, backed by the queue shard Durable Object. */
export function createQueue(env: QueueEnv): Queue {
  return {
    getDeploymentId: async () => env.DEPLOYMENT_ID,

    queue: async (queueName, message, opts) => {
      ValidQueueName.parse(queueName);
      const target = opts?.deploymentId;
      if (target && target !== env.DEPLOYMENT_ID && !executorFor(env, target)) {
        throw new Error(`Deployment ${target} is unavailable`);
      }
      const delay = opts?.delaySeconds ?? 0;
      if (!Number.isFinite(delay) || delay < 0) throw new RangeError('Invalid queue delay');

      const key = opts?.idempotencyKey ? `${queueName}:${opts.idempotencyKey}` : undefined;
      const encoded = stringify(message);
      const store = env.WORLD_STORE.getByName(STORE_NAME);
      const claim = outboxClaim(message);
      const slot = claim ? await pendingSlot(store, claim) : 0;
      const id = key ? await messageIdFor(key) : crypto.randomUUID();
      const digest = key ? await sha256Hex(encoded) : undefined;
      const result = await env.QUEUE_SHARD.getByName(QUEUE_SHARD_NAME).enqueue(
        id, queueName, encoded, Math.ceil(delay * 1000), target, key, digest,
      );
      if (result.fresh && claim && slot) await markPublished(store, claim, slot);
      return { messageId: MessageId.parse(result.id) };
    },

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

function pendingSlot(store: Store, claim: OutboxClaim): Promise<number> {
  switch (claim.kind) {
    case MessageKind.Step:
      return store.pendingStepDispatchSlot(claim.runId, claim.stepId, claim.stepName);
    case MessageKind.Start:
      return store.pendingRunStartSlot(claim.runId);
    case MessageKind.HookWake:
      return store.pendingHookWakeSlot(claim.runId);
    case MessageKind.WaitContinuation:
      return store.pendingWaitDispatchSlot(claim.runId, claim.correlationId);
  }
}

function markPublished(store: Store, claim: OutboxClaim, slot: number): Promise<void> {
  switch (claim.kind) {
    case MessageKind.Step:
      return store.markStepDispatchPublished(claim.runId, slot);
    case MessageKind.Start:
      return store.markRunStartsPublished(claim.runId, slot);
    case MessageKind.HookWake:
      return store.markHookWakesPublished(claim.runId, slot);
    case MessageKind.WaitContinuation:
      return store.markWaitDispatchPublished(claim.runId, slot);
  }
}
