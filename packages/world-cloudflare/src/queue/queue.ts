import { WorkflowWorldError } from '@workflow/errors';
import { unwrapInvocationOutcome } from '@workflow/errors/invocation';
import { stringify } from 'devalue';
import { MessageId, ValidQueueName, type Queue, type QueueOptions, type QueuePayload } from '@workflow/world';
import { currentRequest, type FlowRequest } from '../lib/request-context.js';
import { MessageKind } from '../enums.js';
import { groupBy } from '../lib/group.js';
import { messageIdFor, sha256Hex } from '../lib/hash.js';
import { transientAsWorldError } from '../lib/transient.js';
import { onRun } from '../store/route.js';
import type { EnqueueRequest, EnqueueResult, HoldingQueue, OutboxClaim, QueueDispatch, WorldEnv } from '../types.js';
import { shardPusher } from './push.js';
import { executorFor } from './routes.js';
import { objectOptions, queueShardFor, runnerShardFor } from '../lib/shards.js';
import { DEFAULT_HOLD_LEASE_SECONDS, MAX_HOLD_LEASE_SECONDS, PUBLISH_BATCH_MAX } from '../constants.js';
import { createQueueHandler } from './handler.js';
import { holdMessage } from './hold.js';

type QueueEnv = Pick<WorldEnv, 'WORLD_STORE' | 'QUEUE_SHARD' | 'RUNNER' | 'DEPLOYMENT_ID' | 'EXECUTOR_ROUTES'>;
type Ready = { request: EnqueueRequest; claim?: OutboxClaim; shard: string };
type Published = Awaited<ReturnType<NonNullable<Queue['queueBatch']>>>[number];
type Waiting = { message: QueuePayload; opts: QueueOptions; resolve: (result: Published | undefined) => void; reject: (error: unknown) => void };

/** The World's `Queue`, backed by the queue shard Durable Objects. */
export function createQueue(env: QueueEnv): HoldingQueue {
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

  // A flow request's single publishes in the same turn go out together, PUBLISH_BATCH_MAX to a
  // `queueBatch`: a replay that starts thousands of steps at once then makes a few calls to the
  // run's store instead of one per step, which overloaded it.
  const turns = new WeakMap<FlowRequest, Map<string, Waiting[]>>();
  const publish = (queueName: string, message: QueuePayload, opts: QueueOptions): Promise<Published | undefined> => {
    const request = currentRequest();
    if (request === undefined) return queueBatch(queueName, [{ message, opts }]).then(([result]) => result);
    const queues = turns.get(request) ?? new Map<string, Waiting[]>();
    turns.set(request, queues);
    const batch = queues.get(queueName) ?? [];
    if (batch.length === 0) {
      queues.set(queueName, batch);
      setTimeout(() => {
        queues.delete(queueName);
        for (let start = 0; start < batch.length; start += PUBLISH_BATCH_MAX) {
          const chunk = batch.slice(start, start + PUBLISH_BATCH_MAX);
          queueBatch(queueName, chunk).then(
            (results) => chunk.forEach(({ resolve }, index) => resolve(results[index])),
            (error: unknown) => chunk.forEach(({ reject }) => reject(error)),
          );
        }
      });
    }
    return new Promise((resolve, reject) => batch.push({ message, opts, resolve, reject }));
  };

  return {
    getDeploymentId: async () => env.DEPLOYMENT_ID,
    // Only the queue consumer, called by runners, and private executor dispatch reach the flow route; no public route serves it.
    queueAuthMode: 'skip',

    queue: async (queueName, message, opts = {}) => {
      const { experimental_hold: hold, ...rest } = opts;
      // Only this deployment's messages are held: another deployment must run its own.
      if (hold !== undefined && (rest.deploymentId === undefined || rest.deploymentId === env.DEPLOYMENT_ID)) {
        if (rest.delaySeconds) throw new Error('experimental_hold cannot be combined with delaySeconds');
        const leaseSeconds = Math.min(MAX_HOLD_LEASE_SECONDS, Math.max(1, (hold === true ? undefined : hold.visibilityTimeoutSeconds)
          ?? DEFAULT_HOLD_LEASE_SECONDS));
        const ready = await prepare(env, queueName, message, { ...rest, delaySeconds: leaseSeconds });
        if ('error' in ready) throw new Error(ready.error);
        return holdMessage(() => env.QUEUE_SHARD.getByName(ready.shard, objectOptions()), queueName, message, ready.request, leaseSeconds * 1000);
      }
      const target = rest.deploymentId;
      if (target !== undefined && !reachable(env, target)) {
        throw new WorkflowWorldError(`Deployment ${target} is unavailable`, { status: 404, code: DEPLOYMENT_UNAVAILABLE });
      }
      const result = await publish(queueName, message, rest);
      if (result === undefined || result.error !== undefined) throw new Error(result?.error ?? 'Message was not enqueued');
      return hold === undefined
        ? { messageId: result.messageId }
        : { messageId: result.messageId, experimental_hold: { status: 'not_held', reason: 'unsupported' } };
    },

    queueBatch,

    /**
     * Hands an invocation to the run's runner (`Runner.invoke`), which runs it beside the run's
     * delivery in progress, if any. The outcome carries the handler's errors with their class.
     */
    invoke: async (runId, payload, { idempotencyKey = crypto.randomUUID(), timeoutMs } = {}) => {
      const outcome = transientAsWorldError(env.RUNNER.getByName(runnerShardFor(runId), objectOptions())).invoke(runId, payload, idempotencyKey);
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timedOut = new Promise<never>((_, reject) => {
        if (timeoutMs === undefined) return;
        timer = setTimeout(() => reject(new WorkflowWorldError('Timed out awaiting the invocation; its outcome is unknown', { status: 408 })), timeoutMs);
      });
      try {
        return unwrapInvocationOutcome(await Promise.race([outcome, timedOut]));
      } finally {
        clearTimeout(timer);
      }
    },

    // Only a deployment with no executor to run it is definitely unreachable.
    isDeploymentUnavailableError: (error) => WorkflowWorldError.is(error) && error.code === DEPLOYMENT_UNAVAILABLE,

    createQueueHandler,
  };
}

/** `WorkflowWorldError.code` for a message to a deployment that has no executor here. */
const DEPLOYMENT_UNAVAILABLE = 'DEPLOYMENT_UNAVAILABLE';

/** Whether this Worker runs `deploymentId`'s flows, itself or on a retained executor. */
function reachable(env: Pick<QueueEnv, 'DEPLOYMENT_ID' | 'EXECUTOR_ROUTES'>, deploymentId: string): boolean {
  return deploymentId === env.DEPLOYMENT_ID || executorFor(env, deploymentId) !== undefined;
}

/** The legacy store answers outbox calls for a run it lacks, such as one whose `run_created` is still in flight. */
const answersUnknown = { answersUnknown: true };

/** Validates one message and computes what the shard stores, or why it cannot be queued. */
async function prepare(
  env: QueueEnv, queueName: string, message: QueuePayload,
  { deploymentId: target, delaySeconds: delay = 0, idempotencyKey }: QueueOptions,
): Promise<Ready | { error: string }> {
  if (target && !reachable(env, target)) return { error: `Deployment ${target} is unavailable` };
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

