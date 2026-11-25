import { WorkflowWorldError } from '@workflow/errors';
import { unwrapInvocationOutcome } from '@workflow/errors/invocation';
import { MessageId, SPEC_VERSION_SINGLE_ORCHESTRATOR, ValidQueueName, type Queue, type QueueOptions, type QueuePayload } from '@workflow/world';
import { waitUntil } from 'cloudflare:workers';
import { stringify } from 'devalue';
import { DEFAULT_HOLD_LEASE_SECONDS, DISPATCHED_STEPS_READ_MS, INLINE_START_BUDGET_MS, MAX_HOLD_LEASE_SECONDS, PAYLOAD_MAX_BYTES } from '../constants.js';
import { DeliveryResult, MessageKind } from '../enums.js';
import { groupBy } from '../lib/group.js';
import { fnv1a32Hex, messageIdFor, sha256Hex } from '../lib/hash.js';
import { isRecord } from '../lib/is-record.js';
import { tooLarge } from '../lib/payloads.js';
import { currentRequest, type FlowRequest } from '../lib/request-context.js';
import { objectOptions, queueShardFor, runnerShardFor } from '../lib/shards.js';
import { transientAsWorldError } from '../lib/transient.js';
import { gatherTurns } from '../lib/turns.js';
import { answersUnknown, onRun } from '../store/route.js';
import type { DeliveryOutcome, EnqueueRequest, EnqueueResult, HoldingQueue, OutboxClaim, QueueDispatch, WorldEnv } from '../types.js';
import { createQueueHandler } from './handler.js';
import { HELD_FAILURE_RETRY_MS, holdMessage } from './hold.js';
import { shardPusher } from './push.js';
import { executorFor } from './routes.js';

type QueueEnv = Pick<WorldEnv, 'WORLD_STORE' | 'QUEUE_SHARD' | 'RUNNER' | 'CONSUMER' | 'DEPLOYMENT_ID' | 'EXECUTOR_ROUTES' | 'INLINE_START'>;
type Ready = { request: EnqueueRequest; claim?: OutboxClaim; shard: string };
/** The steps of a run a flow request read as dispatched, and when it last asked. */
type Dispatched = { steps: Set<string>; readAt: number; reading?: Promise<unknown> };

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
        // The legacy store answers for a run it lacks, such as one whose `run_created` is still in flight.
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

  // A replay that starts thousands of steps publishes them all at once, which as a call each
  // overloaded the run's store.
  const publish = gatherTurns((queueName: string, messages: { message: QueuePayload; opts: QueueOptions }[]) => queueBatch(queueName, messages));

  // A replay of a spec 8 run publishes every step still pending again, one at a time, under the
  // step's dispatch key. A step whose message the queue already has needs nothing, so a flow request
  // answers those from the run's dispatched steps, read at most once per DISPATCHED_STEPS_READ_MS,
  // instead of a call per step, which took minutes for thousands of steps.
  const dispatched = new WeakMap<FlowRequest, Map<string, Dispatched>>();
  const isDispatched = async (runId: string, stepId: string): Promise<boolean> => {
    const request = currentRequest();
    if (request === undefined) return false;
    const runs = dispatched.get(request) ?? new Map<string, Dispatched>();
    dispatched.set(request, runs);
    const known = runs.get(runId) ?? { steps: new Set<string>(), readAt: Number.NEGATIVE_INFINITY };
    runs.set(runId, known);
    if (known.steps.has(stepId)) return true;
    if (known.reading === undefined && Date.now() - known.readAt >= DISPATCHED_STEPS_READ_MS) {
      known.readAt = Date.now();
      known.reading = onRun(env.WORLD_STORE, runId, (store) => store.dispatchedSteps(runId))
        .then((steps) => steps.forEach((step) => known.steps.add(step)))
        // A failed read leaves the publish to the store.
        .catch((error: unknown) => console.error('Could not read dispatched steps', { runId, error }))
        .finally(() => { known.reading = undefined; });
    }
    await known.reading;
    return known.steps.has(stepId);
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
      if (hold === undefined && startsInline(env, message, rest)) {
        return startInline(env, queueName, message, rest, async (delaySeconds) => {
          const result = await publish(queueName, { message, opts: { ...rest, delaySeconds } });
          if (result.error !== undefined) throw new Error(result.error);
        });
      }
      const target = rest.deploymentId;
      if (target !== undefined && !reachable(env, target)) {
        throw new WorkflowWorldError(`Deployment ${target} is unavailable`, { status: 404, code: DEPLOYMENT_UNAVAILABLE });
      }
      if (hold === undefined && 'stepId' in message && message.stepId && message.stepName && !rest.delaySeconds && target === undefined) {
        // The SDK's dispatch key for the step (`stepDispatchIdempotencyKey`), which the store's outbox publishes under too.
        const key = `${message.stepId}:${fnv1a32Hex(message.stepName)}`;
        if (rest.idempotencyKey === key && await isDispatched(message.runId, message.stepId)) {
          return { messageId: MessageId.parse(await messageIdFor(`${queueName}:${key}`)) };
        }
      }
      const result = await publish(queueName, { message, opts: rest });
      if (result.error !== undefined) throw new Error(result.error);
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
      // A hook resume carries its payload to the run's executor, which would run out of memory on
      // one far over the limit before the write that rejects it, so it is refused here.
      const bytes = isRecord(payload) && payload.payload instanceof Uint8Array ? payload.payload.byteLength : 0;
      if (bytes > PAYLOAD_MAX_BYTES) throw tooLarge(`A ${bytes}-byte hook payload is over the ${PAYLOAD_MAX_BYTES}-byte payload limit`);
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

/** Validates one message and computes what the shard stores, or why it cannot be queued. */
async function prepare(
  env: QueueEnv, queueName: string, message: QueuePayload,
  { deploymentId: target = env.DEPLOYMENT_ID, delaySeconds: delay = 0, idempotencyKey }: QueueOptions,
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


/**
 * Whether a start runs its first delivery in the caller (`startInline`): a spec 9 start for this
 * deployment, which carries the run's input, so the delivery needs nothing the store has not been
 * sent already. `INLINE_START=0` sends every start through the queue.
 */
function startsInline(env: QueueEnv, message: QueuePayload, { delaySeconds, deploymentId }: QueueOptions): boolean {
  if (env.INLINE_START === '0' || delaySeconds || (deploymentId !== undefined && deploymentId !== env.DEPLOYMENT_ID)) return false;
  if (!('runInput' in message) || !isRecord(message.runInput)) return false;
  const { specVersion } = message.runInput;
  return typeof specVersion === 'number' && specVersion >= SPEC_VERSION_SINGLE_ORCHESTRATOR;
}

/**
 * Runs a start's first delivery now, through this Worker's consumer, in place of the store, queue
 * shard and runner it would otherwise cross: at low traffic each of those can be an idle object
 * that takes hundreds of milliseconds to come back. The queue holds the message as the backstop
 * (`experimental_queueHold`) and delivers it only if this delivery does not settle it before its
 * lease ends, as when this invocation is cut off. The delivery starts beside the hold rather than
 * after it, and the queue takes the run over after `INLINE_START_BUDGET_MS`, inside the caller's
 * `waitUntil`. The spec 9 fence keeps one orchestrator, and `run_started` settles the store's start
 * row as on the queue path.
 */
async function startInline(
  env: QueueEnv, queueName: ValidQueueName, message: QueuePayload, opts: QueueOptions, requeue: (delaySeconds: number) => Promise<void>,
) {
  const leaseMs = DEFAULT_HOLD_LEASE_SECONDS * 1000;
  const ready = await prepare(env, queueName, message, { ...opts, delaySeconds: DEFAULT_HOLD_LEASE_SECONDS });
  if ('error' in ready) throw new Error(ready.error);
  const { id, encoded, targetDeployment } = ready.request;
  const shard = () => env.QUEUE_SHARD.getByName(ready.shard, objectOptions());
  const enqueuedAt = Date.now();
  const deadline = enqueuedAt + INLINE_START_BUDGET_MS;
  const consume = (attempt: number, deliveryCount: number) => env.CONSUMER.consume([{
    id, attempt, deliveryCount, enqueuedAt, body: { queueName, encoded, targetDeployment },
  }]).then(([outcome]) => outcome, (error: unknown): DeliveryOutcome => ({ kind: DeliveryResult.Failed, error: String(error) }));
  const run = consume(1, 1);
  const accepted = shard().hold(ready.request).then((result) => {
    if ('error' in result) throw new Error(result.error);
    return result;
  });
  waitUntil(accepted.then(async (result) => {
    // A repeat of an accepted key is not held: the queue runs it, and this delivery was a duplicate.
    if (!result.fresh) return;
    const renewal = setInterval(() => {
      shard().renewHold(result.id, leaseMs).catch((error: unknown) => console.error('Hold renewal failed', error));
    }, leaseMs / 2);
    try {
      await shard().beginHeld(result.id, leaseMs);
      for (let delivery = run; ;) {
        // The SDK hands off only after minutes, past the caller's cut-off. At the budget the queue
        // delivers the run to its runner instead, whose orchestrator takes over: the fence refuses
        // this delivery's next write, which ends it.
        const outcome = await Promise.race([delivery, scheduler.wait(Math.max(0, deadline - Date.now())).then(() => undefined)]);
        if (outcome === undefined) return await shard().settleHold(result.id, 0, true);
        switch (outcome.kind) {
          case DeliveryResult.Done:
            return await shard().settleHold(result.id, null, false);
          case DeliveryResult.Wake:
            // A short wake, such as a sleep or steps still running, is delivered here too, where the
            // run's route and objects are warm, rather than through the queue's cold path.
            if (outcome.seconds > 0 && Date.now() + outcome.seconds * 1000 < deadline) {
              await scheduler.wait(outcome.seconds * 1000);
              const { deliveries, handoffs } = await shard().beginHeld(result.id, leaseMs);
              delivery = consume(deliveries - handoffs, deliveries);
              continue;
            }
            // A spec 9 wake of 0 is a handoff, which is not an attempt.
            return await shard().settleHold(result.id, outcome.seconds * 1000, outcome.seconds === 0);
          default:
            console.error('Inline start failed; the queue delivers it', { id, error: outcome.error });
            return await shard().settleHold(result.id, HELD_FAILURE_RETRY_MS, false);
        }
      }
    } finally {
      clearInterval(renewal);
    }
  }, async (error: unknown) => {
    // No hold, so no backstop: once the delivery ends, send what it left to the queue as usual.
    console.error('Could not hold an inline start', { id, error });
    const outcome = await run;
    if (outcome?.kind !== DeliveryResult.Done) await requeue(outcome?.kind === DeliveryResult.Wake ? outcome.seconds : HELD_FAILURE_RETRY_MS / 1000);
  }).catch((error: unknown) => console.error('Could not settle an inline start; the queue delivers it', { id, error })));
  // The run is under way either way, so start() reports it accepted.
  return { messageId: MessageId.parse(await accepted.then(({ id: accepted }) => accepted, () => id)) };
}
