import { DurableObject } from 'cloudflare:workers';
import { DurabilityFanout, DurabilityScheduler, type DurabilityFanoutMessage, type FanoutTarget, type StoredMessage } from 'durability';
import { DurabilityRouting, type EnqueueResult as RoutedResult } from 'durability/routing';
import { parse } from 'devalue';
import { isSingleOrchestratorMessage } from '@workflow/world';
import migrations from '../../drizzle/queue/migrations.js';
import {
  DEFAULT_QUEUE_MAX_ATTEMPTS, DELIVERY_COUNT_RETENTION_MS, FLOW_ATTEMPT_TIMEOUT_MS, FLOW_TARGET, MAX_QUEUE_RETRY_DELAY_MS,
  REPLAY_MARKS_MAX,
} from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { ClaimStatus, DeliveryResult, MessageKind } from '../enums.js';
import { replayOf } from '../lib/messages.js';
import { unreachable } from '../lib/unreachable.js';
import { onRun } from '../store/route.js';
import type { Delivery, EnqueueRequest, EnqueueResult, Envelope, WorldEnv } from '../types.js';
import { DedupTable, type OutgoingMessage } from './dedup.js';
import { DeliveryCounts } from './delivery-counts.js';
import { InflightTable } from './inflight.js';

type FanoutMessage = DurabilityFanoutMessage<Envelope>;

/** What `enqueueMany` does with one message: settled already, or sent to the fanout. */
type Plan = { done: EnqueueResult } | { message: OutgoingMessage; key?: string; fresh: boolean };

/** The run wake a dead-lettered message carried, so the store can reopen its outbox row. */
type DeadLetter =
  | { kind: MessageKind.Step; runId: string; stepId: string; stepName: string }
  | { kind: MessageKind.WaitContinuation; runId: string; correlationId: string }
  | { kind: MessageKind.HookWake; runId: string };

/** The single `flow` shard's delivery lanes before queue shards. */
const LEGACY_LANES = Array.from({ length: 8 }, (_, lane) => `${FLOW_TARGET}-${lane}`);

/**
 * One queue shard: delayed, retried delivery of its runs' messages to the `CONSUMER` entrypoint. A
 * shard holds all of a run's replay messages (`queueShardFor`), delivers them one at a time, and
 * settles the pure ones a later successful replay covers without delivering them.
 */
export class QueueShard extends DurableObject<WorldEnv> {
  readonly #dedup = new DedupTable(this.ctx.storage.sql);
  readonly #inflight = new InflightTable(this.ctx.storage.sql);
  readonly #counts = new DeliveryCounts(this.ctx.storage.sql);
  /** When each run's latest successful replay started, for the newest `REPLAY_MARKS_MAX` runs. */
  readonly #replayed = new Map<string, number>();
  readonly #scheduler = new DurabilityScheduler({ context: this.ctx });
  // Feeds the load snapshots `push` returns to the routing client.
  readonly #routing = new DurabilityRouting();
  readonly #fanout = new DurabilityFanout<Envelope>({
    scheduler: this.#scheduler,
    routing: this.#routing,
    retries: {
      // Holds, delay waits and handoffs take fanout attempts too, so `#deliver` enforces the cap
      // on deliveries itself and the fanout's own cap is out of reach.
      maxAttempts: Number.MAX_SAFE_INTEGER,
      delay: retryDelayMs,
    },
    // `flow`, plus the lanes the single `flow` shard delivered to before queue shards, so messages
    // it still holds for them are delivered. New messages go to `flow` only.
    targets: Object.fromEntries([FLOW_TARGET, ...LEGACY_LANES].map((target): [string, FanoutTarget<Envelope>] => [target, {
      ackOnReturn: true, attemptTimeoutMs: FLOW_ATTEMPT_TIMEOUT_MS, deliver: (messages) => this.#deliver(messages),
    }])),
    dlq: (message) => this.#deadLetter(message),
  });

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    migrateOnStart(ctx, migrations);
    ctx.blockConcurrencyWhile(() => this.#redeliverOrphans());
  }

  /** Starts the object, so a fresh instance after a reset redelivers what the reset cut off. */
  wake(): void {}

  /** Enqueues a message. With a deduplication key, a repeat returns the first accepted message's ID. */
  async enqueue(
    id: string, queueName: string, encoded: string, delayMs: number,
    targetDeployment?: string, deduplicationKey?: string, payloadDigest?: string,
  ): Promise<{ id: string; fresh: boolean }> {
    const [result] = await this.enqueueMany([{ id, queueName, encoded, delayMs, targetDeployment, deduplicationKey, payloadDigest }]);
    if (result === undefined || 'error' in result) throw new Error(result?.error ?? 'Message was not enqueued');
    return result;
  }

  /**
   * Enqueues messages in one fanout transaction, each with its own outcome. A message whose
   * deduplication key was accepted before returns that message's ID instead.
   */
  async enqueueMany(messages: EnqueueRequest[]): Promise<EnqueueResult[]> {
    const plans = messages.map((request) => this.#plan(request));
    const sends = plans.flatMap((plan) => ('message' in plan ? [plan] : []));
    const ids = new Map<Plan, string>();
    let failure = 'Message was not enqueued';
    if (sends.length) {
      const result = await this.#fanout.enqueue(
        sends.map(({ message: { id, ...body }, key }) => ({ id, body, deduplicationKey: key })), { targets: [FLOW_TARGET] },
      );
      if (result.success) {
        for (const [index, send] of sends.entries()) {
          ids.set(send, result.value[index]);
          if (send.key) this.#dedup.markAccepted(send.key);
        }
      } else {
        failure = result.error.message;
      }
    }
    return plans.map((plan) => {
      if ('done' in plan) return plan.done;
      const id = ids.get(plan);
      return id === undefined ? { error: failure } : { id, fresh: plan.fresh };
    });
  }

  /** `enqueueMany` for durability's routing client, which reads the shard's load from the result. */
  async push(requests: EnqueueRequest[]): Promise<RoutedResult<EnqueueResult[]>> {
    const value = await this.enqueueMany(requests);
    return { success: true, value, load: await this.#fanout.load() };
  }

  /** Claims a message's deduplication key, deciding whether to send it and as what. */
  #plan({ id, queueName, encoded, delayMs, targetDeployment, deduplicationKey, payloadDigest, messageId }: EnqueueRequest): Plan {
    const message: OutgoingMessage = { id, queueName, encoded, delayMs, targetDeployment, messageId };
    if (!deduplicationKey) return { message, fresh: true };
    if (!payloadDigest) return { done: { error: 'Missing queue payload digest' } };
    try {
      const claim = this.#dedup.claim(deduplicationKey, message, payloadDigest);
      switch (claim.status) {
        case ClaimStatus.Accepted:
          return { done: { id: claim.id, fresh: false } };
        case ClaimStatus.Replay:
          return { message: claim.message, key: deduplicationKey, fresh: false };
        case ClaimStatus.New:
          return { message, key: deduplicationKey, fresh: true };
        default:
          return unreachable(claim);
      }
    } catch (error) {
      return { done: { error: String(error) } };
    }
  }

  alarm(info?: AlarmInvocationInfo) {
    return this.#scheduler.alarm(info);
  }

  /**
   * Holds messages until their delay passes, then hands the due ones to the consumer. The fanout
   * delivers one batch per shard at a time and a run's replays all reach its shard, so holding a
   * run's second replay in a batch keeps its replays to one at a time, whatever its spec version.
   * Two replays of a run at once would let the SDK's in-process step single-flight make one request
   * wait on a promise another request settles, which Workers reject. A pure replay never delivered
   * is settled without a delivery once a replay of its run that started after it was due has
   * succeeded: that replay read a log holding whatever the message was sent for. Holds and delays
   * are not deliveries: only `#counts` feeds `attempt`.
   */
  async #deliver(messages: FanoutMessage[]): Promise<void> {
    const due: { message: FanoutMessage; delivery: Delivery; appends: boolean; replays: string | undefined }[] = [];
    const running = new Set<string>();
    const maxAttempts = Number(this.env.QUEUE_MAX_ATTEMPTS ?? DEFAULT_QUEUE_MAX_ATTEMPTS);
    const now = Date.now();
    this.#counts.prune(now - DELIVERY_COUNT_RETENTION_MS);
    for (const message of messages) {
      const deadline = message.body.due ?? (message.enqueuedAt + (message.body.delayMs ?? Number.NaN));
      if (!Number.isFinite(deadline)) throw new Error('Invalid persisted queue deadline');
      const remaining = deadline - now;
      if (remaining > 0) {
        await message.retry(remaining);
        continue;
      }
      const payload = readPayload(message.body.encoded);
      const replay = replayOf(payload);
      const runId = replay?.runId;
      if (runId !== undefined && running.has(runId)) {
        // Behind its run's delivery in this batch; the next batch starts once this one ends.
        await message.retry(0);
        continue;
      }
      const id = message.body.messageId ?? message.id;
      const before = this.#counts.get(id);
      const replayedAt = runId === undefined ? undefined : this.#replayed.get(runId);
      // Left unsettled, so the fanout acks it.
      if (replay?.pure && before.deliveries === 0 && replayedAt !== undefined && deadline <= replayedAt) continue;
      if (before.deliveries - before.handoffs >= maxAttempts) {
        await message.deadLetter();
        continue;
      }
      if (runId !== undefined) running.add(runId);
      const { deliveries, handoffs } = this.#counts.begin(id, now);
      due.push({
        message, appends: isSingleOrchestratorMessage(payload),
        // A start may skip reading the log, so its replay covers no other message.
        replays: replay?.start === false ? runId : undefined,
        delivery: { id, attempt: deliveries - handoffs, deliveryCount: deliveries, enqueuedAt: message.enqueuedAt, body: message.body },
      });
    }
    if (!due.length) return;
    const deliveries = due.map(({ delivery }) => delivery);
    this.#inflight.add(deliveries);
    const startedAt = Date.now();
    const outcomes = await this.env.CONSUMER.consume(deliveries).finally(() => {
      for (const { id } of deliveries) this.#inflight.remove(id);
    });
    if (outcomes.length !== due.length) throw new Error('Incomplete queue delivery response');
    for (const [index, outcome] of outcomes.entries()) {
      const { message, delivery, appends, replays } = due[index];
      switch (outcome.kind) {
        case DeliveryResult.Done:
          if (replays !== undefined) this.#markReplayed(replays, startedAt);
          break;
        case DeliveryResult.Wake:
          // A spec 9 run's `{ timeoutSeconds: 0 }` is a handoff: delivered again at once, not another attempt.
          if (appends && outcome.seconds === 0) this.#counts.handoff(delivery.id);
          await message.retry(outcome.seconds * 1000);
          break;
        case DeliveryResult.Failed:
          console.error('Flow delivery failed', { messageId: delivery.id, attempt: delivery.attempt, error: outcome.error });
          await message.retry(retryDelayMs(delivery.attempt));
          break;
        default:
          unreachable(outcome);
      }
    }
  }

  /** Records that a replay of `runId` that started at `startedAt` succeeded. */
  #markReplayed(runId: string, startedAt: number): void {
    this.#replayed.delete(runId);
    this.#replayed.set(runId, startedAt);
    if (this.#replayed.size > REPLAY_MARKS_MAX) {
      const [oldest] = this.#replayed.keys();
      this.#replayed.delete(oldest);
    }
  }

  /**
   * A fresh instance runs nothing yet, so deliveries still recorded were cut off by a reset (a
   * deploy, or a storage error) and would otherwise wait out their attempt timeout.
   */
  async #redeliverOrphans(): Promise<void> {
    for (const { id, ...message } of this.#inflight.all()) {
      // A new fanout ID: the cut-off delivery stays claimed until it times out, and the SDK tolerates
      // the copy. The copy keeps the message's ID, so its delivery counts as a redelivery.
      const [result] = await this.enqueueMany([{ ...message, id: crypto.randomUUID(), messageId: id }]);
      if (result === undefined || 'error' in result) throw new Error(result?.error ?? 'Orphan was not enqueued');
      this.#inflight.remove(id);
    }
  }

  /** Saves an exhausted message to R2 and reopens the outbox row whose wake it was. */
  async #deadLetter(message: StoredMessage<Envelope>): Promise<void> {
    await this.env.DLQ.put(`workflow/${message.id}`, JSON.stringify(message), { onlyIf: { etagDoesNotMatch: '*' } });
    let payload: unknown;
    try {
      payload = parse(message.body.encoded);
    } catch (error) {
      console.error('Cannot recover corrupt DLQ payload', { messageId: message.id, error });
      return;
    }
    const letter = readDeadLetter(payload);
    if (!letter) return;
    // The legacy store answers for a run it lacks, with nothing to reopen.
    const answersUnknown = { answersUnknown: true };
    switch (letter.kind) {
      case MessageKind.Step:
        await onRun(this.env.WORLD_STORE, letter.runId,
          (store) => store.reopenPublishedStepDispatch(letter.runId, letter.stepId, letter.stepName), answersUnknown);
        break;
      case MessageKind.WaitContinuation:
        await onRun(this.env.WORLD_STORE, letter.runId,
          (store) => store.reopenPublishedWaitDispatch(letter.runId, letter.correlationId), answersUnknown);
        break;
      case MessageKind.HookWake:
        // Reopens the run's start instead if it never started.
        await onRun(this.env.WORLD_STORE, letter.runId, (store) => store.reopenPublishedHookWakes(letter.runId), answersUnknown);
        break;
      default:
        unreachable(letter);
    }
  }
}

/**
 * Backoff after a failed delivery, by attempt: quadratic up to five minutes, so the default 64
 * attempts keep a failing message alive for hours, past the SDK's one-hour step redelivery span.
 */
function retryDelayMs(attempt: number): number {
  return Math.min(MAX_QUEUE_RETRY_DELAY_MS, attempt ** 2 * 1000);
}

/** The SDK payload a message carries, or undefined when it cannot be read. */
function readPayload(encoded: string): unknown {
  try {
    return parse(encoded);
  } catch {
    return undefined;
  }
}

function readDeadLetter(payload: unknown): DeadLetter | undefined {
  if (!isRecord(payload) || typeof payload.runId !== 'string') return undefined;
  const { runId } = payload;
  if (typeof payload.stepId === 'string') {
    return typeof payload.stepName === 'string'
      ? { kind: MessageKind.Step, runId, stepId: payload.stepId, stepName: payload.stepName }
      : undefined;
  }
  if ('waitContinuation' in payload) {
    const continuation = payload.waitContinuation;
    return isRecord(continuation) && typeof continuation.correlationId === 'string'
      ? { kind: MessageKind.WaitContinuation, runId, correlationId: continuation.correlationId }
      : undefined;
  }
  return { kind: MessageKind.HookWake, runId };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object';
}
