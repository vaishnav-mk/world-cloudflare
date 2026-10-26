import { DurableObject } from 'cloudflare:workers';
import { DurabilityFanout, DurabilityScheduler, type DurabilityFanoutMessage, type StoredMessage } from 'durability';
import { DurabilityRouting, type EnqueueResult as RoutedResult } from 'durability/routing';
import { parse } from 'devalue';
import migrations from '../../drizzle/queue/migrations.js';
import {
  DEFAULT_QUEUE_MAX_ATTEMPTS, FLOW_ATTEMPT_TIMEOUT_MS, FLOW_TARGET, MAX_QUEUE_RETRY_DELAY_MS,
} from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { ClaimStatus, DeliveryResult, MessageKind } from '../enums.js';
import { unreachable } from '../lib/unreachable.js';
import { onRun } from '../store/route.js';
import type { Delivery, EnqueueRequest, EnqueueResult, Envelope, WorldEnv } from '../types.js';
import { DedupTable, type OutgoingMessage } from './dedup.js';
import { InflightTable } from './inflight.js';

type FanoutMessage = DurabilityFanoutMessage<Envelope>;

/** What `enqueueMany` does with one message: settled already, or sent to the fanout. */
type Plan = { done: EnqueueResult } | { message: OutgoingMessage; key?: string; fresh: boolean };

/** The run wake a dead-lettered message carried, so the store can reopen its outbox row. */
type DeadLetter =
  | { kind: MessageKind.Step; runId: string; stepId: string; stepName: string }
  | { kind: MessageKind.WaitContinuation; runId: string; correlationId: string }
  | { kind: MessageKind.HookWake; runId: string };

/** One queue shard: delayed, retried delivery of its runs' messages to the `CONSUMER` entrypoint. */
export class QueueShard extends DurableObject<WorldEnv> {
  readonly #dedup = new DedupTable(this.ctx.storage.sql);
  readonly #inflight = new InflightTable(this.ctx.storage.sql);
  readonly #scheduler = new DurabilityScheduler({ context: this.ctx });
  // Feeds the load snapshots `push` returns to the routing client.
  readonly #routing = new DurabilityRouting();
  readonly #fanout = new DurabilityFanout<Envelope>({
    scheduler: this.#scheduler,
    routing: this.#routing,
    retries: {
      maxAttempts: Number(this.env.QUEUE_MAX_ATTEMPTS ?? DEFAULT_QUEUE_MAX_ATTEMPTS),
      delay: (attempt) => Math.min(MAX_QUEUE_RETRY_DELAY_MS, attempt * 1000),
    },
    targets: {
      [FLOW_TARGET]: {
        ackOnReturn: true, attemptTimeoutMs: FLOW_ATTEMPT_TIMEOUT_MS, deliver: (messages) => this.#deliver(messages),
      },
    },
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
      const result = await this.#fanout.enqueue(sends.map(({ message: { id, ...body }, key }) => ({ id, body, deduplicationKey: key })));
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
  #plan({ id, queueName, encoded, delayMs, targetDeployment, deduplicationKey, payloadDigest }: EnqueueRequest): Plan {
    const message: OutgoingMessage = { id, queueName, encoded, delayMs, targetDeployment };
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

  /** Holds messages until their delay passes, then hands the due ones to the consumer. */
  async #deliver(messages: FanoutMessage[]): Promise<void> {
    const due: FanoutMessage[] = [];
    for (const message of messages) {
      const deadline = message.body.due ?? (message.enqueuedAt + (message.body.delayMs ?? Number.NaN));
      if (!Number.isFinite(deadline)) throw new Error('Invalid persisted queue deadline');
      const remaining = deadline - Date.now();
      if (remaining > 0) await message.retry(remaining);
      else due.push(message);
    }
    if (!due.length) return;
    const deliveries: Delivery[] = due.map(({ id, attempt, enqueuedAt, body }) => ({ id, attempt, enqueuedAt, body }));
    this.#inflight.add(deliveries);
    const outcomes = await this.env.CONSUMER.consume(deliveries).finally(() => {
      for (const { id } of deliveries) this.#inflight.remove(id);
    });
    if (outcomes.length !== due.length) throw new Error('Incomplete queue delivery response');
    for (const [index, outcome] of outcomes.entries()) {
      const message = due[index];
      switch (outcome.kind) {
        case DeliveryResult.Done:
          break;
        case DeliveryResult.Wake:
          await message.retry(outcome.seconds * 1000);
          break;
        case DeliveryResult.Failed:
          console.error('Flow delivery failed', { messageId: message.id, attempt: message.attempt, error: outcome.error });
          await message.retry(Math.min(MAX_QUEUE_RETRY_DELAY_MS, message.attempt * 1000));
          break;
        default:
          unreachable(outcome);
      }
    }
  }

  /**
   * A fresh instance runs nothing yet, so deliveries still recorded were cut off by a reset (a
   * deploy, or a storage error) and would otherwise wait out their attempt timeout.
   */
  async #redeliverOrphans(): Promise<void> {
    for (const { id, ...message } of this.#inflight.all()) {
      // A new ID: the cut-off delivery stays claimed until it times out, and the SDK tolerates the copy.
      const [result] = await this.enqueueMany([{ ...message, id: crypto.randomUUID() }]);
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
    // The directory answers for a run it lacks, with nothing to reopen.
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
