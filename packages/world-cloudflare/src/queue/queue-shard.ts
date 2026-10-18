import { DurableObject } from 'cloudflare:workers';
import { DurabilityFanout, DurabilityScheduler, type DurabilityFanoutMessage, type StoredMessage } from 'durability';
import { parse } from 'devalue';
import {
  DEFAULT_QUEUE_MAX_ATTEMPTS, FLOW_TARGET, MAX_QUEUE_RETRY_DELAY_MS, STORE_NAME,
} from '../constants.js';
import { ClaimStatus, MessageKind } from '../enums.js';
import type { Delivery, Envelope, WorldEnv } from '../types.js';
import { DedupTable, type OutgoingMessage } from './dedup.js';

type FanoutMessage = DurabilityFanoutMessage<Envelope>;

/** The run wake a dead-lettered message carried, so the store can reopen its outbox row. */
type DeadLetter =
  | { kind: MessageKind.Step; runId: string; stepId: string; stepName: string }
  | { kind: MessageKind.WaitContinuation; runId: string; correlationId: string }
  | { kind: MessageKind.HookWake; runId: string };

// shortcut: one shard for every queue; partition by run once delivery throughput is measured.
/** Delayed, retried delivery of workflow queue messages to the `CONSUMER` entrypoint. */
export class QueueShard extends DurableObject<WorldEnv> {
  readonly #dedup = new DedupTable(this.ctx.storage.sql);
  readonly #scheduler = new DurabilityScheduler({ context: this.ctx });
  readonly #fanout = new DurabilityFanout<Envelope>({
    scheduler: this.#scheduler,
    retries: {
      maxAttempts: Number(this.env.QUEUE_MAX_ATTEMPTS ?? DEFAULT_QUEUE_MAX_ATTEMPTS),
      delay: (attempt) => Math.min(MAX_QUEUE_RETRY_DELAY_MS, attempt * 1000),
    },
    targets: { [FLOW_TARGET]: { ackOnReturn: true, deliver: (messages) => this.#deliver(messages) } },
    dlq: (message) => this.#deadLetter(message),
  });

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    this.#dedup.migrate();
  }

  /** Enqueues a message. With a deduplication key, a repeat returns the first accepted message's ID. */
  async enqueue(
    id: string, queueName: string, encoded: string, delayMs: number,
    targetDeployment?: string, deduplicationKey?: string, payloadDigest?: string,
  ): Promise<{ id: string; fresh: boolean }> {
    let message: OutgoingMessage = { id, queueName, encoded, delayMs, targetDeployment };
    let fresh = true;
    if (deduplicationKey) {
      if (!payloadDigest) throw new Error('Missing queue payload digest');
      const claim = this.#dedup.claim(deduplicationKey, message, payloadDigest);
      switch (claim.status) {
        case ClaimStatus.Accepted:
          return { id: claim.id, fresh: false };
        case ClaimStatus.Replay:
          message = claim.message;
          fresh = false;
          break;
        case ClaimStatus.New:
          break;
      }
    }
    const { id: messageId, ...body } = message;
    const result = await this.#fanout.enqueue({ id: messageId, body, deduplicationKey });
    if (!result.success) throw new Error(result.error.message);
    if (deduplicationKey) this.#dedup.markAccepted(deduplicationKey);
    return { id: result.value[0], fresh };
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
    const timeouts = await this.env.CONSUMER.consume(deliveries);
    if (timeouts.length !== due.length) throw new Error('Incomplete queue delivery response');
    // A number is the SDK asking to be woken again after that many seconds.
    for (const [index, timeout] of timeouts.entries()) {
      if (typeof timeout === 'number') await due[index].retry(timeout * 1000);
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
    const store = this.env.WORLD_STORE.getByName(STORE_NAME);
    switch (letter.kind) {
      case MessageKind.Step:
        await store.reopenPublishedStepDispatch(letter.runId, letter.stepId, letter.stepName);
        break;
      case MessageKind.WaitContinuation:
        await store.reopenPublishedWaitDispatch(letter.runId, letter.correlationId);
        break;
      case MessageKind.HookWake:
        // Reopens the run's start instead if it never started.
        await store.reopenPublishedHookWakes(letter.runId);
        break;
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
