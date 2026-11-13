import { DurableObject } from 'cloudflare:workers';
import { DurabilityFanout, DurabilityScheduler, type DurabilityFanoutMessage, type FanoutTarget, type StoredMessage } from 'durability';
import { DurabilityRouting, type EnqueueResult as RoutedResult } from 'durability/routing';
import { parse } from 'devalue';
import migrations from '../../drizzle/queue/migrations.js';
import {
  DEFAULT_QUEUE_MAX_ATTEMPTS, FLOW_ATTEMPT_TIMEOUT_MS, FLOW_TARGET, MAX_QUEUE_RETRY_DELAY_MS, REPLAY_MARKS_MAX,
} from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { ClaimStatus, DeliveryResult, MessageKind } from '../enums.js';
import { groupBy } from '../lib/group.js';
import { isRecord, replayOf, type Replay } from '../lib/messages.js';
import { unreachable } from '../lib/unreachable.js';
import { onRun } from '../store/route.js';
import type { Delivery, DeliveryOutcome, EnqueueRequest, EnqueueResult, Envelope, WorldEnv } from '../types.js';
import { DedupTable, type OutgoingMessage } from './dedup.js';
import { InflightTable } from './inflight.js';

type FanoutMessage = DurabilityFanoutMessage<Envelope>;

/** A due message that replays its run. */
type DueReplay = Replay & { message: FanoutMessage; deadline: number };

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
  /** Each run's latest turn of replay deliveries in this instance; the next turn waits for it. */
  readonly #turns = new Map<string, Promise<void>>();
  /** When each run's latest successful replay started, for the newest `REPLAY_MARKS_MAX` runs. */
  readonly #replayed = new Map<string, number>();
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

  /**
   * Holds messages until their delay passes, then hands the due ones to the consumer. Step messages
   * and other messages without a replay go together; each run's replays go in its own turn.
   */
  async #deliver(messages: FanoutMessage[]): Promise<void> {
    const due: { message: FanoutMessage; deadline: number }[] = [];
    for (const message of messages) {
      const deadline = message.body.due ?? (message.enqueuedAt + (message.body.delayMs ?? Number.NaN));
      if (!Number.isFinite(deadline)) throw new Error('Invalid persisted queue deadline');
      const remaining = deadline - Date.now();
      if (remaining > 0) await message.retry(remaining);
      else due.push({ message, deadline });
    }
    if (!due.length) return;
    // Every due message, including those waiting for their run's turn, so a reset redelivers them all.
    this.#inflight.add(due.map(({ message }) => toDelivery(message)));
    try {
      const others: FanoutMessage[] = [];
      const replays: DueReplay[] = [];
      for (const entry of due) {
        const replay = replayOf(readPayload(entry.message.body.encoded));
        if (replay === undefined) others.push(entry.message);
        else replays.push({ ...replay, ...entry });
      }
      const turns = [...groupBy(replays, ({ runId }) => runId)]
        .map(([runId, group]) => this.#inTurn(runId, () => this.#replay(runId, group)));
      // Every delivery settles before this returns, because the fanout acks what is left unsettled then.
      for (const result of await Promise.allSettled([this.#consume(others), ...turns])) {
        if (result.status === 'rejected') throw result.reason;
      }
    } finally {
      for (const { message } of due) this.#inflight.remove(message.id);
    }
  }

  /** Runs `work` after the run's earlier turns in this instance settle, so its replays never overlap. */
  #inTurn(runId: string, work: () => Promise<void>): Promise<void> {
    const turn = (this.#turns.get(runId) ?? Promise.resolve()).then(work, work);
    this.#turns.set(runId, turn);
    const forget = () => {
      if (this.#turns.get(runId) === turn) this.#turns.delete(runId);
    };
    void turn.then(forget, forget);
    return turn;
  }

  /**
   * Delivers one run's replays one at a time. A pure replay message on its first attempt is settled
   * without a delivery once a replay that started after it was due has succeeded: that replay read a
   * log holding whatever the message was sent for. Messages that must be delivered go first, since
   * their replays can cover the pure ones.
   */
  async #replay(runId: string, group: DueReplay[]): Promise<void> {
    for (const { message, deadline, pure, start } of [...group.filter((m) => !m.pure), ...group.filter((m) => m.pure)]) {
      const replayedAt = this.#replayed.get(runId);
      // Left unsettled, so the fanout acks it.
      if (pure && message.attempt === 1 && replayedAt !== undefined && deadline <= replayedAt) continue;
      const startedAt = Date.now();
      const [outcome] = await this.#consume([message]);
      if (outcome.kind === DeliveryResult.Done && !start) {
        this.#replayed.delete(runId);
        this.#replayed.set(runId, startedAt);
        if (this.#replayed.size > REPLAY_MARKS_MAX) {
          const [oldest] = this.#replayed.keys();
          this.#replayed.delete(oldest);
        }
      }
    }
  }

  /** Hands messages to the consumer together and settles each by its own outcome, returned in order. */
  async #consume(messages: FanoutMessage[]): Promise<DeliveryOutcome[]> {
    if (!messages.length) return [];
    const outcomes = await this.env.CONSUMER.consume(messages.map(toDelivery));
    if (outcomes.length !== messages.length) throw new Error('Incomplete queue delivery response');
    for (const [index, outcome] of outcomes.entries()) {
      const message = messages[index];
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
    return outcomes;
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

function toDelivery({ id, attempt, enqueuedAt, body }: FanoutMessage): Delivery {
  return { id, attempt, enqueuedAt, body };
}

/** A stored message's payload, or `undefined` if it does not decode; the consumer then fails it. */
function readPayload(encoded: string): unknown {
  try {
    return parse(encoded);
  } catch {
    return undefined;
  }
}
