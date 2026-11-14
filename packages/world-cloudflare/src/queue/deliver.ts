import { isSingleOrchestratorMessage } from '@workflow/world';
import { parse } from 'devalue';
import type { DurabilityFanoutMessage } from 'durability';
import { DEFAULT_QUEUE_MAX_ATTEMPTS, DELIVERY_COUNT_RETENTION_MS, MAX_QUEUE_RETRY_DELAY_MS, REPLAY_MARKS_MAX } from '../constants.js';
import { DeliveryResult } from '../enums.js';
import { groupBy } from '../lib/group.js';
import { isRecord } from '../lib/is-record.js';
import { replayOf } from '../lib/messages.js';
import { runnerShardFor } from '../lib/shards.js';
import { unreachable } from '../lib/unreachable.js';
import type { Delivery, DeliveryOutcome, Envelope, WorldEnv } from '../types.js';
import type { DeliveryCounts } from './delivery-counts.js';
import type { Holds } from './holds.js';
import type { InflightTable } from './inflight.js';

export type FanoutMessage = DurabilityFanoutMessage<Envelope>;

/** A due delivery and the runner it goes to: its run's, in the run's turn, for a replay of `run`. */
type HandOff = { delivery: Delivery; runner: string; run?: string };

/**
 * Backoff after a failed delivery, by attempt: quadratic up to five minutes, so the default 64
 * attempts keep a failing message alive for hours, past the SDK's one-hour step redelivery span.
 */
export function retryDelayMs(attempt: number): number {
  return Math.min(MAX_QUEUE_RETRY_DELAY_MS, attempt ** 2 * 1000);
}

/**
 * A queue shard's deliveries: holds messages until their delay passes, then hands the due ones to
 * the runners. The fanout delivers one batch per shard at a time and a run's replays all reach its
 * shard, so holding a run's second replay in a batch keeps its replays to one at a time, whatever
 * its spec version. Two replays of a run at once would let the SDK's in-process step single-flight
 * make one request wait on a promise another request settles, which Workers reject. A pure replay
 * never delivered is settled without a delivery once a replay of its run that started after it was
 * due has succeeded: that replay read a log holding whatever the message was sent for. Holds and
 * delays are not deliveries: only `DeliveryCounts` feeds `attempt`.
 */
export class Deliverer {
  readonly #env: Pick<WorldEnv, 'QUEUE_MAX_ATTEMPTS' | 'RUNNER'>;
  readonly #counts: DeliveryCounts;
  readonly #holds: Holds;
  readonly #inflight: InflightTable;
  /** When each run's latest successful replay started, for the newest `REPLAY_MARKS_MAX` runs. */
  readonly #replayed = new Map<string, number>();

  constructor(env: Pick<WorldEnv, 'QUEUE_MAX_ATTEMPTS' | 'RUNNER'>, counts: DeliveryCounts, holds: Holds, inflight: InflightTable) {
    this.#env = env;
    this.#counts = counts;
    this.#holds = holds;
    this.#inflight = inflight;
  }

  async deliver(messages: FanoutMessage[]): Promise<void> {
    const due: (HandOff & { message: FanoutMessage; appends: boolean; replays: string | undefined })[] = [];
    const running = new Set<string>();
    const maxAttempts = Number(this.#env.QUEUE_MAX_ATTEMPTS ?? DEFAULT_QUEUE_MAX_ATTEMPTS);
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
      // A held message's queued original is only its sender's backstop.
      const held = message.body.messageId === undefined ? this.#holds.find(message.id) : undefined;
      if (held !== undefined && !held.done && held.leaseUntil > now) {
        await message.retry(held.leaseUntil - now);
        continue;
      }
      if (held !== undefined) this.#holds.remove(message.id);
      if (held?.done) continue;
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
        // Other messages spread over the run's epoch by ID; a health check names no run.
        ...(runId === undefined ? { runner: runnerShardFor(runIdOf(payload), id) } : { runner: runnerShardFor(runId), run: runId }),
      });
    }
    if (!due.length) return;
    const deliveries = due.map(({ delivery }) => delivery);
    this.#inflight.add(deliveries);
    const startedAt = Date.now();
    const outcomes = await this.#handOff(due).finally(() => {
      for (const { id } of deliveries) this.#inflight.remove(id);
    });
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

  /**
   * Hands each delivery to its runner, and returns their outcomes in order: a replay to its run's
   * runner on its own, the rest together to each runner. A runner call that fails fails its own
   * deliveries, which are delivered again.
   */
  async #handOff(due: HandOff[]): Promise<DeliveryOutcome[]> {
    const outcomes: DeliveryOutcome[] = [];
    const failed = (error: unknown): DeliveryOutcome => ({ kind: DeliveryResult.Failed, error: String(error) });
    const batches = groupBy(due.flatMap((entry, index) => entry.run === undefined ? [{ ...entry, index }] : []), ({ runner }) => runner);
    await Promise.all([
      ...due.map(async ({ delivery, runner, run }, index) => {
        if (run === undefined) return;
        outcomes[index] = await this.#env.RUNNER.getByName(runner).replay(run, delivery).catch(failed);
      }),
      ...[...batches].map(async ([runner, batch]) => {
        const results = await this.#env.RUNNER.getByName(runner).consume(batch.map(({ delivery }) => delivery))
          .then((settled) => {
            if (settled.length !== batch.length) throw new Error('Incomplete queue delivery response');
            return settled;
          })
          .catch((error: unknown) => batch.map(() => failed(error)));
        for (const [position, { index }] of batch.entries()) outcomes[index] = results[position];
      }),
    ]);
    return outcomes;
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
}

/** The SDK payload a message carries, or undefined when it cannot be read. */
function readPayload(encoded: string): unknown {
  try {
    return parse(encoded);
  } catch {
    return undefined;
  }
}

/** The run a message names, or `''` for one that names none, such as a health check. */
function runIdOf(payload: unknown): string {
  return isRecord(payload) && typeof payload.runId === 'string' ? payload.runId : '';
}
