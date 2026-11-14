import { DurableObject } from 'cloudflare:workers';
import { DurabilityFanout, DurabilityScheduler, type FanoutTarget } from 'durability';
import { DurabilityRouting, type EnqueueResult as RoutedResult } from 'durability/routing';
import migrations from '../../drizzle/queue/migrations.js';
import { FLOW_ATTEMPT_TIMEOUT_MS, FLOW_TARGET, FRESH_RUNNER_ARM_INTERVAL_MS, STEPS_CHANNEL } from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { ClaimStatus } from '../enums.js';
import { groupBy } from '../lib/group.js';
import { readPayload, replayOf } from '../lib/messages.js';
import { objectOptions, stepRunnerFor } from '../lib/shards.js';
import { unreachable } from '../lib/unreachable.js';
import type { EnqueueRequest, EnqueueResult, Envelope, WorldEnv } from '../types.js';
import { deadLetter } from './dead-letter.js';
import { DedupTable, type OutgoingMessage } from './dedup.js';
import { Deliverer, retryDelayMs, type RunnerStubs } from './deliver.js';
import { DeliveryCounts, type Counts } from './delivery-counts.js';
import { Holds } from './holds.js';
import { InflightTable } from './inflight.js';

/** What `enqueueMany` does with one message: settled already, or sent to the fanout. */
type Plan = { done: EnqueueResult } | { message: OutgoingMessage; key?: string; fresh: boolean };

/** The single `flow` shard's delivery lanes before queue shards. */
const LEGACY_LANES = Array.from({ length: 8 }, (_, lane) => `${FLOW_TARGET}-${lane}`);

/**
 * One queue shard: delayed, retried delivery of its runs' messages to the runners
 * (`runnerShardFor`), which run the workflow code. A shard holds all of a run's replay messages
 * (`queueShardFor`), delivers them one at a time to the run's runner, and settles the pure ones a
 * later successful replay covers without delivering them. Other messages, such as steps, have a lane
 * of their own, which never waits for a replay.
 */
export class QueueShard extends DurableObject<WorldEnv> {
  readonly #dedup = new DedupTable(this.ctx.storage.sql);
  readonly #inflight = new InflightTable(this.ctx.storage.sql);
  readonly #counts = new DeliveryCounts(this.ctx.storage.sql);
  readonly #holds = new Holds(this.ctx.storage.sql);
  /**
   * Runner stubs first called from this object's alarm. A stub's calls spend the subrequest depth
   * of the request that made its first call, and an alarm starts a new chain, so a delivery through
   * one starts a new chain too, however deep the enqueue behind it was. Without them, a flow
   * enqueues through a store into a queue shard that delivers to a runner that runs a flow, and so
   * on, each cycle spending about five of the platform's few levels.
   */
  readonly #fresh = new Map<string, ReturnType<WorldEnv['RUNNER']['getByName']>>();
  /** Runners a delivery asked for before this object had a fresh stub to them. */
  readonly #wanted = new Set<string>();
  #armedAt = 0;
  readonly #runners: RunnerStubs = {
    get: (name) => {
      const fresh = this.#fresh.get(name);
      if (fresh !== undefined) return fresh;
      this.#wanted.add(name);
      if (Date.now() - this.#armedAt >= FRESH_RUNNER_ARM_INTERVAL_MS) {
        this.#armedAt = Date.now();
        this.ctx.waitUntil(this.#armSoon());
      }
      return this.env.RUNNER.getByName(name, objectOptions());
    },
    forget: (name) => {
      this.#fresh.delete(name);
    },
  };
  readonly #deliverer: Deliverer;
  readonly #scheduler = new DurabilityScheduler({ context: this.ctx });
  // Feeds the load snapshots `push` returns to the routing client.
  readonly #routing = new DurabilityRouting();
  readonly #fanout = this.#lane();
  /**
   * Messages that replay no run (`replayOf`): steps, health checks and a long wait's timer hops. A
   * lane of their own, because the fanout delivers one batch per lane at a time and a replay can
   * hold its batch for long, as a spec 9 orchestrator waiting on these very steps does.
   */
  readonly #steps = this.#lane(STEPS_CHANNEL);

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    const name = ctx.id.name;
    if (name === undefined) throw new Error('Queue shards are reached by name');
    this.#deliverer = new Deliverer(env, this.#runners, stepRunnerFor(name), this.#counts, this.#holds, this.#inflight);
    migrateOnStart(ctx, migrations);
    ctx.blockConcurrencyWhile(() => this.#redeliverOrphans());
  }

  /** A fanout lane, whose batches of due messages `Deliverer` delivers. */
  #lane(channel?: string): DurabilityFanout<Envelope> {
    return new DurabilityFanout<Envelope>({
      ...(channel === undefined ? {} : { channel }),
      scheduler: this.#scheduler,
      routing: this.#routing,
      retries: {
        // Holds, delay waits and handoffs take fanout attempts too, so `Deliverer` enforces the cap
        // on deliveries itself and the fanout's own cap is out of reach.
        maxAttempts: Number.MAX_SAFE_INTEGER,
        delay: retryDelayMs,
      },
      // `flow`, plus the lanes the single `flow` shard delivered to before queue shards, so messages
      // it still holds for them are delivered. New messages go to `flow` only.
      targets: Object.fromEntries([FLOW_TARGET, ...LEGACY_LANES].map((target): [string, FanoutTarget<Envelope>] => [target, {
        ackOnReturn: true, attemptTimeoutMs: FLOW_ATTEMPT_TIMEOUT_MS, deliver: (messages) => this.#deliverer.deliver(messages),
      }])),
      dlq: (message) => deadLetter(this.env, message),
    });
  }

  /** Starts the object, so a fresh instance after a reset redelivers what the reset cut off. */
  wake(): void {}

  /** Enqueues a message. With a deduplication key, a repeat returns the first accepted message's ID. */
  async enqueue(
    id: string, queueName: string, encoded: string, delayMs: number,
    targetDeployment?: string, deduplicationKey?: string, payloadDigest?: string,
  ): Promise<{ id: string; fresh: boolean }> {
    return this.#enqueueOne({ id, queueName, encoded, delayMs, targetDeployment, deduplicationKey, payloadDigest });
  }

  /** Enqueues one message, or throws why it was not. */
  async #enqueueOne(request: EnqueueRequest): Promise<{ id: string; fresh: boolean }> {
    const [result] = await this.enqueueMany([request]);
    if ('error' in result) throw new Error(result.error);
    return result;
  }

  /**
   * Enqueues messages in one fanout transaction per lane, each with its own outcome. A message whose
   * deduplication key was accepted before returns that message's ID instead.
   */
  async enqueueMany(messages: EnqueueRequest[]): Promise<EnqueueResult[]> {
    const plans = messages.map((request) => this.#plan(request));
    const sends = plans.flatMap((plan) => ('message' in plan ? [plan] : []));
    const ids = new Map<Plan, string>();
    const failures = new Map<Plan, string>();
    const lanes = groupBy(sends, ({ message }) => (replayOf(readPayload(message.encoded)) === undefined ? STEPS_CHANNEL : 'replays'));
    await Promise.all([...lanes].map(async ([lane, group]) => {
      const result = await (lane === STEPS_CHANNEL ? this.#steps : this.#fanout).enqueue(
        group.map(({ message: { id, ...body }, key }) => ({ id, body, deduplicationKey: key })), { targets: [FLOW_TARGET] },
      );
      if (!result.success) {
        for (const send of group) failures.set(send, result.error.message);
        return;
      }
      for (const [index, send] of group.entries()) {
        ids.set(send, result.value[index]);
        if (send.key) this.#dedup.markAccepted(send.key);
      }
    }));
    return plans.map((plan) => {
      if ('done' in plan) return plan.done;
      const id = ids.get(plan);
      return id === undefined ? { error: failures.get(plan) ?? 'Message was not enqueued' } : { id, fresh: plan.fresh };
    });
  }

  /**
   * Enqueues a message held for its sender (`experimental_hold`), delayed by the hold's lease: the
   * sender runs it and renews the lease, and the queue delivers it only if the lease lapses before
   * the sender settles it. A repeat of an accepted key is not held.
   */
  async hold(request: EnqueueRequest): Promise<EnqueueResult> {
    const [result] = await this.enqueueMany([request]);
    if ('fresh' in result && result.fresh) this.#holds.add({ ...request, id: result.id }, Date.now() + request.delayMs);
    return result;
  }

  /** The sender starts running its held message: that is the message's first delivery. */
  async beginHeld(id: string): Promise<Counts> {
    return this.#counts.begin(id, Date.now());
  }

  /** Extends the lease of a message its sender still runs. Returns whether it is still held. */
  async renewHold(id: string, leaseMs: number): Promise<boolean> {
    return this.#holds.renew(id, Date.now() + leaseMs);
  }

  /**
   * Settles a held message: done (`delayMs` null), or delivered again after `delayMs`, as a copy
   * that keeps its ID, which a handoff leaves out of the attempt.
   */
  async settleHold(id: string, delayMs: number | null, handoff: boolean): Promise<void> {
    const message = this.#holds.message(id);
    if (message === undefined) return;
    if (delayMs !== null) {
      if (handoff) this.#counts.handoff(id);
      await this.#enqueueOne({ ...message, id: crypto.randomUUID(), delayMs, messageId: id });
    }
    this.#holds.settle(id);
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

  async alarm(info?: AlarmInvocationInfo): Promise<void> {
    await this.#freshen();
    await this.#scheduler.alarm(info);
  }

  /** Makes the runner stubs deliveries asked for, first calling each from here, the alarm. */
  async #freshen(): Promise<void> {
    await Promise.all([...this.#wanted].map(async (name) => {
      const stub = this.env.RUNNER.getByName(name, objectOptions());
      try {
        await stub.wake();
        this.#fresh.set(name, stub);
        this.#wanted.delete(name);
      } catch (error) {
        console.error('Could not reach a runner from the alarm', { runner: name, error });
      }
    }));
  }

  /** Brings the alarm forward, so it makes the wanted stubs. The scheduler may move it later, which only delays them. */
  async #armSoon(): Promise<void> {
    try {
      const at = await this.ctx.storage.getAlarm();
      if (at === null || at > Date.now()) await this.ctx.storage.setAlarm(Date.now());
    } catch (error) {
      console.error('Could not arm the alarm for fresh runner stubs', error);
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
      await this.#enqueueOne({ ...message, id: crypto.randomUUID(), messageId: id });
      this.#inflight.remove(id);
    }
  }
}
