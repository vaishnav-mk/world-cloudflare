import { EVENT_PAGE_READS, FAN_OUT_CONCURRENCY, STORE_NAME } from '../constants.js';
import { Semaphore } from '../lib/semaphore.js';
import { runnerShardFor } from '../lib/shards.js';
import { Waiters } from '../lib/waiters.js';
import { shardPusher } from '../queue/push.js';
import type { IndexAnswer, WorldEnv } from '../types.js';
import { Db } from './db.js';
import { HookClaimIntents } from './hook-intents.js';
import { IndexOutbox } from './index-outbox.js';
import { IndexFeed } from './indexing.js';
import { Outbox } from './outbox.js';
import { RunIndex } from './run-index.js';
import { TokenTakeovers } from './takeovers.js';
import { TokenOutbox } from './token-outbox.js';
import { TokenRegistry } from './tokens.js';
import type { WorldStore } from './world-store.js';

/** A store as another store calls it: itself directly, any other over RPC. */
export type Peer = WorldStore | DurableObjectStub<WorldStore>;

/** One store's storage, tables and in-memory state, shared by the modules its RPC methods delegate to. */
export class StoreContext {
  readonly name: string;
  /** The store named `STORE_NAME`, which still holds the runs created before store shards. */
  readonly isLegacy: boolean;
  readonly db: Db;
  readonly outbox: Outbox;
  readonly releases: TokenOutbox;
  readonly tokens: TokenRegistry;
  readonly takeovers: TokenTakeovers;
  readonly intents: HookClaimIntents;
  readonly #writes = new Map<string, { gate: Semaphore; users: number }>();
  readonly index: IndexOutbox;
  readonly runIndex: RunIndex;
  readonly feed: IndexFeed;
  readonly pushQueue: ReturnType<typeof shardPusher>;
  readonly waiters = new Waiters();
  readonly pageReads = new Semaphore(EVENT_PAGE_READS);
  /** The token release push in flight, if any. */
  #pushing: Promise<void> | undefined;
  /** An index shard's round of asking its missing stores to report, while one is in flight. */
  #filling: Promise<void> | undefined;
  readonly #runners: WorldEnv['RUNNER'];
  /** The runners this instance has created or found, by name. */
  readonly #placed = new Map<string, Promise<void>>();

  constructor(
    readonly ctx: DurableObjectState, queueShards: WorldEnv['QUEUE_SHARD'], runners: WorldEnv['RUNNER'], readonly peer: (name: string) => Peer,
  ) {
    const name = ctx.id.name;
    if (name === undefined) throw new Error('World stores are reached by name');
    const sql = ctx.storage.sql;
    this.name = name;
    this.isLegacy = name === STORE_NAME;
    this.db = new Db(sql);
    this.outbox = new Outbox(sql);
    this.releases = new TokenOutbox(sql);
    this.tokens = new TokenRegistry(sql);
    this.takeovers = new TokenTakeovers(sql);
    this.intents = new HookClaimIntents(sql, ctx.storage.kv);
    this.index = new IndexOutbox(sql, this.db);
    this.runIndex = new RunIndex(sql, this.db, name);
    this.feed = new IndexFeed(this);
    this.pushQueue = shardPusher(queueShards);
    this.#runners = runners;
  }

  /**
   * Creates a run's runner near this store before the run's first delivery reaches it. The run's
   * workflow code runs where its runner is and calls this store a few times per step, while
   * `LOCATION_HINT` puts each object anywhere in its region, so the runner is reached without the
   * hint here, and an object is created near its first caller. A runner that exists stays where it
   * is. Placement only speeds runs up, so a failure here is logged, not thrown.
   */
  placeRunner(runId: string): Promise<void> {
    const name = runnerShardFor(runId);
    let placed = this.#placed.get(name);
    if (placed === undefined) {
      placed = this.#runners.getByName(name).wake().catch((error: unknown) => {
        this.#placed.delete(name);
        console.error('Could not create a runner near its store', { runner: name, error });
      });
      this.#placed.set(name, placed);
    }
    return placed;
  }

  /** A store shard answers only for its own runs. The legacy store answers for any run, as before. */
  own(runId: string): void {
    if (!this.isLegacy) this.db.getRun(runId);
  }

  /** Only event writers enter. Token/disposal/status RPCs must remain callable during remote claims. */
  async writeRun<T>(runId: string, work: () => Promise<T>): Promise<T> {
    let entry = this.#writes.get(runId);
    if (entry === undefined) {
      entry = { gate: new Semaphore(1), users: 0 };
      this.#writes.set(runId, entry);
    }
    entry.users++;
    try {
      return await entry.gate.run(work);
    } finally {
      if (--entry.users === 0) this.#writes.delete(runId);
    }
  }

  /** Moves the alarm earlier. A scheduling failure must not acknowledge unrecoverable work. */
  async armAlarm(at: number, purpose: string): Promise<void> {
    try {
      const alarm = await this.ctx.storage.getAlarm();
      if (alarm === null || alarm > at) await this.ctx.storage.setAlarm(at);
    } catch (cause) {
      throw new Error(`Failed to schedule ${purpose}`, { cause });
    }
  }

  /**
   * An index shard's answer. While stores have yet to report, the caller asks every store instead,
   * and this shard asks those stores to report, so it answers alone soon after, such as after a new
   * epoch.
   */
  answerFromIndex<T>(read: () => T): IndexAnswer<T> {
    const answer = this.runIndex.answer(read);
    if (!answer.complete && this.#filling === undefined) {
      const calls = new Semaphore(FAN_OUT_CONCURRENCY);
      this.#filling = Promise.all(this.runIndex.missingSources().map((store) => calls.run(() =>
        this.peer(store).reportIndex(this.name).catch((error: unknown) => console.error(`Could not ask ${store} to report`, error)))))
        .then(() => { this.#filling = undefined; });
      this.ctx.waitUntil(this.#filling);
    }
    return answer;
  }

  /** Sends owed token releases, one push at a time: a release owed during a push rides its loop. A failed one arms the alarm. */
  pushReleases(): Promise<void> {
    this.#pushing ??= this.releases.push((name) => this.peer(name))
      .then((retryAt) => (retryAt === undefined ? undefined : this.armAlarm(retryAt, 'token release retry')))
      .finally(() => { this.#pushing = undefined; });
    return this.#pushing;
  }
}
