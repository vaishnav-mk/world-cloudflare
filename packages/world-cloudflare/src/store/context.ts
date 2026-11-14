import { EVENT_PAGE_READS, STORE_NAME } from '../constants.js';
import { Semaphore } from '../lib/semaphore.js';
import { Waiters } from '../lib/waiters.js';
import { shardPusher } from '../queue/push.js';
import type { WorldEnv } from '../types.js';
import { Db } from './db.js';
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
  readonly index: IndexOutbox;
  readonly runIndex: RunIndex;
  readonly feed: IndexFeed;
  readonly pushQueue: ReturnType<typeof shardPusher>;
  readonly waiters = new Waiters();
  readonly pageReads = new Semaphore(EVENT_PAGE_READS);
  /** The token release push in flight, if any. */
  #pushing: Promise<void> | undefined;

  constructor(readonly ctx: DurableObjectState, queueShards: WorldEnv['QUEUE_SHARD'], readonly peer: (name: string) => Peer) {
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
    this.index = new IndexOutbox(sql, this.db);
    this.runIndex = new RunIndex(sql, this.db, name);
    this.feed = new IndexFeed(this);
    this.pushQueue = shardPusher(queueShards);
  }

  /** A store shard answers only for its own runs. The legacy store answers for any run, as before. */
  own(runId: string): void {
    if (!this.isLegacy) this.db.getRun(runId);
  }

  /** Moves the alarm earlier when needed. On failure the cron covers the legacy store's rows, the next write a shard's. */
  async armAlarm(at: number, purpose: string): Promise<void> {
    try {
      const alarm = await this.ctx.storage.getAlarm();
      if (alarm === null || alarm > at) await this.ctx.storage.setAlarm(at);
    } catch (error) {
      console.error(`Failed to schedule ${purpose}`, error);
    }
  }

  /** Sends owed token releases, one push at a time: a release owed during a push rides its loop. A failed one arms the alarm. */
  pushReleases(): Promise<void> {
    this.#pushing ??= this.releases.push((name) => this.peer(name))
      .then((retryAt) => (retryAt === undefined ? undefined : this.armAlarm(retryAt, 'token release retry')))
      .finally(() => { this.#pushing = undefined; });
    return this.#pushing;
  }
}
