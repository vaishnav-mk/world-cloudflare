import { DISPATCH_RECOVERY_DELAY_MS, INDEX_BACKFILL_BATCH, INDEX_PUSH_WAIT_MS } from '../constants.js';
import { BackfillStage } from '../enums.js';
import { indexShardsOf } from '../lib/shards.js';
import type { IndexBackfill } from '../types.js';
import type { StoreContext } from './context.js';

/** Storage key for a store's `IndexBackfill`. */
const INDEX_BACKFILL = 'index-backfill';

/**
 * A store's side of the list index: every store owes the index shards the list rows of its runs and
 * hooks (`IndexOutbox`), pushes them after each write, and backfills the runs it held before the index.
 */
export class IndexFeed {
  readonly #store: StoreContext;
  /** The last push round, and the round queued after it that has not started yet, if any. */
  #round: Promise<void> = Promise.resolve();
  #queued: Promise<void> | undefined;

  constructor(store: StoreContext) {
    this.#store = store;
  }

  /**
   * Pushes owed list rows in a round after the one in flight, so it carries every row owed before
   * this call. A failed push stays owed and arms the alarm; a backlog gets more rounds.
   */
  push(): Promise<void> {
    if (this.#queued === undefined) {
      const store = this.#store;
      this.#round = this.#round.then(async () => {
        this.#queued = undefined;
        try {
          const retryAt = await store.index.push((name) => store.peer(name));
          if (retryAt !== undefined) await store.armAlarm(retryAt, 'index push retry');
          else if (store.index.hasDue()) store.ctx.waitUntil(this.push());
        } catch (error) {
          console.error('Index push round failed', error);
        }
      });
      this.#queued = this.#round;
    }
    return this.#queued;
  }

  /**
   * Pushes owed list rows at once. A write that owes a hook's row answers once it reaches its index
   * shard, or after `INDEX_PUSH_WAIT_MS` while a slow push goes on, because the SDK reads a hook
   * (`hooks.get`) right after creating it. Runs' rows do not hold the write: the SDK never lists
   * runs, so `POST /runs` pays no index round trip and a list sees a new run a moment later.
   * `settle` waits for runs' rows too, for a run's last write, so its store can drop its alarm.
   */
  async flush(settle = false): Promise<void> {
    if (!this.#store.index.hasDue()) return;
    const pushed = this.push();
    this.#store.ctx.waitUntil(pushed);
    if (!settle && !this.#store.index.hasDueHook()) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([pushed, new Promise((resolve) => { timer = setTimeout(resolve, INDEX_PUSH_WAIT_MS); })]);
    clearTimeout(timer);
  }

  backfill(): IndexBackfill {
    // SAFETY: only `#setBackfill` writes this key.
    return this.#store.ctx.storage.kv.get<IndexBackfill>(INDEX_BACKFILL) ?? { stage: BackfillStage.Scanning, after: '' };
  }

  /**
   * On waking: a store that holds nothing reports to its index shards at once, and one with runs
   * from before the index backfills them through the alarm. Rows a reset left owed go out too.
   */
  async resume(): Promise<void> {
    const store = this.#store;
    let backfill = this.backfill();
    if (backfill.stage === BackfillStage.Scanning && backfill.after === '' && store.db.runIdsAfter('', 1).length === 0) {
      backfill = { stage: BackfillStage.Scanned };
      this.#setBackfill(backfill);
    }
    if (backfill.stage === BackfillStage.Scanned && store.index.nextDueAt() === null) {
      // Not through the alarm, which would also publish dispatches still in their grace period.
      store.ctx.waitUntil(this.#report().catch(async (error: unknown) => {
        console.error('Index backfill report failed', error);
        await store.armAlarm(Date.now() + DISPATCH_RECOVERY_DELAY_MS, 'index backfill report');
      }));
    } else if (backfill.stage !== BackfillStage.Reported || store.index.nextDueAt() !== null) {
      await store.armAlarm(Date.now(), 'index backfill');
    }
  }

  /**
   * The alarm's index work: the next backfill batch, a push round, and the report once every run
   * is pushed. Returns when to run again, or null.
   */
  async alarm(): Promise<number | null> {
    const store = this.#store;
    const backfill = this.backfill();
    if (backfill.stage === BackfillStage.Scanning) {
      store.ctx.storage.transactionSync(() => {
        const after = store.index.oweRunsAfter(backfill.after, INDEX_BACKFILL_BATCH);
        this.#setBackfill(after === null ? { stage: BackfillStage.Scanned } : { stage: BackfillStage.Scanning, after });
      });
    }
    await this.push();
    const owedAt = store.index.nextDueAt();
    switch (this.backfill().stage) {
      case BackfillStage.Scanning:
        return Date.now();
      case BackfillStage.Scanned:
        if (owedAt !== null) return owedAt;
        try {
          await this.#report();
          return null;
        } catch (error) {
          console.error('Index backfill report failed', error);
          return Date.now() + DISPATCH_RECOVERY_DELAY_MS;
        }
      case BackfillStage.Reported:
        return owedAt;
    }
  }

  #setBackfill(backfill: IndexBackfill): void {
    this.#store.ctx.storage.kv.put(INDEX_BACKFILL, backfill);
  }

  /** Tells every index shard this store feeds that it holds every run and hook this store held before the index. */
  async #report(): Promise<void> {
    const { name } = this.#store;
    await Promise.all(indexShardsOf(name).map((shard) => this.#store.peer(shard).markIndexed(name)));
    this.#setBackfill({ stage: BackfillStage.Reported });
  }
}
