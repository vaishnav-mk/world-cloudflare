import type { Hook } from '@workflow/world';
import { DOQB, FetchTypes, Raw } from 'workers-qb';
import { MAX_PUBLISH_BACKOFF_MS } from '../constants.js';
import { groupBy } from '../lib/group.js';
import { allRows, requireRow } from '../lib/qb.js';
import type { TokenRelease } from '../types.js';
import { storeShardFor } from './route.js';
import type { StoreSchema } from './tables.js';

/** The method a store calls on a token's shard. */
type TokenShard = { releaseTokens(releases: TokenRelease[]): Promise<void> };

/**
 * Token releases a store owes the token shards: a disposed hook, or a hook whose run finished. Rows
 * commit with the event that caused them and stay until the token's shard accepts them, so a failed
 * release is retried rather than lost.
 */
export class TokenOutbox {
  readonly #qb: DOQB<StoreSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<StoreSchema>(sql);
  }

  /** Owes `hook`'s shard a release, due now. A disposal outranks a finish still owed for the same hook. */
  owe(hook: Hook, disposed: boolean): void {
    this.#qb.insert({
      tableName: 'token_releases',
      data: { hook_id: hook.hookId, token: hook.token, disposed: disposed ? 1 : 0, next_at: Date.now() },
      onConflict: {
        column: 'hook_id',
        data: { disposed: new Raw('MAX(disposed, excluded.disposed)'), next_at: new Raw('excluded.next_at') },
      },
    }).execute();
  }

  /**
   * Sends due releases, one call per token shard, until none are due. A release owed during a push
   * goes in its next round. A shard that fails keeps its rows owed with backoff. Returns when to
   * retry after a failure.
   */
  async push(shard: (name: string) => TokenShard): Promise<number | undefined> {
    let retryAt: number | undefined;
    for (;;) {
      const due = allRows(this.#qb.fetchAll<'token_releases'>({
        tableName: 'token_releases', where: { conditions: 'next_at <= ?', params: [Date.now()] },
      }));
      if (due.length === 0) return retryAt;
      await Promise.all([...groupBy(due, (row) => storeShardFor(row.token))].map(async ([name, rows]) => {
        try {
          await shard(name).releaseTokens(rows.map((row) => ({ token: row.token, hookId: row.hook_id, disposed: row.disposed === 1 })));
        } catch (error) {
          console.error('Token release failed', { shard: name, error });
          for (const row of rows) {
            const at = Date.now() + Math.min(MAX_PUBLISH_BACKOFF_MS, (row.attempts + 1) ** 2 * 1_000);
            this.#qb.update({
              tableName: 'token_releases', data: { attempts: row.attempts + 1, next_at: at },
              where: { conditions: 'hook_id = ?', params: [row.hook_id] },
            }).execute();
            retryAt = Math.min(retryAt ?? at, at);
          }
          return;
        }
        // A disposal owed during the call raised `disposed`; that row stays owed.
        for (const row of rows) {
          this.#qb.delete({
            tableName: 'token_releases', where: { conditions: ['hook_id = ?', 'disposed = ?'], params: [row.hook_id, row.disposed] },
          }).execute();
        }
      }));
    }
  }

  nextDueAt(): number | null {
    return requireRow(this.#qb.raw<{ next_at: number | null }>({
      query: 'SELECT MIN(next_at) AS next_at FROM token_releases', fetchType: FetchTypes.ONE,
    })).next_at;
  }
}
