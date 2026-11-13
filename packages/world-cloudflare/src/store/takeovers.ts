import type { Hook } from '@workflow/world';
import { DOQB, FetchTypes } from 'workers-qb';
import { MAX_PUBLISH_BACKOFF_MS } from '../constants.js';
import { decode, encode } from '../lib/codec.js';
import { allRows, findRow, requireRow } from '../lib/qb.js';
import type { StoreSchema } from './tables.js';

/** A forced claim the token's holder has not yet confirmed: the hook holding the token, and the one taking it. */
export type Takeover = { holder: Hook; claimer: Hook; attempts: number };

/**
 * On a token shard: forced claims in progress. A row commits before the holder's store is asked to
 * dispose of its hook, and goes once the token has moved, so the alarm finishes a takeover a failed
 * call or a reset left half done. Call these inside `transactionSync`.
 */
export class TokenTakeovers {
  readonly #qb: DOQB<StoreSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<StoreSchema>(sql);
  }

  find(token: string): Takeover | undefined {
    const row = findRow(this.#qb.fetchOne({ tableName: 'token_takeovers', where: { conditions: 'token = ?', params: [token] } }));
    return row === undefined ? undefined : { holder: decode<Hook>(row.holder), claimer: decode<Hook>(row.claimer), attempts: row.attempts };
  }

  add(holder: Hook, claimer: Hook, dueAt: number): void {
    this.#qb.insert({
      tableName: 'token_takeovers',
      data: { token: claimer.token, holder: encode(holder), claimer: encode(claimer), next_at: dueAt },
    }).execute();
  }

  remove(token: string): void {
    this.#qb.delete({ tableName: 'token_takeovers', where: { conditions: 'token = ?', params: [token] } }).execute();
  }

  /** Backs off a takeover whose holder could not be reached. Returns when to retry. */
  recordFailure(token: string, attempts: number): number {
    const at = Date.now() + Math.min(MAX_PUBLISH_BACKOFF_MS, (attempts + 1) ** 2 * 1_000);
    this.#qb.update({
      tableName: 'token_takeovers', data: { attempts: attempts + 1, next_at: at }, where: { conditions: 'token = ?', params: [token] },
    }).execute();
    return at;
  }

  due(now: number): string[] {
    return allRows(this.#qb.fetchAll<'token_takeovers', 'token'>({
      tableName: 'token_takeovers', fields: ['token'], where: { conditions: 'next_at <= ?', params: [now] },
    })).map((row) => row.token);
  }

  nextDueAt(): number | null {
    return requireRow(this.#qb.raw<{ next_at: number | null }>({
      query: 'SELECT MIN(next_at) AS next_at FROM token_takeovers', fetchType: FetchTypes.ONE,
    })).next_at;
  }
}
