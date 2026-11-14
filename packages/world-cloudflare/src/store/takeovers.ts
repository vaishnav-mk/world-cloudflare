import type { Hook } from '@workflow/world';
import { DOQB, FetchTypes } from 'workers-qb';
import { MAX_PUBLISH_BACKOFF_MS } from '../constants.js';
import { decode, encode } from '../lib/codec.js';
import { allRows, findRow, requireRow } from '../lib/qb.js';
import type { HookClaimOperation } from '../types.js';
import type { StoreSchema } from './tables.js';

/** A forced claim the token's holder has not yet confirmed: the hook holding the token, and the one taking it. */
export type Takeover = { holder: Hook; claimer: Hook; attempts: number; operation: HookClaimOperation | null };

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
    return row === undefined ? undefined : {
      holder: decode<Hook>(row.holder), claimer: decode<Hook>(row.claimer), attempts: row.attempts,
      operation: row.operation === null ? null : decode<HookClaimOperation>(row.operation),
    };
  }

  add(holder: Hook, claimer: Hook, dueAt: number, operation: HookClaimOperation): void {
    this.#qb.insert({
      tableName: 'token_takeovers',
      data: { token: claimer.token, holder: encode(holder), claimer: encode(claimer), next_at: dueAt, operation: encode(operation) },
    }).execute();
  }

  remove(takeover: Takeover): void {
    if (!this.matches(takeover)) return;
    this.#qb.delete({ tableName: 'token_takeovers', where: { conditions: 'token = ?', params: [takeover.claimer.token] } }).execute();
  }

  matches(takeover: Takeover): boolean {
    const current = this.find(takeover.claimer.token);
    return current !== undefined && current.claimer.hookId === takeover.claimer.hookId
      && current.claimer.runId === takeover.claimer.runId && current.operation?.id === takeover.operation?.id;
  }

  /** Late failures must not back off a newer reservation. */
  recordFailure(takeover: Takeover): number {
    const { claimer: { token }, attempts } = takeover;
    if (!this.matches(takeover)) return Date.now();
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
