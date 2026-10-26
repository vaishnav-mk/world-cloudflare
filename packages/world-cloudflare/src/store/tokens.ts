import { EntityConflictError, HookNotFoundError } from '@workflow/errors';
import type { Hook } from '@workflow/world';
import { DOQB } from 'workers-qb';
import { decode, encode } from '../lib/codec.js';
import { findRow } from '../lib/qb.js';
import type { HeldToken, TokenRelease } from '../types.js';
import type { StoreSchema } from './tables.js';

/** Whether a finished run's hook no longer keeps its token. */
function retentionLapsed(hook: Hook, now: Date): boolean {
  return hook.tokenRetentionUntil === undefined || hook.tokenRetentionUntil.getTime() <= now.getTime();
}

/**
 * A token shard's registry: the hook that holds each token hashing to this shard. Run stores claim
 * tokens here before writing `hook_created`, and release them after a disposal or the run finishing.
 * Call these inside `transactionSync`.
 */
export class TokenRegistry {
  readonly #qb: DOQB<StoreSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<StoreSchema>(sql);
  }

  find(token: string): HeldToken | undefined {
    const row = findRow(this.#qb.fetchOne<'tokens', 'data' | 'finished'>({
      tableName: 'tokens', fields: ['data', 'finished'], where: { conditions: 'token = ?', params: [token] },
    }));
    return row === undefined ? undefined : { hook: decode<Hook>(row.data), finished: row.finished === 1 };
  }

  /** The hook holding `token`. A finished run's hook past its retention is gone to readers, as on its run's shard. */
  get(token: string, now = new Date()): Hook {
    const held = this.find(token);
    if (held === undefined || (held.finished && retentionLapsed(held.hook, now))) throw new HookNotFoundError(token);
    return held.hook;
  }

  /**
   * Claims `hook.token` for `hook`. A token is free when no hook holds it, or when its holder's run
   * has finished and the hook's retention has lapsed. Returns the run that holds it, or null once
   * granted. A repeated claim for the same hook is granted again, so a write retried after a crash succeeds.
   */
  claim(hook: Hook, now: Date): string | null {
    const holder = this.find(hook.token);
    if (holder !== undefined && holder.hook.hookId !== hook.hookId) {
      if (!holder.finished || !retentionLapsed(holder.hook, now)) return holder.hook.runId;
    } else if (holder !== undefined && holder.hook.runId !== hook.runId) {
      throw new EntityConflictError(`Hook ${hook.hookId} already exists`);
    }
    this.#qb.insert({
      tableName: 'tokens',
      data: { token: hook.token, hook_id: hook.hookId, data: encode(hook), finished: 0 },
      onConflict: 'REPLACE',
    }).execute();
    return null;
  }

  /** Applies a release, unless another hook holds the token by now. */
  release({ token, hookId, disposed }: TokenRelease, now: Date): void {
    const holder = this.find(token);
    if (holder === undefined || holder.hook.hookId !== hookId) return;
    if (disposed || retentionLapsed(holder.hook, now)) {
      this.#qb.delete({ tableName: 'tokens', where: { conditions: 'token = ?', params: [token] } }).execute();
    } else {
      this.#qb.update({ tableName: 'tokens', data: { finished: 1 }, where: { conditions: 'token = ?', params: [token] } }).execute();
    }
  }

  /** Adds tokens legacy hooks hold, keeping any token this shard already granted. */
  import(held: readonly HeldToken[]): void {
    for (const { hook, finished } of held) {
      this.#qb.insert({
        tableName: 'tokens',
        data: { token: hook.token, hook_id: hook.hookId, data: encode(hook), finished: finished ? 1 : 0 },
        onConflict: 'IGNORE',
      }).execute();
    }
  }
}
