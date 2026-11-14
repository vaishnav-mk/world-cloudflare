import { EntityConflictError } from '@workflow/errors';
import { DOQB, FetchTypes } from 'workers-qb';
import { DISPATCH_RECOVERY_DELAY_MS } from '../constants.js';
import { decode, encode } from '../lib/codec.js';
import { allRows, findRow, requireRow } from '../lib/qb.js';
import type { HookClaimIntent } from '../types.js';
import type { StoreSchema } from './tables.js';

/** Run-store admission log. Accepted operations stay addressable after completion for RPC evidence. */
export class HookClaimIntents {
  readonly #qb: DOQB<StoreSchema>;
  constructor(sql: SqlStorage, readonly kv: DurableObjectStorage['kv']) {
    this.#qb = new DOQB<StoreSchema>(sql);
  }

  find(runId: string): HookClaimIntent | undefined {
    const row = findRow(this.#qb.fetchOne({ tableName: 'hook_claim_intents', where: { conditions: 'run_id = ?', params: [runId] } }));
    return row === undefined ? undefined : decode<HookClaimIntent>(row.data);
  }

  evidence(id: string): string | undefined {
    // SAFETY: add is the only writer; the argument digest stays after completion.
    return this.kv.get<string>(`hook-intent:${id}`);
  }

  add(intent: HookClaimIntent): void {
    const { operation } = intent;
    if (this.kv.get(`hook-completed:${operation.hook.hookId}`) !== undefined) {
      throw new EntityConflictError(`Hook ${operation.hook.hookId} already had a claim decision`);
    }
    const reserved = findRow(this.#qb.fetchOne({ tableName: 'hook_claim_intents', where: {
      conditions: 'hook_id = ?', params: [operation.hook.hookId],
    } }));
    if (reserved !== undefined) throw new EntityConflictError(`Hook ${operation.hook.hookId} is being created`);
    this.#qb.insert({ tableName: 'hook_claim_intents', data: {
      run_id: operation.hook.runId, operation_id: operation.id, hook_id: operation.hook.hookId,
      data: encode(intent), next_at: Date.now(),
    } }).execute();
    this.kv.put(`hook-intent:${operation.id}`, intent.digest);
  }

  complete(intent: HookClaimIntent, completed: true | string): void {
    // Public retries carry a hook ID; absent start candidates also need a bounded token identity proof.
    this.kv.put(`hook-completed:${intent.operation.hook.hookId}`, completed);
    this.#qb.delete({ tableName: 'hook_claim_intents', where: {
      conditions: 'operation_id = ?', params: [intent.operation.id],
    } }).execute();
  }

  retry(intent: HookClaimIntent): void {
    this.#qb.update({ tableName: 'hook_claim_intents', data: { next_at: Date.now() + DISPATCH_RECOVERY_DELAY_MS },
      where: { conditions: 'operation_id = ?', params: [intent.operation.id] },
    }).execute();
  }

  due(): string[] {
    return allRows(this.#qb.fetchAll<'hook_claim_intents', 'run_id'>({
      tableName: 'hook_claim_intents', fields: ['run_id'], where: { conditions: 'next_at <= ?', params: [Date.now()] },
    })).map(({ run_id }) => run_id);
  }

  nextDueAt(): number | null {
    return requireRow(this.#qb.raw<{ at: number | null }>({
      query: 'SELECT MIN(next_at) AS at FROM hook_claim_intents', fetchType: FetchTypes.ONE,
    })).at;
  }
}
