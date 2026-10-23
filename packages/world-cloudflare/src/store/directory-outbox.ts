import { DOQB, FetchTypes, Raw } from 'workers-qb';
import { allRows, findRow, requireRow } from '../lib/qb.js';
import type { StoreSchema } from './tables.js';

/** What a run store owes the directory: its run row, and the hooks it disposed of since the last push. */
export type DirectoryDebt = { runId: string; attempts: number; released: string[] };

/**
 * A run store's pending pushes to the directory. Rows commit with the event that caused them and
 * stay until the directory accepts the push, so a failed push is retried rather than lost.
 */
export class DirectoryOutbox {
  readonly #qb: DOQB<StoreSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<StoreSchema>(sql);
  }

  /** Owes the directory the run's row as of `slot`. */
  mark(runId: string, slot: number): void {
    this.#qb.insert({
      tableName: 'directory_outbox',
      data: { run_id: runId, slot, next_at: Date.now() },
      onConflict: { column: 'run_id', data: { slot: new Raw('excluded.slot') } },
    }).execute();
  }

  /** Owes the directory the release of a disposed hook's token. */
  release(runId: string, hookId: string): void {
    this.#qb.insert({ tableName: 'directory_releases', data: { hook_id: hookId, run_id: runId }, onConflict: 'IGNORE' }).execute();
  }

  pending(): DirectoryDebt | undefined {
    const row = findRow(this.#qb.fetchOne<'directory_outbox', 'run_id' | 'attempts'>({
      tableName: 'directory_outbox', fields: ['run_id', 'attempts'],
    }));
    if (row === undefined) return undefined;
    const released = allRows(this.#qb.fetchAll<'directory_releases', 'hook_id'>({
      tableName: 'directory_releases', fields: ['hook_id'], where: { conditions: 'run_id = ?', params: [row.run_id] },
    }));
    return { runId: row.run_id, attempts: row.attempts, released: released.map(({ hook_id }) => hook_id) };
  }

  /** Settles a push of the run row as of `slot`. A change committed after the push read it stays owed. */
  pushed(runId: string, slot: number, released: readonly string[]): void {
    this.#qb.delete({ tableName: 'directory_outbox', where: { conditions: ['run_id = ?', 'slot <= ?'], params: [runId, slot] } })
      .execute();
    for (const hookId of released) {
      this.#qb.delete({ tableName: 'directory_releases', where: { conditions: 'hook_id = ?', params: [hookId] } }).execute();
    }
  }

  postpone(runId: string, nextAt: number): void {
    this.#qb.raw({
      query: 'UPDATE directory_outbox SET attempts = attempts + 1, next_at = ? WHERE run_id = ?',
      args: [nextAt, runId],
    }).execute();
  }

  nextDueAt(): number | null {
    return requireRow(this.#qb.raw<{ next_at: number | null }>({
      query: 'SELECT MIN(next_at) AS next_at FROM directory_outbox', fetchType: FetchTypes.ONE,
    })).next_at;
  }
}
