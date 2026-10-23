import { requireEventSlot, type AnyEventRequest, type EventResult, type WorkflowRun } from '@workflow/world';
import { DOQB, FetchTypes, Raw } from 'workers-qb';
import { DIRECTORY_KINDS, MAX_PUBLISH_BACKOFF_MS } from '../constants.js';
import { EventType } from '../enums.js';
import { allRows, findRow, requireRow } from '../lib/qb.js';
import type { Db } from './db.js';
import type { StoreSchema } from './tables.js';

/** The directory method a run store pushes to. */
type Directory = { indexRun(run: WorkflowRun, slot: number, released: string[]): Promise<void> };

/**
 * A run store's pending pushes to the directory: its run row for the run index, and the hooks it
 * disposed of. Rows commit with the event that caused them and stay until the directory accepts
 * the push, so a failed push is retried rather than lost.
 */
export class DirectoryOutbox {
  readonly #qb: DOQB<StoreSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<StoreSchema>(sql);
  }

  /** Inside the write's transaction: owes the directory the run row after a run change, and a disposed hook. */
  owe(data: AnyEventRequest, result: EventResult): void {
    if (result.event === undefined || !DIRECTORY_KINDS.has(data.eventType)) return;
    const { runId } = result.event;
    if (data.eventType === EventType.HookDisposed) {
      this.#qb.insert({
        tableName: 'directory_releases', data: { hook_id: data.correlationId, run_id: runId }, onConflict: 'IGNORE',
      }).execute();
    }
    this.#qb.insert({
      tableName: 'directory_outbox',
      data: { run_id: runId, slot: requireEventSlot(result.event.eventId), next_at: Date.now() },
      onConflict: { column: 'run_id', data: { slot: new Raw('excluded.slot') } },
    }).execute();
  }

  /**
   * Pushes what is owed now, with the run row as it is now. A push that failed waits out its
   * backoff. Returns when to retry after a failure.
   */
  async push(db: Db, directory: Directory): Promise<number | undefined> {
    const row = findRow(this.#qb.fetchOne<'directory_outbox', 'run_id' | 'attempts'>({
      tableName: 'directory_outbox', fields: ['run_id', 'attempts'], where: { conditions: 'next_at <= ?', params: [Date.now()] },
    }));
    if (row === undefined) return undefined;
    const runId = row.run_id;
    const released = allRows(this.#qb.fetchAll<'directory_releases', 'hook_id'>({
      tableName: 'directory_releases', fields: ['hook_id'], where: { conditions: 'run_id = ?', params: [runId] },
    })).map(({ hook_id }) => hook_id);
    const slot = db.lastSlot(runId);
    try {
      await directory.indexRun(db.getRun(runId), slot, released);
    } catch (error) {
      console.error('Run index push failed', { runId, error });
      const retryAt = Date.now() + Math.min(MAX_PUBLISH_BACKOFF_MS, (row.attempts + 1) ** 2 * 1_000);
      this.#qb.raw({
        query: 'UPDATE directory_outbox SET attempts = attempts + 1, next_at = ? WHERE run_id = ?', args: [retryAt, runId],
      }).execute();
      return retryAt;
    }
    // A change committed while the push was in flight has a later slot, so it stays owed.
    this.#qb.delete({ tableName: 'directory_outbox', where: { conditions: ['run_id = ?', 'slot <= ?'], params: [runId, slot] } })
      .execute();
    for (const hookId of released) {
      this.#qb.delete({ tableName: 'directory_releases', where: { conditions: 'hook_id = ?', params: [hookId] } }).execute();
    }
    return undefined;
  }

  nextDueAt(): number | null {
    return requireRow(this.#qb.raw<{ next_at: number | null }>({
      query: 'SELECT MIN(next_at) AS next_at FROM directory_outbox', fetchType: FetchTypes.ONE,
    })).next_at;
  }
}
