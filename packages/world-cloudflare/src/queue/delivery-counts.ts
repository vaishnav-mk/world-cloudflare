import { DOQB, Raw } from 'workers-qb';
import { findRow, requireRow } from '../lib/qb.js';
import type { QueueSchema } from './tables.js';

/** A message's deliveries so far, and how many of them handed it off. */
export type Counts = { deliveries: number; handoffs: number };

/**
 * How often each message reached the consumer, keyed by its stable message ID. The SDK reads
 * `deliveryCount` (1 only on a first delivery) and `attempt` (deliveries less handoffs) from it, so
 * waiting out a delay or behind a run's other delivery is never counted.
 */
export class DeliveryCounts {
  readonly #qb: DOQB<QueueSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<QueueSchema>(sql);
  }

  get(id: string): Counts {
    const row = findRow(this.#qb.fetchOne<'delivery_counts', 'deliveries' | 'handoffs'>({
      tableName: 'delivery_counts', fields: ['deliveries', 'handoffs'], where: { conditions: 'id = ?', params: [id] },
    }));
    return row ?? { deliveries: 0, handoffs: 0 };
  }

  /** Counts a delivery about to start, and returns the counts with it. */
  begin(id: string, now: number): Counts {
    return requireRow(this.#qb.insert<Counts>({
      tableName: 'delivery_counts',
      data: { id, deliveries: 1, handoffs: 0, updated_at: now },
      onConflict: { column: 'id', data: { deliveries: new Raw('deliveries + 1'), updated_at: new Raw('excluded.updated_at') } },
      returning: ['deliveries', 'handoffs'],
    }));
  }

  /** The delivery handed the message off: the next one is not another attempt. */
  handoff(id: string): void {
    this.#qb.update({
      tableName: 'delivery_counts', data: { handoffs: new Raw('handoffs + 1') }, where: { conditions: 'id = ?', params: [id] },
    }).execute();
  }

  /** Forgets messages nothing delivered since `before`. */
  prune(before: number): void {
    this.#qb.delete({ tableName: 'delivery_counts', where: { conditions: 'updated_at < ?', params: [before] } }).execute();
  }
}
