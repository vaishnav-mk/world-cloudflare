import { DOQB } from 'workers-qb';
import { allRows } from '../lib/qb.js';
import type { Delivery } from '../types.js';
import type { OutgoingMessage } from './dedup.js';
import type { QueueSchema } from './tables.js';

/** Deliveries handed to the consumer and not yet settled, so a reset cannot strand them. */
export class InflightTable {
  readonly #qb: DOQB<QueueSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<QueueSchema>(sql);
  }

  add(deliveries: Delivery[]): void {
    for (const { id, body } of deliveries) {
      this.#qb.insert({
        tableName: 'inflight',
        data: { id, queue_name: body.queueName, encoded: body.encoded, target_deployment: body.targetDeployment ?? null },
        onConflict: 'REPLACE',
      }).execute();
    }
  }

  remove(id: string): void {
    this.#qb.delete({ tableName: 'inflight', where: { conditions: 'id = ?', params: [id] } }).execute();
  }

  /** Every recorded delivery, by its stable message ID, as messages to send again now. */
  all(): OutgoingMessage[] {
    return allRows(this.#qb.fetchAll<'inflight'>({ tableName: 'inflight' })).map((row) => ({
      id: row.id,
      queueName: row.queue_name,
      encoded: row.encoded,
      delayMs: 0,
      targetDeployment: row.target_deployment ?? undefined,
    }));
  }
}
