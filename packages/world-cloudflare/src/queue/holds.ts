import { DOQB } from 'workers-qb';
import { findRow } from '../lib/qb.js';
import type { OutgoingMessage } from './dedup.js';
import type { QueueSchema } from './tables.js';

/** A held message as its queued copy finds it: still held until `leaseUntil`, or settled by its sender. */
export type Hold = { leaseUntil: number; done: boolean };

/** Messages held for their sender (`experimental_hold`); see `QueueShard.hold`. */
export class Holds {
  readonly #qb: DOQB<QueueSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<QueueSchema>(sql);
  }

  find(id: string): Hold | undefined {
    const row = findRow(this.#qb.fetchOne<'holds', 'lease_until' | 'done'>({
      tableName: 'holds', fields: ['lease_until', 'done'], where: { conditions: 'id = ?', params: [id] },
    }));
    return row === undefined ? undefined : { leaseUntil: row.lease_until, done: row.done === 1 };
  }

  /** The message a held ID stands for, while its sender has not settled it. */
  message(id: string): OutgoingMessage | undefined {
    const row = findRow(this.#qb.fetchOne({ tableName: 'holds', where: { conditions: ['id = ?', 'done = 0'], params: [id] } }));
    return row === undefined ? undefined : {
      id, queueName: row.queue_name, encoded: row.encoded, delayMs: 0, targetDeployment: row.target_deployment ?? undefined,
    };
  }

  add({ id, queueName, encoded, targetDeployment }: OutgoingMessage, leaseUntil: number): void {
    this.#qb.insert({
      tableName: 'holds',
      data: { id, lease_until: leaseUntil, queue_name: queueName, encoded, target_deployment: targetDeployment ?? null },
    }).execute();
  }

  /** Extends a hold its sender still runs. Returns whether it is still held. */
  renew(id: string, leaseUntil: number): boolean {
    const held = this.find(id);
    if (held === undefined || held.done || held.leaseUntil <= Date.now()) return false;
    this.#qb.update({ tableName: 'holds', data: { lease_until: leaseUntil }, where: { conditions: 'id = ?', params: [id] } }).execute();
    return true;
  }

  settle(id: string): void {
    this.#qb.update({ tableName: 'holds', data: { done: 1 }, where: { conditions: 'id = ?', params: [id] } }).execute();
  }

  remove(id: string): void {
    this.#qb.delete({ tableName: 'holds', where: { conditions: 'id = ?', params: [id] } }).execute();
  }
}
