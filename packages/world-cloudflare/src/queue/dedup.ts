import { DOQB } from 'workers-qb';
import { ClaimStatus } from '../enums.js';
import { findRow } from '../lib/qb.js';
import type { QueueSchema } from './tables.js';

/** A message as the shard will enqueue it. */
export type OutgoingMessage = {
  id: string;
  queueName: string;
  encoded: string;
  delayMs: number;
  targetDeployment?: string;
  messageId?: string;
};

type Claim =
  | { status: ClaimStatus.New }
  | { status: ClaimStatus.Replay; message: OutgoingMessage }
  | { status: ClaimStatus.Accepted; id: string };

/**
 * Idempotency keys the queue has seen. The fanout only deduplicates pending messages, so this
 * table keeps the first accepted message for a key forever.
 */
export class DedupTable {
  readonly #qb: DOQB<QueueSchema>;

  constructor(sql: SqlStorage) {
    this.#qb = new DOQB<QueueSchema>(sql);
  }

  /** Records a new key, or returns what an earlier claim of it stored. */
  claim(key: string, message: OutgoingMessage, payloadDigest: string): Claim {
    const existing = findRow(this.#qb.fetchOne({
      tableName: 'queue_dedup', where: { conditions: 'key = ?', params: [key] },
    }));
    if (existing === undefined) {
      this.#qb.insert({
        tableName: 'queue_dedup',
        data: {
          key, id: message.id, queue_name: message.queueName, encoded: message.encoded,
          payload_digest: payloadDigest, delay_ms: message.delayMs, target_deployment: message.targetDeployment ?? null,
        },
      }).execute();
      return { status: ClaimStatus.New };
    }
    // The first accepted message wins, even if a retry changed its timestamps or delay.
    if (existing.accepted) return { status: ClaimStatus.Accepted, id: existing.id };
    if (existing.encoded === null && existing.payload_digest !== payloadDigest) {
      throw new Error('Cannot replay a pre-upgrade queue claim with different bytes');
    }
    return { status: ClaimStatus.Replay, message: {
      id: existing.id,
      queueName: message.queueName,
      encoded: existing.encoded ?? message.encoded,
      delayMs: existing.delay_ms,
      targetDeployment: existing.target_deployment ?? undefined,
    } };
  }

  markAccepted(key: string): void {
    this.#qb.update({ tableName: 'queue_dedup', data: { accepted: 1, encoded: null }, where: { conditions: 'key = ?', params: [key] } }).execute();
  }
}
