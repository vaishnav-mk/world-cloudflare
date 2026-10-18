import { ClaimStatus } from '../enums.js';

/** A message as the shard will enqueue it. */
export type OutgoingMessage = {
  id: string;
  queueName: string;
  encoded: string;
  delayMs: number;
  targetDeployment?: string;
};

type DedupRow = {
  id: string;
  queue_name: string;
  encoded: string | null;
  payload_digest: string;
  delay_ms: number;
  target_deployment: string | null;
  accepted: number;
};

export type Claim =
  | { status: ClaimStatus.New }
  | { status: ClaimStatus.Replay; message: OutgoingMessage }
  | { status: ClaimStatus.Accepted; id: string };

const SCHEMA = `CREATE TABLE IF NOT EXISTS queue_dedup (
  key TEXT PRIMARY KEY, id TEXT NOT NULL, queue_name TEXT NOT NULL,
  encoded TEXT, payload_digest TEXT NOT NULL, delay_ms INTEGER NOT NULL,
  target_deployment TEXT, accepted INTEGER NOT NULL DEFAULT 0
)`;

/**
 * Idempotency keys the queue has seen. The fanout only deduplicates pending messages, so this
 * table keeps the first accepted message for a key forever.
 */
export class DedupTable {
  constructor(private readonly sql: SqlStorage) {}

  migrate(): void {
    this.sql.exec(SCHEMA);
    // Claims recorded before payloads were stored lack the column.
    const columns = this.sql.exec<{ name: string }>('PRAGMA table_info(queue_dedup)').toArray();
    if (!columns.some(({ name }) => name === 'encoded')) this.sql.exec('ALTER TABLE queue_dedup ADD COLUMN encoded TEXT');
  }

  /** Records a new key, or returns what an earlier claim of it stored. */
  claim(key: string, message: OutgoingMessage, payloadDigest: string): Claim {
    const existing = this.sql.exec<DedupRow>('SELECT * FROM queue_dedup WHERE key = ?', key).toArray()[0];
    if (!existing) {
      this.sql.exec(
        `INSERT INTO queue_dedup (key, id, queue_name, encoded, payload_digest, delay_ms, target_deployment)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        key, message.id, message.queueName, message.encoded, payloadDigest, message.delayMs,
        message.targetDeployment ?? null,
      );
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
    this.sql.exec('UPDATE queue_dedup SET accepted = 1 WHERE key = ?', key);
  }
}
