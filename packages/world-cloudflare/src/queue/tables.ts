import { integer, sqliteTable, text } from 'drizzle-orm/sqlite-core';

/** Idempotency keys the queue has seen; the first accepted message for a key wins forever. */
export const queueDedup = sqliteTable('queue_dedup', {
  key: text('key').primaryKey(),
  id: text('id').notNull(),
  queue_name: text('queue_name').notNull(),
  encoded: text('encoded'),
  payload_digest: text('payload_digest').notNull(),
  delay_ms: integer('delay_ms').notNull(),
  target_deployment: text('target_deployment'),
  accepted: integer('accepted').notNull().default(0),
});

/**
 * Deliveries handed to the consumer and not yet settled. A fresh instance runs nothing, so rows it
 * finds were cut off by a reset of the previous one.
 */
export const inflight = sqliteTable('inflight', {
  id: text('id').primaryKey(),
  queue_name: text('queue_name').notNull(),
  encoded: text('encoded').notNull(),
  target_deployment: text('target_deployment'),
});

export type QueueSchema = {
  queue_dedup: typeof queueDedup.$inferSelect;
  inflight: typeof inflight.$inferSelect;
};
