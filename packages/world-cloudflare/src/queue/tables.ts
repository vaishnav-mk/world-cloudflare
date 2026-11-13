import { index, integer, sqliteTable, text } from 'drizzle-orm/sqlite-core';

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

/**
 * How often each message reached the consumer, by its stable message ID, and how many of those
 * deliveries handed it off. Holds and delay waits are not deliveries, so they leave no trace. A row
 * outlives its message by a day, so a late duplicate still counts as a redelivery.
 */
export const deliveryCounts = sqliteTable('delivery_counts', {
  id: text('id').primaryKey(),
  deliveries: integer('deliveries').notNull().default(0),
  handoffs: integer('handoffs').notNull().default(0),
  updated_at: integer('updated_at').notNull(),
}, (table) => [index('delivery_counts_updated').on(table.updated_at)]);

export type QueueSchema = {
  queue_dedup: typeof queueDedup.$inferSelect;
  inflight: typeof inflight.$inferSelect;
  delivery_counts: typeof deliveryCounts.$inferSelect;
};
