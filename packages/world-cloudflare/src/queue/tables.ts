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

export type QueueSchema = {
  queue_dedup: typeof queueDedup.$inferSelect;
};
