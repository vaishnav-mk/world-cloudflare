import { integer, primaryKey, sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { bytes } from '../db/columns.js';

/**
 * One row per stream. `tail` is the last chunk's position, -1 while empty. `lease_until` is when the
 * writer's lease ends (ms since the epoch), null for a stream written with none.
 */
export const streams = sqliteTable('streams', {
  name: text('name').primaryKey(),
  tail: integer('tail').notNull().default(-1),
  closed: integer('closed').notNull().default(0),
  lease_until: integer('lease_until'),
});

export const chunks = sqliteTable('chunks', {
  name: text('name').notNull(),
  position: integer('position').notNull(),
  bytes: bytes('bytes').notNull(),
}, (table) => [primaryKey({ columns: [table.name, table.position] })]);

/** The run's VM snapshot envelope: at most one row, its total size in bytes. */
export const snapshot = sqliteTable('snapshot', {
  size: integer('size').notNull(),
});

/** The envelope split into ordered parts, each under the SQLite value limit. */
export const snapshotParts = sqliteTable('snapshot_parts', {
  part: integer('part').primaryKey(),
  bytes: bytes('bytes').notNull(),
});

export type StreamsSchema = {
  streams: typeof streams.$inferSelect;
  chunks: typeof chunks.$inferSelect;
  snapshot: typeof snapshot.$inferSelect;
  snapshot_parts: typeof snapshotParts.$inferSelect;
};
