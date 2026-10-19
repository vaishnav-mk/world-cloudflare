import { integer, primaryKey, sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { bytes } from '../db/columns.js';

/** One row per stream. `tail` is the last chunk's position, -1 while empty. */
export const streams = sqliteTable('streams', {
  name: text('name').primaryKey(),
  tail: integer('tail').notNull().default(-1),
  closed: integer('closed').notNull().default(0),
});

export const chunks = sqliteTable('chunks', {
  name: text('name').notNull(),
  position: integer('position').notNull(),
  bytes: bytes('bytes').notNull(),
}, (table) => [primaryKey({ columns: [table.name, table.position] })]);

export type StreamsSchema = {
  streams: typeof streams.$inferSelect;
  chunks: typeof chunks.$inferSelect;
};
