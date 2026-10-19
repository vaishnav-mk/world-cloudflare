import { sql } from 'drizzle-orm';
import { index, integer, primaryKey, sqliteTable, text, uniqueIndex } from 'drizzle-orm/sqlite-core';
import { bytes } from '../db/columns.js';
import { OutboxStatus } from '../enums.js';

/**
 * The store's tables. Drizzle Kit generates migrations from these definitions; queries use
 * workers-qb with the row types inferred here. Keys are the SQL column names.
 */

/** Entities are encoded whole in `data`; the other columns exist to filter and order. */
export const runs = sqliteTable('runs', {
  id: text('id').primaryKey(),
  data: text('data').notNull(),
  created_at: integer('created_at').notNull(),
  status: text('status').notNull(),
  workflow_name: text('workflow_name').notNull(),
}, (table) => [index('runs_created').on(table.created_at, table.id)]);

export const steps = sqliteTable('steps', {
  run_id: text('run_id').notNull(),
  id: text('id').notNull(),
  data: text('data').notNull(),
  created_at: integer('created_at').notNull(),
}, (table) => [
  primaryKey({ columns: [table.run_id, table.id] }),
  index('steps_created').on(table.run_id, table.created_at, table.id),
]);

/** A token is unique across every run. */
export const hooks = sqliteTable('hooks', {
  id: text('id').primaryKey(),
  run_id: text('run_id').notNull(),
  token: text('token').notNull().unique(),
  data: text('data').notNull(),
  created_at: integer('created_at').notNull(),
}, (table) => [index('hooks_created').on(table.run_id, table.created_at, table.id)]);

export const waits = sqliteTable('waits', {
  run_id: text('run_id').notNull(),
  id: text('id').notNull(),
  data: text('data').notNull(),
}, (table) => [primaryKey({ columns: [table.run_id, table.id] })]);

/** The event log. `slot` is the event's dense 1-based position in its run. */
export const events = sqliteTable('events', {
  run_id: text('run_id').notNull(),
  slot: integer('slot').notNull(),
  kind: text('kind').notNull(),
  correlation_id: text('correlation_id'),
  spec_version: integer('spec_version').notNull(),
  created_at: integer('created_at').notNull(),
  occurred_at: integer('occurred_at'),
  resume_id: text('resume_id'),
  resume_digest: text('resume_digest'),
  data: text('data'),
  payload: bytes('payload'),
}, (table) => [
  primaryKey({ columns: [table.run_id, table.slot] }),
  index('events_correlation').on(table.run_id, table.correlation_id, table.slot),
  uniqueIndex('events_resume').on(table.run_id, table.resume_id).where(sql`resume_id IS NOT NULL`),
]);

/** Queue messages owed by committed events. */
export const dispatchOutbox = sqliteTable('dispatch_outbox', {
  run_id: text('run_id').notNull(),
  slot: integer('slot').notNull(),
  queue_name: text('queue_name').notNull(),
  deployment_id: text('deployment_id').notNull(),
  attempts: integer('attempts').notNull().default(0),
  redrive: integer('redrive').notNull().default(0),
  next_at: integer('next_at').notNull(),
  status: text('status').notNull().default(OutboxStatus.Pending),
}, (table) => [
  primaryKey({ columns: [table.run_id, table.slot] }),
  index('dispatch_outbox_pending').on(table.status, table.next_at),
]);

/** Row types for workers-qb, keyed by table name. */
export type StoreSchema = {
  runs: typeof runs.$inferSelect;
  steps: typeof steps.$inferSelect;
  hooks: typeof hooks.$inferSelect;
  waits: typeof waits.$inferSelect;
  events: typeof events.$inferSelect;
  dispatch_outbox: typeof dispatchOutbox.$inferSelect;
};
