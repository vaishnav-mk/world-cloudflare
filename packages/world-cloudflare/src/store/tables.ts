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
  /**
   * Set only on the old directory's index copy of a sharded run, left in the legacy store: the run's
   * last event slot when the copy was pushed. Null for a run this store holds. Nothing writes it now.
   */
  index_slot: integer('index_slot'),
  /**
   * Spec 9 runs: positions allocated to in-band writes, the in-band fence's count. Null for runs
   * below spec 9, which have no fence.
   */
  seq_in_band: integer('seq_in_band'),
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

/**
 * On a token shard: the hook that holds each token hashing to the shard, copied at claim time. A
 * token is unique across every run because only its shard grants it.
 */
export const tokens = sqliteTable('tokens', {
  token: text('token').primaryKey(),
  hook_id: text('hook_id').notNull(),
  data: text('data').notNull(),
  /** 1 once the holder run has finished: the token is then free after the hook's retention. */
  finished: integer('finished').notNull().default(0),
});

/** Token releases a store owes the token shards: a disposed hook (`disposed` 1), or a hook whose run finished. */
export const tokenReleases = sqliteTable('token_releases', {
  hook_id: text('hook_id').primaryKey(),
  token: text('token').notNull(),
  disposed: integer('disposed').notNull(),
  attempts: integer('attempts').notNull().default(0),
  next_at: integer('next_at').notNull(),
});

/**
 * Hooks another run took over with a forced claim, and what took them. A row also stands for a
 * hook not created yet, so its creation is refused once it commits. Never deleted.
 */
export const forcedDisposals = sqliteTable('forced_disposals', {
  hook_id: text('hook_id').primaryKey(),
  run_id: text('run_id').notNull(),
  token: text('token').notNull(),
  claimed_by_run_id: text('claimed_by_run_id').notNull(),
  claimed_by_hook_id: text('claimed_by_hook_id').notNull(),
  /** The encoded `HookClaimedFrom` the takeover answered with, so a retry gets the same answer. */
  claimed_from: text('claimed_from').notNull(),
});

/**
 * On a token shard: forced claims whose holder has not yet confirmed the disposal. The token moves
 * to the claimer only after its holder's store disposed of the hook, so a delivery that resolved
 * the holder first is refused rather than lost. The alarm retries a row a failed call left.
 */
export const tokenTakeovers = sqliteTable('token_takeovers', {
  token: text('token').primaryKey(),
  /** Null only for takeovers accepted before operation receipts. */
  operation: text('operation'),
  /** The encoded hook that holds the token. */
  holder: text('holder').notNull(),
  /** The encoded hook taking it over. */
  claimer: text('claimer').notNull(),
  attempts: integer('attempts').notNull().default(0),
  next_at: integer('next_at').notNull(),
});

/** Accepted events awaiting a token decision. One per run, with store-wide hook ID reservations. */
export const hookClaimIntents = sqliteTable('hook_claim_intents', {
  run_id: text('run_id').primaryKey(),
  operation_id: text('operation_id').notNull().unique(),
  hook_id: text('hook_id').notNull().unique(),
  data: text('data').notNull(),
  next_at: integer('next_at').notNull(),
});

/**
 * Start Hook candidates refused because another run held the token: the decision is immutable, so
 * every later copy of the candidate is refused naming the same run. An accepted candidate is its run.
 */
export const startHookRefusals = sqliteTable('start_hook_refusals', {
  run_id: text('run_id').primaryKey(),
  token: text('token').notNull(),
  conflicting_run_id: text('conflicting_run_id').notNull(),
});

/**
 * List rows a store owes the index shards: a run's (`hook_id` '') or one of its hooks'. A push sends
 * the current state of each, so a row owed again before its push needs no second one.
 */
export const indexOutbox = sqliteTable('index_outbox', {
  run_id: text('run_id').notNull(),
  hook_id: text('hook_id').notNull(),
  /** Bumped by each owe, so a push settles only the owes it carried. */
  seq: integer('seq').notNull().default(0),
  attempts: integer('attempts').notNull().default(0),
  next_at: integer('next_at').notNull(),
}, (table) => [primaryKey({ columns: [table.run_id, table.hook_id] }), index('index_outbox_due').on(table.next_at)]);

/** On an index shard: the list rows of the runs that hash to it, each the run without its payloads. */
export const indexRuns = sqliteTable('index_runs', {
  id: text('id').primaryKey(),
  data: text('data').notNull(),
  created_at: integer('created_at').notNull(),
  status: text('status').notNull(),
  workflow_name: text('workflow_name').notNull(),
}, (table) => [
  index('index_runs_created').on(table.created_at, table.id),
  index('index_runs_workflow').on(table.workflow_name, table.created_at, table.id),
  index('index_runs_status').on(table.status, table.created_at, table.id),
]);

/** On an index shard: the hooks whose IDs hash to it, with whether their run has finished (an encoded `HeldToken`). */
export const indexHooks = sqliteTable('index_hooks', {
  id: text('id').notNull(),
  run_id: text('run_id').notNull(),
  data: text('data').notNull(),
  created_at: integer('created_at').notNull(),
}, (table) => [primaryKey({ columns: [table.id, table.run_id] }), index('index_hooks_created').on(table.created_at, table.id)]);

/** On an index shard: the stores that have pushed it every run and hook they held before the index. */
export const indexSources = sqliteTable('index_sources', {
  store: text('store').primaryKey(),
  indexed_at: integer('indexed_at').notNull(),
});

// The directory's run index used these two. Nothing writes them now; they stay because schema changes are additive.

/** A run store's pending push of its run row to the directory, at the slot that changed it. */
export const directoryOutbox = sqliteTable('directory_outbox', {
  run_id: text('run_id').primaryKey(),
  slot: integer('slot').notNull(),
  attempts: integer('attempts').notNull().default(0),
  next_at: integer('next_at').notNull(),
});

/** Hooks a run store disposed of, released in the directory with the next push. */
export const directoryReleases = sqliteTable('directory_releases', {
  hook_id: text('hook_id').primaryKey(),
  run_id: text('run_id').notNull(),
});

/** Row types for workers-qb, keyed by table name. */
export type StoreSchema = {
  runs: typeof runs.$inferSelect;
  steps: typeof steps.$inferSelect;
  hooks: typeof hooks.$inferSelect;
  waits: typeof waits.$inferSelect;
  events: typeof events.$inferSelect;
  dispatch_outbox: typeof dispatchOutbox.$inferSelect;
  tokens: typeof tokens.$inferSelect;
  token_releases: typeof tokenReleases.$inferSelect;
  forced_disposals: typeof forcedDisposals.$inferSelect;
  token_takeovers: typeof tokenTakeovers.$inferSelect;
  hook_claim_intents: typeof hookClaimIntents.$inferSelect;
  start_hook_refusals: typeof startHookRefusals.$inferSelect;
  index_outbox: typeof indexOutbox.$inferSelect;
  index_runs: typeof indexRuns.$inferSelect;
  index_hooks: typeof indexHooks.$inferSelect;
  index_sources: typeof indexSources.$inferSelect;
};
