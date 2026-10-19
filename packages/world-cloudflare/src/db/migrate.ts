import { drizzle } from 'drizzle-orm/durable-sqlite';
import { migrate } from 'drizzle-orm/durable-sqlite/migrator';

export type Migrations = Parameters<typeof migrate>[1];

/** Applies a Durable Object's pending Drizzle migrations before it serves any request. */
export function migrateOnStart(ctx: DurableObjectState, migrations: Migrations): void {
  ctx.blockConcurrencyWhile(() => migrate(drizzle(ctx.storage), migrations));
}
