import { env, runInDurableObject } from 'cloudflare:test';
import { drizzle } from 'drizzle-orm/durable-sqlite';
import { migrate } from 'drizzle-orm/durable-sqlite/migrator';
import { expect, it } from 'vitest';
import queueMigrations from '../packages/world-cloudflare/drizzle/queue/migrations.js';
import storeMigrations from '../packages/world-cloudflare/drizzle/store/migrations.js';
import streamsMigrations from '../packages/world-cloudflare/drizzle/streams/migrations.js';

const tables = (sql: SqlStorage) => sql.exec<{ name: string }>(
  `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY name`,
).toArray().map(({ name }) => name);

it('creates each object’s tables on first use', async () => {
  await runInDurableObject(env.WORLD_STORE.getByName(crypto.randomUUID()), (_store, state) => {
    expect(tables(state.storage.sql)).toEqual(expect.arrayContaining(['dispatch_outbox', 'events', 'hooks', 'runs', 'steps', 'waits']));
  });
  await runInDurableObject(env.QUEUE_SHARD.getByName(crypto.randomUUID()), (_shard, state) => {
    expect(tables(state.storage.sql)).toContain('queue_dedup');
  });
  await runInDurableObject(env.RUN_STREAMS.getByName(crypto.randomUUID()), (_streams, state) => {
    expect(tables(state.storage.sql)).toEqual(expect.arrayContaining(['chunks', 'streams']));
  });
});

it('applies the baselines over tables created before Drizzle, keeping their rows', async () => {
  const cases = [
    { namespace: env.WORLD_STORE, migrations: storeMigrations,
      seed: `INSERT INTO runs (id, data, created_at, status, workflow_name) VALUES ('kept', 'x', 1, 'pending', 'w')`,
      read: 'SELECT id FROM runs' },
    { namespace: env.QUEUE_SHARD, migrations: queueMigrations,
      seed: `INSERT INTO queue_dedup (key, id, queue_name, payload_digest, delay_ms) VALUES ('kept', 'm', 'q', 'd', 0)`,
      read: 'SELECT key AS id FROM queue_dedup' },
    { namespace: env.RUN_STREAMS, migrations: streamsMigrations,
      seed: `INSERT INTO streams (name) VALUES ('kept')`,
      read: 'SELECT name AS id FROM streams' },
  ] as const;
  for (const { namespace, migrations, seed, read } of cases) {
    const stub = (namespace as DurableObjectNamespace).getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (_object, state) => {
      const sql = state.storage.sql;
      sql.exec(seed);
      // A pre-Drizzle object has the tables but no migration journal.
      sql.exec('DROP TABLE __drizzle_migrations');
      await migrate(drizzle(state.storage), migrations);
      expect(sql.exec(read).toArray()).toEqual([{ id: 'kept' }]);
      expect(sql.exec('SELECT COUNT(*) AS applied FROM __drizzle_migrations').one()).toEqual({ applied: 1 });
    });
  }
});
