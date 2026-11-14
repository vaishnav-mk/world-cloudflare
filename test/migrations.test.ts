import { env, runDurableObjectAlarm, runInDurableObject } from 'cloudflare:test';
import { stringify } from 'devalue';
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
    expect(tables(state.storage.sql)).toEqual(expect.arrayContaining([
      'directory_outbox', 'directory_releases', 'dispatch_outbox', 'events', 'forced_disposals', 'hooks', 'index_hooks', 'index_outbox',
      'index_runs', 'index_sources', 'runs', 'start_hook_refusals', 'steps', 'token_releases', 'token_takeovers', 'tokens', 'waits',
    ]));
  });
  await runInDurableObject(env.QUEUE_SHARD.getByName(crypto.randomUUID()), (_shard, state) => {
    expect(tables(state.storage.sql)).toEqual(expect.arrayContaining(['delivery_counts', 'holds', 'inflight', 'queue_dedup']));
  });
  await runInDurableObject(env.RUN_STREAMS.getByName(crypto.randomUUID()), (_streams, state) => {
    expect(tables(state.storage.sql)).toEqual(expect.arrayContaining(['chunks', 'snapshot', 'snapshot_parts', 'streams']));
  });
});

it('upgrades a queue shard from durability 4.2 with its pending message intact', async () => {
  // A fresh stub per call: a stub stays broken once its object resets.
  const shardName = crypto.randomUUID();
  const shard = () => env.QUEUE_SHARD.getByName(shardName);
  const id = crypto.randomUUID();
  const keyOf = (state: DurableObjectState) => state.storage.sql.exec<{ name: string }>(
    `SELECT name FROM pragma_table_info('durability_fanout_deliveries') WHERE pk > 0 ORDER BY pk`,
  ).toArray().map(({ name }) => name);
  // A replay, which durability 4.2 held in the default channel too.
  await shard().enqueue(id, 'workflow_demo', stringify({ runId: 'migrated' }), 500);
  await runInDurableObject(shard(), (_shard, state) => {
    // Back to durability 4.2's schema the way a 4.2 deploy reverts newer migrations: run each stored
    // down script and forget it. That drops the fanout channel keys and the memo table.
    const sql = state.storage.sql;
    const newer = sql.exec<{ name: string; down: string }>(
      `SELECT name, down FROM durability_migrations WHERE name IN ('durability_0017_fanout_channels', 'durability_0012_create_memos')`,
    ).toArray();
    expect(newer).toHaveLength(2);
    for (const { name, down } of newer) {
      sql.exec(down);
      sql.exec('DELETE FROM durability_migrations WHERE name = ?', name);
    }
    expect(keyOf(state)).toEqual(['id']);
  });
  // A new instance runs durability 5's migrations over that storage, as on deploy.
  await runInDurableObject(shard(), (_shard, state) => state.abort('reset')).catch(() => {});
  await runInDurableObject(shard(), (_shard, state) => {
    expect(keyOf(state)).toEqual(['channel', 'id']);
    expect(state.storage.sql.exec('SELECT id, channel FROM durability_fanout_messages').toArray()).toEqual([{ id, channel: 'default' }]);
  });
  // The new instance has no in-memory timer, so the message waits for the physical alarm.
  await new Promise((resolve) => setTimeout(resolve, 500));
  expect(await runDurableObjectAlarm(shard())).toBe(true);
  await expect.poll(() => env.SINK.getByName('sink').received()).toContain(id);
});

it('applies the baselines over tables created before Drizzle, keeping their rows', async () => {
  const cases = [
    { namespace: env.WORLD_STORE, migrations: storeMigrations,
      seed: `INSERT INTO runs (id, data, created_at, status, workflow_name) VALUES ('kept', 'x', 1, 'pending', 'w')`,
      read: 'SELECT id FROM runs',
      newer: [
        'DROP TABLE directory_outbox', 'DROP TABLE directory_releases', 'ALTER TABLE runs DROP COLUMN index_slot',
        'DROP TABLE tokens', 'DROP TABLE token_releases',
        'DROP TABLE forced_disposals', 'DROP TABLE token_takeovers', 'ALTER TABLE runs DROP COLUMN seq_in_band',
        'DROP TABLE start_hook_refusals',
        'DROP TABLE index_outbox', 'DROP TABLE index_runs', 'DROP TABLE index_hooks', 'DROP TABLE index_sources',
      ] },
    { namespace: env.QUEUE_SHARD, migrations: queueMigrations,
      seed: `INSERT INTO queue_dedup (key, id, queue_name, payload_digest, delay_ms) VALUES ('kept', 'm', 'q', 'd', 0)`,
      read: 'SELECT key AS id FROM queue_dedup', newer: ['DROP TABLE inflight', 'DROP TABLE delivery_counts', 'DROP TABLE holds'] },
    { namespace: env.RUN_STREAMS, migrations: streamsMigrations,
      seed: `INSERT INTO streams (name) VALUES ('kept')`,
      read: 'SELECT name AS id FROM streams',
      newer: ['DROP TABLE snapshot', 'DROP TABLE snapshot_parts', 'ALTER TABLE streams DROP COLUMN lease_until'] },
  ] as const;
  for (const { namespace, migrations, seed, read, newer } of cases) {
    const stub = (namespace as DurableObjectNamespace).getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (_object, state) => {
      const sql = state.storage.sql;
      sql.exec(seed);
      // A pre-Drizzle object has the baseline tables but no migration journal and nothing later.
      for (const statement of ['DROP TABLE __drizzle_migrations', ...newer]) sql.exec(statement);
      await migrate(drizzle(state.storage), migrations);
      expect(sql.exec(read).toArray()).toEqual([{ id: 'kept' }]);
      expect(sql.exec('SELECT COUNT(*) AS applied FROM __drizzle_migrations').one()).toEqual({ applied: migrations.journal.entries.length });
    });
  }
});
