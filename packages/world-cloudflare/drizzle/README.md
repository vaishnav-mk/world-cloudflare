# Migrations

One Drizzle Kit project per Durable Object class. Each object applies its pending migrations in its constructor, before it serves a request.

| Folder | Durable Object | Tables defined in |
| --- | --- | --- |
| `store/` | `WorldStore` | `src/store/tables.ts` |
| `queue/` | `QueueShard` | `src/queue/tables.ts` |
| `streams/` | `RunStreams` | `src/streams/tables.ts` |

To change a schema, edit its `tables.ts`, then run `pnpm db:generate` in this package and commit the new SQL, snapshot and `migrations.js`. Keep changes additive: retained executors and older Worker versions must keep working against the new tables.

The `0000_baseline.sql` files were edited to `IF NOT EXISTS`, because objects created before Drizzle already have those tables.
