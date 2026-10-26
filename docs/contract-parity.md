# Workflow SDK v5 contract parity

Status against the pinned `@workflow/world` 5.x interface (`.vendor/workflow-sdk/packages/world/src/interfaces.ts`) and the capabilities the Vercel World advertises.

- **Built**: implemented in this World.
- **Workerd**: covered by `pnpm test` or `pnpm run test:e2e`.
- **Remote**: exercised against a deployed Worker.

A passing typecheck is not conformance, and none of this is checked by the SDK's own `@workflow/world-testing` suite yet. The target is equivalent SDK-visible behavior; Vercel's OIDC, billing, dashboard and regions are out of scope.

## Storage

| Contract | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| `runs.get` | ✓ | ✓ | ✓ | |
| `runs.getMany` | ✓ | ✓ | ✗ | Input order, nulls for missing runs. One call per store shard the IDs hash to, then one to the legacy store |
| `runs.list` with status filters | ✓ | ✓ | ✗ | One status, several, or an empty set. Asks every store shard and the legacy store, and merges their pages by `createdAt` then ID |
| `runs.waitForTerminalStatus` | ✓ | ✓ | ✗ | `AbortSignal` is ignored: it cannot cross RPC |
| `steps.get`, `steps.list` | ✓ | ✓ | ✗ | |
| `events.create`, atomic with entity state | ✓ | ✓ | ✓ | One SQLite transaction in the run's store shard |
| `events.list` | ✓ | ✓ | ✓ | |
| `events.get`, `events.listByCorrelationId` | ✓ | ✗ | ✗ | |
| `hooks.get`, `hooks.getByToken`, `hooks.list` | ✓ | ✓ | ✓ | `list` and `getByToken` remotely. `getByToken` reads the token's shard, a run's `list` its run's shard; `get` and a global `list` ask every store |
| `resolveData: none` and `skip-step-inputs` | ✓ | ✓ | ✗ | `skip-step-inputs` leaves out `step_created` inputs, so replay never loads them from R2 |
| Dense per-run event slots | ✓ | ✓ | ✓ | |
| Stale-writer reports (`eventCount`, `sinceCursor`) | ✓ | ✓ | ✗ | |
| `run_created` with a null run ID | ✓ | ✓ | ✗ | The World client mints `wrun_<ulid>` |
| Payloads over 1 MiB, in R2 | ✓ | ✓ | ✓ | Resolved by the World client, as the SDK expects; 16 MiB step outputs on Workers with QuickJS ([limits](limits.md#payloads)) |
| Existing runs keep their spec version | ✓ | ✓ | ✗ | |

## Event semantics

| Contract | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| Run lifecycle: start, complete, fail | ✓ | ✓ | ✓ | |
| Run cancellation | ✓ | ✓ | ✗ | |
| Lazy `run_started` that carries the run | ✓ | ✓ | ✗ | |
| Steps: create, start, complete, fail | ✓ | ✓ | ✓ | |
| Lazy `step_started`, one winner per step | ✓ | ✓ | ✗ | |
| Step retries with `retryAfter` | ✓ | ✓ | ✓ | An early start throws a plain `Error`, not `TooEarlyError` |
| Running steps may finish after cancellation | ✓ | ✓ | ✗ | |
| Waits: create and complete | ✓ | ✓ | ✓ | 45 s sleep tested remotely |
| Hooks: create, receive, dispose | ✓ | ✓ | ✓ | |
| Hook tokens unique across runs, `hook_conflict` | ✓ | ✓ | ✗ | The run's shard claims the token on the token's shard before its write. A freed token is free just after the freeing write, not with it |
| Token retention after a run finishes | ✓ | ✓ | ✗ | |
| `hook_received` rejected after a run finishes | ✓ | ✓ | ✗ | `RunExpiredError` |
| `resumeId` dedup for `hook_received` | ✓ | ✗ | ✗ | |
| `attr_set`: 64 keys, 8 KiB per write | ✓ | ✓ | ✗ | Limits come from the SDK |
| Forced hook claims | ✗ | ✗ | ✗ | Throws |
| In-band writer fencing | ✗ | ✗ | ✗ | Throws |
| Dynamic workflow code refs | ✗ | ✗ | ✗ | Throws |

## Queue

| Contract | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| `queue()` and `createQueueHandler` | ✓ | ✓ | ✓ | |
| Delays | ✓ | ✓ | ✓ | Nothing longer than 45 s tested |
| Retries requested by the SDK (`timeoutSeconds`) | ✓ | ✓ | ✓ | |
| Idempotency keys, first message wins | ✓ | ✓ | ✗ | |
| Runs stay on their deployment's executor | ✓ | ✓ | ✓ | Workers for Platforms, across D, E and F |
| Dead letters saved to R2 | ✓ | ✓ | ✗ | |
| Dead-lettered wakes, steps and waits reopened | ✓ | ✓ | ✗ | With a fresh queue key |
| `queueBatch` | ✓ | ✓ | ✗ | Three round trips per batch and one fanout transaction; per-entry failures ([limits](limits.md#queue-delivery)) |

## Dispatch recovery

Each event that owes a queue message writes an outbox row in its own transaction. The store shard's alarm publishes any row the SDK's own publish missed; the shard arms it before the write, so a crash just after the commit cannot strand the row. The one-minute cron reaches only the legacy store, which holds runs created before store shards.

| Event | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| `run_created` | ✓ | ✓ | ✗ | |
| `step_created` | ✓ | ✓ | ✗ | Uses the SDK's step queue key |
| `wait_created` | ✓ | ✓ | ✗ | Published at `resumeAt` |
| `hook_received` | ✓ | ✓ | ✓ | A failed publish recovered remotely without a second POST |
| Delivery cut off by a queue shard reset | ✓ | ✓ | ✗ | A fresh shard redelivers it; the cron wakes the shard every minute |
| Recovery of a step that was already running | ✗ | ✗ | ✗ | The SDK has owner leases; no test proves ours |

## Streams

| Contract | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| `write`, `close` | ✓ | ✓ | ✓ | |
| `writeMulti` | ✓ | ✓ | ✗ | Atomic and ordered |
| `list`, `getInfo`, `getChunks` | ✓ | ✓ | ✗ | |
| `get`: live reads and negative start | ✓ | ✓ | ✓ | Remote reader connected before the first write |
| Chunks over 1 MiB, in R2 | ✓ | ✓ | ✗ | 70 MiB locally; the SDK's reader rejects larger frames ([limits](limits.md#payloads)) |

## Optional APIs and capabilities

| Contract | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| `createBatch` | ✓ | ✓ | ✗ | One transaction with a savepoint per event; a conflict is a 409 item. Used by the node and isolate engines, not QuickJS |
| `cancelMany` | ✗ | ✗ | ✗ | |
| `experimentalSetAttributes` | ✗ | ✗ | ✗ | `attr_set` works |
| Snapshots (`experimental_snapshots`) | ✓ | ✓ | ✓ | In the run's `RunStreams` object, in 1.9 MB parts; stored in plaintext ([limits](limits.md#long-runs)) |
| Encryption and key rotation | ✗ | ✗ | ✗ | Storage, snapshots and DLQ hold plaintext |
| Analytics | ✗ | ✗ | ✗ | |
| `hookRetention`, `hookForceClaim`, `hookResumeDedup` | ✗ | ✗ | ✗ | Not advertised |
| `inBandFence`, `maxConcurrency`, `invoke` | ✗ | ✗ | ✗ | Not advertised |
| `deploymentAffinity` | ✗ | ✗ | ✗ | Routing works but the executor registry is a config variable |

## Operations

| Item | Done | Notes |
| --- | :---: | --- |
| Installable `createWorld()` package | ✓ | `packages/world-cloudflare`, unpublished |
| SDK web UI (`workflow web`) | ✓ | Through the `@workflow/world-cloudflare/remote` Node client; tested against a deployed Worker |
| Versioned schema migrations | ✓ | Drizzle Kit, applied by each Durable Object on start; tested over pre-Drizzle tables |
| Published to npm | ✗ | The `@workflow` scope belongs to Vercel |
| Listed in `worlds-manifest.json` | ✗ | |
| SDK patch upstreamed | ✗ | Pinned to one SDK commit |
| SDK `@workflow/world-testing` suite | ✗ | It starts a Node server and cannot inject a Worker-bound World |
| Retention and cleanup of finished runs | ✗ | Runs, outbox and dedup rows, and R2 payloads, are kept forever |
| Payloads over 1 MiB | ✓ | In R2; 16 MiB step outputs on Workers with QuickJS, and payloads over 18 MiB serialized are rejected ([limits](limits.md#payloads)) |
| Cleanup of orphaned R2 payloads | ✗ | A write that fails after its payload reached R2 leaves the object behind |
| Store shards, no single object per run or token | ✓ | 128 store shards hold runs and hook tokens; runs created before them stay in the legacy store |
| Sharding beyond one queue shard | ✓ | 64 queue shards |
| Multi-region placement | ✗ | |
