# Workflow SDK v5 contract parity

Status against the pinned `@workflow/world` 5.x interface (`.vendor/workflow-sdk/packages/world/src/interfaces.ts`) and the capabilities the Vercel World advertises.

- **Built**: implemented in this World.
- **Workerd**: covered by `pnpm test` or `pnpm run test:e2e`.
- **Remote**: exercised against a deployed staging Worker.

A passing typecheck is not conformance, and none of this is checked by the SDK's own `@workflow/world-testing` suite yet. The target is equivalent SDK-visible behavior; Vercel's OIDC, billing, dashboard and regions are out of scope.

## Storage

| Contract | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| `runs.get` | ✓ | ✓ | ✓ | |
| `runs.getMany` | ✓ | ✓ | ✗ | Input order, nulls for missing runs |
| `runs.list` with status filters | ✓ | ✓ | ✗ | One status, several, or an empty set |
| `runs.waitForTerminalStatus` | ✓ | ✓ | ✗ | `AbortSignal` is ignored: it cannot cross RPC |
| `steps.get`, `steps.list` | ✓ | ✓ | ✗ | |
| `events.create`, atomic with entity state | ✓ | ✓ | ✓ | One SQLite transaction |
| `events.list` | ✓ | ✓ | ✓ | |
| `events.get`, `events.listByCorrelationId` | ✓ | ✗ | ✗ | |
| `hooks.get`, `hooks.getByToken`, `hooks.list` | ✓ | ✓ | ✓ | `list` and `getByToken` remotely |
| `resolveData: none` and `skip-step-inputs` | ✓ | ✓ | ✗ | |
| Dense per-run event slots | ✓ | ✓ | ✓ | |
| Stale-writer reports (`eventCount`, `sinceCursor`) | ✓ | ✓ | ✗ | |
| `run_created` with a null run ID | ✓ | ✓ | ✗ | The World mints `wrun_<ulid>` |
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
| Hook tokens unique across runs, `hook_conflict` | ✓ | ✓ | ✗ | |
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
| `queueBatch` | ✗ | ✗ | ✗ | |

## Dispatch recovery

Each event that owes a queue message writes an outbox row in its own transaction. The store's alarm and a one-minute cron publish any row the SDK's own publish missed.

| Event | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| `run_created` | ✓ | ✓ | ✗ | |
| `step_created` | ✓ | ✓ | ✗ | Uses the SDK's step queue key |
| `wait_created` | ✓ | ✓ | ✗ | Published at `resumeAt` |
| `hook_received` | ✓ | ✓ | ✓ | A failed publish recovered remotely without a second POST |
| Recovery of a step that was already running | ✗ | ✗ | ✗ | The SDK has owner leases; no test proves ours |

## Streams

| Contract | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| `write`, `close` | ✓ | ✓ | ✓ | |
| `writeMulti` | ✓ | ✓ | ✗ | Atomic and ordered |
| `list`, `getInfo`, `getChunks` | ✓ | ✓ | ✗ | |
| `get`: live reads and negative start | ✓ | ✓ | ✓ | Remote reader connected before the first write |
| Chunks over 2 MB | ✗ | ✗ | ✗ | Limited by the Durable Object row size |

## Optional APIs and capabilities

| Contract | Built | Workerd | Remote | Notes |
| --- | :---: | :---: | :---: | --- |
| `createBatch` | ✗ | ✗ | ✗ | |
| `cancelMany` | ✗ | ✗ | ✗ | |
| `experimentalSetAttributes` | ✗ | ✗ | ✗ | `attr_set` works |
| Snapshots (`experimental_snapshots`) | ✗ | ✗ | ✗ | |
| Encryption and key rotation | ✗ | ✗ | ✗ | Storage and DLQ hold plaintext |
| Analytics | ✗ | ✗ | ✗ | |
| `hookRetention`, `hookForceClaim`, `hookResumeDedup` | ✗ | ✗ | ✗ | Not advertised |
| `inBandFence`, `maxConcurrency`, `invoke` | ✗ | ✗ | ✗ | Not advertised |
| `deploymentAffinity` | ✗ | ✗ | ✗ | Routing works but the executor registry is a config variable |

## Operations

| Item | Done | Notes |
| --- | :---: | --- |
| Installable `createWorld()` package | ✓ | `packages/world-cloudflare`, unpublished |
| Published to npm | ✗ | The `@workflow` scope belongs to Vercel |
| Listed in `worlds-manifest.json` | ✗ | |
| SDK patch upstreamed | ✗ | Pinned to one SDK commit |
| SDK `@workflow/world-testing` suite | ✗ | It starts a Node server and cannot inject a Worker-bound World |
| Retention and cleanup of finished runs | ✗ | Runs, outbox and dedup rows are kept forever |
| Payloads over 2 MB | ✗ | No R2 offload |
| Sharding beyond one store and one queue shard | ✗ | |
| Multi-region placement | ✗ | |
