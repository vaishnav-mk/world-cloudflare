# Workflow SDK v5 on Cloudflare

A Cloudflare World for the [Workflow SDK](https://workflow-sdk.dev/worlds/building-a-world). The SDK still owns workflow compilation and event replay. SQLite Durable Objects store its run log, entities, and live streams; [`durability@4.1.2`](https://www.npmjs.com/package/durability) delivers queue messages through an alarm-backed fanout and a private Worker service binding. This is **not** a wrapper around native `WorkflowEntrypoint`.

The World is the [`@workflow/world-cloudflare`](packages/world-cloudflare/README.md) workspace package. The repo root is an example app that uses it: `src/index.ts` is the public API and queue consumer, `src/executor.ts` is the retained executor, and `workflows/demo.ts` holds the sample workflows. The tests in `test/` cover both.

## Reproduce locally

Requires Node 24, pnpm, and Git. Start with the setup script, not `pnpm install`: it checks out the pinned Workflow SDK commit, applies [`patches/workflow-sdk-cloudflare-workers.patch`](patches/workflow-sdk-cloudflare-workers.patch), builds the edge-compatible CLI/runtime, links them, and compiles the example workflows. The SDK checkout lives in ignored `.vendor/`.

```sh
./scripts/setup-sdk.sh
pnpm run typecheck
pnpm test
pnpm run test:e2e
pnpm build                         # SDK build + Wrangler dry-run
```

The workerd E2E suite starts real SDK runs and covers concurrent requests, step execution and retry/failure, a durable sleep, hook resume, named streaming output, result decoding, and event subscription with cursor replay. `pnpm test` checks storage, queue retry/delay, and raw stream isolation in workerd. The same paths passed against a remote staging Worker; see [staging results](docs/staging-e2e.md).

The sample HTTP API has no bearer authentication. Its `workers.dev` endpoints are public: anyone can start billable runs, read a run if they know its ID, and use a hook if they know its hook token. Input/body-size validation remains. The executor's private service-binding check remains in place. Do not put customer data into this demo. Provision/configure the DLQ R2 bucket and use an immutable deployment ID per release. Never deploy `wrangler.app.test.jsonc` as a public app.

The sample API accepts public `POST /runs` (`{"name":"demo","value":21}`), `GET /runs/:id`, `GET /runs/:id/result`, `GET /runs/:id/streams/:namespace`, `GET /runs/:id/hooks`, `POST /hooks/:token`, and `GET /runs/:id/events`. The last endpoint is SSE: it replays the SDK event log, follows new commits, accepts `?cursor=evnt_...` or `Last-Event-ID`, optionally filters with `?types=run_completed,run_failed`, and closes at a terminal run. Native Cloudflare `WorkflowInstance.subscribe()` reports *native* instance events; it cannot subscribe to this SDK World.

## Remote staging tests

Deploy the example with your own copy of `wrangler.jsonc`. For retained executors, also deploy one copy of `wrangler.executor.jsonc` per release into a dispatch namespace. Staging configs (`wrangler.staging.jsonc`, `wrangler.executor.staging*.jsonc`) are local and gitignored. The remote scripts need no API token. The `:token` in `POST /hooks/:token` is the SDK's hook identifier, not an API bearer token.

```sh
WORKFLOW_BASE_URL=https://<your-worker>.workers.dev node scripts/remote-e2e.mjs
# Optional bounded test, at most 100 runs:
WORKFLOW_BASE_URL=https://<your-worker>.workers.dev WORKFLOW_LOAD_COUNT=50 node scripts/remote-load.mjs
```

To ship a changed workflow build:

1. Upload a new immutable executor to the dispatch namespace.
2. Add its deployment ID to `EXECUTOR_ROUTES`, keeping the old mappings.
3. Move the stable Worker's `DEPLOYMENT_ID` to the new ID.

Hook and sleep continuations survived this sequence across three executors; see the [staging report](docs/staging-e2e.md). Do not delete old executors until their runs drain. Staging Workers, executors, buckets and test data stay billable until you remove them.

## Current limits

This is an E2E **staging integration**, not production-complete v5 conformance. One global store DO makes event slots, run state, and cross-run hook-token uniqueness transactional but bottlenecks all runs. The queue is one fanout shard. Retained D/E executors route correctly, but the executor registry is a config variable, not a durable publication/retirement protocol. An outbox now journals `run_created`, eager `step_created`, `wait_created`, and `hook_received` alongside their events; alarm recovery has a one-minute cron backstop. A failed hook publish recovered remotely without a second POST. Start recovery has only workerd and remote happy-path coverage. This does **not** cover recovery of an already-running step after losing its owner lease, or every `wait_completed`/child handoff. Exhausted deliveries reopen retained start/hook wakes, pending steps, and waiting timers with a fresh redrive key (tested in workerd, not remotely); other messages still lack recovery. Store and streams are unencrypted. Fanout dead-letters exhausted messages to R2, but this adapter does not yet mark their runs failed or implement general redrive/retention. Runs, outbox history and deduplication records have no retention policy. The staging cron and Worker resources remain billable until removed.

The full [v5 contract-parity ledger](docs/contract-parity.md) tracks remaining work. [Prior art](docs/prior-art.md) compares other Cloudflare Worlds. The World does not implement event batching, forced hook claims, in-band fencing, snapshots, or dynamic workflow code refs. Error classes and additional edge-case semantics still need the SDK's full conformance suite in workerd; its published suite starts a Node server and cannot directly inject a Worker-bound World. Do not enable optional World capabilities without their full contracts. A step's external side effects still need idempotency keys; durable execution is at-least-once, not exactly-once.

The upstream SDK patch removes Node-only imports from the Cloudflare flow bundle and statically imports compiled QuickJS WASM modules. It is pinned to `eb9acbc8ba0041e2cbcd06049c29e56568b7ece3`; it must be rebased and tested before upgrading the SDK. The tested bundle is about 14.6 MB uncompressed, below Cloudflare's current [64 MiB Worker size limit](https://developers.cloudflare.com/workers/platform/limits/#worker-size); compressed size is no longer capped. The Free plan's 10 ms CPU budget may still be too low for the QuickJS runtime; the staging account's billing plan was not verified.
