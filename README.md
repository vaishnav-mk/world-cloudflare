# Workflow SDK World for Cloudflare

A [Workflow SDK](https://workflow-sdk.dev) v5 World that runs on Cloudflare Workers. The SDK compiles and replays workflows; this World stores their state in Durable Objects and delivers their queue messages. It is not a wrapper around native Cloudflare Workflows.

## Layout

| Path | Contents |
| --- | --- |
| [`packages/world-cloudflare`](packages/world-cloudflare/README.md) | The World package |
| [`packages/world-cloudflare/drizzle`](packages/world-cloudflare/drizzle/README.md) | Drizzle migrations, one set per Durable Object |
| `src/` | Example Worker: HTTP API, queue consumer, retained executor |
| `workflows/demo.ts` | Sample workflows |
| `test/` | workerd tests and SDK end-to-end tests |
| [`docs/contract-parity.md`](docs/contract-parity.md) | What is implemented and what is tested |
| [`docs/limits.md`](docs/limits.md) | Measured payload, step, CPU and concurrency limits |
| [`docs/isolate-engine.md`](docs/isolate-engine.md) | The native V8 workflow engine (the SDK's node engine, `WORKFLOW_VM=node`), measured against QuickJS |

## Run it

Requires Node 24 and pnpm. The SDK is the published `workflow` 5.5.0, unpatched.

```sh
pnpm install
pnpm run build:workflow   # compiles workflows/ with @workflow/world-cloudflare/build
pnpm run typecheck
pnpm test                 # workerd storage, queue, stream and contract tests
pnpm run test:e2e         # real SDK runs in workerd: QuickJS, the native engine, encrypted, and spec 9
pnpm build                # the workflow build and a Wrangler dry run
```

## Example API

| Method | Path | Does |
| --- | --- | --- |
| `POST` | `/runs` | Starts a run: `{"name":"demo","value":21}`; an array value, such as `{"name":"streamBytes","value":[1024,64]}`, passes several arguments |
| `GET` | `/runs/:id` | Run status |
| `GET` | `/runs/:id/result` | Waits for and returns the result |
| `GET` | `/runs/:id/events` | Server-sent events; resume with `?cursor=` or `Last-Event-ID`, filter with `?types=` |
| `GET` | `/runs/:id/hooks` | The run's hooks |
| `GET` | `/runs/:id/streams` | Stream names |
| `GET` | `/runs/:id/streams/:namespace` | Reads a stream, following live writes |
| `POST` | `/hooks/:token` | Resumes a hook with the JSON body |

The example has no authentication. Anyone who can reach it can start runs, and with a run ID or hook token can read or resume them. Don't deploy it with real data.

## Deploy

1. Create the R2 dead-letter and payload buckets, then deploy the Worker with your copy of `wrangler.jsonc`. To encrypt payloads at rest, set a secret before the first run: `openssl rand -base64 32 | wrangler secret put WORKFLOW_ENCRYPTION_SECRET` ([encryption](packages/world-cloudflare/README.md#encryption)).
2. Optional: to keep in-flight runs on the code that started them, give each release its own executor.
   1. Deploy a copy of `wrangler.executor.jsonc` into a Workers for Platforms dispatch namespace, bound as `EXECUTORS`, and point the main Worker's `main` at `src/dispatcher.ts`, which leaves the workflow bundle out so the Worker starts faster.
   2. Give it the main Worker's `WORKFLOW_ENCRYPTION_SECRET`, if set, and add its deployment ID to `EXECUTOR_ROUTES`, keeping the old entries.
   3. Move `DEPLOYMENT_ID` to the new release. Keep old executors until their runs drain.
3. Run the remote tests:

```sh
WORKFLOW_BASE_URL=https://<worker>.workers.dev node scripts/remote-e2e.mjs
WORKFLOW_BASE_URL=https://<worker>.workers.dev WORKFLOW_LOAD_COUNT=50 node scripts/remote-load.mjs
```

## Web UI

The SDK's web UI (`workflow web`) runs locally against a deployed World through `@workflow/world-cloudflare/remote`, a Node client for the Worker's `/world/rpc` endpoint.

1. Set a token on the Worker: `wrangler secret put WORLD_RPC_TOKEN`. Without it the endpoint does not exist. It can write events, so treat the token like a credential.
2. Create `.dev.vars.remote-ui` (gitignored):

   ```sh
   WORKFLOW_TARGET_WORLD=@workflow/world-cloudflare/remote
   WORKFLOW_CLOUDFLARE_URL=https://<worker>.workers.dev
   WORKFLOW_CLOUDFLARE_TOKEN=<the same token>
   ```

3. Run `pnpm ui` and open http://localhost:3456.

## Limits

- Runs, hook tokens and messages hash onto store and queue shards, 128 and 256 by default, and `SHARD_EPOCHS` adds shards for new runs without moving old ones. `runs.list` and global `hooks.list` ask every store, so they cost more than a run-scoped read and grow with the shard count.
- Payloads and stream chunks over 1 MiB go to R2. On Workers with QuickJS a 16 MiB step output works and 20 MiB does not, because the workflow VM runs out of memory, so the World rejects any serialized payload over 18 MiB with a 413 and the step fails instead of retrying forever. HTTP bodies are capped at 100 MB. Larger data goes from step to step in a byte stream: locally 4 GiB moved in 64 MiB chunks ([streams](docs/limits.md#streams)). Measurements are in [limits](docs/limits.md#payloads).
- R2 payload objects are never deleted, including ones left by writes that failed.
- A step that exceeds the CPU limit is retried until the queue gives up, and holds up its delivery lane meanwhile. The templates raise `limits.cpu_ms` to 300,000; split longer work across steps.
- Payloads and VM snapshots are stored in plaintext unless the Worker has a `WORKFLOW_ENCRYPTION_SECRET` ([encryption](packages/world-cloudflare/README.md#encryption)), and that secret cannot be rotated. Finished runs are kept forever. See [contract parity](docs/contract-parity.md).
- Steps run at least once, so give their side effects idempotency keys.
- `@workflow/world-cloudflare/build` relies on a few internals of `@workflow/builders` and `@workflow/core` 5.5.0 (see its comments); check it when upgrading the SDK.
- QuickJS probably needs more CPU than the Free plan's 10 ms limit.
- Workflow code runs in QuickJS by default, with VM snapshots so a new invocation does not replay the whole log. The SDK's node engine (`WORKFLOW_VM=node`) runs it natively in V8 on the World's `node:vm` replacement; see [isolate engine](docs/isolate-engine.md).
