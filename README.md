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
| `patches/` | SDK patch that adds a Workers build target |
| [`docs/contract-parity.md`](docs/contract-parity.md) | What is implemented and what is tested |
| [`docs/limits.md`](docs/limits.md) | Measured payload, step, CPU and concurrency limits |

## Run it

Requires Node 24, pnpm and Git. Start with the setup script rather than `pnpm install`: it clones the pinned SDK, applies the patch, then builds and links it.

```sh
./scripts/setup-sdk.sh
pnpm run typecheck
pnpm test            # workerd storage, queue, stream and contract tests
pnpm run test:e2e    # real SDK runs in workerd
pnpm build           # workflow build and a Wrangler dry run
```

## Example API

| Method | Path | Does |
| --- | --- | --- |
| `POST` | `/runs` | Starts a run: `{"name":"demo","value":21}` |
| `GET` | `/runs/:id` | Run status |
| `GET` | `/runs/:id/result` | Waits for and returns the result |
| `GET` | `/runs/:id/events` | Server-sent events; resume with `?cursor=` or `Last-Event-ID`, filter with `?types=` |
| `GET` | `/runs/:id/hooks` | The run's hooks |
| `GET` | `/runs/:id/streams` | Stream names |
| `GET` | `/runs/:id/streams/:namespace` | Reads a stream, following live writes |
| `POST` | `/hooks/:token` | Resumes a hook with the JSON body |

The example has no authentication. Anyone who can reach it can start runs, and with a run ID or hook token can read or resume them. Don't deploy it with real data.

## Deploy

1. Create the R2 dead-letter and payload buckets, then deploy the Worker with your copy of `wrangler.jsonc`.
2. Optional: to keep in-flight runs on the code that started them, give each release its own executor.
   1. Deploy a copy of `wrangler.executor.jsonc` into a Workers for Platforms dispatch namespace, bound as `EXECUTORS`.
   2. Add its deployment ID to `EXECUTOR_ROUTES`, keeping the old entries.
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

- One Durable Object holds every run and one queue shard delivers every message, so both cap throughput.
- Payloads and stream chunks over 1 MiB go to R2. Locally, step and run payloads up to 100 MiB, stream chunks up to 70 MiB and 40 MB hook payloads work; Workers' 128 MB memory limit will make production lower, and it is not measured yet. HTTP bodies are capped at 100 MB. Measurements are in [limits](docs/limits.md#payload-sizes).
- R2 payload objects are never deleted, including ones left by writes that failed.
- A step that exceeds the CPU limit is retried indefinitely and holds up the single queue shard. The templates raise `limits.cpu_ms` to 300,000; split longer work across steps.
- No encryption, retention, batching or forced hook claims. Payloads and VM snapshots are stored in plaintext. See [contract parity](docs/contract-parity.md).
- Steps run at least once, so give their side effects idempotency keys.
- The SDK patch is pinned to one SDK commit; rebase it before upgrading the SDK.
- QuickJS probably needs more CPU than the Free plan's 10 ms limit.
