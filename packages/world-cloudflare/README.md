# world-cloudflare

A [Workflow SDK](https://workflow-sdk.dev/worlds/building-a-world) v5 World for Cloudflare Workers, built on SQLite Durable Objects and R2. It runs the published, unpatched `workflow` 5.5.0, with spec 8 runs by default or spec 9 with `WORKFLOW_SPEC_VERSION=9`. Independent project, not an official Cloudflare or Vercel package.

[Documentation](https://world-cloudflare.vaishnav.one/) · [Architecture](https://world-cloudflare.vaishnav.one/docs/architecture) · [Measurements](https://world-cloudflare.vaishnav.one/docs/benchmarks) · [Source](https://github.com/vaishnav-mk/world-cloudflare)

| Durable Object | Holds |
| --- | --- |
| `WorldStore` | Runs with their events, steps, hooks and waits, sharded by run ID; also the list indexes and hook token registry |
| `QueueShard` | Messages, delays, retries and idempotency keys |
| `Runner` | Where flows and steps run; holds nothing durable |
| `RunStreams` | One per run: its streams and VM snapshot |

Queue shards use [durability](https://github.com/avenceslau/durability) for alarms, batched delivery, retries and dead letters; the World keeps run state and the attempt cap. See [every service](https://world-cloudflare.vaishnav.one/docs/services).

## Setup

```sh
pnpm add world-cloudflare workflow@5.5.0
```

With pnpm 11 or later, copy `allowBuilds` from the repo's [`pnpm-workspace.yaml`](https://github.com/vaishnav-mk/world-cloudflare/blob/main/pnpm-workspace.yaml), or install fails with `ERR_PNPM_IGNORED_BUILDS`.

Build the workflows before `wrangler dev` or `wrangler deploy`. It takes the directories to scan and writes `.well-known/workflow/v1/`:

```sh
node node_modules/world-cloudflare/dist/build.js workflows
```

Then export the Durable Objects and the queue consumer, and register the World:

```ts
import { createQueueConsumer, createWorld, drainOutbox, registerWorld, type WorldEnv } from 'world-cloudflare';
import { POST as flow } from '../.well-known/workflow/v1/flow.mjs';

export { QueueShard, Runner, RunStreams, WorldStore } from 'world-cloudflare';
export const QueueConsumer = createQueueConsumer(flow);

registerWorld(createWorld());

export default {
  scheduled: (_controller: ScheduledController, env: WorldEnv) => drainOutbox(env),
  fetch: (request: Request, env: WorldEnv) => new Response('ok'), // your routes
};
```

Use `registerWorld()`, not `setWorld()` or `WORKFLOW_TARGET_WORLD`: `workflow/runtime` draws a random ID as it loads, which Workers forbid at startup. For TypeScript, declare the generated module as [`src/generated-flow.d.ts`](https://github.com/vaishnav-mk/world-cloudflare/blob/main/src/generated-flow.d.ts) does.

Wrangler does not run the SDK's client transform, so start workflows by ID. IDs are `workflow//./<file without extension>//<export>`, listed in `.well-known/workflow/v1/manifest.json`:

```ts
import { start } from 'workflow/api';

const run = await start({ workflowId: 'workflow//./workflows/hello//hello' }, [21]);
```

### Wrangler config

See the example [`wrangler.jsonc`](https://github.com/vaishnav-mk/world-cloudflare/blob/main/wrangler.jsonc). It needs:
- the `nodejs_compat` flag;
- the four Durable Object bindings, with a migration that makes them SQLite-backed (without it, each fails with `DrizzleError: Rollback`):
  ```jsonc
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["WorldStore", "QueueShard", "RunStreams", "Runner"] }]
  ```
- a `CONSUMER` service binding to the Worker itself, with `"entrypoint": "QueueConsumer"`;
- the `DLQ` and `PAYLOADS` R2 buckets and a `DEPLOYMENT_ID`;
- aliases for SDK packages that do not load on Workers (in vitest's Workers pool, use `workerAliases` from `world-cloudflare/build` as `resolve.alias`):
  ```jsonc
  "alias": {
    "@workflow/world-local": "world-cloudflare/stubs/world",
    "@workflow/world-vercel": "world-cloudflare/stubs/world",
    "@vercel/functions": "world-cloudflare/stubs/vercel-functions"
  }
  ```
- a `* * * * *` cron for `drainOutbox`;
- `limits.cpu_ms` of 300,000 and a high `limits.subrequests`: the SDK makes about one Durable Object call per event.

Migration SQL is bundled by Wrangler's default `Text` rule (`**/*.sql`). A config with its own `Text` rule must include that glob or set `"fallthrough": true`.

## Configuration

| Binding | Type |
| --- | --- |
| `WORLD_STORE`, `QUEUE_SHARD`, `RUN_STREAMS`, `RUNNER` | SQLite Durable Objects of the matching classes |
| `CONSUMER` | Service binding to this Worker's `QueueConsumer` entrypoint |
| `DLQ`, `PAYLOADS` | R2 buckets for dead letters, and for payloads and stream chunks over 1 MiB |
| `EXECUTORS` | Optional dispatch namespace of [retained executors](#retained-executors) |

| Variable | Meaning |
| --- | --- |
| `DEPLOYMENT_ID` | Required. A new ID per release |
| `SHARD_EPOCHS` | Shard counts per epoch. Default: 128 store, 256 queue and 16 index shards. [Scaling](https://world-cloudflare.vaishnav.one/docs/scaling#routing) |
| `WORKFLOW_VM` | Engine for new runs: `quickjs` or `node` (default). [Engines](https://world-cloudflare.vaishnav.one/docs/engines) |
| `WORKFLOW_ENCRYPTION_SECRET` | Turns on [encryption](#encryption) |
| `WORKFLOW_SPEC_VERSION` | `9` for spec 9 runs on the Worker and its executors; older runs keep spec 8 |
| `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED` | `1` saves QuickJS VM snapshots without a secret; otherwise each invocation replays the whole log |
| `WORKFLOW_COMPRESSION_CODEC` | Set `gzip`; zstd in workerd throws on incompressible input |
| `QUEUE_MAX_ATTEMPTS` | Deliveries before dead-lettering. Default 64 |
| `FLOW_BACKGROUND_BUDGET_MS` | How long a flow request waits for background work. Default 13.5 minutes |
| `EXECUTOR_ROUTES` | JSON map from deployment ID to retained executor script |
| `LOCATION_HINT` | Optional region, such as `weur`, for every object the World creates. Set it before the first run. [Placement](https://world-cloudflare.vaishnav.one/docs/scaling#placement) |

Every Worker that runs World code, retained executors included, needs the same bindings, `SHARD_EPOCHS`, `LOCATION_HINT` and `WORKFLOW_ENCRYPTION_SECRET`. [Which changes affect old runs](https://world-cloudflare.vaishnav.one/docs/operations#config).

## Behavior to know

- **Steps can run more than once.** Make side effects idempotent. [Delivery](https://world-cloudflare.vaishnav.one/docs/state-and-delivery#delivery)
- **Adding capacity.** Append a `SHARD_EPOCHS` entry, `{ "from": <ms>, "store": n, "queue": n, "index"?: n }`, with `from` after the deploy. Never change an existing epoch. Keep each queue count a multiple of its store count.
- **Lists are eventually consistent.** A run appears in `runs.list` a moment after `start()` returns. [Indexes](https://world-cloudflare.vaishnav.one/docs/services#indexes)
- **Large values.** Binary values over 1 MiB go to R2. A serialized payload over 18 MiB or a stream chunk over 32 MiB is rejected with a 413. For more, return a `type: 'bytes'` `ReadableStream` from a step and set `autoAllocateChunkSize`, or workerd stores each 4 KiB read as a chunk ([example](https://github.com/vaishnav-mk/world-cloudflare/blob/main/workflows/limits.ts#L140-L162)). [Payloads and streams](https://world-cloudflare.vaishnav.one/docs/streams)
- **Nothing is deleted.** Runs and R2 objects are retained indefinitely.
- **Hook upgrades** follow a receiver-first procedure. [Hook ownership](https://github.com/vaishnav-mk/world-cloudflare/blob/main/docs/hook-ownership.md)

## Encryption

```sh
openssl rand -base64 32 | wrangler secret put WORKFLOW_ENCRYPTION_SECRET
```

The SDK then encrypts payloads, hook data, stream chunks and VM snapshots with AES-256-GCM before they reach the World. Names, statuses, tokens, attributes, timestamps and byte streams stay readable. There is no rotation: a changed secret makes encrypted runs unreadable. [Encryption](https://world-cloudflare.vaishnav.one/docs/security#encryption)

## Without Workers for Platforms

> [!IMPORTANT]
> **Workers for Platforms is optional.** It is a separate $25/month plan, and it only keeps runs that are waiting during a deploy on the code that started them. Without it, the main Worker runs its own flows: export `createQueueConsumer(flow)` with the flow bundle, as the example's [`src/index.ts`](https://github.com/vaishnav-mk/world-cloudflare/blob/main/src/index.ts) does, and leave out `EXECUTORS` and `EXECUTOR_ROUTES`.

What you give up: a run created under an older `DEPLOYMENT_ID` cannot resume. Its deliveries fail with `Deployment <id> is unavailable`, retry up to `QUEUE_MAX_ATTEMPTS` and then go to the `DLQ` bucket. So drain before changing `DEPLOYMENT_ID`: stop starting runs and wait for the waiting ones to finish. Keeping the same `DEPLOYMENT_ID` instead replays waiting runs against the new code, which is only safe for changes that replay the same steps in the same order; that path is not tested here.

You still need Workers Paid. The templates set `limits.cpu_ms` and `limits.subrequests`, and Workers Free allows 10 ms of CPU per request; a warm QuickJS delivery measured 12–19 ms.

## Retained executors

To keep in-flight runs on the code that started them, upload one executor per release to a Workers for Platforms dispatch namespace:

```ts
registerWorld(createWorld());
export default createExecutor(flow);
```

Executors bind the World's Durable Objects with `script_name` set to the main Worker, as in [`wrangler.executor.jsonc`](https://github.com/vaishnav-mk/world-cloudflare/blob/main/wrangler.executor.jsonc). The main Worker binds the namespace as `EXECUTORS` and routes with `EXECUTOR_ROUTES`; keep old entries until their runs drain. Deploy the main Worker first. [Releases](https://world-cloudflare.vaishnav.one/docs/security#releases)

## Exports

| Export | Use |
| --- | --- |
| `createWorld(env?)` | The World; also the default export |
| `registerWorld(world)` | Makes `world` the SDK's World |
| `WorldStore`, `QueueShard`, `Runner`, `RunStreams` | Durable Object classes to re-export |
| `createQueueConsumer(flow)` | The `QueueConsumer` entrypoint class |
| `createExecutor(flow)` | A retained executor's default export |
| `drainOutbox(env)` | The cron handler |
| `nextEvents(env, runId, cursor, types?)` | Long-polls a run's events |
| `EventType`, `RunStatus`, `StepStatus`, `WaitStatus`, `ResolveData`, `DeliveryResult` | Enums of the SDK's string values |
| `handleWorldRpc(request, world, token)` | Serves the World over HTTP for the remote client |
| `toJson(value)` | JSON with bytes as base64 and payload refs as `{ "$ref": key }` |

`world-cloudflare/build` exports `buildWorkflows({ cwd?, dirs? })` and `workerAliases`.

## Remote client

`world-cloudflare/remote` is a Node World that forwards calls to a Worker's `handleWorldRpc` endpoint, so SDK tools such as `workflow web` can read and write its runs. Set `WORKFLOW_TARGET_WORLD=world-cloudflare/remote`, `WORKFLOW_CLOUDFLARE_URL` and `WORKFLOW_CLOUDFLARE_TOKEN`.

To start runs that need the server's spec version or capabilities, use `await connectWorld({ url?, token? })`, or point `WORKFLOW_TARGET_WORLD` at a module containing:

```js
export { connectWorld as createWorld } from 'world-cloudflare/remote';
```

The token can write events and, with an encryption secret, fetch every run's key; keep it secret. Workflows still run on Cloudflare, and each payload must fit in one request.

## License

Apache-2.0.
