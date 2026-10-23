# @workflow/world-cloudflare

A [Workflow SDK](https://workflow-sdk.dev/worlds/building-a-world) v5 World for Cloudflare Workers.

| Durable Object | Holds |
| --- | --- |
| `WorldStore` | Every run's events, runs, steps, hooks and waits, plus the dispatch outbox |
| `QueueShard` | Queue messages, delays, retries and idempotency keys; dead letters go to R2 |
| `RunStreams` | One per run: that run's streams and its VM snapshot |

Schemas are Drizzle tables with Drizzle Kit migrations ([`drizzle/`](drizzle/README.md)), and queries use [workers-qb](https://workers-qb.massadas.com/). It is v1 and unpublished; see [contract parity](../../docs/contract-parity.md) for what is built and tested. The `@workflow` npm scope belongs to Vercel, so publishing needs another name.

## Requirements

- The Workflow SDK built with this repo's [patch](../../patches/workflow-sdk-cloudflare-workers.patch), which adds `workflow build --target cloudflare-workers`.
- `WORKFLOW_VM=quickjs` and the `nodejs_compat` flag, because workerd has no working `node:vm`.
- A Wrangler rule that bundles the migration SQL as text: `"rules": [{ "type": "Text", "globs": ["**/*.sql"], "fallthrough": true }]`.
- `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED=1` to turn on VM snapshots. The World has no encryption keys, so without it the SDK replays each run's whole event log on every invocation. Snapshots hold the run's memory in plaintext, as the store already holds its payloads.

## Add it to a Worker

Build your workflows with `workflow build --target cloudflare-workers`, then:

```ts
import { setWorld } from 'workflow/runtime';
import { createQueueConsumer, createWorld, drainOutbox, type WorldEnv } from '@workflow/world-cloudflare';
import { POST as flow } from '../.well-known/workflow/v1/flow.mjs';

export { QueueShard, RunStreams, WorldStore } from '@workflow/world-cloudflare';
export const QueueConsumer = createQueueConsumer(flow);

setWorld(createWorld());

export default {
  scheduled: (_controller: ScheduledController, env: WorldEnv) => drainOutbox(env),
  fetch: (request: Request, env: WorldEnv) => new Response('ok'), // your routes
};
```

Bindings, as in the repo's `wrangler.jsonc`:

| Binding | Type |
| --- | --- |
| `WORLD_STORE`, `QUEUE_SHARD`, `RUN_STREAMS` | SQLite Durable Objects with those class names |
| `CONSUMER` | Service binding to this Worker's `QueueConsumer` entrypoint |
| `DLQ` | R2 bucket for dead letters |
| `PAYLOADS` | R2 bucket for payloads and stream chunks over 1 MiB |
| `DEPLOYMENT_ID` | A new ID per release |
| `EXECUTORS`, `EXECUTOR_ROUTES` | Optional retained executors, below |

Add a `* * * * *` cron so `drainOutbox` can recover dispatches whose alarm was lost.

`WORKFLOW_TARGET_WORLD` does not work on Workers, because the SDK loads that package with `require.resolve`. Call `setWorld(createWorld())` instead.

## Large payloads

Durable Object SQLite documents a 2 MB value limit and Durable Object RPC a 32 MiB message limit, so the World keeps large values in R2.

- **Writes.** The World client runs in the calling Worker. Before `events.create` or a stream write reaches its Durable Object, it puts each stream chunk and each binary `eventData` field (the event's payload, and dynamic workflow code) over 1 MiB into `PAYLOADS` at `runs/<runId>/<uuid>`. The object stores a ref instead: a 0xff byte, `r2:` and the key. The R2 write finishes before the event commits, so a committed ref always has its object.
- **Reads.** Every `runs`, `steps`, `events`, `hooks` and `streams` read, and the result of `events.create`, swaps refs back for their bytes from R2 in the same Worker. `resolveData: 'none'` drops payloads before they leave the object, so it reads nothing from R2.
- **Opaque bytes.** Payloads stay the SDK's serialized, possibly gzipped, bytes. Small bytes that happen to start like a ref go to R2 too, so a stored ref is never user data.

The SDK expects the World to resolve refs. Core asks for payloads with `resolveData` and uses the bytes it gets back as they are (`packages/core/src/runtime/helpers.ts`, where replay calls `events.list`). The Vercel World keeps large payloads behind refs in its backend and asks it to resolve them (`remoteRefBehavior: 'resolve'` in `packages/world-vercel/src/events.ts` and `runs.ts`), and `resolveData: 'none'` drops them (`stripEventDataRefs` in `packages/world/src/events.ts`). Refs never reach the SDK from either World.

Every Worker that runs World client code needs the `PAYLOADS` binding, including retained executors. An executor deployed before this change reads a ref as if it were the payload and fails the run, so don't send hook payloads over 1 MiB to runs pinned to such an executor. [Limits](../../docs/limits.md#payload-sizes) has the measured sizes and the open costs: memory, and objects left behind by failed writes.

## Retained executors

To keep in-flight runs on the code that started them, upload one executor per release to a Workers for Platforms dispatch namespace:

```ts
setWorld(createWorld());
export default createExecutor(flow);
```

Each executor binds the three Durable Objects with `script_name` set to the main Worker. See [`wrangler.executor.jsonc`](../../wrangler.executor.jsonc). The main Worker binds the namespace as `EXECUTORS`, and `EXECUTOR_ROUTES` maps each deployment ID to its executor script.

## Exports

| Export | Use |
| --- | --- |
| `createWorld(env?)` | The World, including VM snapshot storage (`experimental_snapshots`); also the default export |
| `WorldStore`, `QueueShard`, `RunStreams` | Durable Object classes to re-export from the Worker |
| `createQueueConsumer(flow)` | The `QueueConsumer` entrypoint class |
| `createExecutor(flow)` | A retained executor's default export |
| `drainOutbox(env)` | Cron handler body |
| `nextEvents(env, runId, cursor, types?)` | Long-polls a run's events, for a live subscription |
| `EventType`, `RunStatus`, `StepStatus`, `WaitStatus`, `ResolveData` | Enums of the SDK's string values |
| `handleWorldRpc(request, world, token)` | Serves the World over HTTP for the remote client |
| `toJson(value)` | JSON with bytes as base64 and payload refs as `{ "$ref": key }`, for HTTP responses |

## Remote client

`@workflow/world-cloudflare/remote` is a World for Node that forwards every call to a deployed Worker. The SDK's tools load it with `WORKFLOW_TARGET_WORLD=@workflow/world-cloudflare/remote`.

- **Configure it** with `WORKFLOW_CLOUDFLARE_URL` and `WORKFLOW_CLOUDFLARE_TOKEN`.
- **The Worker side:** mount `handleWorldRpc` at `POST /world/rpc` and keep the token in a secret.
- **Build it** with `pnpm build`. Unlike the Worker entry, this entry ships as compiled JavaScript (`dist/remote/client.js`), because Node loads it directly.

It reads and writes runs, steps, events, hooks, streams and VM snapshots. It cannot receive queue deliveries; workflows still run on Cloudflare. Payloads travel as devalue base64 in one request or response, so the Worker's request body limit and memory bound the largest value it can write or read.
