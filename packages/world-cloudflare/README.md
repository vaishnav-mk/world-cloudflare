# @workflow/world-cloudflare

A [Workflow SDK](https://workflow-sdk.dev/worlds/building-a-world) v5 World for Cloudflare Workers.

| Durable Object | Holds |
| --- | --- |
| `WorldStore` | One per run, named by its run ID: the run's events, steps, hooks and waits, plus its dispatch outbox. The one named `global` is the directory ([below](#run-stores-and-the-directory)) |
| `QueueShard` | Queue messages, delays, retries and idempotency keys; dead letters go to R2 |
| `RunStreams` | One per run: that run's streams and its VM snapshot |

Schemas are Drizzle tables with Drizzle Kit migrations ([`drizzle/`](drizzle/README.md)), and queries use [workers-qb](https://workers-qb.massadas.com/). It is unpublished; see [contract parity](../../docs/contract-parity.md) for what is built and tested. The `@workflow` npm scope belongs to Vercel, so publishing needs another name.

## Requirements

- The Workflow SDK built with this repo's [patch](../../patches/workflow-sdk-cloudflare-workers.patch), which adds `workflow build --target cloudflare-workers`.
- `WORKFLOW_VM=quickjs` (or `isolate`, see [isolate engine](../../docs/isolate-engine.md)) and the `nodejs_compat` flag, because workerd has no working `node:vm`.
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

Add a `* * * * *` cron so `drainOutbox` can recover the directory's dispatches whose alarm was lost, and wake the queue shard.

`WORKFLOW_TARGET_WORLD` does not work on Workers, because the SDK loads that package with `require.resolve`. Call `setWorld(createWorld())` instead.

## Run stores and the directory

Each run lives in its own `WorldStore`, named by its run ID, so runs no longer queue behind each other's writes. The one named `global` is the directory: an index copy of every run for `runs.list` and `runs.getMany`, the hook-token registry, and every run created before per-run stores.
- **Routing.** The World client sends a run-scoped call to the run's store. A store throws `WorkflowRunNotFoundError` for a run it does not hold, so the client retries on the directory and remembers, per isolate, that the run lives there.
- **Run index.** After a write that changes the run, its store pushes the run row to the directory just after the commit, in the background unless the write frees hook tokens (below). Until then `runs.list` and `runs.getMany` can show an older status. A failed push stays owed and the store's alarm retries it.
- **Hook tokens.** Before a `hook_created` write, the run's store claims the token from the directory. A disposal or the run finishing waits for its push, so a freed token is free once that write returns. A claim whose write never commits holds its token until the run finishes.
- **Recovery.** Each run store drains its own outbox from its alarm, armed before any write that owes a queue message or a push. The cron reaches only the directory.

Retained executors deployed before per-run stores talk only to the directory, so don't route new runs to them.

## Large payloads

Durable Object SQLite documents a 2 MB value limit and Durable Object RPC a 32 MiB message limit, so the World client keeps large values in R2. It runs in the calling Worker:
- **Writes.** Before a write reaches its Durable Object, each binary `eventData` field and stream chunk over 1 MiB goes to `PAYLOADS` at `runs/<runId>/<uuid>`, and the object stores a ref. The R2 write finishes first, so a committed ref always has its object. A serialized payload over 18 MiB is rejected with a 413, because reading it back would run the workflow out of memory.
- **Reads.** Every read swaps refs back for their bytes, as the SDK expects; `resolveData: 'none'` reads nothing from R2.

Every Worker that runs World client code needs the `PAYLOADS` binding, including retained executors. [Limits](../../docs/limits.md#payloads) has the measured sizes and the open costs.

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
