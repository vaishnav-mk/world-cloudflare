# @workflow/world-cloudflare

A [Workflow SDK](https://workflow-sdk.dev/worlds/building-a-world) v5 World for Cloudflare Workers.

| Durable Object | Holds |
| --- | --- |
| `WorldStore` | Store shards ([below](#store-shards)). Each holds the runs that hash to it, with their events, steps, hooks, waits and outboxes, and the registry for the hook tokens that hash to it. The one named `global` holds runs created before store shards |
| `QueueShard` | Queue shards: queue messages, delays, retries and idempotency keys; dead letters go to R2 |
| `RunStreams` | One per run: that run's streams and its VM snapshot |

Schemas are Drizzle tables with Drizzle Kit migrations ([`drizzle/`](drizzle/README.md)), and queries use [workers-qb](https://workers-qb.massadas.com/). It advertises the `hookRetention` and `hookResumeDedup` capabilities. It is unpublished; see [contract parity](../../docs/contract-parity.md) for what is built and tested. The `@workflow` npm scope belongs to Vercel, so publishing needs another name.

## Requirements

- The Workflow SDK built with this repo's [patch](../../patches/workflow-sdk-cloudflare-workers.patch), which adds `workflow build --target cloudflare-workers`.
- `WORKFLOW_VM=quickjs` (or `isolate`, see [isolate engine](../../docs/isolate-engine.md)) and the `nodejs_compat` flag, because workerd has no working `node:vm`.
- A Wrangler rule that bundles the migration SQL as text: `"rules": [{ "type": "Text", "globs": ["**/*.sql"], "fallthrough": true }]`.
- For VM snapshots, either an [encryption secret](#encryption) or `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED=1`. With neither, the SDK replays each run's whole event log on every invocation. Without a secret, snapshots hold the run's memory in plaintext, as the store holds its payloads.

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
| `WORKFLOW_ENCRYPTION_SECRET` | Optional secret that turns on [encryption](#encryption) |
| `EXECUTORS`, `EXECUTOR_ROUTES` | Optional retained executors, below |

Add a `* * * * *` cron so `drainOutbox` can recover the legacy store's dispatches whose alarm was lost, and wake the queue shards.

`WORKFLOW_TARGET_WORLD` does not work on Workers, because the SDK loads that package with `require.resolve`. Call `setWorld(createWorld())` instead.

## Store shards

A run lives in one store shard from its first event, so no single object sees every run. The store named `global` is the legacy store: it holds the runs created before store shards, with their hooks.
- **Epochs.** `SHARD_EPOCHS` is a JSON list of `{ "from": <ms>, "store": <count>, "queue": <count> }`, oldest first, the first at 0. The default is one epoch of 128 store and 256 queue shards. A run's ULID time picks its epoch, and the run lives in store shard `hash(runId) % store` of it; its messages go to queue shard `hash(key) % queue`. To add capacity, append an epoch whose `from` is later than the deploy: new runs spread over its shards and no existing run moves. Every Worker running World code, retained executors included, needs the same value. Size shards to load: a shard that sees too few runs stays cold, and each cold one costs about 1.7 s on Workers.
- **Routing.** The World client sends a run-scoped call to the run's shard. A shard throws `WorkflowRunNotFoundError` for a run it does not hold, so the client retries on the legacy store and remembers, per isolate, that the run lives there.
- **Lists.** `runs.list` and `hooks.list` without a run ID ask every shard of every epoch and the legacy store, 32 at a time, merge their pages by `createdAt` then ID, and return the last row's sort key as the cursor. `runs.getMany` makes one call per shard its IDs hash to, then one to the legacy store for the rest. `hooks.get` asks every store.
- **Hook tokens.** A token's registry row lives on shard `hash(token) % store` of the first epoch, which never changes, a copy of the hook that holds it. Before a `hook_created` write, the run's shard claims the token there and journals `hook_created` or `hook_conflict`. `hooks.getByToken` reads that shard.
- **Releases.** A disposal or the run finishing owes the token's shard a release, committed with the write and sent just after it in the background, so a freed token is free shortly after the write returns. A finished run's retained token stays held until its retention lapses. A failed release stays owed and the shard's alarm retries it with backoff. A claim whose write is rejected is released the same way; one cut off by a reset of the run's shard holds its token until the run retries the write.
- **Legacy tokens.** Each token shard, the first time it answers, copies in the tokens that legacy hooks hold, so they still conflict. Legacy runs claim new tokens on the shards like any other run.
- **Recovery.** Each shard drains its own outboxes from its alarm, armed before any write that owes a queue message or a release. The cron reaches only the legacy store.

Retained executors deployed before store shards talk only to the legacy store, so don't route new runs to them.

## Large payloads

Durable Object SQLite documents a 2 MB value limit and Durable Object RPC a 32 MiB message limit, so the World client keeps large values in R2. It runs in the calling Worker:
- **Writes.** Before a write reaches its Durable Object, each binary `eventData` field and stream chunk over 1 MiB goes to `PAYLOADS` at `runs/<runId>/<uuid>`, and the object stores a ref. The R2 write finishes first, so a committed ref always has its object. A serialized payload over 18 MiB is rejected with a 413, because reading it back would run the workflow out of memory.
- **Reads.** Every read swaps refs back for their bytes, as the SDK expects; `resolveData: 'none'` reads nothing from R2.

Every Worker that runs World client code needs the `PAYLOADS` binding, including retained executors. [Limits](../../docs/limits.md#payloads) has the measured sizes and the open costs.

## Encryption

Set a secret of 32 random bytes and the SDK encrypts every payload before it reaches the World: run and step inputs, outputs and errors, hook payloads and metadata, stream chunks and VM snapshots. The Durable Objects and R2 then hold AES-256-GCM ciphertext.

```sh
openssl rand -base64 32 | wrangler secret put WORKFLOW_ENCRYPTION_SECRET
```

- **Keys.** `getEncryptionKeyForRun` derives each run's key with HKDF-SHA256 from the secret, with the run ID as info. The deployment ID is left out, so every release, and every retained executor, must have the same secret.
- **What stays readable.** Names, statuses, event types, hook tokens, attributes and timestamps. The example API's `/runs/:id` and `/runs/:id/events` show ciphertext; `/result` and stream reads decrypt through the SDK.
- **Rotation.** Not supported. A changed or deleted secret leaves every encrypted run unreadable. Runs started before the secret was set keep their plaintext payloads.
- **The remote client** fetches run keys from `/world/rpc`, so `workflow web` can decrypt. Anyone holding `WORLD_RPC_TOKEN` can read every run.

Without the secret the World stores plaintext, as before.

## Retained executors

To keep in-flight runs on the code that started them, upload one executor per release to a Workers for Platforms dispatch namespace:

```ts
setWorld(createWorld());
export default createExecutor(flow);
```

Each executor binds the three Durable Objects with `script_name` set to the main Worker. See [`wrangler.executor.jsonc`](../../wrangler.executor.jsonc). The main Worker binds the namespace as `EXECUTORS`, and `EXECUTOR_ROUTES` maps each deployment ID to its executor script. It never runs a flow itself, so it can leave the workflow bundle out, as the example's `src/dispatcher.ts` does.

## Exports

| Export | Use |
| --- | --- |
| `createWorld(env?)` | The World, including VM snapshot storage (`experimental_snapshots`) and `runs.cancelMany`; also the default export |
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

It reads and writes runs, steps, events, hooks, streams and VM snapshots, cancels runs in bulk, and fetches run keys from a Worker with an encryption secret. It cannot receive queue deliveries; workflows still run on Cloudflare. Payloads travel as devalue base64 in one request or response, so the Worker's request body limit and memory bound the largest value it can write or read.
