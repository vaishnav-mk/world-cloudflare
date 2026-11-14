# @workflow/world-cloudflare

A [Workflow SDK](https://workflow-sdk.dev/worlds/building-a-world) v5 World for Cloudflare Workers.

| Durable Object | Holds |
| --- | --- |
| `WorldStore` | Store shards ([below](#store-shards)). Each holds the runs that hash to it, with their events, steps, hooks, waits and outboxes, and the registry for the hook tokens that hash to it. The one named `global` holds runs created before store shards |
| `QueueShard` | Queue shards: queue messages, delays, retries and idempotency keys; dead letters go to R2 |
| `RunStreams` | One per run: that run's streams and its VM snapshot |

Schemas are Drizzle tables with Drizzle Kit migrations ([`drizzle/`](drizzle/README.md)), and queries use [workers-qb](https://workers-qb.massadas.com/). It creates spec 8 runs, or spec 9 (single-orchestrator) runs with `WORKFLOW_SPEC_VERSION=9`, and advertises every capability Vercel's World does: `inBandFence`, `maxConcurrency`, `hookRetention`, `hookResumeDedup`, `resolvedHookResume`, `hookForceClaim`, `atomicStartHook`, `experimental_queueHold`, `deploymentAffinity` and `dynamicWorkflowCode`. It is unpublished; see [contract parity](../../docs/contract-parity.md) for what is built and tested. The `@workflow` npm scope belongs to Vercel, so publishing needs another name.

## Requirements

- The published Workflow SDK (`workflow` 5.5.0), with workflows built by `@workflow/world-cloudflare/build` (below).
- The `nodejs_compat` flag. Workflow code runs on QuickJS (`WORKFLOW_VM=quickjs`) or natively on the SDK's node engine (`WORKFLOW_VM=node`, or unset), which the build gives a `node:vm` of its own; see [isolate engine](../../docs/isolate-engine.md).
- Wrangler `alias` entries for the SDK packages that do not load on Workers, which the SDK imports to pick a default World and to hand off background work:

  ```jsonc
  "alias": {
    "@workflow/world-local": "@workflow/world-cloudflare/stubs/world",
    "@workflow/world-vercel": "@workflow/world-cloudflare/stubs/world",
    "@vercel/functions": "@workflow/world-cloudflare/stubs/vercel-functions"
  }
  ```

  In a vitest config for the Workers pool, pass `workerAliases` from `@workflow/world-cloudflare/build` as `resolve.alias`.
- A Wrangler rule that bundles the migration SQL as text: `"rules": [{ "type": "Text", "globs": ["**/*.sql"], "fallthrough": true }]`.
- For VM snapshots, either an [encryption secret](#encryption) or `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED=1`. With neither, the SDK replays each run's whole event log on every invocation. Without a secret, snapshots hold the run's memory in plaintext, as the store holds its payloads.

## Add it to a Worker

Build your workflows before `wrangler dev` or `wrangler deploy`:

```sh
node node_modules/@workflow/world-cloudflare/src/build.ts workflows   # the directories to scan; default .
```

It writes `.well-known/workflow/v1/`: `flow.mjs`, whose `POST` is the flow handler, the steps module it loads, the QuickJS WebAssembly modules and `manifest.json`. `flow.mjs` loads the SDK on its first delivery, not at startup. Then:

```ts
import { createQueueConsumer, createWorld, drainOutbox, registerWorld, type WorldEnv } from '@workflow/world-cloudflare';
import { POST as flow } from '../.well-known/workflow/v1/flow.mjs';

export { QueueShard, RunStreams, WorldStore } from '@workflow/world-cloudflare';
export const QueueConsumer = createQueueConsumer(flow);

registerWorld(createWorld());

export default {
  scheduled: (_controller: ScheduledController, env: WorldEnv) => drainOutbox(env),
  fetch: (request: Request, env: WorldEnv) => new Response('ok'), // your routes
};
```

Bindings, as in the repo's `wrangler.jsonc`:

| Binding | Type |
| --- | --- |
| `WORLD_STORE`, `QUEUE_SHARD`, `RUN_STREAMS` | SQLite Durable Objects with those class names |
| `CONSUMER` | Service binding to the main Worker's `QueueConsumer` entrypoint; executors use it to resolve the latest deployment |
| `DLQ` | R2 bucket for dead letters |
| `PAYLOADS` | R2 bucket for payloads and stream chunks over 1 MiB |
| `DEPLOYMENT_ID` | A new ID per release |
| `WORKFLOW_ENCRYPTION_SECRET` | Optional secret that turns on [encryption](#encryption) |
| `EXECUTORS`, `EXECUTOR_ROUTES` | Optional retained executors, below |

Add a `* * * * *` cron so `drainOutbox` can recover the legacy store's dispatches whose alarm was lost, and wake the queue shards.

`WORKFLOW_TARGET_WORLD` does not work on Workers, because the SDK loads that package with `require.resolve`. Register the World instead. `registerWorld()` does what `setWorld()` from `workflow/runtime` does, without importing it: `workflow/runtime` 5.5.0 draws a random ID as it loads, which Workers forbid at startup. The SDK's `main` defers that ID, so `setWorld()` works again from its next release.

## Store shards

A run lives in one store shard from its first event, so no single object sees every run. The store named `global` is the legacy store: it holds the runs created before store shards, with their hooks.
- **Epochs.** `SHARD_EPOCHS` is a JSON list of `{ "from": <ms>, "store": <count>, "queue": <count> }`, oldest first, the first at 0. The default is one epoch of 128 store and 256 queue shards. A run's ULID time picks its epoch, and the run lives in store shard `hash(runId) % store` of it; its workflow messages (start, wakes, hook resumes and timers) go to queue shard `hash(runId) % queue`, and other messages, such as steps, to `hash(key) % queue`. To add capacity, append an epoch whose `from` is later than the deploy: new runs spread over its shards and no existing run moves. Every Worker running World code, retained executors included, needs the same value. Size shards to load: a shard that sees too few runs stays cold, and each cold one costs about 1.7 s on Workers.
- **Replays.** A run's queue shard delivers its workflow messages one at a time. A wake that asks only for a replay, delivered for the first time, is settled without a delivery once a replay that started after it was due has succeeded; a start, a timer, a legacy hook resume or a message with a retry counter is always delivered.
- **Event pages.** A page of events ends at its count limit or once its stored rows pass 4 MiB, and the SDK reads on with the cursor. A store reads at most two pages at once, 512 KiB at a time.
- **Routing.** The World client sends a run-scoped call to the run's shard. A shard throws `WorkflowRunNotFoundError` for a run it does not hold, so the client retries on the legacy store and remembers, per isolate, that the run lives there.
- **Lists.** `runs.list` and `hooks.list` without a run ID ask every shard of every epoch and the legacy store, 32 at a time, merge their pages by `createdAt` then ID, and return the last row's sort key as the cursor. `runs.getMany` makes one call per shard its IDs hash to, then one to the legacy store for the rest. `hooks.get` asks every store.
- **Hook tokens.** A token's registry row lives on shard `hash(token) % store` of the first epoch, which never changes, a copy of the hook that holds it. Before a `hook_created` write, the run's shard claims the token there and journals `hook_created` or `hook_conflict`. `hooks.getByToken` reads that shard. A start Hook (`start({ experimental_hook })`) is claimed the same way before its run is created, and a refused candidate is recorded so every copy of it gets the same answer.
- **Forced claims.** For `createHook({ experimental_force: true })` the token's shard records the takeover, asks the holder's store to journal `hook_disposed{forceClaimedBy}` and drop the hook, and only then gives the token to the claimer. A resume of the old hook is refused with `HookForceClaimedError`. The shard's alarm finishes a takeover a failed call left.
- **Releases.** A disposal or the run finishing owes the token's shard a release, committed with the write and sent before the write returns, so a freed token is free when it does. A finished run's retained token stays held until its retention lapses. A failed release stays owed and the shard's alarm retries it with backoff. A claim whose write is rejected is released the same way; one cut off by a reset of the run's shard holds its token until the run retries the write.
- **Legacy tokens.** Each token shard, the first time it answers, copies in the tokens that legacy hooks hold, so they still conflict. Legacy runs claim new tokens on the shards like any other run.
- **Recovery.** Each shard drains its own outboxes from its alarm, armed before any write that owes a queue message or a release. The cron reaches only the legacy store.

Retained executors deployed before store shards talk only to the legacy store, so don't route new runs to them.

## Spec 9

With `WORKFLOW_SPEC_VERSION=9` on the Worker and its executors, new runs follow the SDK's single-orchestrator model:
- **The in-band fence.** Each spec 9 run counts the positions its orchestrator's writes took (`runs.seq_in_band`). `events.list` returns `snapshot: { seq, seqInBand }`, and an in-band write whose count is stale is refused with `InBandSupersededError` before anything is written.
- **Appends.** Step and wait events are not checked against step or wait state, and writes return no step or wait. An inline step's first `step_started` creates it.
- **One orchestrator at a time.** A run's orchestrator messages go to its run's queue shard, which delivers one batch at a time and holds the run's second message in a batch until the batch ends. `{ timeoutSeconds: 0 }` hands the message off without counting an attempt, and `meta.deliveryCount` is 1 only on a message's first delivery.
- **Live feed.** `events.subscribe` long-polls the run's store, which answers as each event commits.

Runs created before keep spec 8, which the World still serves.

## Large payloads

Durable Object SQLite documents a 2 MB value limit and Durable Object RPC a 32 MiB message limit, so the World client keeps large values in R2. It runs in the calling Worker:
- **Writes.** Before a write reaches its Durable Object, each binary `eventData` field and stream chunk over 1 MiB goes to `PAYLOADS` at `runs/<runId>/<uuid>`, and the object stores a ref. The R2 write finishes first, so a committed ref always has its object. A serialized payload over 18 MiB is rejected with a 413, because reading it back would run the workflow out of memory.
- **Reads.** Every read swaps refs back for their bytes, as the SDK expects; `resolveData: 'none'` reads nothing from R2. Stream reads instead pipe each chunk from R2 as the reader takes it, so no chunk is held whole, and `getChunks` pages stop at 4 MiB.

Every Worker that runs World client code needs the `PAYLOADS` binding, including retained executors. [Limits](../../docs/limits.md#payloads) has the measured sizes and the open costs.

## Large data

A payload over 18 MiB is rejected, and the SDK reads each payload whole. For more, have a step return a `ReadableStream` of `type: 'bytes'` and pass it to the next step. The workflow holds only a handle, each chunk over 1 MiB goes to R2 on its own, and the reading step gets the bytes as they come out of R2. Locally 4 GiB moved this way in 64 MiB chunks:

```ts
const CHUNK = 16 * 1024 * 1024;

export async function bigData(chunks: number) {
  'use workflow';
  return countBytes(await makeBytes(chunks));
}

async function makeBytes(chunks: number) {
  'use step';
  let made = 0;
  return new ReadableStream({
    type: 'bytes',
    // Each read hands `pull` one buffer of CHUNK bytes to fill. Without it, workerd reads the
    // stream 4 KiB at a time and stores each read as its own chunk.
    autoAllocateChunkSize: CHUNK,
    pull(controller) {
      const request = controller.byobRequest!;
      if (made === chunks) {
        controller.close();
        request.respond(0);
        return;
      }
      new Uint8Array(request.view!.buffer, request.view!.byteOffset, CHUNK).fill(made++);
      request.respond(CHUNK);
    },
  });
}

async function countBytes(stream: ReadableStream<Uint8Array>) {
  'use step';
  let bytes = 0;
  for await (const piece of stream) bytes += piece.byteLength;
  return bytes;
}
```

The writing step holds the chunk it is writing, the SDK stores byte streams unencrypted, and a returned stream is written in the background after its step completes. [Limits](../../docs/limits.md#streams) has the measurements and the rest.

## Encryption

Set a secret of 32 random bytes and the SDK encrypts every payload before it reaches the World: run and step inputs, outputs and errors, hook payloads and metadata, stream chunks and VM snapshots. The Durable Objects and R2 then hold AES-256-GCM ciphertext.

```sh
openssl rand -base64 32 | wrangler secret put WORKFLOW_ENCRYPTION_SECRET
```

- **Keys.** `getEncryptionKeyForRun` derives each run's key with HKDF-SHA256 from the secret, with the run ID as info. The deployment ID is left out, so every release, and every retained executor, must have the same secret.
- **What stays readable.** Names, statuses, event types, hook tokens, attributes and timestamps, and byte streams (`type: 'bytes'`), which the SDK stores as written. The example API's `/runs/:id` and `/runs/:id/events` show ciphertext; `/result` and stream reads decrypt through the SDK.
- **Rotation.** Not supported. A changed or deleted secret leaves every encrypted run unreadable. Runs started before the secret was set keep their plaintext payloads.
- **The remote client** fetches run keys from `/world/rpc`, so `workflow web` can decrypt. Anyone holding `WORLD_RPC_TOKEN` can read every run.

Without the secret the World stores plaintext, as before.

## Retained executors

To keep in-flight runs on the code that started them, upload one executor per release to a Workers for Platforms dispatch namespace:

```ts
registerWorld(createWorld());
export default createExecutor(flow);
```

Each executor binds the three Durable Objects with `script_name` set to the main Worker. See [`wrangler.executor.jsonc`](../../wrangler.executor.jsonc). The main Worker binds the namespace as `EXECUTORS`, and `EXECUTOR_ROUTES` maps each deployment ID to its executor script. It never runs a flow itself, so it can leave the workflow bundle out, as the example's `src/dispatcher.ts` does.

## Exports

| Export | Use |
| --- | --- |
| `createWorld(env?)` | The World, including VM snapshot storage (`experimental_snapshots`) and `runs.cancelMany`; also the default export |
| `registerWorld(world)` | Makes `world` the SDK's World, as `setWorld()` does |
| `WorldStore`, `QueueShard`, `RunStreams` | Durable Object classes to re-export from the Worker |
| `createQueueConsumer(flow)` | The `QueueConsumer` entrypoint class |
| `createExecutor(flow)` | A retained executor's default export |
| `drainOutbox(env)` | Cron handler body |
| `nextEvents(env, runId, cursor, types?)` | Long-polls a run's events, for a live subscription |
| `EventType`, `RunStatus`, `StepStatus`, `WaitStatus`, `ResolveData` | Enums of the SDK's string values |
| `handleWorldRpc(request, world, token)` | Serves the World over HTTP for the remote client |
| `toJson(value)` | JSON with bytes as base64 and payload refs as `{ "$ref": key }`, for HTTP responses |

`@workflow/world-cloudflare/build` exports `buildWorkflows({ cwd?, dirs? })`, which the command above runs, and `workerAliases`, the replacements for the Wrangler `alias` and vitest `resolve.alias`.

## Remote client

`@workflow/world-cloudflare/remote` is a World for Node that forwards every call to a deployed Worker. The SDK's tools load it with `WORKFLOW_TARGET_WORLD=@workflow/world-cloudflare/remote`.

- **Configure it** with `WORKFLOW_CLOUDFLARE_URL` and `WORKFLOW_CLOUDFLARE_TOKEN`.
- **The Worker side:** mount `handleWorldRpc` at `POST /world/rpc` and keep the token in a secret.
- **Build it** with `pnpm build`. Unlike the Worker entry, this entry ships as compiled JavaScript (`dist/remote/client.js`), because Node loads it directly.

It reads and writes runs, steps, events, hooks, streams and VM snapshots, cancels runs in bulk, and fetches run keys from a Worker with an encryption secret. It cannot receive queue deliveries; workflows still run on Cloudflare. Payloads travel as devalue base64 in one request or response, so the Worker's request body limit and memory bound the largest value it can write or read.
