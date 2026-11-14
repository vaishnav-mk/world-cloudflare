# @workflow/world-cloudflare

A [Workflow SDK](https://workflow-sdk.dev/worlds/building-a-world) v5 World for Cloudflare Workers, built on SQLite Durable Objects and R2. It runs the published SDK (`workflow` 5.5.0). It creates spec 8 runs, or spec 9 (single-orchestrator) runs with `WORKFLOW_SPEC_VERSION=9`, and declares every capability Vercel's World does (`inBandFence`, `maxConcurrency`, `hookRetention`, `hookResumeDedup`, `resolvedHookResume`, `hookForceClaim`, `atomicStartHook`, `deploymentAffinity`, `dynamicWorkflowCode`) plus `invoke`. [Contract parity](../../docs/contract-parity.md) lists what is built and tested, and [limits](../../docs/limits.md) what was measured.

The package is unpublished. The `@workflow` npm scope belongs to Vercel, so publishing it needs another name.

| Durable Object | Holds |
| --- | --- |
| `WorldStore` | Store shards: the runs that hash to each, with their events, steps, hooks, waits and outboxes, and the registry for the hook tokens that hash to it. Also the index shards and the legacy store, [below](#shards-and-epochs) |
| `QueueShard` | Queue shards: messages, delays, retries and idempotency keys. Dead letters go to R2 |
| `Runner` | Where flows and steps run ([runners](#runners)). Holds nothing durable |
| `RunStreams` | One per run: that run's streams and its VM snapshot |

Tables are Drizzle definitions with Drizzle Kit migrations ([`drizzle/`](drizzle/README.md)); queries use [workers-qb](https://workers-qb.massadas.com/).

## Setup

The package ships compiled JavaScript with type declarations, in `dist/`. Until it is published, pack it with `pnpm pack` in this directory, which builds it first, and depend on the tarball: `"@workflow/world-cloudflare": "file:<path>/workflow-world-cloudflare-0.2.0.tgz"`. Also depend on `workflow` 5.5.0: your code imports `workflow/api`, and the build bundles `workflow/runtime`. With pnpm 11 or later, copy `allowBuilds` from the repo's `pnpm-workspace.yaml`, or `pnpm install` fails with `ERR_PNPM_IGNORED_BUILDS`.

Build the workflows before `wrangler dev` or `wrangler deploy`. The command takes the directories to scan, `.` by default:

```sh
node node_modules/@workflow/world-cloudflare/dist/build.js workflows
```

It writes `.well-known/workflow/v1/`: `flow.mjs`, whose `POST` is the flow handler, the steps module it loads, the QuickJS WebAssembly modules and `manifest.json`. `flow.mjs` loads the SDK on its first delivery, not at startup. The Worker then exports the Durable Objects and the queue consumer, and registers the World:

```ts
import { createQueueConsumer, createWorld, drainOutbox, registerWorld, type WorldEnv } from '@workflow/world-cloudflare';
import { POST as flow } from '../.well-known/workflow/v1/flow.mjs';

export { QueueShard, Runner, RunStreams, WorldStore } from '@workflow/world-cloudflare';
export const QueueConsumer = createQueueConsumer(flow);

registerWorld(createWorld());

export default {
  scheduled: (_controller: ScheduledController, env: WorldEnv) => drainOutbox(env),
  fetch: (request: Request, env: WorldEnv) => new Response('ok'), // your routes
};
```

`registerWorld()` does what `setWorld()` from `workflow/runtime` does, without importing it: `workflow/runtime` 5.5.0 draws a random ID as it loads, which Workers forbid at startup. `WORKFLOW_TARGET_WORLD` does not work on Workers, because the SDK loads that package with `require.resolve`. For TypeScript, declare the generated `.mjs` module as the example's [`src/generated-flow.d.ts`](../../src/generated-flow.d.ts) does.

Wrangler does not run the SDK's client transform, so `start(hello, [21])` throws `'start' received an invalid workflow function`. Pass the workflow's ID instead. It is `workflow//./<file without extension>//<export>`, and `.well-known/workflow/v1/manifest.json` lists every one. The example sets them in [`src/workflows.ts`](../../src/workflows.ts).

```ts
import { start } from 'workflow/api';

const run = await start({ workflowId: 'workflow//./workflows/hello//hello' }, [21]);
```

The Wrangler config, as in the repo's [`wrangler.jsonc`](../../wrangler.jsonc), needs:
- the `nodejs_compat` flag;
- the four Durable Object bindings and a migration that makes their classes SQLite-backed. Without it, each object fails in its constructor with `DrizzleError: Rollback`:

  ```jsonc
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["WorldStore", "QueueShard", "RunStreams", "Runner"] }]
  ```

- a `CONSUMER` service binding to the Worker itself: `"services": [{ "binding": "CONSUMER", "service": "<this Worker's name>", "entrypoint": "QueueConsumer" }]`;
- the `DLQ` and `PAYLOADS` R2 buckets and a `DEPLOYMENT_ID` ([configuration](#configuration));
- `alias` entries for the SDK packages that do not load on Workers, which the SDK imports to pick a default World and to hand off background work. In a vitest config for the Workers pool, pass `workerAliases` from `@workflow/world-cloudflare/build` as `resolve.alias` instead:

  ```jsonc
  "alias": {
    "@workflow/world-local": "@workflow/world-cloudflare/stubs/world",
    "@workflow/world-vercel": "@workflow/world-cloudflare/stubs/world",
    "@vercel/functions": "@workflow/world-cloudflare/stubs/vercel-functions"
  }
  ```

- a `* * * * *` cron for `drainOutbox`, which recovers the legacy store's dispatches and wakes the queue shards;
- `limits.cpu_ms` of 300,000 and a high `limits.subrequests`, because the SDK makes about one Durable Object call per event.

Wrangler's default `Text` rule bundles the migration SQL (`**/*.sql`), from `node_modules` too. A config with a `Text` rule of its own must list that glob or set `"fallthrough": true` on its rule.

## Configuration

| Binding | Type |
| --- | --- |
| `WORLD_STORE`, `QUEUE_SHARD`, `RUN_STREAMS`, `RUNNER` | SQLite Durable Objects of those classes (`Runner` for `RUNNER`) |
| `CONSUMER` | Service binding to the main Worker's `QueueConsumer` entrypoint, which runners call to run deliveries |
| `DLQ` | R2 bucket for dead letters |
| `PAYLOADS` | R2 bucket for payloads and stream chunks over 1 MiB |
| `EXECUTORS` | Optional dispatch namespace of [retained executors](#retained-executors) |

| Variable | Meaning |
| --- | --- |
| `DEPLOYMENT_ID` | Required. A new ID per release |
| `SHARD_EPOCHS` | Shard counts per epoch ([below](#shards-and-epochs)). Default: one epoch of 128 store, 256 queue and 16 index shards |
| `WORKFLOW_VM` | The SDK's engine for new runs: `quickjs`, or `node` (the default), which runs natively on the build's `node:vm` ([native engine](../../docs/native-engine.md)). The templates set `quickjs` |
| `WORKFLOW_ENCRYPTION_SECRET` | Optional secret that turns on [encryption](#encryption) |
| `WORKFLOW_SPEC_VERSION` | `9` for spec 9 runs ([below](#spec-9)); spec 8 otherwise |
| `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED` | `1` lets the SDK save QuickJS VM snapshots without a secret. With neither, it replays each run's whole log on every invocation |
| `WORKFLOW_COMPRESSION_CODEC` | `gzip`: zstd in workerd throws on incompressible input |
| `QUEUE_MAX_ATTEMPTS` | Deliveries before a message is dead-lettered, 64 by default |
| `FLOW_BACKGROUND_BUDGET_MS` | How long after its delivery starts a flow request waits for its background work, 13.5 minutes by default |
| `EXECUTOR_ROUTES` | JSON map from deployment ID to retained executor script name |
| `LOCATION_HINT` | Optional [location hint](https://developers.cloudflare.com/durable-objects/reference/data-location/#provide-a-location-hint), such as `weur`, for every Durable Object the World creates ([below](#object-location)) |

Every Worker that runs World code, retained executors included, needs the same bindings, `SHARD_EPOCHS`, `LOCATION_HINT` and `WORKFLOW_ENCRYPTION_SECRET`.

### Object location

A Durable Object is created near its first caller, so objects first reached from different places, such as load tests from two regions, can end up far apart, and every call between them pays the distance: a run whose store shard and runner were apart took about 170 ms per event write on Workers, where 8 to 27 ms is usual. `LOCATION_HINT` keeps them in one region: the World passes it on every `getByName` of its stores, index and token shards, queue shards, step runners and `RunStreams`. A region spans several data centers, and the hint puts each object in any of them, so a run's own runner is created by the run's store shard without the hint, next to it ([runners](#runners)). It only places objects created after it is set; an object keeps the place it was created in, so set it before the first run, or add a [shard epoch](#shards-and-epochs) so new runs use new objects.

## Shards and epochs

No single object sees every run. `WorldStore` objects play four roles, by name:
- **Store shards** (`store-<n>`) hold the runs that hash to them, from a run's first event. A write commits its event, state changes and outbox rows in one transaction.
- **Token shards** are store shards too: a hook token's registry row lives on shard `hash(token) % store` of the first epoch, which never changes. A run's store claims the token there before it journals `hook_created` or `hook_conflict`, and releases it when the hook is disposed of or the run finishes. A forced claim (`experimental_force`) moves a token only after the holder's store has disposed of its hook.
- **Index shards** (`index-<n>`) hold list rows ([listing](#listing)).
- **The legacy store** (`global`) holds the runs created before store shards, with their hooks. The World client tries a run's shard first and falls back to it.

`SHARD_EPOCHS` is a JSON list of `{ "from": <ms>, "store": <count>, "queue": <count>, "index"?: <count> }`, oldest first, the first at 0. A run's ULID time picks its epoch, and the run lives in store shard `hash(runId) % store` of it. Its workflow messages (start, wakes, hook resumes and timers) go to queue shard `hash(runId) % queue`, which delivers them one at a time; other messages, such as steps, go to `hash(key) % queue`, in a lane of their own that never waits for a replay. To add capacity, append an epoch whose `from` is later than the deploy: new runs spread over its shards and no run moves. Never change an existing epoch's counts, `index` included: its rows would hash elsewhere. Size shards to the load, because a cold Durable Object is slow on its first call ([run creation](../../docs/limits.md#run-creation)).

Each store drains its own outboxes from its alarm, which it arms before any write that owes a queue message or a token release. A failed push, list rows included, stays owed and the alarm retries it with backoff. The cron reaches only the legacy store, and wakes queue shards so each redelivers what a reset cut off.

## Runners

A queue shard runs no workflow code: it hands each due message to a `Runner`, which calls the `CONSUMER` entrypoint, where the flow handler or a retained executor runs it. Flows started by a queue shard itself competed with it for CPU ([run creation](../../docs/limits.md#run-creation)).
- **One runner per run.** An epoch has as many runners as queue shards, and a run's workflow messages go to runner `hash(runId) % queue`, so a run has one runner and a busy one stays warm. Other messages, such as steps, spread over the epoch's step runners by message ID.
- **Next to the run's store.** The run's workflow code runs where its runner is and calls the run's store shard a few times per step, so the store creates the runner, before it publishes the run's start: a Durable Object is created near its first caller. With `LOCATION_HINT=enam` on Workers, runs whose runner and store were in different data centers took 105 to 140 ms per step, and runs with both in one about 70 ms. Keep each epoch's queue count a multiple of its store count, so that each runner's runs all live on one store shard.
- **One replay at a time.** A runner runs a run's workflow deliveries one after another, even when a reset queue shard delivers again a message it still runs. Two replays of a run in one isolate would let the SDK's in-process step single-flight make one request wait on a promise another settles.
- **`invoke`.** Because a run has one runner, the World declares `capabilities.invoke`. A step's delivery then wakes its run instead of replaying it, and `resumeHook()` hands its input to the run's runner, which writes `hook_received` beside the delivery in progress and wakes the run.
- **Nothing durable.** A delivery a runner was running when it reset fails on its queue shard, which delivers it again.

## Listing

A run's list row, the run without its payloads, lives on index shard `hash(runId) % index` of its epoch, and each hook's on `hash(hookId) % index`. A store pushes the latest row of every run and hook change right after the write, from an outbox. A write that changes a hook, or finishes a run, answers once the push lands or after 1 s, because the SDK reads a hook right after creating it. Other run writes, including `run_created`, do not wait, so a run appears in lists a moment after `start()` returns.

`runs.list` and `hooks.list` without a run ID ask every index shard, 32 at a time, and merge their pages by `createdAt`, then ID; filters apply on the index shards. With `resolveData: 'all'`, the page's runs then come whole from their stores. `hooks.get` asks one index shard per epoch, and `runs.getMany` one store per shard its IDs hash to.

Runs written before the index reach it through a one-time backfill per store, 200 runs per alarm. An index shard answers only once every store that feeds it has reported its backfill; until then lists and `hooks.get` ask every store, and the shard asks the stores it is missing to report, so it answers alone soon after an upgrade or a new epoch.

## Payloads and streams

Durable Object SQLite documents a 2 MB value limit and RPC a 32 MiB message limit, so the World client, in the calling Worker, keeps large values in R2:
- **Writes.** Each binary `eventData` field and stream chunk over 1 MiB goes to `PAYLOADS` at `runs/<runId>/<uuid>` before the write, and the object stores a ref. A serialized payload over 18 MiB, or a stream chunk over 16 MiB, is rejected with a 413 marked `fatal`, so the step fails instead of running the workflow out of memory.
- **Reads.** Every read swaps refs back for their bytes; `resolveData: 'none'` reads nothing from R2. Stream reads pipe each chunk from R2 as the reader takes it, in 1 MiB pieces, and `getChunks` pages stop at 4 MiB.
- **Not cleaned up.** R2 objects are never deleted, including ones left by writes that failed.

For more than one payload holds, have a step return a `ReadableStream` of `type: 'bytes'` and pass it to the next step. The workflow holds only a handle, and each chunk goes to R2 on its own:

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
    // Without it, workerd reads the stream 4 KiB at a time and stores each read as a chunk.
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

A returned stream is written in the background after its step completes. The flow request waits for that before it answers, up to `FLOW_BACKGROUND_BUDGET_MS`, and a stream whose writer stopped ends its readers with an error after a 30 s lease. The SDK stores byte streams unencrypted.

## Encryption

Set a secret of 32 random bytes and the SDK encrypts every payload before it reaches the World: run and step inputs, outputs and errors, hook payloads and metadata, stream chunks and VM snapshots. The Durable Objects and R2 then hold AES-256-GCM ciphertext.

```sh
openssl rand -base64 32 | wrangler secret put WORKFLOW_ENCRYPTION_SECRET
```

- **Keys.** Each run's key is HKDF-SHA256 of the secret, with the run ID as info. The deployment ID is left out, so every release and every retained executor must have the same secret.
- **What stays readable.** Names, statuses, event types, hook tokens, attributes, timestamps, and byte streams, which the SDK stores as written.
- **No rotation.** A changed or deleted secret leaves every encrypted run unreadable. Runs started before the secret was set keep their plaintext payloads.

Without the secret the World stores plaintext, VM snapshots included when `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED=1`.

## Retained executors

To keep in-flight runs on the code that started them, upload one executor per release to a Workers for Platforms dispatch namespace:

```ts
registerWorld(createWorld());
export default createExecutor(flow);
```

Each executor binds the World's Durable Objects with `script_name` set to the main Worker, as in [`wrangler.executor.jsonc`](../../wrangler.executor.jsonc). The main Worker binds the namespace as `EXECUTORS`, and `EXECUTOR_ROUTES` maps each deployment ID to its executor script; keep old entries until their runs drain. Each delivery then runs on its run's deployment. The main Worker runs no flows itself, so it can leave the workflow bundle out, as the example's `src/dispatcher.ts` does. Deploy the main Worker before the executors: they call it through `CONSUMER`.

## Spec 9

With `WORKFLOW_SPEC_VERSION=9` on the Worker and its executors, new runs follow the SDK's single-orchestrator model. Each run counts the positions its orchestrator's writes took, `events.list` returns them as `snapshot: { seq, seqInBand }`, and a stale in-band write is refused with `InBandSupersededError` before anything is written. Step and wait events are appends, and `{ timeoutSeconds: 0 }` hands a message off without counting an attempt. Runs created before keep spec 8.

## Exports

| Export | Use |
| --- | --- |
| `createWorld(env?)` | The World; also the default export |
| `registerWorld(world)` | Makes `world` the SDK's World, as `setWorld()` does |
| `WorldStore`, `QueueShard`, `Runner`, `RunStreams` | Durable Object classes to re-export from the Worker |
| `createQueueConsumer(flow)` | The `QueueConsumer` entrypoint class |
| `createExecutor(flow)` | A retained executor's default export |
| `drainOutbox(env)` | The cron handler |
| `nextEvents(env, runId, cursor, types?)` | Long-polls a run's events, for a live subscription |
| `EventType`, `RunStatus`, `StepStatus`, `WaitStatus`, `ResolveData`, `DeliveryResult` | Enums of the SDK's string values |
| `handleWorldRpc(request, world, token)` | Serves the World over HTTP for the remote client |
| `toJson(value)` | JSON with bytes as base64 and payload refs as `{ "$ref": key }` |

`@workflow/world-cloudflare/build` exports `buildWorkflows({ cwd?, dirs? })`, which the build command runs, and `workerAliases`.

## Remote client

`@workflow/world-cloudflare/remote` is a World for Node that forwards every call to a deployed Worker's `handleWorldRpc` endpoint, so the SDK's tools, such as the web UI (`workflow web`), can read and write its runs. Load it with `WORKFLOW_TARGET_WORLD=@workflow/world-cloudflare/remote`, and set `WORKFLOW_CLOUDFLARE_URL` and `WORKFLOW_CLOUDFLARE_TOKEN`. In this repo, build it first with `pnpm build` in this package.

The token can write events and, with an encryption secret, fetch every run's key, so keep it in a secret. The client cannot receive queue deliveries; workflows still run on Cloudflare. Payloads travel in one request or response, so the Worker's request body limit and memory bound the largest value it can move.
