# @workflow/world-cloudflare

A [Workflow SDK](https://workflow-sdk.dev/worlds/building-a-world) v5 World for Cloudflare Workers. Run state, events, hooks and the dispatch outbox are stored in a SQLite Durable Object. Queue delivery goes through a `durability` fanout Durable Object with an R2 dead-letter bucket. Streams get one Durable Object per run.

This is v1: it runs end to end on Cloudflare, but it is not a production-complete World. See the [contract-parity ledger](../../docs/contract-parity.md) and [current limits](../../README.md#current-limits) before relying on it. The package is unpublished. Note that the `@workflow` npm scope belongs to Vercel, so publishing will need a different name.

## Requirements

- **Patched Workflow SDK.** The SDK must be built from the repo's [patch](../../patches/workflow-sdk-cloudflare-workers.patch), which adds `workflow build --target cloudflare-workers`. `scripts/setup-sdk.sh` applies and links it.
- **QuickJS VM and Node compatibility.** Set `WORKFLOW_VM=quickjs` and enable the `nodejs_compat` flag. `node:vm` does not work in workerd.
- **CPU time.** The Free plan's 10 ms CPU limit is probably too low for QuickJS.

## Add it to a Worker

1. Build your workflows with `workflow build --target cloudflare-workers`. This writes `.well-known/workflow/v1/flow.mjs`.
2. Register the World once, at module scope. Export its Durable Objects and queue consumer, and drain the outbox from a cron:

```ts
import { setWorld } from 'workflow/runtime';
import { createQueueConsumer, createWorld, drainOutbox, type WorldEnv } from '@workflow/world-cloudflare';
import { POST as flow } from '../.well-known/workflow/v1/flow.mjs';

export { QueueShard, RunStreams, WorldStore } from '@workflow/world-cloudflare';
export const QueueConsumer = createQueueConsumer(flow);

setWorld(createWorld());

export default {
  scheduled: (_controller: ScheduledController, env: WorldEnv) => drainOutbox(env),
  fetch: (request: Request, env: WorldEnv) => {
    // Your routes: start() / getRun() / resumeHook() from 'workflow/api'.
    return new Response('ok');
  },
};
```

3. Configure bindings. `CONSUMER` is a service binding from the Worker to its own `QueueConsumer` entrypoint.

```jsonc
{
  "main": "src/index.ts",
  "compatibility_flags": ["nodejs_compat"],
  "vars": { "DEPLOYMENT_ID": "my-app-v1", "WORKFLOW_VM": "quickjs" },
  "durable_objects": { "bindings": [
    { "name": "WORLD_STORE", "class_name": "WorldStore" },
    { "name": "QUEUE_SHARD", "class_name": "QueueShard" },
    { "name": "RUN_STREAMS", "class_name": "RunStreams" }
  ] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["WorldStore", "QueueShard", "RunStreams"] }],
  "r2_buckets": [{ "binding": "DLQ", "bucket_name": "my-app-workflow-dlq" }],
  "services": [{ "binding": "CONSUMER", "service": "my-app", "entrypoint": "QueueConsumer" }],
  "triggers": { "crons": ["* * * * *"] }
}
```

Without an `EXECUTORS` binding, every message runs on the current deployment's flow route. Without retained executors, give each release a new `DEPLOYMENT_ID` only after its old runs have drained.

## Retained executors (optional)

A release can keep its in-flight runs on the exact code that started them:

1. Upload an executor for that deployment to a Workers for Platforms dispatch namespace. Its entry is:

   ```ts
   setWorld(createWorld());
   export default createExecutor(flow);
   ```

   It binds the same three Durable Object classes with `script_name` pointing at the main Worker.
2. Bind the namespace as `EXECUTORS`.
3. Map each deployment ID to its executor script in the `EXECUTOR_ROUTES` JSON variable.

The repo's [`wrangler.executor.jsonc`](../../wrangler.executor.jsonc) is a template for that executor.

## Worlds manifest entry (draft)

```json
{
  "id": "cloudflare-durable-objects",
  "type": "community",
  "package": "@workflow/world-cloudflare",
  "name": "Cloudflare",
  "description": "Cloudflare Workers World on SQLite Durable Objects with a transactional dispatch outbox.",
  "repository": "<repository URL>",
  "docs": "<repository URL>#readme",
  "requiresDeployment": true
}
```

`WORKFLOW_TARGET_WORLD` is not used on Workers: the SDK loads that package with `require.resolve` and a dynamic import, which a bundled Worker cannot do. Call `setWorld(createWorld())` instead.
