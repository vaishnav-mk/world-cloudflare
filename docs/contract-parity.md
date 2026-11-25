# Workflow SDK v5 contract parity

Every method, option, capability and spec-version rule of the contract, the published `@workflow/world` 5.4.0 run by `workflow` 5.8.0, that Vercel's World implements or the runtime uses. The target is the behaviour the SDK sees: where a row names something of Vercel's platform (OIDC, billing, regions), it says what this World does instead.

- **Before** is `3f24570`, the World before this contract work; **After** is the current code.
- **Built**: implemented as the contract describes. **Partly**: implemented with a stated gap. **Missing**: not implemented. **N/A**: the contract says a World like this one omits it.
- **Tests** names where it is checked. `pnpm test` runs the workerd tests; `pnpm run test:e2e` runs real SDK runs in workerd on four lanes: QuickJS at spec 8, the node engine, encrypted, and QuickJS at spec 9.

## Summary

| | Built | Partly | Missing | N/A |
| --- | ---: | ---: | ---: | ---: |
| Before | 33 | 7 | 35 | 9 |
| After | 69 | 2 | 4 | 9 |

What is missing is optional and off the runtime's path: `describeRun`, `analytics`, `telemetry` and stream write sessions. The partial rows are message retention, which the attempt cap bounds rather than age, and the queue hold, which 5.2.0 has no capability for.

## Spec versions and capabilities

| Item | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| `specVersion` | Partly | Built | contract, spec9-e2e | `mintedSpecVersion(process.env, { inBandFence: true })`: spec 9 by default since SDK 5.8, spec 8 with `WORKFLOW_SPEC_VERSION=8`. Existing runs keep the version they were stamped with |
| Spec 8 runs (`SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM`) | Built | Built | unit, spec8 e2e lane | New runs with `WORKFLOW_SPEC_VERSION=8`, and runs started before the upgrade |
| Spec 9 runs (`SPEC_VERSION_SINGLE_ORCHESTRATOR`) | Missing | Built | conformance, spec9, spec9-e2e lane | See [Spec 9](#spec-9) |
| `inBandFence` | Missing | Built | conformance | Declared; the SDK's in-band fence suite passes, 13 tests |
| `maxConcurrency` | Missing | Built | queue | One replay per run at a time, spec 9 or earlier |
| `hookRetention` | Built | Built | contract, conformance | |
| `hookResumeDedup` | Built | Built | contract | Static, as world-local and world-postgres declare it. Vercel attests per lookup instead |
| `resolvedHookResume` | Missing | Built | contract | Every resume is deduplicated; `requireHookResumeDedup` without a `resumeId` is a 400, and a missing hook a `HookNotFoundError`, both before anything is written |
| `hookForceClaim` | Missing | Built | force-claim, e2e | See [Forced hook claims](#forced-hook-claims) |
| `atomicStartHook` | Missing | Built | conformance, start-hook, e2e | See [Start Hooks](#start-hooks) |
| `experimental_queueHold` | Missing | Partly | queue | `@workflow/world` 5.2.0 has no such capability and its runtime never asks for a hold, so it is not declared; `queue()` still takes `experimental_hold` ([Queue](#queue)) |
| `deploymentAffinity` | Missing | Built | contract, e2e lanes | A deployment ID names one release, run by this Worker or its retained executor, so the runtime's guard sends a misrouted delivery back to its run's deployment |
| `dynamicWorkflowCode` | Partly | Built | contract, e2e | Before, inline code was stored but not declared and a code ref threw. Now inline or uploaded code is stored with the run; see `uploadDynamicWorkflowCode` |
| `invoke` | N/A | Built | e2e, queue | Vercel does not declare it. A run's workflow deliveries and invocations reach its one `Runner`, which runs the deliveries one at a time and the invocations beside them ([runners](https://world-cloudflare.vaishnav.one/docs/services#runners)) |

## Storage

| Item | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| `runs.get`, `steps.get`, `steps.list` | Built | Built | contract, storage | |
| `runs.getMany` | Built | Built | storage, sharding | One call per store shard, then the legacy store |
| `runs.list` | Built | Built | contract, sharding, index | The index shards, merged by `createdAt` then ID; every store while the index backfills |
| `runs.waitForTerminalStatus` | Built | Built | storage | `AbortSignal` cannot cross RPC and is ignored, as the contract allows |
| `runs.cancelMany` | Built | Built | contract | |
| `runs.experimentalSetAttributes` | N/A | N/A | storage | The contract's stopgap until `attr_set`, which this World stores; the runtime feature-detects it |
| `events.create` | Built | Built | contract, storage | Event, state and outbox commit together in the run's store; dense slots. Claim-bearing writes first persist recoverable admission |
| `events.create` params: `eventCount`, `sinceCursor`, `occurredAt`, `resumeId`, `resumePayloadDigest`, `resolveData` | Built | Built | contract | |
| `events.create` params: `inBand`, `expectedSeqInBand` | Missing | Built | conformance | Threw before |
| `events.create` param: `requireHookResumeDedup` | Missing | Built | contract | |
| Advisory params: `skipPreload`, `preloadEvents`, `viaStepDispatch`, `afterStepBody`, `stepCreatedAt`, `replayDivergenceCount`, `computeInstanceId`, `requestId` | Built | Built | | Ignored, which the contract allows; `preloadEvents` returns no preload, so the runtime reads the log |
| `EventResult.allocated` | Built | Built | conformance, spec9 | 0 for a write that converged on an existing event |
| `EventResult.eventCursor` | Missing | Built | contract | `eid:<eventId>`, given where an ascending list returns the event as written |
| `EventResult.events`, `cursor`, `hasMore` | Built | Built | contract | The skipped-slot report, one page at most; none for a write outside the in-band fence that skipped more than 100 slots |
| `events.createBatch` | Built | Built | contract, spec9 | Spec 9 batches: one fenced block, per-entity refusal ordering, top-level report and `allocated` |
| `events.get`, `events.listByCorrelationId` | Built | Built | contract | |
| `events.list` | Built | Built | contract | `signal` cannot cross RPC and is ignored |
| `events.list` `snapshot` | Missing | Built | conformance | On every page of a spec 9 run, read with the page |
| `events.subscribe` | Missing | Built | contract, spec9-e2e lane | A long poll per page on the run's store, woken by each commit; reports a finished run through `onError` |
| `hooks.get`, `hooks.getByToken`, `hooks.list` | Built | Built | contract, sharding, index | `hooks.get` and a global `hooks.list` read the index shards |
| `Hook.claimedFrom` | Missing | Built | force-claim | |
| `Hook.resumeCapabilities` | N/A | N/A | | Vercel's per-lookup attestation; this World declares `hookResumeDedup` statically |
| `experimental_snapshots` | Built | Built | snapshots, e2e | |
| Slot-numbered event IDs (spec 6), sealed log (spec 7) | Built | Built | world-testing | Positions are allocated at the commit, so there are no holes to seal |

## Spec 9

| Rule | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| Per-run `seq` and `seqInBand`, `run_created` holds the first in-band position | Missing | Built | conformance | `seq` is the last slot; `seqInBand` is `runs.seq_in_band`, set for spec 9 runs only |
| Stale in-band write refused with `InBandSupersededError` (412), nothing written | Missing | Built | conformance | Checked first, in the write's transaction |
| In-band write without an expected count is a 400 | Missing | Built | conformance | So is an in-band write to a run below spec 9, which has no fence |
| Out-of-band writes move `seq` only | Missing | Built | conformance | |
| A resilient `run_started` that creates the run reports `allocated: 1` | Missing | Built | spec9 | It writes the implied `run_created` and the start; the count says two |
| Step and wait events are appends | Missing | Built | spec9 | Nothing refused; the step and wait records follow the events that fit them, for `steps.get` |
| An inline step is created by its first `step_started` alone, keeping its input | Missing | Built | conformance | |
| `step_created`, `step_started`, `step_retrying`, `wait_created` refused on a finished run (`RunExpiredError`); `step_completed`, `step_failed`, `wait_completed` accepted | Missing | Built | spec9 | |
| Writes return no step or wait | Missing | Built | spec9 | |
| Bookkeeping fields kept (`inline`, `creatorMessageId`, `attempt`, `stepName`) | Built | Built | conformance | |
| An in-band write's `createdAt` is its `occurredAt` | Built | Built | conformance | |
| Dispatch outbox | Built | Built | spec9 | A spec 9 run owes no step message or timer: its runtime re-sends them under its own keys. Starts and hook wakes are still recovered, stamped with `runSpecVersion` |

## Forced hook claims

The token's shard records the takeover in `token_takeovers`, asks the holder's store to journal `hook_disposed{forceClaimedBy}` and drop the hook (`forced_disposals`), and only then gives the token to the claimer, whose `hook_created` names what it took in `forceClaimedFrom`. A running holder below spec 8 is refused, and the claimer journals `hook_conflict` with `forceRefusedReason: 'victim-spec-version'`. A resume that found the holder before the move is refused with `HookForceClaimedError` naming the claimer. The run store first validates and persists claim admission; the token shard keeps immutable decisions and reserves pending takeovers. Recovery rolls accepted operations forward across lost replies and resets. See [hook ownership](hook-ownership.md) for the gates, receipts, unmaterialized candidates and receiver-first upgrade procedure. `force-claim.test.ts` and `hook-recovery.test.ts` cover these transitions; the e2e lanes take a real run's token over.

## Start Hooks

The run's store admits `start({ experimental_hook })`: it claims the token on its shard, then creates the run with the start Hook `startHookId(runId)`, so the two commit together. A refusal is recorded in `start_hook_refusals`, so every later copy of the candidate is refused naming the same run. The SDK's atomic start Hook suite passes (25 tests); its 7 orphan-bound cases skip because this World reads the Durable Objects' clock, and `start-hook.test.ts` crosses the 24-hour bound with a candidate minted in the past instead. Retention is limited to 30 days, as world-local's default. A durable intent recovers a grant whose run has not committed. An expired uncreated candidate obtains a token-side cancellation receipt before owing its conditional release. This does not backfill historical orphan grants.

## Queue

| Item | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| `queue`, `queueBatch`, `getDeploymentId` | Built | Built | contract, queue | |
| `queueAuthMode: 'skip'` | Built | Built | contract | Only the queue consumer, called by runners, and private executor dispatch reach the flow route |
| `QueueOptions.deploymentId` | Built | Built | queue | Routed to the deployment's retained executor; omitted targets persist the sender's deployment, not the later consumer's default |
| `QueueOptions.idempotencyKey`, dedupe for 24 hours | Built | Built | queue | Keys are kept forever; a repeat next to a message in backoff changes nothing |
| `QueueOptions.delaySeconds` | Built | Built | queue | |
| Handler error `retryAfter` | Missing | Built | queue-retry-floor | A finite positive number of seconds is a retry floor over local backoff, capped at 23 hours. Invalid values are ignored; this is not step `RetryableError` Date semantics |
| `QueueOptions.retentionSeconds` | Partly | Partly | | No message expires by age, but one whose deliveries keep failing is dead-lettered after `QUEUE_MAX_ATTEMPTS`: about 25 minutes before, about 4 hours now (below) |
| `QueueOptions.experimental_hold` | Missing | Built | queue | Not in 5.2.0's `QueueOptions`, so the World types it itself (`HoldingQueue`). The shard enqueues the message behind a lease the sender renews (1 to 3,600 s, default 30). `consume` runs it as its first delivery. A successful spec 8 `{ timeoutSeconds }` enqueues a fresh message before settling the held original; spec 9 timeouts and `release` retain its ID. Only this deployment's messages, and never a repeat of an accepted key |
| `QueueOptions.specVersion`, `headers` | Built | Built | | The payload is devalue-encoded whatever the version |
| `QueueOptions.region` | N/A | N/A | | No per-message regions; the World creates its objects in the `LOCATION_HINT` region, or near their first caller ([placement](limits.md#shards-and-placement)) |
| One replay per run at a time | Missing | Built | queue, stress | A run's workflow messages (`orchestratorRunIdOf`, any spec version), keyed timers included, go to its run's queue shard, which delivers one batch at a time and holds a run's second message in a batch until the batch ends. A wake that asks only for a replay is settled without a delivery once a later replay of its run has succeeded. Step messages and intermediate timer hops are never held. 5.2.0 asks this only of spec 9 runs; it also keeps the SDK's in-process step single-flight from making one request wait on a promise another settles, which Workers reject |
| `{ timeoutSeconds: n }` by run spec | Partly | Built | queue, e2e lanes | Spec 9: the same message after `n` s, counted; `0` is a handoff, not counted. Spec 8: enqueues a fresh message after `n` s before acknowledging the original, with the same bytes and deployment but fresh ID and counts, even beyond the retry cap. Enqueue failure keeps the original retryable. An unbounded value waits 900 s (spec 9) or 23 hours (spec 8), never 0 |
| `meta.messageId` stable across redeliveries | Built | Built | queue | Retries and copies made after a reset keep it; successful spec 8 timeouts start a new message |
| `meta.deliveryCount` | Partly | Built | queue | Counted per message in `delivery_counts`: 1 only on the first delivery. Before, it equalled the attempt, which counted delay waits |
| `meta.attempt` | Partly | Built | queue | Deliveries less handoffs; the cap counts it |
| `meta.invocationStartedAt` | Missing | Built | queue | When the consumer took the batch |
| `meta.createdAt` | Built | Built | queue | |
| Failing messages redelivered for at least an hour | Partly | Built | | Backoff is `attempt²` seconds up to 5 minutes, so the default 64 attempts last about 4 hours; it was about 25 minutes |
| Coalesced re-runs | N/A | N/A | | Optional; this World holds instead |
| `isDeploymentUnavailableError` | Missing | Built | queue | True for a target with no executor here |
| `invoke` | N/A | Built | e2e | To the run's runner, on this deployment's flow handler; `createQueueHandler` answers an `invoke: true` message with its outcome, errors included. `timeoutMs` is a 408 with the outcome unknown |
| `registerHandler`, `registerRoute` | N/A | N/A | | For in-process queues; deliveries reach `createQueueHandler` through the consumer |
| Dead letters | Built | Built | queue, storage | In R2; a dead-lettered wake reopens its outbox row |

## Streams

| Item | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| `write`, `writeMulti`, `close`, `get`, `list`, `getChunks`, `getInfo` | Built | Built | contract, streams, e2e | Chunks over 1 MiB in R2, up to 32 MiB each. A reader of a step's stream whose writer is gone ends with a `StreamError` ([streams](limits.md#streams)) |
| `createWriteSession`, `streamFlushIntervalMs` | Missing | Missing | | Optional. Each write is one RPC to the run's `RunStreams`, and the runtime falls back to the stateless methods |

## World methods

| Item | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| `getEncryptionKeyForRun` | Built | Built | encryption, encrypted lane | Per-run HKDF keys from `WORKFLOW_ENCRYPTION_SECRET`; no rotation |
| `getRuntimeDeadline` | Missing | Built | request-context | `FLOW_DELIVERY_BUDGET_MS` (2 minutes) after the consumer took the delivery, well inside the CPU limit, so spec 9 stops starting inline steps in time |
| `resolveLatestDeploymentId` | Missing | Built | request-context | The main Worker's `DEPLOYMENT_ID`, asked through `CONSUMER`, so a retained executor gets the latest release rather than its own |
| `uploadDynamicWorkflowCode` | Missing | Built | contract, e2e | To R2 at `runs/<runId>/dynamic-code/<uuid>`; a run accepts a ref only under its own prefix |
| `validateRunExecutionContext` | Missing | Built | contract | 64 KiB of JSON, which the run's row and `run_created` both hold |
| `createRunId`, `regionForRunId` | N/A | N/A | | No regional dimension: the contract says such a World omits them, and run IDs stay plain ULIDs, whose time picks the shard epoch |
| `getEnvironment` | N/A | N/A | | One tenant per deployment |
| `describeRun` | Missing | Missing | | Optional display fields. Its field here would be the run's store shard, which the remote client that tools run cannot compute without the Worker's `SHARD_EPOCHS` |
| `analytics` | Missing | Missing | | Optional. Tools fall back to the storage lists, which the remote client serves |
| `telemetry.recordStepExecution` | Missing | Missing | | Optional; Vercel ties step executions to its platform invocations |
| `start`, `close` | N/A | N/A | | No background workers or pools |

## Conformance

`@workflow/world-testing` starts the World in a Node server, which cannot host a World bound to Durable Objects. Instead `test/conformance.test.ts` runs the SDK's `in-band-fence-conformance` (13 tests) and `atomic-start-hook-conformance` suites, imported by path from `@workflow/world`, and `test/world-testing.test.ts` ports the world-testing scenarios with their workflows (`workflows/world-testing.ts`) to the QuickJS, node engine and spec 9 lanes. Left out: flow-invocation counts, which read world-local's queue, and the lineage workflows, whose body-level `start()` does not load in the workerd test bundle.

## Retained release check

A real-Workers probe left an approval run waiting on release A, then deployed B with the workflow returning 1,000 more. Both received input 35 and resume value 7. The old run kept deployment A and returned 42; the new run kept B and returned 1,042. The main store used receiver-first claim-protocol installation before switching writers. Final deployment restored the original workflow; A stayed retained and the temporary B executor was removed after its run completed.

This checks one unencrypted QuickJS/spec-8 workflow with an `enam` placement hint and published SDK 5.5.0. It does not establish arbitrary workflow changes, encryption/key upgrades, all regions or mixed-version protocol rollback safety. Separate Workers smoke covered forced unused tokens, live takeover notification and remote SDK spec-9 atomic start Hooks.

## Operations

| Item | Done | Notes |
| --- | :---: | --- |
| Installable `createWorld()` package | ✓ | `packages/world-cloudflare`, on npm as `world-cloudflare` |
| SDK web UI (`workflow web`) | ✓ | Through the `world-cloudflare/remote` Node client |
| Versioned schema migrations | ✓ | Drizzle Kit, additive, applied by each Durable Object on start |
| Published to npm | ✓ | As `world-cloudflare` |
| Listed in `worlds-manifest.json` | ✗ | |
| Runs on the published SDK | ✓ | `workflow` 5.8.0 as published. Reading about 4 GiB of stream in one step needed an SDK fix on 5.5.0 ([streams](limits.md#streams)); not rechecked on 5.8.0 |
| SDK `@workflow/world-testing` suite | ✗ | It starts a Node server and cannot inject a Worker-bound World; its scenarios are ported ([Conformance](#conformance)) |
| Retention and cleanup of finished runs | ✗ | Runs, outbox, dedup and takeover rows, and R2 payloads, are kept forever |
| Payloads over 1 MiB | ✓ | In R2; 16 MiB step outputs on Workers, and payloads over 18 MiB serialized are rejected ([limits](limits.md#payloads)) |
| Cleanup of orphaned R2 payloads | ✗ | A write that fails after its payload reached R2 leaves the object behind |
| Store shards, no single object per run or token | ✓ | Store shards hold runs and hook tokens, 128 by default, and `SHARD_EPOCHS` adds more for new runs; runs created before them stay in the legacy store |
| Sharding beyond one queue shard | ✓ | 256 queue shards by default |
| Multi-region placement | ✗ | One region per deployment, with `LOCATION_HINT` |

## Upgrading

- **Hook claim protocol.** Existing installations need the [receiver-first upgrade](hook-ownership.md#compatibility-and-rollout) before enabling intent-producing writers. Old direct acquisition calls can return retryable 503 during that stage. Receipts and disposal tombstones must survive later releases.
- **Spec 9 is the default from SDK 5.8.** New runs are spec 9 once the Worker runs 5.8; set `WORKFLOW_SPEC_VERSION=8` on the Worker and every retained executor to keep new runs on spec 8. Existing runs keep their spec version, and a retained executor on an older SDK keeps running its own.
- **Rate limits (spec 10) are not supported.** The World does not declare `capabilities.rateLimits`, so the SDK never creates a spec 10 run, and the store refuses rate events.
- **Schema changes are additive** and each Durable Object applies them on start. An existing run has no in-band count, so an in-band write to one is a 400; the spec 8 runtime never sends one.
- **List index.** Each store backfills the index on its first wake after the upgrade, and lists and `hooks.get` ask every store until all have. A store that took writes on a version without the index after it backfilled (a rollback) leaves those changes out: delete its `index-backfill` storage key and its `index_sources` rows to backfill it again.
- **Runners.** The `Runner` class needs its own Wrangler migration (`new_sqlite_classes`) and a `RUNNER` binding, so deploy the main Worker at once, not gradually. The current deployment's executor needs `RUNNER` too, and `EXECUTOR_ROUTES` must name it, because invocations run there.
- **Deploy the main Worker first.** Retained executors call `latestDeploymentId` on its `CONSUMER`.
- **Executors built before runners** keep working: their deliveries reach them through runners. Their World does not declare `invoke`, so their spec 8 step deliveries still read the whole log until those runs drain.
