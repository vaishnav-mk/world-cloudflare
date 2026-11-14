# Workflow SDK v5 contract parity

The gap list below is taken from the contract itself, the published `@workflow/world` 5.2.0 (`dist/*.d.ts`) run by `workflow` 5.5.0: every method, option, capability and spec-version rule in its interfaces, capabilities, spec versions, events, queue and start Hooks that Vercel's World implements or the runtime uses.

- **Before** is `main` at `3f24570`; **After** is this branch.
- **Built**: implemented as the contract describes. **Partly**: implemented with a stated gap. **Missing**: not implemented. **N/A**: the contract says a World like this one omits it.
- **Tests** names where it is checked. `pnpm test` runs the workerd tests; `pnpm run test:e2e` runs real SDK runs in workerd on four lanes: QuickJS at spec 8, the native engine, encrypted, and QuickJS at spec 9 (`WORKFLOW_SPEC_VERSION=9`).

The target is the behaviour the SDK sees. Vercel's OIDC, billing, dashboard and regions are out of scope; where a contract item names one, the row says what this World does instead.

## Summary

Rows of the tables below, by status:

| | Built | Partly | Missing | N/A |
| --- | ---: | ---: | ---: | ---: |
| Before | 33 | 7 | 35 | 9 |
| After | 69 | 2 | 4 | 9 |

What is still missing is optional and outside the runtime's path: `describeRun`, `analytics`, `telemetry` and stream write sessions. The partial rows are message retention, which the attempt cap bounds rather than age, and the queue hold, which `@workflow/world` 5.2.0 has no capability for. The SDK's world-testing suite cannot be pointed at this World, so its scenarios are ported ([Conformance](#conformance)).

## Spec versions and capabilities

| Item | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| `specVersion` | Partly | Built | contract, spec9-e2e | `mintedSpecVersion(process.env, { inBandFence: true })`: spec 8 by default, spec 9 with `WORKFLOW_SPEC_VERSION=9`. Existing runs keep the version they were stamped with |
| Spec 8 runs (`SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM`) | Built | Built | e2e lanes | Every new run until the variable is set |
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
| `invoke` | N/A | Built | e2e, queue | Vercel does not declare it. A run's workflow deliveries and invocations reach its one `Runner`, which runs the deliveries one at a time and the invocations beside them ([runners](../packages/world-cloudflare/README.md#runners)) |

## Storage

| Item | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| `runs.get`, `steps.get`, `steps.list` | Built | Built | contract, storage | |
| `runs.getMany` | Built | Built | storage, sharding | One call per store shard, then the legacy store |
| `runs.list` | Built | Built | contract, sharding | Every store, merged by `createdAt` then ID |
| `runs.waitForTerminalStatus` | Built | Built | storage | `AbortSignal` cannot cross RPC and is ignored, as the contract allows |
| `runs.cancelMany` | Built | Built | contract | |
| `runs.experimentalSetAttributes` | N/A | N/A | storage | The contract's stopgap until `attr_set`, which this World stores; the runtime feature-detects it |
| `events.create` | Built | Built | contract, storage | One SQLite transaction per write in the run's store shard; dense slots |
| `events.create` params: `eventCount`, `sinceCursor`, `occurredAt`, `resumeId`, `resumePayloadDigest`, `resolveData` | Built | Built | contract | |
| `events.create` params: `inBand`, `expectedSeqInBand` | Missing | Built | conformance | Threw before |
| `events.create` param: `requireHookResumeDedup` | Missing | Built | contract | |
| Advisory params: `skipPreload`, `preloadEvents`, `viaStepDispatch`, `afterStepBody`, `stepCreatedAt`, `replayDivergenceCount`, `computeInstanceId`, `requestId` | Built | Built | | Ignored, which the contract allows; `preloadEvents` returns no preload, so the runtime reads the log |
| `EventResult.allocated` | Built | Built | conformance, spec9 | 0 for a write that converged on an existing event |
| `EventResult.eventCursor` | Missing | Built | contract | `eid:<eventId>`, given where an ascending list returns the event as written |
| `EventResult.events`, `cursor`, `hasMore` | Built | Built | contract | The skipped-slot report, one page at most |
| `events.createBatch` | Built | Built | contract, spec9 | Spec 9 batches: one fenced block, per-entity refusal ordering, top-level report and `allocated` |
| `events.get`, `events.listByCorrelationId` | Built | Built | contract | |
| `events.list` | Built | Built | contract | `signal` cannot cross RPC and is ignored |
| `events.list` `snapshot` | Missing | Built | conformance | On every page of a spec 9 run, read with the page |
| `events.subscribe` | Missing | Built | contract, spec9-e2e lane | A long poll per page on the run's store, woken by each commit; reports a finished run through `onError` |
| `hooks.get`, `hooks.getByToken`, `hooks.list` | Built | Built | contract, sharding | |
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

The token's shard coordinates a takeover across three stores: the claimer's, the token's and the holder's.

1. The claimer's store asks the token's shard (`claimHookTokenForced`). The shard records the takeover in `token_takeovers`, with its alarm armed first.
2. The shard asks the holder's store (`disposeForcedHook`), which journals `hook_disposed{forceClaimedBy}` while the run is live, removes the hook and records the takeover in `forced_disposals`. A running holder below spec 8 is refused: the claimer journals `hook_conflict` with `forceRefusedReason: 'victim-spec-version'`. A finished holder with a retained token loses it without a write to its log.
3. Only then does the token move to the claimer, whose hook names what it took in `claimedFrom`. The claimer journals `hook_created{force, forceClaimedFrom}`, and the runtime wakes the holder.
4. A resume that found the holder before the move is refused there with `HookForceClaimedError` naming the claimer, after the holder's store asks the token's shard to finish the move. A hook taken before its own creation committed is created and disposed in one write.

A failed call leaves the takeover row, and the token's shard retries it from its alarm. `force-claim.test.ts` covers each step; the e2e lanes take a real run's token over and resume the claimer.

## Start Hooks

`start({ experimental_hook })` is admitted by the run's store: it claims the token on its shard, then creates the run with the start Hook `startHookId(runId)`, so the run and its Hook commit together. A refusal is recorded in `start_hook_refusals`, so every later copy of the candidate, through `run_created` or `run_started`, is refused naming the same run. The SDK's atomic start Hook suite passes, 25 tests; its 7 orphan-bound cases skip because this World reads the Durable Objects' clock, and `start-hook.test.ts` crosses the 24-hour bound with a candidate minted in the past instead. Retention is limited to 30 days, as world-local's default.

A candidate whose token claim succeeded but whose run never committed, because both of its copies died, keeps its token. The SDK retries a start with the same candidate, so this needs both the direct write and the queued copy to be lost.

## Queue

| Item | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| `queue`, `queueBatch`, `getDeploymentId` | Built | Built | contract, queue | |
| `queueAuthMode: 'skip'` | Built | Built | contract | Only the queue consumer, called by runners, and private executor dispatch reach the flow route |
| `QueueOptions.deploymentId` | Built | Built | queue | Routed to the deployment's retained executor |
| `QueueOptions.idempotencyKey`, dedupe for 24 hours | Built | Built | queue | Keys are kept forever; a repeat next to a message in backoff changes nothing |
| `QueueOptions.delaySeconds` | Built | Built | queue | |
| `QueueOptions.retentionSeconds` | Partly | Partly | | No message expires by age, but one whose deliveries keep failing is dead-lettered after `QUEUE_MAX_ATTEMPTS`: about 25 minutes before, about 4 hours now (below) |
| `QueueOptions.experimental_hold` | Missing | Built | queue | Not in 5.2.0's `QueueOptions`, so the World types it itself (`HoldingQueue`). The shard enqueues the message behind a lease the sender renews (1 to 3,600 s, default 30). `consume` runs it as its first delivery; a `{ timeoutSeconds }` result or a `release` delivers it again at once under the same ID. Only this deployment's messages, and never a repeat of an accepted key |
| `QueueOptions.specVersion`, `headers` | Built | Built | | The payload is devalue-encoded whatever the version |
| `QueueOptions.region` | N/A | N/A | | No regions; Durable Objects are placed where they are first used |
| One replay per run at a time | Missing | Built | queue, stress | A run's workflow messages (`orchestratorRunIdOf`, any spec version), keyed timers included, go to its run's queue shard, which delivers one batch at a time and holds a run's second message in a batch until the batch ends. A wake that asks only for a replay is settled without a delivery once a later replay of its run has succeeded. Step messages and intermediate timer hops are never held. 5.2.0 asks this only of spec 9 runs; it also keeps the SDK's in-process step single-flight from making one request wait on a promise another settles, which Workers reject |
| `{ timeoutSeconds: n }` by run spec | Partly | Built | queue, e2e lanes | Spec 9: the same message after `n` s, counted; `0` is a handoff, not counted. Spec 8: continued after `n` s. An unbounded value waits 900 s (spec 9) or 23 hours (spec 8), never 0 |
| `meta.messageId` stable across redeliveries | Built | Built | queue | A copy made after a reset keeps it |
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
| `write`, `writeMulti`, `close`, `get`, `list`, `getChunks`, `getInfo` | Built | Built | contract, streams, e2e | Chunks over 1 MiB in R2, up to 16 MiB each. A reader of a step's stream whose writer is gone ends with a `StreamError` ([streams](limits.md#streams)) |
| `createWriteSession`, `streamFlushIntervalMs` | Missing | Missing | | Optional. Each write is one RPC to the run's `RunStreams`, and the runtime falls back to the stateless methods |

## World methods

| Item | Before | After | Tests | Notes |
| --- | --- | --- | --- | --- |
| `getEncryptionKeyForRun` | Built | Built | encryption, encrypted lane | Per-run HKDF keys from `WORKFLOW_ENCRYPTION_SECRET`; no rotation |
| `getRuntimeDeadline` | Missing | Built | request-context | `FLOW_ATTEMPT_TIMEOUT_MS` (14 minutes) after the consumer took the delivery, when the queue gives up on it. This raises the runtime's inline budget from 2 to 5 minutes, and lets spec 9 stop starting inline steps a minute before |
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

`@workflow/world-testing` starts the World in a Node server, which cannot host a World bound to Durable Objects, so it cannot be pointed at this one. Instead:

- `test/conformance.test.ts` runs the SDK's own `in-band-fence-conformance` (13 tests) and `atomic-start-hook-conformance` (25 tests, 7 orphan-bound cases skipped), imported by path from `@workflow/world`.
- `test/world-testing.test.ts` ports the world-testing scenarios with their workflows (`workflows/world-testing.ts`) and runs them on the QuickJS, native-engine and spec 9 lanes: addition, slot IDs, parallel steps, the fence counters, hook token release, hook collection, null bytes, retryable and fatal errors, the inline-execution workflows and `skip-step-inputs`. Left out: flow-invocation counts, which read world-local's queue, and the lineage workflows, whose body-level `start()` does not load in the workerd test bundle.

## Operations

| Item | Done | Notes |
| --- | :---: | --- |
| Installable `createWorld()` package | ✓ | `packages/world-cloudflare`, unpublished |
| SDK web UI (`workflow web`) | ✓ | Through the `@workflow/world-cloudflare/remote` Node client |
| Versioned schema migrations | ✓ | Drizzle Kit, additive, applied by each Durable Object on start |
| Published to npm | ✗ | The `@workflow` scope belongs to Vercel |
| Listed in `worlds-manifest.json` | ✗ | |
| Runs on the published SDK | ✓ | `workflow` 5.5.0 unpatched; two performance changes are prepared upstream (the spec 8 event-log memo, and QuickJS's inline window) |
| SDK `@workflow/world-testing` suite | ✗ | It starts a Node server and cannot inject a Worker-bound World; its scenarios are ported ([Conformance](#conformance)) |
| Retention and cleanup of finished runs | ✗ | Runs, outbox, dedup and takeover rows, and R2 payloads, are kept forever |
| Payloads over 1 MiB | ✓ | In R2; 16 MiB step outputs on Workers with QuickJS, and payloads over 18 MiB serialized are rejected ([limits](limits.md#payloads)) |
| Cleanup of orphaned R2 payloads | ✗ | A write that fails after its payload reached R2 leaves the object behind |
| Store shards, no single object per run or token | ✓ | Store shards hold runs and hook tokens, 128 by default, and `SHARD_EPOCHS` adds more for new runs; runs created before them stay in the legacy store |
| Sharding beyond one queue shard | ✓ | 64 queue shards |
| Multi-region placement | ✗ | |

## Rollout

- **Spec 9 is opt-in.** Nothing changes for new runs until `WORKFLOW_SPEC_VERSION=9` is set on the Worker and every retained executor. Runs created before keep spec 8.
- **Schema.** Store migrations `0003` and `0004` add `runs.seq_in_band`, `forced_disposals`, `token_takeovers` and `start_hook_refusals`; queue migrations `0002` and `0003` add `delivery_counts` and `holds`. All additive. Existing runs have no in-band count, so an in-band write to one is a 400; the spec 8 runtime never sends one.
- **Queue accounting.** In-flight messages start counting deliveries from 0 when the new code reaches their shard, so each gets up to `QUEUE_MAX_ATTEMPTS` more deliveries. The fanout's own attempt cap no longer dead-letters; the shard does. The longer backoff applies to every failing message.
- **Routing.** Every run's workflow messages, keyed timers included, already go to the run's queue shard on `main`; this branch adds delivery counts and spec 9 handoffs to that shard's serialization rather than a second one.
- **Token release.** A write that frees a token now waits for the token's shard to apply the release, so it is free when the write returns. That adds one RPC to a run's last write when it held hooks.
- **Inline budget.** `getRuntimeDeadline` raises the runtime's inline budget from 2 to 5 minutes per delivery on spec 8 runs too. A CPU-bound workflow near the 300 s CPU limit can set `WORKFLOW_V2_TIMEOUT_MS` to keep the old budget.
- **`CONSUMER`.** Retained executors call `latestDeploymentId` on the main Worker's consumer, so deploy the main Worker first.
- **Runners.** The `Runner` class needs a `v2` migration (`new_sqlite_classes`) and a `RUNNER` binding, so deploy the main Worker at once, not gradually. Queue shards hand deliveries to runners once the new code reaches them; a delivery in flight on the old code is cut off by the deploy and delivered again. The current deployment's executor needs the `RUNNER` binding too, and `EXECUTOR_ROUTES` must name it, because invocations run there.
- **Retained executors built before runners** keep working: their runs' deliveries reach them through runners. Their World does not declare `invoke`, so their spec 8 step deliveries still read the whole log and the last ones replay inline, beside the run's runner, as on `main`, until those runs drain. A hook resume for such a run from the main Worker is an invocation, which the current deployment handles, and its wake reaches the old executor.
