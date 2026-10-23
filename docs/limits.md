# Limits

Measured on Workers Paid with `scripts/remote-limits.mjs`, `scripts/remote-bench.mjs` and the probe workflows in `workflows/limits.ts`, unless marked local. Payloads are random bytes, so compression does not shrink them.

## Compared with Vercel

Vercel's column is its documented limits.

| Limit | Vercel | This World |
| --- | --- | --- |
| Steps per run | 10,000 | 30,000 sequential steps completed in 23 minutes |
| Events per run | 25,000 | 90,003 completed; the World sets no cap |
| Replay | 240 s budget | A snapshot restore took 81 ms at 30,000 events; pauses between invocations stay at 4 to 8 s |
| One payload | 50 MB | 16 MiB step output with QuickJS, 20 MiB with the isolate engine; larger payloads are rejected |
| One stream chunk | 10 MB | 70 MiB locally; not measured on Workers |
| Run creations | 1,000 per second | 26 to 39 per second with one store for every run, lower while the same Worker runs many flows; per-run stores not measured on Workers yet |
| 1,000 parallel steps | Not documented | 66 to 74 s |

Run creation is the open gap. One store object used to serialize every run's writes; each run now has its own store, but one directory object still takes every run's index updates (about three per run) and hook-token claims.

## Payloads

Payloads and stream chunks over 1 MiB (`PAYLOAD_INLINE_MAX_BYTES`) go to the `PAYLOADS` R2 bucket under `runs/<runId>/<uuid>`. The World client writes them before the Durable Object call and resolves them after it, so large values cross neither Durable Object RPC (32 MiB) nor SQLite (2 MB documented).

| What | Workers, QuickJS | Local workerd |
| --- | --- | --- |
| Step output | 16 MiB works; 20 MiB runs out of memory | 105 MiB |
| Step input, run output | Not measured | 100 MiB |
| One stream chunk | Not measured | 70 MiB; the SDK's reader rejects frames of 100,000,000 bytes or more |
| Hook payload (a JSON string) | Not measured | 40 MB |
| `POST /runs`, `POST /hooks/:token` body | 100,000,000 bytes, the Free and Pro plan limit | Same |

The limit on Workers is memory. Reading a value back into the workflow VM holds several copies of it (the R2 read, decompression, deserialization, the VM's copy), and past about 16 MiB a QuickJS invocation exceeds 128 MB. Turning compression off does not help. The isolate engine copies less: 20 MiB worked and 32 MiB did not.

An invocation that runs out of memory is redelivered and runs out again, and when flows run in the main Worker it takes other flows in the same isolate with it. So the World rejects any serialized event payload over 18 MiB (`PAYLOAD_MAX_BYTES`) with a `WorkflowWorldError` of status 413 before writing anything, and the step fails. Step outputs serialize bytes as-is and step inputs as base64, so the largest step input is about 13.5 MiB.

Other costs:
- A replay reads every payload in the run's log except step inputs, so large values add up in one isolate.
- The event subscription (`GET /runs/:id/events`) shows offloaded payloads as `{ "$ref": "<key>" }`; `GET /runs/:id` and `/result` return the bytes.
- R2 objects are never deleted, including ones written for writes that then failed.

## Long runs

A 30,000-step `sequentialSteps` run (90,003 events) with QuickJS and per-deployment executors completed in 1,402 s, 81 s of it in pauses between invocations. Three things made that possible:
- **VM snapshots.** The World implements `experimental_snapshots`: every 1,000 events (`WORKFLOW_SNAPSHOT_THRESHOLD`) the SDK saves the QuickJS heap when an invocation suspends, and the next one restores it instead of replaying the whole log. Each run's snapshot lives in its `RunStreams` object in 1.9 MB parts written in one transaction, and moves over RPC as a stream because it can pass 32 MiB. Without snapshots, the replay at 35,000 events took 80 s.
- **An inline window for QuickJS.** The SDK runs steps inline in one invocation until its replay budget runs out, but that budget excludes step time, so a QuickJS invocation hit Workers' 300 s CPU limit first and never suspended or snapshotted. The SDK patch gives QuickJS the node engine's 2 minute inline window.
- **No log rereads.** The SDK walked the whole event log once per step, so step cost grew with the log (13.6 ms per step at 30,000 events locally). The SDK patch memoizes those passes (`runtime/event-log-memo.ts`) and drops settled operations from QuickJS's pending list: 1.2 ms per step at 30,000 events.

The SDK takes the per-run event ceiling from the World's `run_started` result, and this World returns none, so runs are not stopped at 25,000 events.

Snapshots are stored in plaintext, like every payload. The SDK only snapshots runs with an encryption key unless `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED=1`, which the Wrangler templates set.

## Queue delivery

The queue shard delivers to the flow handler through durability's fanout.
- **Lanes.** Durability runs one batch per target at a time, and a batch lasts as long as its slowest flow, up to the 2 minute inline window. Messages hash by run ID onto 8 lanes (`FLOW_LANES`), so a long run holds up only its own lane. A run started during a long run waited 118 s to start before lanes and 0.5 s after.
- **Parallel deliveries.** A batch of up to 10 deliveries runs together, and each gets its own outcome, so a failure retries only that delivery.
- **Batches.** `events.createBatch` (one transaction, a savepoint per event) and `queueBatch` (three round trips per batch) are implemented. The node and isolate engines use them for fan-out; QuickJS writes each pending step on its own.
- **Resets.** A delivery cut off by a reset of the shard (a deploy, a storage error, eviction) is recorded in an `inflight` table. A fresh instance sends it again, and the `scheduled` handler wakes the shard every minute.

1,000 parallel steps took 188 s before these changes and 66 s after.

## CPU

The Wrangler templates set `limits.cpu_ms` to 300,000, the Workers Paid maximum. A step that needs more is killed and redelivered until the queue gives up (64 attempts), holding up its lane meanwhile, so split long CPU work across steps. With the default 30 s limit, a step of about 60 s of CPU never finished.

zstd compression in workerd throws on incompressible input from 64 KiB, so the templates set `WORKFLOW_COMPRESSION_CODEC=gzip`.

## Platform and SDK limits

| Limit | Value |
| --- | --- |
| Worker CPU per invocation | 30 s default, 300 s maximum on Workers Paid |
| Worker memory | 128 MB per isolate |
| Worker startup | 1 s; this Worker starts in 400 to 760 ms |
| Subrequests per invocation | 10,000 default; the templates raise it to 1,000,000 because the SDK makes about one Durable Object call per event |
| RPC message | 32 MiB serialized |
| Durable Object alarm | 15 minutes wall time; a flow delivery may run 14 minutes |
| Workflow replay budget | 240 s (`WORKFLOW_REPLAY_TIMEOUT_MS`) |
| SDK deliveries per queue message | 48 |
| Run attributes | 64 keys, 8 KiB per write |
