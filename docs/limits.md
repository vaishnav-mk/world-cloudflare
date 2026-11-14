# Limits

Measured on Workers Paid with `scripts/remote-limits.mjs`, `scripts/remote-bench.mjs` and the probe workflows in `workflows/limits.ts`, unless marked local. Payloads are random bytes, so compression does not shrink them.

## Compared with Vercel

Vercel's column is its documented limits.

| Limit | Vercel | This World |
| --- | --- | --- |
| Steps per run | 10,000 | 30,000 sequential steps completed in 23 minutes |
| Events per run | 25,000 | 90,003 completed; the World sets no cap |
| Replay | 240 s budget | A snapshot restore took 81 ms at 30,000 events; pauses between invocations stay at 4 to 8 s |
| One payload | 50 MB | 16 MiB step output with QuickJS, 20 MiB with the isolate engine; larger payloads are rejected, and larger data goes in a [byte stream](#streams) |
| One stream chunk | 10 MB | 16 MiB (`STREAM_CHUNK_MAX_BYTES`); on Workers 32 MiB byte-stream chunks worked and 64 MiB ones ran out of memory |
| One stream | Not documented | 4 GiB locally in 64 MiB chunks, every byte checked, before the chunk cap; 1 GiB in 8 MiB chunks on Workers |
| Run creations | 1,000 per second | Up to 362 per second; 321 per second with 4,800 runs all completing, 82 per second end to end |
| 1,000 parallel steps | Not documented | 66 to 74 s |

Run creation is the open gap. Measured on Workers with `POST /runs` from one machine through one colo, every run tracked to completion:

| Build | Concurrency | Created per second | End to end |
| --- | --- | --- | --- |
| One store and one queue shard for every run | 6 | 0.9 | 100 runs in 13 minutes |
| 128 store and 256 queue shards | 480 | 117 | 26 per second at concurrency 120 |
| An epoch of 512 store and 1,024 queue shards | 480 | 321 | 4,800 runs in 59 s, 82 per second |
| An epoch of 2,048 store and 4,096 queue shards | 480 | 102 to 153 | 62 to 66 per second |

What limits it:
- **Cold shards.** A new Durable Object costs about 1.7 s on its first call, so one store per run made every creation pay it. Too many shards for the load does the same, which is why 2,048 shards did worse than 512.
- **Flow CPU.** With flows switched off, creation reached 358 per second. A call from a Durable Object to a service binding or a dispatch namespace runs on the caller's thread, so flows started by a queue shard used that shard's CPU. They now run on runners ([runners](../packages/world-cloudflare/README.md#runners)); not re-measured on Workers.
- **Distance.** Each Durable Object call from the test colo took about 150 ms, because shards live where they were first called.
- **The test.** One machine and one colo may cap it before the World does.
- **Lists.** `runs.list` and a global `hooks.list` ask every store, so their cost grows with the shard count, and a large `limit` with `resolveData: 'all'` costs memory. `hooks.get` asks every store too.
- **Token release.** A disposal or a finished run releases its tokens on their shards before the write returns, which adds a call to a run's last write when it held hooks.
- **The legacy store.** It still serves runs created before store shards, each token shard's first answer (once), and a shard that cannot find a lazily started run.

## Payloads

Payloads and stream chunks over 1 MiB (`PAYLOAD_INLINE_MAX_BYTES`) go to the `PAYLOADS` R2 bucket under `runs/<runId>/<uuid>`. The World client writes them before the Durable Object call and resolves them after it, so large values cross neither a Durable Object RPC message (32 MiB) nor SQLite (2 MB documented). Stream reads are the exception: the run's `RunStreams` object pipes chunks from R2 into the stream it returns ([streams](#streams)).

| What | Workers, QuickJS | Local workerd |
| --- | --- | --- |
| Step output | 16 MiB works; 20 MiB runs out of memory | 105 MiB |
| Step input, run output | Not measured | 100 MiB |
| One stream chunk | 32 MiB worked, 64 MiB ran out of memory; the World now accepts 16 MiB | 16 MiB, the cap; `getWritable()` serializes bytes as base64, so about 12 MiB of bytes |
| Hook payload (a JSON string) | Not measured | 40 MB |
| `POST /runs`, `POST /hooks/:token` body | 100,000,000 bytes, the Free and Pro plan limit | Same |

The limit on Workers is memory. Reading a value back into the workflow VM holds several copies of it (the R2 read, decompression, deserialization, the VM's copy), and past about 16 MiB a QuickJS invocation exceeds 128 MB. Turning compression off does not help. The isolate engine copies less: 20 MiB worked and 32 MiB did not.

An invocation that runs out of memory is redelivered and runs out again, and when flows run in the main Worker it takes other flows in the same isolate with it. So the World rejects any serialized event payload over 18 MiB (`PAYLOAD_MAX_BYTES`) before writing anything, with a `WorkflowWorldError` of status 413 marked `fatal`, so the SDK fails the step that wrote it, or the run committing a suspension, instead of retrying. A step's result is the exception: the SDK redelivers any rejected `step_completed`, and on the native engine the redelivery re-ran the step until it showed only "exceeded max retries". So for an oversized result of a spec 8 step the World writes `step_failed` with a `FatalError` naming the limit and answers the completion with a 409 conflict, which the SDK skips; the run then fails on the first attempt with that message on either engine. A spec 9 orchestrator stops at the refused write and asks for a redelivery, which runs the step again. Step outputs serialize bytes as-is and step inputs as base64, so the largest step input is about 13.5 MiB.

Other costs:
- A replay reads every payload in the run's log except step inputs, so large values add up in one isolate.
- The event subscription (`GET /runs/:id/events`) shows offloaded payloads as `{ "$ref": "<key>" }`; `GET /runs/:id` and `/result` return the bytes.
- R2 objects are never deleted, including ones written for writes that then failed.

Step payloads cannot be streamed without SDK changes. The SDK serializes a step's output into one `Uint8Array`, the World's API takes and returns bytes, and the SDK reads each value whole into the workflow. So the 18 MiB limit stays, and larger data travels as a byte stream.

## Streams

A step that returns a `ReadableStream` of `type: 'bytes'` gives the workflow a handle, and the next step that takes it reads the bytes from the run's stream. The SDK writes such a stream as it is, chunk by chunk, with no serializing, compression or encryption, and the World stores each chunk over 1 MiB in R2. The [README](../packages/world-cloudflare/README.md#large-data) has an example.

Measured locally under `wrangler dev` with `streamBytes(totalMiB, chunkMiB)`: its writing step fills a pattern, and its reading step checks every byte and times itself.

| Stream | Run | Reading step |
| --- | --- | --- |
| 256 MiB in 64 MiB chunks | 3.4 s | 1.2 s, 215 MiB/s |
| 1 GiB in 1 MiB chunks | 9.5 s | 5.3 s, 192 MiB/s |
| 1 GiB in 8 MiB chunks | 14 s | 5.5 s, 185 MiB/s |
| 1 GiB in 64 MiB chunks | 13.7 s | 5.2 s, 196 MiB/s |
| 1 GiB in 256 MiB chunks | 13.4 s | 5.1 s, 201 MiB/s |
| 1 GiB in one chunk | 13.3 s | 5.3 s, 195 MiB/s |
| 4 GiB in 64 MiB chunks | 54 s | 19.6 s, 209 MiB/s |

The writing step ran at about 130 MiB/s: 7.8 s for 1 GiB and 33 s for 4 GiB, most of it spent filling the pattern a byte at a time and writing to local R2.

- **Writes.** The SDK hands `streams.write` one chunk at a time. A chunk over 1 MiB goes to R2 before the `RunStreams` call, 1 MiB at a time so the runtime never copies it whole, and the object records a ref; a smaller one is stored in the object without another copy. A small chunk that is a view of a larger buffer is copied first, because RPC sends a view's whole buffer: a 16-byte view of a 40 MiB buffer failed with the 32 MiB RPC limit.
- **Chunk cap.** A chunk over 16 MiB (`STREAM_CHUNK_MAX_BYTES`) is rejected before it is copied anywhere, with a `fatal` 413, so the step that wrote it fails on its first attempt instead of the isolate running out of memory. A stream returned by a step is written after the step ends; if its first oversized chunk arrives more than 500 ms later, the step has completed and the stream's reader fails instead.
- **Reads.** `streams.get` asks the run's `RunStreams` for the chunks written so far as one stream, waiting up to 15 s for the first, and joins those streams until the stream closes. The object reads a stored chunk only once the reader has taken the one before, and pipes a chunk in R2 from its object body, so neither the object nor the reading Worker holds a chunk. When the reader cancels, the reads stop at the next chunk.
- **Pages.** `getChunks`, and the paged `read` that executors on older World code call, stop at 4 MiB (`STREAM_PAGE_MAX_BYTES`) and return at least one chunk. A page of 40 inline 1 MiB chunks used to fail with the 32 MiB RPC limit.

Live memory in the isolate, measured locally with `node scripts/perf/memory.mjs <engine> streamBytes 256x<chunk>`: the most heap plus external memory a garbage collection left, so memory still in use. The SDK holds about one chunk being stored, and the stream fills the next. Ranges cover runs before and after the World's copies were removed, which differed less than one run did from the next:

| Chunk | QuickJS | Native engine |
| --- | --- | --- |
| 4 MiB | 53 MiB | 41 MiB |
| 8 MiB | 52 MiB | 40 MiB |
| 16 MiB | 60 to 69 MiB | 48 MiB |
| 32 MiB | 76 MiB | 64 to 93 MiB |
| 64 MiB | 228 to 236 MiB | 157 to 221 MiB |

16 MiB is the largest of these that stays near half the 128 MB limit with both engines. On Workers, 32 MiB chunks worked for 32 MiB and failed for 4 GiB with QuickJS, and 64 MiB failed with both. The R2 copy the World no longer makes is native memory, which this measurement does not see.

What still holds data:
- **The writing step holds the chunks it writes**, because the SDK passes the World whole chunks. Filling the buffer `pull` is handed, as `streamBytes` does, avoids a second copy.
- **A JavaScript byte stream needs `autoAllocateChunkSize`.** Without it workerd reads it 4 KiB at a time, and each read becomes a chunk. A `fetch` or R2 body returned from a step is read the same way, so it is stored as 4 KiB chunks in the `RunStreams` object. Through the World's API, 256 MiB of 4 KiB chunks wrote in 1.1 s and read back in 2.1 s.
- **A reading step gets 4 KiB pieces**, because the SDK's reader is a byte stream without `autoAllocateChunkSize`. That, and checking every byte, keeps the reading step near 200 MiB/s at any chunk size.
- **`getChunks` returns whole chunks**, because its result is bytes. A chunk over 4 MiB comes back alone.
- **Object streams**, including `getWritable()`, serialize and frame each chunk, which copies it several times. Use byte streams for large data.
- **Background writes.** A step's returned stream is written after the step completes, as `waitUntil` work. Workers cancels such work 30 s after an invocation ends, which cut a 1 GiB stream off at 128 to 416 MiB on Workers. So a flow request now waits for the work it handed to `waitUntil` before it answers, until 13.5 minutes after its delivery started (`FLOW_BACKGROUND_BUDGET_MS`), and nothing it started runs past the invocation. A reading step in the run's next invocation waits on the run's queue shard meanwhile, so it starts once the stream is written; one inline in the same invocation reads as the stream is written.
- **Abandoned streams.** A stream the SDK names for a step boundary (`strm_<ULID>`, as for a returned stream) has one writer, the flow request running that step. The request renews a 30 s writer lease (`STREAM_WRITER_LEASE_MS`) while it runs, and refuses further writes once it ends. If the request's budget ran out with the stream still being written, the lease ends at once; if the writer stopped or was killed, it lapses 30 s later. Then a reader that has every chunk ends with a `StreamError` instead of waiting, and the reading step fails after its retries, so its delivery no longer holds its queue shard. Streams `getWritable()` names for the run take writes from later steps, so they get no lease and their readers wait as before. `node scripts/remote-limits.mjs streamSlowly:120` writes a stream for two minutes after its step returns.
- **Not encrypted.** The SDK stores byte streams as written, even with `WORKFLOW_ENCRYPTION_SECRET`.

On Workers, run the probes with `WORKFLOW_BASE_URL=https://<worker>.workers.dev node scripts/remote-limits.mjs streamBytes:1024x16 streamBytes:4096x16 streamSlowly:120`; with no arguments the default matrix includes byte streams. Chunks over 16 MiB now fail their step.

## Long runs

A 30,000-step `sequentialSteps` run (90,003 events) with QuickJS and per-deployment executors completed in 1,402 s, 81 s of it in pauses between invocations, on the SDK with this repo's old patch. Three things made that possible:
- **VM snapshots.** The World implements `experimental_snapshots`: every 1,000 events (`WORKFLOW_SNAPSHOT_THRESHOLD`) the SDK saves the QuickJS heap when an invocation suspends, and the next one restores it instead of replaying the whole log. Each run's snapshot lives in its `RunStreams` object in 1.9 MB parts written in one transaction, and moves over RPC as a stream because it can pass 32 MiB. Without snapshots, the replay at 35,000 events took 80 s.
- **An inline window for QuickJS.** The SDK runs steps inline in one invocation until its replay budget runs out, but that budget excludes step time, so a QuickJS invocation hit Workers' 300 s CPU limit first and never suspended or snapshotted. The old SDK patch gave QuickJS the node engine's 2 minute inline window. The published SDK 5.5.0 does not have it, so a long QuickJS run can hit the CPU limit again (not re-measured on Workers); the change is prepared upstream.
- **No log rereads.** The SDK walked the whole event log once per step, so step cost grew with the log (13.6 ms per step at 30,000 events locally). The SDK now drops settled operations from QuickJS's pending list, and QuickJS on 5.5.0 stays at 1.6 to 2.3 ms per step to 27,000 events. The node engine on 5.5.0 still rereads the log: 7.0 ms per step at 3,000 events and 15.8 ms at 27,000, against 6.2 and 6.9 ms with the old patch's memo (`runtime/event-log-memo.ts`), which is prepared upstream ([isolate engine](isolate-engine.md#measurements)).

The SDK takes the per-run event ceiling from the World's `run_started` result, and this World returns none, so runs are not stopped at 25,000 events.

Snapshots are encrypted when `WORKFLOW_ENCRYPTION_SECRET` is set ([encryption](../packages/world-cloudflare/README.md#encryption)) and stored in plaintext otherwise, like every payload. Without a key the SDK only snapshots with `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED=1`, which the Wrangler templates set.

## Queue delivery

The queue shard delivers to the runners, which run the flow handler, through durability's fanout.
- **Lanes.** Durability runs one batch per target at a time, and a batch lasts as long as its slowest flow, up to the 2 minute inline window. Messages hash by run ID onto 8 lanes (`FLOW_LANES`), so a long run holds up only its own lane. A run started during a long run waited 118 s to start before lanes and 0.5 s after.
- **Parallel deliveries.** A batch of up to 10 deliveries runs together, and each gets its own outcome, so a failure retries only that delivery.
- **Batches.** `events.createBatch` (one transaction, a savepoint per event) and `queueBatch` (three round trips per batch) are implemented. The node and isolate engines use them for fan-out; QuickJS writes each pending step on its own.
- **Resets.** A delivery cut off by a reset of the shard (a deploy, a storage error, eviction) is recorded in an `inflight` table. A fresh instance sends it again, and the `scheduled` handler wakes the shard every minute.

1,000 parallel steps took 188 s before these changes and 66 s after.

Before runners, a spec 8 step's delivery read the run's whole log to see whether it was the last step, and the last ones replayed the run inline, on the step's queue shard. With `capabilities.invoke` it wakes the run instead, and the run's queue shard and runner coalesce the wakes. `parallelSteps` in local workerd (`vitest.replays.config.ts`; two runs each; a full read is an `events.list` of the run from the start):

| Engine | Steps | Before | Full reads before | After | Full reads after | Workflow deliveries after |
| --- | --- | --- | --- | --- | --- | --- |
| node | 200 | 2.4 to 3.2 s | 198 | 2.5 to 3.1 s | 2 to 3 | 2 to 3 |
| node | 1,000 | 20.8 to 26.5 s | 998 | 10.5 to 10.9 s | 3 | 3 |
| QuickJS | 200 | 1.9 to 2.5 s | 198 | 1.9 to 2.0 s | 1 | 3 |
| QuickJS | 1,000 | 16.4 to 16.8 s | 998 | 5.4 to 5.5 s | 1 | 2 |

QuickJS replays after the first restore a VM snapshot (the test config's threshold is 2 events), so they read from the snapshot's cursor. Not re-measured on Workers.

## CPU

The Wrangler templates set `limits.cpu_ms` to 300,000, the Workers Paid maximum. A step that needs more is killed and redelivered until the queue gives up (64 attempts), holding up its lane meanwhile, so split long CPU work across steps. With the default 30 s limit, a step of about 60 s of CPU never finished.

zstd compression in workerd throws on incompressible input from 64 KiB, so the templates set `WORKFLOW_COMPRESSION_CODEC=gzip`.

## Platform and SDK limits

| Limit | Value |
| --- | --- |
| Worker CPU per invocation | 30 s default, 300 s maximum on Workers Paid |
| Worker memory | 128 MB per isolate |
| Worker startup | 1 s; with the old SDK patch the main Worker with the workflow bundle measured 518 to 720 ms and was rejected once, and `src/dispatcher.ts` without it 279 to 298 ms. The workflow bundle now loads on the first delivery, and `wrangler check startup` measures about 40 ms of CPU locally for either |
| Subrequests per invocation | 10,000 default; the templates raise it to 1,000,000 because the SDK makes about one Durable Object call per event |
| RPC message | 32 MiB serialized |
| Durable Object alarm | 15 minutes wall time; a flow delivery may run 14 minutes |
| Workflow replay budget | 240 s (`WORKFLOW_REPLAY_TIMEOUT_MS`) |
| SDK deliveries per queue message | 48 |
| Run attributes | 64 keys, 8 KiB per write |
