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
| One stream chunk | 10 MB | 1 GiB locally in a byte stream, the probe's cap; 70 MiB through `getWritable()`; not measured on Workers |
| One stream | Not documented | 4 GiB locally in 64 MiB chunks, every byte checked; not measured on Workers |
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
- **Flow CPU.** With flows switched off, creation reached 358 per second. Flows appear to run on the machines of the queue shards that deliver them, so their CPU competes with those shards.
- **Distance.** Each Durable Object call from the test colo took about 150 ms, because shards live where they were first called.
- **The test.** One machine and one colo may cap it before the World does.
- **Lists.** `runs.list` and a global `hooks.list` ask every store, so their cost grows with the shard count, and a large `limit` with `resolveData: 'all'` costs memory. `hooks.get` asks every store too.
- **Token release lag.** A disposal or a finished run releases its tokens on their shards just after the write, in the background, so another run's claim in between still conflicts.
- **The legacy store.** It still serves runs created before store shards, each token shard's first answer (once), and a shard that cannot find a lazily started run.

## Payloads

Payloads and stream chunks over 1 MiB (`PAYLOAD_INLINE_MAX_BYTES`) go to the `PAYLOADS` R2 bucket under `runs/<runId>/<uuid>`. The World client writes them before the Durable Object call and resolves them after it, so large values cross neither a Durable Object RPC message (32 MiB) nor SQLite (2 MB documented). Stream reads are the exception: the run's `RunStreams` object pipes chunks from R2 into the stream it returns ([streams](#streams)).

| What | Workers, QuickJS | Local workerd |
| --- | --- | --- |
| Step output | 16 MiB works; 20 MiB runs out of memory | 105 MiB |
| Step input, run output | Not measured | 100 MiB |
| One stream chunk | Not measured | 1 GiB in a byte stream ([streams](#streams)); 70 MiB through `getWritable()`, whose reader rejects frames of 100,000,000 bytes or more |
| Hook payload (a JSON string) | Not measured | 40 MB |
| `POST /runs`, `POST /hooks/:token` body | 100,000,000 bytes, the Free and Pro plan limit | Same |

The limit on Workers is memory. Reading a value back into the workflow VM holds several copies of it (the R2 read, decompression, deserialization, the VM's copy), and past about 16 MiB a QuickJS invocation exceeds 128 MB. Turning compression off does not help. The isolate engine copies less: 20 MiB worked and 32 MiB did not.

An invocation that runs out of memory is redelivered and runs out again, and when flows run in the main Worker it takes other flows in the same isolate with it. So the World rejects any serialized event payload over 18 MiB (`PAYLOAD_MAX_BYTES`) with a `WorkflowWorldError` of status 413 before writing anything, and the step fails. Step outputs serialize bytes as-is and step inputs as base64, so the largest step input is about 13.5 MiB.

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

- **Writes.** The SDK hands `streams.write` one chunk at a time. A chunk over 1 MiB goes to R2 in one `put` before the `RunStreams` call, which records a ref; a smaller one is stored in the object.
- **Reads.** `streams.get` asks the run's `RunStreams` for the chunks written so far as one stream, waiting up to 15 s for the first, and joins those streams until the stream closes. The object reads a stored chunk only once the reader has taken the one before, and pipes a chunk in R2 from its object body, so neither the object nor the reading Worker holds a chunk. When the reader cancels, the reads stop at the next chunk.
- **Pages.** `getChunks`, and the paged `read` that executors on older World code call, stop at 4 MiB (`STREAM_PAGE_MAX_BYTES`) and return at least one chunk. A page of 40 inline 1 MiB chunks used to fail with the 32 MiB RPC limit.

What still holds data:
- **The writing step holds each chunk it writes**, because the SDK passes the World whole chunks. On Workers that bounds a chunk well under 128 MB; not measured. Filling the buffer `pull` is handed, as `streamBytes` does, avoids a second copy.
- **A JavaScript byte stream needs `autoAllocateChunkSize`.** Without it workerd reads it 4 KiB at a time, and each read becomes a chunk. A `fetch` or R2 body returned from a step is read the same way, so it is stored as 4 KiB chunks in the `RunStreams` object. Through the World's API, 256 MiB of 4 KiB chunks wrote in 1.1 s and read back in 2.1 s.
- **A reading step gets 4 KiB pieces**, because the SDK's reader is a byte stream without `autoAllocateChunkSize`. That, and checking every byte, keeps the reading step near 200 MiB/s at any chunk size.
- **`getChunks` returns whole chunks**, because its result is bytes. A chunk over 4 MiB comes back alone.
- **Object streams**, including `getWritable()`, serialize and frame each chunk, which copies it several times. Use byte streams for large data.
- **Background writes.** A step's returned stream is written after the step completes, as `waitUntil` work, while the next step reads it. Workers documents up to 30 s of such work after an invocation ends, so a stream that takes longer to write may be cut off; the remote probes measure it.
- **Not encrypted.** The SDK stores byte streams as written, even with `WORKFLOW_ENCRYPTION_SECRET`.

On Workers, run the probes with `WORKFLOW_BASE_URL=https://<worker>.workers.dev node scripts/remote-limits.mjs streamBytes:64x64 streamBytes:1024x64`; with no arguments the default matrix includes them.

## Long runs

A 30,000-step `sequentialSteps` run (90,003 events) with QuickJS and per-deployment executors completed in 1,402 s, 81 s of it in pauses between invocations. Three things made that possible:
- **VM snapshots.** The World implements `experimental_snapshots`: every 1,000 events (`WORKFLOW_SNAPSHOT_THRESHOLD`) the SDK saves the QuickJS heap when an invocation suspends, and the next one restores it instead of replaying the whole log. Each run's snapshot lives in its `RunStreams` object in 1.9 MB parts written in one transaction, and moves over RPC as a stream because it can pass 32 MiB. Without snapshots, the replay at 35,000 events took 80 s.
- **An inline window for QuickJS.** The SDK runs steps inline in one invocation until its replay budget runs out, but that budget excludes step time, so a QuickJS invocation hit Workers' 300 s CPU limit first and never suspended or snapshotted. The SDK patch gives QuickJS the node engine's 2 minute inline window.
- **No log rereads.** The SDK walked the whole event log once per step, so step cost grew with the log (13.6 ms per step at 30,000 events locally). The SDK patch memoizes those passes (`runtime/event-log-memo.ts`), and the SDK drops settled operations from QuickJS's pending list: 1.2 ms per step at 30,000 events.

The SDK takes the per-run event ceiling from the World's `run_started` result, and this World returns none, so runs are not stopped at 25,000 events.

Snapshots are encrypted when `WORKFLOW_ENCRYPTION_SECRET` is set ([encryption](../packages/world-cloudflare/README.md#encryption)) and stored in plaintext otherwise, like every payload. Without a key the SDK only snapshots with `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED=1`, which the Wrangler templates set.

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
| Worker startup | 1 s; the main Worker with the workflow bundle measured 518 to 720 ms and was rejected once, and `src/dispatcher.ts` without it 279 to 298 ms |
| Subrequests per invocation | 10,000 default; the templates raise it to 1,000,000 because the SDK makes about one Durable Object call per event |
| RPC message | 32 MiB serialized |
| Durable Object alarm | 15 minutes wall time; a flow delivery may run 14 minutes |
| Workflow replay budget | 240 s (`WORKFLOW_REPLAY_TIMEOUT_MS`) |
| SDK deliveries per queue message | 48 |
| Run attributes | 64 keys, 8 KiB per write |
