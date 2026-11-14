# Limits

This is the one place for measured numbers; the other docs link here. Each number says where it was measured and on which build:

- **Workers** is Workers Paid, with the example Worker and `scripts/remote-limits.mjs`, `scripts/remote-bench.mjs` or `scripts/remote-load.mjs`. **Local** is workerd under `wrangler dev` or in the end-to-end tests.
- **The build** is the commit that recorded the number. Builds before `5321a43` ran the SDK with local changes; later ones run the published `workflow` 5.5.0.
- Payloads are random bytes, so compression does not shrink them. The probe workflows are in `workflows/limits.ts`.

## Compared with Vercel

Vercel's column is its documented limits.

| Limit | Vercel | This World | Measured |
| --- | --- | --- | --- |
| Steps per run | 10,000 | 30,000 sequential steps completed in 23 minutes | Workers, `a72251d` |
| Events per run | 25,000 | 90,003 in one run; the World sets no cap | Workers, `a72251d` |
| Replay | 240 s budget | A QuickJS snapshot restore took 81 ms at 30,000 events; pauses between invocations stayed at 4 to 8 s | Workers, `a72251d` |
| One payload | 50 MB | 16 MiB step output with either engine; 20 MiB ran out of memory. Larger payloads are rejected, and larger data goes in a [byte stream](#streams) | Workers, `3f15876` |
| One stream chunk | 10 MB | 16 MiB (`STREAM_CHUNK_MAX_BYTES`); 32 MiB chunks worked and 64 MiB ran out of memory | Workers, `e888166` |
| One stream | Not documented | 1 GiB in 8 MiB chunks on Workers; 4 GiB in 64 MiB chunks locally, every byte checked. A reading step on SDK 5.5.0 holds about 40 MiB per GiB it reads ([below](#streams)) | Workers, `e888166`; local, `25028b4`, `819e11b` |
| Run creations | 1,000 per second | 321 per second, 82 per second end to end ([below](#run-creation)) | Workers, `3fdbf00` |
| 1,000 parallel steps | Not documented | 66 to 74 s | Workers, `c4cecd7` |

## Run creation

Run creation is the open gap. Measured on Workers with `POST /runs` from one machine through one data center, every run tracked to completion, on `3fdbf00`. That build ran flows on the queue shards; runners have not been measured on Workers yet.

| Shards | Concurrency | Created per second | End to end |
| --- | --- | --- | --- |
| 128 store and 256 queue shards (the default) | 480 | 117 | 26 per second at concurrency 120 |
| An epoch of 512 store and 1,024 queue shards | 480 | 321 | 4,800 runs in 59 s, 82 per second |
| An epoch of 2,048 store and 4,096 queue shards | 480 | 102 to 153 | 62 to 66 per second |

What limits it:
- **Cold shards.** A new Durable Object cost about 1.7 s on its first call (Workers, `3fdbf00`). Too many shards for the load means more cold ones, which is why 2,048 shards did worse than 512.
- **Flow CPU.** With flows switched off, creation reached 358 per second (Workers, `3fdbf00`): flows started by a queue shard competed with it for CPU. They now run on [runners](../packages/world-cloudflare/README.md#runners).
- **Distance.** Each Durable Object call from the test's data center took about 150 ms (Workers, `3fdbf00`), because shards live where they were first called.
- **The test.** One machine and one data center may cap it before the World does.
- **Lists.** `runs.list` and a global `hooks.list` ask every index shard, and `hooks.get` one per epoch. With `resolveData: 'all'` a list also reads its page's runs from their stores, so a large `limit` costs calls and memory.
- **Index and token writes.** A run change or hook change also writes a row to an index shard before the write answers, and a write that frees a token waits for the token's shard: one more call each.

## Payloads

Payloads and stream chunks over 1 MiB (`PAYLOAD_INLINE_MAX_BYTES`) go to R2, so large values cross neither a Durable Object RPC message (32 MiB) nor SQLite (2 MB documented).

| What | Workers, QuickJS | Local |
| --- | --- | --- |
| Step output | 16 MiB works; 20 MiB runs out of memory (`58ee25d`) | 105 MiB (`c7f836f`) |
| Step input, run output | Not measured | 100 MiB (`c7f836f`) |
| Hook payload, a JSON string | Not measured | 40 MB (`c7f836f`) |

The limit on Workers is memory. Reading a value back into the workflow VM holds several copies of it (the R2 read, decompression, deserialization, the VM's copy), and past about 16 MiB a QuickJS invocation exceeds 128 MB; turning compression off did not help (`58ee25d`). The node engine copies less: 20 MiB worked on a Worker running its own flows (`58ee25d`) and ran a retained executor out of memory (`3f15876`).

An invocation that runs out of memory is redelivered and runs out again. So the World rejects any serialized payload over 18 MiB (`PAYLOAD_MAX_BYTES`) before writing it, with a 413 marked `fatal`, and the step or run fails on its first attempt. Step inputs carry bytes as base64, so the largest step input is about 13.5 MiB. `POST /runs` and `POST /hooks/:token` bodies are capped at 100,000,000 bytes, the Free and Pro plan limit.

## Streams

A step that returns a `ReadableStream` of `type: 'bytes'` passes a handle, and the next step reads the bytes from the run's stream; the [README](../packages/world-cloudflare/README.md#payloads-and-streams) has an example.

Locally under `wrangler dev`, `streamBytes(totalMiB, chunkMiB)` writes a pattern and its reading step checks every byte (`25028b4`):

| Stream | Run | Reading step |
| --- | --- | --- |
| 256 MiB in 64 MiB chunks | 3.4 s | 1.2 s, 215 MiB/s |
| 1 GiB in 1 MiB chunks | 9.5 s | 5.3 s, 192 MiB/s |
| 1 GiB in 8 MiB chunks | 14 s | 5.5 s, 185 MiB/s |
| 1 GiB in 64 MiB chunks | 13.7 s | 5.2 s, 196 MiB/s |
| 1 GiB in 256 MiB chunks | 13.4 s | 5.1 s, 201 MiB/s |
| 1 GiB in one chunk | 13.3 s | 5.3 s, 195 MiB/s |
| 4 GiB in 64 MiB chunks | 54 s | 19.6 s, 209 MiB/s |

The writing step ran at about 130 MiB/s, most of it filling the pattern a byte at a time. A JavaScript byte stream without `autoAllocateChunkSize` is read 4 KiB at a time, and each read becomes a chunk: 256 MiB of 4 KiB chunks wrote in 1.1 s and read back in 2.1 s (`25028b4`).

`streams.get` reads each `RunStreams` stream 1 MiB at a time (`STREAM_READ_PIECE_BYTES`) and hands the reader 1 MiB pieces. Before `6f9051c` it read 4 KiB at a time, each read a wait on I/O; through the World alone, 512 MiB read at 610 MiB/s that way and 3,200 MiB/s now. The reading step still gets 4 KiB pieces, because workerd reads the SDK's byte stream 4 KiB at a time, but those reads come from memory. Reading steps of `streamBytes`, locally:

| Stream | QuickJS, `3cdb4fc` | QuickJS, `819e11b` | Node engine, `3cdb4fc` | Node engine, `819e11b` |
| --- | --- | --- | --- | --- |
| 256 MiB in 16 MiB chunks | 226 MiB/s | 394 MiB/s | 212 MiB/s | 386 MiB/s |
| 1 GiB in 8 MiB chunks | 196 MiB/s | 319 MiB/s | 195 MiB/s | 317 MiB/s |
| 2 GiB in 16 MiB chunks | 191 MiB/s | 307 MiB/s | 190 MiB/s | 306 MiB/s |

Before the stream changes of `a61d88f` to `4faa354`, `8e5bcc2` read 1 GiB in 8 MiB chunks at 194 MiB/s with QuickJS and 192 MiB/s with the node engine, so the slowdown measured on Workers after them (15 MiB/s to 4 MiB/s with QuickJS) does not show locally. Each R2 upload now has its next 1 MiB part queued while one is sent (`819e11b`), so it does not wait on the isolate between parts; local R2 writes about 155 MiB/s either way, so only Workers can show the difference.

Live memory, locally, with `node scripts/perf/memory.mjs <engine> streamBytes 256x<chunk>`: the most heap plus external memory a garbage collection left (`e888166`). The script now reports that as `peakMiB`, and as `liveMiB` only after full collections, because a scavenge leaves the old generation's garbage in place.

| Chunk | QuickJS | Node engine |
| --- | --- | --- |
| 4 MiB | 53 MiB | 41 MiB |
| 8 MiB | 52 MiB | 40 MiB |
| 16 MiB | 60 to 69 MiB | 48 MiB |
| 32 MiB | 76 MiB | 64 to 93 MiB |
| 64 MiB | 228 to 236 MiB | 157 to 221 MiB |

16 MiB is the largest of these that stays near half the 128 MB limit with both engines, so the World rejects larger chunks with a `fatal` 413. On Workers, 32 MiB chunks worked for a 32 MiB stream and failed for 4 GiB with QuickJS, and 64 MiB chunks failed with both engines (`e888166`).

A reading step's memory grows with the bytes it reads, about 40 MiB per GiB with either engine. `liveMiB` for `streamBytes` in 16 MiB chunks, locally on `819e11b`, with the published SDK and with a local copy of it whose per-read race (below) is removed:

| Stream | QuickJS | QuickJS, race removed | Node engine | Node engine, race removed |
| --- | --- | --- | --- | --- |
| 2 GiB | 133 MiB | 71 MiB | 125 MiB | 63 MiB |
| 4 GiB | 224 MiB | 71 MiB | 215 MiB | 63 MiB |

The SDK holds it, not the World. `flushablePipe` in `@workflow/core` 5.5.0, which pipes the World's stream into the reading step, races every read against its sink's `closed` promise. Each race adds a reaction to that promise, kept until the stream ends, and with 4 KiB reads that is about 150 bytes per 4 KiB. A 4 GiB read in one step holds about 150 MiB more, past the 128 MB isolate limit, which matches `streamBytes:4096x16` running out of memory on Workers with both engines (`35fd052`). Without the race, memory stays flat from 2 GiB to 4 GiB. The World on `3cdb4fc` held the same: 140 MiB with QuickJS and 123 MiB with the node engine at 2 GiB with the published SDK, 55 to 71 MiB with QuickJS without the race. So the World holds memory flat in the total size, and `test/streams.test.ts` checks that it keeps at most two 16 MiB chunks while writing 512 MiB and two 1 MiB pieces while reading it back. The upstream fix is to watch `closed` once per pipe instead of once per read.

A step's returned stream is written after the step completes, as `waitUntil` work, which Workers cancels 30 s after an invocation ends: that cut a 1 GiB stream off at 128 to 416 MiB on Workers (`e888166`). So a flow request waits for its background work before it answers, for up to 13.5 minutes after its delivery started (`FLOW_BACKGROUND_BUDGET_MS`). `node scripts/remote-limits.mjs streamSlowly:120` writes a stream for two minutes after its step returns.

## Long runs

A 30,000-step `sequentialSteps` run (90,003 events) with QuickJS and retained executors completed in 1,402 s, 81 s of it in pauses between invocations (Workers, `a72251d`). Without VM snapshots, the replay at 35,000 events took 80 s (Workers, `a72251d`). Every 1,000 events (`WORKFLOW_SNAPSHOT_THRESHOLD`) the SDK saves the QuickJS heap as an invocation suspends, and the next invocation restores it instead of replaying the log.

The SDK on that build gave QuickJS the node engine's 2 minute inline window. The published SDK 5.5.0 does not, and its inline budget excludes step time, so a long QuickJS run can hit the CPU limit before it suspends. That is not re-measured on Workers; the change is prepared upstream.

The SDK takes the per-run event ceiling from the World's `run_started` result, and this World returns none, so runs are not stopped at 25,000 events.

## Engines

QuickJS against the node engine on a Worker running its own flows, one run at a time (Workers, `b190e29`):

| | QuickJS | Node engine |
| --- | --- | --- |
| Full replay at 30,000 events | 54 s (81 ms from a snapshot) | 2.6 s |
| 30,000 sequential steps | 2,101 s | 2,076 s |

The node engine has no snapshots and replays the whole log natively at each new invocation, which stays cheap but grows with the log.

Step cost against log size, locally, with `node scripts/perf/run-steps.mjs 10000` (30,003 events, `44c3515`):

| | ~3,000 events | ~27,000 events | Whole run |
| --- | --- | --- | --- |
| Node engine | 7.0 ms per step | 15.8 ms | 117 s |
| Node engine with the event-log memo prepared upstream | 6.2 ms | 6.9 ms | 68 s |
| QuickJS, either way | 1.6 to 1.9 ms | 1.6 to 2.3 ms | 18 s |

Spec 8's node engine rereads the whole log at every step, so its step cost grows with the log.

Freezing the built-ins runs share ([native engine](native-engine.md#how-it-works)) costs about 20 ms once per isolate and 60 µs per replay, measured in Node (local, `44c3515`).

## Queue delivery

A batch of up to 10 deliveries runs together, each with its own outcome, and a queue shard delivers one batch at a time. 1,000 parallel steps took 66 to 74 s (Workers, `c4cecd7`).

With `capabilities.invoke`, a spec 8 step's delivery wakes its run instead of reading the whole log and replaying it, and the run's queue shard and runner coalesce the wakes. `parallelSteps`, locally (`vitest.replays.config.ts`, two runs each; a full read is an `events.list` of the run from the start), on `df2be13`:

| Engine | Steps | Run | Full log reads | Workflow deliveries |
| --- | --- | --- | --- | --- |
| Node | 200 | 2.5 to 3.1 s | 2 to 3 | 2 to 3 |
| Node | 1,000 | 10.5 to 10.9 s | 3 | 3 |
| QuickJS | 200 | 1.9 to 2.0 s | 1 | 3 |
| QuickJS | 1,000 | 5.4 to 5.5 s | 1 | 2 |

Without `invoke`, those runs read the whole log about once per step: 198 and 998 full reads. QuickJS replays after the first restore a VM snapshot (the test config's threshold is 2 events), so they read from the snapshot's cursor.

## CPU

The Wrangler templates set `limits.cpu_ms` to 300,000, the Workers Paid maximum. A step that needs more is killed and redelivered until the queue gives up (64 attempts), so split long CPU work across steps. With the default 30 s limit, a step of about 60 s of CPU never finished (Workers, `a72251d`).

zstd compression in workerd throws on incompressible input from 64 KiB (local, `d675cd8`), so the templates set `WORKFLOW_COMPRESSION_CODEC=gzip`.

## Platform and SDK limits

Documented limits, not measurements:

| Limit | Value |
| --- | --- |
| Worker CPU per invocation | 30 s default, 300 s maximum on Workers Paid |
| Worker memory | 128 MB per isolate |
| Worker startup | 1 s. `wrangler check startup` measures about 40 ms of CPU for the example Worker, which loads the workflow bundle on its first delivery (local, `44c3515`) |
| Subrequests per invocation | 10,000 default; the templates raise it to 1,000,000 because the SDK makes about one Durable Object call per event |
| RPC message | 32 MiB serialized |
| Durable Object alarm | 15 minutes wall time; a flow delivery may run 14 minutes |
| Workflow replay budget | 240 s (`WORKFLOW_REPLAY_TIMEOUT_MS`) |
| SDK deliveries per queue message | 48 |
| Run attributes | 64 keys, 8 KiB per write |
