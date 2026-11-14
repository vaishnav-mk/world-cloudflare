# Limits

Measured numbers live here, and the other docs link to this page. Each number says where it was measured, on Workers (Workers Paid) or locally (workerd under `wrangler dev` or in the end-to-end tests), and the build that recorded it. Builds before `5321a43` ran the SDK with local changes; later ones run the published `workflow` 5.5.0. Payloads are random bytes, so compression does not shrink them. The probe workflows are in `workflows/limits.ts`.

## Compared with Vercel

Vercel's column is its documented limits. This World's column was measured on Workers, on a deployment of `main` with `LOCATION_HINT` set so every object is in one region, and with load sent from Workers in that region.

| Limit | Vercel | This World | Build |
| --- | --- | --- | --- |
| Run creations | 1,000 per second | 4,220 per second | `4ceac87` |
| Steps per run | 10,000 | 30,000 sequential steps completed | `adff891` |
| Events per run | 25,000 | 90,003 in that run; the World sets no cap | `adff891` |
| Replay after 10,000 steps | 240 s budget | 185 ms with QuickJS, 3.9 s native | `adff891` |
| One payload | 50 MB | 16 MiB step output | `3f15876` |
| One stream chunk | 10 MB | 32 MiB | `adff891` |
| Parallel steps in one run | Not documented | 1,000 in 7 to 26 s | `adff891` |

<!-- parallel: to be updated -->

- Over 100,000 runs, creation averaged 4,220 per second, with p50 latency 93 ms, p95 237 ms and no errors. On `adff891`, 20,000-run bursts ran at 3,376 to 4,225 per second with QuickJS, the node engine and spec 9, and every run completed with the right result.
- The 30,000-step run used QuickJS and took 3,896 s. The SDK takes its event ceiling from the World's `run_started` result, and this World returns none.
- Replay is the time from a sleep's end to the next step in a run of 10,000 steps (30,008 events). QuickJS restores a VM snapshot, which the SDK saves every 1,000 events; the node engine has none and replays the whole log.
- One payload is the one row below Vercel. The limit is the 128 MB isolate's memory ([payloads](#payloads)). Larger data goes in byte streams, which carry 1 GiB and more ([streams](#streams)).
- The chunk cap is `STREAM_CHUNK_MAX_BYTES`. 1 GiB in 32 MiB chunks completed with every byte checked, at 12 MiB/s with QuickJS and 18 to 20 MiB/s native.

## How to measure

Each script runs against `WORKFLOW_BASE_URL`, a deployed example Worker or `wrangler dev`.

| Script | Does | Prints |
| --- | --- | --- |
| `scripts/remote-load.mjs [count] [concurrency] [--wait] [--verify]` | Starts `count` runs of `demo`, `concurrency` at a time | One JSON line: created per second, p50, p95 and p99 latency, errors by status. `--wait` adds end-to-end throughput; `--verify` also checks every result |
| `scripts/remote-bench.mjs [probe:value ...]` | Runs `sequentialSteps` or `replayAfterSteps` | A JSON line per run: its seconds, and for `replayAfterSteps` the replay time (`replayMs`) |
| `scripts/remote-limits.mjs [probe:value ...]` | Starts one run per probe, such as `streamBytes:1024x32` | A JSON line per probe: status, seconds and error, event counts for step probes, and bytes read, first wrong byte and MiB/s for streams |
| `scripts/remote-e2e.mjs` | Checks validation, results, subscriptions, retries, sleeps, hooks and streams | An `ok` line per check, then `remote E2E passed` |

One client machine caps creation throughput by its own latency: with `concurrency` requests in flight, it starts at most `concurrency` runs per round trip. The creation figures above came from load generated inside Cloudflare, in the same region as the objects.

## Payloads

Payloads over 1 MiB (`PAYLOAD_INLINE_MAX_BYTES`) go to R2, so no value crosses a Durable Object RPC message (32 MiB) or a SQLite value (2 MB documented). The limit is memory. Reading a value back into the workflow holds several copies of it at once: the R2 read, decompression, deserialization and the VM's copy. Past about 16 MiB a QuickJS invocation exceeds 128 MB (Workers, `58ee25d`). The node engine copies less: a 20 MiB output worked on a Worker running its own flows (`58ee25d`) and ran a retained executor out of memory (`3f15876`).

An invocation that runs out of memory is redelivered and runs out again. So the World rejects a serialized payload over 18 MiB (`PAYLOAD_MAX_BYTES`) before writing it, with a 413 marked `fatal`, and the step fails on its first attempt. Step inputs carry bytes as base64, so the largest step input is about 13.5 MiB. The example's `POST /runs` and `POST /hooks/:token` bodies are capped at 100,000,000 bytes.

## Streams

A step that returns a `ReadableStream` of `type: 'bytes'` passes a handle, and the next step reads the bytes from the run's stream ([example](../packages/world-cloudflare/README.md#payloads-and-streams)). Chunks over 1 MiB go to R2 one at a time, so a stream can be far larger than memory.

The writing step's isolate holds a few chunks at once: the one being stored and the next one its stream fills. 64 MiB chunks ran out of memory with both engines (Workers, `e888166`) and 32 MiB chunks worked (Workers, `adff891`), so the World rejects a larger chunk with a `fatal` 413.

A returned stream is written in the background after its step completes, and Workers cancel background work 30 s after an invocation ends. That cut a 1 GiB stream off at 128 to 416 MiB (Workers, `e888166`). So a flow request waits for its background work before it answers, up to 13.5 minutes after its delivery started (`FLOW_BACKGROUND_BUDGET_MS`). While it runs, it renews a 30 s writer lease on the stream (`STREAM_WRITER_LEASE_MS`). Once the lease lapses, readers end with a `StreamError` instead of waiting for chunks nobody will write. `node scripts/remote-limits.mjs streamSlowly:120` writes a stream for two minutes after its step returns.

A reading step on SDK 5.5.0 grows by about 40 MiB per GiB it reads, with either engine. `flushablePipe` in `@workflow/core` races every 4 KiB read against its sink's `closed` promise, and each race leaves a reaction on that promise until the stream ends. Locally on `819e11b`, a step reading 4 GiB in 16 MiB chunks held 224 MiB with QuickJS and 215 MiB native; with the race removed it held 71 and 63 MiB, the same as at 2 GiB. On Workers a 4 GiB read ran out of memory with both engines (`35fd052`). Reading 4 GiB in one step needs an SDK fix: watch `closed` once per pipe, not once per read. The World's own memory stays flat, and `test/streams.test.ts` checks that it holds at most two chunks while writing 512 MiB and two 1 MiB pieces while reading it back.

## Shards and placement

A run's ULID time picks its [shard epoch](../packages/world-cloudflare/README.md#shards-and-epochs), and its ID picks a store shard and a queue shard in that epoch. A new Durable Object is slow on its first call, about 1.7 s (Workers, `3fdbf00`), so more shards than the load needs means more cold calls. To add capacity, append an epoch; never change an existing one.

A Durable Object is created near its first caller and stays there. Without `LOCATION_HINT`, objects first reached from different places can end up far apart: a run whose store and runner were apart took about 170 ms per event write, where 8 to 27 ms is usual (Workers, the build before `2ada977`). With `LOCATION_HINT`, the World creates its objects in that region. A region spans several data centers, so a run's store creates the run's runner itself, without the hint, before it publishes the run's start, and the runner is created next to the store. With `LOCATION_HINT=enam`, runs whose runner and store were in different data centers took 105 to 140 ms per step, and runs with both in one took about 70 ms (Workers, the build before `28b3063`). The hint places only new objects, so set it before the first run or add an epoch.

## Subrequest depth

Workers limit how deep a chain of calls between Workers and Durable Objects can go. A flow enqueues through its store into a queue shard, which delivers to a runner, which runs the next flow, and each cycle would spend about five levels. So a queue shard calls its runners through stubs it first called from its own alarm. An alarm starts a fresh chain, and so does every delivery through those stubs, however deep the enqueue behind it was. A call that still runs out of depth fails as a 503, and the retry starts a fresh chain.

## CPU

The templates set `limits.cpu_ms` to 300,000, the Workers Paid maximum. A step that needs more is killed and redelivered until the queue gives up after 64 attempts (`QUEUE_MAX_ATTEMPTS`), so split long CPU work across steps. With the default 30 s limit, a step of about 60 s of CPU never finished (Workers, `a72251d`).

## Platform and SDK limits

Documented limits, not measurements:

| Limit | Value |
| --- | --- |
| Worker CPU per invocation | 30 s default, 300 s maximum on Workers Paid |
| Worker memory | 128 MB per isolate |
| Worker startup | 1 s. The example loads its workflow bundle on its first delivery, not at startup |
| Subrequests per invocation | 10,000 default. The templates raise it to 1,000,000, because the SDK makes about one Durable Object call per event |
| RPC message | 32 MiB serialized |
| Durable Object alarm | 15 minutes wall time; a flow delivery may run 14 minutes |
| Workflow replay budget | 240 s (`WORKFLOW_REPLAY_TIMEOUT_MS`) |
| SDK deliveries per queue message | 48 |
| Run attributes | 64 keys, 8 KiB per write |
