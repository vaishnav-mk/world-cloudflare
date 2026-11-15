# Limits

Measured numbers live here, and the other docs link to this page. Each number says where it was measured, on Workers (Workers Paid) or locally (workerd under `wrangler dev` or in the end-to-end tests), and the build that recorded it. Builds before `5321a43` ran the SDK with local changes; later ones run the published `workflow` 5.5.0. Payloads are random bytes, so compression does not shrink them. The probe workflows are in `workflows/limits.ts`.

## Latest regression and boundary check

Workers Paid, 2026-11-14/15, build `8cddcbd`, published SDK 5.5.0, unencrypted runs. Store and load-generator placement requested `enam`; this is not a residency guarantee. The latest epoch used 256 store, 512 queue and 16 index shards. These measurements do not establish capacity in other regions or under other workloads.

### Regression checks

Typecheck and strict-unused checks passed. Unit tests passed 325 with 7 skipped. E2E passed 171 with 31 expected failures and 3 skipped. The deployed validation/result/retry/sleep/hook/stream smoke passed before and after stress testing. Independent review and targeted checks found no new confirmed correctness regression.

There is a measured local writer-path cost. Three paired, interleaved Miniflare samples compared the old writer and its actual admission helper from `de56f97` with the current writer, using the same restored SQLite seed and current reducers. Each sample wrote 750 full step lifecycles. Mean host wall time rose from 144.34 to 195.53 ms, about 35%. Ordinary step events issue five extra SQL calls each, including fence/run reads and recovery checks. This isolates the writer path, not deployed end-to-end latency; it does not establish a 35% whole-workflow regression. No safety checks were removed to recover that cost.

Hook-intent recovery currently schedules all due intents together. Recovery after a large outage backlog has no measured safe burst size. This remains an untested overload risk, not a reproduced loss-of-correctness finding.

### Enforced caps

These are byte or policy caps, not throughput measurements. MiB means 1,048,576 bytes.

| Boundary | Enforced value | Meaning |
| --- | --- | --- |
| Serialized event payload | 18 MiB = 18,874,368 bytes | Rejects oversized writes with fatal 413; not a raw-value allowance |
| Encoded World stream chunk | 32 MiB = 33,554,432 bytes | SDK framing/encryption may reduce the raw application bytes that fit |
| R2 offload threshold | Above 1 MiB | Storage choice, not maximum payload size |
| Execution context | 64 KiB | Stored run context, not the whole event log |
| Explicit list page size | 1,000 records | Event pages also stop at about 4 MiB of stored rows |
| Event list without an explicit limit | Up to 100,000 records before byte paging | Not a total-run event ceiling; follow cursors |
| Request-scoped write batching | 500 items/call, two calls in flight | Not a global admission semaphore |
| Flow attempt/background budget | 14 / 13.5 minutes | Successful spec-8 continuations use fresh messages; not a run lifetime limit |
| Queue attempts | 64 by default | SDK delivery budget is separately 48 per message; successful continuation resets message identity |
| Hold lease | At most 3,600 seconds | Not exactly-once execution |
| Start Hook retention | At most 30 days requested | Does not garbage-collect all ownership receipts |

Byte-exact Workers probes on this QuickJS/SDK build:

- `stepInput:14155722` passed. `stepInput:14155723` failed because the encoded input was 18,874,370 bytes. This is the boundary for this unencrypted byte-array argument, not every possible input shape.
- `streamChunk:25165800` passed. `streamChunk:25165801` failed at 33,554,433 encoded bytes. This workflow uses `getWritable` framing. Returned byte streams can use a different encoding.
- An 18,790,000-byte random step output passed. An 18,800,000-byte output failed at 18,879,647 serialized bytes. Random contents and serialization overhead prevent treating either raw size as a universal cap.

### Observed memory boundaries

| Probe | Completed | Next tested failure |
| --- | --- | --- |
| QuickJS parallel steps | 30,000, 269.4 s | 35,000 hit executor `exceededMemory`; 40,000 and 60,000 also produced memory-limit events |
| Native parallel steps | 2,500, 28.5 s | 3,000 hit executor `exceededMemory` |
| QuickJS returned byte stream, 16 MiB producer chunks | 1.75 GiB, 188.4 s, every byte checked | 2 GiB hit executor `exceededMemory` after about 275 s |

The first memory-limit event stopped each failing probe; its run was cancelled and open streams closed. These are invocation failures, not proof that all retries would fail or that every larger run must fail. Other isolate occupants, code, payloads and placement can move these boundaries. The enforceable memory budget is 128 MB per isolate, not a fixed step count or stream length.

The example normally clamps step-count probes to 30,000. Only the temporary boundary-test releases raised that probe guard to 100,000; the World package and published SDK were unchanged. The original guard and normal executor were restored. Native's cold 1,000-step sample took 67.7 s, versus 21.9 s for the later 2,000-step sample; these are not controlled engine speed comparisons.

### Admission versus verified completion

A Workers-side service-binding generator issued these QuickJS `demo` workloads, then checked every accepted run's doubled result. Verification itself adds status/result traffic. Server completion time comes from stored run timestamps; verified time includes readback. Errors were not silently retried by the generator.

| Workload | Accepted | Admission/s | Admission errors | Server completion span | All accepted outputs verified |
| --- | --- | --- | --- | --- | --- |
| 5,000 burst | 5,000 | 1,583 | 0 | 7.23 s | 9.90 s |
| 20,000 burst | 20,000 | 4,238 | 0 | 20.26 s | 22.31 s |
| 100,000 burst | 100,000 | 5,082 | 0 | 87.64 s | 89.07 s |
| Nominal 1,000/s, 30,000 offered | 30,000 | 905 | 0 | 33.97 s | 37.87 s |
| Nominal 2,000/s, 60,000 offered | 59,975 | 1,652 | 25 HTTP 503s | 52.72 s | 54.83 s |
| Nominal 3,000/s, 90,000 offered | 90,000 | 2,277 | 0 | 81.12 s | 82.77 s |

Every returned run ID completed with zero failed, wrong or unfinished results in these samples. The 25 HTTP 503s returned no run ID to the generator; a retryable response does not prove that creation did not commit. Their outputs were not independently checked. The 100,000 burst had admission p50 107 ms, p95 397 ms and p99 1,207 ms. Its verified end-to-end rate was 1,123/s, distinct from 5,082 admissions/s. At admission's end it still had 47,996 runs without a recorded completion.

The paced jobs did not produce their nominal global rates: leaf startup and request latency spread admissions. They lasted roughly 33 to 40 seconds, not hours. The non-monotonic 503s, readback load and finite duration prevent calling any row an absolute or sustained throughput ceiling. No production load, disk-filling test or all-region capacity test was performed.

## Compared with Vercel

Vercel's column is its [documented limits](https://vercel.com/docs/workflows/pricing#workflow-run-limits), not a benchmark. Its 10,000-step and 25,000-event caps apply to the default Node runtime; QuickJS runs with snapshotting enabled are exempt from those two caps. This World's column was measured on Workers with `LOCATION_HINT` and load generated from Workers using the same region hint. A hint does not guarantee physical colocation or regional residency.

| Limit | Vercel | This World | Build |
| --- | --- | --- | --- |
| Run creations | 1,000 per second | 5,082 per second in a 100,000-run burst | `8cddcbd` |
| Steps per run | 10,000 | 30,000 sequential steps completed | `adff891` |
| Events per run | 25,000 | 90,003 in that run; the World sets no cap | `adff891` |
| Replay after 10,000 steps | 240 s budget | 185 ms with QuickJS, 3.9 s native | `adff891` |
| One payload | 50 MB | 18 MiB serialized cap; 18,790,000-byte raw step output demonstrated | `8cddcbd` |
| One stream chunk | 10 MB | 32 MiB encoded World cap; SDK framing differs by stream type | `8cddcbd` |
| Parallel steps in one run | Not documented | 30,000 in 269.4 s QuickJS; 2,500 in 28.5 s native | `8cddcbd` |

- Historically, on `4ceac87`, creation over 100,000 runs averaged 4,220 per second, with p50 latency 93 ms, p95 237 ms and no errors. On `adff891`, 20,000-run bursts ran at 3,376 to 4,225 per second with QuickJS, the node engine and spec 9, and every run completed with the right result.
- The 30,000-step run used QuickJS and took 3,896 s. The SDK takes its event ceiling from the World's `run_started` result, and this World returns none.
- Replay is the time from a sleep's end to the next step in a run of 10,000 steps (30,008 events). QuickJS restores a VM snapshot. Core saves at an eligible suspension after its default threshold of 1,000 processed events; the node engine has no snapshots and replays the log. The measured gap includes delivery and storage time, not just VM CPU.
- The payload allowance is lower than Vercel's documented value. We enforce a serialized cap to stay inside the isolate memory budget ([payloads](#payloads)). Streams carry larger data, subject to the SDK reader's retained memory ([streams](#streams)).
- Historical parallel-step measurements on `3e5bab2`: with QuickJS, 1,000 took 13 s, 5,000 took 41 s and 10,000 took 69 s; at 20,000 the run's store ran out of memory and three steps failed after their retries. At the first fan-out the store writes a turn's `step_created` events and publishes in batches, which keeps its memory near 60 MiB for 5,000 steps (locally) instead of growing past the 128 MB isolate; before that, 5,000 steps took 683 s and 10,000 never started (`845c1ec`). The node engine finished 1,000 in 14 s and 2,000 in 22 s; its last replay needs memory that grows with the square of the step count, which is in core, so 3,000 and more run it out of memory.
- The encoded chunk cap is `STREAM_CHUNK_MAX_BYTES`. Historically, on `adff891`, 1 GiB in 32 MiB producer chunks completed with every byte checked, at 12 MiB/s with QuickJS and 18 to 20 MiB/s native.

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

Binary payload fields over 1 MiB (`PAYLOAD_INLINE_MAX_BYTES`) go to R2 before commit, leaving references rather than those bytes in Durable Object RPC messages or SQLite rows. Current platform documentation lists 32 MiB serialized RPC messages and 8 MB SQLite values/rows. The conservative offload threshold is unchanged. Non-offloaded metadata and other stored fields still face SQLite's row limit.

Reading a value back holds several copies: the R2 read, decompression, deserialization and VM data. Historical builds ran out of memory near 16 MiB; `8cddcbd` passed an 18,790,000-byte random output. A 20 MiB output worked on a Worker running its own flows (`58ee25d`) but exhausted a retained executor (`3f15876`). These are configuration-dependent observations, not alternative raw-value guarantees.

An oversized invocation can be redelivered and hit the memory limit again. So the World rejects a serialized payload over 18 MiB (`PAYLOAD_MAX_BYTES`) before writing it, with a 413 marked `fatal`, and the step fails on its first attempt. In the current byte-array probe, base64 and framing leave exactly 14,155,722 raw input bytes under that cap; one byte more was rejected. Other shapes, compression and encryption can change the raw allowance. The example's `POST /runs` and `POST /hooks/:token` bodies are capped at 100,000,000 bytes.

## Streams

A step that returns a `ReadableStream` of `type: 'bytes'` passes a handle, and the next step reads the bytes from the run's stream ([example](../packages/world-cloudflare/README.md#payloads-and-streams)). Chunks over 1 MiB go to R2 one at a time, so a stream can be far larger than memory.

The writing isolate holds a few chunks at once: the one being stored and the next one its stream fills. Historical 64 MiB producer chunks exhausted both engines (`e888166`); 32 MiB producer chunks worked (`adff891`). The enforced World cap is 32 MiB of encoded chunk data, rejected with fatal 413 above it. This is not a blanket 32 MiB raw `getWritable` allowance: that probe's base64/framing allows 25,165,800 raw bytes at `8cddcbd`.

A returned stream is written in the background after its step completes, and Workers cancel background work 30 s after an invocation ends. That cut a 1 GiB stream off at 128 to 416 MiB (Workers, `e888166`). So a flow request waits for its background work before it answers, up to 13.5 minutes after its delivery started (`FLOW_BACKGROUND_BUDGET_MS`). While it runs, it renews a 30 s writer lease on the stream (`STREAM_WRITER_LEASE_MS`). Once the lease lapses, readers end with a `StreamError` instead of waiting for chunks nobody will write. Registration starts at the producer's first attempted write, including a chunk still uploading to R2; a producer that never attempts a write has no lease and is not detected yet. `node scripts/remote-limits.mjs streamSlowly:120` writes a stream for two minutes after its step returns.

A reading step on SDK 5.5.0 grows by about 40 MiB per GiB it reads, with either engine. `flushablePipe` in `@workflow/core` races every 4 KiB read against its sink's `closed` promise, and each race leaves a reaction on that promise until the stream ends. Locally on `819e11b`, a step reading 4 GiB in 16 MiB chunks held 224 MiB with QuickJS and 215 MiB native; with the race removed it held 71 and 63 MiB, the same as at 2 GiB. On Workers a 4 GiB read ran out of memory with both engines (`35fd052`). The current retained-executor check failed earlier: 1.75 GiB passed byte-for-byte but 2 GiB produced `exceededMemory`, with 16 MiB producer chunks. Four GiB is not a safe allowance just because an earlier build failed there. Fixing the known retention needs an SDK change to watch `closed` once per pipe, not once per read; the published SDK remains unpatched. The World's own memory stays flat, and `test/streams.test.ts` checks that it holds at most two chunks while writing 512 MiB and two 1 MiB pieces while reading it back.

## Shards and placement

A run's ULID time picks its [shard epoch](../packages/world-cloudflare/README.md#shards-and-epochs), and its ID picks a store shard and a queue shard in that epoch. A new Durable Object is slow on its first call, about 1.7 s (Workers, `3fdbf00`), so more shards than the load needs means more cold calls. To add capacity, append an epoch; never change an existing one.

A Durable Object's initial placement normally favors its first caller; placement and later migration are platform decisions. Without `LOCATION_HINT`, objects first reached from different places can end up far apart: a run whose store and runner were apart took about 170 ms per event write, where 8 to 27 ms is usual (Workers, the build before `2ada977`). With `LOCATION_HINT`, the World requests placement in that region for newly created objects. A region spans several data centers, so a run's store first touches its shared workflow runner without the hint before publishing the start, favoring proximity without guaranteeing colocation. With `LOCATION_HINT=enam`, runs whose runner and store were in different data centers took 105 to 140 ms per step, and runs with both in one took about 70 ms (Workers, the build before `28b3063`). The hint places only new objects, so set it before the first run or add an epoch.

## Subrequest depth

Workers limit how deep a chain of calls between Workers and Durable Objects can go. A flow enqueues through its store into a queue shard, which delivers to a runner, which runs the next flow, and each cycle would spend about five levels. So a queue shard calls its runners through stubs it first called from its own alarm. An alarm starts a fresh chain, and so does every delivery through those stubs, however deep the enqueue behind it was. A call that still runs out of depth fails as a 503, and the retry starts a fresh chain.

## CPU

The templates set `limits.cpu_ms` to 300,000, the Workers Paid maximum. A step that needs more is killed and redelivered until the queue gives up after 64 attempts (`QUEUE_MAX_ATTEMPTS`), so split long CPU work across steps. With the default 30 s limit, a step of about 60 s of CPU never finished (Workers, `a72251d`).

## Platform and SDK limits

Current [Workers](https://developers.cloudflare.com/workers/platform/limits/), [Durable Objects](https://developers.cloudflare.com/durable-objects/platform/limits/) and [RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/#limitations) documentation, retrieved 2026-11-14/15, plus explicit SDK/adapter policies. Platform values below were not established by exhausting every resource:

| Limit | Value |
| --- | --- |
| Worker CPU per invocation | 30 s default, 300 s maximum on Workers Paid |
| Worker memory | 128 MB per isolate, shared by its concurrent requests |
| SQLite-backed object storage | 10 GB per object on Paid; 1 GB per object on Free |
| SQLite key/value, string/BLOB or row | 8 MB documented; not the old 2 MB figure |
| Durable Object request rate | 1,000/s is a soft limit, not an absolute hard ceiling |
| Worker startup | 1 s. The example loads its workflow bundle on its first delivery, not at startup |
| Subrequests per invocation | 10,000 Paid default; templates/executors request 1,000,000. The measured main Worker used the platform default |
| RPC message | 32 MiB serialized |
| Durable Object alarm | 15 minutes wall time; a flow delivery may run 14 minutes |
| Workflow replay budget | 240 s (`WORKFLOW_REPLAY_TIMEOUT_MS`) |
| SDK deliveries per queue message | 48 |
| Run attributes | 64 keys, 8 KiB per write |
