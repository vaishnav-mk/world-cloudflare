# Limits

Measured on 2026-10-19 against a staging deployment on Workers Paid, using `scripts/remote-limits.mjs` and the probe workflows in `workflows/limits.ts`. Payloads are random bytes, so compression cannot shrink them.

## Payload sizes

Payloads and stream chunks over 1 MiB (`PAYLOAD_INLINE_MAX_BYTES`) now go to R2. The World client in the calling Worker writes them to the `PAYLOADS` bucket under `runs/<runId>/<uuid>` before the Durable Object call, and the object stores only a small ref. Reads fetch the bytes from R2 in the same Worker, so large values cross neither Durable Object RPC nor SQLite.

| What | Before, inline (staging) | After, through R2 (local workerd) | What stops it now |
| --- | --- | --- | --- |
| Step output | 5.75 MiB; 6 MiB failed | 105 MiB; 110 MiB failed | The workflow's QuickJS VM runs out of memory reading the value |
| Step input (one step's output passed to the next) | 4 MiB; 8 MiB failed | 100 MiB; 200 MiB failed | Same |
| Run output | 4 MiB; 5 MiB failed | 100 MiB; 200 MiB failed | Same |
| One stream chunk | 5 MiB; 6 MiB failed | 70 MiB; 72 MiB failed | The SDK's stream reader rejects frames of 100,000,000 bytes or more, and a chunk serializes to about 1.33 times its size |
| Hook payload (a JSON string) | 1,040,000 bytes; 1,100,000 failed | 40 MB; 50 MB failed | workerd ran out of V8 heap (about 1.4 GB) resuming the hook. Step outputs of 105 MiB pass the same World code, so the cost is in handling one large JSON string |
| `POST /runs` and `POST /hooks/:token` body | 1 MiB | 100,000,000 bytes | The 100 MB request body limit of the Free and Pro plans (200 MB on Business) |

Before, every failure was `SQLITE_TOOBIG` except the hook payload, which hit the example API's 1 MiB body cap.

Payloads are random bytes, so compression cannot shrink them. The local numbers come from runs of the `workflows/limits.ts` probes in the end-to-end test Worker. `pnpm run test:e2e` covers a 20 MiB step output passed to the next step, a 20 MiB run output, a 12 MiB stream chunk and a 20 MB hook payload.

**Production will be lower.** Local workerd has no 128 MB memory limit per isolate, and production has not been measured since this change. Serializing, compressing and reading back one value holds several copies of it at once, so expect a single value to top out somewhere around 20 to 30 MiB on Workers. Measure it with `scripts/remote-limits.mjs`, whose default matrix now probes these sizes.

Other costs of large payloads:
- **Memory adds up.** A replay reads the run's whole event log with payloads, so every large step output, run input and hook payload in a run counts against the same isolate, not only the largest one. Replay leaves out step inputs. `getChunks` resolves a whole page of chunks at once, while `streams.get` reads one large chunk at a time.
- **The remote client** sends and receives payloads as devalue base64, about 1.33 times their size, in one request. The Worker's request body limit and memory bound what `workflow web` can write or read.
- **The event subscription** (`GET /runs/:id/events`, `nextEvents`) reads the store directly, so an offloaded payload appears as `{ "$ref": "runs/<runId>/<uuid>" }` instead of its bytes. `GET /runs/:id` and `GET /runs/:id/result` return the bytes.
- **R2 objects outlive failed writes.** A payload is written before its event commits, so an event that is then rejected, or a retried write, leaves an object no event points at. Nothing deletes them, and objects of finished runs are kept forever like the rest of the run.

Why the old numbers were lower: Durable Object SQLite accepted values and rows up to about 8 MiB in a direct test, against a documented 2 MB limit, and the store keeps each payload twice, once as the event's binary payload and once inside its entity row as base64. The 1 MiB threshold keeps both copies under the documented limit.

## Compression

zstd compression in workerd throws `Unexpected end of file` on incompressible input as small as 64 KiB. The SDK has no fallback, so a step output or run input that size failed with `Failed to serialize step return value`.

Every Wrangler config now sets `WORKFLOW_COMPRESSION_CODEC=gzip`, and an end-to-end test stores a 256 KiB step output. Data written with zstd stays readable, because reads detect the codec from each payload.

## Steps and events

| Probe | Time | Events |
| --- | --- | --- |
| 100 sequential steps | 4.9 s | 303 |
| 500 sequential steps | 22 s | 1,503 |
| 1,000 sequential steps | 41 s | 3,003 |
| 2,000 sequential steps | 79 s | 6,003 |
| 50 parallel steps | 6.7 s | 153 |
| 200 parallel steps | 13 s | 603 |
| 500 parallel steps | 48 s | 1,503 |
| 1,000 parallel steps | 155 s | 3,003 |

- **Sequential steps** cost about 40 ms each.
- **Fan-out** gets slower faster than linearly.
- **Event cap.** The SDK stops a run at 25,000 events, about 8,300 steps at three events each, unless `WORKFLOW_MAX_EVENTS` raises it.

## Step throughput and log size

Each step used to cost more than the one before it, because the SDK walked the whole event log once per step. A 15,000-step `sequentialSteps` run (45,003 events) under `wrangler dev` on an Apple silicon laptop, timed from its own event timestamps:

| Engine | ms per step at 3,000 events | at 30,000 events | Whole run |
| --- | --- | --- | --- |
| QuickJS, before | 2.9 | 13.6 | 169 s |
| QuickJS, after | 1.4 | 1.2 | 19.5 s |
| Isolate (branch `iso-curated`), before | 9.3 | 15.0 | 220 s |
| Isolate (branch `iso-curated`), after | 8.6 | 8.9 | 135 s |

Local isolate steps bottom out near 9 ms because local workerd clamps the timer turns that engine waits on.

CPU profiles taken through the inspector at about 3,000 and 30,000 events found the cause. The store's `createEvent` stayed at 0.65 ms of CPU per event; what grew was SDK code that re-read the log on every step:
- **The node engine's replay loop** (which the isolate engine reuses): the hole check `findEventSlotGap`, the retained session's prefix compare in `resume`, `appendUniqueEvents` rebuilding a set of every event ID, the step count for latency telemetry, the terminal-event check and the wait filters.
- **QuickJS.** The VM's `__pending` list kept every finished step. Each suspension sent all their IDs back to the host and each event searched the list from the front, about 11 of its 13 ms per step at 30,000 events.

The fix is in the SDK patch. `runtime/event-log-memo.ts` keeps those facts per log array and reads only the events appended since its last read. The two helpers that rewrite a log in place mark it, which makes the memos start over, and `resume` skips the prefix compare only for the same array with nothing but appends since it was last fed. QuickJS drops settled steps, waits and attribute writes from `__pending`; it keeps hooks, which `abort()` and the terminal drain still look up. In the profiles after the fix no function's cost per step grows with the log.

To reproduce, run `node scripts/perf/run-steps.mjs` against `wrangler dev --var WORLD_RPC_TOKEN:x --var WORKFLOW_V2_TIMEOUT_MS:1800000`, and use `scripts/perf/profile.mjs` and `scripts/perf/analyze.mjs` for profiles. The script keeps the queue shard busy, because local workerd evicts an idle shard about 70 s after its last request and cancels the delivery it was running. A fresh shard redelivers deliveries a reset cut off, and the `scheduled` handler wakes it every minute; local `wrangler dev` does not run the cron.

## VM snapshots

Without snapshots, every flow invocation replays the run's whole event log inside the QuickJS VM. On a 30,000-step sequential run on staging, the pauses grew with the log: about 9 s at 10,000 events, 34 s at 20,000 and 80 s at 35,000.

The World now implements the SDK's `experimental_snapshots`. When an invocation suspends after processing `WORKFLOW_SNAPSHOT_THRESHOLD` events (1,000 by default) since the last snapshot, the SDK saves the VM's memory. The next invocation restores it and replays only the events after it. The end-to-end test checks that a restored invocation reads only those events. The pauses above were measured before this change; staging has not been measured since.

Each run's snapshot lives in its `RunStreams` object as one envelope of metadata and bytes, split into 1.9 MB parts and written in one transaction. The SDK deletes it when the run finishes. The SDK does not save heaps over 32 MiB, but a compressed snapshot plus its framing can still pass the 32 MiB RPC limit, so it moves between the Worker and the object as a stream. A workerd test round-trips 33 MiB.

Snapshots are stored in plaintext. The SDK only snapshots runs that have an encryption key unless `WORKFLOW_SNAPSHOT_ALLOW_UNENCRYPTED=1` is set. This World has no keys, so `wrangler.jsonc`, `wrangler.executor.jsonc` and the end-to-end test config set it. A snapshot holds the run's memory, including values the workflow still references, which is the same exposure as the payloads the store already keeps in plaintext. Encrypting both is a follow-up.

## CPU

| Step work | Time | Result at the 30 s default CPU limit |
| --- | --- | --- |
| 1 billion loop iterations | 8.5 s | completed |
| 3 billion | 24 s | completed |
| 5 billion | 38 s | completed; Workers tolerates occasional overruns |
| 8 billion | about 60 s | killed and redelivered on every attempt |

The 8-billion step did more damage than failing on its own.
- **It blocked unrelated runs.** One queue shard delivers every message, and a batch is retried as a whole, so new runs stayed `pending` while the step kept being retried.
- **Recovery.** Raising the executor's CPU limit to 300 s let the step finish on its next attempt, after which the queue drained on its own.

The Wrangler templates now set `limits.cpu_ms` to 300,000, the Workers Paid maximum. Split longer CPU work across steps.

## Timers and concurrency

- **A 45 s sleep** completed.
- **A 30 hour sleep** was accepted: it recorded `wait_created` with `resumeAt` 30 hours ahead. Its completion had not been observed when this was written.
- **100 runs started at once** all completed in 17.9 s (p95 16.6 s). Runs of 50 took 20 to 24 s, so the variance is large.
- **Both bottlenecks** remain: the single store object and the single queue shard.

## Event stream

`GET /runs/:id/events` used to write binary payloads as JSON index maps, about 12.6 times their size. Above roughly 2.4 MiB a page passed the 32 MiB RPC limit and the stream came back empty.

It now writes `{ "$bytes": "<base64>" }`, 1.34 times the payload size. `GET /runs/:id` and `GET /runs/:id/result` use the same encoding. Payloads over 1 MiB are in R2 and appear in the event stream as `{ "$ref": "<key>" }`, so a page stays small.

## Platform and SDK limits

| Limit | Value |
| --- | --- |
| Worker CPU per invocation | 30 s default, 300 s maximum on Workers Paid |
| Worker memory | 128 MB per isolate; the SDK's 256 MB QuickJS limit cannot be reached |
| RPC message | 32 MiB serialized |
| Workflow replay budget | 240 s (`WORKFLOW_REPLAY_TIMEOUT_MS`) |
| VM snapshot | Every 1,000 events (`WORKFLOW_SNAPSHOT_THRESHOLD`), heaps up to 32 MiB |
| SDK deliveries per queue message | 48 |
| Queue shard retries | 64 attempts, backoff up to 30 s |
| Run attributes | 64 keys, 8 KiB per write |
| Largest payload kept inline | 1 MiB; larger ones go to R2 |
| SDK stream frame | Under 100,000,000 bytes |
| Example API request body | 100,000,000 bytes, the Free and Pro plan limit |
