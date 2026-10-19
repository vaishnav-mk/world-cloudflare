# Limits

Measured on 2026-10-19 against a staging deployment on Workers Paid, using `scripts/remote-limits.mjs` and the probe workflows in `workflows/limits.ts`. Payloads are random bytes, so compression cannot shrink them.

## Payload sizes

| What | Largest that worked | Smallest that failed | Failure |
| --- | --- | --- | --- |
| Step output | 5.75 MiB | 6 MiB | `SQLITE_TOOBIG`; the run fails |
| Step input (one step's output passed to the next) | 4 MiB | 8 MiB | `SQLITE_TOOBIG` |
| Run output | 4 MiB | 5 MiB | `SQLITE_TOOBIG` |
| One stream chunk | 5 MiB | 6 MiB | `SQLITE_TOOBIG` |
| Hook payload | 1,040,000 bytes | 1,100,000 bytes | 413 from the example API's body cap; the run keeps waiting |
| `POST /runs` body | 1,048,576 bytes | 1,048,577 bytes | 413 |

Why these numbers:
- **The row cap.** A direct test accepted single values and whole rows up to 8,388,635 bytes (about 8 MiB) in Durable Object SQLite. Cloudflare documents a 2 MB limit, so don't count on more than that.
- **Why less than 8 MiB.** The store keeps a value twice: once as the event's binary payload, and once inside its entity row as base64, about 1.33 times larger. Stream chunks are also encoded on the way in. That is why each kind of payload tops out between 4 and 6 MiB.
- **Raising it** needs payloads offloaded to R2; see [contract parity](contract-parity.md).

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

It now writes `{ "$bytes": "<base64>" }`, 1.34 times the payload size, and a 4 MiB payload streams normally. `GET /runs/:id` uses the same encoding.

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
| Example API request body | 1 MiB |
