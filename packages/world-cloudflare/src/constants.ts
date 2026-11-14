import { EventType } from './enums.js';
import type { DispatchKind, ShardEpoch } from './types.js';

/**
 * Durable Object names. Runs and hook tokens live on store shards (`lib/shards.ts`); the store named
 * `STORE_NAME` is the legacy store, which still holds runs created before store shards.
 * `QUEUE_SHARD_NAME` named the single queue shard before queue shards. Runners are named
 * `RUNNER_NAME-<n>` (`runnerShardFor`).
 */
export const STORE_NAME = 'global';
export const QUEUE_SHARD_NAME = 'flow';
export const RUNNER_NAME = 'runner';

/** Fanout target the queue shard delivers to. */
export const FLOW_TARGET = 'flow';
/** The queue shard's fanout channel for messages that replay no run, such as steps. */
export const STEPS_CHANNEL = 'steps';
/** How often a queue shard may bring its alarm forward to make fresh runner stubs (`QueueShard`). */
export const FRESH_RUNNER_ARM_INTERVAL_MS = 1_000;
export const FLOW_PATH = '/.well-known/workflow/v1/flow';
export const FLOW_URL = `https://internal${FLOW_PATH}`;

/** Pagination. */
export const DEFAULT_PAGE_LIMIT = 100;
export const MAX_PAGE_LIMIT = 1000;
/** `events.list` without a limit returns the whole log, up to this many events. */
export const FULL_EVENT_LOG_LIMIT = 100_000;
/**
 * A page of events also ends once its stored rows pass this many bytes, and the caller reads on with
 * its cursor. A store reads a page `EVENT_READ_BYTES` at a time, and at most `EVENT_PAGE_READS` pages
 * at once; further readers wait.
 */
export const EVENT_PAGE_MAX_BYTES = 4 * 1_048_576;
export const EVENT_READ_BYTES = 512 * 1024;
export const EVENT_PAGE_READS = 2;
/** Events returned with `sinceCursor` on `events.create`. */
export const SINCE_CURSOR_LIMIT = 1000;
/**
 * A write outside a spec 9 run's in-band fence that skipped more slots than this gets no
 * skipped-slot report: the SDK drops a truncated report whole, and writes fanned out from one
 * snapshot would otherwise each read every event before their own, quadratic in the fan-out.
 */
export const OUT_OF_BAND_REPORT_LIMIT = 100;

/** A run's execution context is stored in the run's row and its run_created, so it stays small. */
export const MAX_EXECUTION_CONTEXT_BYTES = 64 * 1024;

/** The longest retention a start Hook may ask for, as world-local's default limit. */
export const MAX_START_HOOK_RETENTION_MS = 30 * 24 * 60 * 60_000;

/** Runs `runs.cancelMany` cancels at once, as core's own fallback does. */
export const CANCEL_MANY_CONCURRENCY = 20;

/** Long polls. */
export const EVENT_POLL_MS = 15_000;
/** Events per subscription read, which also stops at `EVENT_PAGE_MAX_BYTES` of JSON. */
export const EVENT_POLL_BATCH = 1_000;
export const STREAM_POLL_MS = 15_000;
export const STREAM_READ_BATCH = 100;
/**
 * A page of stream chunks holds at most this many bytes, and always at least one chunk, whether a
 * `RunStreams` call returns it or `getChunks` resolves it from R2. A page of 100 inline chunks
 * would otherwise pass the 32 MiB RPC limit.
 */
export const STREAM_PAGE_MAX_BYTES = 4 * 1_048_576;
/**
 * `streams.get` reads a `RunStreams` object's stream this many bytes at a time, and hands its
 * reader pieces this size, short only where the chunks written so far end. Read without a buffer,
 * the runtime reads 4 KiB at a time, each read a wait on I/O; locally the World read 610 MiB/s that
 * way and 3,200 MiB/s with 1 MiB pieces. A reader holds about two pieces.
 */
export const STREAM_READ_PIECE_BYTES = 1_048_576;
export const MAX_TERMINAL_WAIT_MS = 30_000;
/**
 * The largest stream chunk the World accepts. The writing step's isolate holds a few chunks at once
 * (the one being stored, the next its stream fills), so on Workers 64 MiB chunks ran out of the
 * 128 MB isolate memory with both engines, while 1 GiB streams in 32 MiB chunks worked with both.
 * Locally, live memory peaked near 60 MiB with 16 MiB chunks and near 90 MiB with 32 MiB ones. A
 * larger chunk fails its step instead.
 */
export const STREAM_CHUNK_MAX_BYTES = 32 * 1_048_576;
/**
 * How long a stream passed across a step boundary waits for a chunk once no flow request holds its
 * writer. Then its readers end with an error instead of waiting for chunks nobody will write. The
 * request writing it renews the lease every third of this while it runs.
 */
export const STREAM_WRITER_LEASE_MS = 30_000;

/**
 * Payloads and stream chunks above this size go to R2, and the Durable Objects keep a ref instead.
 * The store keeps each payload twice, once as base64, so this stays well under SQLite's 2 MB limit.
 */
export const PAYLOAD_INLINE_MAX_BYTES = 1_048_576;
/**
 * The largest single serialized event payload the World accepts. A workflow reads each value back
 * into its VM, holding several copies at once; past this size a QuickJS invocation runs out of
 * Workers' 128 MB memory and retries forever. On Workers a 16 MiB step output worked and a 20 MiB
 * one did not. Step outputs serialize bytes as-is, while a step input carries them as base64, about
 * 1.33 times their size, so the largest step input is about 13.5 MiB. Rejecting the write fails
 * the step with an error instead.
 */
export const PAYLOAD_MAX_BYTES = 18 * 1_048_576;
/** A payload goes to R2 this many bytes at a time. */
export const R2_PUT_PART_BYTES = 1_048_576;
/** Starts a payload ref. No UTF-8 text, SDK payload or SDK stream frame starts with 0xff. */
export const PAYLOAD_REF_PREFIX = Uint8Array.of(0xff, ...new TextEncoder().encode('r2:'));

/** VM snapshots are stored in parts under Durable Object SQLite's documented 2 MB value limit. */
export const SNAPSHOT_PART_BYTES = 1_900_000;

/** Events whose commit owes a queue message. */
export const DISPATCH_KINDS: ReadonlySet<string> = new Set<DispatchKind>([
  EventType.RunCreated, EventType.HookReceived, EventType.StepCreated, EventType.WaitCreated,
]);

/** Events whose commit may owe token shards a release: a disposal, or the run finishing. */
export const RELEASE_KINDS: ReadonlySet<string> = new Set<string>([
  EventType.HookDisposed, EventType.RunCompleted, EventType.RunFailed, EventType.RunCancelled,
]);

/** Dispatch outbox recovery. */
export const OUTBOX_DRAIN_BATCH = 50;
export const DISPATCH_RECOVERY_DELAY_MS = 5_000;
export const REDRIVE_DELAY_MS = 10_000;
export const WAIT_RETRY_DELAY_MS = 1_000;
export const MIN_ALARM_DELAY_MS = 1_000;
export const MAX_PUBLISH_BACKOFF_MS = 60_000;

/** Queue delivery. */
export const DEFAULT_QUEUE_MAX_ATTEMPTS = 64;
export const MAX_QUEUE_RETRY_DELAY_MS = 300_000;
/** The wake an unbounded `{ timeoutSeconds }` of a spec 8 run gets: 23 hours, as world-vercel's. */
export const MAX_WAKE_SECONDS = 82_800;
/** The redelivery an unbounded `{ timeoutSeconds }` of a spec 9 message gets, as world-vercel's. */
export const MAX_SPEC9_REDELIVERY_SECONDS = 900;
/** A held message's lease (`experimental_hold`), in seconds: the default, and the longest. */
export const DEFAULT_HOLD_LEASE_SECONDS = 30;
export const MAX_HOLD_LEASE_SECONDS = 3_600;
/** A message's delivery count outlives its last delivery by this long, past any late duplicate. */
export const DELIVERY_COUNT_RETENTION_MS = 24 * 60 * 60_000;
/**
 * One flow delivery. The SDK runs inline steps in one invocation until its replay budget runs out,
 * which can take minutes. Durability's 5 minute default cut those invocations off before they
 * could suspend and save a VM snapshot. Stays under the 15 minute alarm wall time limit.
 */
export const FLOW_ATTEMPT_TIMEOUT_MS = 14 * 60_000;
/**
 * Runs whose latest successful replay a queue shard remembers, to settle the wakes it covers. A
 * forgotten run only costs one more replay.
 */
export const REPLAY_MARKS_MAX = 10_000;
/**
 * How long a flow delivery runs before the SDK hands off (`getRuntimeDeadline`). Workers end an
 * invocation at its CPU limit (`limits.cpu_ms`, 300 s in the templates) long before the queue's
 * attempt timeout, and replay is mostly CPU, so the SDK hands off well inside that.
 */
export const FLOW_DELIVERY_BUDGET_MS = 2 * 60_000;
/**
 * A flow request waits for the background work it started, such as writing a stream a step
 * returned, until this long after its delivery started, so it answers before the delivery times out.
 */
export const FLOW_BACKGROUND_BUDGET_MS = FLOW_ATTEMPT_TIMEOUT_MS - 30_000;
/**
 * Shard counts per epoch when `SHARD_EPOCHS` is unset. Queue shards: durability delivers one batch
 * per lane at a time, and a batch lasts as long as its slowest flow, so a long run holds up only
 * the runs on its own shard. Store shards: runs spread over warm objects instead of each paying to
 * create a new Durable Object on its first write.
 */
export const DEFAULT_SHARD_EPOCHS: readonly ShardEpoch[] = [{ from: 0, store: 128, queue: 256 }];
/** Queue shards the cron wakes per tick. With more shards, each tick wakes the next slice instead. */
export const CRON_WAKES_PER_TICK = 512;
/** Store calls in flight at once when a cross-run read, such as `runs.list`, asks every store. */
export const FAN_OUT_CONCURRENCY = 32;
/**
 * Index shards per epoch when its `SHARD_EPOCHS` entry sets no `index`. Every list asks all of them,
 * and every new run's list row goes to one of the latest epoch's.
 */
export const DEFAULT_INDEX_SHARDS = 16;
/** Owed list rows a store pushes to the index shards in one round. */
export const INDEX_PUSH_BATCH = 200;
/** Runs a store's backfill owes the index per alarm. */
export const INDEX_BACKFILL_BATCH = 200;
/**
 * How long a write that owes a hook's row, or finishes a run, waits for the index shards before it answers. A slower
 * push goes on in the background, and a failed one stays owed for the alarm.
 */
export const INDEX_PUSH_WAIT_MS = 1_000;

/** Workers for Platforms script names in `EXECUTOR_ROUTES`. */
export const EXECUTOR_SCRIPT_PATTERN = /^[a-zA-Z0-9_-]{1,63}$/;

/** Prefix that marks the devalue encoding, as opposed to rows written by the original JSON codec. */
export const ENCODING_PREFIX = 'dv1:';
