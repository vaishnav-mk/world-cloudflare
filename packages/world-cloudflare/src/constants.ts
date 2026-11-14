import { EventType } from './enums.js';
import type { DispatchKind, ShardEpoch } from './types.js';

/**
 * Durable Object names. Runs and hook tokens live on store shards (`lib/shards.ts`); the store named
 * `STORE_NAME` is the legacy store, which still holds runs created before store shards.
 * `QUEUE_SHARD_NAME` named the single queue shard before queue shards.
 */
export const STORE_NAME = 'global';
export const QUEUE_SHARD_NAME = 'flow';

/** Fanout target the queue shard delivers to. */
export const FLOW_TARGET = 'flow';
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

/** The longest retention a start Hook may ask for, as world-local's default limit. */
export const MAX_START_HOOK_RETENTION_MS = 30 * 24 * 60 * 60_000;

/** Runs `runs.cancelMany` cancels at once, as core's own fallback does. */
export const CANCEL_MANY_CONCURRENCY = 20;

/** Long polls. */
export const EVENT_POLL_MS = 15_000;
export const EVENT_POLL_BATCH = 100;
export const STREAM_POLL_MS = 15_000;
export const STREAM_READ_BATCH = 100;
/**
 * A page of stream chunks holds at most this many bytes, and always at least one chunk, whether a
 * `RunStreams` call returns it or `getChunks` resolves it from R2. A page of 100 inline chunks
 * would otherwise pass the 32 MiB RPC limit.
 */
export const STREAM_PAGE_MAX_BYTES = 4 * 1_048_576;
export const MAX_TERMINAL_WAIT_MS = 30_000;

/**
 * Payloads and stream chunks above this size go to R2, and the Durable Objects keep a ref instead.
 * The store keeps each payload twice, once as base64, so this stays well under SQLite's 2 MB limit.
 */
export const PAYLOAD_INLINE_MAX_BYTES = 1_048_576;
/**
 * The largest single serialized event payload the World accepts. A workflow reads each value back
 * into its VM, holding several copies at once; past this size a QuickJS invocation runs out of
 * Workers' 128 MB memory and retries forever. On staging a 16 MiB step output worked and a 20 MiB
 * one did not. Step outputs serialize bytes as-is, while a step input carries them as base64, about
 * 1.33 times their size, so the largest step input is about 13.5 MiB. Rejecting the write fails
 * the step with an error instead.
 */
export const PAYLOAD_MAX_BYTES = 18 * 1_048_576;
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
 * Shard counts per epoch when `SHARD_EPOCHS` is unset. Queue shards: durability delivers one batch
 * per shard at a time, and a batch lasts as long as its slowest flow, so a long run holds up only
 * the runs on its own shard. Store shards: runs spread over warm objects instead of each paying to
 * create a new Durable Object on its first write.
 */
export const DEFAULT_SHARD_EPOCHS: readonly ShardEpoch[] = [{ from: 0, store: 128, queue: 256 }];
/** Queue shards the cron wakes per tick. With more shards, each tick wakes the next slice instead. */
export const CRON_WAKES_PER_TICK = 512;
/** Store calls in flight at once when a cross-run read, such as `runs.list`, asks every store. */
export const FAN_OUT_CONCURRENCY = 32;

/** Workers for Platforms script names in `EXECUTOR_ROUTES`. */
export const EXECUTOR_SCRIPT_PATTERN = /^[a-zA-Z0-9_-]{1,63}$/;

/** Prefix that marks the devalue encoding, as opposed to rows written by the original JSON codec. */
export const ENCODING_PREFIX = 'dv1:';
