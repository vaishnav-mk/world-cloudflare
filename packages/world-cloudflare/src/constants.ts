import { EventType } from './enums.js';
import type { DispatchKind } from './types.js';

/** Durable Object names. One store and one queue shard serve every run. */
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
/** Events returned with `sinceCursor` on `events.create`. */
export const SINCE_CURSOR_LIMIT = 1000;

/** Long polls. */
export const EVENT_POLL_MS = 15_000;
export const EVENT_POLL_BATCH = 100;
export const STREAM_POLL_MS = 15_000;
export const STREAM_READ_BATCH = 100;
export const MAX_TERMINAL_WAIT_MS = 30_000;

/**
 * Payloads and stream chunks above this size go to R2, and the Durable Objects keep a ref instead.
 * The store keeps each payload twice, once as base64, so this stays well under SQLite's 2 MB limit.
 */
export const PAYLOAD_INLINE_MAX_BYTES = 1_048_576;
/** Starts a payload ref. No UTF-8 text, SDK payload or SDK stream frame starts with 0xff. */
export const PAYLOAD_REF_PREFIX = Uint8Array.of(0xff, ...new TextEncoder().encode('r2:'));

/** VM snapshots are stored in parts under Durable Object SQLite's documented 2 MB value limit. */
export const SNAPSHOT_PART_BYTES = 1_900_000;

/** Events whose commit owes a queue message. */
export const DISPATCH_KINDS: ReadonlySet<string> = new Set<DispatchKind>([
  EventType.RunCreated, EventType.HookReceived, EventType.StepCreated, EventType.WaitCreated,
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
export const MAX_QUEUE_RETRY_DELAY_MS = 30_000;
/**
 * One flow delivery. The SDK runs inline steps in one invocation until its replay budget runs out,
 * which can take minutes. Durability's 5 minute default cut those invocations off before they
 * could suspend and save a VM snapshot. Stays under the 15 minute alarm wall time limit.
 */
export const FLOW_ATTEMPT_TIMEOUT_MS = 14 * 60_000;

/** Workers for Platforms script names in `EXECUTOR_ROUTES`. */
export const EXECUTOR_SCRIPT_PATTERN = /^[a-zA-Z0-9_-]{1,63}$/;

/** Prefix that marks the devalue encoding, as opposed to rows written by the original JSON codec. */
export const ENCODING_PREFIX = 'dv1:';
