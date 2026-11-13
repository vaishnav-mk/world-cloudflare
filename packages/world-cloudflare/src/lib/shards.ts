import { env } from 'cloudflare:workers';
import { decodeTime } from 'ulid';
import { CRON_WAKES_PER_TICK, DEFAULT_SHARD_EPOCHS, QUEUE_SHARD_NAME } from '../constants.js';
import type { ShardEpoch, WorldEnv } from '../types.js';
import { fnv1a32Hex } from './hash.js';
import { isStepMessage } from './messages.js';

/**
 * Shard epochs. A run's ULID carries its creation time, which picks the epoch, and the epoch's
 * counts pick its store shard and queue shards. Adding an epoch that starts in the future adds
 * capacity for new runs without moving any existing run, so there is no ceiling to migrate past.
 */
let parsed: readonly ShardEpoch[] | undefined;

function epochs(): readonly ShardEpoch[] {
  parsed ??= parseEpochs((env as Partial<WorldEnv>).SHARD_EPOCHS);
  return parsed;
}

/** Parses `SHARD_EPOCHS`, a JSON list of `{ from, store, queue }` ordered by `from`, the first at 0. */
export function parseEpochs(raw: string | undefined): readonly ShardEpoch[] {
  if (raw === undefined || raw === '') return DEFAULT_SHARD_EPOCHS;
  const value: unknown = JSON.parse(raw);
  const count = (n: unknown) => Number.isSafeInteger(n) && (n as number) > 0;
  if (!Array.isArray(value) || value.length === 0) throw new Error('SHARD_EPOCHS must be a non-empty JSON list');
  let previous = -1;
  for (const epoch of value as Record<string, unknown>[]) {
    const { from, store, queue } = epoch;
    if (!Number.isSafeInteger(from) || (from as number) <= previous || !count(store) || !count(queue)) {
      throw new Error('SHARD_EPOCHS entries need ascending integer `from` and positive `store` and `queue` counts');
    }
    previous = from as number;
  }
  if ((value[0] as ShardEpoch).from !== 0) throw new Error('The first SHARD_EPOCHS entry must start at 0');
  // SAFETY: every entry was checked above.
  return value as ShardEpoch[];
}

/** The epoch a run was created in. An ID that is not a run ULID belongs to the first epoch. */
function epochOf(runId: string): { epoch: ShardEpoch; tag: string } {
  const list = epochs();
  let time = 0;
  try {
    time = decodeTime(runId.slice(runId.indexOf('_') + 1));
  } catch {
    // Not a ULID: the first epoch.
  }
  // Epochs ascend, so the last one started by `time` is the run's.
  const index = list.reduce((found, epoch, at) => (epoch.from <= time ? at : found), 0);
  return { epoch: list[index], tag: tagOf(list[index]) };
}

/** Epoch 0 keeps the unsuffixed names shards had before epochs; later ones are named by their start. */
function tagOf(epoch: ShardEpoch): string {
  return epoch.from === 0 ? '' : `e${epoch.from.toString(36)}-`;
}

function bucket(key: string, count: number): number {
  return Number.parseInt(fnv1a32Hex(key), 16) % count;
}

/** The store shard a run lives in. */
export function storeShardFor(runId: string): string {
  const { epoch, tag } = epochOf(runId);
  return `store-${tag}${bucket(runId, epoch.store)}`;
}

/**
 * The store shard that registers a hook token. A token has no creation time, so tokens stay on the
 * first epoch's store shards: a token must keep one registry for as long as any run may hold it.
 */
export function tokenShardFor(token: string): string {
  return `store-${bucket(token, epochs()[0].store)}`;
}

/**
 * The queue shard for a message, within its run's epoch. A step message hashes by its idempotency
 * key (`key`, as the shard deduplicates it), so a run's steps spread over the shards. Every other
 * message hashes by its run, so one shard holds all of a run's replays and runs them one at a time.
 * A key belongs to one run and one kind of message, so every repeat of it reaches the shard that
 * deduplicates it. A message with no run, such as a health check, uses the first epoch.
 */
export function queueShardFor(runId: string, payload: unknown, key?: string): string {
  const { epoch, tag } = epochOf(runId);
  const routingKey = key !== undefined && isStepMessage(payload) ? key : runId;
  return `${QUEUE_SHARD_NAME}-${tag}${bucket(routingKey, epoch.queue)}`;
}

/** Every store shard in every epoch. */
export function storeShardNames(): string[] {
  return epochs().flatMap((epoch) => Array.from({ length: epoch.store }, (_, index) => `store-${tagOf(epoch)}${index}`));
}

/** Every queue shard in every epoch, plus the single `flow` shard that served messages queued before sharding. */
export function queueShardNames(): string[] {
  return [
    QUEUE_SHARD_NAME,
    ...epochs().flatMap((epoch) => Array.from({ length: epoch.queue }, (_, index) => `${QUEUE_SHARD_NAME}-${tagOf(epoch)}${index}`)),
  ];
}

/**
 * The queue shards the cron wakes on tick `tick` (minutes since the epoch): one slice of at most
 * `CRON_WAKES_PER_TICK`, so every shard is woken once per `ceil(shards / CRON_WAKES_PER_TICK)` ticks.
 */
export function queueShardsToWake(tick: number, names = queueShardNames()): string[] {
  const slices = Math.ceil(names.length / CRON_WAKES_PER_TICK);
  return names.filter((_, index) => index % slices === tick % slices);
}
