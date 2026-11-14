import { env } from 'cloudflare:workers';
import { orchestratorRunIdOf } from '@workflow/world';
import { decodeTime } from 'ulid';
import { CRON_WAKES_PER_TICK, DEFAULT_INDEX_SHARDS, DEFAULT_SHARD_EPOCHS, QUEUE_SHARD_NAME, RUNNER_NAME, STORE_NAME } from '../constants.js';
import type { ShardEpoch, WorldEnv } from '../types.js';
import { fnv1a32Hex } from './hash.js';

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

/** Parses `SHARD_EPOCHS`, a JSON list of `{ from, store, queue, index? }` ordered by `from`, the first at 0. */
export function parseEpochs(raw: string | undefined): readonly ShardEpoch[] {
  if (raw === undefined || raw === '') return DEFAULT_SHARD_EPOCHS;
  const value: unknown = JSON.parse(raw);
  const count = (n: unknown) => Number.isSafeInteger(n) && (n as number) > 0;
  if (!Array.isArray(value) || value.length === 0) throw new Error('SHARD_EPOCHS must be a non-empty JSON list');
  let previous = -1;
  for (const epoch of value as Record<string, unknown>[]) {
    const { from, store, queue, index } = epoch;
    if (!Number.isSafeInteger(from) || (from as number) <= previous || !count(store) || !count(queue) || !(index === undefined || count(index))) {
      throw new Error('SHARD_EPOCHS entries need ascending integer `from`, positive `store` and `queue` counts, and an optional positive `index`');
    }
    previous = from as number;
  }
  if ((value[0] as ShardEpoch).from !== 0) throw new Error('The first SHARD_EPOCHS entry must start at 0');
  // SAFETY: every entry was checked above.
  return value as ShardEpoch[];
}

const LOCATION_HINTS: ReadonlySet<string> = new Set<DurableObjectLocationHint>([
  'wnam', 'enam', 'sam', 'weur', 'eeur', 'apac', 'apac-ne', 'apac-se', 'oc', 'afr', 'me',
]);
let hint: { locationHint?: DurableObjectLocationHint } | undefined;

/**
 * The options for every `getByName` of the World's own objects: the `LOCATION_HINT` region, so a
 * new object is created there instead of near its first caller. Objects that exist keep their place.
 */
export function objectOptions(): { locationHint?: DurableObjectLocationHint } {
  hint ??= parseLocationHint((env as Partial<WorldEnv>).LOCATION_HINT);
  return hint;
}

/** Parses `LOCATION_HINT`: unset, or one of the Durable Object location hints. */
export function parseLocationHint(raw: string | undefined): { locationHint?: DurableObjectLocationHint } {
  if (raw === undefined || raw === '') return {};
  if (!LOCATION_HINTS.has(raw)) throw new Error(`LOCATION_HINT must be one of ${[...LOCATION_HINTS].join(', ')}`);
  // SAFETY: checked against the hints above.
  return { locationHint: raw as DurableObjectLocationHint };
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
 * The queue shard for a message, within its run's epoch. A run's workflow messages
 * (`orchestratorRunIdOf`: its start, wakes, hook resumes and timers, of any spec version) hash by
 * the run, so one shard holds all of a run's replays and runs them one at a time. Other messages,
 * such as steps, hash by their idempotency key (`key`, as the shard deduplicates it), so a run's
 * steps spread over the shards. A key belongs to one run and one kind of message, so every repeat
 * of it reaches the shard that deduplicates it. A message with no run, such as a health check,
 * uses the first epoch.
 */
export function queueShardFor(runId: string, payload: unknown, key?: string): string {
  const { epoch, tag } = epochOf(runId);
  const routingKey = orchestratorRunIdOf(payload) === undefined ? key ?? runId : runId;
  return `${QUEUE_SHARD_NAME}-${tag}${bucket(routingKey, epoch.queue)}`;
}

/**
 * The runner (`Runner`) that runs a run's workflow deliveries and invocations, so a run has one.
 * An epoch has as many runners as queue shards, and a run's runner has its queue shard's index, so
 * that shard hands its replays to one runner; with a queue count that is a multiple of the store
 * count, a runner's runs all live on one store shard, which creates it (`StoreContext.placeRunner`).
 * With `key`, the runner for another of the run's messages, such as a step: any of the epoch's step
 * runners, named apart so that a step never creates a run's runner before its store does.
 */
export function runnerShardFor(runId: string, key?: string): string {
  const { epoch, tag } = epochOf(runId);
  return key === undefined ? `${RUNNER_NAME}-${tag}${bucket(runId, epoch.queue)}` : `${RUNNER_NAME}-steps-${tag}${bucket(key, epoch.queue)}`;
}

/** Every store shard in every epoch. */
export function storeShardNames(): string[] {
  return epochs().flatMap((epoch) => Array.from({ length: epoch.store }, (_, index) => `store-${tagOf(epoch)}${index}`));
}

/** The epoch whose shards are named `<prefix>-<tag><n>`, as `storeShardNames` and `indexShardNames` name them. */
function epochNamed(name: string, prefix: string): ShardEpoch {
  const epoch = epochs().find((each) => {
    const start = `${prefix}-${tagOf(each)}`;
    return name.startsWith(start) && /^\d+$/.test(name.slice(start.length));
  });
  if (epoch === undefined) throw new Error(`${name} is not a ${prefix} shard`);
  return epoch;
}

const indexCount = (epoch: ShardEpoch) => epoch.index ?? DEFAULT_INDEX_SHARDS;
const indexShardsIn = (epoch: ShardEpoch) => Array.from({ length: indexCount(epoch) }, (_, index) => `index-${tagOf(epoch)}${index}`);

/**
 * The index shard that holds a run's list row (`key` is the run ID) or one of its hooks' (`key` is
 * the hook ID): `hash(key) % index` within the run's epoch.
 */
export function indexShardFor(runId: string, key = runId): string {
  const { epoch, tag } = epochOf(runId);
  return `index-${tag}${bucket(key, indexCount(epoch))}`;
}

/** The index shards, one per epoch, that may hold hook `hookId`'s row: a hook ID does not name its run. */
export function hookIndexShards(hookId: string): string[] {
  return epochs().map((epoch) => `index-${tagOf(epoch)}${bucket(hookId, indexCount(epoch))}`);
}

/** Every index shard in every epoch. */
export function indexShardNames(): string[] {
  return epochs().flatMap(indexShardsIn);
}

/**
 * The index shards a store's runs and hooks go to: its epoch's. The legacy store's runs predate
 * sharding, and an epoch added later starts after its deploy, so they all fall in the first epoch.
 */
export function indexShardsOf(store: string): string[] {
  return indexShardsIn(store === STORE_NAME ? epochs()[0] : epochNamed(store, 'store'));
}

/**
 * The stores that feed index shard `name`: its epoch's store shards, and for the first epoch the
 * legacy store. A later epoch must not wait for it: it backfilled before that epoch existed.
 */
export function indexSourceNames(name: string): string[] {
  const epoch = epochNamed(name, 'index');
  const stores = Array.from({ length: epoch.store }, (_, index) => `store-${tagOf(epoch)}${index}`);
  return epoch === epochs()[0] ? [...stores, STORE_NAME] : stores;
}

/** How many stores feed index shard `name`. */
export function indexSourceCount(name: string): number {
  return indexSourceNames(name).length;
}

/** Whether `name` is a store that holds runs: a store shard or the legacy store, not an index shard. */
export function holdsRuns(name: string): boolean {
  return name === STORE_NAME || name.startsWith('store-');
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
