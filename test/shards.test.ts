import { describe, expect, it } from 'vitest';
import { DEFAULT_SHARD_EPOCHS } from '../packages/world-cloudflare/src/constants.js';
import {
  parseEpochs, queueShardFor, queueShardNames, storeShardFor, storeShardNames, tokenShardFor,
} from '../packages/world-cloudflare/src/lib/shards.js';

// wrangler.test.jsonc sets two epochs: { from: 0, store: 4, queue: 4 } and { from: 1700000000000, store: 8, queue: 8 }.
const SECOND = (1_700_000_000_000).toString(36);

/** A run ID whose ULID time is `ms`: 10 Crockford base32 time characters, then a fixed random part. */
function runIdAt(ms: number): string {
  const alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  let time = '';
  for (let rest = ms, index = 0; index < 10; index++, rest = Math.floor(rest / 32)) time = alphabet[rest % 32] + time;
  return `wrun_${time}${'0'.repeat(16)}`;
}

describe('shard epochs', () => {
  it('routes a run by the epoch its ULID time falls in', () => {
    const old = runIdAt(1_600_000_000_000);
    const current = runIdAt(Date.now());
    expect(storeShardFor(old)).toMatch(/^store-[0-3]$/);
    expect(storeShardFor(current)).toMatch(new RegExp(`^store-e${SECOND}-[0-7]$`));
    expect(queueShardFor('key', old)).toMatch(/^flow-[0-3]$/);
    expect(queueShardFor('key', current)).toMatch(new RegExp(`^flow-e${SECOND}-[0-7]$`));
    expect(storeShardFor('wrun_not-a-ulid')).toMatch(/^store-[0-3]$/);
    expect(storeShardFor(current)).toBe(storeShardFor(current));
  });

  it('keeps hook tokens on the first epoch', () => {
    expect(tokenShardFor(`token-${crypto.randomUUID()}`)).toMatch(/^store-[0-3]$/);
  });

  it('lists every shard of every epoch', () => {
    expect(storeShardNames()).toHaveLength(12);
    expect(queueShardNames()).toEqual(['flow', 'flow-0', 'flow-1', 'flow-2', 'flow-3', ...Array.from({ length: 8 }, (_, n) => `flow-e${SECOND}-${n}`)]);
  });

  it('parses and validates SHARD_EPOCHS', () => {
    expect(parseEpochs(undefined)).toBe(DEFAULT_SHARD_EPOCHS);
    expect(parseEpochs('[{"from":0,"store":2,"queue":3}]')).toEqual([{ from: 0, store: 2, queue: 3 }]);
    for (const bad of ['[]', '[{"from":5,"store":1,"queue":1}]', '[{"from":0,"store":0,"queue":1}]',
      '[{"from":0,"store":1,"queue":1},{"from":0,"store":2,"queue":2}]']) {
      expect(() => parseEpochs(bad), bad).toThrow();
    }
  });
});
