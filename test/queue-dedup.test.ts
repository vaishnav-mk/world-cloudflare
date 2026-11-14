import { env, runInDurableObject } from 'cloudflare:test';
import { stringify } from 'devalue';
import { expect, it } from 'vitest';
import { ClaimStatus } from '../packages/world-cloudflare/src/enums.js';
import { DedupTable } from '../packages/world-cloudflare/src/queue/dedup.js';

it('keeps pending bytes for replay but drops accepted bytes without losing the key or ID', async () => {
  await runInDurableObject(env.QUEUE_SHARD.getByName(`dedup-bytes-${crypto.randomUUID()}`), (_shard, state) => {
    const dedup = new DedupTable(state.storage.sql);
    const first = { id: 'first', queueName: 'workflow-test', encoded: stringify({ runId: 'original' }), delayMs: 1_000, targetDeployment: 'A' };
    const retry = { ...first, id: 'retry', encoded: stringify({ runId: 'changed' }), delayMs: 0, targetDeployment: 'B' };
    expect(dedup.claim('key', first, 'original-digest')).toEqual({ status: ClaimStatus.New });
    expect(dedup.claim('key', retry, 'changed-digest')).toEqual({ status: ClaimStatus.Replay, message: first });
    dedup.markAccepted('key');
    expect(state.storage.sql.exec('SELECT key, id, accepted, encoded FROM queue_dedup WHERE key = ?', 'key').one())
      .toEqual({ key: 'key', id: 'first', accepted: 1, encoded: null });
    expect(dedup.claim('key', retry, 'changed-digest')).toEqual({ status: ClaimStatus.Accepted, id: 'first' });
  });
});
