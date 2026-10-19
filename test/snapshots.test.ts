import { env, runInDurableObject } from 'cloudflare:test';
import { Buffer } from 'node:buffer';
import { describe, expect, it } from 'vitest';
import { SNAPSHOT_PART_BYTES } from '../packages/world-cloudflare/src/constants.js';
import { createSnapshots } from '../packages/world-cloudflare/src/streams/snapshots.js';

const snapshots = createSnapshots(env).experimental_snapshots;
const metadata = (eventCount: number) => ({
  eventsCursor: `eid:${eventCount}`,
  createdAt: new Date(1_700_000_000_000 + eventCount),
  eventCount,
  rngDraws: 3,
  serdeRootPtr: 0xffff_fff0,
  lastUlid: '01J0000000000000000000000',
  engineVersion: '1.2.3',
  clockMs: 1_700_000_000_123,
  formatVersion: 3,
});

function randomBytes(length: number): Uint8Array {
  const data = new Uint8Array(length);
  for (let offset = 0; offset < length; offset += 65_536) {
    crypto.getRandomValues(data.subarray(offset, Math.min(offset + 65_536, length)));
  }
  return data;
}

describe('VM snapshots', () => {
  it('round-trips bytes and metadata', async () => {
    const runId = `wrun_${crypto.randomUUID()}`;
    await snapshots.save(runId, new Uint8Array([0, 255, 7]), metadata(10));
    expect(await snapshots.load(runId)).toEqual({ data: new Uint8Array([0, 255, 7]), metadata: metadata(10) });
  });

  it('replaces the previous snapshot on save', async () => {
    const runId = `wrun_${crypto.randomUUID()}`;
    await snapshots.save(runId, randomBytes(SNAPSHOT_PART_BYTES * 2), metadata(10));
    await snapshots.save(runId, new Uint8Array([1, 2]), metadata(20));
    expect(await snapshots.load(runId)).toEqual({ data: new Uint8Array([1, 2]), metadata: metadata(20) });
    await runInDurableObject(env.RUN_STREAMS.getByName(runId), (_object, state) => {
      expect(state.storage.sql.exec('SELECT part FROM snapshot_parts').toArray()).toEqual([{ part: 0 }]);
    });
  });

  it('splits a snapshot larger than one part and reassembles it in order', async () => {
    const runId = `wrun_${crypto.randomUUID()}`;
    const data = randomBytes(5 * 1024 * 1024);
    await snapshots.save(runId, data, metadata(30));
    await runInDurableObject(env.RUN_STREAMS.getByName(runId), (_object, state) => {
      const parts = state.storage.sql.exec<{ size: number }>('SELECT length(bytes) AS size FROM snapshot_parts ORDER BY part').toArray();
      expect(parts.length).toBe(3);
      expect(Math.max(...parts.map(({ size }) => size))).toBeLessThanOrEqual(SNAPSHOT_PART_BYTES);
    });
    const loaded = await snapshots.load(runId);
    expect(loaded?.metadata).toEqual(metadata(30));
    // Buffer.compare, because a deep equal over 5 MiB of bytes takes seconds.
    expect(Buffer.compare(loaded?.data ?? new Uint8Array(), data)).toBe(0);
  });

  it('passes the 32 MiB RPC message limit', async () => {
    const runId = `wrun_${crypto.randomUUID()}`;
    const data = new Uint8Array(33 * 1024 * 1024);
    data[data.length - 1] = 9;
    await snapshots.save(runId, data, metadata(35));
    const loaded = await snapshots.load(runId);
    expect(loaded?.data.length).toBe(data.length);
    expect(loaded?.data.at(-1)).toBe(9);
  });

  it('loads null for a run without a snapshot', async () => {
    expect(await snapshots.load(`wrun_${crypto.randomUUID()}`)).toBeNull();
  });

  it('deletes idempotently', async () => {
    const runId = `wrun_${crypto.randomUUID()}`;
    await snapshots.delete(runId);
    await snapshots.save(runId, randomBytes(SNAPSHOT_PART_BYTES + 1), metadata(40));
    await snapshots.delete(runId);
    await snapshots.delete(runId);
    expect(await snapshots.load(runId)).toBeNull();
  });

  it('refuses a snapshot with missing parts', async () => {
    const runId = `wrun_${crypto.randomUUID()}`;
    await snapshots.save(runId, randomBytes(SNAPSHOT_PART_BYTES + 1), metadata(50));
    await runInDurableObject(env.RUN_STREAMS.getByName(runId), (_object, state) => {
      state.storage.sql.exec('DELETE FROM snapshot_parts WHERE part = 1');
    });
    await expect(snapshots.load(runId)).rejects.toThrow('Snapshot has');
  });
});
