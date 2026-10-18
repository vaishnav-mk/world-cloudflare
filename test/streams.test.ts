import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { createStreamer } from '../packages/world-cloudflare/src/streams.js';

const streams = createStreamer(env);
describe('run-scoped live streams', () => {
  it('isolates names and preserves binary chunk indexes', async () => {
    await streams.streams.write('run-a', 'stdout', new Uint8Array([0, 255, 1]));
    await streams.streams.write('run-b', 'stdout', 'other');
    expect((await streams.streams.getChunks('run-a', 'stdout')).data).toEqual([
      { index: 0, data: new Uint8Array([0, 255, 1]) },
    ]);
    expect((await streams.streams.getChunks('run-b', 'stdout')).data[0].data).toEqual(new TextEncoder().encode('other'));
    await streams.streams.close('run-a', 'stdout');
    expect(await streams.streams.getInfo('run-b', 'stdout')).toEqual({ tailIndex: 0, done: false });
  });

  it('writes multiple ordered chunks in one durable operation', async () => {
    const runId = crypto.randomUUID();
    await streams.streams.writeMulti!(runId, 'data', ['a', new Uint8Array([0, 255]), 'b']);
    expect(await streams.streams.getInfo(runId, 'data')).toEqual({ tailIndex: 2, done: false });
    expect((await streams.streams.getChunks(runId, 'data')).data).toEqual([
      { index: 0, data: new TextEncoder().encode('a') },
      { index: 1, data: new Uint8Array([0, 255]) },
      { index: 2, data: new TextEncoder().encode('b') },
    ]);
    await streams.streams.close(runId, 'data');
    await expect(streams.streams.writeMulti!(runId, 'data', ['late', 'write'])).rejects.toThrow('closed');
    expect((await streams.streams.getChunks(runId, 'data')).data).toHaveLength(3);
  });

  it('unblocks a reader started before its first write', async () => {
    const reader = (await streams.streams.get('run-live', 'output')).getReader();
    const pending = reader.read();
    await streams.streams.write('run-live', 'output', 'ready');
    expect(new TextDecoder().decode((await pending).value)).toBe('ready');
    await streams.streams.close('run-live', 'output');
    expect((await reader.read()).done).toBe(true);
  });
});
