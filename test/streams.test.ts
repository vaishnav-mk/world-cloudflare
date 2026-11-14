import { createExecutionContext, env } from 'cloudflare:test';
import { describe, expect, it, vi } from 'vitest';
import {
  PAYLOAD_INLINE_MAX_BYTES, STREAM_CHUNK_MAX_BYTES, STREAM_PAGE_MAX_BYTES, STREAM_READ_PIECE_BYTES,
} from '../packages/world-cloudflare/src/constants.js';
import { currentRequest, withRequestContext } from '../packages/world-cloudflare/src/lib/request-context.js';
import { createStreamer } from '../packages/world-cloudflare/src/streams/streamer.js';

const streams = createStreamer(env);
const MiB = 1024 * 1024;

/** Bytes no shifted or repeated piece of another call matches. */
function pattern(length: number, seed: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(length);
  for (let index = 0; index < length; index++) bytes[index] = Math.imul(index + seed * length, 0x9e3779b1) >>> 24;
  return bytes;
}
const concat = (parts: Uint8Array[]) => Buffer.concat(parts);
/** Compares without handing vitest megabytes to diff. */
const same = (actual: Uint8Array, expected: Uint8Array) => Buffer.from(actual).equals(Buffer.from(expected));
const objects = async (runId: string) => (await env.PAYLOADS.list({ prefix: `runs/${runId}/` })).objects;

const CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
/** A name like the one the SDK gives a stream a step returns, which takes a writer lease. */
const stepStream = () => `strm_${Array.from(crypto.getRandomValues(new Uint8Array(26)), (byte) => CROCKFORD[byte % 32]).join('')}`;
const text = (bytes: Uint8Array | undefined) => new TextDecoder().decode(bytes);
const ok = () => new Response('ok');

async function pieces(stream: ReadableStream<Uint8Array>): Promise<Uint8Array[]> {
  const reader = stream.getReader();
  const read: Uint8Array[] = [];
  for (let result = await reader.read(); !result.done; result = await reader.read()) read.push(result.value);
  return read;
}

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

describe('large stream chunks', () => {
  it('pipes chunks in R2 to a live reader in pieces', async () => {
    const runId = crypto.randomUUID();
    const reader = (await streams.streams.get(runId, 'big')).getReader();
    const first = reader.read();
    const written = [pattern(3 * MiB + 5, 1), pattern(10, 2), pattern(2 * MiB, 3)];
    await streams.streams.write(runId, 'big', written[0]);
    await streams.streams.writeMulti!(runId, 'big', written.slice(1));
    await streams.streams.close(runId, 'big');
    const read: Uint8Array[] = [];
    for (let result = await first; !result.done; result = await reader.read()) read.push(result.value);
    expect(same(concat(read), concat(written))).toBe(true);
    // No piece is a whole chunk from R2, and a short one ends the chunks written so far.
    expect(Math.max(...read.map(({ byteLength }) => byteLength))).toBe(STREAM_READ_PIECE_BYTES);
    expect(read.filter(({ byteLength }) => byteLength < STREAM_READ_PIECE_BYTES).length).toBeLessThanOrEqual(2);
    expect(await objects(runId)).toHaveLength(2);
    // A negative start counts back from the end.
    expect(same(concat(await pieces(await streams.streams.get(runId, 'big', -2))), concat(written.slice(1)))).toBe(true);
  });

  it('keeps a page of inline chunks under the RPC message limit', async () => {
    const runId = crypto.randomUUID();
    const written = Array.from({ length: 40 }, (_, index) => pattern(PAYLOAD_INLINE_MAX_BYTES, index));
    for (const chunk of written) await streams.streams.write(runId, 'inline', chunk);
    await streams.streams.close(runId, 'inline');
    expect(await objects(runId)).toHaveLength(0);

    const pages: number[][] = [];
    const chunks: Uint8Array[] = [];
    for (let cursor: string | null = '0'; cursor !== null;) {
      const page = await streams.streams.getChunks(runId, 'inline', { cursor, limit: 1000 });
      pages.push(page.data.map(({ index }) => index));
      chunks.push(...page.data.map(({ data }) => data));
      cursor = page.cursor;
    }
    const perPage = STREAM_PAGE_MAX_BYTES / PAYLOAD_INLINE_MAX_BYTES;
    expect(pages).toHaveLength(40 / perPage);
    expect(pages[1]).toEqual(Array.from({ length: perPage }, (_, index) => perPage + index));
    expect(same(concat(chunks), concat(written))).toBe(true);
    expect(same(concat(await pieces(await streams.streams.get(runId, 'inline'))), concat(written))).toBe(true);
    // Executors on older World code read the stream through `read`.
    const page = await env.RUN_STREAMS.getByName(runId).read('inline', 0);
    expect(page.data).toHaveLength(perPage);
    expect(page).toMatchObject({ hasMore: true, cursor: String(perPage) });
  });

  it('ends a page before a chunk in R2 that would take it over the byte limit', async () => {
    const runId = crypto.randomUUID();
    const written = [pattern(3 * MiB, 1), pattern(2 * MiB, 2), pattern(10, 3)];
    await streams.streams.writeMulti!(runId, 'paged', written);
    const first = await streams.streams.getChunks(runId, 'paged');
    expect(first.data.map(({ index }) => index)).toEqual([0]);
    expect(first).toMatchObject({ hasMore: true, cursor: '1', done: false });
    const second = await streams.streams.getChunks(runId, 'paged', { cursor: '1' });
    expect(second.data.map(({ index }) => index)).toEqual([1, 2]);
    expect(same(concat([...first.data, ...second.data].map(({ data }) => data)), concat(written))).toBe(true);
  });

  it('stops reading R2 when the reader cancels mid-chunk', async () => {
    const runId = crypto.randomUUID();
    const chunk = pattern(5 * MiB, 1);
    await streams.streams.write(runId, 'cancelled', chunk);
    await streams.streams.close(runId, 'cancelled');
    const reader = (await streams.streams.get(runId, 'cancelled')).getReader();
    expect((await reader.read()).value?.byteLength).toBeLessThan(chunk.byteLength);
    await reader.cancel();
    expect(same(concat(await pieces(await streams.streams.get(runId, 'cancelled'))), chunk)).toBe(true);
  });

  it('rejects a chunk over the chunk limit before storing anything', async () => {
    const runId = crypto.randomUUID();
    await expect(streams.streams.write(runId, 'huge', new Uint8Array(STREAM_CHUNK_MAX_BYTES + 1)))
      .rejects.toMatchObject({ status: 413, fatal: true });
    await expect(streams.streams.writeMulti!(runId, 'huge', ['small', new Uint8Array(STREAM_CHUNK_MAX_BYTES + 1)]))
      .rejects.toMatchObject({ status: 413 });
    expect(await streams.streams.getInfo(runId, 'huge')).toEqual({ tailIndex: -1, done: false });
    expect(await objects(runId)).toHaveLength(0);
  });

  it('sends a small view of a large buffer without the rest of the buffer', async () => {
    // RPC would carry the whole 40 MiB buffer, over its 32 MiB message limit.
    const view = pattern(40 * MiB, 1).subarray(100, 116);
    const runId = crypto.randomUUID();
    await streams.streams.write(runId, 'view', view);
    expect((await streams.streams.getChunks(runId, 'view')).data).toEqual([{ index: 0, data: new Uint8Array(view) }]);
  });

  it('errors a read whose chunk is missing from R2', async () => {
    const runId = crypto.randomUUID();
    await streams.streams.write(runId, 'lost', pattern(2 * MiB, 1));
    await streams.streams.close(runId, 'lost');
    await env.PAYLOADS.delete((await objects(runId)).map(({ key }) => key));
    await expect(pieces(await streams.streams.get(runId, 'lost'))).rejects.toThrow();
  });
});

describe('stream memory', () => {
  it('holds at most two chunks writing and two pieces reading many more chunks than fit in memory', async () => {
    const runId = crypto.randomUUID();
    // 512 MiB, four times the 128 MB isolate limit.
    const count = 32;
    let tracked: WeakRef<ArrayBufferLike>[] = [];
    let most = 0;
    // Counts the buffers still held after a full collection; vitest.config.ts starts workerd with `--expose-gc`.
    const settle = async () => {
      if (gc === undefined) throw new Error('gc() needs --expose-gc');
      await scheduler.wait(1);
      gc();
      tracked = tracked.filter((buffer) => buffer.deref() !== undefined);
      most = Math.max(most, tracked.length);
    };

    for (let index = 0; index < count; index++) {
      const chunk = new Uint8Array(STREAM_CHUNK_MAX_BYTES).fill(index);
      tracked.push(new WeakRef(chunk.buffer));
      await streams.streams.write(runId, 'memory', chunk);
      await settle();
    }
    await streams.streams.close(runId, 'memory');
    // The chunk this loop is writing, and at times one the runtime has yet to let go of.
    expect(most).toBeLessThanOrEqual(2);

    tracked = [];
    most = 0;
    const startedAt = Date.now();
    const reader = (await streams.streams.get(runId, 'memory')).getReader();
    let bytes = 0;
    for (let read = await reader.read(); !read.done; read = await reader.read()) {
      const index = Math.floor(bytes / STREAM_CHUNK_MAX_BYTES);
      expect([read.value.byteLength, read.value[0], read.value.at(-1)]).toEqual([STREAM_READ_PIECE_BYTES, index, index]);
      tracked.push(new WeakRef(read.value.buffer));
      bytes += read.value.byteLength;
      if (bytes % (64 * MiB) === 0) await settle();
    }
    const seconds = (Date.now() - startedAt) / 1000;
    expect(bytes).toBe(count * STREAM_CHUNK_MAX_BYTES);
    // The piece the loop holds and the one the stream has ready.
    expect(most).toBeLessThanOrEqual(2);
    // A sanity floor: 4 KiB reads made 610 MiB/s locally and 1 MiB pieces 3,200, collections aside.
    expect(bytes / MiB / seconds).toBeGreaterThan(100);
  }, 120_000);
});

describe('abandoned streams', () => {
  it('expires a registered writer even before its first chunk is stored', async () => {
    const runId = crypto.randomUUID();
    const name = stepStream();
    const object = env.RUN_STREAMS.getByName(runId);
    await object.lease([name], 0);
    expect(await object.list()).toEqual([name]);
    const reader = (await streams.streams.get(runId, name)).getReader();
    await expect(reader.read()).rejects.toThrow('ended without being closed');
    expect(await object.info(name)).toEqual({ tailIndex: -1, done: false });
  });

  it('ends a reader once the request writing the stream ends with that work cut off', async () => {
    const runId = crypto.randomUUID();
    const name = stepStream();
    const reader = (await streams.streams.get(runId, name)).getReader();
    await withRequestContext(createExecutionContext(), { FLOW_BACKGROUND_BUDGET_MS: '100' }, async () => {
      await streams.streams.write(runId, name, 'first');
      // The rest of the stream is still being written when the budget runs out.
      currentRequest()?.waitUntil(new Promise(() => {}));
      return ok();
    });
    expect(text((await reader.read()).value)).toBe('first');
    await expect(reader.read()).rejects.toThrow('ended without being closed');
    expect(await streams.streams.getInfo(runId, name)).toEqual({ tailIndex: 0, done: false });
  });

  it('refuses a write from a request that has ended', async () => {
    const runId = crypto.randomUUID();
    const name = stepStream();
    let late = Promise.resolve();
    await withRequestContext(createExecutionContext(), { FLOW_BACKGROUND_BUDGET_MS: '50' }, async () => {
      late = scheduler.wait(200).then(() => streams.streams.write(runId, name, 'late'));
      currentRequest()?.waitUntil(late.catch(() => {}));
      return ok();
    });
    await expect(late).rejects.toThrow('after its flow request ended');
    expect(await streams.streams.getInfo(runId, name)).toEqual({ tailIndex: -1, done: false });
  });

  it.each(['write', 'writeMulti'] as const)('refuses %s after an R2 upload outlives its request', async (method) => {
    const runId = crypto.randomUUID();
    const name = stepStream();
    let uploaded = () => {};
    let release = () => {};
    const upload = new Promise<void>((resolve) => { uploaded = resolve; });
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const put = env.PAYLOADS.put.bind(env.PAYLOADS);
    using uploads = vi.spyOn(env.PAYLOADS, 'put').mockImplementation(async (...args) => {
      uploaded();
      await gate;
      return put(...args);
    });
    let late = Promise.resolve();
    try {
      await withRequestContext(createExecutionContext(), { FLOW_BACKGROUND_BUDGET_MS: '100' }, async () => {
        await streams.streams.write(runId, name, 'first');
        const chunk = new Uint8Array(PAYLOAD_INLINE_MAX_BYTES + 1);
        if (method === 'write') late = streams.streams.write(runId, name, chunk);
        else {
          const writeMulti = streams.streams.writeMulti;
          if (writeMulti === undefined) throw new Error('writeMulti is required');
          late = writeMulti(runId, name, ['inline', chunk]);
        }
        currentRequest()?.waitUntil(late.catch(() => {}));
        await upload;
        return ok();
      });
      expect(uploads).toHaveBeenCalledTimes(1);
      expect(await streams.streams.getInfo(runId, name)).toEqual({ tailIndex: 0, done: false });
      release();
      await expect(late).rejects.toThrow('after its flow request ended');
      expect((await streams.streams.getChunks(runId, name)).data).toEqual([
        { index: 0, data: new TextEncoder().encode('first') },
      ]);
      // The upload must not renew the abandoned writer's lease either.
      const reader = (await streams.streams.get(runId, name, 1)).getReader();
      await expect(reader.read()).rejects.toThrow('ended without being closed');
    } finally {
      release();
      await late.catch(() => {});
    }
  });

  it('ends a reader once the writer lease runs out unrenewed, as when the writer is killed', async () => {
    const runId = crypto.randomUUID();
    await env.RUN_STREAMS.getByName(runId).write('killed', new TextEncoder().encode('only'), 50);
    await scheduler.wait(100);
    const reader = (await streams.streams.get(runId, 'killed')).getReader();
    expect(text((await reader.read()).value)).toBe('only');
    await expect(reader.read()).rejects.toThrow('ended without being closed');
  });

  it('keeps a reader waiting while the lease of a stream its request finished writing lasts', async () => {
    const runId = crypto.randomUUID();
    const name = stepStream();
    await withRequestContext(createExecutionContext(), {}, async () => {
      await streams.streams.write(runId, name, 'a');
      return ok();
    });
    const reader = (await streams.streams.get(runId, name)).getReader();
    expect(text((await reader.read()).value)).toBe('a');
    const next = reader.read();
    expect(await Promise.race([next.then(() => 'ended', () => 'ended'), scheduler.wait(200).then(() => 'waiting')])).toBe('waiting');
    await streams.streams.close(runId, name);
    expect((await next).done).toBe(true);
  });
});
