import { createExecutionContext, env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { PAYLOAD_INLINE_MAX_BYTES, STREAM_CHUNK_MAX_BYTES, STREAM_PAGE_MAX_BYTES } from '../packages/world-cloudflare/src/constants.js';
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
    // No piece is a whole chunk from R2.
    expect(Math.max(...read.map(({ byteLength }) => byteLength))).toBeLessThan(2 * MiB);
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

describe('abandoned streams', () => {
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
