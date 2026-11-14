import { DurableObject } from 'cloudflare:workers';
import { DOQB, FetchTypes, OrderTypes } from 'workers-qb';
import migrations from '../../drizzle/streams/migrations.js';
import { SNAPSHOT_PART_BYTES, STREAM_PAGE_MAX_BYTES, STREAM_POLL_MS, STREAM_READ_BATCH } from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { createPayloads, isPayloadRef } from '../lib/payloads.js';
import { allRows, findRow, requireRow } from '../lib/qb.js';
import { Waiters } from '../lib/waiters.js';
import type { WorldEnv } from '../types.js';
import type { StreamsSchema } from './tables.js';

type StreamInfo = { tailIndex: number; done: boolean };
type ChunkStream = { body: ReadableStream<Uint8Array>; next: number; done: boolean; abandoned: boolean };
type ChunkPage = {
  data: { index: number; data: Uint8Array }[];
  cursor: string | null;
  hasMore: boolean;
  done: boolean;
};

/** A chunk's `ArrayBuffer`, the BLOB type Durable Object SQLite binds; a copy only for a partial view. */
const toBuffer = (bytes: Uint8Array): ArrayBuffer =>
  bytes.buffer instanceof ArrayBuffer && bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength
    ? bytes.buffer : new Uint8Array(bytes).buffer;

/**
 * One object per run: the run's streams and its VM snapshot. Stream names and chunk positions are
 * scoped to that run.
 */
export class RunStreams extends DurableObject<WorldEnv> {
  readonly #qb: DOQB<StreamsSchema>;
  readonly #waiters = new Waiters();
  readonly #payloads: ReturnType<typeof createPayloads>;

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    this.#qb = new DOQB<StreamsSchema>(ctx.storage.sql);
    this.#payloads = createPayloads(env.PAYLOADS);
    migrateOnStart(ctx, migrations);
  }

  write(name: string, bytes: Uint8Array, leaseMs?: number): void {
    this.writeMulti(name, [bytes], leaseMs);
  }

  /** Appends chunks in order, atomically. With `leaseMs`, the writer holds the stream's lease that long. */
  writeMulti(name: string, chunks: Uint8Array[], leaseMs?: number): void {
    if (!chunks.length) return;
    this.ctx.storage.transactionSync(() => {
      const stream = this.#ensureStream(name);
      if (stream.closed) throw new Error('Stream is closed');
      let position = stream.tail;
      for (const bytes of chunks) {
        position++;
        this.#qb.insert({ tableName: 'chunks', data: { name, position, bytes: toBuffer(bytes) } }).execute();
      }
      const lease = leaseMs === undefined ? {} : { lease_until: Date.now() + leaseMs };
      this.#qb.update({ tableName: 'streams', data: { tail: position, ...lease }, where: { conditions: 'name = ?', params: [name] } }).execute();
    });
    this.#waiters.wake(name);
  }

  /**
   * Registers or extends each open stream's writer lease to `ms` from now; 0 ends it. Registration
   * can precede the first stored chunk, whose upload may still be pending when its request ends.
   */
  lease(names: string[], ms: number): void {
    if (!names.length) return;
    this.ctx.storage.transactionSync(() => {
      for (const name of names) this.#ensureStream(name);
      this.#qb.update({
        tableName: 'streams', data: { lease_until: Date.now() + ms },
        where: { conditions: `closed = 0 AND name IN (${names.map(() => '?').join(', ')})`, params: names },
      }).execute();
    });
    for (const name of names) this.#waiters.wake(name);
  }

  close(name: string): void {
    this.#ensureStream(name);
    this.#qb.update({ tableName: 'streams', data: { closed: 1 }, where: { conditions: 'name = ?', params: [name] } }).execute();
    this.#waiters.wake(name);
  }

  list(): string[] {
    const rows = allRows(this.#qb.fetchAll<'streams', 'name'>({ tableName: 'streams', fields: ['name'], orderBy: { name: OrderTypes.ASC } }));
    return rows.map(({ name }) => name);
  }

  info(name: string): StreamInfo {
    const row = findRow(this.#qb.fetchOne({
      tableName: 'streams', fields: ['tail', 'closed'], where: { conditions: 'name = ?', params: [name] },
    }));
    // A stream nobody has written to yet is empty and open.
    return row === undefined ? { tailIndex: -1, done: false } : { tailIndex: row.tail, done: Boolean(row.closed) };
  }

  /** Up to `limit` chunks from `start`, ending early at `STREAM_PAGE_MAX_BYTES`. */
  chunks(name: string, start: number, limit: number): ChunkPage {
    // Sizes first, so the chunks past the byte limit are never read.
    const sizes = allRows(this.#qb.raw<{ size: number }>({
      query: 'SELECT length(bytes) AS size FROM chunks WHERE name = ? AND position >= ? ORDER BY position LIMIT ?',
      args: [name, start, limit + 1],
      fetchType: FetchTypes.ALL,
    }));
    let count = 0;
    let bytes = 0;
    while (count < Math.min(limit, sizes.length) && (count === 0 || bytes + sizes[count].size <= STREAM_PAGE_MAX_BYTES)) {
      bytes += sizes[count].size;
      count++;
    }
    const rows = count === 0 ? [] : allRows(this.#qb.fetchAll<'chunks', 'position' | 'bytes'>({
      tableName: 'chunks', fields: ['position', 'bytes'],
      where: { conditions: ['name = ?', 'position >= ?'], params: [name, start] },
      orderBy: { position: OrderTypes.ASC }, limit: count,
    }));
    const data = rows.map(({ position, bytes }) => ({ index: position, data: new Uint8Array(bytes) }));
    const hasMore = sizes.length > count;
    const last = data.at(-1);
    return { data, cursor: hasMore && last ? String(last.index + 1) : null, hasMore, done: this.info(name).done };
  }

  /** Chunks from `start`, waiting for the next write when there are none yet. Executors on older World code read through it. */
  async read(name: string, start: number): Promise<ChunkPage> {
    const page = this.chunks(name, start, STREAM_READ_BATCH);
    if (page.data.length || page.done) return page;
    await this.#waitForChunk(name, start);
    return this.chunks(name, start, STREAM_READ_BATCH);
  }

  /**
   * Every chunk written so far from `start`, as one byte stream, after waiting up to one poll for
   * the first. `next` is the position after them, and `done` means the stream is closed, so no
   * chunk follows. `abandoned` means none follows either: there was none to send, and the writer's
   * lease has run out. Each chunk is read when the reader has taken the one before it, and a chunk
   * in R2 is piped from its object.
   */
  async stream(name: string, start: number): Promise<ChunkStream> {
    if (!this.#lapsed(name)) await this.#waitForChunk(name, start);
    const { tailIndex, done } = this.info(name);
    const next = Math.max(start, tailIndex + 1);
    const { readable, writable } = new IdentityTransformStream();
    this.ctx.waitUntil(this.#pipeChunks(name, start, next, writable));
    return { body: readable, next, done, abandoned: next === start && this.#lapsed(name) };
  }

  /** Whether the stream is open and its writer's lease has run out. */
  #lapsed(name: string): boolean {
    const row = findRow(this.#qb.fetchOne<'streams', 'closed' | 'lease_until'>({
      tableName: 'streams', fields: ['closed', 'lease_until'], where: { conditions: 'name = ?', params: [name] },
    }));
    return row !== undefined && !row.closed && row.lease_until !== null && row.lease_until <= Date.now();
  }

  async #pipeChunks(name: string, start: number, end: number, writable: WritableStream<Uint8Array>): Promise<void> {
    try {
      for (let position = start; position < end; position++) {
        const { bytes } = requireRow(this.#qb.fetchOne<'chunks', 'bytes'>({
          tableName: 'chunks', fields: ['bytes'], where: { conditions: ['name = ?', 'position = ?'], params: [name, position] },
        }));
        const chunk = new Uint8Array(bytes);
        const body = isPayloadRef(chunk) ? (await this.#payloads.open(chunk)).body : new Blob([bytes]).stream();
        await body.pipeTo(writable, { preventClose: true });
      }
      await writable.close();
    } catch (error) {
      // The reader cancelled, or a chunk could not be read: end the stream with the error.
      await writable.abort(error).catch(() => {});
    }
  }

  #waitForChunk(name: string, position: number): Promise<void> {
    return this.#waiters.wait(name, STREAM_POLL_MS, () => {
      const info = this.info(name);
      return info.tailIndex >= position || info.done;
    });
  }

  /**
   * Replaces the run's snapshot envelope. It arrives as a stream because an envelope can pass the
   * 32 MiB RPC message limit. Its parts and size are written in one transaction.
   */
  async saveSnapshot(envelope: ReadableStream<Uint8Array>): Promise<void> {
    const buffer = await new Response(envelope).arrayBuffer();
    this.ctx.storage.transactionSync(() => {
      this.#clearSnapshot();
      for (let part = 0; part * SNAPSHOT_PART_BYTES < buffer.byteLength; part++) {
        const bytes = buffer.slice(part * SNAPSHOT_PART_BYTES, (part + 1) * SNAPSHOT_PART_BYTES);
        this.#qb.insert({ tableName: 'snapshot_parts', data: { part, bytes } }).execute();
      }
      this.#qb.insert({ tableName: 'snapshot', data: { size: buffer.byteLength } }).execute();
    });
  }

  /** The run's snapshot envelope as a stream, or `null` when it has none. */
  loadSnapshot(): ReadableStream<Uint8Array> | null {
    const snapshot = findRow(this.#qb.fetchOne<'snapshot', 'size'>({ tableName: 'snapshot', fields: ['size'] }));
    if (snapshot === undefined) return null;
    const parts = allRows(this.#qb.fetchAll<'snapshot_parts', 'bytes'>({
      tableName: 'snapshot_parts', fields: ['bytes'], orderBy: { part: OrderTypes.ASC },
    }));
    const envelope = new Blob(parts.map(({ bytes }) => bytes));
    if (envelope.size !== snapshot.size) throw new Error(`Snapshot has ${envelope.size} of ${snapshot.size} bytes`);
    return envelope.stream();
  }

  deleteSnapshot(): void {
    this.ctx.storage.transactionSync(() => this.#clearSnapshot());
  }

  #clearSnapshot(): void {
    // No conditions: every row.
    this.#qb.delete({ tableName: 'snapshot_parts', where: [] }).execute();
    this.#qb.delete({ tableName: 'snapshot', where: [] }).execute();
  }

  /** Creates the stream if needed and returns its state. */
  #ensureStream(name: string): { tail: number; closed: number } {
    return requireRow(this.#qb.raw<{ tail: number; closed: number }>({
      query: 'INSERT INTO streams (name) VALUES (?) ON CONFLICT(name) DO UPDATE SET name = excluded.name RETURNING tail, closed',
      args: [name],
      fetchType: FetchTypes.ONE,
    }));
  }
}
