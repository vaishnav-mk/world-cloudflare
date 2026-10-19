import { DurableObject } from 'cloudflare:workers';
import { DOQB, OrderTypes } from 'workers-qb';
import migrations from '../../drizzle/streams/migrations.js';
import { STREAM_POLL_MS, STREAM_READ_BATCH } from '../constants.js';
import { migrateOnStart } from '../db/migrate.js';
import { Waiters } from '../lib/waiters.js';
import type { WorldEnv } from '../types.js';
import type { StreamsSchema } from './tables.js';

export type StreamInfo = { tailIndex: number; done: boolean };
export type ChunkPage = {
  data: { index: number; data: Uint8Array }[];
  cursor: string | null;
  hasMore: boolean;
  done: boolean;
};

/** Copies a view into its own `ArrayBuffer`, the BLOB type Durable Object SQLite binds. */
const toBuffer = (bytes: Uint8Array): ArrayBuffer => new Uint8Array(bytes).buffer;

/** One object per run. Stream names and chunk positions are scoped to that run. */
export class RunStreams extends DurableObject<WorldEnv> {
  readonly #qb: DOQB<StreamsSchema>;
  readonly #waiters = new Waiters();

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    this.#qb = new DOQB<StreamsSchema>(ctx.storage.sql);
    migrateOnStart(ctx, migrations);
  }

  write(name: string, bytes: Uint8Array): void {
    this.writeMulti(name, [bytes]);
  }

  /** Appends chunks in order, atomically. */
  writeMulti(name: string, chunks: Uint8Array[]): void {
    if (!chunks.length) return;
    this.ctx.storage.transactionSync(() => {
      this.#ensureStream(name);
      const stream = this.#qb.fetchOne({
        tableName: 'streams', fields: ['tail', 'closed'], where: { conditions: 'name = ?', params: [name] },
      }).execute().results!;
      if (stream.closed) throw new Error('Stream is closed');
      let position = stream.tail;
      for (const bytes of chunks) {
        position++;
        this.#qb.insert({ tableName: 'chunks', data: { name, position, bytes: toBuffer(bytes) } }).execute();
      }
      this.#qb.update({ tableName: 'streams', data: { tail: position }, where: { conditions: 'name = ?', params: [name] } }).execute();
    });
    this.#waiters.wake(name);
  }

  close(name: string): void {
    this.#ensureStream(name);
    this.#qb.update({ tableName: 'streams', data: { closed: 1 }, where: { conditions: 'name = ?', params: [name] } }).execute();
    this.#waiters.wake(name);
  }

  list(): string[] {
    const rows = this.#qb.fetchAll<'streams', 'name'>({ tableName: 'streams', fields: ['name'], orderBy: { name: OrderTypes.ASC } }).execute().results;
    return (rows ?? []).map(({ name }) => name);
  }

  info(name: string): StreamInfo {
    const row = this.#qb.fetchOne({
      tableName: 'streams', fields: ['tail', 'closed'], where: { conditions: 'name = ?', params: [name] },
    }).execute().results;
    return { tailIndex: row?.tail ?? -1, done: Boolean(row?.closed) };
  }

  chunks(name: string, start: number, limit: number): ChunkPage {
    const rows = this.#qb.fetchAll<'chunks', 'position' | 'bytes'>({
      tableName: 'chunks', fields: ['position', 'bytes'],
      where: { conditions: ['name = ?', 'position >= ?'], params: [name, start] },
      orderBy: { position: OrderTypes.ASC }, limit: limit + 1,
    }).execute().results ?? [];
    const data = rows.slice(0, limit).map(({ position, bytes }) => ({ index: position, data: new Uint8Array(bytes) }));
    const hasMore = rows.length > limit;
    const last = data.at(-1);
    return { data, cursor: hasMore && last ? String(last.index + 1) : null, hasMore, done: this.info(name).done };
  }

  /** Chunks from `start`, waiting for the next write when there are none yet. */
  async read(name: string, start: number): Promise<ChunkPage> {
    const page = this.chunks(name, start, STREAM_READ_BATCH);
    if (page.data.length || page.done) return page;
    await this.#waiters.wait(name, STREAM_POLL_MS, () => {
      const info = this.info(name);
      return info.tailIndex >= start || info.done;
    });
    return this.chunks(name, start, STREAM_READ_BATCH);
  }

  #ensureStream(name: string): void {
    this.#qb.insert({ tableName: 'streams', data: { name }, onConflict: 'IGNORE' }).execute();
  }
}
