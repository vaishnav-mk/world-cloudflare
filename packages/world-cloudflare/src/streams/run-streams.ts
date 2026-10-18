import { DurableObject } from 'cloudflare:workers';
import { STREAM_POLL_MS, STREAM_READ_BATCH } from '../constants.js';
import { Waiters } from '../lib/waiters.js';
import type { WorldEnv } from '../types.js';

type StreamRow = { tail: number; closed: number };
type ChunkRow = { position: number; bytes: ArrayBuffer };

export type StreamInfo = { tailIndex: number; done: boolean };
export type ChunkPage = {
  data: { index: number; data: Uint8Array }[];
  cursor: string | null;
  hasMore: boolean;
  done: boolean;
};

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS streams (
    name TEXT PRIMARY KEY, tail INTEGER NOT NULL DEFAULT -1, closed INTEGER NOT NULL DEFAULT 0);
  CREATE TABLE IF NOT EXISTS chunks (
    name TEXT NOT NULL, position INTEGER NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY (name, position));
`;

/** One object per run. Stream names and chunk positions are scoped to that run. */
export class RunStreams extends DurableObject<WorldEnv> {
  readonly #waiters = new Waiters();

  constructor(ctx: DurableObjectState, env: WorldEnv) {
    super(ctx, env);
    ctx.storage.sql.exec(SCHEMA);
  }

  write(name: string, bytes: Uint8Array): void {
    this.writeMulti(name, [bytes]);
  }

  /** Appends chunks in order, atomically. */
  writeMulti(name: string, chunks: Uint8Array[]): void {
    if (!chunks.length) return;
    const sql = this.ctx.storage.sql;
    this.ctx.storage.transactionSync(() => {
      sql.exec('INSERT OR IGNORE INTO streams (name) VALUES (?)', name);
      const stream = sql.exec<StreamRow>('SELECT tail, closed FROM streams WHERE name = ?', name).one();
      if (stream.closed) throw new Error('Stream is closed');
      let position = stream.tail;
      for (const bytes of chunks) {
        position++;
        sql.exec('INSERT INTO chunks (name, position, bytes) VALUES (?, ?, ?)', name, position, bytes);
      }
      sql.exec('UPDATE streams SET tail = ? WHERE name = ?', position, name);
    });
    this.#waiters.wake(name);
  }

  close(name: string): void {
    const sql = this.ctx.storage.sql;
    sql.exec('INSERT OR IGNORE INTO streams (name) VALUES (?)', name);
    sql.exec('UPDATE streams SET closed = 1 WHERE name = ?', name);
    this.#waiters.wake(name);
  }

  list(): string[] {
    return this.ctx.storage.sql.exec<{ name: string }>('SELECT name FROM streams ORDER BY name').toArray()
      .map(({ name }) => name);
  }

  info(name: string): StreamInfo {
    const row = this.ctx.storage.sql.exec<StreamRow>('SELECT tail, closed FROM streams WHERE name = ?', name).toArray()[0];
    return { tailIndex: row?.tail ?? -1, done: Boolean(row?.closed) };
  }

  chunks(name: string, start: number, limit: number): ChunkPage {
    const rows = this.ctx.storage.sql.exec<ChunkRow>(
      'SELECT position, bytes FROM chunks WHERE name = ? AND position >= ? ORDER BY position LIMIT ?',
      name, start, limit + 1,
    ).toArray();
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
}
