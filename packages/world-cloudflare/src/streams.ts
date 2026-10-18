import { DurableObject } from 'cloudflare:workers';
import type { GetChunksOptions, Streamer } from '@workflow/world';

type Env = { RUN_STREAMS: DurableObjectNamespace<RunStreams> };
type StreamRow = { tail: number; closed: number };
type ChunkRow = { position: number; bytes: ArrayBuffer };

/** One object per run. Names and chunk indexes are scoped to that run. */
export class RunStreams extends DurableObject<Env> {
  private readonly waiting = new Map<string, Set<() => void>>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS streams (
      name TEXT PRIMARY KEY, tail INTEGER NOT NULL DEFAULT -1, closed INTEGER NOT NULL DEFAULT 0
    ); CREATE TABLE IF NOT EXISTS chunks (
      name TEXT NOT NULL, position INTEGER NOT NULL, bytes BLOB NOT NULL,
      PRIMARY KEY (name, position)
    );`);
  }

  write(name: string, bytes: Uint8Array): void {
    this.writeMulti(name, [bytes]);
  }

  writeMulti(name: string, chunks: Uint8Array[]): void {
    if (!chunks.length) return;
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec('INSERT OR IGNORE INTO streams (name) VALUES (?)', name);
      const stream = this.ctx.storage.sql.exec<StreamRow>('SELECT tail, closed FROM streams WHERE name = ?', name).one();
      if (stream.closed) throw new Error('Stream is closed');
      let next = stream.tail;
      for (const bytes of chunks) {
        next++;
        this.ctx.storage.sql.exec('INSERT INTO chunks (name, position, bytes) VALUES (?, ?, ?)', name, next, bytes);
      }
      this.ctx.storage.sql.exec('UPDATE streams SET tail = ? WHERE name = ?', next, name);
    });
    this.notify(name);
  }

  close(name: string): void {
    this.ctx.storage.sql.exec('INSERT OR IGNORE INTO streams (name) VALUES (?)', name);
    this.ctx.storage.sql.exec('UPDATE streams SET closed = 1 WHERE name = ?', name);
    this.notify(name);
  }

  list(): string[] {
    return this.ctx.storage.sql.exec<{ name: string }>('SELECT name FROM streams ORDER BY name').toArray().map(({ name }) => name);
  }

  info(name: string): { tailIndex: number; done: boolean } {
    const row = this.ctx.storage.sql.exec<StreamRow>('SELECT tail, closed FROM streams WHERE name = ?', name).toArray()[0];
    return { tailIndex: row?.tail ?? -1, done: Boolean(row?.closed) };
  }

  chunks(name: string, start: number, limit: number) {
    const rows = this.ctx.storage.sql.exec<ChunkRow>(
      'SELECT position, bytes FROM chunks WHERE name = ? AND position >= ? ORDER BY position LIMIT ?',
      name, start, limit + 1,
    ).toArray();
    const data = rows.slice(0, limit).map(({ position, bytes }) => ({ index: position, data: new Uint8Array(bytes) }));
    const hasMore = rows.length > limit;
    return { data, cursor: hasMore ? String(data.at(-1)!.index + 1) : null, hasMore, done: this.info(name).done };
  }

  async read(name: string, start: number) {
    let result = this.chunks(name, start, 100);
    if (result.data.length || result.done) return result;
    await new Promise<void>((resolve) => {
      const waiters = this.waiting.get(name) ?? new Set<() => void>();
      this.waiting.set(name, waiters);
      const timer = setTimeout(() => { waiters.delete(wake); resolve(); }, 15_000);
      const wake = () => { clearTimeout(timer); waiters.delete(wake); resolve(); };
      waiters.add(wake);
      // A write could land between the initial read and registering the waiter.
      if (this.info(name).tailIndex >= start || this.info(name).done) wake();
    });
    result = this.chunks(name, start, 100);
    return result;
  }

  private notify(name: string): void {
    for (const wake of this.waiting.get(name) ?? []) wake();
  }
}

export function createStreamer(env: Env): Streamer {
  const get = (runId: string) => env.RUN_STREAMS.getByName(runId);
  return {
    streams: {
      write: async (runId, name, chunk) => {
        await get(runId).write(name, typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk);
      },
      writeMulti: async (runId, name, chunks) => {
        await get(runId).writeMulti(name, chunks.map((chunk) =>
          typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk));
      },
      close: (runId, name) => get(runId).close(name),
      list: (runId) => get(runId).list(),
      getChunks: async (runId, name, options?: GetChunksOptions) => {
        const limit = options?.limit ?? 100;
        const start = options?.cursor === undefined ? 0 : Number(options.cursor);
        if (!Number.isSafeInteger(start) || start < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
          throw new RangeError('Invalid stream cursor or limit');
        }
        return get(runId).chunks(name, start, limit);
      },
      getInfo: (runId, name) => get(runId).info(name),
      get: async (runId, name, startIndex = 0) => {
        const stub = get(runId);
        const start = startIndex < 0 ? Math.max(0, (await stub.info(name)).tailIndex + 1 + startIndex) : startIndex;
        if (!Number.isSafeInteger(start) || start < 0) throw new RangeError('Invalid stream start index');
        let next = start;
        return new ReadableStream<Uint8Array>({
          async pull(controller) {
            const page = await stub.read(name, next);
            for (const chunk of page.data) {
              controller.enqueue(chunk.data);
              next = chunk.index + 1;
            }
            if (page.done && !page.hasMore && page.data.length === 0) controller.close();
          },
        });
      },
    },
  };
}
