import type { GetChunksOptions, Streamer } from '@workflow/world';
import { DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT } from '../constants.js';
import type { WorldEnv } from '../types.js';

const encoder = new TextEncoder();
const toBytes = (chunk: string | Uint8Array) => typeof chunk === 'string' ? encoder.encode(chunk) : chunk;

/** The World's `Streamer`. Each run's streams live in that run's `RunStreams` object. */
export function createStreamer(env: Pick<WorldEnv, 'RUN_STREAMS'>): Streamer {
  const streamsOf = (runId: string) => env.RUN_STREAMS.getByName(runId);
  return {
    streams: {
      write: async (runId, name, chunk) => {
        await streamsOf(runId).write(name, toBytes(chunk));
      },
      writeMulti: async (runId, name, chunks) => {
        await streamsOf(runId).writeMulti(name, chunks.map(toBytes));
      },
      close: (runId, name) => streamsOf(runId).close(name),
      list: (runId) => streamsOf(runId).list(),
      getInfo: (runId, name) => streamsOf(runId).info(name),
      getChunks: async (runId, name, options?: GetChunksOptions) => {
        const limit = options?.limit ?? DEFAULT_PAGE_LIMIT;
        const start = options?.cursor === undefined ? 0 : Number(options.cursor);
        if (!Number.isSafeInteger(start) || start < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > MAX_PAGE_LIMIT) {
          throw new RangeError('Invalid stream cursor or limit');
        }
        return streamsOf(runId).chunks(name, start, limit);
      },
      /** A negative `startIndex` counts back from the end of the stream. */
      get: async (runId, name, startIndex = 0) => {
        const stub = streamsOf(runId);
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
