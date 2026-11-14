import type { GetChunksOptions, Streamer } from '@workflow/world';
import { DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, STREAM_CHUNK_MAX_BYTES, STREAM_PAGE_MAX_BYTES } from '../constants.js';
import { createPayloads, isPayloadRef, tooLarge } from '../lib/payloads.js';
import { transientAsWorldError } from '../lib/transient.js';
import type { WorldEnv } from '../types.js';

const encoder = new TextEncoder();
/** A chunk as bytes, checked against `STREAM_CHUNK_MAX_BYTES` before the World copies it anywhere. */
function toChunk(chunk: string | Uint8Array): Uint8Array {
  const bytes = typeof chunk === 'string' ? encoder.encode(chunk) : chunk;
  if (bytes.byteLength > STREAM_CHUNK_MAX_BYTES) {
    throw tooLarge(`A ${bytes.byteLength}-byte stream chunk is over the ${STREAM_CHUNK_MAX_BYTES}-byte chunk limit`);
  }
  return bytes;
}

/**
 * The World's `Streamer`. Each run's streams live in that run's `RunStreams` object. Large chunks
 * go to R2 first, and the object keeps a ref to each. `get` joins the object's streams of the
 * chunks written so far, one after another, and a chunk in R2 is piped from its object body.
 */
export function createStreamer(env: Pick<WorldEnv, 'RUN_STREAMS' | 'PAYLOADS'>): Streamer {
  const streamsOf = (runId: string) => transientAsWorldError(env.RUN_STREAMS.getByName(runId));
  const payloads = createPayloads(env.PAYLOADS);
  return {
    streams: {
      write: async (runId, name, chunk) => {
        await streamsOf(runId).write(name, await payloads.offload(runId, toChunk(chunk)));
      },
      writeMulti: async (runId, name, chunks) => {
        const checked = chunks.map(toChunk);
        await streamsOf(runId).writeMulti(name, await Promise.all(checked.map((bytes) => payloads.offload(runId, bytes))));
      },
      close: (runId, name) => streamsOf(runId).close(name),
      list: (runId) => streamsOf(runId).list(),
      getInfo: (runId, name) => streamsOf(runId).info(name),
      getChunks: async (runId, name, { limit = DEFAULT_PAGE_LIMIT, cursor = '0' }: GetChunksOptions = {}) => {
        const start = Number(cursor);
        if (!Number.isSafeInteger(start) || start < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > MAX_PAGE_LIMIT) {
          throw new RangeError('Invalid stream cursor or limit');
        }
        const page = await streamsOf(runId).chunks(name, start, limit);
        // Chunks come out of R2 one at a time, and the page ends before one that would take it past
        // STREAM_PAGE_MAX_BYTES, so a page of large chunks is never all in memory at once.
        let bytes = 0;
        for (const [position, chunk] of page.data.entries()) {
          if (isPayloadRef(chunk.data)) {
            const object = await payloads.open(chunk.data);
            if (position > 0 && bytes + object.size > STREAM_PAGE_MAX_BYTES) {
              await object.body.cancel();
              return { ...page, data: page.data.slice(0, position), cursor: String(chunk.index), hasMore: true };
            }
            chunk.data = new Uint8Array(await object.arrayBuffer());
          }
          bytes += chunk.data.byteLength;
        }
        return page;
      },
      /** A negative `startIndex` counts back from the end of the stream. */
      get: async (runId, name, startIndex = 0) => {
        const stub = streamsOf(runId);
        const start = startIndex < 0 ? Math.max(0, (await stub.info(name)).tailIndex + 1 + startIndex) : startIndex;
        if (!Number.isSafeInteger(start) || start < 0) throw new RangeError('Invalid stream start index');
        const { readable, writable } = new IdentityTransformStream();
        // Ends with the stream, or with an error once the reader cancels or a read fails.
        const follow = async () => {
          try {
            for (let next = start; ;) {
              const chunks = await stub.stream(name, next);
              await chunks.body.pipeTo(writable, { preventClose: true });
              if (chunks.done) break;
              next = chunks.next;
            }
            await writable.close();
          } catch (error) {
            await writable.abort(error).catch(() => {});
          }
        };
        void follow();
        return readable;
      },
    },
  };
}
