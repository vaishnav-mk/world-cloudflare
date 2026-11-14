import { StreamError } from '@workflow/errors';
import type { GetChunksOptions, Streamer } from '@workflow/world';
import {
  DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, STREAM_CHUNK_MAX_BYTES, STREAM_PAGE_MAX_BYTES, STREAM_READ_PIECE_BYTES,
  STREAM_WRITER_LEASE_MS,
} from '../constants.js';
import { createPayloads, isPayloadRef, tooLarge } from '../lib/payloads.js';
import { currentRequest, type FlowRequest } from '../lib/request-context.js';
import { transientAsWorldError } from '../lib/transient.js';
import type { WorldEnv } from '../types.js';

const encoder = new TextEncoder();
/**
 * The names the SDK gives the streams it passes across a step boundary, such as one a step returns.
 * Only the flow request running that step writes one, so once that request is gone no chunk
 * follows. A stream `getWritable()` names for the run takes writes from later steps, so it gets no
 * writer lease and its readers wait for it as before.
 */
const STEP_STREAM_NAME = /^strm_[0-9A-HJKMNP-TV-Z]{26}$/;
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
 * go to R2 first, and the object keeps a ref to each. `get` reads the object's streams of the
 * chunks written so far, one after another, in `STREAM_READ_PIECE_BYTES` pieces, and a chunk in R2
 * is piped from its object body.
 */
export function createStreamer(env: Pick<WorldEnv, 'RUN_STREAMS' | 'PAYLOADS'>): Streamer {
  const streamsOf = (runId: string) => transientAsWorldError(env.RUN_STREAMS.getByName(runId));
  const payloads = createPayloads(env.PAYLOADS);
  /** The step streams each flow request writes and has not closed, by run. */
  const writing = new WeakMap<FlowRequest, Map<string, Set<string>>>();

  /**
   * The writer lease a write to a step stream takes, held by the flow request writing it: renewed
   * while the request runs, and ended at once if the request ends with that work cut off. A write
   * outside a flow request, or to another stream, takes none.
   */
  function lease(runId: string, name: string): number | undefined {
    const request = currentRequest();
    if (request === undefined || !STEP_STREAM_NAME.test(name)) return undefined;
    // The request's work is being cut off; a late chunk would only revive a stream it abandoned.
    if (request.signal.aborted) throw new StreamError(`Stream ${name} was written after its flow request ended`);
    let runs = writing.get(request);
    if (runs === undefined) {
      const held = new Map<string, Set<string>>();
      const renew = (ms: number) => Promise.all([...held].filter(([, names]) => names.size > 0)
        .map(([heldRunId, names]) => streamsOf(heldRunId).lease([...names], ms)));
      // shortcut: a failed renewal waits for the next; the lease lapses only after three fail in a row.
      const timer = setInterval(() => void renew(STREAM_WRITER_LEASE_MS).catch(() => {}), STREAM_WRITER_LEASE_MS / 3);
      request.onEnd.push(async (cut) => {
        clearInterval(timer);
        // Cut-off work never finishes these streams, so their readers need not wait out the lease.
        if (cut) await renew(0);
      });
      writing.set(request, held);
      runs = held;
    }
    const names = runs.get(runId) ?? new Set<string>();
    names.add(name);
    runs.set(runId, names);
    return STREAM_WRITER_LEASE_MS;
  }

  return {
    streams: {
      write: async (runId, name, chunk) => {
        const bytes = toChunk(chunk);
        const leaseMs = lease(runId, name);
        await streamsOf(runId).write(name, await payloads.offload(runId, bytes), leaseMs);
      },
      writeMulti: async (runId, name, chunks) => {
        const checked = chunks.map(toChunk);
        const leaseMs = lease(runId, name);
        await streamsOf(runId).writeMulti(name, await Promise.all(checked.map((bytes) => payloads.offload(runId, bytes))), leaseMs);
      },
      close: (runId, name) => {
        const request = currentRequest();
        if (request !== undefined) writing.get(request)?.get(runId)?.delete(name);
        return streamsOf(runId).close(name);
      },
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
        let next = start;
        let cancelled = false;
        // The object's stream of the chunks written so far from `next`, while it is being read.
        let chunks: { reader: ReadableStreamBYOBReader; next: number; done: boolean; abandoned: boolean } | undefined;
        return new ReadableStream<Uint8Array>({
          async pull(controller) {
            for (;;) {
              if (chunks === undefined) {
                const { body, ...rest } = await stub.stream(name, next);
                if (cancelled) return body.cancel();
                chunks = { reader: body.getReader({ mode: 'byob' }), ...rest };
              }
              const piece = await chunks.reader.read(new Uint8Array(STREAM_READ_PIECE_BYTES), { min: STREAM_READ_PIECE_BYTES });
              if (!piece.done) {
                // A short piece ends the chunks written so far; copied, so it does not hold a whole buffer.
                controller.enqueue(piece.value.byteLength === STREAM_READ_PIECE_BYTES ? piece.value : piece.value.slice());
                return;
              }
              if (chunks.done) return controller.close();
              if (chunks.abandoned) throw new StreamError(`Stream ${name} ended without being closed: its writer stopped`);
              next = chunks.next;
              chunks = undefined;
            }
          },
          // Stops the object's reads at the next chunk.
          cancel: (reason) => {
            cancelled = true;
            return chunks?.reader.cancel(reason);
          },
        });
      },
    },
  };
}
