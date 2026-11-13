import { createHook, getWritable, sleep } from 'workflow';

/**
 * Probes for measuring the World's limits. Each takes numbers from `POST /runs`. Payload sizes use
 * random bytes so compression cannot shrink them; byte streams are stored as written, so
 * `streamBytes` uses a pattern it can check. Every input is capped, because the example API is
 * public.
 */

const MiB = 1024 * 1024;
const MAX_BYTES = 128 * MiB;
/** The pattern's byte index must fit in 32 bits. */
const MAX_STREAM_MIB = 4096;
const MAX_CHUNK_MIB = 1024;
const MAX_COUNT = 30_000;
const MAX_MEGA_ITERATIONS = 20_000;
const MAX_SLEEP_HOURS = 24 * 400;

const clamp = (value: number, max: number) => Math.max(0, Math.min(Math.floor(value), max));

/** A step returns `bytes` random bytes; the run returns only their length. */
export async function stepOutput(bytes: number) {
  'use workflow';
  const data = await randomBytes(clamp(bytes, MAX_BYTES));
  return data.length;
}

/** A step returns `bytes` random bytes, and a second step takes them as input. */
export async function stepInput(bytes: number) {
  'use workflow';
  const data = await randomBytes(clamp(bytes, MAX_BYTES));
  return byteLength(data);
}

/** The run's own result is `bytes` random bytes. */
export async function runOutput(bytes: number) {
  'use workflow';
  return randomBytes(clamp(bytes, MAX_BYTES));
}

/** Writes one stream chunk of `bytes` random bytes to the `limits` stream. */
export async function streamChunk(bytes: number) {
  'use workflow';
  return writeChunk(clamp(bytes, MAX_BYTES));
}

/**
 * A step returns a byte stream of `totalMiB` MiB, made `chunkMiB` MiB at a time as it is read, and
 * a second step reads it to the end and checks every byte. The run returns the bytes read, the
 * offset of the first wrong byte (-1 for none) and the reading step's time.
 */
export async function streamBytes(totalMiB: number, chunkMiB: number) {
  'use workflow';
  const stream = await patternStream(clamp(totalMiB, MAX_STREAM_MIB) * MiB, Math.max(1, clamp(chunkMiB, MAX_CHUNK_MIB)) * MiB);
  return checkPattern(stream);
}

/** Runs `count` steps one after another. */
export async function sequentialSteps(count: number) {
  'use workflow';
  let total = 0;
  for (let index = 0; index < clamp(count, MAX_COUNT); index++) total = await increment(total);
  return total;
}

/** Runs `count` steps, then sleeps a second so a fresh invocation replays the whole log, then one more step. */
export async function replayAfterSteps(count: number) {
  'use workflow';
  let total = 0;
  for (let index = 0; index < clamp(count, MAX_COUNT); index++) total = await increment(total);
  await sleep(1000);
  return increment(total);
}

/** Runs `count` steps at once. */
export async function parallelSteps(count: number) {
  'use workflow';
  const results = await Promise.all(Array.from({ length: clamp(count, MAX_COUNT) }, (_, index) => increment(index)));
  return results.length;
}

/** Sleeps for `hours` hours. */
export async function sleepHours(hours: number) {
  'use workflow';
  await sleep(clamp(hours, MAX_SLEEP_HOURS) * 60 * 60 * 1000);
  return hours;
}

/**
 * One step runs `megaIterations` million loop iterations. Workers clocks do not advance during
 * pure computation, so the work is counted rather than timed; read the duration from the events.
 */
export async function cpuStep(megaIterations: number) {
  'use workflow';
  return spin(clamp(megaIterations, MAX_MEGA_ITERATIONS));
}

/** Waits for a hook and returns the resumed payload's JSON length. */
export async function hookPayload(_value: number) {
  'use workflow';
  using hook = createHook<unknown>();
  return JSON.stringify(await hook).length;
}

async function randomBytes(length: number) {
  'use step';
  return fillRandom(length);
}

async function byteLength(data: Uint8Array) {
  'use step';
  return data.length;
}

async function writeChunk(length: number) {
  'use step';
  const writer = getWritable<Uint8Array>({ namespace: 'limits' }).getWriter();
  try {
    await writer.write(fillRandom(length));
    await writer.close();
  } finally {
    writer.releaseLock();
  }
  return length;
}

async function patternStream(total: number, chunk: number) {
  'use step';
  let offset = 0;
  return new ReadableStream({
    type: 'bytes',
    // Every read hands `pull` a buffer of one chunk to fill. Without it workerd reads 4 KiB at a
    // time, and each read becomes its own stream chunk.
    autoAllocateChunkSize: chunk,
    pull(controller) {
      const request = controller.byobRequest;
      if (request?.view == null) throw new Error('Expected a buffer to fill');
      if (offset === total) {
        controller.close();
        request.respond(0);
        return;
      }
      const length = Math.min(request.view.byteLength, total - offset);
      fillPattern(new Uint8Array(request.view.buffer, request.view.byteOffset, length), offset);
      request.respond(length);
      offset += length;
    },
  });
}

async function checkPattern(stream: ReadableStream<Uint8Array>) {
  'use step';
  const startedAt = Date.now();
  const reader = stream.getReader();
  let bytes = 0;
  let firstMismatch = -1;
  for (let read = await reader.read(); !read.done; read = await reader.read()) {
    if (firstMismatch === -1) {
      const at = findMismatch(read.value, bytes);
      if (at !== -1) firstMismatch = bytes + at;
    }
    bytes += read.value.byteLength;
  }
  const seconds = (Date.now() - startedAt) / 1000;
  return { bytes, firstMismatch, seconds, mibPerSecond: Math.round(bytes / MiB / seconds) };
}

async function increment(value: number) {
  'use step';
  return value + 1;
}

async function spin(megaIterations: number) {
  'use step';
  let checksum = 0;
  for (let index = 0; index < megaIterations * 1_000_000; index++) checksum = (checksum * 31 + index) | 0;
  return checksum;
}

/** Byte `index` of the stream pattern: one byte of a hash of its 32-bit word, so a shifted, lost or repeated piece fails the check. */
const patternByte = (index: number) => (Math.imul(index >>> 2, 0x9e3779b1) >>> ((index & 3) << 3)) & 0xff;

function fillPattern(bytes: Uint8Array, offset: number): void {
  for (let index = 0; index < bytes.length; index++) bytes[index] = patternByte(offset + index);
}

/** The index of the first byte of `bytes`, which start at `offset` in the stream, that is not the pattern's, or -1. */
function findMismatch(bytes: Uint8Array, offset: number): number {
  for (let index = 0; index < bytes.length; index++) if (bytes[index] !== patternByte(offset + index)) return index;
  return -1;
}

function fillRandom(length: number): Uint8Array {
  const data = new Uint8Array(length);
  // getRandomValues fills at most 64 KiB per call.
  for (let offset = 0; offset < length; offset += 65_536) {
    crypto.getRandomValues(data.subarray(offset, Math.min(offset + 65_536, length)));
  }
  return data;
}
