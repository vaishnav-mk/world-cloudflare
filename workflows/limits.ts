import { createHook, getWritable, sleep } from 'workflow';

/**
 * Probes for measuring the World's limits. Each takes one number from `POST /runs`. Sizes use
 * random bytes so compression cannot shrink them. Every input is capped, because the example API
 * is public.
 */

const MAX_BYTES = 16 * 1024 * 1024;
const MAX_COUNT = 2_000;
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

/** Runs `count` steps one after another. */
export async function sequentialSteps(count: number) {
  'use workflow';
  let total = 0;
  for (let index = 0; index < clamp(count, MAX_COUNT); index++) total = await increment(total);
  return total;
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

function fillRandom(length: number): Uint8Array {
  const data = new Uint8Array(length);
  // getRandomValues fills at most 64 KiB per call.
  for (let offset = 0; offset < length; offset += 65_536) {
    crypto.getRandomValues(data.subarray(offset, Math.min(offset + 65_536, length)));
  }
  return data;
}
