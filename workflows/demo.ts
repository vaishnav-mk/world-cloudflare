import { createHook, FatalError, getStepMetadata, getWritable, sleep } from 'workflow';

export async function demo(value: number) {
  'use workflow';
  const doubled = await double(value);
  return doubled;
}

export async function delayed(value: number) {
  'use workflow';
  await sleep(200);
  return double(value);
}

export async function longDelay(value: number) {
  'use workflow';
  await sleep(45_000);
  return double(value);
}

export async function approval(value: number) {
  'use workflow';
  using hook = createHook<number>();
  return value + await hook;
}

export async function retryOnce(value: number) {
  'use workflow';
  return recover(value);
}

async function recover(value: number) {
  'use step';
  if (getStepMetadata().attempt === 1) throw new Error('transient test failure');
  return value * 2;
}

export async function failPermanently(value: number) {
  'use workflow';
  await fail(value);
}

async function fail(value: number) {
  'use step';
  throw new FatalError(`permanent test failure ${value}`);
}

export async function progress(value: number) {
  'use workflow';
  await writeProgress(value);
  return value;
}

async function writeProgress(value: number) {
  'use step';
  const writer = getWritable<Uint8Array>({ namespace: 'progress' }).getWriter();
  try {
    await writer.write(new TextEncoder().encode(`value=${value}`));
    await writer.close();
  } finally {
    writer.releaseLock();
  }
}

async function double(value: number) {
  'use step';
  return value * 2;
}
