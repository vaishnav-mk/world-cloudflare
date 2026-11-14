// The workflows of the SDK's `@workflow/world-testing` suite, which runs them against a World in a
// Node server. This World runs in workerd, so `test/world-testing.test.ts` runs them here instead.
import { createHook, FatalError, getStepMetadata, getWritable, RetryableError, sleep } from 'workflow';

async function add(num: number, num2: number): Promise<number> {
  'use step';
  return num + num2;
}

export async function addition(num: number, num2: number): Promise<number> {
  'use workflow';
  return add(num, num2);
}

export async function addTenWorkflow(input: number): Promise<number> {
  'use workflow';
  const a = await add(input, 2);
  const b = await add(a, 3);
  return add(b, 5);
}

let count = 0;
async function noop(_index: number) {
  'use step';
  count++;
  return count;
}

export async function brokenWf() {
  'use workflow';
  const numbers: number[] = [];
  numbers.push(...await Promise.all(Array.from({ length: 5 }, (_, index) => noop(index))));
  numbers.push(...await Promise.all(Array.from({ length: 15 }, (_, index) => noop(100 + index))));
  return { numbers };
}

async function nullByteStep() {
  'use step';
  return 'null byte \0';
}

export async function nullByteWorkflow() {
  'use workflow';
  return nullByteStep();
}

export async function retryableAndFatalErrorWorkflow() {
  'use workflow';
  const retryableResult = await stepThatThrowsRetryableError();
  let gotFatalError = false;
  try {
    await stepThatFails();
  } catch (error) {
    if (FatalError.is(error)) gotFatalError = true;
  }
  return { retryableResult, gotFatalError };
}

async function stepThatThrowsRetryableError() {
  'use step';
  const { attempt, stepStartedAt } = getStepMetadata();
  if (attempt === 1) throw new RetryableError('Retryable error', { retryAfter: '2s' });
  return { attempt, stepStartedAt, duration: Date.now() - stepStartedAt.getTime() };
}

async function stepThatFails() {
  'use step';
  throw new FatalError('step failed');
}

type Payload = { data: string; done?: boolean };

export async function collectWithHook(token: string, customData: string) {
  'use workflow';
  const hook = createHook<Payload>({ token, metadata: { customData } });
  const collected: Payload[] = [];
  for await (const event of hook) {
    collected.push(event);
    if (event.done) break;
  }
  return { collected };
}

export async function registerUntilTerminal(token: string, fail: boolean) {
  'use workflow';
  const hook = createHook({ token });
  if (await hook.getConflict()) throw new Error('Token was not released');
  if (fail) throw new Error('Intentional terminal failure');
  return 'registered';
}

async function writeToStream(writable: WritableStream, text: string): Promise<string> {
  'use step';
  const writer = writable.getWriter();
  await writer.write(new TextEncoder().encode(text));
  writer.releaseLock();
  return text;
}

async function closeOutputStream(writable: WritableStream): Promise<void> {
  'use step';
  await writable.getWriter().close();
}

export async function sequentialStepsWithStreamWorkflow(): Promise<string> {
  'use workflow';
  const writable = getWritable();
  const a = await writeToStream(writable, 'hello');
  const b = await writeToStream(writable, ' world');
  await closeOutputStream(writable);
  return a + b;
}

export async function sleepWorkflow(): Promise<number> {
  'use workflow';
  await sleep('1s');
  return add(1, 2);
}

export async function parallelStepsWorkflow(): Promise<number> {
  'use workflow';
  const [a, b] = await Promise.all([add(10, 1), add(20, 2)]);
  return a + b;
}

async function trivialStep(input: { index: number; abortSignal?: AbortSignal }): Promise<number> {
  'use step';
  return input.index;
}

export async function signalCostWorkflow(input: { count: number; withSignal: boolean }): Promise<string> {
  'use workflow';
  const controller = new AbortController();
  for (let index = 0; index < input.count; index++) {
    await trivialStep({ abortSignal: input.withSignal ? controller.signal : undefined, index });
  }
  return 'done';
}
