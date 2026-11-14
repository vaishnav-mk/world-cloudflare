import { createHook, FatalError, getStepMetadata, getWritable, RetryableError, sleep } from 'workflow';
import { getRun, resumeHook, start } from 'workflow/api';

/**
 * Workflows that stress the workflow engines; `test/stress.test.ts` runs each on QuickJS and on the
 * node engine and compares them. Each takes one number from `POST /runs`, and every input is
 * capped, because the example API is public. The ones that patch built-ins undo it before returning.
 */

const MAX_STEPS = 2_000;
const MAX_DEPTH = 100_000;
const clamp = (value: number, max: number) => Math.max(0, Math.min(Math.floor(value), max));

// ---- Determinism

type Reading = Record<string, string | number>;

/** The APIs the engines make deterministic. */
function readClocks(): Reading {
  return {
    now: Date.now(),
    date: new Date().getTime(),
    dateString: new Date().toISOString(),
    random: Math.random(),
    uuid: crypto.randomUUID(),
    bytes: Array.from(crypto.getRandomValues(new Uint8Array(8))).join(','),
  };
}

/** Other ways to read a clock or random source; each is `'throws'` when the engine lacks it. */
function readEscapes(): Reading {
  const read = (fn: () => string | number) => { try { return fn(); } catch { return 'throws'; } };
  return {
    performanceNow: read(() => performance.now()),
    dateConstructor: read(() => new (new Date().constructor as DateConstructor)().getTime()),
    mathPrototype: read(() => (Object.getPrototypeOf(Math) as Math).random()),
    intlNow: read(() => new Intl.DateTimeFormat('en-US', { timeZone: 'UTC', timeStyle: 'medium' }).format()),
  };
}

/**
 * Reads `read()` before and after a step, a sleep and a hook, records each reading in a step, and
 * ends with a sleep, so the last invocation replays every read cold. Returns the readings that
 * differ from their recorded values, and the clock at each point.
 */
async function replayReadings(read: () => Reading) {
  const drift: string[] = [];
  const clocks: number[] = [];
  const take = async (label: string) => {
    clocks.push(Date.now());
    const reading = read();
    const recorded = await echo(reading);
    for (const key of Object.keys(reading)) if (recorded[key] !== reading[key]) drift.push(`${label}.${key}`);
  };
  await take('start');
  await echo(0);
  await take('afterStep');
  await sleep(50);
  await take('afterSleep');
  const hook = createHook<number>();
  await hook.getConflict();
  await resume(hook.token, 1);
  await hook;
  await take('afterHook');
  await sleep(1);
  await take('end');
  return { drift, clocks };
}

export async function clocks(_value: number) {
  'use workflow';
  return replayReadings(readClocks);
}

export async function clockEscapes(_value: number) {
  'use workflow';
  return replayReadings(readEscapes);
}

/**
 * Branches on a clock read through `new Date().constructor`: when a cold replay reads a different
 * value than the run first recorded, it calls a step where the log has a sleep.
 */
export async function clockBranch(_value: number) {
  'use workflow';
  const now = new (new Date().constructor as DateConstructor)().getTime();
  if (now !== await echo(now)) await increment(0);
  await sleep(1);
  return 'done';
}

// ---- Leaks through shared built-ins

/** Built-ins a leak probe marks, by index; `test/stress.test.ts` lists the same ones. */
const leakTargets = (): object[] => [Array.prototype, Object.prototype, JSON, Promise, Promise.prototype, Reflect,
  console, Uint8Array.prototype, TextEncoder.prototype, crypto, Math, Date, globalThis];

const LEAK_MARK = '__stressLeak';

/** Marks one built-in, waits on a hook so the test can look for the mark elsewhere, then removes it. */
export async function leakMark(target: number) {
  'use workflow';
  const object = leakTargets()[clamp(target, 12)];
  Object.defineProperty(object, LEAK_MARK, { value: 'marked', configurable: true, writable: true });
  try {
    await createHook<number>();
  } finally {
    delete (object as Record<string, unknown>)[LEAK_MARK];
  }
  return 'done';
}

/** What another run sees of a built-in's mark. */
export async function leakObserve(target: number) {
  'use workflow';
  const object = leakTargets()[clamp(target, 12)];
  return String(Object.getOwnPropertyDescriptor(object, LEAK_MARK)?.value);
}

/** Replaces `JSON.stringify` with a counting wrapper while it waits on a hook. */
export async function leakJsonStringify(_value: number) {
  'use workflow';
  type Stringify = typeof JSON.stringify & { original?: typeof JSON.stringify };
  // A suspended run never reaches its `finally`, so a replay may find its own earlier wrapper here.
  const original = (JSON.stringify as Stringify).original ?? JSON.stringify;
  const json = JSON as JSON & { [LEAK_MARK]?: number };
  json[LEAK_MARK] = 0;
  const wrapper: Stringify = function (this: JSON, ...args: Parameters<typeof original>) {
    json[LEAK_MARK] = (json[LEAK_MARK] ?? 0) + 1;
    return original.apply(this, args);
  } as Stringify;
  wrapper.original = original;
  JSON.stringify = wrapper;
  try {
    await createHook<number>();
  } finally {
    JSON.stringify = original;
    delete json[LEAK_MARK];
  }
  return 'done';
}

/** Assigns `crypto.randomUUID`, which a workflow may expect to stay local to its run. */
export async function leakCryptoAssign(_value: number) {
  'use workflow';
  const writable = crypto as unknown as Record<string, unknown>;
  writable.randomUUID = () => '00000000-0000-4000-8000-000000000000';
  try {
    await createHook<number>();
  } finally {
    delete writable.randomUUID;
  }
  return 'done';
}

// Module state of a workflow file: each run and each replay must start from these values.
let moduleCounter = 0;
let moduleEvaluations = 0;
moduleEvaluations++;

/** Counts loop iterations in module state across steps and a sleep. */
export async function moduleState(count: number) {
  'use workflow';
  for (let index = 0; index < clamp(count, 50); index++) {
    moduleCounter++;
    await increment(index);
    if (index === 1) await sleep(20);
  }
  return { counter: moduleCounter, evaluations: moduleEvaluations };
}

// ---- Concurrency

const mix = (acc: number, round: number) => (acc * 31 + round) % 1_000_003;

/** Interleaves steps and sleeps in four rounds, then checks its random draws survive a cold replay. */
export async function interleaved(value: number) {
  'use workflow';
  let acc = clamp(value, 1_000_000);
  const draws: number[] = [];
  for (let round = 0; round < 4; round++) {
    draws.push(Math.random());
    const [mixed, extra] = await Promise.all([
      mixStep(acc, round),
      round % 2 ? sleep(10 + (acc % 5) * 10).then(() => 0) : increment(round),
    ]);
    acc = (mixed + extra) % 1_000_003;
  }
  const recorded = await echo(draws);
  await sleep(1);
  return { acc, stable: recorded.every((draw, index) => draw === draws[index]) };
}

// ---- Control flow and values

export async function combinators(_value: number) {
  'use workflow';
  const all = await Promise.all([increment(1), increment(2), sleep(10).then(() => 'slept')]);
  const race = await Promise.race([increment(10), sleep('1h').then(() => 'late')]);
  const settled = await Promise.allSettled([increment(3), explode('settled')]);
  const any = await Promise.any([explode('any'), increment(4)]);
  let none = '';
  try {
    await Promise.any([explode('a'), explode('b')]);
  } catch (error) {
    none = `${(error as Error).name}:${(error as AggregateError).errors.map((e: Error) => e.message).join('|')}`;
  }
  return {
    all, race, any, none,
    settled: settled.map((s) => (s.status === 'fulfilled' ? s.value : describeError(s.reason))),
  };
}

function describeError(error: unknown): string {
  if (!(error instanceof Error)) return `non-error:${typeof error}`;
  const cause = (error as { cause?: unknown }).cause;
  return [error.name, error.message, FatalError.is(error), RetryableError.is(error), typeof error.stack,
    cause instanceof Error ? `cause=${cause.name}:${cause.message}` : `cause=${typeof cause}`].join('|');
}

export async function errorFlow(value: number) {
  'use workflow';
  const log: string[] = [];
  try {
    await explode('fatal');
  } catch (error) {
    log.push(describeError(error));
  } finally {
    log.push(`finally:${await increment(1)}`);
  }
  log.push(`retried:${await retryOnceStep(clamp(value, 1000))}`);
  try {
    await throwWithCause();
  } catch (error) {
    log.push(describeError(error));
  }
  try {
    throw new TypeError('in workflow', { cause: new RangeError('inner') });
  } catch (error) {
    log.push(describeError(error));
  }
  try {
    await Promise.reject(new FatalError('rejected'));
  } catch (error) {
    log.push(describeError(error));
  }
  return log;
}

/** Returns rich values from a step, passes them through a second step, and describes both copies. */
export async function richValues(_value: number) {
  'use workflow';
  const first = await makeValues();
  const second = await passValues(first);
  return { first: describeValues(first), second: describeValues(second), host: await describeOnHost(first) };
}

type Values = Awaited<ReturnType<typeof makeValues>>;

function describeValues(values: Values): string[] {
  const { map, set, date, regexp, bytes, floats, big, url, params, headers, nested } = values;
  return [
    `map:${map instanceof Map}:${[...map].map(([k, v]) => `${k}=${typeof v}:${v}`).join(',')}`,
    `set:${set instanceof Set}:${[...set].join(',')}`,
    `date:${date instanceof Date}:${date.toISOString()}`,
    `regexp:${regexp instanceof RegExp}:${regexp.source}/${regexp.flags}:${regexp.test('xAAB')}`,
    `bytes:${bytes instanceof Uint8Array}:${bytes.join(',')}`,
    `floats:${floats instanceof Float64Array}:${[...floats].join(',')}`,
    `big:${typeof big}:${big}`,
    `url:${url instanceof URL}:${url.href}:${url.pathname}`,
    `params:${params instanceof URLSearchParams}:${params.toString()}`,
    `headers:${headers instanceof Headers}:${headers.get('x-a')}`,
    `nested:${'undef' in nested}:${nested.undef}:${nested.nan}:${Object.is(nested.negZero, -0)}:${nested.inf}:${nested.sparse.length}:${1 in nested.sparse}`,
  ];
}

/** Features that stay inside workflow code: classes, generators, symbols, BigInt and friends. */
export async function language(value: number) {
  'use workflow';
  const out: string[] = [];
  class Counter {
    static created = 0;
    #count = 0;
    static { Counter.created = -1; }
    constructor() { Counter.created++; }
    get count() { return this.#count; }
    set count(next: number) { this.#count = next; }
    bump() { this.#count++; return this; }
    static isCounter(value: unknown) { return typeof value === 'object' && value !== null && #count in value; }
  }
  const counter = new Counter().bump().bump();
  counter.count += 10;
  out.push(`class:${counter.count}:${Counter.created}:${Counter.isCounter(counter)}:${Counter.isCounter({})}`);

  const closures: (() => number)[] = [];
  for (let index = 0; index < 3; index++) closures.push(() => index * value);
  out.push(`closures:${closures.map((fn) => fn()).join(',')}`);

  function* fib() { let [a, b] = [0, 1]; for (;;) { yield a; [a, b] = [b, a + b]; } }
  const fibs: number[] = [];
  for (const n of fib()) { if (n > 50) break; fibs.push(n); }
  out.push(`generator:${fibs.join(',')}`);

  async function* stepsInOrder(count: number) { for (let index = 0; index < count; index++) yield await increment(index); }
  const yielded: number[] = [];
  for await (const n of stepsInOrder(3)) yielded.push(n);
  out.push(`asyncGenerator:${yielded.join(',')}`);

  const local = Symbol('local');
  const shared = Symbol.for('stress.shared');
  const tagged = { [local]: 1, [shared]: 2, [Symbol.toStringTag]: 'Tagged' };
  const iterable = { *[Symbol.iterator]() { yield 'x'; yield 'y'; } };
  out.push(`symbols:${String(local)}:${Symbol.keyFor(shared)}:${Object.prototype.toString.call(tagged)}:${[...iterable].join('')}:${Object.getOwnPropertySymbols(tagged).length}`);

  const big = BigInt(value) ** 30n + 7n;
  out.push(`bigint:${big % 1_000_000_007n}:${BigInt.asUintN(16, -1n)}:${typeof big}`);

  const seen: string[] = [];
  const proxy = new Proxy({ a: 1 } as Record<string, number>, { get: (target, key) => { seen.push(String(key)); return Reflect.get(target, key); } });
  out.push(`proxy:${proxy.a}:${proxy.b}:${seen.join(',')}`);

  const weak = new WeakMap<object, number>([[counter, 1]]);
  out.push(`weakmap:${weak.get(counter)}:${weak.has({})}`);

  const disposed: string[] = [];
  {
    using _resource = { [Symbol.dispose]: () => disposed.push('disposed') };
    disposed.push('used');
  }
  out.push(`using:${disposed.join(',')}`);

  const tag = (strings: TemplateStringsArray, ...values: unknown[]) => strings.raw.join('|') + values.join('+');
  out.push(`tagged:${tag`a${1}b${2}c`}`);

  outer: for (let i = 0; i < 3; i++) for (let j = 0; j < 3; j++) { if (j === 1) continue outer; if (i === 2) break outer; out.push(`label:${i}${j}`); }

  out.push(`arrays:${[3, 1, 2].sort().join('')}:${[1, 2, 3].at(-1)}:${[[1, [2]], [3]].flat(2).join('')}:${Array.from({ length: 3 }, (_, i) => i * i).join('')}`);
  out.push(`json:${JSON.stringify(JSON.parse('{"a":[1,{"b":2}]}', (key, val) => (key === 'b' ? val * 10 : val)))}`);
  out.push(`clone:${structuredClone(new Map([[1, new Set([2])]])).get(1)?.has(2)}`);
  out.push(`string:${'abc'.padStart(5, '-')}:${'a-b-c'.replaceAll('-', '+')}:${'x'.repeat(3)}:${'Straße'.toUpperCase()}`);
  out.push(`number:${(0.1 + 0.2).toFixed(10)}:${Number.MAX_SAFE_INTEGER}:${(255).toString(16)}:${parseInt('08', 10)}:${Math.hypot(3, 4)}`);
  out.push(`error:${new Error('m', { cause: 1 }).cause}:${new AggregateError([new Error('x')], 'agg').errors.length}`);
  return out;
}

/** Runs `count` steps one after another. */
export async function manySteps(count: number) {
  'use workflow';
  let total = 0;
  for (let index = 0; index < clamp(count, MAX_STEPS); index++) total = await increment(total);
  return total;
}

/** Recurses `depth` frames in workflow code, catching a stack overflow. */
export async function recursion(depth: number) {
  'use workflow';
  const recurse = (n: number): number => (n === 0 ? 0 : 1 + recurse(n - 1));
  try {
    return `depth:${recurse(clamp(depth, MAX_DEPTH))}`;
  } catch (error) {
    return `caught:${(error as Error).name}`;
  }
}

/** Recurses through `depth` awaited steps. */
export async function asyncRecursion(depth: number) {
  'use workflow';
  const walk = async (n: number): Promise<number> => (n === 0 ? 0 : (await increment(0)) + (await walk(n - 1)));
  return walk(clamp(depth, 200));
}

// ---- The global surface: host APIs, strict mode and engine differences

/** Evaluates each probe in workflow code and reports its value (`ok:`) or its error's class (`throw:`). */
export async function surface(_value: number) {
  'use workflow';
  const g = globalThis as unknown as Record<string, unknown>;
  const call = (name: string, ...args: unknown[]) => (g[name] as (...args: unknown[]) => unknown)(...args);
  const probes: Record<string, () => unknown> = {
    'setTimeout()': () => call('setTimeout', () => {}, 0),
    'setInterval()': () => call('setInterval', () => {}, 10),
    'setImmediate()': () => call('setImmediate', () => {}),
    'queueMicrotask()': () => call('queueMicrotask', () => {}),
    'fetch()': () => typeof call('fetch', 'https://example.com'),
    'indirect eval()': () => (0, eval)('1 + 1'),
    'Function constructor': () => (function () {}).constructor('return 1')(),
    'performance.now()': () => typeof performance.now(),
    'import(cloudflare:workers)': async () => typeof (await import(/* @vite-ignore */ String('cloudflare:workers'))).env,
    'import(node:timers)': async () => typeof (await import(/* @vite-ignore */ String('node:timers'))).setTimeout,
    'typeof WeakRef': () => typeof g.WeakRef,
    'typeof FinalizationRegistry': () => typeof g.FinalizationRegistry,
    'typeof WebAssembly': () => typeof g.WebAssembly,
    'typeof Atomics': () => typeof g.Atomics,
    'Intl.NumberFormat': () => new Intl.NumberFormat('en-US').format(1234.5),
    'toLocaleString(locale)': () => (1234.5).toLocaleString('en-US'),
    'toLocaleString()': () => (1234.5).toLocaleString(),
    'Date#toString': () => new Date(0).toString(),
    'crypto.subtle.digest': async () => new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode('x')))[0],
    'undeclared assignment': () => {
      // @ts-expect-error assigns an undeclared variable, as sloppy code may
      stressUndeclared = 1;
      return g.stressUndeclared;
    },
    'this in plain call': () => {
      const self = function (this: unknown) { return this === undefined ? 'undefined' : this === globalThis ? 'globalThis' : typeof this; };
      return (self as () => string)();
    },
    'arguments.callee': () => (function () { return typeof (arguments as unknown as { callee: unknown }).callee; })(),
    'frozen write': () => {
      const frozen = Object.freeze({ a: 1 }) as { a: number };
      frozen.a = 2;
      return frozen.a;
    },
    'process.env write': () => {
      process.env.STRESS_WRITE = '1';
      return process.env.STRESS_WRITE;
    },
    'delete non-configurable': () => delete (Object as { prototype?: unknown }).prototype,
    'primitive property write': () => {
      const text = 'abc' as unknown as { x: number };
      text.x = 1;
      return text.x;
    },
    'globalThis write then bare read': () => {
      g.stressGlobal = 7;
      // @ts-expect-error reads the global written above
      return stressGlobal;
    },
    'top-level function on globalThis': () => typeof g.surface,
    'URL#searchParams': () => new URL('https://a.example/?c=1').searchParams.get('c'),
    'RegExp v flag': () => new RegExp('[\\p{L}--[a-z]]', 'v').test('A'),
    'Date.prototype.constructor': () => Date.prototype.constructor === Date,
    'Object.keys(Math)': () => Object.keys(Math).join(','),
    'JSON.stringify(1n)': () => JSON.stringify(1n),
    'structuredClone(Map)': () => structuredClone(new Map([[1, 2]])).get(1),
    'Headers': () => new Headers({ a: '1' }).get('a'),
    'TextEncoder': () => new TextEncoder().encode('hé').length,
    'AbortController': () => new AbortController().signal.aborted,
  };
  const out: Record<string, string> = {};
  for (const [name, probe] of Object.entries(probes)) {
    try {
      out[name] = `ok:${String(await probe())}`;
    } catch (error) {
      out[name] = `throw:${error instanceof Error ? error.name : typeof error}`;
    }
  }
  return out;
}

/** Calls a forbidden global without catching it: 0 `setTimeout`, 1 `fetch`. */
export async function uncaught(index: number) {
  'use workflow';
  const name = ['setTimeout', 'fetch'][clamp(index, 1)];
  (globalThis as unknown as Record<string, (...args: unknown[]) => unknown>)[name](() => {}, 0);
  return 'returned';
}

// ---- Hooks, sleeps, streams and child runs

/** Takes three hook payloads, writes them to a stream, sleeps, and runs a child workflow from a step. */
export async function hooksStreamsChild(value: number) {
  'use workflow';
  const payloads: number[] = [];
  {
    using hook = createHook<number>();
    for await (const payload of hook) {
      payloads.push(payload);
      if (payloads.length === 3) break;
    }
  }
  await writeLines('stress', payloads.map(String));
  await sleep('10ms');
  const childRunId = await startChild(clamp(value, 1000));
  return { payloads, childRunId, childResult: await awaitChild(childRunId) };
}

/** Leaves a `for await` over a hook after the first payload, without `using`. */
export async function hookBreak(_value: number) {
  'use workflow';
  for await (const payload of createHook<number>()) return payload;
  return -1;
}

/** Creates an AbortController it never aborts. */
export async function unusedAbortController(_value: number) {
  'use workflow';
  return new AbortController().signal.aborted;
}

/** BigInts outside the int64 range, returned by a step and passed through another. */
export async function bigints(_value: number) {
  'use workflow';
  const fromStep = await bigValues();
  const throughStep = await passBigints([2n ** 70n, -(2n ** 64n)]);
  return [...fromStep, ...throughStep].map(String);
}

export async function child(value: number) {
  'use workflow';
  await sleep(10);
  return increment(value);
}

// ---- Steps

async function echo<T>(value: T): Promise<T> {
  'use step';
  return value;
}

async function increment(value: number) {
  'use step';
  return value + 1;
}

async function mixStep(acc: number, round: number) {
  'use step';
  return mix(acc, round);
}

async function resume(token: string, payload: number) {
  'use step';
  await resumeHook(token, payload);
}

async function explode(tag: string): Promise<number> {
  'use step';
  throw new FatalError(`boom ${tag}`);
}

async function retryOnceStep(value: number) {
  'use step';
  if (getStepMetadata().attempt === 1) throw new RetryableError('again', { retryAfter: 10 });
  return value * 2;
}

async function throwWithCause(): Promise<void> {
  'use step';
  const error = new FatalError('outer');
  (error as { cause?: unknown }).cause = new TypeError('inner');
  throw error;
}

async function makeValues() {
  'use step';
  return {
    map: new Map<string, bigint | number>([['a', 1n], ['b', 2]]),
    set: new Set<number | string>([1, 'two']),
    date: new Date(Date.UTC(2020, 1, 29, 12)),
    regexp: /a+b/gi,
    bytes: new Uint8Array([1, 2, 255]),
    floats: new Float64Array([1.5, Number.NaN, -0]),
    big: 2n ** 40n,
    url: new URL('https://example.com/a%20b?c=d#e'),
    params: new URLSearchParams('x=1&y=2&x=3'),
    headers: new Headers({ 'x-a': '1' }),
    nested: { undef: undefined, nan: Number.NaN, negZero: -0, inf: -Infinity, sparse: [1, , 3] },
  };
}

async function bigValues() {
  'use step';
  return [2n ** 63n - 1n, 2n ** 63n, 2n ** 64n, 10n ** 30n];
}

async function passBigints(values: bigint[]) {
  'use step';
  return values;
}

async function passValues(values: Values) {
  'use step';
  return values;
}

async function describeOnHost(values: Values) {
  'use step';
  return describeValues(values);
}

async function writeLines(namespace: string, lines: string[]) {
  'use step';
  const writer = getWritable<Uint8Array>({ namespace }).getWriter();
  try {
    for (const line of lines) await writer.write(new TextEncoder().encode(`${line}\n`));
    await writer.close();
  } finally {
    writer.releaseLock();
  }
}

async function startChild(value: number) {
  'use step';
  return (await start(child, [value])).runId;
}

async function awaitChild(runId: string) {
  'use step';
  return await getRun(runId).returnValue as number;
}
