import { toJson } from '@workflow/world-cloudflare';
import { getRun, resumeHook, start } from 'workflow/api';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { world } from '../src/world.js';
import { workflows } from '../src/workflows.js';

// Runs under vitest.isolate.config.ts. Each parity test starts the same workflow on QuickJS and on
// the isolate engine (a run keeps the engine `start()` stamped from WORKFLOW_VM) and compares the
// outcome and a normalized event log. `it.fails` marks a known engine bug or divergence: flip it to
// `it` once fixed.

type Engine = 'quickjs' | 'isolate';
type Name = keyof typeof workflows;
const ENGINES: Engine[] = ['quickjs', 'isolate'];
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Starts runs on one engine. Never overlap two calls: `start()` reads WORKFLOW_VM. */
async function startOn(engine: Engine, name: Name, values: number[]): Promise<string[]> {
  const previous = process.env.WORKFLOW_VM;
  process.env.WORKFLOW_VM = engine;
  try {
    const workflow = workflows[name] as (value: number) => Promise<unknown>;
    return await Promise.all(values.map(async (value) => (await start(workflow, [value])).runId));
  } finally {
    process.env.WORKFLOW_VM = previous;
  }
}

type Outcome = { status: string; value?: unknown };

async function settle(runId: string, timeout = 25_000): Promise<Outcome> {
  const deadline = Date.now() + timeout;
  for (;;) {
    const { status } = await world.runs.get(runId);
    if (status === 'completed') return { status, value: await getRun(runId).returnValue };
    if (status === 'failed' || status === 'cancelled') return { status };
    if (Date.now() > deadline) return { status: `still ${status}` };
    await sleep(50);
  }
}

/** Each event with its step, wait or hook as `kind#n name`, numbered by first appearance. */
async function labelledEvents(runId: string) {
  const { data } = await world.events.list({ runId });
  const labels = new Map<string, string>();
  return data.map((event) => {
    const id = event.correlationId;
    if (id === undefined) return { event, entity: undefined };
    let entity = labels.get(id);
    if (entity === undefined) {
      const kind = event.eventType.split('_')[0];
      const count = [...labels.values()].filter((label) => label.startsWith(`${kind}#`)).length + 1;
      const stepName = (event.eventData as { stepName?: string } | undefined)?.stepName?.split('//').pop();
      entity = [`${kind}#${count}`, stepName].filter(Boolean).join(' ');
      labels.set(id, entity);
    }
    return { event, entity };
  });
}

/**
 * The run's events without IDs or timestamps: run events in order, then each step, wait and hook
 * with its event types. Grouping by entity drops the interleaving of concurrent steps and timers,
 * which depends on queue timing.
 */
async function eventLog(runId: string): Promise<string[]> {
  const run: string[] = [];
  const entities = new Map<string, string[]>();
  for (const { event, entity } of await labelledEvents(runId)) {
    if (entity === undefined) run.push(event.eventType);
    else entities.set(entity, [...(entities.get(entity) ?? []), event.eventType]);
  }
  return [...run, ...[...entities].map(([entity, types]) => `${entity}: ${types.join(',')}`)];
}

async function hookToken(runId: string): Promise<string> {
  for (const deadline = Date.now() + 15_000; Date.now() < deadline; await sleep(50)) {
    const [hook] = (await world.hooks.list({ runId })).data;
    if (hook !== undefined) return hook.token;
  }
  throw new Error(`run ${runId} created no hook`);
}

type Result = Outcome & { events: string[] };

/** Runs `name(value)` on both engines; `drive` feeds a run while it executes, such as resuming its hook. */
async function onBoth(name: Name, value = 0, drive?: (runId: string) => Promise<void>, timeout = 50_000) {
  const ids = { quickjs: (await startOn('quickjs', name, [value]))[0], isolate: (await startOn('isolate', name, [value]))[0] };
  const [quickjs, isolate] = await Promise.all(ENGINES.map(async (engine): Promise<Result> => {
    await drive?.(ids[engine]);
    const outcome = await settle(ids[engine], timeout);
    return { ...outcome, events: await eventLog(ids[engine]) };
  }));
  return { quickjs, isolate, ids };
}

async function expectParity(name: Name, value = 0, drive?: (runId: string) => Promise<void>, timeout?: number) {
  const { quickjs, isolate } = await onBoth(name, value, drive, timeout);
  expect(quickjs.status).toBe('completed');
  expect(isolate).toEqual(quickjs);
}

// Events that move a run's clock when they reach the workflow.
const CLOCK_EVENTS = new Set(['step_completed', 'step_failed', 'wait_completed', 'hook_created', 'hook_received', 'hook_conflict']);

/** For each clock reading, the run ID if it is the ID's ULID time, else the clock events recorded at that millisecond. */
async function clockSources(runId: string, clocks: number[]): Promise<string[][]> {
  const events = (await labelledEvents(runId)).filter(({ event }) => CLOCK_EVENTS.has(event.eventType));
  const runIdTime = [...runId.slice(5, 15)].reduce((ms, char) => ms * 32 + '0123456789ABCDEFGHJKMNPQRSTVWXYZ'.indexOf(char), 0);
  return clocks.map((clock) => (clock === runIdTime ? ['run id'] : events
    .filter(({ event }) => +event.createdAt === clock).map(({ event, entity }) => `${event.eventType} ${entity}`)));
}

describe('determinism', () => {
  it('replays Date, Math.random and crypto identically after steps, sleeps and hooks', async () => {
    const { quickjs, isolate, ids } = await onBoth('clocks');
    // The clock starts at the run's creation and then reads the time of the last delivery.
    const expected = ['run id', 'step_completed step#2 echo', 'wait_completed wait#1', 'step_completed step#5 resume', 'wait_completed wait#2'];
    for (const [engine, result] of [['quickjs', quickjs], ['isolate', isolate]] as const) {
      expect(result.status).toBe('completed');
      const { drift, clocks } = result.value as { drift: string[]; clocks: number[] };
      expect(drift).toEqual([]);
      expect(await clockSources(ids[engine], clocks)).toEqual(expected.map((source) => expect.arrayContaining([source])));
      result.value = undefined;
    }
    expect(isolate).toEqual(quickjs);
  });

  // One clockEscapes run per engine, shared by the tests below.
  const escapes = new Map<Engine, Promise<string[]>>();
  const drift = (engine: Engine) => {
    let readings = escapes.get(engine);
    if (readings === undefined) {
      readings = startOn(engine, 'clockEscapes', [0]).then(async ([runId]) => {
        const { status, value } = await settle(runId);
        expect(status).toBe('completed');
        return (value as { drift: string[] }).drift;
      });
      escapes.set(engine, readings);
    }
    return readings;
  };

  it('keeps every other clock and random source stable on QuickJS', async () => {
    expect(await drift('quickjs')).toEqual([]);
  });

  // Bug: Date.prototype.constructor is the host's real Date, so `new date.constructor()` reads the wall clock.
  it.fails('isolate: new (new Date().constructor)() is replay-stable', async () => {
    expect((await drift('isolate')).filter((key) => key.endsWith('.dateConstructor'))).toEqual([]);
  });

  // Bug: the sandbox Math inherits from the host's Math, whose random() is unseeded.
  it.fails('isolate: Object.getPrototypeOf(Math).random() is replay-stable', async () => {
    expect((await drift('isolate')).filter((key) => key.endsWith('.mathPrototype'))).toEqual([]);
  });

  // Bug: Intl formats "now" from the host clock, not the sandbox's Date.
  it.fails('isolate: Intl.DateTimeFormat().format() is replay-stable', async () => {
    expect((await drift('isolate')).filter((key) => key.endsWith('.intlNow'))).toEqual([]);
  });

  // Bug: the wall clock behind Date.prototype.constructor makes a cold replay branch differently; the
  // run fails with a replay divergence (CorruptedEventLogError) on isolate and completes on QuickJS.
  it.fails('branching on new (new Date().constructor)() replays', () => expectParity('clockBranch'));
});

describe('leaks through shared built-ins', () => {
  // The objects `leakTargets()` in workflows/stress.ts lists, in its order, as the host sees them.
  const targets: [string, object][] = [
    ['Array.prototype', Array.prototype], ['Object.prototype', Object.prototype], ['JSON', JSON],
    ['Promise', Promise], ['Promise.prototype', Promise.prototype], ['Reflect', Reflect], ['console', console],
    ['Uint8Array.prototype', Uint8Array.prototype], ['TextEncoder.prototype', TextEncoder.prototype],
    ['crypto', crypto], ['Math', Math], ['Date', Date], ['globalThis', globalThis],
  ];
  const MARK = '__stressLeak';
  const own = (object: object) => String(Object.getOwnPropertyDescriptor(object, MARK)?.value);
  const seen = new Map<Engine, { host: string[]; otherRuns: string[] }>();

  // Per engine: every target marked by a run waiting on its hook, while the host and other runs look.
  beforeAll(async () => {
    for (const engine of ENGINES) {
      const leakers = await startOn(engine, 'leakMark', targets.map((_, index) => index));
      try {
        const tokens = await Promise.all(leakers.map(hookToken));
        const host = targets.map(([, object]) => own(object));
        const observers = await startOn(engine, 'leakObserve', targets.map((_, index) => index));
        const otherRuns = (await Promise.all(observers.map((runId) => settle(runId)))).map(({ value }) => String(value));
        seen.set(engine, { host, otherRuns });
        await Promise.all(tokens.map((token) => resumeHook(token, 0)));
        expect((await Promise.all(leakers.map((runId) => settle(runId)))).map(({ value }) => value)).toEqual(targets.map(() => 'done'));
      } finally {
        for (const [, object] of targets) delete (object as Record<string, unknown>)[MARK];
      }
    }
  }, 60_000);

  // Bug for the first ten targets on the isolate engine: the sandbox hands workflow code the host's own objects.
  const leaksOnIsolate = new Set(targets.slice(0, 10).map(([name]) => name));
  for (const engine of ENGINES) {
    for (const [index, [name]] of targets.entries()) {
      (engine === 'isolate' && leaksOnIsolate.has(name) ? it.fails : it)(`${engine}: a mark on ${name} stays in its run`, () => {
        const { host, otherRuns } = seen.get(engine) ?? { host: [], otherRuns: [] };
        expect({ host: host[index], otherRun: otherRuns[index] }).toEqual({ host: 'undefined', otherRun: 'undefined' });
      });
    }
  }

  /** Starts `name`, which patches a built-in and waits on a hook; returns what `probe` saw meanwhile. */
  async function whilePatched<T>(engine: Engine, name: Name, probe: () => T, restore: () => void): Promise<T> {
    const [runId] = await startOn(engine, name, [0]);
    try {
      const token = await hookToken(runId);
      const seenMeanwhile = probe();
      await resumeHook(token, 0);
      expect(await settle(runId)).toEqual({ status: 'completed', value: 'done' });
      return seenMeanwhile;
    } finally {
      restore();
    }
  }

  for (const engine of ENGINES) {
    // Bug on isolate: workflow code assigning JSON.stringify replaces it for the World's own serialization.
    (engine === 'isolate' ? it.fails : it)(`${engine}: a workflow's JSON.stringify patch does not reach World code`, async () => {
      const stringify = JSON.stringify;
      const calls = await whilePatched(engine, 'leakJsonStringify', () => {
        toJson({ a: 1 });
        return { patched: JSON.stringify !== stringify, counted: (JSON as unknown as Record<string, unknown>)[MARK] };
      }, () => {
        JSON.stringify = stringify;
        delete (JSON as unknown as Record<string, unknown>)[MARK];
      });
      expect(calls).toEqual({ patched: false, counted: undefined });
    });

    // Bug on isolate: the sandbox crypto is a Proxy with no set trap, so assignments land on the host's crypto.
    (engine === 'isolate' ? it.fails : it)(`${engine}: assigning crypto.randomUUID stays in the run`, async () => {
      const uuid = await whilePatched(engine, 'leakCryptoAssign', () => crypto.randomUUID(),
        () => { delete (crypto as unknown as Record<string, unknown>).randomUUID; });
      expect(uuid).not.toBe('00000000-0000-4000-8000-000000000000');
    });
  }

  it('starts module state afresh for each run and replay', async () => {
    for (const engine of ENGINES) {
      const runIds = await startOn(engine, 'moduleState', Array.from({ length: 10 }, () => 5));
      const outcomes = await Promise.all(runIds.map((runId) => settle(runId)));
      expect(outcomes).toEqual(runIds.map(() => ({ status: 'completed', value: { counter: 5, evaluations: 1 } })));
    }
  });
});

describe('concurrency', () => {
  it('replays 60 isolate runs and 10 QuickJS runs at once', async () => {
    const values = Array.from({ length: 60 }, (_, index) => index * 7919);
    const isolateIds = await startOn('isolate', 'interleaved', values);
    const quickjsIds = await startOn('quickjs', 'interleaved', values.slice(0, 10));
    const expected = (value: number) => {
      let acc = value;
      for (let round = 0; round < 4; round++) acc = ((acc * 31 + round) % 1_000_003 + (round % 2 ? 0 : round + 1)) % 1_000_003;
      return { status: 'completed', value: { acc, stable: true } };
    };
    const outcomes = await Promise.all([...isolateIds, ...quickjsIds].map((runId) => settle(runId, 50_000)));
    expect(outcomes).toEqual([...values, ...values.slice(0, 10)].map(expected));
  }, 60_000);
});

describe('control flow and values', () => {
  it('Promise.all, race, allSettled and any over steps and sleeps', () => expectParity('combinators'));
  it('try/catch/finally, FatalError, RetryableError and causes', () => expectParity('errorFlow', 3));
  it('Map, Set, Date, RegExp, typed arrays, URL, Headers and BigInt across steps', () => expectParity('richValues'));
  it('classes, private fields, generators, async iterators, symbols and BigInt in workflow code', () => expectParity('language', 3));
  it('2,000 sequential steps', () => expectParity('manySteps', 2000), 60_000);
  it('recursion 1,000 frames deep', () => expectParity('recursion', 1000));
  it('recursion through 50 awaited steps', () => expectParity('asyncRecursion', 50));
  it('isolate: catches a stack overflow in workflow code', async () => {
    const [runId] = await startOn('isolate', 'recursion', [100_000]);
    expect(await settle(runId)).toEqual({ status: 'completed', value: 'caught:RangeError' });
  });

  // Divergence (QuickJS bug): BigInts outside the int64 range wrap modulo 2^64 when they cross a step boundary.
  it.fails('BigInts outside the int64 range across steps', () => expectParity('bigints'));
  // Divergence (QuickJS bug): QuickJS overflows the host stack at ~2,000 frames and fails the run; isolate recurses.
  it.fails('recursion 5,000 frames deep', () => expectParity('recursion', 5000));
});

describe('hooks, sleeps, streams and child runs', () => {
  it('hook payloads, a stream, a sleep and a child run started from a step', async () => {
    const { quickjs, isolate, ids } = await onBoth('hooksStreamsChild', 4, async (runId) => {
      const token = await hookToken(runId);
      for (const payload of [1, 2, 3]) await resumeHook(token, payload);
    });
    for (const engine of ENGINES) {
      const result = { quickjs, isolate }[engine];
      const { childRunId, ...value } = result.value as { childRunId: string };
      expect(value).toEqual({ payloads: [1, 2, 3], childResult: 5 });
      // A child takes the starting Worker's WORKFLOW_VM, not its parent's engine.
      expect((await world.runs.get(childRunId)).executionContext).toMatchObject({ workflowVm: 'isolate' });
      const stream = getRun(ids[engine]).getReadable<Uint8Array>({ namespace: 'stress' });
      expect(await new Response(stream).text()).toBe('1\n2\n3\n');
      result.value = value;
    }
    expect(isolate).toEqual(quickjs);
  });

  const resumeOnce = async (runId: string) => { await resumeHook(await hookToken(runId), 9); };
  // Divergence: leaving `for await` over a hook disposes it on QuickJS (hook_disposed) but not on isolate.
  it.fails('leaving a for await over a hook disposes it', () => expectParity('hookBreak', 0, resumeOnce));
  // Divergence: an AbortController that is never aborted writes hook_created and hook_disposed on isolate only.
  it.fails('an unused AbortController', () => expectParity('unusedAbortController'));
});

describe('forbidden and host APIs', () => {
  it('an uncaught setTimeout fails the run on both engines', async () => {
    const { quickjs, isolate } = await onBoth('uncaught', 0);
    expect(quickjs.status).toBe('failed');
    expect(isolate).toEqual(quickjs);
  });

  // Divergence: QuickJS runs the bundle as a sloppy script, so the SDK's bundled `var fetch` (its fetch step)
  // becomes globalThis.fetch and the call starts a step; the isolate bundle keeps it local and throws.
  it.fails('an uncaught fetch fails the run on both engines', async () => {
    const { quickjs, isolate } = await onBoth('uncaught', 1);
    expect(quickjs.status).toBe('failed');
    expect(isolate).toEqual(quickjs);
  });
});

describe('global surface', () => {
  // Each probe in the `surface` workflow, and why the engines disagree on it.
  const divergences: Record<string, string | undefined> = {
    // Timers: isolate throws the SDK's WorkflowRuntimeError; QuickJS has no such global (TypeError).
    'setTimeout()': 'error class', 'setInterval()': 'error class', 'setImmediate()': 'error class',
    // Isolate throws the SDK's fetch error; on QuickJS globalThis.fetch is the bundle's `var fetch` step, or absent
    // (TypeError) when some workflow names `fetch` bare and esbuild renames the var.
    'fetch()': 'bundle var on QuickJS',
    // QuickJS has queueMicrotask and performance (with a replay-stable clock); the isolate sandbox has neither.
    'queueMicrotask()': 'missing on isolate', 'performance.now()': 'missing on isolate',
    // Workers forbid code generation; QuickJS evaluates strings. The probe takes Function from a function, because
    // vitest-pool-workers proxies the global Function to allow `new Function`, which production rejects too.
    'indirect eval()': 'EvalError on isolate', 'Function constructor': 'EvalError on isolate',
    // Bug: isolate's import() reaches the host's modules, such as cloudflare:workers (env bindings) and real timers.
    'import(cloudflare:workers)': 'host modules', 'import(node:timers)': 'host modules',
    // The SDK hides WeakRef and FinalizationRegistry on isolate (as on node:vm) but not on QuickJS.
    'typeof WeakRef': 'hidden on isolate', 'typeof FinalizationRegistry': 'hidden on isolate',
    // QuickJS has no WebAssembly or Atomics.
    'typeof WebAssembly': 'missing on QuickJS', 'typeof Atomics': 'missing on QuickJS',
    // QuickJS has no ICU, so it throws on explicit locales and formats differently without one.
    'Intl.NumberFormat': 'no ICU on QuickJS', 'toLocaleString(locale)': 'no ICU on QuickJS',
    'toLocaleString()': 'no ICU on QuickJS', 'Date#toString': 'no ICU on QuickJS',
    // QuickJS throws on crypto.subtle.digest; isolate computes it as node:vm does.
    'crypto.subtle.digest': 'missing on QuickJS',
    // Strict mode: the isolate bundle runs strict, QuickJS sloppy.
    'undeclared assignment': 'strict mode', 'this in plain call': 'strict mode', 'arguments.callee': 'strict mode',
    'frozen write': 'strict mode', 'process.env write': 'strict mode', 'delete non-configurable': 'strict mode',
    'primitive property write': 'strict mode',
    // Isolate binds bare globals once and keeps top-level declarations local to the factory.
    'globalThis write then bare read': 'globals resolve once', 'top-level function on globalThis': 'module scope',
    // Bug in QuickJS: URL has no searchParams.
    'URL#searchParams': 'missing on QuickJS',
    // QuickJS lacks the RegExp v flag.
    'RegExp v flag': 'missing on QuickJS',
    // Isolate's Date is a wrapper around the host Date, and its Math an object inheriting from the host's.
    'Date.prototype.constructor': 'wrapped Date', 'Object.keys(Math)': 'wrapped Math',
    'JSON.stringify(1n)': undefined, 'structuredClone(Map)': undefined, Headers: undefined, TextEncoder: undefined,
    AbortController: undefined,
  };
  const results = new Map<Engine, Record<string, string>>();
  beforeAll(async () => {
    for (const engine of ENGINES) {
      const [runId] = await startOn(engine, 'surface', [0]);
      const { status, value } = await settle(runId);
      expect(status).toBe('completed');
      results.set(engine, value as Record<string, string>);
    }
  });

  it('probes the same names on both engines', () => {
    for (const engine of ENGINES) expect(Object.keys(results.get(engine) ?? {}).sort()).toEqual(Object.keys(divergences).sort());
  });

  for (const [probe, divergence] of Object.entries(divergences)) {
    (divergence === undefined ? it : it.fails)(`${probe}${divergence === undefined ? '' : ` (${divergence})`}`, () => {
      expect(results.get('isolate')?.[probe]).toBe(results.get('quickjs')?.[probe]);
    });
  }
});

// Last, because the isolate run it starts may never finish.
describe('scale', () => {
  it('QuickJS: Promise.all over 200 steps', async () => {
    const [runId] = await startOn('quickjs', 'parallelSteps', [200]);
    expect(await settle(runId)).toEqual({ status: 'completed', value: 200 });
  });

  // Bug: on isolate, step deliveries wait out earlier cascades with setImmediate turns; ~120 or more
  // steps in one Promise.all exceed Workers' 10,000 active timeouts, deliveries throw
  // QuotaExceededError, and the run often hangs with a step never started (1,000 ran workerd out of heap).
  it.fails('isolate: Promise.all over 200 steps', async () => {
    // Muted: the failed deliveries log tens of thousands of errors, which overflow the test pool's socket.
    using errors = vi.spyOn(console, 'error').mockImplementation(() => {});
    // Some also reject unhandled. A listener stops vitest failing the file on them; it stays installed
    // because a hung run keeps rejecting after this test ends.
    let unhandled = 0;
    process.on('unhandledRejection', () => { unhandled++; });
    const [runId] = await startOn('isolate', 'parallelSteps', [200]);
    const outcome = await settle(runId, 15_000);
    const quotaErrors = errors.mock.calls.filter((args) => String(args[0]).includes('QuotaExceededError')).length;
    expect({ outcome, quotaErrors, unhandled }).toEqual({ outcome: { status: 'completed', value: 200 }, quotaErrors: 0, unhandled: 0 });
  }, 30_000);
});
