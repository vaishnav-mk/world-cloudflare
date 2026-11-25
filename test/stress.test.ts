import { toJson } from 'world-cloudflare';
import { getRun, resumeHook, start } from 'workflow/api';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { world } from '../src/world.js';
import { stressWorkflows, workflows as appWorkflows } from '../src/workflows.js';

const workflows = { ...appWorkflows, ...stressWorkflows };

// Runs under vitest.node.config.ts. Each parity test starts the same workflow on QuickJS and on the
// SDK's node engine on vm.ts (a run keeps the engine `start()` stamped from WORKFLOW_VM) and compares
// the outcome and a normalized event log. `it.fails` marks a known engine bug or divergence: flip it to
// `it` once fixed.

type Engine = 'quickjs' | 'node';
type Name = keyof typeof workflows;
const ENGINES: Engine[] = ['quickjs', 'node'];
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
 * with its event types, sorted. Grouping and sorting drop the interleaving of concurrent steps and
 * timers, and the order they first appear in, which depend on queue timing and the SDK's speculation.
 */
async function eventLog(runId: string): Promise<string[]> {
  const run: string[] = [];
  const entities = new Map<string, string[]>();
  for (const { event, entity } of await labelledEvents(runId)) {
    if (entity === undefined) run.push(event.eventType);
    else entities.set(entity, [...(entities.get(entity) ?? []), event.eventType]);
  }
  return [...run, ...[...entities].map(([entity, types]) => `${entity}: ${types.join(',')}`).sort()];
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
  const ids = { quickjs: (await startOn('quickjs', name, [value]))[0], node: (await startOn('node', name, [value]))[0] };
  const [quickjs, node] = await Promise.all(ENGINES.map(async (engine): Promise<Result> => {
    await drive?.(ids[engine]);
    const outcome = await settle(ids[engine], timeout);
    return { ...outcome, events: await eventLog(ids[engine]) };
  }));
  return { quickjs, node, ids };
}

async function expectParity(name: Name, value = 0, drive?: (runId: string) => Promise<void>, timeout?: number) {
  const { quickjs, node } = await onBoth(name, value, drive, timeout);
  expect(quickjs.status).toBe('completed');
  expect(node).toEqual(quickjs);
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
    const { quickjs, node, ids } = await onBoth('clocks');
    // The clock starts at the run's creation and then reads the time of the last delivery.
    const expected = ['run id', 'step_completed step#2 echo', 'wait_completed wait#1', 'step_completed step#5 resume', 'wait_completed wait#2'];
    for (const [engine, result] of [['quickjs', quickjs], ['node', node]] as const) {
      expect(result.status).toBe('completed');
      const { drift, clocks } = result.value as { drift: string[]; clocks: number[] };
      expect(drift).toEqual([]);
      expect(await clockSources(ids[engine], clocks)).toEqual(expected.map((source) => expect.arrayContaining([source])));
      result.value = undefined;
    }
    expect(node).toEqual(quickjs);
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

  it('node: new (new Date().constructor)() is replay-stable', async () => {
    expect((await drift('node')).filter((key) => key.endsWith('.dateConstructor'))).toEqual([]);
  });

  it('node: Object.getPrototypeOf(Math).random() is replay-stable', async () => {
    expect((await drift('node')).filter((key) => key.endsWith('.mathPrototype'))).toEqual([]);
  });

  it('node: Intl.DateTimeFormat().format() is replay-stable', async () => {
    expect((await drift('node')).filter((key) => key.endsWith('.intlNow'))).toEqual([]);
  });

  it('branching on new (new Date().constructor)() replays', () => expectParity('clockBranch'));
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
  const seen = new Map<Engine, { host: string[]; otherRuns: string[]; outcomes: string[] }>();
  // Divergence: the node engine freezes the built-ins runs share, so marking one throws and fails the
  // run before its hook. The namespaces (JSON, Reflect, console, crypto, Math), Date and globalThis are
  // each run's own, and take the mark as on QuickJS.
  const frozenOnNode = new Set(['Array.prototype', 'Object.prototype', 'Promise', 'Promise.prototype', 'Uint8Array.prototype', 'TextEncoder.prototype']);
  const expectedOutcome = (engine: Engine, name: string) => (engine === 'node' && frozenOnNode.has(name) ? 'failed' : 'done');

  // Per engine: every target marked by a run waiting on its hook, while the host and other runs look.
  beforeAll(async () => {
    for (const engine of ENGINES) {
      const leakers = await startOn(engine, 'leakMark', targets.map((_, index) => index));
      try {
        const tokens = await Promise.all(leakers.map((runId, index) =>
          (expectedOutcome(engine, targets[index][0]) === 'done' ? hookToken(runId) : undefined)));
        const host = targets.map(([, object]) => own(object));
        const observers = await startOn(engine, 'leakObserve', targets.map((_, index) => index));
        const otherRuns = (await Promise.all(observers.map((runId) => settle(runId)))).map(({ value }) => String(value));
        await Promise.all(tokens.map((token) => token !== undefined && resumeHook(token, 0)));
        const outcomes = (await Promise.all(leakers.map((runId) => settle(runId)))).map(({ status, value }) => String(value ?? status));
        seen.set(engine, { host, otherRuns, outcomes });
      } finally {
        for (const [, object] of targets) delete (object as Record<string, unknown>)[MARK];
      }
    }
  }, 60_000);

  for (const engine of ENGINES) {
    it(`${engine}: marking each built-in ${engine === 'node' ? 'fails the run where it is shared' : 'completes'}`, () => {
      expect(seen.get(engine)?.outcomes).toEqual(targets.map(([name]) => expectedOutcome(engine, name)));
    });
    for (const [index, [name]] of targets.entries()) {
      it(`${engine}: a mark on ${name} stays in its run`, () => {
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
    it(`${engine}: a workflow's JSON.stringify patch does not reach World code`, async () => {
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

    it(`${engine}: assigning crypto.randomUUID stays in the run`, async () => {
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
  it('replays 60 node runs and 10 QuickJS runs at once', async () => {
    const values = Array.from({ length: 60 }, (_, index) => index * 7919);
    const nodeIds = await startOn('node', 'interleaved', values);
    const quickjsIds = await startOn('quickjs', 'interleaved', values.slice(0, 10));
    const expected = (value: number) => {
      let acc = value;
      for (let round = 0; round < 4; round++) acc = ((acc * 31 + round) % 1_000_003 + (round % 2 ? 0 : round + 1)) % 1_000_003;
      return { status: 'completed', value: { acc, stable: true } };
    };
    const outcomes = await Promise.all([...nodeIds, ...quickjsIds].map((runId) => settle(runId, 50_000)));
    expect(outcomes).toEqual([...values, ...values.slice(0, 10)].map(expected));
  }, 60_000);
});

describe('control flow and values', () => {
  it('Promise.all, race, allSettled and any over steps and sleeps', () => expectParity('combinators'));
  it('try/catch/finally, FatalError, RetryableError and causes', () => expectParity('errorFlow', 3));
  it('Map, Set, Date, RegExp, typed arrays, URL, Headers and BigInt across steps', () => expectParity('richValues'));
  it('classes, private fields, generators, async iterators, symbols and BigInt in workflow code', () => expectParity('language', 3));
  it('2,000 sequential steps', async () => {
    // SDK 5.8's run-ahead costs QuickJS time that grows with the log: 2,000 steps take about 45 s
    // with it and 5 s without. Parity is what this checks, so it runs without.
    process.env.WORKFLOW_RUN_AHEAD_DEPTH = '0';
    try {
      await expectParity('manySteps', 2000);
    } finally {
      delete process.env.WORKFLOW_RUN_AHEAD_DEPTH;
    }
  }, 60_000);
  it('recursion 1,000 frames deep', () => expectParity('recursion', 1000));
  it('recursion through 50 awaited steps', () => expectParity('asyncRecursion', 50));
  it('node: catches a stack overflow in workflow code', async () => {
    const [runId] = await startOn('node', 'recursion', [100_000]);
    expect(await settle(runId)).toEqual({ status: 'completed', value: 'caught:RangeError' });
  });

  // Divergence (QuickJS bug): BigInts outside the int64 range wrap modulo 2^64 when they cross a step boundary.
  it.fails('BigInts outside the int64 range across steps', () => expectParity('bigints'));
  // Divergence (QuickJS bug): QuickJS overflows the host stack at ~2,000 frames and fails the run; the node engine recurses.
  it.fails('recursion 5,000 frames deep', () => expectParity('recursion', 5000));
});

describe('hooks, sleeps, streams and child runs', () => {
  it('hook payloads, a stream, a sleep and a child run started from a step', async () => {
    const { quickjs, node, ids } = await onBoth('hooksStreamsChild', 4, async (runId) => {
      const token = await hookToken(runId);
      for (const payload of [1, 2, 3]) await resumeHook(token, payload);
    });
    for (const engine of ENGINES) {
      const result = { quickjs, node }[engine];
      const { childRunId, ...value } = result.value as { childRunId: string };
      expect(value).toEqual({ payloads: [1, 2, 3], childResult: 5 });
      // A child takes the starting Worker's WORKFLOW_VM, not its parent's engine.
      expect((await world.runs.get(childRunId)).executionContext).toMatchObject({ workflowVm: 'node' });
      const stream = getRun(ids[engine]).getReadable<Uint8Array>({ namespace: 'stress' });
      expect(await new Response(stream).text()).toBe('1\n2\n3\n');
      result.value = value;
    }
    expect(node).toEqual(quickjs);
  });

  const resumeOnce = async (runId: string) => { await resumeHook(await hookToken(runId), 9); };
  it('leaving a for await over a hook disposes it', () => expectParity('hookBreak', 0, resumeOnce));
  it('an unused AbortController', () => expectParity('unusedAbortController'));
});

describe('forbidden and host APIs', () => {
  it('an uncaught setTimeout fails the run on both engines', async () => {
    const { quickjs, node } = await onBoth('uncaught', 0);
    expect(quickjs.status).toBe('failed');
    expect(node).toEqual(quickjs);
  });

  // Divergence: QuickJS runs the bundle as a sloppy script, so the SDK's bundled `var fetch` (its fetch step)
  // becomes globalThis.fetch and the call starts a step; the node engine's bundle keeps it local and throws.
  it.fails('an uncaught fetch fails the run on both engines', async () => {
    const { quickjs, node } = await onBoth('uncaught', 1);
    expect(quickjs.status).toBe('failed');
    expect(node).toEqual(quickjs);
  });
});

describe('global surface', () => {
  // Each probe in the `surface` workflow, and why the engines disagree on it.
  const divergences: Record<string, string | undefined> = {
    // Timers: the node engine throws the SDK's WorkflowRuntimeError; QuickJS has no such global (TypeError).
    'setTimeout()': 'error class', 'setInterval()': 'error class', 'setImmediate()': 'error class',
    // The node engine throws the SDK's fetch error; on QuickJS globalThis.fetch is the bundle's `var fetch` step, or absent
    // (TypeError) when some workflow names `fetch` bare and esbuild renames the var.
    'fetch()': 'bundle var on QuickJS',
    // QuickJS has queueMicrotask and performance (with a replay-stable clock); the node engine's sandbox has neither.
    'queueMicrotask()': 'missing on node', 'performance.now()': 'missing on node',
    // Workers forbid code generation; QuickJS evaluates strings. The probe takes Function from a function, because
    // vitest-pool-workers proxies the global Function to allow `new Function`, which production rejects too.
    'indirect eval()': 'EvalError on node', 'Function constructor': 'EvalError on node',
    // Both reject import(): QuickJS finds no module, and the node engine's build lowers import() to a throwing call.
    'import(cloudflare:workers)': undefined, 'import(node:timers)': undefined,
    // The SDK hides WeakRef and FinalizationRegistry on node (as on node:vm) but not on QuickJS.
    'typeof WeakRef': 'hidden on node', 'typeof FinalizationRegistry': 'hidden on node',
    // QuickJS has no WebAssembly or Atomics.
    'typeof WebAssembly': 'missing on QuickJS', 'typeof Atomics': 'missing on QuickJS',
    // QuickJS has no ICU, so it throws on explicit locales and formats differently without one.
    'Intl.NumberFormat': 'no ICU on QuickJS', 'toLocaleString(locale)': 'no ICU on QuickJS',
    'toLocaleString()': 'no ICU on QuickJS', 'Date#toString': 'no ICU on QuickJS',
    // QuickJS throws on crypto.subtle.digest; the node engine computes it as node:vm does.
    'crypto.subtle.digest': 'missing on QuickJS',
    // Strict mode: the node engine's bundle runs strict, QuickJS sloppy.
    'undeclared assignment': 'strict mode', 'this in plain call': 'strict mode', 'arguments.callee': 'strict mode',
    'frozen write': 'strict mode', 'process.env write': 'strict mode', 'delete non-configurable': 'strict mode',
    'primitive property write': 'strict mode',
    // The node engine binds bare globals once and keeps top-level declarations local to the factory.
    'globalThis write then bare read': 'globals resolve once', 'top-level function on globalThis': 'module scope',
    // Bug in QuickJS: URL has no searchParams.
    'URL#searchParams': 'missing on QuickJS',
    'RegExp v flag': undefined,
    'Date.prototype.constructor': undefined, 'Object.keys(Math)': undefined,
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
      expect(results.get('node')?.[probe]).toBe(results.get('quickjs')?.[probe]);
    });
  }
});

describe('scale', () => {
  it('QuickJS: Promise.all over 200 steps', async () => {
    const [runId] = await startOn('quickjs', 'parallelSteps', [200]);
    expect(await settle(runId)).toEqual({ status: 'completed', value: 200 });
  });

  // Step deliveries must not exhaust Workers' 10,000 active timeouts or reject unhandled.
  it('node: Promise.all over 200 steps', async () => {
    // Muted: the failed deliveries log tens of thousands of errors, which overflow the test pool's socket.
    using errors = vi.spyOn(console, 'error').mockImplementation(() => {});
    // Some also reject unhandled. A listener stops vitest failing the file on them; it stays installed
    // because a hung run keeps rejecting after this test ends.
    let unhandled = 0;
    process.on('unhandledRejection', () => { unhandled++; });
    const [runId] = await startOn('node', 'parallelSteps', [200]);
    const outcome = await settle(runId, 15_000);
    const quotaErrors = errors.mock.calls.filter((args) => String(args[0]).includes('QuotaExceededError')).length;
    expect({ outcome, quotaErrors, unhandled }).toEqual({ outcome: { status: 'completed', value: 200 }, quotaErrors: 0, unhandled: 0 });
  }, 30_000);
});
