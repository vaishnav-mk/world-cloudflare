# Isolate engine

The isolate engine runs `"use workflow"` code natively in V8 on Workers instead of in the QuickJS WebAssembly interpreter. It is the SDK's own node engine (`WORKFLOW_VM=node`, or `WORKFLOW_VM` unset) on a `node:vm` that the World's build provides, so it needs no SDK changes. QuickJS stays the default in the Wrangler templates. The engine is stamped on each run when it starts, so existing runs keep their engine when the variable changes.

Builds with the old SDK patch called it `WORKFLOW_VM=isolate`. The published SDK knows only `node` and `quickjs` and fails a run stamped `isolate`, so let such runs finish on the executor that started them.

## Why

The SDK replays workflow code from the event log, so each replay needs fresh globals and deterministic `Date`, `Math.random` and `crypto`. On Node it uses `node:vm` and evaluates the workflow bundle from a string. Workers have no `node:vm` and block `eval`, and patching the real globals would leak across concurrent requests in one isolate. QuickJS gets around all three by being a separate engine, at the cost of interpreting.

## How it works

- **Build.** `@workflow/world-cloudflare/build` compiles the workflow bundle a second time, into a function whose parameters shadow every ECMAScript, workerd and Node global (`SHADOWED_GLOBALS` in `build.ts`), and aliases `node:vm` to `vm.ts`. V8 compiles the function with the Worker, so nothing is evaluated from a string at run time. esbuild lowers the bundle's `import()` calls to `require()`, which the build points at a function that throws, so workflow code cannot load the Worker's modules (`cloudflare:workers`, `node:timers`).
- **`node:vm`.** The SDK calls `createContext()`, `runInContext('globalThis', context)`, `new Script(code)` and `script.runInContext(context)`, with no timeout or microtask options. `vm.ts` returns a fresh sandbox object for the context, on which the SDK installs its deterministic globals as it does on Node. Running the bundle's `Script` calls the function with the sandbox, so the bundle's top level runs again and module state starts clean; the SDK's lookup script reads the sandbox. Other code (a dynamic workflow) throws. Like node:vm without options, replays share the host's microtask queue, and the SDK bounds replays with its own wall-clock budget.
- **What a run can write stays in the run.** The sandbox shares the host realm, so it cannot have its own `Array.prototype`: `[]` always makes the realm's arrays. Instead:
  - namespaces (`Math`, `JSON`, `Reflect`, `Intl`, `Atomics`, `WebAssembly`, `console`, `crypto`) and `Date` are copies per sandbox. The copies of `Date.prototype` and `Intl.DateTimeFormat` read the run's clock, so `new (new Date().constructor)()` and `Intl.DateTimeFormat().format()` replay identically;
  - the built-ins runs share (constructors, prototypes, the host classes the SDK passes in, and hidden ones such as `%GeneratorFunction%`) are frozen for the whole isolate before its first replay on this engine, so a write to one throws. Frozen prototypes keep the usual overrides working: an object can still assign its own `constructor`, `toString`, `valueOf`, `name` or `message`.

  Reverting each run's writes after its replay would not do: replays of other runs interleave at every `await`, host code (the World, the SDK, steps) runs in between and would see the writes, and a suspended replay never reaches its end. Freezing holds under any interleaving, and costs about 20 ms once per isolate and 60 µs per replay in Node.
- **Everything else** is the SDK's node engine: its replay loop, retained sessions, suspension handling and batched writes.

## Measurements

On a Worker running flows in-process, one run at a time:

| | QuickJS | Isolate |
| --- | --- | --- |
| Full replay at 30,000 events | 54 s (81 ms from a snapshot) | 2.6 s |
| 30,000 sequential steps | 2,101 s | 2,076 s |
| Largest step output | 16 MiB | 16 MiB; 20 MiB runs the executor out of memory |

With VM snapshots, QuickJS no longer replays the whole log, so the two engines finish long sequential runs in about the same time. The isolate engine has no snapshots and replays the whole log natively at each new invocation, which stays cheap but grows with the log. These were measured with the old SDK patch.

Step cost against log size on the published SDK 5.5.0, locally (`scripts/perf/run-steps.mjs 10000`, 30,003 events):

| | ~3,000 events | ~27,000 events | Whole run |
| --- | --- | --- | --- |
| Isolate | 7.0 ms per step | 15.8 ms | 117 s |
| Isolate with the event-log memo | 6.2 ms | 6.9 ms | 68 s |
| QuickJS, either way | 1.6 to 1.9 ms | 1.6 to 2.3 ms | 18 s |

Spec 8's node engine rereads the whole log at every step, so its step cost grows with the log. The memo from the old SDK patch removes that; it is prepared as an upstream change.

## Limits

- **Shared built-ins are read-only** after the first replay on this engine, for workflow code and for host code in the isolate: a write to `Array.prototype`, `Promise` or `TextEncoder.prototype` throws a `TypeError`. QuickJS lets each run change its own. `Error.stackTraceLimit` and `Error.prepareStackTrace` stay writable, because libraries and test runners set them around a stack capture, so workflow code can set them too. Like node:vm, this is not a security boundary.
- **Strict mode.** The bundle runs in strict mode, where `node:vm` ran it sloppy, so assigning an undeclared variable throws.
- **Globals resolve once.** A bare global name is bound when the bundle runs, so writing `globalThis.x` and then reading `x` does not see the write. A host global missing from the shadow list stays visible, and the engine logs one warning listing them.
- **Only the deployed bundle.** Dynamic workflow code needs QuickJS.
- **Not proven by the SDK's own determinism suites**, only by this repo's end-to-end tests and the stress suite below.

## Stress suite

`test/stress.test.ts` runs each workflow in `workflows/stress.ts` on QuickJS and on the isolate engine in one process (a run keeps the engine `start()` stamped) and compares the outcome and the event log, grouped by step, wait and hook so queue timing drops out. Run it with `pnpm exec vitest run --config vitest.isolate.config.ts test/stress.test.ts`. Each known bug or divergence is an `it.fails` test; flip it to `it` once fixed.

Fixed: shared built-ins leaking to the host and other runs (now per-run copies or frozen), assignments to `crypto` reaching the host, `import()` of host modules, and the clock and random escapes (`new (new Date().constructor)()`, `Object.getPrototypeOf(Math).random()`, `Intl.DateTimeFormat().format()`). A wide `Promise.all` (200 steps) and the hook event-log differences were fixed in the SDK.

Open:

- **Slower sequential steps.** Locally a step takes 6 to 7 ms on isolate and under 2 ms on QuickJS, both with one replay per invocation.

Divergences that follow from the design, from QuickJS or from Workers:

- **Writes to shared built-ins throw** on isolate (above), where on QuickJS a run marks its own copy.
- **Sloppy patterns throw** under strict mode: undeclared assignment, `arguments.callee`, writes to frozen objects, `process.env` or primitives, and deleting non-configurable properties; `this` in a plain call is `undefined`.
- **Top-level declarations stay local.** On QuickJS they become globals, including the SDK's bundled `var fetch` step: there `fetch(url)` starts a step unless some workflow names `fetch` bare (esbuild then renames the var), where isolate throws the SDK's fetch error.
- **Missing on isolate:** `queueMicrotask`, `performance`, string evaluation (`eval`, `Function`), `WeakRef` and `FinalizationRegistry`. Timers throw `WorkflowRuntimeError` where QuickJS has no such global. Under vitest `new Function` works, because vitest-pool-workers proxies `Function`.
- **Missing on QuickJS:** ICU (`Intl`, explicit locales, `Date#toString` suffix), `crypto.subtle.digest`, `WebAssembly`, `Atomics`, `URL#searchParams`, and, in the SDK's quickjs-wasi 3.4.0, the RegExp `v` flag.
- **QuickJS bugs the isolate engine does not have:** BigInts outside the int64 range wrap modulo 2^64 across a step boundary, and recursion past about 2,000 frames overflows the host stack and fails the run, even inside `try`.
- A child run started from a step takes the Worker's `WORKFLOW_VM`, not its parent's engine.

## Dynamic Workers

Running the engine in a fresh Dynamic Worker per invocation was measured and rejected. Each load costs $0.002 beyond 1,000 a month and about 3 s cold. The replay loop needs a fresh context per replay anyway, which the build-time function already gives. Every World call becomes an RPC back to the host. A Dynamic Worker would add real isolation of built-ins, so hosting the same engine in one reused per deployment (`LOADER.get()`) is the option if that matters.
