# Isolate engine

`WORKFLOW_VM=isolate` runs `"use workflow"` code natively in V8 on Workers instead of in the QuickJS WebAssembly interpreter. QuickJS stays the default. The engine is stamped on each run when it starts, so existing runs keep their engine when the variable changes.

## Why

The SDK replays workflow code from the event log, so each replay needs fresh globals and deterministic `Date`, `Math.random` and `crypto`. On Node it uses `node:vm` and evaluates the workflow bundle from a string. Workers have no `node:vm` and block `eval`, and patching the real globals would leak across concurrent requests in one isolate. QuickJS gets around all three by being a separate engine, at the cost of interpreting.

## How it works

- **Build.** `workflow build --target cloudflare-workers` also compiles the workflow bundle into a function whose parameters shadow every ECMAScript, workerd and Node global (`builders/src/isolate-globals.ts`). V8 compiles it with the Worker, so nothing is evaluated from a string at run time.
- **Each replay** builds a fresh sandbox object with the host's intrinsics and the same deterministic globals `node:vm` installs (`core/src/vm/isolate.ts`), then calls the function with it. The bundle's top level runs again, so module state starts clean.
- **Everything else** is the SDK's node engine: its replay loop, retained sessions, suspension handling and batched writes.

The changes are in `patches/workflow-sdk-cloudflare-workers.patch`.

## Measurements

On a Worker running flows in-process, one run at a time:

| | QuickJS | Isolate |
| --- | --- | --- |
| Full replay at 30,000 events | 54 s (81 ms from a snapshot) | 2.6 s |
| 30,000 sequential steps | 2,101 s | 2,076 s |
| Largest step output | 16 MiB | 20 MiB |

With VM snapshots, QuickJS no longer replays the whole log, so the two engines finish long sequential runs in about the same time. The isolate engine has no snapshots and replays the whole log natively at each new invocation, which stays cheap but grows with the log.

## Limits

- **Shared intrinsics.** Workflow code that patches `Array.prototype` or another built-in changes it for the host and for other runs in the isolate. Like `node:vm`, this is not a security boundary.
- **Strict mode.** The bundle runs in strict mode, where `node:vm` ran it sloppy, so assigning an undeclared variable throws.
- **Globals resolve once.** A bare global name is bound when the bundle runs, so writing `globalThis.x` and then reading `x` does not see the write. A host global missing from the shadow list stays visible, and the engine logs one warning listing them.
- **Only the deployed bundle.** Dynamic workflow code needs QuickJS.
- **Not proven by the SDK's own determinism suites**, only by this repo's end-to-end tests and the stress suite below.

## Stress suite

`test/stress.test.ts` runs each workflow in `workflows/stress.ts` on QuickJS and on the isolate engine in one process (a run keeps the engine `start()` stamped) and compares the outcome and the event log, grouped by step, wait and hook so queue timing drops out. Run it with `pnpm exec vitest run --config vitest.isolate.config.ts test/stress.test.ts`. Each known bug or divergence is an `it.fails` test; flip it to `it` once fixed.

Bugs in the isolate engine, worst first:

1. **Wide `Promise.all` hangs.** Each step delivery waits out earlier cascades with `setImmediate` turns (`quiesceEarlierCascades`). With about 120 or more steps in one `Promise.all` the turns exceed Workers' 10,000 active timeouts: deliveries throw `QuotaExceededError`, some promises reject unhandled, and from about 135 steps the run usually hangs with one step never started. 1,000 steps ran workerd out of heap. 200 parallel sleeps finish, 12 times slower than QuickJS, after 30,000 logged errors. QuickJS finishes 1,000 parallel steps, in 220 s. (`scale`)
2. **Shared built-ins leak.** A property set on `Array.prototype`, `Object.prototype`, `JSON`, `Promise`, `Promise.prototype`, `Reflect`, `console`, `Uint8Array.prototype`, `TextEncoder.prototype` or `crypto` is visible to the host and to other runs, and a `JSON.stringify` patch runs inside the World's own `toJson`. A patch made before a suspension stays on the host after the run ends unless a later replay undoes it, because a suspended execution never reaches its `finally`. `Math`, `Date` and `globalThis` are per run. (`leaks through shared built-ins`)
3. **Assignments to `crypto` reach the host.** The sandbox `crypto` is a `Proxy` without a `set` trap, so `crypto.randomUUID = f` replaces it for every caller in the isolate.
4. **`import()` reaches host modules**, such as `cloudflare:workers` (the Worker's bindings) and `node:timers` (real timers). QuickJS rejects them.
5. **Clock and random escapes.** `new (new Date().constructor)()` reads the wall clock, `Object.getPrototypeOf(Math).random()` is unseeded, and `Intl.DateTimeFormat().format()` formats the host's now. A workflow that branches on one fails with `CorruptedEventLogError` after replay divergence. The documented APIs (`Date`, `Math.random`, `crypto.randomUUID`, `crypto.getRandomValues`) replay identically on both engines and move the clock at the same events.
6. **Event log differences.** Leaving a `for await` over a hook does not dispose it (QuickJS writes `hook_disposed`), and an `AbortController` that is never aborted writes `hook_created` and `hook_disposed`.
7. **Slower sequential steps.** Locally, 2,000 sequential steps take 13.6 s on isolate and 2.8 s on QuickJS, though both use one replay.

Divergences that follow from the design, from QuickJS or from Workers:

- **Sloppy patterns throw** under strict mode: undeclared assignment, `arguments.callee`, writes to frozen objects, `process.env` or primitives, and deleting non-configurable properties; `this` in a plain call is `undefined`.
- **Top-level declarations stay local.** On QuickJS they become globals, including the SDK's bundled `var fetch` step: there `fetch(url)` starts a step unless some workflow names `fetch` bare (esbuild then renames the var), where isolate throws the SDK's fetch error.
- **Missing on isolate:** `queueMicrotask`, `performance`, string evaluation (`eval`, `Function`), `WeakRef` and `FinalizationRegistry`. Timers throw `WorkflowRuntimeError` where QuickJS has no such global. Under vitest `new Function` works, because vitest-pool-workers proxies `Function`.
- **Missing on QuickJS:** ICU (`Intl`, explicit locales, `Date#toString` suffix), `crypto.subtle.digest`, `WebAssembly`, `Atomics`, the RegExp `v` flag, and `URL#searchParams`.
- **QuickJS bugs the isolate engine does not have:** BigInts outside the int64 range wrap modulo 2^64 across a step boundary, and recursion past about 2,000 frames overflows the host stack and fails the run, even inside `try`.
- A child run started from a step takes the Worker's `WORKFLOW_VM`, not its parent's engine.

## Dynamic Workers

Running the engine in a fresh Dynamic Worker per invocation was measured and rejected. Each load costs $0.002 beyond 1,000 a month and about 3 s cold. The replay loop needs a fresh context per replay anyway, which the build-time function already gives. Every World call becomes an RPC back to the host. A Dynamic Worker would add real isolation of built-ins, so hosting the same engine in one reused per deployment (`LOADER.get()`) is the option if that matters.
