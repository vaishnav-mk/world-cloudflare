# Native engine

The SDK's `node` engine (`WORKFLOW_VM=node`, or `WORKFLOW_VM` unset) runs `"use workflow"` code natively in V8, where the `quickjs` engine interprets it in a WebAssembly build of QuickJS. On Node the engine uses `node:vm`; on Workers it runs on `vm.ts`, a `node:vm` that the World's build provides, so it needs no SDK changes. The Wrangler templates default to QuickJS. The engine is stamped on each run when it starts, so existing runs keep theirs when the variable changes.

[Limits](limits.md#summary) has the node engine and QuickJS measurements.

## Why a shim

The SDK replays workflow code from the event log, so each replay needs fresh globals and deterministic `Date`, `Math.random` and `crypto`. On Node it uses `node:vm` and evaluates the workflow bundle from a string. Workers have no `node:vm` and block `eval`, and patching the real globals would leak across concurrent requests in one isolate. QuickJS avoids all three by being a separate engine, at the cost of interpreting.

## How it works

- **Build.** `@workflow/world-cloudflare/build` compiles the workflow bundle a second time, into a function whose parameters shadow every ECMAScript, workerd and Node global (`SHADOWED_GLOBALS` in `build.ts`), and aliases `node:vm` to `vm.ts`. V8 compiles the function with the Worker, so nothing is evaluated from a string at run time. esbuild lowers the bundle's `import()` calls to `require()`, which the build points at a function that throws, so workflow code cannot load the Worker's modules (`cloudflare:workers`, `node:timers`).
- **`node:vm`.** The SDK calls `createContext()`, `runInContext('globalThis', context)`, `new Script(code)` and `script.runInContext(context)`, with no timeout or microtask options. `vm.ts` returns a fresh sandbox object for the context, on which the SDK installs its deterministic globals as it does on Node. Running the bundle's `Script` calls the function with the sandbox, so the bundle's top level runs again and module state starts clean; the SDK's lookup script reads the sandbox. Other code, such as a dynamic workflow, throws. As with `node:vm` without options, replays share the host's microtask queue, and the SDK bounds replays with its own wall-clock budget.
- **Most mutable globals stay in the run.** The sandbox shares the host realm, so it cannot have its own `Array.prototype`: `[]` always makes the realm's arrays. Instead:
  - namespaces (`Math`, `JSON`, `Reflect`, `Intl`, `Atomics`, `WebAssembly`, `console`, `crypto`) and `Date` are copies per sandbox. The copies of `Date.prototype` and `Intl.DateTimeFormat` read the run's clock, so `new (new Date().constructor)()` and `Intl.DateTimeFormat().format()` replay identically;
  - the built-ins runs share (constructors, prototypes, the host classes the SDK passes in, and hidden ones such as `%GeneratorFunction%`) are frozen for the whole isolate before its first replay on this engine, so a write to one throws. Frozen prototypes keep the usual overrides working: an object can still assign its own `constructor`, `toString`, `valueOf`, `name` or `message`. Freezing costs about 20 ms once per isolate and 60 µs per replay (local, in Node, `44c3515`).

  Reverting each run's writes after its replay would not do: replays of other runs interleave at every `await`, host code (the World, the SDK, steps) runs in between and would see the writes, and a suspended replay never reaches its end. Freezing holds under any interleaving.
- **Everything else** is the SDK's node engine: its replay loop, retained sessions, suspension handling and batched writes.

## Limits

- **Shared built-ins are read-only** after the first replay on this engine, for workflow code and for host code in the isolate: a write to `Array.prototype`, `Promise` or `TextEncoder.prototype` throws a `TypeError`. QuickJS lets each run change its own. `Error.stackTraceLimit` and `Error.prepareStackTrace` stay writable, because libraries and test runners set them around a stack capture, so workflow code can set them too. Those two settings are shared with other runs and host requests, including while a replay is suspended. Do not change them in workflows; use QuickJS when code needs independent built-ins. This shim is not a security boundary.
- **Strict mode.** The bundle runs in strict mode, where `node:vm` ran it sloppy, so assigning an undeclared variable throws.
- **Globals resolve once.** A bare global name is bound when the bundle runs, so writing `globalThis.x` and then reading `x` does not see the write. A host global missing from the shadow list stays visible, and the engine logs one warning listing them.
- **Only the deployed bundle.** Dynamic workflow code needs QuickJS.
- **No VM snapshots.** Each new invocation replays the whole log natively, so replay grows with the log: 3.9 s after 10,000 steps, where QuickJS restores a snapshot in 185 ms (Workers, `adff891`).
- **Not proven by the SDK's own determinism suites**, only by this repo's end-to-end tests and the stress suite below.

## Stress suite

`test/stress.test.ts` runs each workflow in `workflows/stress.ts` on both engines in one process (a run keeps the engine `start()` stamped) and compares the outcome and the event log, grouped by step, wait and hook so queue timing drops out. Run it with `pnpm exec vitest run --config vitest.node.config.ts test/stress.test.ts`. Each known bug or divergence is an `it.fails` test; flip it to `it` once fixed.

Fixed: most shared built-in writes leaking to the host and other runs (now per-run copies or frozen, except the `Error` settings above), assignments to `crypto` reaching the host, `import()` of host modules, and the clock and random escapes (`new (new Date().constructor)()`, `Object.getPrototypeOf(Math).random()`, `Intl.DateTimeFormat().format()`). A wide `Promise.all` (200 steps) and the hook event-log differences were fixed in the SDK.

Open: replay time grows with the log, because the engine has no VM snapshots ([limits](#limits)).

Divergences that follow from the design, from QuickJS or from Workers:

- **Writes to shared built-ins throw** on the node engine (above), where on QuickJS a run marks its own copy.
- **Sloppy patterns throw** under strict mode: undeclared assignment, `arguments.callee`, writes to frozen objects, `process.env` or primitives, and deleting non-configurable properties; `this` in a plain call is `undefined`.
- **Top-level declarations stay local.** On QuickJS they become globals, including the SDK's bundled `var fetch` step: there `fetch(url)` starts a step unless some workflow names `fetch` bare (esbuild then renames the var), where the node engine throws the SDK's fetch error.
- **Missing on the node engine:** `queueMicrotask`, `performance`, string evaluation (`eval`, `Function`), `WeakRef` and `FinalizationRegistry`. Timers throw `WorkflowRuntimeError` where QuickJS has no such global. Under vitest `new Function` works, because vitest-pool-workers proxies `Function`.
- **Missing on QuickJS:** ICU (`Intl`, explicit locales, `Date#toString` suffix), `crypto.subtle.digest`, `WebAssembly`, `Atomics`, `URL#searchParams`, and, in the SDK's quickjs-wasi 3.4.0, the RegExp `v` flag.
- **QuickJS bugs the node engine does not have:** BigInts outside the int64 range wrap modulo 2^64 across a step boundary, and recursion past about 2,000 frames overflows the host stack and fails the run, even inside `try`.
- A child run started from a step takes the Worker's `WORKFLOW_VM`, not its parent's engine.

## Dynamic Workers

Running the engine in a fresh Dynamic Worker per invocation was tried and rejected. Each load is billed and slow to start cold, the replay loop needs a fresh context per replay anyway, which the build-time function already gives, and every World call would become an RPC back to the host. A Dynamic Worker would add real isolation of built-ins, so hosting the same engine in one reused per deployment (`LOADER.get()`) is the option if that matters.
