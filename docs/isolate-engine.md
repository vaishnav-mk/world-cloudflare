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
- **Not proven by the SDK's own determinism suites**, only by this repo's end-to-end tests.

## Dynamic Workers

Running the engine in a fresh Dynamic Worker per invocation was measured and rejected. Each load costs $0.002 beyond 1,000 a month and about 3 s cold. The replay loop needs a fresh context per replay anyway, which the build-time function already gives. Every World call becomes an RPC back to the host. A Dynamic Worker would add real isolation of built-ins, so hosting the same engine in one reused per deployment (`LOADER.get()`) is the option if that matters.
