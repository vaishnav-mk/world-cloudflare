import { expect, it } from 'vitest';
import { createContext, registerWorkflowBundle, runInContext, Script } from '../packages/world-cloudflare/src/vm.js';

it('gives each context its own copies of the namespaces the SDK changes', () => {
  const g = runInContext('globalThis', createContext()) as typeof globalThis;
  // What the SDK's createContext does to a fresh context.
  g.Math.random = () => 0.5;
  delete (g.WebAssembly as Partial<typeof WebAssembly>).compile;
  // SAFETY: Atomics.waitAsync is ES2024, past this project's lib.
  const atomics = (realm: typeof globalThis) => realm.Atomics as unknown as Record<string, unknown>;
  delete atomics(g).waitAsync;
  expect([g.Math.random(), g.WebAssembly.compile, atomics(g).waitAsync]).toEqual([0.5, undefined, undefined]);
  expect(Math.random).not.toBe(g.Math.random);
  expect(typeof WebAssembly.compile).toBe('function');
  expect(typeof atomics(globalThis).waitAsync).toBe('function');
  expect(g.Math.PI).toBe(Math.PI);
  expect(g.globalThis).toBe(g);
  expect(createContext().Math).not.toBe(g.Math);
});

it('runs a registered bundle against the context, and the SDK lookup script', () => {
  const code = 'the bundle source';
  registerWorkflowBundle(code, ['Date', 'fetch'], function (globalThis: Record<string, unknown>, Date: unknown, fetch: unknown) {
    let calls = 0;
    globalThis.__private_workflows = new Map([['wf', () => ({ calls: ++calls, Date, fetch })]]);
  });
  const run = () => {
    const context = createContext();
    context.Date = 'the sandbox Date';
    new Script(code).runInContext(context);
    return (new Script('globalThis.__private_workflows?.get("wf")').runInContext(context) as () => unknown)();
  };
  // Module state starts over in each context, and globals come from the context, not the host.
  expect(run()).toEqual({ calls: 1, Date: 'the sandbox Date', fetch: undefined });
  expect(run()).toEqual({ calls: 1, Date: 'the sandbox Date', fetch: undefined });
});

it('rejects code that was not compiled into the deployment', () => {
  expect(() => new Script('globalThis.dynamic = 1').runInContext(createContext())).toThrow(/WORKFLOW_VM=quickjs/);
  expect(() => runInContext('1 + 1', createContext())).toThrow(/WORKFLOW_VM=quickjs/);
});
