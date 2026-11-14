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

/** A context as the SDK prepares it, before it runs the bundle. */
function sdkContext() {
  const context = createContext();
  Object.assign(context, { crypto, console });
  return context;
}

it('runs a registered bundle against the context, and the SDK lookup script', () => {
  const code = 'the bundle source';
  registerWorkflowBundle(code, ['Headers', 'fetch'], function (globalThis: Record<PropertyKey, unknown>, _import: unknown, Headers: unknown, fetch: unknown) {
    let calls = 0;
    globalThis.__private_workflows = new Map([['wf', () => ({ calls: ++calls, Headers, fetch })]]);
  });
  const run = () => {
    const context = sdkContext();
    context.Headers = 'the sandbox Headers';
    new Script(code).runInContext(context);
    return (new Script('globalThis.__private_workflows?.get("wf")').runInContext(context) as () => unknown)();
  };
  // Module state starts over in each context, and globals come from the context, not the host.
  expect(run()).toEqual({ calls: 1, Headers: 'the sandbox Headers', fetch: undefined });
  expect(run()).toEqual({ calls: 1, Headers: 'the sandbox Headers', fetch: undefined });
});

it('keeps writes in the run: its own crypto and console, frozen shared built-ins', () => {
  const code = 'the writing bundle';
  const writes: Record<string, unknown> = {};
  registerWorkflowBundle(code, ['crypto', 'console'], function (_g: unknown, _import: unknown, ...globals: unknown[]) {
    const [crypto, console] = globals as Record<string, unknown>[];
    crypto.randomUUID = () => 'mine';
    console.log = () => 'mine';
    try {
      Object.defineProperty(Array.prototype, '__mark', { value: 1 });
    } catch (error) {
      writes.array = (error as Error).name;
    }
    // Assigning an inherited property still works on a frozen prototype.
    class Named extends Error { constructor() { super('m'); this.name = 'Named'; } }
    writes.error = `${new Named()}`;
  });
  new Script(code).runInContext(sdkContext());
  expect(writes).toEqual({ array: 'TypeError', error: 'Named: m' });
  expect(crypto.randomUUID()).not.toBe('mine');
  expect(console.log).not.toBe(undefined);
  expect(Object.isFrozen(Array.prototype)).toBe(true);
  expect(() => { (Object.prototype as Record<string, unknown>).toString = () => ''; }).toThrow(TypeError);
  expect(Object.getOwnPropertyNames(Array.prototype)).not.toContain('__mark');
});

it('rejects code that was not compiled into the deployment', () => {
  expect(() => new Script('globalThis.dynamic = 1').runInContext(createContext())).toThrow(/WORKFLOW_VM=quickjs/);
  expect(() => runInContext('1 + 1', createContext())).toThrow(/WORKFLOW_VM=quickjs/);
});
