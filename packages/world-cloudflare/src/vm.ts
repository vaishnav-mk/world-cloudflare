/**
 * `node:vm` for the Workflow SDK's node engine on Workers, which have neither `node:vm` nor `eval`.
 *
 * The build (`build.ts`) aliases `node:vm` to this module and compiles the workflow bundle a second
 * time, into a function whose parameters shadow every global, registered with `registerWorkflowBundle`.
 * The SDK's `createContext()` gets a fresh sandbox object, and running the bundle's `Script` against
 * it calls that function with the sandbox, so each replay starts with clean module state and sees
 * only the deterministic globals the SDK installs on the sandbox.
 *
 * Unlike node:vm, the sandbox shares the host's intrinsics (`Object.prototype`, `Array.prototype`...),
 * so workflow code that patches them changes them for the whole isolate. Neither is a security boundary.
 */

type Sandbox = Record<string, unknown>;
type Bundle = { names: readonly string[]; evaluate: (globalThis: Sandbox, ...globals: unknown[]) => void };

/** ECMAScript globals a fresh node:vm context has, copied from the host. */
const INTRINSICS = [
  'Object', 'Function', 'Array', 'Number', 'parseFloat', 'parseInt', 'Infinity', 'NaN', 'undefined', 'Boolean',
  'String', 'Symbol', 'Date', 'Promise', 'RegExp', 'Error', 'AggregateError', 'EvalError', 'RangeError',
  'ReferenceError', 'SyntaxError', 'TypeError', 'URIError', 'JSON', 'Math', 'Intl', 'ArrayBuffer', 'Atomics',
  'Uint8Array', 'Int8Array', 'Uint16Array', 'Int16Array', 'Uint32Array', 'Int32Array', 'BigUint64Array',
  'BigInt64Array', 'Uint8ClampedArray', 'Float32Array', 'Float64Array', 'Float16Array', 'DataView', 'Map', 'BigInt',
  'Set', 'Iterator', 'WeakMap', 'WeakSet', 'Proxy', 'Reflect', 'FinalizationRegistry', 'WeakRef', 'decodeURI',
  'decodeURIComponent', 'encodeURI', 'encodeURIComponent', 'escape', 'unescape', 'isFinite', 'isNaN',
  'SuppressedError', 'DisposableStack', 'AsyncDisposableStack', 'SharedArrayBuffer', 'WebAssembly',
];
/** Namespaces the SDK changes in place (`Math.random`, `delete WebAssembly.compile`): each sandbox gets its own copy. */
const COPIED = new Set(['Math', 'Atomics', 'WebAssembly']);

/** Matches the SDK's `globalThis.__private_workflows?.get("<name>")` lookup script. */
const LOOKUP = /^globalThis\.__private_workflows\?\.get\(("(?:[^"\\]|\\.)*")\)$/;

// On globalThis, so every bundled copy of this module, and the tests, see the registered bundles.
// SAFETY: only this module writes the key, and always a Map of bundles.
const bundles = ((globalThis as unknown as Record<symbol, Map<string, Bundle>>)[
  Symbol.for('@workflow/world-cloudflare//workflowBundles')
] ??= new Map());

/** Registers the factory the build compiled `code` into. `names` are its parameters after `globalThis`. */
export function registerWorkflowBundle(code: string, names: readonly string[], evaluate: Bundle['evaluate']): void {
  bundles.set(code, { names, evaluate });
}

export function createContext(): Sandbox {
  const host = globalThis as unknown as Sandbox;
  const sandbox: Sandbox = {};
  for (const name of INTRINSICS) {
    if (!(name in host)) continue;
    const value = host[name] as object;
    sandbox[name] = COPIED.has(name) ? Object.create(Object.getPrototypeOf(value), Object.getOwnPropertyDescriptors(value)) : value;
  }
  sandbox.globalThis = sandbox;
  return sandbox;
}

/** The SDK only reads a context's global object this way. */
export function runInContext(code: string, context: Sandbox): unknown {
  if (code === 'globalThis') return context;
  throw unsupported();
}

export class Script {
  #code: string;

  constructor(code: string) {
    this.#code = code;
  }

  runInContext(context: Sandbox): unknown {
    const bundle = bundles.get(this.#code);
    if (bundle !== undefined) {
      warnUnshadowedGlobals(bundle.names);
      bundle.evaluate(context, ...bundle.names.map((name) => context[name]));
      return undefined;
    }
    const lookup = LOOKUP.exec(this.#code);
    if (lookup !== null) return (context.__private_workflows as Map<string, unknown> | undefined)?.get(JSON.parse(lookup[1]));
    throw unsupported();
  }
}

const unsupported = () =>
  new Error('Workers run only the workflow bundle compiled into this deployment by @workflow/world-cloudflare/build. Dynamic workflow code needs WORKFLOW_VM=quickjs.');

let checkedGlobals = false;

/** A host global the factory does not shadow stays visible to workflow code: warn once per isolate. */
function warnUnshadowedGlobals(names: readonly string[]): void {
  if (checkedGlobals) return;
  checkedGlobals = true;
  const shadowed = new Set([...names, 'globalThis', 'eval', 'undefined', 'NaN', 'Infinity']);
  const visible = new Set<string>();
  for (let scope: object | null = globalThis; scope !== null && scope !== Object.prototype; scope = Object.getPrototypeOf(scope)) {
    for (const name of Object.getOwnPropertyNames(scope)) {
      if (!shadowed.has(name) && /^[A-Za-z_$][\w$]*$/.test(name)) visible.add(name);
    }
  }
  if (visible.size > 0) console.warn(`Host globals visible to workflow code: ${[...visible].join(', ')}`);
}
