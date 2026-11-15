/**
 * `node:vm` for the Workflow SDK's node engine on Workers, which have neither `node:vm` nor `eval`.
 *
 * The build (`build.ts`) aliases `node:vm` to this module and compiles the workflow bundle a second
 * time, into a function whose parameters shadow every global, registered with `registerWorkflowBundle`.
 * The SDK's `createContext()` gets a fresh sandbox object, and running the bundle's `Script` against
 * it calls that function with the sandbox, so each replay starts with clean module state and sees
 * only the deterministic globals the SDK installs on the sandbox.
 *
 * The sandbox shares the host realm, so it cannot have its own `Array.prototype`: `[]` always makes
 * the realm's arrays. Most writes stay in the run, also while other runs replay concurrently in
 * the isolate and between their awaits. Error's stack settings remain shared (see freezeExcept). So:
 * - namespaces (`Math`, `JSON`, `Reflect`, `Intl`, `console`, `crypto`...) and `Date` are copies per
 *   sandbox, which workflow code may change;
 * - the shared built-ins it can reach (constructors, prototypes, host classes) are frozen for the
 *   whole isolate before the first replay, except Error's stack settings. Reverting writes after a replay
 *   would not do: concurrent replays and host code would see them meanwhile, and a suspended replay
 *   never finishes. Frozen prototypes keep the usual overrides working (`this.name = ...` on an error).
 * Like node:vm, this is not a security boundary.
 */

type Sandbox = Record<PropertyKey, unknown>;
type Import = (specifier: unknown) => never;
type Bundle = { names: readonly string[]; evaluate: (globalThis: Sandbox, importModule: Import, ...globals: unknown[]) => void };

const host = globalThis as unknown as Sandbox;
const HostDate = Date;
const HostDateTimeFormat = Intl.DateTimeFormat;
const hostConsole = console as unknown as Record<string, (...args: unknown[]) => unknown>;
const hostCrypto = crypto;

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
/** Namespaces each sandbox gets its own copy of. The SDK itself sets `Math.random` and deletes `WebAssembly.compile`. */
const NAMESPACES = ['Math', 'Atomics', 'WebAssembly', 'JSON', 'Reflect', 'Intl'];

// Taken before hardenShared() freezes the originals, so copies keep their native attributes.
const pristine = new Map<object, PropertyDescriptorMap>(
  [...NAMESPACES.map((name) => host[name] as object), HostDate, HostDate.prototype, HostDateTimeFormat, HostDateTimeFormat.prototype]
    .map((object) => [object, Object.getOwnPropertyDescriptors(object)]),
);
const descriptors = (object: object) => pristine.get(object) ?? {};
const copy = (object: object): Sandbox => Object.create(Object.getPrototypeOf(object), descriptors(object));
const own = (value: unknown): PropertyDescriptor => ({ value, writable: true, configurable: true });

const CONSOLE_METHODS = Object.getOwnPropertyNames(hostConsole).filter((name) => typeof hostConsole[name] === 'function');
const SUBTLE_METHODS = Object.entries(Object.getOwnPropertyDescriptors(Object.getPrototypeOf(hostCrypto.subtle)))
  .flatMap(([name, { value }]) => (typeof value === 'function' && name !== 'constructor' ? [name] : []));

/** Matches the SDK's `globalThis.__private_workflows?.get("<name>")` lookup script. */
const LOOKUP = /^globalThis\.__private_workflows\?\.get\(("(?:[^"\\]|\\.)*")\)$/;

// On globalThis, so every bundled copy of this module, and the tests, see the registered bundles.
// SAFETY: only this module writes the key, and always a Map of bundles.
const bundles = ((globalThis as unknown as Record<symbol, Map<string, Bundle>>)[
  Symbol.for('world-cloudflare//workflowBundles')
] ??= new Map());

/** Registers the factory the build compiled `code` into. `names` are its parameters after `globalThis` and `importModule`. */
export function registerWorkflowBundle(code: string, names: readonly string[], evaluate: Bundle['evaluate']): void {
  bundles.set(code, { names, evaluate });
}

export function createContext(): Sandbox {
  const sandbox: Sandbox = {};
  for (const name of INTRINSICS) {
    if (name in host) sandbox[name] = NAMESPACES.includes(name) ? copy(host[name] as object) : host[name];
  }
  sandbox.Date = sandboxDate(sandbox);
  (sandbox.Intl as Sandbox).DateTimeFormat = sandboxDateTimeFormat(sandbox);
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
      seal(context);
      hardenShared(context);
      warnUnshadowedGlobals(bundle.names);
      bundle.evaluate(context, importModule, ...bundle.names.map((name) => context[name]));
      return undefined;
    }
    const lookup = LOOKUP.exec(this.#code);
    if (lookup !== null) return (context.__private_workflows as Map<string, unknown> | undefined)?.get(JSON.parse(lookup[1]));
    throw unsupported();
  }
}

const unsupported = () =>
  new Error('Workers run only the workflow bundle compiled into this deployment by world-cloudflare/build. Dynamic workflow code needs WORKFLOW_VM=quickjs.');

/** What the build turns the workflow bundle's `import()` into. QuickJS rejects with a ReferenceError too. */
const importModule: Import = (specifier) => {
  throw new ReferenceError(`import(${JSON.stringify(String(specifier))}) is not available in workflow code; import it in a step`);
};

/**
 * The run's Date before the SDK wraps it in its deterministic one, which reuses this prototype: a copy
 * of the host's, so `date.constructor` is the run's Date (see `seal`) and the host's real Date, with
 * its wall clock, is not reachable from workflow code.
 */
function sandboxDate(sandbox: Sandbox): DateConstructor {
  // Also reachable as Object.getPrototypeOf(Date), so it reads the run's clock too.
  const now = () => (sandbox.Date as DateConstructor).now();
  function SandboxDate(...args: unknown[]) {
    if (new.target === undefined) return new HostDate(now()).toString();
    return Reflect.construct(HostDate, args.length === 0 ? [now()] : args, new.target);
  }
  const { parse, UTC } = descriptors(HostDate);
  Object.defineProperties(SandboxDate, { parse, UTC, prototype: { value: Object.create(Object.prototype, descriptors(HostDate.prototype)) } });
  // SAFETY: constructs host Dates with the run's prototype, as `Date` does.
  return SandboxDate as unknown as DateConstructor;
}

/** `Intl.DateTimeFormat`, whose `format()` and `formatToParts()` without a date read the run's clock, not the host's. */
function sandboxDateTimeFormat(sandbox: Sandbox): typeof Intl.DateTimeFormat {
  const now = () => (sandbox.Date as DateConstructor).now();
  const { format, formatToParts } = descriptors(HostDateTimeFormat.prototype);
  const hostFormat = format.get as (this: Intl.DateTimeFormat) => (date?: Date | number) => string;
  const hostFormatToParts = formatToParts.value as (this: Intl.DateTimeFormat, date?: Date | number) => Intl.DateTimeFormatPart[];
  function DateTimeFormat(locales?: unknown, options?: unknown) {
    return Reflect.construct(HostDateTimeFormat, [locales, options], new.target ?? DateTimeFormat);
  }
  const prototype = Object.create(Object.prototype, {
    ...descriptors(HostDateTimeFormat.prototype),
    constructor: own(DateTimeFormat),
    format: { get(this: Intl.DateTimeFormat) { const bound = hostFormat.call(this); return (date?: Date | number) => bound(date === undefined ? now() : date); }, configurable: true },
    formatToParts: own(function (this: Intl.DateTimeFormat, date?: Date | number) { return hostFormatToParts.call(this, date === undefined ? now() : date); }),
  });
  const { supportedLocalesOf } = descriptors(HostDateTimeFormat);
  Object.defineProperties(DateTimeFormat, { supportedLocalesOf, prototype: { value: prototype } });
  // SAFETY: constructs host formatters with the run's prototype, as `Intl.DateTimeFormat` does.
  return DateTimeFormat as unknown as typeof Intl.DateTimeFormat;
}

/**
 * Gives the run its own copies of what the SDK hands it from the host: `crypto` (the SDK's
 * deterministic Proxy over the host's, which forwards writes to it) and `console`.
 */
function seal(context: Sandbox): void {
  const proxy = context.crypto as Crypto;
  const subtle = proxy.subtle as unknown as Sandbox;
  context.crypto = Object.create(Object.getPrototypeOf(hostCrypto), {
    getRandomValues: own(proxy.getRandomValues),
    randomUUID: own(proxy.randomUUID),
    subtle: own(Object.create(Object.getPrototypeOf(hostCrypto.subtle), Object.fromEntries(SUBTLE_METHODS.map((name) => [name, own(subtle[name])])))),
  });
  context.console = Object.fromEntries(CONSOLE_METHODS.map((name) => [name, (...args: unknown[]) => hostConsole[name](...args)]));
  Object.defineProperty((context.Date as DateConstructor).prototype, 'constructor', own(context.Date));
}

/** Inherited properties code commonly assigns on its own objects, which a frozen prototype would make read-only. */
const ERRORS = [Error, EvalError, RangeError, ReferenceError, SyntaxError, TypeError, URIError, AggregateError];
const OVERRIDABLE = new Map<object, string[]>([
  [Object.prototype, ['constructor', 'toString', 'toLocaleString', 'valueOf', 'hasOwnProperty', 'isPrototypeOf', 'propertyIsEnumerable']],
  [Function.prototype, ['toString']],
  ...ERRORS.map((E): [object, string[]] => [E.prototype, ['constructor', 'name', 'message']]),
]);

/** Built-ins reachable only through values of their kind, such as %GeneratorFunction%. */
const hiddenIntrinsics = (): unknown[] => {
  const call = (object: unknown, method: string, ...args: unknown[]) => (object as Record<string, (...rest: unknown[]) => unknown>)[method](...args);
  const segments = new Intl.Segmenter().segment('');
  return [
    function* () {}, async function () {}, async function* () {}, (function* () {})(), (async function* () {})(),
    [][Symbol.iterator](), new Map().entries(), new Set().values(), ''[Symbol.iterator](), /./[Symbol.matchAll](''),
    call([].values(), 'map', (value: unknown) => value), call(host.Iterator, 'from', { next: () => ({ done: true }) }),
    segments, segments[Symbol.iterator](), new Headers().entries(), new URLSearchParams().entries(),
  ];
};

let hardened = false;

/** Freezes, once per isolate, every shared object workflow code can reach; see the module comment. */
function hardenShared(context: Sandbox): void {
  if (hardened) return;
  hardened = true;
  for (const [object, keys] of OVERRIDABLE) for (const key of keys) makeOverridable(object, key);
  const roots: unknown[] = [
    // The host originals of the sandbox's copies stay writable for the host; what the copies share with them does not.
    ...[...pristine.values()].flatMap((properties) => Object.values(properties).flatMap(({ value, get, set }) => [value, get, set])),
    ...hiddenIntrinsics(), Object.getPrototypeOf(context.crypto), Object.getPrototypeOf((context.crypto as Crypto).subtle),
  ];
  // What the SDK hands the sandbox from the host as is: constructors, Headers, URL, structuredClone...
  for (const key of Reflect.ownKeys(context)) if (context[key] === host[key]) roots.push(context[key]);
  const never = new Set<unknown>([host, hostConsole, hostCrypto, ...pristine.keys()]);
  const shared = new Set<object>();
  for (const value of roots) {
    if ((typeof value !== 'object' && typeof value !== 'function') || value === null || never.has(value) || shared.has(value)) continue;
    shared.add(value);
    roots.push(Object.getPrototypeOf(value));
    for (const key of Reflect.ownKeys(value)) {
      const descriptor = Reflect.getOwnPropertyDescriptor(value, key);
      if (descriptor !== undefined) roots.push(descriptor.value, descriptor.get, descriptor.set);
    }
  }
  for (const object of shared) {
    if (object !== Error) Object.freeze(object);
    else freezeExcept(Error, ['stackTraceLimit', 'prepareStackTrace']);
  }
}

/**
 * Like Object.freeze, but leaves `keys` writable: libraries and test runners set `Error.stackTraceLimit`
 * and `Error.prepareStackTrace` around a stack capture. Workflow code can set them too.
 */
function freezeExcept(object: object, keys: string[]): void {
  for (const name of Reflect.ownKeys(object)) {
    const descriptor = Reflect.getOwnPropertyDescriptor(object, name);
    if (descriptor === undefined) continue;
    Object.defineProperty(object, name, 'value' in descriptor && !keys.includes(String(name)) ? { configurable: false, writable: false } : { configurable: false });
  }
  Object.preventExtensions(object);
}

/**
 * Turns `key` on a soon-frozen prototype into an accessor, so an object inheriting it can still
 * assign its own value (`this.name = ...`), while a write to the prototype itself throws.
 */
function makeOverridable(prototype: object, key: string): void {
  const descriptor = Reflect.getOwnPropertyDescriptor(prototype, key);
  if (descriptor === undefined || !('value' in descriptor) || descriptor.writable !== true) return;
  const { value } = descriptor;
  Object.defineProperty(prototype, key, {
    get: () => value,
    set(this: unknown, next: unknown) {
      if (this === prototype) throw new TypeError(`Cannot assign to read only property '${key}' of a shared built-in`);
      if (Object(this) === this) Object.defineProperty(this, key, { value: next, writable: true, enumerable: true, configurable: true });
    },
    enumerable: descriptor.enumerable,
    configurable: false,
  });
}

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
