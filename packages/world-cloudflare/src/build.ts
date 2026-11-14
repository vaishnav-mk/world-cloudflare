/**
 * `@workflow/world-cloudflare/build`: compiles a project's workflows for a Worker with the published
 * `@workflow/builders`. Runs in Node before `wrangler dev` or `wrangler deploy`:
 *
 *   node node_modules/@workflow/world-cloudflare/src/build.ts [dir...]
 *
 * Writes to `.well-known/workflow/v1/`: `flow.mjs`, whose `POST` is the flow handler;
 * `__step_registrations.mjs`, the steps, which `flow.mjs` imports (two modules keep each under the
 * size the Workers vitest pool loads); the QuickJS WebAssembly modules; and `manifest.json`.
 * Wrangler bundles them into the Worker. Both engines work: `WORKFLOW_VM=quickjs`, and the node
 * engine (`WORKFLOW_VM` unset or `node`), which runs on the `node:vm` replacement in `vm.ts`.
 */
import { rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { BaseBuilder, createBaseBuilderConfig, createWorkflowEntrypointOptionsCode, type WorkflowManifest } from '@workflow/builders';
import * as esbuild from 'esbuild';

export interface BuildOptions {
  /** Project directory. Defaults to the working directory. */
  cwd?: string;
  /** Directories under `cwd` holding workflows and steps. */
  dirs?: string[];
}

/**
 * Globals the node engine's workflow factory takes as parameters, so a replay reads them from its
 * sandbox (or gets `undefined`) instead of the host: ECMAScript intrinsics, every global workerd
 * exposes with `nodejs_compat`, and Node's module globals. `eval` cannot be a strict-mode parameter;
 * workerd blocks it anyway. At run time `vm.ts` warns about any host global missing here.
 */
const SHADOWED_GLOBALS = [
  // ECMAScript
  'Object', 'Function', 'Array', 'Number', 'parseFloat', 'parseInt', 'Boolean', 'String', 'Symbol', 'Date',
  'Promise', 'RegExp', 'Error', 'AggregateError', 'EvalError', 'RangeError', 'ReferenceError', 'SyntaxError',
  'TypeError', 'URIError', 'JSON', 'Math', 'Intl', 'ArrayBuffer', 'Atomics', 'Uint8Array', 'Int8Array',
  'Uint16Array', 'Int16Array', 'Uint32Array', 'Int32Array', 'BigUint64Array', 'BigInt64Array', 'Uint8ClampedArray',
  'Float32Array', 'Float64Array', 'Float16Array', 'DataView', 'Map', 'BigInt', 'Set', 'Iterator', 'WeakMap',
  'WeakSet', 'Proxy', 'Reflect', 'FinalizationRegistry', 'WeakRef', 'decodeURI', 'decodeURIComponent', 'encodeURI',
  'encodeURIComponent', 'escape', 'unescape', 'isFinite', 'isNaN', 'console', 'SuppressedError', 'DisposableStack',
  'AsyncDisposableStack', 'SharedArrayBuffer', 'WebAssembly',
  // workerd
  'AbortController', 'AbortSignal', 'Blob', 'Body', 'Buffer', 'ByteLengthQueuingStrategy', 'Cache', 'CacheStorage',
  'CloseEvent', 'Cloudflare', 'CompressionStream', 'CountQueuingStrategy', 'Crypto', 'CryptoKey', 'CustomEvent',
  'DOMException', 'DecompressionStream', 'ErrorEvent', 'Event', 'EventSource', 'EventTarget', 'ExtendableEvent',
  'FetchEvent', 'File', 'FixedLengthStream', 'FormData', 'HTMLRewriter', 'Headers', 'IdentityTransformStream',
  'MessageChannel', 'MessageEvent', 'MessagePort', 'Navigator', 'Performance', 'PerformanceEntry', 'PerformanceMark',
  'PerformanceMeasure', 'PerformanceObserver', 'PerformanceObserverEntryList', 'PerformanceResourceTiming',
  'PromiseRejectionEvent', 'ReadableByteStreamController', 'ReadableStream', 'ReadableStreamBYOBReader',
  'ReadableStreamBYOBRequest', 'ReadableStreamDefaultController', 'ReadableStreamDefaultReader', 'Request', 'Response',
  'ScheduledEvent', 'ServiceWorkerGlobalScope', 'SubtleCrypto', 'TailEvent', 'TextDecoder', 'TextDecoderStream',
  'TextEncoder', 'TextEncoderStream', 'TraceEvent', 'TransformStream', 'TransformStreamDefaultController', 'URL',
  'URLPattern', 'URLSearchParams', 'WebSocket', 'WebSocketPair', 'WebSocketRequestResponsePair', 'WorkerGlobalScope',
  'WritableStream', 'WritableStreamDefaultController', 'WritableStreamDefaultWriter', 'addEventListener', 'atob', 'btoa',
  'caches', 'clearImmediate', 'clearInterval', 'clearTimeout', 'constructor', 'crypto', 'dispatchEvent', 'fetch',
  'global', 'navigator', 'origin', 'performance', 'process', 'queueMicrotask', 'removeEventListener', 'reportError',
  'scheduler', 'self', 'setImmediate', 'setInterval', 'setTimeout', 'structuredClone',
  // Node
  'BroadcastChannel', 'require', 'module', 'exports', '__filename', '__dirname',
];

const here = (path: string) => fileURLToPath(new URL(path, import.meta.url));
const VM_SHIM = here('./vm.ts');
const OUT_DIR = '.well-known/workflow/v1';
const STEPS = '__step_registrations';

/**
 * Packages the SDK imports that do not load on Workers, and their replacements. The build applies
 * them to the flow and steps modules; the Worker needs them too, for its own imports of the SDK
 * (`workflow/api`): as `alias` in the Wrangler config, and as Vite `resolve.alias` in the vitest config.
 */
export const workerAliases = [
  { find: /^@workflow\/world-(local|vercel)$/, replacement: here('./stubs/world.ts') },
  { find: /^@vercel\/functions$/, replacement: here('./stubs/vercel-functions.ts') },
];

class WorkersBuilder extends BaseBuilder {
  async build(): Promise<void> {
    const outDir = this.resolvePath(OUT_DIR);
    const stepsSource = join(outDir, `${STEPS}.src.mjs`);
    const flowSource = join(outDir, 'flow.src.mjs');
    const routeSource = join(outDir, 'route.src.mjs');
    const inputFiles = await this.getInputFiles();
    const tsconfigPath = await this.findTsConfigPath();
    await this.ensureDirectory(flowSource);
    const steps = await this.createStepsBundle({ inputFiles, outfile: stepsSource, tsconfigPath, skipEsmRequireBanner: true });
    const workflows = await this.createWorkflowsBundle({ inputFiles, outfile: `${flowSource}.tmp`, bundleFinalOutput: false, tsconfigPath });
    await rm(`${flowSource}.tmp`, { force: true });
    const code = workflows.interimBundleText;
    if (code === undefined) throw new Error('@workflow/builders returned no workflow bundle');

    // vm.ts runs the same bundle as a function. Its import() calls must not load the Worker's modules
    // (cloudflare:workers, node:timers): esbuild lowers each to `__toESM(require(...))`, without
    // comments in between, and that require goes to vm.ts instead.
    const lowered = await esbuild.transform(code, { format: 'cjs', supported: { 'dynamic-import': false }, minifyWhitespace: true, logLevel: 'warning' });
    const factory = lowered.code.replace(/(__toESM\d*\()require\(/g, '$1__workflowImport(');
    // The route `createCombinedBundle` writes, plus that function, registered with vm.ts.
    await writeFile(routeSource, `import { workflowEntrypoint } from 'workflow/runtime';
import { registerWorkflowBundle } from ${JSON.stringify(VM_SHIM)};

const workflowRouteModuleBodyStartedAt = Date.now();
const workflowCode = ${JSON.stringify(code)};
registerWorkflowBundle(workflowCode, ${JSON.stringify(SHADOWED_GLOBALS)}, function (globalThis, __workflowImport, ${SHADOWED_GLOBALS.join(', ')}) {
${factory}
});
export const POST = workflowEntrypoint(workflowCode${createWorkflowEntrypointOptionsCode({ routeModuleBodyStartedAt: 'workflowRouteModuleBodyStartedAt' })});
`);
    // The SDK loads on the first delivery rather than at startup: it keeps startup short, and
    // \`workflow/runtime\` 5.5.0 draws a random ID as it loads, which Workers forbid at startup.
    await writeFile(flowSource, `let route;
export const POST = async (request) => {
  route ??= Promise.all([import('./${STEPS}.mjs'), import(${JSON.stringify(routeSource)})]).then(([, route]) => route);
  return (await route).POST(request);
};
`);
    const result = await esbuild.build({
      entryPoints: { flow: flowSource, [STEPS]: stepsSource },
      outdir: outDir,
      outExtension: { '.js': '.mjs' },
      absWorkingDir: this.config.workingDir,
      bundle: true,
      format: 'esm',
      platform: 'node',
      target: 'es2022',
      keepNames: true,
      logLevel: 'warning',
      metafile: true,
      plugins: [workersPlugin(outDir)],
    });
    if (!Object.keys(result.metafile.inputs).some((path) => QUICKJS_ASSETS.test(path))) {
      throw new Error('The flow bundle has no @workflow/core QuickJS assets module to precompile');
    }
    const manifest: WorkflowManifest = {
      ...steps.manifest,
      workflows: { ...steps.manifest.workflows, ...workflows.manifest.workflows },
      classes: { ...steps.manifest.classes, ...workflows.manifest.classes },
    };
    await this.createManifest({ workflowBundlePath: routeSource, manifestDir: outDir, manifest });
    await Promise.all([flowSource, routeSource, stepsSource].map((path) => rm(path)));
  }
}

const QUICKJS_ASSETS = /[\\/]@workflow[\\/]core[\\/]dist[\\/]runtime[\\/]quickjs-assets\.generated\.js$/;

/** What a Worker needs in place of Node-only parts of the SDK. */
function workersPlugin(outDir: string): esbuild.Plugin {
  return {
    name: 'world-cloudflare-workers',
    setup(build) {
      build.onResolve({ filter: /^(node:)?vm$/ }, () => ({ path: VM_SHIM }));
      for (const { find, replacement } of workerAliases) build.onResolve({ filter: find }, () => ({ path: replacement }));
      // flow.mjs loads the steps module and the WebAssembly modules; Wrangler bundles them.
      build.onResolve({ filter: new RegExp(`^\\./(${STEPS}\\.mjs|__quickjs_[\\w-]+\\.wasm)$`) }, ({ path }) => ({ path, external: true }));
      // workerd forbids WebAssembly.compile(), so core's embedded QuickJS binaries become \`.wasm\`
      // imports, which Wrangler compiles at deploy time. Core caches its compiled modules in a
      // global singleton; filling it skips the compile.
      build.onLoad({ filter: QUICKJS_ASSETS }, async ({ path }) => {
        const assets = await import(pathToFileURL(path).href) as {
          quickjsWasm: Uint8Array; quickjsWasiVersion: string; quickjsExtensions: { name: string; wasm: Uint8Array; initFn?: string }[];
        };
        const modules = [{ name: 'runtime', wasm: assets.quickjsWasm }, ...assets.quickjsExtensions];
        await Promise.all(modules.map(({ name, wasm }) => writeFile(join(outDir, `__quickjs_${name}.wasm`), wasm)));
        const extensions = assets.quickjsExtensions.map(({ wasm: _, ...ext }, index) => `{ ...${JSON.stringify(ext)}, wasm: m${index + 1} }`);
        return {
          contents: `${modules.map(({ name }, index) => `import m${index} from './__quickjs_${name}.wasm';`).join('\n')}
export const quickjsWasiVersion = ${JSON.stringify(assets.quickjsWasiVersion)};
export const quickjsWasm = m0;
export const quickjsExtensions = [${extensions.join(', ')}];
(globalThis[Symbol.for('@workflow/core//quickjsCompiledAssets/v1')] ??= {}).promise = Promise.resolve({ wasm: quickjsWasm, extensions: quickjsExtensions });
`,
        };
      });
    },
  };
}

export async function buildWorkflows({ cwd = process.cwd(), dirs = ['.'] }: BuildOptions = {}): Promise<void> {
  await new WorkersBuilder({
    // Left as imports, so the final pass replaces them (workerAliases).
    ...createBaseBuilderConfig({ workingDir: cwd, dirs, externalPackages: ['@workflow/world-local', '@workflow/world-vercel', '@vercel/functions'] }),
    buildTarget: 'standalone',
    stepsBundlePath: join(OUT_DIR, `${STEPS}.mjs`),
    workflowsBundlePath: join(OUT_DIR, 'flow.mjs'),
    webhookBundlePath: join(OUT_DIR, 'webhook.mjs'),
  }).build();
}

if (import.meta.main) {
  const dirs = process.argv.slice(2);
  await buildWorkflows(dirs.length > 0 ? { dirs } : {});
}
