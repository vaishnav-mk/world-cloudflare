import { AsyncLocalStorage } from 'node:async_hooks';

type RequestContext = { waitUntil(promise: Promise<unknown>): void };

const requests = new AsyncLocalStorage<RequestContext>();

// The SDK hands background work, such as VM snapshot saves, to `@vercel/functions` `waitUntil`,
// which reads the current request from this global. Without it the work is dropped when the
// response ends.
// SAFETY: the symbol is the lookup key `@vercel/functions` uses; globalThis does not declare it.
(globalThis as Record<symbol, unknown>)[Symbol.for('@vercel/request-context')] ??= {
  get: () => requests.getStore() ?? {},
};

/** Runs a flow request so the SDK's background work extends `ctx` instead of being dropped. */
export function withRequestContext<T>(ctx: ExecutionContext, run: () => T): T {
  return requests.run({ waitUntil: (promise) => ctx.waitUntil(promise) }, run);
}
