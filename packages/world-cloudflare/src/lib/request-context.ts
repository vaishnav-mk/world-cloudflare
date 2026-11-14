import { AsyncLocalStorage } from 'node:async_hooks';
import { FLOW_DELIVERY_BUDGET_MS } from '../constants.js';

type RequestContext = { waitUntil(promise: Promise<unknown>): void; startedAt?: number };

const requests = new AsyncLocalStorage<RequestContext>();

// The SDK hands background work, such as VM snapshot saves, to `@vercel/functions` `waitUntil`,
// which reads the current request from this global. Without it the work is dropped when the
// response ends.
// SAFETY: the symbol is the lookup key `@vercel/functions` uses; globalThis does not declare it.
(globalThis as Record<symbol, unknown>)[Symbol.for('@vercel/request-context')] ??= {
  get: () => requests.getStore() ?? {},
};

/**
 * Runs a flow request so the SDK's background work extends `ctx` instead of being dropped, and so
 * `getRuntimeDeadline` knows when to hand off: `FLOW_DELIVERY_BUDGET_MS` after the consumer took
 * the delivery, as its `x-workflow-invocation-started-at` header says.
 */
export function withRequestContext<T>(ctx: ExecutionContext, run: () => T, request?: Request): T {
  const header = Number(request?.headers.get('x-workflow-invocation-started-at') ?? Number.NaN);
  const startedAt = Number.isSafeInteger(header) ? header : undefined;
  return requests.run({ waitUntil: (promise) => ctx.waitUntil(promise), startedAt }, run);
}

/** When the consumer took the current flow delivery, if this is one. */
export function currentInvocationStart(): number | undefined {
  return requests.getStore()?.startedAt;
}

/** When the current flow delivery should hand off, if this is one. */
export function currentDeadline(): Date | undefined {
  const startedAt = currentInvocationStart();
  return startedAt === undefined ? undefined : new Date(startedAt + FLOW_DELIVERY_BUDGET_MS);
}
