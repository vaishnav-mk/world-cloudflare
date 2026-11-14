import { AsyncLocalStorage } from 'node:async_hooks';
import { FLOW_BACKGROUND_BUDGET_MS, FLOW_DELIVERY_BUDGET_MS } from '../constants.js';
import type { WorldEnv } from '../types.js';

/** One flow request, as the SDK's background work and the World's stream writers see it. */
export type FlowRequest = {
  /** Hands over background work. The request answers once it settles or the budget runs out. */
  waitUntil(promise: Promise<unknown>): void;
  /** When the consumer took the delivery, from its `x-workflow-invocation-started-at` header. */
  readonly startedAt?: number;
  /** Aborted when the request ends. Its stream writers must stop appending chunks. */
  readonly signal: AbortSignal;
  /**
   * Run as the request ends, awaited only until its background deadline; unfinished cleanup goes
   * to the platform's `waitUntil`. `cut` means background work was still running.
   */
  readonly onEnd: ((cut: boolean) => Promise<void>)[];
};

const requests = new AsyncLocalStorage<FlowRequest>();

// The SDK hands background work, such as VM snapshot saves and writing the streams its steps
// return, to `@vercel/functions` `waitUntil`, which reads the current request from this global.
// SAFETY: the symbol is the lookup key `@vercel/functions` uses; globalThis does not declare it.
(globalThis as Record<symbol, unknown>)[Symbol.for('@vercel/request-context')] ??= {
  get: () => requests.getStore() ?? {},
};

/** The flow request the caller runs in, if any. */
export const currentRequest = (): FlowRequest | undefined => requests.getStore();

/**
 * Runs a flow request, then waits for the background work it handed to `waitUntil` before
 * answering, so none of it runs past the invocation: Workers cancels `waitUntil` work 30 s after
 * an invocation ends, which cut off long stream writes. The wait stops at the background budget,
 * counted from when the consumer took the delivery, so the request answers before the delivery
 * times out. `getRuntimeDeadline` hands off `FLOW_DELIVERY_BUDGET_MS` after that same start.
 */
export async function withRequestContext<T>(
  ctx: ExecutionContext, env: Pick<WorldEnv, 'FLOW_BACKGROUND_BUDGET_MS'>, run: () => Promise<T>, request?: Request,
): Promise<T> {
  const header = Number(request?.headers.get('x-workflow-invocation-started-at') ?? Number.NaN);
  const startedAt = Number.isSafeInteger(header) ? header : undefined;
  const deadline = (startedAt ?? Date.now()) + Number(env.FLOW_BACKGROUND_BUDGET_MS ?? FLOW_BACKGROUND_BUDGET_MS);
  const pending = new Set<Promise<unknown>>();
  const ended = new AbortController();
  const flowRequest: FlowRequest = {
    waitUntil(promise) {
      const settled: Promise<unknown> = promise.then(() => pending.delete(settled), () => pending.delete(settled));
      pending.add(settled);
      // Work the budget cuts off still gets the platform's time after the response.
      ctx.waitUntil(promise);
    },
    startedAt,
    signal: ended.signal,
    onEnd: [],
  };
  try {
    return await requests.run(flowRequest, run);
  } finally {
    const cut = !(await settle(pending, deadline));
    ended.abort();
    for (const end of flowRequest.onEnd) flowRequest.waitUntil(end(cut));
    await settle(pending, deadline);
  }
}

/** Waits for `pending`, and work added to it meanwhile, until `deadline`. Whether all of it settled. */
async function settle(pending: Set<Promise<unknown>>, deadline: number): Promise<boolean> {
  for (;;) {
    // The SDK hands work over a few microtasks late, after a dynamic import.
    await scheduler.wait(0);
    const left = deadline - Date.now();
    if (pending.size === 0) return true;
    if (!(left > 0)) return false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([Promise.allSettled(pending), new Promise((resolve) => { timer = setTimeout(resolve, left); })]);
    clearTimeout(timer);
  }
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
