import { createExecutionContext, env, waitOnExecutionContext } from 'cloudflare:test';
import { expect, it, vi } from 'vitest';
import { createWorld } from 'world-cloudflare';
import { FLOW_DELIVERY_BUDGET_MS } from '../packages/world-cloudflare/src/constants.js';
import { currentRequest, withRequestContext } from '../packages/world-cloudflare/src/lib/request-context.js';

// The lookup `@vercel/functions` `waitUntil` makes, which the SDK uses for background work.
const sdkWaitUntil = (promise: Promise<unknown>): void => {
  // SAFETY: the global is installed by request-context.ts; this mirrors @vercel/functions' read.
  const context = (globalThis as Record<symbol, { get(): { waitUntil?: (p: Promise<unknown>) => void } }>)[
    Symbol.for('@vercel/request-context')
  ];
  context.get().waitUntil?.(promise);
};
const ok = () => new Response('ok');
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

it('answers only once the background work the request handed over settles', async () => {
  let saved = false;
  let late = false;
  await withRequestContext(createExecutionContext(), {}, async () => {
    await Promise.resolve();
    sdkWaitUntil(sleep(20).then(() => { saved = true; }));
    // The SDK hands work over after a dynamic import, a few microtasks after the flow returns.
    void Promise.resolve().then(() => Promise.resolve()).then(() => sdkWaitUntil(sleep(40).then(() => { late = true; })));
    return ok();
  });
  expect([saved, late]).toEqual([true, true]);
});

it('stops waiting at the budget and tells the request it was cut off', async () => {
  const ends: boolean[] = [];
  let signal: AbortSignal | undefined;
  const startedAt = Date.now();
  await withRequestContext(createExecutionContext(), { FLOW_BACKGROUND_BUDGET_MS: '50' }, async () => {
    sdkWaitUntil(new Promise(() => {}));
    const request = currentRequest();
    signal = request?.signal;
    request?.onEnd.push(async (cut) => { ends.push(cut); });
    return ok();
  });
  expect(Date.now() - startedAt).toBeLessThan(5_000);
  expect(ends).toEqual([true]);
  expect(signal?.aborted).toBe(true);
});

it('ends a request whose background work finished without a cut', async () => {
  const ends: boolean[] = [];
  await withRequestContext(createExecutionContext(), {}, async () => {
    currentRequest()?.onEnd.push(async (cut) => { ends.push(cut); });
    return ok();
  });
  expect(ends).toEqual([false]);
});

it.each([false, true])('bounds gated cleanup by the request budget, cut=%s', async (cut) => {
  const ctx = createExecutionContext();
  using handoff = vi.spyOn(ctx, 'waitUntil');
  let release = () => {};
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let cleaned = false;
  const cleanup = gate.then(() => { cleaned = true; });
  const ends: boolean[] = [];
  let aborted = false;
  const invocation = withRequestContext(ctx, { FLOW_BACKGROUND_BUDGET_MS: '50' }, async () => {
    if (cut) sdkWaitUntil(gate);
    const request = currentRequest();
    if (request === undefined) throw new Error('Expected a flow request');
    request.onEnd.push((wasCut) => {
      ends.push(wasCut);
      aborted = request.signal.aborted;
      return cleanup;
    });
    return ok();
  });
  try {
    expect(await Promise.race([invocation.then(() => 'answered'), sleep(500).then(() => 'blocked')])).toBe('answered');
    expect(ends).toEqual([cut]);
    expect(aborted).toBe(true);
    expect(cleaned).toBe(false);
    expect(handoff).toHaveBeenCalledWith(cleanup);
    release();
    await waitOnExecutionContext(ctx);
    expect(cleaned).toBe(true);
  } finally {
    release();
    await invocation;
    await waitOnExecutionContext(ctx);
  }
});

it('hands late cleanup rejections to the platform without replacing the response', async () => {
  const ctx = createExecutionContext();
  // Observe the platform handoff here instead of making the test worker report an expected failure.
  using handoff = vi.spyOn(ctx, 'waitUntil').mockImplementation(() => {});
  let fail = (_reason: Error) => {};
  const cleanup = new Promise<void>((_resolve, reject) => { fail = reject; });
  const failure = new Error('cleanup failed');
  const rejected = expect(cleanup).rejects.toBe(failure);
  const invocation = withRequestContext(ctx, { FLOW_BACKGROUND_BUDGET_MS: '50' }, async () => {
    currentRequest()?.onEnd.push(() => cleanup);
    return ok();
  });
  try {
    expect(await Promise.race([invocation.then(() => 'answered'), sleep(500).then(() => 'blocked')])).toBe('answered');
    expect(handoff).toHaveBeenCalledWith(cleanup);
    fail(failure);
    await expect(handoff.mock.calls[0][0]).rejects.toBe(failure);
    expect((await invocation).status).toBe(200);
  } finally {
    fail(failure);
    await rejected;
    await invocation;
  }
});

it('has no request outside a flow request', () => {
  expect(currentRequest()).toBeUndefined();
  expect(() => sdkWaitUntil(Promise.resolve())).not.toThrow();
});

it('reports when the current delivery should hand off, and no deadline outside one', async () => {
  const world = createWorld(env);
  expect(await world.getRuntimeDeadline!()).toBeUndefined();
  const request = new Request('https://internal/flow', { headers: { 'x-workflow-invocation-started-at': '1000' } });
  const deadline = await withRequestContext(createExecutionContext(), {}, () => world.getRuntimeDeadline!(), request);
  expect(deadline).toEqual(new Date(1000 + FLOW_DELIVERY_BUDGET_MS));
});

it("resolves the latest deployment from the main Worker's consumer", async () => {
  expect(await createWorld(env).resolveLatestDeploymentId!()).toBe('test-v1');
});
