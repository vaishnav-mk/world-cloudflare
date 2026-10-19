import { createExecutionContext, waitOnExecutionContext } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { withRequestContext } from '../packages/world-cloudflare/src/lib/request-context.js';

// The lookup `@vercel/functions` `waitUntil` makes, which the SDK uses for background work.
const sdkWaitUntil = (promise: Promise<unknown>): void => {
  // SAFETY: the global is installed by request-context.ts; this mirrors @vercel/functions' read.
  const context = (globalThis as Record<symbol, { get(): { waitUntil?: (p: Promise<unknown>) => void } }>)[
    Symbol.for('@vercel/request-context')
  ];
  context.get().waitUntil?.(promise);
};

it('ties the SDK background work to the request that started it', async () => {
  const ctx = createExecutionContext();
  let saved = false;
  await withRequestContext(ctx, async () => {
    await Promise.resolve();
    sdkWaitUntil(new Promise((resolve) => setTimeout(resolve, 20)).then(() => { saved = true; }));
  });
  expect(saved).toBe(false);
  await waitOnExecutionContext(ctx);
  expect(saved).toBe(true);
});

it('has no request outside a flow request', () => {
  expect(() => sdkWaitUntil(Promise.resolve())).not.toThrow();
});
