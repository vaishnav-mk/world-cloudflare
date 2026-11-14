/**
 * Replaces `@vercel/functions` in a Worker, where it does not load. The SDK uses only `waitUntil`,
 * which hands background work to the request context the World installs (`lib/request-context.ts`),
 * as `@vercel/functions` does.
 */
export function waitUntil(promise: Promise<unknown>): void {
  // SAFETY: `@vercel/request-context` is the lookup key `@vercel/functions` reads; lib/request-context.ts sets it.
  const context = (globalThis as Record<symbol, { get?(): { waitUntil?(promise: Promise<unknown>): void } } | undefined>)[
    Symbol.for('@vercel/request-context')
  ];
  context?.get?.()?.waitUntil?.(promise);
}
