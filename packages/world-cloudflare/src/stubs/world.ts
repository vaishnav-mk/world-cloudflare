/**
 * Replaces `@workflow/world-local` and `@workflow/world-vercel` in a Worker: the SDK imports both
 * to pick a default World, and neither loads on Workers. Register this World with `registerWorld()`.
 */
export function createWorld(): never {
  throw new Error('The SDK\'s built-in Worlds do not run on Workers: register one with registerWorld() from world-cloudflare');
}
