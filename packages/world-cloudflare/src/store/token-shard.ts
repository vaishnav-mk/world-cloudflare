import type { Hook } from '@workflow/world';
import { STORE_NAME } from '../constants.js';
import { tokenShardFor } from '../lib/shards.js';
import type { ResolveDataMode, TokenRelease } from '../types.js';
import type { StoreContext } from './context.js';
import { retentionLapsed } from './tokens.js';
import { retryClaim } from './force-claims.js';
import { hookView } from './views.js';

/** Storage key a token shard sets once it has imported the legacy store's tokens. */
const LEGACY_TOKENS_IMPORTED = 'legacy-tokens-imported';

/**
 * A token shard is the registry (`TokenRegistry`) for the hook tokens that hash to it
 * (`tokenShardFor`), which keeps a token unique across runs. Every store calls it.
 */

/**
 * Claims a token for a store's new hook. Returns the run that holds it, or null once granted. A
 * store that is not the token's shard, such as the legacy store called by an older store, passes
 * the claim on.
 */
export async function claimHookToken(store: StoreContext, hook: Hook): Promise<string | null> {
  const shard = tokenShardFor(hook.token);
  if (shard !== store.name) return store.peer(shard).claimHookToken(hook);
  await importLegacyTokens(store, shard);
  return store.ctx.storage.transactionSync(() => {
    if (store.takeovers.find(hook.token) !== undefined) throw retryClaim(hook.token);
    const holder = store.tokens.find(hook.token);
    if (holder !== undefined) {
      if (holder.hook.hookId === hook.hookId && holder.hook.runId === hook.runId) return null;
      if (!holder.finished || !retentionLapsed(holder.hook, new Date())) return holder.hook.runId;
    }
    throw retryClaim(hook.token);
  });
}

/** Applies releases from the stores whose hooks held these tokens. */
export async function releaseTokens(store: StoreContext, releases: TokenRelease[]): Promise<void> {
  const shards = new Set(releases.map(({ token }) => tokenShardFor(token)));
  for (const shard of shards) {
    if (shard !== store.name) throw new Error('Token released on the wrong shard');
    await importLegacyTokens(store, shard);
  }
  const now = new Date();
  store.ctx.storage.transactionSync(() => {
    for (const release of releases) store.tokens.release(release, now);
  });
}

/** The hook holding `token`, read from the token's shard. */
export async function getHookByToken(store: StoreContext, token: string, resolveData?: ResolveDataMode): Promise<Hook> {
  const shard = tokenShardFor(token);
  if (shard !== store.name) return store.peer(shard).getHookByToken(token, resolveData);
  await importLegacyTokens(store, shard);
  return hookView(store.tokens.get(token), resolveData);
}

/**
 * Once per token shard, before its registry first answers: copies in the tokens the legacy store's
 * hooks hold, so they still conflict. Legacy runs claim new tokens on the shards too, so one copy
 * is enough. Other requests wait meanwhile, so a release that arrives during the copy applies after it.
 */
export async function importLegacyTokens(store: StoreContext, shard: string): Promise<void> {
  const { ctx } = store;
  const imported = () => ctx.storage.kv.get(LEGACY_TOKENS_IMPORTED) !== undefined;
  if (imported()) return;
  let failure: { error: unknown } | undefined;
  await ctx.blockConcurrencyWhile(async () => {
    if (imported()) return;
    try {
      const held = await store.peer(STORE_NAME).legacyTokens(shard);
      ctx.storage.transactionSync(() => {
        store.tokens.import(held);
        ctx.storage.kv.put(LEGACY_TOKENS_IMPORTED, Date.now());
      });
    } catch (error) {
      // Thrown from here it would reset the object; the request that needed the import fails instead.
      failure = { error };
    }
  });
  if (failure !== undefined) throw failure.error;
}
