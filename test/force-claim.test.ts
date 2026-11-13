import { env, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { HookForceClaimedError, HookNotFoundError } from '@workflow/errors';
import type { Hook } from '@workflow/world';
import { createWorld } from '@workflow/world-cloudflare';
import { encode } from '../packages/world-cloudflare/src/lib/codec.js';
import { storeShardFor, tokenShardFor } from '../packages/world-cloudflare/src/lib/shards.js';

const world = createWorld(env);
const bytes = (text: string) => new TextEncoder().encode(text);

async function newRun(specVersion = 8): Promise<string> {
  const created = await world.events.create(null, {
    eventType: 'run_created', specVersion, eventData: { deploymentId: 'test-v1', workflowName: 'force', input: bytes('in') },
  });
  await world.events.create(created.run.runId, { eventType: 'run_started', specVersion });
  return created.run.runId;
}

/** Hook IDs are unique across a store, as the SDK's ULIDs are. */
const hookId = (name: string) => `${name}_${crypto.randomUUID()}`;
const hookCreated = (correlationId: string, token: string, force?: boolean) => ({
  eventType: 'hook_created' as const, correlationId, eventData: { token, ...(force ? { force } : {}) },
});
const received = (correlationId: string, token: string) => ({
  eventType: 'hook_received' as const, correlationId, eventData: { token, payload: bytes('7') },
});
const eventsOf = async (runId: string) => (await world.events.list({ runId })).data;

/** A token whose shard holds neither run, so the claim crosses three stores. */
function tokenAwayFrom(...runIds: string[]): string {
  const shards = new Set(runIds.map(storeShardFor));
  for (;;) {
    const token = crypto.randomUUID();
    if (!shards.has(tokenShardFor(token))) return token;
  }
}

describe('forced hook claims', () => {
  it("takes a live run's token: the holder is disposed of first, and its resumes follow the token", async () => {
    const [owner, claimer] = await Promise.all([newRun(), newRun()]);
    const [owned, claiming] = [hookId('owned'), hookId('claiming')];
    const token = tokenAwayFrom(owner, claimer);
    await world.events.create(owner, hookCreated(owned, token));
    const taken = await world.events.create(claimer, hookCreated(claiming, token, true));
    expect(taken.event?.eventType).toBe('hook_created');
    const claimedFrom = { runId: owner, hookId: owned, workflowName: 'force', deploymentId: 'test-v1', runSpecVersion: 8 };
    expect(taken.hook?.claimedFrom).toEqual(claimedFrom);
    expect(taken.event).toMatchObject({ eventData: { token, force: true, forceClaimedFrom: claimedFrom } });
    expect((await eventsOf(owner)).at(-1)).toMatchObject({
      eventType: 'hook_disposed', correlationId: owned, eventData: { token, forceClaimedBy: { runId: claimer, hookId: claiming } },
    });
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: claimer, hookId: claiming, claimedFrom });
    expect((await world.hooks.list({ runId: owner })).data).toEqual([]);
    // A resume that found the old holder is refused naming the new one, and lands nowhere.
    const refused = await world.events.create(owner, received(owned, token)).catch((error: unknown) => error);
    expect(HookForceClaimedError.is(refused)).toBe(true);
    expect(refused).toMatchObject({ token, claimedByRunId: claimer, claimedByHookId: claiming });
    await world.events.create(claimer, received(claiming, token));
    // The old holder's own disposal after the takeover finds no hook, which the SDK takes as done.
    expect(HookNotFoundError.is(await world.events.create(owner, { eventType: 'hook_disposed', correlationId: owned })
      .catch((error: unknown) => error))).toBe(true);
  });

  it('refuses to take a token from a running run that predates forced disposals', async () => {
    const [owner, claimer] = await Promise.all([newRun(7), newRun()]);
    const token = tokenAwayFrom(owner, claimer);
    await world.events.create(owner, hookCreated(hookId('owned'), token));
    const conflict = await world.events.create(claimer, hookCreated(hookId('claiming'), token, true));
    expect(conflict.event).toMatchObject({
      eventType: 'hook_conflict', eventData: { token, conflictingRunId: owner, forceRefusedReason: 'victim-spec-version' },
    });
    expect((await world.hooks.getByToken(token)).runId).toBe(owner);
    expect((await eventsOf(owner)).at(-1)?.eventType).toBe('hook_created');
  });

  it("takes a finished run's retained token without writing to its log", async () => {
    const [owner, claimer] = await Promise.all([newRun(), newRun()]);
    const kept = hookId('kept');
    const token = tokenAwayFrom(owner, claimer);
    await world.events.create(owner, { eventType: 'hook_created', correlationId: kept, eventData: {
      token, tokenRetentionUntil: new Date(Date.now() + 60_000),
    } });
    await world.events.create(owner, { eventType: 'run_completed', eventData: { output: bytes('done') } });
    const before = (await eventsOf(owner)).length;
    const taken = await world.events.create(claimer, hookCreated(hookId('claiming'), token, true));
    expect(taken.hook?.claimedFrom).toEqual({ runId: owner, hookId: kept });
    expect(await eventsOf(owner)).toHaveLength(before);
    expect((await world.hooks.list({ runId: owner })).data).toEqual([]);
  });

  it('answers a repeated forced claim with the same takeover', async () => {
    const [owner, claimer] = await Promise.all([newRun(), newRun()]);
    const owned = hookId('owned');
    const token = tokenAwayFrom(owner, claimer);
    await world.events.create(owner, hookCreated(owned, token));
    const shard = env.WORLD_STORE.getByName(tokenShardFor(token));
    const hook = { ...(await world.hooks.getByToken(token)), runId: claimer, hookId: hookId('claiming') } satisfies Hook;
    const first = await shard.claimHookTokenForced(hook);
    expect(first).toMatchObject({ kind: 'granted', hook: { claimedFrom: { runId: owner, hookId: owned } } });
    expect(await shard.claimHookTokenForced(hook)).toEqual(first);
    expect((await eventsOf(owner)).filter((event) => event.eventType === 'hook_disposed')).toHaveLength(1);
  });

  it('finishes a takeover a failed call left, from the alarm', async () => {
    const [owner, claimer] = await Promise.all([newRun(), newRun()]);
    const claiming = hookId('claiming');
    const token = tokenAwayFrom(owner, claimer);
    await world.events.create(owner, hookCreated(hookId('owned'), token));
    const holder = await world.hooks.getByToken(token);
    const shard = env.WORLD_STORE.getByName(tokenShardFor(token));
    // What a claim leaves when the holder's store cannot be reached: the row, and the token unmoved.
    await runInDurableObject(shard, (_store, state) => {
      state.storage.sql.exec('INSERT INTO token_takeovers (token, holder, claimer, next_at) VALUES (?, ?, ?, ?)',
        token, encode(holder), encode({ ...holder, runId: claimer, hookId: claiming }), Date.now());
    });
    await shard.drainOutbox();
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: claimer, hookId: claiming });
    expect((await eventsOf(owner)).at(-1)).toMatchObject({ eventType: 'hook_disposed', eventData: { forceClaimedBy: { runId: claimer } } });
  });

  it('disposes of a hook at its creation when another run took its token first', async () => {
    const [run, thief] = await Promise.all([newRun(), newRun()]);
    const [late, stolen] = [hookId('late'), hookId('thief')];
    const token = tokenAwayFrom(run, thief);
    // The token's shard took the hook before its creation committed, as in a chain of forced claims.
    expect(await env.WORLD_STORE.getByName(storeShardFor(run)).disposeForcedHook(run, late, token, { runId: thief, hookId: stolen }))
      .toMatchObject({ runId: run, hookId: late });
    await world.events.create(run, hookCreated(late, token));
    expect((await eventsOf(run)).slice(-2)).toMatchObject([
      { eventType: 'hook_created', correlationId: late },
      { eventType: 'hook_disposed', correlationId: late, eventData: { forceClaimedBy: { runId: thief, hookId: stolen } } },
    ]);
    expect((await world.hooks.list({ runId: run })).data).toEqual([]);
  });
});
