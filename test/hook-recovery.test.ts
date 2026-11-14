import { env, runDurableObjectAlarm, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it, vi } from 'vitest';
import { createWorld, type WorldEnv, type WorldStore } from '@workflow/world-cloudflare';
import { START_HOOK_CANDIDATE_MAX_AGE_MS, startHookId } from '@workflow/world';
import { newRunId } from './ids.js';
import { HookNotFoundError } from '@workflow/errors';
import type { HookClaimOperation } from '../packages/world-cloudflare/src/types.js';
import { StoreContext } from '../packages/world-cloudflare/src/store/context.js';
import { STORE_NAME } from '../packages/world-cloudflare/src/constants.js';
import { storeShardFor, tokenShardFor } from '../packages/world-cloudflare/src/lib/shards.js';

const world = createWorld(env);
const bytes = new TextEncoder().encode('input');
async function newRun(specVersion = 8) {
  const { run } = await world.events.create(null, {
    eventType: 'run_created', specVersion, eventData: { deploymentId: 'test-v1', workflowName: 'hook-recovery', input: bytes },
  });
  await world.events.create(run.runId, { eventType: 'run_started', specVersion });
  return run.runId;
}
const hookCreated = (token: string, force = false, correlationId = crypto.randomUUID()) => ({
  eventType: 'hook_created' as const, correlationId, eventData: { token, force },
});

/** Deterministic network barrier, with no timers or polling. */
function barrier() {
  let open = () => {};
  const reached = new Promise<void>((resolve) => { open = resolve; });
  return { reached, open };
}

/** Swap only a named RPC; all index/release/status traffic still reaches real objects. */
function intercept(store: WorldStore, method: string, call: (peer: DurableObjectStub<WorldStore>, args: unknown[]) => Promise<unknown>) {
  // SAFETY: this is the object's own environment, replaced only for the duration of a test.
  const self = store as unknown as { env: WorldEnv };
  const real = self.env;
  self.env = { ...real, WORLD_STORE: new Proxy(real.WORLD_STORE, { get(target, key) {
    if (key !== 'getByName') return Reflect.get(target, key, target);
    return (name: string) => {
      const peer = target.getByName(name);
      return new Proxy(peer, { get(stub, rpc) {
        if (rpc === method) return (...args: unknown[]) => call(peer, args);
        return Reflect.get(stub, rpc, stub);
      } });
    };
  } }) };
  return () => { self.env = real; };
}

const free = (token: string) => world.hooks.getByToken(token).then(() => false, (error: unknown) => HookNotFoundError.is(error));
function awayFrom(...runIds: string[]) {
  for (;;) {
    const token = crypto.randomUUID();
    if (runIds.every((runId) => storeShardFor(runId) !== tokenShardFor(token))) return token;
  }
}

describe('durable hook ownership', () => {
  it('keeps queued run writers on one gate when the first caller returns', async () => {
    await runInDurableObject(env.WORLD_STORE.getByName('gate-test'), (_store, state) => {
      const context = new StoreContext(state, env.QUEUE_SHARD, env.RUNNER, (name) => env.WORLD_STORE.getByName(name));
      const first = barrier();
      const second = barrier();
      let active = 0;
      let maximum = 0;
      const work = async (pause?: Promise<void>) => {
        maximum = Math.max(maximum, ++active);
        if (pause !== undefined) await pause;
        active--;
      };
      return (async () => {
        const a = context.writeRun('run', () => work(first.reached));
        const b = context.writeRun('run', () => work(second.reached));
        first.open();
        await a;
        const c = context.writeRun('run', () => work());
        second.open();
        await Promise.all([b, c]);
        expect(maximum).toBe(1);
      })();
    });
  });

  it('accepts large hook metadata without retaining it in receipts', async () => {
    const runId = await newRun();
    const token = crypto.randomUUID();
    const request = hookCreated(token);
    const metadata = new Uint8Array(140_000);
    const result = await world.events.create(runId, { ...request, eventData: { ...request.eventData, metadata } });
    expect(result.hook?.metadata).toEqual(metadata);
    await runInDurableObject(env.WORLD_STORE.getByName(tokenShardFor(token)), (_store, state) => {
      const receipts = [...state.storage.kv.list({ prefix: 'hook-decision:' })];
      expect(receipts.every(([, receipt]) => JSON.stringify(receipt).length < 1_024)).toBe(true);
      const digests = [...state.storage.kv.list({ prefix: 'hook-operation:' })];
      expect(digests.every(([, digest]) => typeof digest === 'string' && /^[0-9a-f]{64}$/.test(digest))).toBe(true);
    });
  });

  it('validates requester parameters and reserves hook IDs before remote disposal', async () => {
    const owner = await newRun();
    const requester = await newRun();
    const token = awayFrom(owner, requester);
    const original = await world.events.create(owner, hookCreated(token));
    const stub = env.WORLD_STORE.getByName(storeShardFor(requester));
    expect(await Promise.resolve(stub.createEvent(requester, hookCreated(token, true), { requireHookResumeDedup: true }))
      .catch((error: unknown) => error)).toMatchObject({ status: 400 });
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner, hookId: original.hook?.hookId });
    expect((await world.events.list({ runId: owner })).data.at(-1)?.eventType).toBe('hook_created');
  });

  it('serializes a gated forced claim with later fenced, batch and terminal writes', async () => {
    const owner = await newRun();
    const requester = await newRun(9);
    const token = awayFrom(owner, requester);
    await world.events.create(owner, hookCreated(token));
    const firstToken = awayFrom(owner, requester);
    const entered = barrier();
    const resume = barrier();
    await runInDurableObject(env.WORLD_STORE.getByName(storeShardFor(requester)), async (store) => {
      const restore = intercept(store, 'claimAcceptedHook', async (peer, args) => {
        entered.open();
        await resume.reached;
        return peer.claimAcceptedHook(args[0] as HookClaimOperation);
      });
      try {
        const first = store.createEvent(requester, hookCreated(firstToken), { inBand: true, expectedSeqInBand: 1 });
        await entered.reached;
        const stale = store.createEvent(requester, hookCreated(token, true), { inBand: true, expectedSeqInBand: 1 })
          .catch((error: unknown) => error);
        const batch = store.createEventBatch(requester, [{ event: { eventType: 'step_created', correlationId: 'behind',
          eventData: { stepName: 'behind', input: bytes } } }], { inBand: true, expectedSeqInBand: 1 }).catch((error: unknown) => error);
        const terminal = store.createEvent(requester, { eventType: 'run_cancelled' });
        expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner });
        resume.open();
        expect((await first).event?.eventType).toBe('hook_created');
        expect(await stale).toMatchObject({ status: 412 });
        expect(await batch).toMatchObject({ status: 412 });
        expect((await terminal).run?.status).toBe('cancelled');
      } finally { resume.open(); restore(); }
    });
    expect(await free(firstToken)).toBe(true);
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner });
    expect((await world.events.list({ runId: owner })).data.at(-1)?.eventType).toBe('hook_created');
  });

  it('does not accept a public hook creation retry after disposal and reset', async () => {
    const runId = await newRun();
    const token = awayFrom(runId);
    const request = hookCreated(token);
    await world.events.create(runId, request);
    await world.events.create(runId, { eventType: 'hook_disposed', correlationId: request.correlationId });
    const stub = () => env.WORLD_STORE.getByName(storeShardFor(runId));
    await runInDurableObject(stub(), (_store, state) => state.abort('lost old creation reply')).catch(() => {});
    expect(await world.events.create(runId, request).catch((error: unknown) => error)).toMatchObject({ name: 'EntityConflictError' });
    expect(await free(token)).toBe(true);
    expect((await world.events.list({ runId })).data.filter((event) => event.eventType === 'hook_created')).toHaveLength(1);
  });

  it('durably rejects a registry hook ID collision without blocking later run mutations', async () => {
    const owner = await newRun();
    let requester = await newRun();
    while (storeShardFor(requester) === storeShardFor(owner)) requester = await newRun();
    const token = awayFrom(owner, requester);
    const request = hookCreated(token);
    await world.events.create(owner, request);
    const collision = await world.events.create(requester, request).catch((error: unknown) => error);
    expect(collision).toMatchObject({ name: 'EntityConflictError' });
    expect((await world.events.create(requester, { eventType: 'run_cancelled' })).run?.status).toBe('cancelled');
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner });
  });

  it('rolls back admission when recovery scheduling fails', async () => {
    const owner = await newRun();
    const requester = await newRun();
    const token = awayFrom(owner, requester);
    await world.events.create(owner, hookCreated(token));
    await runInDurableObject(env.WORLD_STORE.getByName(storeShardFor(requester)), async (store, state) => {
      const before = [...state.storage.kv.list({ prefix: 'hook-intent:' })];
      const alarm = vi.spyOn(state.storage, 'getAlarm').mockRejectedValue(new Error('alarm unavailable'));
      try {
        expect(await store.createEvent(requester, hookCreated(token, true)).catch((error: unknown) => error))
          .toMatchObject({ message: 'Failed to schedule hook intent' });
        expect(state.storage.sql.exec('SELECT run_id FROM hook_claim_intents WHERE run_id = ?', requester).toArray()).toEqual([]);
        expect([...state.storage.kv.list({ prefix: 'hook-intent:' })]).toEqual(before);
      } finally { alarm.mockRestore(); }
    });
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner });
    expect((await world.events.list({ runId: owner })).data.at(-1)?.eventType).toBe('hook_created');
  });

  it('rolls back an unschedulable token reservation, then recovers the accepted source intent', async () => {
    const owner = await newRun();
    const requester = await newRun();
    const token = awayFrom(owner, requester);
    await world.events.create(owner, hookCreated(token));
    await runInDurableObject(env.WORLD_STORE.getByName(tokenShardFor(token)), async (_registry, state) => {
      const alarm = vi.spyOn(state.storage, 'getAlarm').mockRejectedValue(new Error('registry alarm unavailable'));
      try {
        expect(await world.events.create(requester, hookCreated(token, true)).catch((error: unknown) => error))
          .toMatchObject({ message: 'Failed to schedule token takeover' });
        expect(state.storage.sql.exec('SELECT token FROM token_takeovers WHERE token = ?', token).toArray()).toEqual([]);
      } finally { alarm.mockRestore(); }
    });
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner });
    await env.WORLD_STORE.getByName(storeShardFor(requester)).drainOutbox();
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: requester });
  });

  it('reserves pending hook IDs store-wide and retains their recovery alarm through terminal writes', async () => {
    const runId = await newRun();
    let other: string;
    do { other = newRunId(); } while (storeShardFor(other) !== storeShardFor(runId));
    await world.events.create(other, { eventType: 'run_created', specVersion: 8, eventData: {
      deploymentId: 'test-v1', workflowName: 'reservation', input: bytes,
    } });
    const token = awayFrom(runId);
    const request = hookCreated(token);
    const stub = () => env.WORLD_STORE.getByName(storeShardFor(runId));
    await runInDurableObject(stub(), async (store, state) => {
      const restore = intercept(store, 'claimAcceptedHook', async () => { throw new Error('down before grant'); });
      try {
        await store.createEvent(runId, request).catch(() => {});
        expect(await store.createEvent(other, hookCreated(crypto.randomUUID(), false, request.correlationId))
          .catch((error: unknown) => error)).toMatchObject({ name: 'EntityConflictError' });
        await store.createEvent(other, { eventType: 'run_cancelled' });
        expect(await state.storage.getAlarm()).not.toBeNull();
      } finally { restore(); }
    });
    await runInDurableObject(stub(), (_store, state) => state.abort('reset')).catch(() => {});
    expect(await runDurableObjectAlarm(stub())).toBe(true);
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId });
  });

  it('rechecks the fence when an earlier RPC delays admission', async () => {
    const owner = await newRun();
    const requester = await newRun(9);
    const token = awayFrom(owner, requester);
    await world.events.create(owner, hookCreated(token));
    const entered = barrier();
    const resume = barrier();
    await runInDurableObject(env.WORLD_STORE.getByName(storeShardFor(requester)), async (store, state) => {
      const debtToken = awayFrom(requester);
      state.storage.sql.exec('INSERT INTO token_releases (hook_id, token, disposed, next_at) VALUES (?, ?, 1, ?)',
        crypto.randomUUID(), debtToken, Date.now());
      const restore = intercept(store, 'releaseTokens', async (peer, args) => {
        entered.open();
        await resume.reached;
        return peer.releaseTokens(args[0] as { hookId: string; token: string; disposed: boolean }[]);
      });
      try {
        const stale = store.createEvent(requester, hookCreated(token, true), { inBand: true, expectedSeqInBand: 1 })
          .catch((error: unknown) => error);
        await entered.reached;
        await store.createEventBatch(requester, [{ event: { eventType: 'step_created', correlationId: 'advance-before-claim',
          eventData: { stepName: 'advance', input: bytes } } }], { inBand: true, expectedSeqInBand: 1 });
        resume.open();
        expect(await stale).toMatchObject({ status: 412 });
      } finally { resume.open(); restore(); }
    });
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner });
    expect((await world.events.list({ runId: owner })).data.at(-1)?.eventType).toBe('hook_created');
  });

  it('keeps a pending takeover reserved when its old owner releases', async () => {
    const owner = await newRun();
    const requester = await newRun();
    const contender = await newRun();
    const token = awayFrom(owner, requester, contender);
    const original = await world.events.create(owner, hookCreated(token));
    const entered = barrier();
    const resume = barrier();
    await runInDurableObject(env.WORLD_STORE.getByName(tokenShardFor(token)), async (registry) => {
      const restore = intercept(registry, 'disposeForcedHook', async (peer, args) => {
        entered.open();
        await resume.reached;
        return peer.disposeForcedHook(args[0] as string, args[1] as string, args[2] as string, args[3] as { runId: string; hookId: string });
      });
      try {
        const taking = world.events.create(requester, hookCreated(token, true));
        await entered.reached;
        await world.events.create(owner, { eventType: 'hook_disposed', correlationId: original.hook?.hookId ?? '' });
        for (const force of [false, true]) {
          expect(await world.events.create(contender, hookCreated(token, force)).catch((error: unknown) => error)).toMatchObject({ status: 503 });
        }
        resume.open();
        await taking;
      } finally { resume.open(); restore(); }
    });
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: requester });
    await env.WORLD_STORE.getByName(storeShardFor(contender)).drainOutbox();
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: requester });
  });

  for (const fail of [false, true]) it(`ignores a delayed takeover ${fail ? 'failure' : 'completion'} after a newer reservation`, async () => {
    const [a, b, c] = [await newRun(), await newRun(), await newRun()];
    const token = awayFrom(a, b, c);
    await world.events.create(a, hookCreated(token));
    const first = barrier();
    const late = barrier();
    const newer = barrier();
    const complete = barrier();
    await runInDurableObject(env.WORLD_STORE.getByName(tokenShardFor(token)), async (registry, state) => {
      let calls = 0;
      const restore = intercept(registry, 'disposeForcedHook', async (peer, args) => {
        const n = ++calls;
        if (n === 1) {
          first.open();
          await late.reached;
          if (fail) throw new Error('late disposal failure');
        }
        if (n === 3) { newer.open(); await complete.reached; }
        return peer.disposeForcedHook(args[0] as string, args[1] as string, args[2] as string, args[3] as { runId: string; hookId: string });
      });
      try {
        const takingB = world.events.create(b, hookCreated(token, true)).catch((error: unknown) => error);
        await first.reached;
        // A retry finishes B while its original RPC is still in flight.
        await registry.finishTakeover(token);
        const takingC = world.events.create(c, hookCreated(token, true));
        await newer.reached;
        const reservation = () => state.storage.sql.exec('SELECT operation, attempts, next_at FROM token_takeovers WHERE token = ?', token).one();
        const before = reservation();
        late.open();
        await takingB;
        expect(reservation()).toEqual(before);
        complete.open();
        await takingC;
      } finally { late.open(); complete.open(); restore(); }
    });
    await env.WORLD_STORE.getByName(storeShardFor(b)).drainOutbox();
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: c });
  });

  it('recovers a forced grant lost before local commit, even when another claim already took it', async () => {
    const [owner, requester, thief] = [await newRun(), await newRun(), await newRun()];
    const token = awayFrom(owner, requester, thief);
    await world.events.create(owner, hookCreated(token));
    const stub = () => env.WORLD_STORE.getByName(storeShardFor(requester));
    await runInDurableObject(stub(), async (store) => {
      const restore = intercept(store, 'claimAcceptedHook', async (peer, args) => {
        const operation = args[0] as HookClaimOperation;
        if (operation.hook.runId !== requester) return peer.claimAcceptedHook(operation);
        await peer.claimAcceptedHook(operation);
        await world.events.create(thief, hookCreated(token, true));
        throw new Error('lost forced grant');
      });
      try {
        expect(await store.createEvent(requester, hookCreated(token, true)).catch((error: unknown) => error))
          .toMatchObject({ message: 'lost forced grant' });
      } finally { restore(); }
    });
    await runInDurableObject(stub(), (_store, state) => state.abort('reset')).catch(() => {});
    expect(await runDurableObjectAlarm(stub())).toBe(true);
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: thief });
    expect((await world.hooks.list({ runId: requester })).data).toEqual([]);
    expect((await world.events.list({ runId: requester })).data.slice(-2).map((event) => event.eventType))
      .toEqual(['hook_created', 'hook_disposed']);
  });

  for (const grant of [false, true]) it(`recovers reset ${grant ? 'after a lost grant reply' : 'before registry acquisition'} without resurrection`, async () => {
    const runId = await newRun();
    const token = awayFrom(runId);
    const request = hookCreated(token);
    const stub = () => env.WORLD_STORE.getByName(storeShardFor(runId));
    let operation: HookClaimOperation | undefined;
    await runInDurableObject(stub(), async (store) => {
      const restore = intercept(store, 'claimAcceptedHook', async (peer, args) => {
        operation = args[0] as HookClaimOperation;
        if (grant) await peer.claimAcceptedHook(operation);
        throw new Error('lost claim reply');
      });
      try {
        expect(await store.createEvent(runId, request).catch((error: unknown) => error)).toMatchObject({ message: 'lost claim reply' });
      } finally { restore(); }
    });
    await runInDurableObject(stub(), (_store, state) => state.abort('reset')).catch(() => {});
    // A later mutation must replay admission before cancelling, even without a drain call.
    await stub().createEvent(runId, { eventType: 'run_cancelled' });
    await stub().drainOutbox();
    expect(await free(token)).toBe(true);
    if (operation === undefined) throw new Error('Expected a captured operation');
    await env.WORLD_STORE.getByName(tokenShardFor(token)).claimAcceptedHook(operation);
    expect(await free(token)).toBe(true);
    expect((await world.events.list({ runId })).data.filter((event) => event.eventType === 'hook_created')).toHaveLength(1);
  });
  for (const specVersion of [8, 9]) for (const eventType of ['run_created', 'run_started'] as const) {
    it(`can take a start Hook whose accepted run creation has not committed yet (${eventType}, spec ${specVersion})`, async () => {
      const candidate = newRunId();
      const thief = await newRun();
      const token = awayFrom(candidate, thief);
      const claiming = hookCreated(token, true);
      await runInDurableObject(env.WORLD_STORE.getByName(storeShardFor(candidate)), async (store, state) => {
        const restore = intercept(store, 'claimAcceptedHook', async (peer, args) => {
          const operation = args[0] as HookClaimOperation;
          const answer = await peer.claimAcceptedHook(operation);
          if (operation.hook.runId !== candidate) return answer;
          const taken = await world.events.create(thief, claiming);
          expect(taken.hook?.claimedFrom).toEqual({ runId: candidate, hookId: startHookId(candidate) });
          return answer;
        });
        try {
          const result = await store.createEvent(candidate, { eventType, specVersion, eventData: {
            deploymentId: 'test-v1', workflowName: 'precommit-start', input: bytes, startHook: { token },
          } }, { inBand: true, expectedSeqInBand: 1 });
          expect(result.allocated).toBe(1);
          if (specVersion === 9) {
            expect(state.storage.sql.exec('SELECT seq_in_band FROM runs WHERE id = ?', candidate).one().seq_in_band)
              .toBe(eventType === 'run_created' ? 1 : 2);
          }
        } finally { restore(); }
      });
      await env.WORLD_STORE.getByName(storeShardFor(candidate)).drainOutbox();
      expect(await world.hooks.getByToken(token)).toMatchObject({ runId: thief });
      expect((await world.hooks.list({ runId: candidate })).data).toEqual([]);
      const events = (await world.events.list({ runId: candidate })).data;
      expect(events.map((event) => event.eventType))
        .toEqual(eventType === 'run_created' ? ['run_created', 'hook_disposed'] : ['run_created', 'run_started', 'hook_disposed']);
      expect(events.at(-1)).toMatchObject({ correlationId: startHookId(candidate), eventData: {
        token, forceClaimedBy: { runId: thief, hookId: claiming.correlationId },
      } });
    });
  }

  it('finishes a paused takeover after its start candidate expires and removes its intent', async () => {
    const candidate = newRunId();
    const thief = await newRun();
    const token = awayFrom(candidate, thief);
    const stub = () => env.WORLD_STORE.getByName(storeShardFor(candidate));
    let operation: HookClaimOperation | undefined;
    await runInDurableObject(stub(), async (store) => {
      const restore = intercept(store, 'claimAcceptedHook', async (peer, args) => {
        operation = args[0] as HookClaimOperation;
        await peer.claimAcceptedHook(operation);
        throw new Error('lost start grant');
      });
      try {
        expect(await store.createEvent(candidate, { eventType: 'run_created', specVersion: 9, eventData: {
          deploymentId: 'test-v1', workflowName: 'expiry-during-takeover', input: bytes, startHook: { token },
        } }).catch((error: unknown) => error)).toMatchObject({ message: 'lost start grant' });
      } finally { restore(); }
    });
    const claiming = hookCreated(token, true);
    const checkIdentity = async () => {
      const claimedBy = { runId: thief, hookId: claiming.correlationId };
      const unknown = newRunId();
      for (const [runId, hookId, otherToken] of [
        [candidate, crypto.randomUUID(), token],
        [candidate, startHookId(candidate), crypto.randomUUID()],
        [unknown, startHookId(unknown), token],
      ]) {
        expect(await stub().disposeForcedHook(runId, hookId, otherToken, claimedBy).catch((error: unknown) => error))
          .toMatchObject({ name: 'WorkflowRunNotFoundError' });
      }
      expect(await env.WORLD_STORE.getByName(STORE_NAME).disposeForcedHook(candidate, startHookId(candidate), token, claimedBy)
        .catch((error: unknown) => error)).toMatchObject({ name: 'WorkflowRunNotFoundError' });
    };
    await checkIdentity();
    const entered = barrier();
    const resume = barrier();
    await runInDurableObject(env.WORLD_STORE.getByName(tokenShardFor(token)), async (registry, state) => {
      const restore = intercept(registry, 'disposeForcedHook', async (peer, args) => {
        entered.open();
        await resume.reached;
        return peer.disposeForcedHook(args[0] as string, args[1] as string, args[2] as string, args[3] as { runId: string; hookId: string });
      });
      const taking = world.events.create(thief, claiming).catch((error: unknown) => error);
      try {
        await entered.reached;
        const clock = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + START_HOOK_CANDIDATE_MAX_AGE_MS + 1_000);
        try { await stub().drainOutbox(); } finally { clock.mockRestore(); }
        await runInDurableObject(stub(), (_store, source) => {
          expect(source.storage.sql.exec('SELECT run_id FROM hook_claim_intents WHERE run_id = ?', candidate).toArray()).toEqual([]);
        });
        await runInDurableObject(stub(), (_store, source) => source.abort('expired candidate reset')).catch(() => {});
        await checkIdentity();
        expect(state.storage.sql.exec('SELECT token FROM tokens WHERE token = ?', token).toArray()).toEqual([]);
        expect(state.storage.sql.exec('SELECT token FROM token_takeovers WHERE token = ?', token).toArray()).toHaveLength(1);
        resume.open();
        expect(await taking).toMatchObject({ hook: { claimedFrom: { runId: candidate, hookId: startHookId(candidate) } } });
        expect(state.storage.sql.exec('SELECT token FROM token_takeovers WHERE token = ?', token).toArray()).toEqual([]);
      } finally { resume.open(); restore(); }
    });
    if (operation === undefined) throw new Error('Expected a start operation');
    expect(await env.WORLD_STORE.getByName(tokenShardFor(token)).claimAcceptedHook(operation)
      .catch((error: unknown) => error)).toMatchObject({ status: 503 });
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: thief });
    expect(await Promise.resolve(stub().getRun(candidate)).catch((error: unknown) => error))
      .toMatchObject({ name: 'WorkflowRunNotFoundError' });
    await runInDurableObject(stub(), (_store, state) => {
      expect(state.storage.sql.exec('SELECT id FROM hooks WHERE run_id = ?', candidate).toArray()).toEqual([]);
      expect(state.storage.sql.exec('SELECT run_id FROM forced_disposals WHERE hook_id = ?', startHookId(candidate)).toArray())
        .toEqual([{ run_id: candidate }]);
    });
  });

  for (const grant of [false, true]) it(`cancels an expired start candidate ${grant ? 'after grant' : 'before delayed grant'}`, async () => {
    const runId = newRunId();
    const token = awayFrom(runId);
    const stub = () => env.WORLD_STORE.getByName(storeShardFor(runId));
    let operation: HookClaimOperation | undefined;
    await runInDurableObject(stub(), async (store) => {
      const restore = intercept(store, 'claimAcceptedHook', async (peer, args) => {
        operation = args[0] as HookClaimOperation;
        if (grant) await peer.claimAcceptedHook(operation);
        throw new Error('lost start grant');
      });
      try {
        expect(await store.createEvent(runId, { eventType: 'run_created', specVersion: 8, eventData: {
          deploymentId: 'test-v1', workflowName: 'expiry', input: bytes, startHook: { token },
        } }).catch((error: unknown) => error)).toMatchObject({ message: 'lost start grant' });
      } finally { restore(); }
    });
    await runInDurableObject(stub(), (_store, state) => state.abort('reset')).catch(() => {});
    const clock = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + START_HOOK_CANDIDATE_MAX_AGE_MS + 1_000);
    try { await stub().drainOutbox(); } finally { clock.mockRestore(); }
    expect(await free(token)).toBe(true);
    if (operation === undefined) throw new Error('Expected a start operation');
    expect(await env.WORLD_STORE.getByName(tokenShardFor(token)).claimAcceptedHook(operation)
      .catch((error: unknown) => error)).toMatchObject({ status: 503 });
    expect(await free(token)).toBe(true);
    expect(await Promise.resolve(stub().getRun(runId)).catch((error: unknown) => error)).toMatchObject({ name: 'WorkflowRunNotFoundError' });
  });

  it('supports same-store forced claims and reciprocal takeovers without a gate deadlock', async () => {
    // Tokens stay on epoch 0; an epoch 0 run can share its token's store.
    const same = newRunId(1_600_000_000_000);
    await world.events.create(same, { eventType: 'run_created', specVersion: 8, eventData: {
      deploymentId: 'test-v1', workflowName: 'same-store', input: bytes,
    } });
    await world.events.create(same, { eventType: 'run_started' });
    let token: string;
    do { token = crypto.randomUUID(); } while (tokenShardFor(token) !== storeShardFor(same));
    await world.events.create(same, hookCreated(token));
    await world.events.create(same, hookCreated(token, true));
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: same });

    const a = await newRun();
    let b = await newRun();
    while (storeShardFor(a) === storeShardFor(b)) b = await newRun();
    const [tokenA, tokenB] = [awayFrom(a, b), awayFrom(a, b)];
    await world.events.create(a, hookCreated(tokenA));
    await world.events.create(b, hookCreated(tokenB));
    const both = barrier();
    let count = 0;
    const paused = async (peer: DurableObjectStub<WorldStore>, args: unknown[]) => {
      if (++count === 2) both.open();
      await both.reached;
      return peer.claimAcceptedHook(args[0] as HookClaimOperation);
    };
    await runInDurableObject(env.WORLD_STORE.getByName(storeShardFor(a)), async (storeA) => {
      const restoreA = intercept(storeA, 'claimAcceptedHook', paused);
      try {
        const takingA = storeA.createEvent(a, hookCreated(tokenB, true));
        await runInDurableObject(env.WORLD_STORE.getByName(storeShardFor(b)), async (storeB) => {
          const restoreB = intercept(storeB, 'claimAcceptedHook', paused);
          try {
            await Promise.all([takingA, storeB.createEvent(b, hookCreated(tokenA, true))]);
          } finally { restoreB(); }
        });
      } finally { restoreA(); }
    });
    expect(await world.hooks.getByToken(tokenA)).toMatchObject({ runId: b });
    expect(await world.hooks.getByToken(tokenB)).toMatchObject({ runId: a });
  });

  it('does not repeat a local commit after reset or replay a grant into registry ownership', async () => {
    const owner = await newRun();
    const thief = await newRun();
    const token = awayFrom(owner, thief);
    const stub = () => env.WORLD_STORE.getByName(storeShardFor(owner));
    let operation: HookClaimOperation | undefined;
    await runInDurableObject(stub(), async (store) => {
      const restore = intercept(store, 'claimAcceptedHook', async (peer, args) => {
        operation = args[0] as HookClaimOperation;
        return peer.claimAcceptedHook(operation);
      });
      try { await store.createEvent(owner, hookCreated(token)); } finally { restore(); }
    });
    await runInDurableObject(stub(), (_store, state) => state.abort('lost commit reply')).catch(() => {});
    await stub().drainOutbox();
    await world.events.create(thief, hookCreated(token, true));
    if (operation === undefined) throw new Error('Expected an operation');
    const registry = env.WORLD_STORE.getByName(tokenShardFor(token));
    expect(await registry.claimAcceptedHook(operation)).toMatchObject({ kind: 'granted', hook: { runId: owner } });
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: thief });
    expect(await registry.claimAcceptedHook({ ...operation, force: true }).catch((error: unknown) => error)).toMatchObject({ status: 400 });
    expect((await world.events.list({ runId: owner })).data.filter((event) => event.eventType === 'hook_created')).toHaveLength(1);
  });

  it('refuses a stale forced requester before disposing its victim', async () => {
    const owner = await newRun();
    const requester = await newRun(9);
    const token = crypto.randomUUID();
    const original = await world.events.create(owner, hookCreated(token));
    const store = env.WORLD_STORE.getByName(storeShardFor(requester));
    await store.createEvent(requester, { eventType: 'step_created', correlationId: 'advance', eventData: { stepName: 'advance', input: bytes } },
      { inBand: true, expectedSeqInBand: 1 });
    expect(await Promise.resolve(store.createEvent(requester, hookCreated(token, true), { inBand: true, expectedSeqInBand: 1 }))
      .catch((error: unknown) => error)).toMatchObject({ name: 'InBandSupersededError' });
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner, hookId: original.hook?.hookId });
    expect((await world.events.list({ runId: owner })).data.at(-1)?.eventType).toBe('hook_created');
  });
  it('registers a free forced token and conflicts with an ordinary claim', async () => {
    const owner = await newRun();
    const contender = await newRun();
    const token = crypto.randomUUID();
    const result = await world.events.create(owner, hookCreated(token, true));
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner, hookId: result.hook?.hookId });
    expect((await world.events.create(contender, hookCreated(token))).event).toMatchObject({ eventType: 'hook_conflict' });
  });
});
