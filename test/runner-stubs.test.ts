import { expect, it } from 'vitest';
import { FreshRunnerStubs } from '../packages/world-cloudflare/src/queue/runner-stubs.js';
import type { WorldEnv } from '../packages/world-cloudflare/src/types.js';

/** A queue shard's state and runner namespace, recording alarms, background work and runner wakes. */
function fakes(failing = new Set<string>()) {
  const pending: Promise<unknown>[] = [];
  const woken: string[] = [];
  let alarm: number | null = null;
  // SAFETY: FreshRunnerStubs uses only `waitUntil`, `storage.getAlarm` and `storage.setAlarm`.
  const ctx = {
    waitUntil: (promise: Promise<unknown>) => pending.push(promise),
    storage: { getAlarm: async () => alarm, setAlarm: async (at: number) => { alarm = at; } },
  } as unknown as DurableObjectState;
  // SAFETY: FreshRunnerStubs only gets stubs by name and calls `wake` on them.
  const runners = {
    getByName: (name: string) => ({
      name,
      wake: async () => {
        if (failing.has(name)) throw new Error('unreachable');
        woken.push(name);
      },
    }),
  } as unknown as WorldEnv['RUNNER'];
  return { stubs: new FreshRunnerStubs(ctx, runners), pending, woken, alarm: () => alarm };
}

it('uses a plain stub until the alarm makes a fresh one, then that stub until it is forgotten', async () => {
  const { stubs, pending, woken, alarm } = fakes();
  const plain = stubs.get('runner-1');
  await Promise.all(pending);
  // The miss brought the alarm forward, so the alarm makes the stub soon.
  expect(alarm()).not.toBeNull();
  expect(stubs.get('runner-1')).not.toBe(plain);
  await stubs.freshen();
  expect(woken).toEqual(['runner-1']);
  const fresh = stubs.get('runner-1');
  expect(stubs.get('runner-1')).toBe(fresh);
  // A stub whose call threw is dropped, so the next delivery gets a new one.
  stubs.forget('runner-1');
  expect(stubs.get('runner-1')).not.toBe(fresh);
});

it('keeps asking the alarm for a runner it could not reach', async () => {
  const failing = new Set(['runner-2']);
  const { stubs, woken } = fakes(failing);
  stubs.get('runner-2');
  await stubs.freshen();
  expect(woken).toEqual([]);
  failing.clear();
  await stubs.freshen();
  expect(woken).toEqual(['runner-2']);
});
