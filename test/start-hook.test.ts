import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { START_HOOK_ADMISSION_REJECTED_CODE, START_HOOK_CANDIDATE_MAX_AGE_MS, startHookId } from '@workflow/world';
import { createWorld } from '@workflow/world-cloudflare';
import { newRunId } from './ids.js';

const world = createWorld(env);
const runData = (token: string) => ({
  deploymentId: 'test-v1', workflowName: 'startHook', input: new Uint8Array([1]), startHook: { token },
});

describe('start Hook admission', () => {
  it('refuses a candidate past the orphan bound through either copy, and claims nothing for it', async () => {
    const token = crypto.randomUUID();
    const late = newRunId(Date.now() - START_HOOK_CANDIDATE_MAX_AGE_MS - 60_000);
    const copies = [
      world.events.create(late, { eventType: 'run_created', eventData: runData(token) }),
      world.events.create(late, { eventType: 'run_started', eventData: runData(token) }),
    ];
    for (const copy of copies) {
      expect(await copy.catch((caught: unknown) => caught)).toMatchObject({
        status: 400, code: START_HOOK_ADMISSION_REJECTED_CODE, reason: 'expired',
      });
    }
    await expect(world.runs.get(late)).rejects.toThrow('not found');
    const admitted = newRunId();
    expect((await world.events.create(admitted, { eventType: 'run_created', eventData: runData(token) })).run.startHook)
      .toMatchObject({ token, hookId: startHookId(admitted) });
  });

  it('refuses a start Hook on a run created without one, and a candidate re-sent under another token', async () => {
    const plain = newRunId();
    await world.events.create(plain, { eventType: 'run_created', eventData: { ...runData('x'), startHook: undefined } });
    expect(await world.events.create(plain, { eventType: 'run_started', eventData: runData(crypto.randomUUID()) })
      .catch((error: unknown) => error)).toMatchObject({ reason: 'ordinary_run' });
    const admitted = newRunId();
    await world.events.create(admitted, { eventType: 'run_created', eventData: runData(crypto.randomUUID()) });
    expect(await world.events.create(admitted, { eventType: 'run_created', eventData: runData(crypto.randomUUID()) })
      .catch((error: unknown) => error)).toMatchObject({ reason: 'token_mismatch' });
  });
});
