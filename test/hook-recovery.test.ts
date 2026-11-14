import { env } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { createWorld } from '@workflow/world-cloudflare';

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

describe('durable hook ownership', () => {
  it('registers a free forced token and conflicts with an ordinary claim', async () => {
    const owner = await newRun();
    const contender = await newRun();
    const token = crypto.randomUUID();
    const result = await world.events.create(owner, hookCreated(token, true));
    expect(await world.hooks.getByToken(token)).toMatchObject({ runId: owner, hookId: result.hook?.hookId });
    expect((await world.events.create(contender, hookCreated(token))).event).toMatchObject({ eventType: 'hook_conflict' });
  });
});
