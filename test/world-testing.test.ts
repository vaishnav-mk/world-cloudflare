// The scenarios of the SDK's `@workflow/world-testing` suite. It drives a World inside a Node server it
// spawns, which cannot host a World bound to Durable Objects, so they run here with real SDK runs in
// workerd. The in-band fence and atomic start Hook checks run as the SDK's own suites, in
// `test/conformance.test.ts`. Left out: its flow-invocation counts, which read world-local's queue, and
// its lineage workflows, whose body-level `start()` does not load in the workerd test bundle.
import { expect, it, vi } from 'vitest';
import {
  eventIdToSlot, FIRST_EVENT_SLOT, mintedSpecVersion, SPEC_VERSION_MAX_SUPPORTED, slotToEventId,
  type WorkflowRun,
} from '@workflow/world';
import { getHookByToken, getRun, resumeHook, start } from 'workflow/api';
import { workflows } from '../src/workflows.js';
import { world } from '../src/world.js';

type Name = keyof typeof workflows;

async function run(name: Name, args: unknown[]): Promise<string> {
  // SAFETY: each test passes the arguments its workflow takes.
  const started = await start(workflows[name] as (...args: unknown[]) => Promise<unknown>, args);
  return started.runId;
}

async function finished(runId: string, status: WorkflowRun['status'] = 'completed', timeout = 25_000): Promise<WorkflowRun> {
  return vi.waitFor(async () => {
    const current = await world.runs.get(runId);
    expect(current.status).toBe(status);
    return current;
  }, { interval: 200, timeout });
}

const output = async (runId: string) => {
  await finished(runId);
  return getRun(runId).returnValue;
};
const events = async (runId: string, resolveData?: 'all' | 'skip-step-inputs') => (await world.events.list({ runId, resolveData })).data;

it('runs an addition', async () => {
  expect(await output(await run('addition', [1, 2]))).toBe(3);
});

it('numbers events by position', async () => {
  const runId = await run('addition', [1, 2]);
  const done = await finished(runId);
  expect(done.specVersion).toBeGreaterThanOrEqual(mintedSpecVersion());
  expect(done.specVersion).toBeLessThanOrEqual(SPEC_VERSION_MAX_SUPPORTED);
  const slots = (await events(runId)).map((event) => ({ eventId: event.eventId, slot: eventIdToSlot(event.eventId) }));
  expect(slots.length).toBeGreaterThan(0);
  expect(slots.map(({ slot }) => slot)).toEqual(slots.map((_, index) => FIRST_EVENT_SLOT + index));
  expect(slots.map(({ eventId }) => eventId)).toEqual(slots.map(({ slot }) => slotToEventId(slot ?? 0)));
});

it('runs twenty parallel steps, each once', async () => {
  expect(await output(await run('brokenWf', []))).toEqual({ numbers: Array.from({ length: 20 }, () => expect.any(Number)) });
}, 60_000);

it('declares the in-band fence and reports a run\'s counters', async () => {
  expect(world.capabilities?.inBandFence).toBe(true);
  const runId = await run('addition', [1, 2]);
  await finished(runId);
  const { data, snapshot } = await world.events.list({ runId });
  // Only spec 9 runs have the fence's counts; earlier runs list none.
  if (snapshot === undefined) {
    expect((await world.runs.get(runId)).specVersion).toBeLessThan(9);
    return;
  }
  expect(data.map((event) => eventIdToSlot(event.eventId))).toContain(snapshot.seq);
  expect(snapshot.seqInBand).toBeGreaterThanOrEqual(1);
  expect(snapshot.seqInBand).toBeLessThanOrEqual(snapshot.seq);
});

it.each(['completed', 'failed'] as const)('releases non-retained hook tokens when a run is %s', async (status) => {
  const token = `terminal-${status}-${crypto.randomUUID()}`;
  for (const expected of [status, 'completed'] as const) {
    const runId = await run('registerUntilTerminal', [token, expected === 'failed']);
    await finished(runId, expected);
    const log = await events(runId);
    expect(log.filter((event) => event.eventType === 'hook_created')).toHaveLength(1);
    expect(log.filter((event) => event.eventType === 'hook_disposed')).toHaveLength(0);
  }
}, 60_000);

it('collects hook payloads in order, with the hook metadata', async () => {
  const token = crypto.randomUUID();
  const customData = `my-custom-data-${token}`;
  const runId = await run('collectWithHook', [token, customData]);
  const hook = await vi.waitFor(() => getHookByToken(token), { interval: 200, timeout: 10_000 });
  expect(await hook.metadata).toEqual({ customData });
  await resumeHook(token, { data: 'first payload' });
  await resumeHook(token, { data: 'second payload' });
  await resumeHook(token, { data: 'third payload', done: true });
  expect(await output(runId)).toEqual({
    collected: [{ data: 'first payload' }, { data: 'second payload' }, { data: 'third payload', done: true }],
  });
}, 60_000);

it('supports null bytes in step results', async () => {
  expect(await output(await run('nullByteWorkflow', []))).toBe('null byte \0');
});

it('retries a retryable step after its delay and catches a fatal one', async () => {
  const result = await output(await run('retryableAndFatalErrorWorkflow', [])) as {
    gotFatalError: boolean; retryableResult: { attempt: number; duration: number; stepStartedAt: Date };
  };
  expect(result).toEqual({
    gotFatalError: true, retryableResult: { attempt: 2, duration: expect.any(Number), stepStartedAt: expect.any(Date) },
  });
}, 60_000);

it('completes sequential, streaming, sleeping and parallel workflows', async () => {
  const [sequential, streamed, slept, parallel, signalled] = await Promise.all([
    run('addTenWorkflow', [10]), run('sequentialStepsWithStreamWorkflow', []), run('sleepWorkflow', []),
    run('parallelStepsWorkflow', []), run('signalCostWorkflow', [{ count: 5, withSignal: true }]),
  ]);
  expect(await output(sequential)).toBe(20);
  expect(await output(streamed)).toBe('hello world');
  // The run's default stream holds both writes, framed by the SDK.
  expect((await world.streams.getInfo(streamed, (await world.streams.list(streamed))[0] ?? '')).done).toBe(true);
  expect(await output(slept)).toBe(3);
  expect(await output(parallel)).toBe(33);
  expect(await output(signalled)).toBe('done');
}, 60_000);

it("treats resolveData 'skip-step-inputs' as 'all' except for step inputs", async () => {
  const runId = await run('addTenWorkflow', [1]);
  await finished(runId);
  const all = await events(runId, 'all');
  const skipped = await events(runId, 'skip-step-inputs');
  expect(skipped.map(({ eventId, eventType }) => ({ eventId, eventType }))).toEqual(all.map(({ eventId, eventType }) => ({ eventId, eventType })));
  for (const [index, event] of all.entries()) {
    if (event.eventType === 'step_created' || event.eventType === 'step_started') continue;
    expect(skipped[index]).toEqual(event);
  }
});
