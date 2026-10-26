import { SELF } from 'cloudflare:test';
import { afterEach, beforeAll, expect, it } from 'vitest';

// Runs under vitest.isolate.config.ts, where WORKFLOW_VM=isolate. The flow handler runs in this
// isolate, so wrapping the build's workflow factory counts isolate-engine replays.
let replays = 0;
beforeAll(async () => {
  await SELF.fetch('https://local/runs/not-real'); // loads the Worker, which registers the factory
  type Bundle = { evaluate: (...args: unknown[]) => void };
  const bundle = (globalThis as unknown as Record<symbol, Bundle>)[Symbol.for('@workflow/core//isolateWorkflowBundle')];
  const { evaluate } = bundle;
  bundle.evaluate = (...args) => { replays++; evaluate(...args); };
});

afterEach(() => { process.env.WORKFLOW_VM = 'isolate'; });

async function run(name: string, value: number) {
  const started = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name, value }),
  });
  expect(started.status).toBe(202);
  const { runId } = await started.json() as { runId: string };
  await expect.poll(async () => (await (await SELF.fetch(`https://local/runs/${runId}`)).json() as { status: string }).status,
    { interval: 100, timeout: 25_000 }).toBe('completed');
  const status = await (await SELF.fetch(`https://local/runs/${runId}`)).json() as { executionContext: { workflowVm?: string } };
  const result = await (await SELF.fetch(`https://local/runs/${runId}/result`)).json() as { value: unknown };
  return { engine: status.executionContext.workflowVm, value: result.value };
}

it('replays sequential steps on the isolate engine', async () => {
  const before = replays;
  expect(await run('sequentialSteps', 50)).toEqual({ engine: 'isolate', value: 50 });
  expect(replays).toBeGreaterThan(before);
});

it('keeps a run started on QuickJS on QuickJS', async () => {
  process.env.WORKFLOW_VM = 'quickjs';
  const started = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name: 'demo', value: 4 }),
  });
  process.env.WORKFLOW_VM = 'isolate';
  const { runId } = await started.json() as { runId: string };
  const before = replays;
  await expect.poll(async () => (await (await SELF.fetch(`https://local/runs/${runId}`)).json() as { status: string }).status,
    { interval: 100, timeout: 15_000 }).toBe('completed');
  const status = await (await SELF.fetch(`https://local/runs/${runId}`)).json() as { executionContext: { workflowVm?: string } };
  expect(status.executionContext.workflowVm).toBe('quickjs');
  expect(await (await SELF.fetch(`https://local/runs/${runId}/result`)).json()).toEqual({ value: 8 });
  expect(replays).toBe(before);
});
