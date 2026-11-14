import { SELF } from 'cloudflare:test';
import { afterEach, beforeAll, expect, it } from 'vitest';

// Runs under vitest.isolate.config.ts, where WORKFLOW_VM=node. The flow handler runs in this isolate, so
// wrapping the workflow factory the build registers with vm.ts counts node-engine replays.
type Bundle = { names: readonly string[]; evaluate: (...args: unknown[]) => void };
let replays = 0;
beforeAll(() => {
  // SAFETY: vm.ts keeps a Map of bundles under this key, and adopts this one if it loads later.
  const bundles = (globalThis as unknown as Record<symbol, Map<string, Bundle>>)[Symbol.for('@workflow/world-cloudflare//workflowBundles')] ??= new Map();
  const set = bundles.set.bind(bundles);
  bundles.set = (code, bundle) => set(code, { ...bundle, evaluate: (...args) => { replays++; bundle.evaluate(...args); } });
  for (const [code, bundle] of bundles) bundles.set(code, bundle);
});

afterEach(() => { process.env.WORKFLOW_VM = 'node'; });

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

it('replays sequential steps on the node engine', async () => {
  const before = replays;
  expect(await run('sequentialSteps', 50)).toEqual({ engine: 'node', value: 50 });
  expect(replays).toBeGreaterThan(before);
});

it('keeps a run started on QuickJS on QuickJS', async () => {
  process.env.WORKFLOW_VM = 'quickjs';
  const started = await SELF.fetch('https://local/runs', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name: 'demo', value: 4 }),
  });
  process.env.WORKFLOW_VM = 'node';
  const { runId } = await started.json() as { runId: string };
  const before = replays;
  await expect.poll(async () => (await (await SELF.fetch(`https://local/runs/${runId}`)).json() as { status: string }).status,
    { interval: 100, timeout: 15_000 }).toBe('completed');
  const status = await (await SELF.fetch(`https://local/runs/${runId}`)).json() as { executionContext: { workflowVm?: string } };
  expect(status.executionContext.workflowVm).toBe('quickjs');
  expect(await (await SELF.fetch(`https://local/runs/${runId}/result`)).json()).toEqual({ value: 8 });
  expect(replays).toBe(before);
});
