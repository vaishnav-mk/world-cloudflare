// Times workflow replay on a deployed (or `wrangler dev`) example Worker.
//   WORKFLOW_BASE_URL=https://<worker>.workers.dev node scripts/remote-bench.mjs [probe:value ...]
// The engine is the Worker's WORKFLOW_VM when each run starts. Every probe runs one workflow from
// `workflows/limits.ts` and reports, from the run's event timestamps:
// - seconds: run_created to run_completed;
// - stepsSeconds (replayAfterSteps only): run_created to wait_created, the steps before the sleep;
// - replayMs (replayAfterSteps only): wait_completed to the next step_created, the time a fresh
//   invocation spends replaying the whole log before it can schedule the step after the sleep.
import { setTimeout as sleep } from 'node:timers/promises';

const base = process.env.WORKFLOW_BASE_URL?.replace(/\/$/, '');
if (!base) throw new Error('WORKFLOW_BASE_URL is required');
const TERMINAL = new Set(['completed', 'failed', 'cancelled']);

const matrix = process.argv.length > 2
  ? process.argv.slice(2).map((arg) => { const [name, value] = arg.split(':'); return [name, Number(value)]; })
  : [['sequentialSteps', 2000], ['replayAfterSteps', 2000], ['replayAfterSteps', 10000]];

async function call(path, init = {}) {
  const response = await fetch(`${base}${path}`, { ...init, signal: AbortSignal.timeout(120_000) });
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status} ${(await response.text()).slice(0, 200)}`);
  return response;
}

async function events(runId) {
  const text = await (await call(`/runs/${runId}/events`)).text();
  return text.split('\n\n').filter((block) => block.includes('event: ')).map((block) => {
    const type = block.match(/^event: (.+)$/m)[1];
    const { createdAt } = JSON.parse(block.match(/^data: (.+)$/m)[1]);
    return { type, at: Date.parse(createdAt) };
  });
}

for (const [name, value] of matrix) {
  const { runId } = await (await call('/runs', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name, value }),
  })).json();
  let run;
  for (const deadline = Date.now() + 3_600_000; Date.now() < deadline;) {
    run = await (await call(`/runs/${runId}`)).json();
    if (TERMINAL.has(run.status)) break;
    await sleep(2_000);
  }
  const log = await events(runId);
  const at = (type) => log.find((event) => event.type === type)?.at;
  const row = {
    probe: name, value, engine: run.executionContext?.workflowVm, status: run.status, events: log.length,
    seconds: (at('run_completed') - at('run_created')) / 1000,
  };
  const woke = log.findIndex((event) => event.type === 'wait_completed');
  if (woke >= 0) {
    row.stepsSeconds = (at('wait_created') - at('run_created')) / 1000;
    row.replayMs = log.slice(woke).find((event) => event.type === 'step_created').at - log[woke].at;
  }
  row.runId = runId;
  console.log(JSON.stringify(row));
}
