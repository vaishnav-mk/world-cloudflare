// Runs `sequentialSteps` against a local `wrangler dev` and prints ms per step as the log grows,
// from the events' own timestamps. See docs/limits.md, "Step throughput and log size".
//
//   pnpm exec wrangler dev --port 8811 --var WORLD_RPC_TOKEN:x --var WORKFLOW_VM:node \
//     --var WORKFLOW_V2_TIMEOUT_MS:1800000
//   node scripts/perf/run-steps.mjs [steps=15000]
import { parse, stringify } from 'devalue';

const base = process.env.WORKFLOW_BASE_URL ?? 'http://localhost:8811';
const token = process.env.WORLD_RPC_TOKEN ?? 'x';
const steps = Number(process.argv[2] ?? 15_000);

async function rpc(method, args) {
  const response = await fetch(`${base}/world/rpc`, {
    method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/x-devalue' },
    body: stringify({ method, args }),
    signal: AbortSignal.timeout(30_000),
  });
  const result = parse(await response.text());
  if (!result.ok) throw new Error(`${method}: ${result.error.message}`);
  return result.value;
}

const started = await fetch(`${base}/runs`, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ name: 'sequentialSteps', value: steps }),
});
const { runId } = await started.json();
console.log(runId);

// Local workerd evicts the queue shard about 70 s after its last incoming request, which cancels
// a flow it started from `waitUntil`. A deduplicated enqueue, delayed a day, is a cheap request.
for (;;) {
  await rpc('queue', ['__wkf_workflow_keepalive', { runId: 'wrun_keepalive' }, {
    delaySeconds: 86_400, idempotencyKey: 'keepalive',
  }]).catch(() => {});
  const run = await rpc('runs.get', [runId, { resolveData: 'none' }]).catch(() => undefined);
  if (run && run.status !== 'pending' && run.status !== 'running') {
    console.log(run.status);
    break;
  }
  await new Promise((resolve) => setTimeout(resolve, 5_000));
}

const createdAt = [];
for (let cursor; ;) {
  const page = await rpc('events.list', [{ runId, pagination: { limit: 1000, cursor }, resolveData: 'none' }]);
  for (const event of page.data) createdAt[Number(event.eventId.replace(/^evnt_/, ''))] = event.createdAt.getTime();
  if (!page.hasMore) break;
  cursor = page.cursor;
}
const last = createdAt.length - 1;
console.log(`${last} events in ${((createdAt[last] - createdAt[1]) / 1000).toFixed(1)} s`);
// Each step writes three events, so a window of 600 events is 200 steps.
for (let mark = 3_000; mark + 300 <= last; mark += 3_000) {
  const ms = createdAt[mark + 300] - createdAt[mark - 300];
  console.log(`~${mark} events: ${(ms / 200).toFixed(2)} ms/step`);
}
