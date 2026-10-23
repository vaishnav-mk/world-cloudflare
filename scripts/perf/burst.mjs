// Starts a burst of runs against a local `wrangler dev` and prints creation throughput, POST
// latency, and how long until every run completed.
//
//   pnpm exec wrangler dev --port 8851 --inspector-port 9851
//   node scripts/perf/burst.mjs [runs=300] [concurrency=30]
const base = process.env.WORKFLOW_BASE_URL ?? 'http://localhost:8851';
const runs = Number(process.argv[2] ?? 300);
const concurrency = Number(process.argv[3] ?? 30);

const startedAt = performance.now();
const latencies = [];
const ids = [];
let next = 0;
await Promise.all(Array.from({ length: concurrency }, async () => {
  while (next < runs) {
    const value = next++;
    const sent = performance.now();
    const response = await fetch(`${base}/runs`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ value }),
      signal: AbortSignal.timeout(120_000),
    });
    if (response.status !== 202) throw new Error(`POST /runs: ${response.status} ${await response.text()}`);
    ids.push((await response.json()).runId);
    latencies.push(performance.now() - sent);
  }
}));
const createdMs = performance.now() - startedAt;

const pending = new Set(ids);
const deadline = performance.now() + 300_000;
while (pending.size && performance.now() < deadline) {
  for (const id of [...pending].slice(0, 50)) {
    const run = await (await fetch(`${base}/runs/${id}`)).json();
    if (run.status === 'completed') pending.delete(id);
    else if (run.status === 'failed' || run.status === 'cancelled') throw new Error(`${id}: ${run.status}`);
  }
  if (pending.size) await new Promise((resolve) => setTimeout(resolve, 250));
}

latencies.sort((a, b) => a - b);
const at = (fraction) => Math.round(latencies[Math.min(latencies.length - 1, Math.floor(latencies.length * fraction))]);
console.log(JSON.stringify({
  runs, concurrency,
  createdPerSecond: Math.round(runs / (createdMs / 1000)),
  postP50Ms: at(0.5), postP95Ms: at(0.95), postMaxMs: at(1),
  allCompletedMs: pending.size ? null : Math.round(performance.now() - startedAt),
  stillPending: pending.size,
}));
