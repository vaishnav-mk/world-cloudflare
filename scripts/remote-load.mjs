// Load test against a deployed (or `wrangler dev`) example Worker. POSTs /runs straight from this machine.
//   WORKFLOW_BASE_URL=https://<worker>.<subdomain>.workers.dev node scripts/remote-load.mjs [count] [concurrency] [--wait] [--verify] [--name=<workflow>]
// Starts `count` runs (default 50), `concurrency` requests at a time (default 10), of `demo` or the
// workflow `--name` names, and prints one JSON line: creation throughput, latency percentiles and
// errors by status. `--wait` then polls every run until it finishes and adds end-to-end throughput.
// `--verify` also checks each completed run's result, for `demo`, which returns its value doubled;
// it implies `--wait`.

const base = process.env.WORKFLOW_BASE_URL?.replace(/\/$/, '');
const flags = process.argv.slice(2).filter((arg) => arg.startsWith('--'));
const [count = 50, concurrency = 10] = process.argv.slice(2).filter((arg) => !arg.startsWith('--')).map(Number);
const verify = flags.includes('--verify');
const wait = verify || flags.includes('--wait');
const name = flags.find((flag) => flag.startsWith('--name='))?.slice('--name='.length);
if (!base || !Number.isSafeInteger(count) || count < 1 || !Number.isSafeInteger(concurrency) || concurrency < 1) {
  throw new Error('Set WORKFLOW_BASE_URL; count and concurrency are positive integers');
}

const TERMINAL = new Set(['completed', 'failed', 'cancelled']);
const WAIT_LIMIT_SECONDS = 1_800;
const POLL_CONCURRENCY = 100;

const startedAt = performance.now();
const seconds = () => (performance.now() - startedAt) / 1000;
const percentile = (sorted, p) => Math.round(sorted[Math.floor(sorted.length * p)] ?? 0);

/** Runs `work` over `items`, `limit` at a time. */
async function pool(items, limit, work) {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (let index = next++; index < items.length; index = next++) await work(items[index]);
  }));
}

// Create.
const latencies = [];
const errors = {};
/** Run ID to the value it was started with. */
const runs = new Map();
await pool(Array.from({ length: count }, (_, index) => index + 1), concurrency, async (value) => {
  const sent = performance.now();
  try {
    const response = await fetch(`${base}/runs`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(name ? { name, value } : { value }),
    });
    const body = await response.text();
    if (response.status !== 202) {
      errors[response.status] = (errors[response.status] ?? 0) + 1;
      return;
    }
    latencies.push(performance.now() - sent);
    runs.set(JSON.parse(body).runId, value);
  } catch (error) {
    const key = error.cause?.code ?? error.name;
    errors[key] = (errors[key] ?? 0) + 1;
  }
});
const createSeconds = seconds();
latencies.sort((a, b) => a - b);
const report = {
  count, concurrency, created: runs.size, createSeconds: +createSeconds.toFixed(1),
  createdPerSecond: +(runs.size / createSeconds).toFixed(1),
  p50Ms: percentile(latencies, 0.5), p95Ms: percentile(latencies, 0.95), p99Ms: percentile(latencies, 0.99), errors,
};

// Wait.
if (wait) {
  /** Run ID to when it finished, in seconds since the start, and how. */
  const finished = new Map();
  let pending = [...runs.keys()];
  while (pending.length > 0 && seconds() < WAIT_LIMIT_SECONDS) {
    const still = [];
    await pool(pending, POLL_CONCURRENCY, async (runId) => {
      try {
        const run = await (await fetch(`${base}/runs/${runId}`)).json();
        if (TERMINAL.has(run.status)) finished.set(runId, { at: seconds(), status: run.status });
        else still.push(runId);
      } catch {
        still.push(runId);
      }
    });
    pending = still;
    if (pending.length > 0) await new Promise((resolve) => setTimeout(resolve, 2_000));
  }
  const times = [...finished.values()].map(({ at }) => at).sort((a, b) => a - b);
  const statuses = {};
  for (const { status } of finished.values()) statuses[status] = (statuses[status] ?? 0) + 1;
  const last = times.at(-1) ?? 0;
  Object.assign(report, {
    finished: finished.size, unfinished: pending.length, statuses,
    finishedP50Seconds: +(times[Math.floor(times.length * 0.5)] ?? 0).toFixed(1), allFinishedSeconds: +last.toFixed(1),
    endToEndPerSecond: last > 0 ? +(finished.size / last).toFixed(1) : 0,
  });

  // Verify.
  if (verify) {
    const completed = [...finished].filter(([, { status }]) => status === 'completed').map(([runId]) => runId);
    const wrong = [];
    await pool(completed, POLL_CONCURRENCY, async (runId) => {
      const expected = runs.get(runId) * 2;
      try {
        const { value } = await (await fetch(`${base}/runs/${runId}/result`)).json();
        if (value !== expected) wrong.push({ runId, expected, got: value });
      } catch (error) {
        wrong.push({ runId, error: String(error) });
      }
    });
    Object.assign(report, { verified: completed.length, wrong: wrong.length, wrongSample: wrong.slice(0, 3) });
  }
}

console.log(JSON.stringify(report));
