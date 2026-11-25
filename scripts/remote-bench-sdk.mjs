// The Workflow SDK's benchmark (`packages/core/e2e/benchmark.test.ts` in vercel/workflow) against a
// deployed example Worker, or any app that serves the same two routes:
//   POST /runs {"name","value"} -> {"runId","clientStart"}   (clientStart: Date.now() right before start())
//   GET  /runs/<id>/result      -> {"value"}                  (waits for the run)
// Usage: WORKFLOW_BASE_URL=https://<app> node scripts/remote-bench-sdk.mjs [scenario ...]
// Every timestamp comes from the deployment (the route's clientStart, each step body's Date.now()),
// so the client's network path is outside every window, as in the SDK's harness.
//   ttfs        time to first step: steps[0].start - clientStart, one no-op step
//   stso        step-to-step overhead: steps[i].start - steps[i-1].end, sequential no-op steps
//   wo          workflow overhead: (last.end - clientStart) - time inside step bodies
//   fanout      first and last of a Promise.all over no-op steps to finish, from clientStart
//   resume      resume after idle: second step's start - first step's end - the sleep
const base = process.env.WORKFLOW_BASE_URL?.replace(/\/$/, '');
if (!base) throw new Error('WORKFLOW_BASE_URL is required');
const env = (name, fallback) => Number(process.env[name] ?? fallback);
const ITERATIONS = env('BENCH_ITERATIONS', 30);
const WARMUP = env('BENCH_WARMUP_ITERATIONS', 2);
const SEQUENTIAL_STEPS = env('BENCH_SEQUENTIAL_STEP_COUNT', 1020);
const SEQUENTIAL_ITERATIONS = env('BENCH_SEQUENTIAL_ITERATIONS', 1);
const FANOUT_STEPS = env('BENCH_FANOUT_STEP_COUNT', 100);
const FANOUT_ITERATIONS = env('BENCH_FANOUT_ITERATIONS', 10);
const RESUME_MS = env('BENCH_RESUME_MS', 1000);
const RESUME_ITERATIONS = env('BENCH_RESUME_ITERATIONS', 10);

async function run(name, value) {
  const started = await fetch(`${base}/runs`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name, value }),
  });
  if (!started.ok) throw new Error(`start ${name}: HTTP ${started.status} ${(await started.text()).slice(0, 200)}`);
  const { runId, clientStart } = await started.json();
  for (let tries = 0; ; tries++) {
    const response = await fetch(`${base}/runs/${runId}/result`, { signal: AbortSignal.timeout(300_000) });
    if (response.ok) return { runId, clientStart, ...(await response.json()).value };
    if (tries >= 5) throw new Error(`result ${runId}: HTTP ${response.status} ${(await response.text()).slice(0, 200)}`);
  }
}

function stats(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (p) => sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)];
  return { samples: sorted.length, best: sorted[0], p50: at(50), p75: at(75), p90: at(90), p99: at(99), max: sorted.at(-1) };
}

const rows = {};
const record = (metric, samples) => { rows[metric] = stats(samples); };

const scenarios = {
  async ttfs() {
    for (let i = 0; i < WARMUP; i++) await run('benchStep', 0);
    const samples = [];
    for (let i = 0; i < ITERATIONS; i++) {
      const { clientStart, steps } = await run('benchStep', 0);
      samples.push(steps[0].start - clientStart);
    }
    record('ttfs', samples);
  },
  async sequential() {
    const inline = [], hops = [], wo = [];
    for (let i = 0; i < SEQUENTIAL_ITERATIONS; i++) {
      const { clientStart, steps } = await run('benchSequentialSteps', SEQUENTIAL_STEPS);
      for (let s = 1; s < steps.length; s++) (steps[s].kind === 'inline' ? inline : hops).push(steps[s].start - steps[s - 1].end);
      wo.push(steps.at(-1).end - clientStart - steps.reduce((sum, step) => sum + step.end - step.start, 0));
    }
    record(`stso inline (${SEQUENTIAL_STEPS} steps)`, inline);
    if (hops.length) record(`stso queue-hop (${SEQUENTIAL_STEPS} steps)`, hops);
    record(`wo (${SEQUENTIAL_STEPS} steps)`, wo);
  },
  async fanout() {
    const first = [], last = [];
    for (let i = 0; i < FANOUT_ITERATIONS; i++) {
      const { clientStart, steps } = await run('benchFanOutSteps', FANOUT_STEPS);
      const ends = steps.map((step) => step.end);
      first.push(Math.min(...ends) - clientStart);
      last.push(Math.max(...ends) - clientStart);
    }
    record(`fanout ttfs (${FANOUT_STEPS} steps)`, first);
    record(`fanout ttls (${FANOUT_STEPS} steps)`, last);
  },
  async resume() {
    const samples = [];
    for (let i = 0; i < RESUME_ITERATIONS; i++) {
      const { steps, sleepMs } = await run('benchResume', RESUME_MS);
      samples.push(steps[1].start - steps[0].end - sleepMs);
    }
    record(`resume after ${RESUME_MS} ms idle`, samples);
  },
};

const chosen = process.argv.length > 2 ? process.argv.slice(2) : Object.keys(scenarios);
for (const name of chosen) {
  if (!(name in scenarios)) throw new Error(`Unknown scenario ${name}; one of ${Object.keys(scenarios).join(', ')}`);
  const startedAt = Date.now();
  await scenarios[name]();
  console.error(`${name} done in ${((Date.now() - startedAt) / 1000).toFixed(1)} s`);
}
console.table(rows);
console.log(JSON.stringify({ base, at: new Date().toISOString(), rows }));
