// Measures the live memory of the Worker's isolate under `wrangler dev` during one probe run.
//
//   pnpm run build:workflow
//   node scripts/perf/memory.mjs <quickjs|node> <workflow> <value>[x<value>...]
//
// Starts `wrangler dev` on fresh local state with V8's GC trace, runs the workflow from
// `workflows/limits.ts` to its end, and prints the most heap plus external memory (ArrayBuffers and
// what the runtime reports) that a garbage collection left in any isolate. That is memory still in
// use, which the 128 MB isolate limit counts; between collections the total runs higher.
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';

const [vm, name, value] = process.argv.slice(2);
if (!vm || !name || !value) throw new Error('Usage: memory.mjs <quickjs|node> <workflow> <value>[x<value>...]');
const port = 8811;
const base = `http://localhost:${port}`;

rmSync('.wrangler/state', { recursive: true, force: true });
const dev = spawn('pnpm', ['exec', 'wrangler', 'dev', '--port', String(port), '--var', `WORKFLOW_VM:${vm}`], {
  env: { ...process.env, MINIFLARE_WORKERD_V8_FLAGS: '--trace-gc-verbose' },
  stdio: ['ignore', 'pipe', 'pipe'],
  detached: true,
});

// Per isolate: the heap the last collection left, and the most heap plus external memory seen.
const heap = new Map();
const peak = new Map();
let partial = '';
const read = (data) => {
  const lines = (partial + data).split('\n');
  partial = lines.pop();
  for (const line of lines) {
    const match = /^\[\d+:(0x[0-9a-f]+)\] (?:All spaces,\s+used:\s+(\d+) KB|External memory reported:\s+(\d+) KB)/.exec(line);
    if (!match) continue;
    const [, isolate, used, external] = match;
    if (used !== undefined) heap.set(isolate, Number(used));
    else if (heap.has(isolate)) peak.set(isolate, Math.max(peak.get(isolate) ?? 0, heap.get(isolate) + Number(external)));
  }
};
dev.stdout.on('data', read);
dev.stderr.on('data', read);

try {
  for (let ready = false; !ready; await sleep(500)) ready = await fetch(`${base}/runs/x`).then(() => true, () => false);
  const args = value.split('x').map(Number);
  const started = await fetch(`${base}/runs`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name, value: args.length === 1 ? args[0] : args }),
  });
  const { runId } = await started.json();
  let run;
  for (;; await sleep(500)) {
    run = await (await fetch(`${base}/runs/${runId}`)).json();
    if (['completed', 'failed', 'cancelled'].includes(run.status)) break;
  }
  const result = run.status === 'completed' ? (await (await fetch(`${base}/runs/${runId}/result`)).json()).value : undefined;
  console.log(JSON.stringify({ vm, name, value, status: run.status, result, liveMiB: +(Math.max(...peak.values()) / 1024).toFixed(1) }));
} finally {
  process.kill(-dev.pid);
}
