// Measures the World's limits against a deployed example Worker.
//   WORKFLOW_BASE_URL=https://<worker>.workers.dev node scripts/remote-limits.mjs [probe:value ...]
// With no arguments it runs the default matrix below. Each probe starts one run of a
// `workflows/limits.ts` workflow and reports how it ended.
import { setTimeout as sleep } from 'node:timers/promises';

const base = process.env.WORKFLOW_BASE_URL?.replace(/\/$/, '');
if (!base) throw new Error('WORKFLOW_BASE_URL is required');

const KiB = 1024;
const MiB = 1024 * KiB;
const TERMINAL = new Set(['completed', 'failed', 'cancelled']);

const DEFAULT_MATRIX = [
  ...[256 * KiB, 512 * KiB, MiB, 1.25 * MiB, 1.5 * MiB, 2 * MiB, 4 * MiB].map((bytes) => ['stepOutput', bytes]),
  ...[MiB, 1.5 * MiB, 2 * MiB].map((bytes) => ['stepInput', bytes]),
  ...[512 * KiB, MiB, 1.5 * MiB, 2 * MiB].map((bytes) => ['runOutput', bytes]),
  ...[512 * KiB, MiB, 1.9 * MiB, 2 * MiB, 4 * MiB, 8 * MiB].map((bytes) => ['streamChunk', bytes]),
  ...[100, 500, 1000].map((count) => ['sequentialSteps', count]),
  ...[50, 200, 500].map((count) => ['parallelSteps', count]),
  ...[100, 1000, 3000].map((mega) => ['cpuStep', mega]),
  ...[512 * KiB, 1000 * KiB, 1100 * KiB].map((bytes) => ['hookPayload', bytes]),
];

const matrix = process.argv.length > 2
  ? process.argv.slice(2).map((arg) => { const [name, value] = arg.split(':'); return [name, Number(value)]; })
  : DEFAULT_MATRIX;

async function call(path, init = {}) {
  const response = await fetch(`${base}${path}`, { ...init, signal: AbortSignal.timeout(60_000) });
  const text = await response.text();
  return { status: response.status, text };
}

async function start(name, value) {
  const { status, text } = await call('/runs', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name, value }),
  });
  if (status !== 202) throw new Error(`start ${name}: HTTP ${status} ${text.slice(0, 120)}`);
  return JSON.parse(text).runId;
}

async function waitFor(runId, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let run;
  while (Date.now() < deadline) {
    run = JSON.parse((await call(`/runs/${runId}`)).text);
    if (TERMINAL.has(run.status)) return run;
    await sleep(1_000);
  }
  return run;
}

async function eventCount(runId) {
  const { text } = await call(`/runs/${runId}/events`);
  return text.split('\n').filter((line) => line.startsWith('event: ')).length;
}

/** Resumes a `hookPayload` run with a JSON string of `bytes` characters. */
async function resumeWithPayload(runId, bytes) {
  for (let attempt = 0; attempt < 30; attempt++) {
    const hooks = JSON.parse((await call(`/runs/${runId}/hooks`)).text).data;
    if (hooks.length) {
      const body = JSON.stringify('x'.repeat(Math.max(0, bytes - 2)));
      return call(`/hooks/${encodeURIComponent(hooks[0].token)}`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, body,
      });
    }
    await sleep(1_000);
  }
  throw new Error('hook never registered');
}

/** Run errors arrive as base64 serialized bytes (`devl…`); pull out the error messages they contain. */
function describe(error) {
  if (!error) return '';
  if (error instanceof Error) return error.message.slice(0, 200);
  const text = typeof error === 'object' && '$bytes' in error
    ? Buffer.from(error.$bytes, 'base64').toString('utf8')
    : String(error.message ?? error);
  const messages = [...text.matchAll(/(?:Error|Exception)[:"]\s*([^"\\]{8,160})/g)].map((match) => match[1].trim());
  return [...new Set(messages)].slice(0, 3).join(' | ') || text.replace(/\s+/g, ' ').slice(0, 200);
}

const timeouts = { cpuStep: 300_000, sequentialSteps: 600_000, parallelSteps: 300_000 };
const results = [];
for (const [name, value] of matrix) {
  const startedAt = Date.now();
  const row = { probe: name, value };
  try {
    const runId = await start(name, value);
    if (name === 'hookPayload') {
      const resumed = await resumeWithPayload(runId, value);
      row.resume = resumed.status;
    }
    const run = await waitFor(runId, timeouts[name] ?? 180_000);
    row.status = run?.status ?? 'unknown';
    row.seconds = Math.round((Date.now() - startedAt) / 100) / 10;
    if (run?.status === 'failed') row.error = describe(run.error);
    if (name === 'sequentialSteps' || name === 'parallelSteps') row.events = await eventCount(runId);
    if (name === 'streamChunk' && run?.status === 'completed') {
      const response = await fetch(`${base}/runs/${runId}/streams/limits`, { signal: AbortSignal.timeout(60_000) });
      row.streamed = (await response.arrayBuffer()).byteLength;
    }
    row.runId = runId;
  } catch (error) {
    row.status = 'error';
    row.error = describe(error);
  }
  results.push(row);
  console.log(JSON.stringify(row));
}
