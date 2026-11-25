// The Workflow SDK's benchmark workflows (`workbench/example/workflows/97_bench.ts` in
// https://github.com/vercel/workflow), so `scripts/remote-bench-sdk.mjs` measures this World the
// way the SDK's own benchmark measures Vercel's. Each step records when its body ran.
import { sleep } from 'workflow';

export interface BenchStepTiming {
  start: number;
  end: number;
  /** `queue-hop` for the first step body an isolate runs, `inline` after that. */
  kind: 'inline' | 'queue-hop';
}

let hasExecutedStepInProcess = false;

async function timedNoopStep(index: number): Promise<BenchStepTiming> {
  'use step';
  const kind = hasExecutedStepInProcess ? 'inline' : 'queue-hop';
  hasExecutedStepInProcess = true;
  const start = Date.now();
  void index;
  return { start, end: Date.now(), kind };
}

/** One no-op step: time to first step. */
export async function benchStep(_value: number) {
  'use workflow';
  return { steps: [await timedNoopStep(0)] };
}

/** `count` sequential no-op steps: step-to-step overhead and workflow overhead. */
export async function benchSequentialSteps(count: number) {
  'use workflow';
  const steps: BenchStepTiming[] = [];
  for (let i = 0; i < count; i++) steps.push(await timedNoopStep(i));
  return { steps };
}

/** `count` no-op steps in one `Promise.all`: fan-out first and last step. */
export async function benchFanOutSteps(count: number) {
  'use workflow';
  return { steps: await Promise.all(Array.from({ length: count }, (_, i) => timedNoopStep(i))) };
}

/** A step, `ms` of sleep, then a step: resume after idle. */
export async function benchResume(ms: number) {
  'use workflow';
  const first = await timedNoopStep(0);
  await sleep(ms);
  return { steps: [first, await timedNoopStep(1)], sleepMs: ms };
}
