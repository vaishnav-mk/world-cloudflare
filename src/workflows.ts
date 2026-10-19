import { approval, delayed, demo, failPermanently, longDelay, progress, retryOnce } from '../workflows/demo.js';
import {
  cpuStep, hookPayload, parallelSteps, runOutput, sequentialSteps, sleepHours, stepInput, stepOutput, streamChunk,
} from '../workflows/limits.js';

/** Workflows `POST /runs` may start, by request name. IDs are the ones `workflow build` generates. */
export const workflows = {
  demo: Object.assign(demo, { workflowId: 'workflow//./workflows/demo//demo' }),
  delayed: Object.assign(delayed, { workflowId: 'workflow//./workflows/demo//delayed' }),
  longDelay: Object.assign(longDelay, { workflowId: 'workflow//./workflows/demo//longDelay' }),
  approval: Object.assign(approval, { workflowId: 'workflow//./workflows/demo//approval' }),
  progress: Object.assign(progress, { workflowId: 'workflow//./workflows/demo//progress' }),
  retry: Object.assign(retryOnce, { workflowId: 'workflow//./workflows/demo//retryOnce' }),
  fatal: Object.assign(failPermanently, { workflowId: 'workflow//./workflows/demo//failPermanently' }),
  stepOutput: Object.assign(stepOutput, { workflowId: 'workflow//./workflows/limits//stepOutput' }),
  stepInput: Object.assign(stepInput, { workflowId: 'workflow//./workflows/limits//stepInput' }),
  runOutput: Object.assign(runOutput, { workflowId: 'workflow//./workflows/limits//runOutput' }),
  streamChunk: Object.assign(streamChunk, { workflowId: 'workflow//./workflows/limits//streamChunk' }),
  sequentialSteps: Object.assign(sequentialSteps, { workflowId: 'workflow//./workflows/limits//sequentialSteps' }),
  parallelSteps: Object.assign(parallelSteps, { workflowId: 'workflow//./workflows/limits//parallelSteps' }),
  sleepHours: Object.assign(sleepHours, { workflowId: 'workflow//./workflows/limits//sleepHours' }),
  cpuStep: Object.assign(cpuStep, { workflowId: 'workflow//./workflows/limits//cpuStep' }),
  hookPayload: Object.assign(hookPayload, { workflowId: 'workflow//./workflows/limits//hookPayload' }),
};

export type WorkflowName = keyof typeof workflows;

export const DEFAULT_WORKFLOW: WorkflowName = 'demo';

export function isWorkflowName(name: unknown): name is WorkflowName {
  return typeof name === 'string' && Object.hasOwn(workflows, name);
}
