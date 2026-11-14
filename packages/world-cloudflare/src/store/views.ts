import type { Hook, Step, StepWithoutData, WorkflowRun, WorkflowRunWithoutData } from '@workflow/world';
import { ResolveData } from '../enums.js';
import type { ResolveDataMode } from '../types.js';

/** `resolveData: 'none'` drops payloads from entities. */
export function runView(run: WorkflowRun, resolveData: ResolveData.None): WorkflowRunWithoutData;
export function runView(run: WorkflowRun, resolveData?: ResolveDataMode): WorkflowRun | WorkflowRunWithoutData;
export function runView(run: WorkflowRun, resolveData?: ResolveDataMode): WorkflowRun | WorkflowRunWithoutData {
  if (resolveData !== ResolveData.None) return run;
  const { dynamicWorkflowCode: _code, ...rest } = run;
  return { ...rest, input: undefined, output: undefined };
}

export function stepView(step: Step, resolveData?: ResolveDataMode): Step | StepWithoutData {
  return resolveData === ResolveData.None ? { ...step, input: undefined, output: undefined } : step;
}

export function hookView(hook: Hook, resolveData?: ResolveDataMode): Hook {
  if (resolveData !== ResolveData.None) return hook;
  const { metadata: _metadata, ...rest } = hook;
  return rest;
}
