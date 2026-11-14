import * as demo from '../workflows/demo.js';
import * as limits from '../workflows/limits.js';
import * as stress from '../workflows/stress.js';
import * as worldTesting from '../workflows/world-testing.js';

/** Gives each exported workflow the ID `workflow build` generates for it, so `start()` can find it. */
function withIds<M extends Record<string, object>>(file: string, module: M): { [K in keyof M]: M[K] & { workflowId: string } } {
  for (const [name, workflow] of Object.entries(module)) {
    Object.assign(workflow, { workflowId: `workflow//./workflows/${file}//${name}` });
  }
  // SAFETY: the loop above gave every export its `workflowId`.
  return module as { [K in keyof M]: M[K] & { workflowId: string } };
}

/** Workflows `POST /runs` may start, by function name. */
export const workflows = { ...withIds('demo', demo), ...withIds('limits', limits), ...withIds('world-testing', worldTesting) };

/** The engine stress workflows, for tests only: `POST /runs` cannot start them. */
export const stressWorkflows = withIds('stress', stress);

type WorkflowName = keyof typeof workflows;

export const DEFAULT_WORKFLOW: WorkflowName = 'demo';

export function isWorkflowName(name: unknown): name is WorkflowName {
  return typeof name === 'string' && Object.hasOwn(workflows, name);
}
