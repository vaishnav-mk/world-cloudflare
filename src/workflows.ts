import { approval, delayed, demo, failPermanently, longDelay, progress, retryOnce } from '../workflows/demo.js';

/** Workflows `POST /runs` may start, by request name. IDs are the ones `workflow build` generates. */
export const workflows = {
  demo: Object.assign(demo, { workflowId: 'workflow//./workflows/demo//demo' }),
  delayed: Object.assign(delayed, { workflowId: 'workflow//./workflows/demo//delayed' }),
  longDelay: Object.assign(longDelay, { workflowId: 'workflow//./workflows/demo//longDelay' }),
  approval: Object.assign(approval, { workflowId: 'workflow//./workflows/demo//approval' }),
  progress: Object.assign(progress, { workflowId: 'workflow//./workflows/demo//progress' }),
  retry: Object.assign(retryOnce, { workflowId: 'workflow//./workflows/demo//retryOnce' }),
  fatal: Object.assign(failPermanently, { workflowId: 'workflow//./workflows/demo//failPermanently' }),
};

export type WorkflowName = keyof typeof workflows;

export const DEFAULT_WORKFLOW: WorkflowName = 'demo';

export function isWorkflowName(name: unknown): name is WorkflowName {
  return typeof name === 'string' && Object.hasOwn(workflows, name);
}
