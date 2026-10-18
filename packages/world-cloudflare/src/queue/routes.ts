import { EXECUTOR_SCRIPT_PATTERN } from '../constants.js';
import type { WorldEnv } from '../types.js';

/** The retained executor script for a deployment, from the `EXECUTOR_ROUTES` JSON map. */
export function executorFor(env: Pick<WorldEnv, 'EXECUTOR_ROUTES'>, deploymentId: string): string | undefined {
  if (!env.EXECUTOR_ROUTES) return undefined;
  const routes: unknown = JSON.parse(env.EXECUTOR_ROUTES);
  if (!routes || typeof routes !== 'object' || Array.isArray(routes)) throw new Error('Invalid executor registry');
  const script = Object.hasOwn(routes, deploymentId) ? (routes as Record<string, unknown>)[deploymentId] : undefined;
  return typeof script === 'string' && EXECUTOR_SCRIPT_PATTERN.test(script) ? script : undefined;
}
