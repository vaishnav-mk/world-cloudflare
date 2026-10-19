import { WorkerEntrypoint } from 'cloudflare:workers';
import { FLOW_PATH } from './constants.js';
import { withRequestContext } from './lib/request-context.js';
import type { ExecutorProps, FlowHandler, WorldEnv } from './types.js';

/** Default export for an immutable executor uploaded to the `EXECUTORS` dispatch namespace. */
export function createExecutor(flow: FlowHandler) {
  return class Executor extends WorkerEntrypoint<WorldEnv, ExecutorProps> {
    fetch(request: Request): Promise<Response> | Response {
      if (request.method !== 'POST' || new URL(request.url).pathname !== FLOW_PATH) {
        return new Response('Not found', { status: 404 });
      }
      // Only the dispatching QueueConsumer sets these props; public requests cannot supply them.
      if (this.ctx.props?.privateDispatch !== true) return new Response('Forbidden', { status: 403 });
      if (this.ctx.props.targetDeployment !== this.env.DEPLOYMENT_ID) {
        return new Response('Deployment mismatch', { status: 409 });
      }
      return withRequestContext(this.ctx, () => flow(request));
    }
  };
}
