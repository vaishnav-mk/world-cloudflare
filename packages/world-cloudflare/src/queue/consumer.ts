import { WorkerEntrypoint } from 'cloudflare:workers';
import { FLOW_URL } from '../constants.js';
import type { Delivery, ExecutorProps, FlowHandler, WorldEnv } from '../types.js';
import { executorFor } from './routes.js';

type ConsumerEnv = WorldEnv & { EXECUTORS?: DispatchNamespace };

/**
 * The entrypoint the queue shard delivers to. With an `EXECUTORS` dispatch namespace, each message
 * runs on the retained executor for its deployment. Without one, it runs on this Worker's flow route.
 */
export function createQueueConsumer(flow: FlowHandler) {
  return class QueueConsumer extends WorkerEntrypoint<ConsumerEnv> {
    /** Returns, per delivery, the SDK's requested re-delivery delay in seconds, or null. */
    async consume(deliveries: Delivery[]): Promise<(number | null)[]> {
      const timeouts: (number | null)[] = [];
      for (const delivery of deliveries) {
        const target = delivery.body.targetDeployment ?? this.env.DEPLOYMENT_ID;
        const response = await this.#runFlow(target, flowRequest(delivery));
        if (!response.ok) throw new Error(`Flow handler returned ${response.status}: ${await response.text()}`);
        const { timeoutSeconds } = await response.json() as { timeoutSeconds?: unknown };
        const valid = typeof timeoutSeconds === 'number' && Number.isFinite(timeoutSeconds) && timeoutSeconds >= 0;
        timeouts.push(valid ? timeoutSeconds : null);
      }
      return timeouts;
    }

    #runFlow(deployment: string, request: Request): Promise<Response> {
      const { EXECUTORS: executors, DEPLOYMENT_ID: current } = this.env;
      if (!executors) {
        if (deployment !== current) throw new Error(`Deployment ${deployment} is unavailable`);
        return flow(request);
      }
      const script = executorFor(this.env, deployment);
      if (!script) throw new Error(`Deployment ${deployment} is unavailable`);
      const props: ExecutorProps = { privateDispatch: true, targetDeployment: deployment };
      return executors.get(script, { props }).fetch(request);
    }
  };
}

function flowRequest(delivery: Delivery): Request {
  return new Request(FLOW_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-vqs-queue-name': delivery.body.queueName,
      'x-vqs-message-id': delivery.id,
      'x-vqs-message-attempt': String(delivery.attempt),
      'x-vqs-created-at': String(delivery.enqueuedAt),
    },
    body: delivery.body.encoded,
  });
}
