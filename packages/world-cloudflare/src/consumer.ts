import { WorkerEntrypoint } from 'cloudflare:workers';
import { executorFor, type Delivery } from './queue.js';
import type { FlowHandler, WorldEnv } from './world.js';

type ConsumerEnv = WorldEnv & { EXECUTORS?: DispatchNamespace };

/**
 * The entrypoint the queue shard delivers to. With an `EXECUTORS` dispatch namespace, each message
 * goes to the retained executor for its deployment; without one, to this Worker's own flow route.
 */
export function createQueueConsumer(flow: FlowHandler) {
  return class QueueConsumer extends WorkerEntrypoint<ConsumerEnv> {
    async consume(deliveries: Delivery[]): Promise<(number | null)[]> {
      const results: (number | null)[] = [];
      for (const delivery of deliveries) {
        const target = delivery.body.targetDeployment ?? this.env.DEPLOYMENT_ID;
        const script = executorFor(this.env, target);
        if (this.env.EXECUTORS && !script) throw new Error(`Deployment ${target} is unavailable`);
        if (!this.env.EXECUTORS && target !== this.env.DEPLOYMENT_ID) {
          throw new Error(`Deployment ${target} is unavailable`);
        }
        const request = new Request('https://internal/.well-known/workflow/v1/flow', {
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
        const response = this.env.EXECUTORS
          ? await this.env.EXECUTORS.get(script!, { props: { privateDispatch: true, targetDeployment: target } }).fetch(request)
          : await flow(request);
        if (!response.ok) throw new Error(`Flow handler returned ${response.status}: ${await response.text()}`);
        const body = await response.json() as { timeoutSeconds?: number };
        const timeout = body.timeoutSeconds;
        results.push(typeof timeout === 'number' && Number.isFinite(timeout) && timeout >= 0 ? timeout : null);
      }
      return results;
    }
  };
}
