import { WorkerEntrypoint } from 'cloudflare:workers';
import { FLOW_URL } from '../constants.js';
import { withRequestContext } from '../lib/request-context.js';
import { DeliveryResult } from '../enums.js';
import type { Delivery, DeliveryOutcome, ExecutorProps, FlowHandler, WorldEnv } from '../types.js';
import { executorFor } from './routes.js';

type ConsumerEnv = WorldEnv & { EXECUTORS?: DispatchNamespace };

/**
 * The entrypoint the queue shard delivers to. With an `EXECUTORS` dispatch namespace, each message
 * runs on the retained executor for its deployment. Without one, it runs on this Worker's flow route.
 */
export function createQueueConsumer(flow: FlowHandler) {
  return class QueueConsumer extends WorkerEntrypoint<ConsumerEnv> {
    /**
     * Runs a batch of deliveries side by side (the shard hands over at most 10) and returns each
     * one's outcome, so a slow or failing flow holds up and retries only itself.
     */
    consume(deliveries: Delivery[]): Promise<DeliveryOutcome[]> {
      const startedAt = Date.now();
      return Promise.all(deliveries.map((delivery) => this.#consumeOne(delivery, startedAt)));
    }

    async #consumeOne(delivery: Delivery, startedAt: number): Promise<DeliveryOutcome> {
      try {
        const target = delivery.body.targetDeployment ?? this.env.DEPLOYMENT_ID;
        const response = await this.#runFlow(target, flowRequest(delivery, startedAt));
        if (!response.ok) {
          return { kind: DeliveryResult.Failed, error: `Flow handler returned ${response.status}: ${await response.text()}` };
        }
        const { timeoutSeconds } = await response.json() as { timeoutSeconds?: unknown };
        const wake = typeof timeoutSeconds === 'number' && Number.isFinite(timeoutSeconds) && timeoutSeconds >= 0;
        return wake ? { kind: DeliveryResult.Wake, seconds: timeoutSeconds } : { kind: DeliveryResult.Done };
      } catch (error) {
        return { kind: DeliveryResult.Failed, error: String(error) };
      }
    }

    #runFlow(deployment: string, request: Request): Promise<Response> {
      const { EXECUTORS: executors, DEPLOYMENT_ID: current } = this.env;
      if (!executors) {
        if (deployment !== current) throw new Error(`Deployment ${deployment} is unavailable`);
        return withRequestContext(this.ctx, () => flow(request));
      }
      const script = executorFor(this.env, deployment);
      if (!script) throw new Error(`Deployment ${deployment} is unavailable`);
      const props: ExecutorProps = { privateDispatch: true, targetDeployment: deployment };
      return executors.get(script, { props }).fetch(request);
    }
  };
}

function flowRequest(delivery: Delivery, startedAt: number): Request {
  return new Request(FLOW_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-vqs-queue-name': delivery.body.queueName,
      'x-vqs-message-id': delivery.id,
      'x-vqs-message-attempt': String(delivery.attempt),
      // A shard from before delivery counts sends none; the handler then reports no count.
      ...(delivery.deliveryCount === undefined ? {} : { 'x-vqs-delivery-count': String(delivery.deliveryCount) }),
      'x-vqs-created-at': String(delivery.enqueuedAt),
      'x-workflow-invocation-started-at': String(startedAt),
    },
    body: delivery.body.encoded,
  });
}
