import { DurableObject } from 'cloudflare:workers';
import { WorkflowWorldError } from '@workflow/errors';
import { captureInvocationOutcome, unwrapInvocationOutcome } from '@workflow/errors/invocation';
import {
  getQueueTopicPrefix, resolveQueueNamespace, SPEC_VERSION_SINGLE_ORCHESTRATOR, ValidQueueName, type InvocationOutcome,
} from '@workflow/world';
import { parse, stringify } from 'devalue';
import type { Delivery, DeliveryOutcome, WorldEnv } from '../types.js';
import { createWorld } from '../world.js';

/**
 * A runner (`runnerShardFor`): where workflow code runs. A call from a Durable Object to a service
 * binding or a dispatch namespace Worker runs on the caller's thread, so queue shards hand every
 * delivery to a runner, which calls the `CONSUMER` entrypoint with it, and flows and steps use the
 * runner's CPU instead of the queue shard's. A run's workflow deliveries and invocations all reach
 * its one runner, which runs the deliveries one at a time and the invocations beside them: the
 * single active runner per run `capabilities.invoke` asks for. It keeps nothing durable: a
 * delivery cut off by a reset fails on its queue shard, which delivers it again.
 */
export class Runner extends DurableObject<WorldEnv> {
  /** Each run's latest workflow delivery in this instance; the next waits for it. */
  readonly #turns = new Map<string, Promise<DeliveryOutcome>>();
  readonly #world = createWorld(this.env);

  /**
   * Runs an invocation (`Queue.invoke`) beside the run's delivery in progress, if any, so its input
   * is written while the run waits on steps, then wakes the run, as the SDK's own hook resume does
   * when it writes the event itself. Returns the outcome, errors included, because an error loses
   * its class over RPC.
   */
  async invoke(runId: string, input: unknown, requestId: string): Promise<InvocationOutcome> {
    const receivedAt = Date.now();
    return captureInvocationOutcome(async () => {
      const run = await this.#world.runs.get(runId, { resolveData: 'none' });
      const queueName = ValidQueueName.parse(`${getQueueTopicPrefix('workflow', resolveQueueNamespace())}${run.workflowName}`);
      const encoded = stringify({ runId, invoke: true, requestId, input });
      const { status, body } = await this.env.CONSUMER.invoke({ id: requestId, attempt: 1, enqueuedAt: receivedAt, body: { queueName, encoded } });
      if (status !== 200) throw new WorkflowWorldError(`Invocation failed with ${status}: ${body}`, { status: 502 });
      const result = unwrapInvocationOutcome(parse(body));
      if (typeof result !== 'object' || result === null || !('status' in result) || result.status !== 'accepted') return result;
      // A spec 9 wake carries no key, as in the SDK; an earlier run's dedupes a retried invocation's.
      const keyed = (run.specVersion ?? 0) < SPEC_VERSION_SINGLE_ORCHESTRATOR;
      await this.#world.queue(queueName, {
        runId,
        ...(run.specVersion === undefined ? {} : { runSpecVersion: run.specVersion }),
        hookResumeTiming: { resumeRequestedAtMs: receivedAt, queuePublishRequestedAtMs: Date.now(), strategy: 'invoke' },
      }, { deploymentId: run.deploymentId, ...(keyed ? { idempotencyKey: `hook-${requestId}` } : {}) }).catch((error: unknown) => {
        // The event's outbox row wakes the run a few seconds later instead.
        console.error('Invocation wake failed', { runId, requestId, error });
      });
      return result;
    });
  }

  /** Runs deliveries side by side, such as steps, and returns each one's outcome in order. */
  consume(deliveries: Delivery[]): Promise<DeliveryOutcome[]> {
    return this.env.CONSUMER.consume(deliveries);
  }

  /**
   * Runs one of a run's workflow deliveries once the run's earlier ones in this instance settle, so
   * two replays of a run never share the isolate, even when a reset of its queue shard delivers
   * again a message this runner still runs, or the legacy `flow` shard delivers for the run too.
   * The SDK's in-process step single-flight would otherwise make one request wait on a promise
   * another settles. One instance of the object runs at a time, and a reset ends its deliveries.
   */
  replay(runId: string, delivery: Delivery): Promise<DeliveryOutcome> {
    const run = async () => {
      const [outcome] = await this.env.CONSUMER.consume([delivery]);
      if (outcome === undefined) throw new Error('Incomplete queue delivery response');
      return outcome;
    };
    const turn = (this.#turns.get(runId) ?? Promise.resolve()).then(run, run);
    this.#turns.set(runId, turn);
    const forget = () => {
      if (this.#turns.get(runId) === turn) this.#turns.delete(runId);
    };
    void turn.then(forget, forget);
    return turn;
  }
}
