import { DurableObject } from 'cloudflare:workers';
import type { Delivery, DeliveryOutcome, WorldEnv } from '../types.js';

/**
 * A runner (`runnerShardFor`): where workflow code runs. A call from a Durable Object to a service
 * binding or a dispatch namespace Worker runs on the caller's thread, so queue shards hand every
 * delivery to a runner, which calls the `CONSUMER` entrypoint with it, and flows and steps use the
 * runner's CPU instead of the queue shard's. A run's workflow deliveries all reach its one runner,
 * which runs them one at a time. It keeps nothing durable: a delivery cut off by a reset fails on
 * its queue shard, which delivers it again.
 */
export class Runner extends DurableObject<WorldEnv> {
  /** Each run's latest workflow delivery in this instance; the next waits for it. */
  readonly #turns = new Map<string, Promise<DeliveryOutcome>>();

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
