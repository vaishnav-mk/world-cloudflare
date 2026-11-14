import { FRESH_RUNNER_ARM_INTERVAL_MS } from '../constants.js';
import { objectOptions } from '../lib/shards.js';
import type { WorldEnv } from '../types.js';
import type { RunnerStubs } from './deliver.js';

type Stub = ReturnType<WorldEnv['RUNNER']['getByName']>;

/**
 * A queue shard's runner stubs, each first called from the shard's alarm. A stub's calls spend the
 * subrequest depth of the request that made its first call, and an alarm starts a new chain, so a
 * delivery through one starts a new chain too, however deep the enqueue behind it was. Without
 * them, a flow enqueues through a store into a queue shard that delivers to a runner that runs a
 * flow, and so on, each cycle spending about five of the platform's few levels. Until the alarm
 * has made a runner's stub, deliveries use a plain one.
 */
export class FreshRunnerStubs implements RunnerStubs {
  readonly #ctx: DurableObjectState;
  readonly #runners: WorldEnv['RUNNER'];
  readonly #fresh = new Map<string, Stub>();
  /** Runners a delivery asked for before there was a fresh stub to them. */
  readonly #wanted = new Set<string>();
  #armedAt = 0;

  constructor(ctx: DurableObjectState, runners: WorldEnv['RUNNER']) {
    this.#ctx = ctx;
    this.#runners = runners;
  }

  get(name: string): Stub {
    const fresh = this.#fresh.get(name);
    if (fresh !== undefined) return fresh;
    this.#wanted.add(name);
    if (Date.now() - this.#armedAt >= FRESH_RUNNER_ARM_INTERVAL_MS) {
      this.#armedAt = Date.now();
      this.#ctx.waitUntil(this.#armSoon());
    }
    return this.#runners.getByName(name, objectOptions());
  }

  forget(name: string): void {
    this.#fresh.delete(name);
  }

  /** Makes the stubs deliveries asked for, first calling each from here. Run from the alarm. */
  async freshen(): Promise<void> {
    await Promise.all([...this.#wanted].map(async (name) => {
      const stub = this.#runners.getByName(name, objectOptions());
      try {
        await stub.wake();
        this.#fresh.set(name, stub);
        this.#wanted.delete(name);
      } catch (error) {
        console.error('Could not reach a runner from the alarm', { runner: name, error });
      }
    }));
  }

  /** Brings the alarm forward, so it makes the wanted stubs. The scheduler may move it later, which only delays them. */
  async #armSoon(): Promise<void> {
    try {
      const at = await this.#ctx.storage.getAlarm();
      if (at === null || at > Date.now()) await this.#ctx.storage.setAlarm(Date.now());
    } catch (error) {
      console.error('Could not arm the alarm for fresh runner stubs', error);
    }
  }
}
