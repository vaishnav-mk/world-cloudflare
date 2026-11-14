import { TURN_BATCH_MAX, TURN_CALLS_IN_FLIGHT } from '../constants.js';
import { currentRequest, type FlowRequest } from './request-context.js';

type Waiting<Item, Result> = { item: Item; resolve: (result: Result) => void; reject: (error: unknown) => void };

/**
 * Gathers the calls a flow request makes under one key in the same turn, and sends them
 * TURN_BATCH_MAX to a call, TURN_CALLS_IN_FLIGHT calls at once, answering each with its own result.
 * A replay that starts thousands of steps then makes a few calls to its run's store, which holds a
 * couple of batches at a time, not every step's. Outside a flow request a call goes alone.
 */
export function gatherTurns<Item, Result>(
  send: (key: string, items: Item[]) => Promise<readonly Result[]>,
): (key: string, item: Item) => Promise<Result> {
  const turns = new WeakMap<FlowRequest, Map<string, Waiting<Item, Result>[]>>();
  const sendAll = async (key: string, batch: Waiting<Item, Result>[]) => {
    let next = 0;
    const lane = async () => {
      for (let start = next; start < batch.length; start = next) {
        next += TURN_BATCH_MAX;
        const chunk = batch.slice(start, start + TURN_BATCH_MAX);
        await send(key, chunk.map(({ item }) => item)).then((results) => {
          if (results.length !== chunk.length) throw new Error(`${results.length} results for ${chunk.length} calls`);
          chunk.forEach(({ resolve }, index) => resolve(results[index]));
        }).catch((error: unknown) => chunk.forEach(({ reject }) => reject(error)));
      }
    };
    await Promise.all(Array.from({ length: TURN_CALLS_IN_FLIGHT }, lane));
  };
  return (key, item) => {
    const request = currentRequest();
    if (request === undefined) return send(key, [item]).then(([result]) => result);
    const keys = turns.get(request) ?? new Map<string, Waiting<Item, Result>[]>();
    turns.set(request, keys);
    const batch = keys.get(key) ?? [];
    if (batch.length === 0) {
      keys.set(key, batch);
      setTimeout(() => {
        keys.delete(key);
        void sendAll(key, batch);
      });
    }
    return new Promise((resolve, reject) => batch.push({ item, resolve, reject }));
  };
}
