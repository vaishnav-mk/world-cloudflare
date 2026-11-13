/**
 * What the World reads from an SDK queue payload. Payloads are plain objects whose fields, except a
 * run's `runId`, are all optional, and newer SDKs add fields, so these check each field they use.
 */

export function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object';
}

/** A step execution message: it runs one step of its run. */
export function isStepMessage(payload: unknown): boolean {
  return isRecord(payload) && typeof payload.stepId === 'string';
}

/** A message that replays its run, as the queue shard orders and coalesces it. */
export type Replay = {
  runId: string;
  /**
   * Delivering it does nothing but replay the run, so a replay that starts once it is due and
   * succeeds covers it.
   */
  pure: boolean;
  /** A run's first message. Its replay may skip reading the log, so it covers no other message. */
  start: boolean;
};

/** Fields that identify, route or trace a replay and ask the handler for nothing else. */
const REPLAY_ONLY_FIELDS: ReadonlySet<string> = new Set(['runId', 'runSpecVersion', 'traceCarrier', 'requestedAt', 'hookResumeTiming']);

/**
 * The replay a message asks for, or `undefined` for a step message, a health check, a message with
 * no run, and a long wait's intermediate timer hop (`waitContinuation.resumeAtMs`), which only sends
 * the next hop. A field outside `REPLAY_ONLY_FIELDS`, such as a start's `runInput`, a timer's
 * `waitContinuation`, a legacy resume's `hookInput`, `completeWaits` or a retry counter, carries
 * something the handler must see, so its message is not pure and is delivered itself.
 */
export function replayOf(payload: unknown): Replay | undefined {
  if (!isRecord(payload) || payload.__healthCheck === true || isStepMessage(payload)) return undefined;
  if (isRecord(payload.waitContinuation) && Number.isFinite(payload.waitContinuation.resumeAtMs)) return undefined;
  if (typeof payload.runId !== 'string') return undefined;
  return {
    runId: payload.runId,
    pure: Object.entries(payload).every(([field, value]) => value === undefined || REPLAY_ONLY_FIELDS.has(field)),
    start: payload.runInput !== undefined,
  };
}
