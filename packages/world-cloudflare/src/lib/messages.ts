import { orchestratorRunIdOf } from '@workflow/world';
import { parse } from 'devalue';

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
 * The replay a message asks for: the SDK's orchestrator messages (`orchestratorRunIdOf`), of any
 * spec version, which leaves out steps, health checks and a long wait's intermediate timer hops. A
 * field outside `REPLAY_ONLY_FIELDS`, such as a start's `runInput`, a timer's `waitContinuation`, a
 * legacy resume's `hookInput`, `completeWaits` or a retry counter, carries something the handler
 * must see, so its message is not pure and is delivered itself.
 */
export function replayOf(payload: unknown): Replay | undefined {
  const runId = orchestratorRunIdOf(payload);
  if (runId === undefined) return undefined;
  // SAFETY: `orchestratorRunIdOf` finds a run only in a payload object.
  const fields = payload as Record<string, unknown>;
  return {
    runId,
    pure: Object.entries(fields).every(([field, value]) => value === undefined || REPLAY_ONLY_FIELDS.has(field)),
    start: fields.runInput !== undefined,
  };
}

/** The SDK payload a message carries, or undefined when it cannot be read. */
export function readPayload(encoded: string): unknown {
  try {
    return parse(encoded);
  } catch {
    return undefined;
  }
}
