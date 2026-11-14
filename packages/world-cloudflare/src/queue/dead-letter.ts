import { parse } from 'devalue';
import type { StoredMessage } from 'durability';
import { MessageKind } from '../enums.js';
import { isRecord } from '../lib/is-record.js';
import { unreachable } from '../lib/unreachable.js';
import { answersUnknown, onRun } from '../store/route.js';
import type { Envelope, WorldEnv } from '../types.js';

/** The run wake a dead-lettered message carried, so the store can reopen its outbox row. */
type DeadLetter =
  | { kind: MessageKind.Step; runId: string; stepId: string; stepName: string }
  | { kind: MessageKind.WaitContinuation; runId: string; correlationId: string }
  | { kind: MessageKind.HookWake; runId: string };

/** The legacy store answers for a run it lacks, with nothing to reopen. */

/** Saves an exhausted message to R2 and reopens the outbox row whose wake it was. */
export async function deadLetter(env: Pick<WorldEnv, 'DLQ' | 'WORLD_STORE'>, message: StoredMessage<Envelope>): Promise<void> {
  await env.DLQ.put(`workflow/${message.id}`, JSON.stringify(message), { onlyIf: { etagDoesNotMatch: '*' } });
  let payload: unknown;
  try {
    payload = parse(message.body.encoded);
  } catch (error) {
    console.error('Cannot recover corrupt DLQ payload', { messageId: message.id, error });
    return;
  }
  const letter = readDeadLetter(payload);
  if (!letter) return;
  switch (letter.kind) {
    case MessageKind.Step:
      await onRun(env.WORLD_STORE, letter.runId,
        (store) => store.reopenPublishedStepDispatch(letter.runId, letter.stepId, letter.stepName), answersUnknown);
      break;
    case MessageKind.WaitContinuation:
      await onRun(env.WORLD_STORE, letter.runId,
        (store) => store.reopenPublishedWaitDispatch(letter.runId, letter.correlationId), answersUnknown);
      break;
    case MessageKind.HookWake:
      // Reopens the run's start instead if it never started.
      await onRun(env.WORLD_STORE, letter.runId, (store) => store.reopenPublishedHookWakes(letter.runId), answersUnknown);
      break;
    default:
      unreachable(letter);
  }
}

function readDeadLetter(payload: unknown): DeadLetter | undefined {
  if (!isRecord(payload) || typeof payload.runId !== 'string') return undefined;
  const { runId } = payload;
  if (typeof payload.stepId === 'string') {
    return typeof payload.stepName === 'string'
      ? { kind: MessageKind.Step, runId, stepId: payload.stepId, stepName: payload.stepName }
      : undefined;
  }
  if ('waitContinuation' in payload) {
    const continuation = payload.waitContinuation;
    return isRecord(continuation) && typeof continuation.correlationId === 'string'
      ? { kind: MessageKind.WaitContinuation, runId, correlationId: continuation.correlationId }
      : undefined;
  }
  return { kind: MessageKind.HookWake, runId };
}
