import { captureInvocationOutcome } from '@workflow/errors/invocation';
import { isSingleOrchestratorMessage, MessageId, QueuePrefix, ValidQueueName, type Queue, type QueuePayload } from '@workflow/world';
import { parse, stringify } from 'devalue';
import { MAX_SPEC9_REDELIVERY_SECONDS, MAX_WAKE_SECONDS } from '../constants.js';

/**
 * What a handler's `{ timeoutSeconds }` asks of the queue, or undefined for an acknowledgment.
 * JSON has no Infinity, so an unbounded or negative wait gets the longest delay instead, never 0,
 * which on a spec 9 run is a handoff that is not an attempt.
 */
export function wakeSeconds(result: unknown, payload: unknown): number | undefined {
  if (typeof result !== 'object' || result === null || !('timeoutSeconds' in result)) return undefined;
  const seconds = Number(result.timeoutSeconds);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds;
  return isSingleOrchestratorMessage(payload) ? MAX_SPEC9_REDELIVERY_SECONDS : MAX_WAKE_SECONDS;
}

/**
 * The flow route's side of the queue: reads one delivery from the headers the consumer sets
 * (`flowRequest` in `consumer.ts`), runs the SDK's handler, and answers with what the queue should
 * do next.
 */
export const createQueueHandler: Queue['createQueueHandler'] = (prefix, handler) => async (request) => {
  const queueName = request.headers.get('x-vqs-queue-name');
  const messageId = request.headers.get('x-vqs-message-id');
  const attempt = Number(request.headers.get('x-vqs-message-attempt'));
  const validQueue = queueName !== null && ValidQueueName.safeParse(queueName).success &&
    queueName.startsWith(QueuePrefix.parse(prefix));
  if (!validQueue || !messageId || !Number.isSafeInteger(attempt) || attempt < 1) {
    return Response.json({ error: 'Invalid queue delivery' }, { status: 400 });
  }
  try {
    // SAFETY: only runners and private dispatch reach this route, with a body `queue()` stored; the SDK handler checks its shape.
    const payload = parse(await request.text()) as QueuePayload;
    // Absent from a consumer that predates them: then every delivery may be a redelivery.
    const deliveryCount = Number(request.headers.get('x-vqs-delivery-count') ?? Number.NaN);
    const startedAt = Number(request.headers.get('x-workflow-invocation-started-at') ?? Number.NaN);
    const meta = {
      queueName: ValidQueueName.parse(queueName), messageId: MessageId.parse(messageId), attempt,
      deliveryCount: Number.isSafeInteger(deliveryCount) && deliveryCount >= 1 ? deliveryCount : undefined,
      createdAt: new Date(Number(request.headers.get('x-vqs-created-at'))),
      invocationStartedAt: Number.isSafeInteger(startedAt) ? startedAt : Date.now(),
    };
    // An invocation's result is its response, never queue control, and its errors go back with it.
    if ('invoke' in payload && payload.invoke === true) {
      return new Response(stringify(await captureInvocationOutcome(() => handler(payload, meta))));
    }
    const result = await handler(payload, meta);
    const seconds = wakeSeconds(result, payload);
    if (seconds !== undefined) return Response.json({ timeoutSeconds: seconds });
    return Response.json({ ok: true });
  } catch (error) {
    console.error('Workflow delivery failed', error);
    // SDK world errors may cross realms; RetryableError's Date is not a queue retry floor.
    const retryAfter = typeof error === 'object' && error !== null && 'retryAfter' in error ? error.retryAfter : undefined;
    return Response.json({
      error: 'Delivery failed',
      ...(typeof retryAfter === 'number' && Number.isFinite(retryAfter) && retryAfter > 0 ? { retryAfter } : {}),
    }, { status: 500 });
  }
};
