import { WorkflowWorldError } from '@workflow/errors';

type DurableObjectError = Error & { retryable?: boolean; overloaded?: boolean };

/**
 * Wraps a Durable Object stub so failures the platform marks transient (`retryable`, such as a
 * storage reset, or `overloaded`) reject as a 503 `WorkflowWorldError`. The SDK retries those
 * through queue redelivery; any other error would fail the run.
 */
export function transientAsWorldError<T extends object>(stub: T): T {
  return new Proxy(stub, {
    get(target, key) {
      const member: unknown = Reflect.get(target, key);
      if (typeof member !== 'function') return member;
      return (...args: unknown[]) => Promise.resolve(Reflect.apply(member, target, args)).catch((error: unknown) => {
        throw toWorldError(error);
      });
    },
  });
}

/**
 * The error to throw for a Durable Object failure: a retryable 503 when the platform marks it
 * transient, or when the call chain ran out of subrequest depth, which a later delivery that
 * starts a fresh chain does not.
 */
export function toWorldError(error: unknown): unknown {
  if (!(error instanceof Error)) return error;
  // SAFETY: Durable Object errors carry these optional flags; any other Error has neither.
  const { retryable, overloaded } = error as DurableObjectError;
  if (retryable !== true && overloaded !== true && !isDepthExhausted(error)) return error;
  return new WorkflowWorldError(error.message, { status: 503, cause: error });
}

/** Whether a call failed because its chain of calls between Workers and objects grew too deep. */
export function isDepthExhausted(error: unknown): boolean {
  return String(error).includes('Subrequest depth limit exceeded');
}
