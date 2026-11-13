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
