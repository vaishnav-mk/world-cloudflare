import { EntityConflictError, WorkflowRunNotFoundError } from '@workflow/errors';
import {
  BulkCancelWorkflowRunsRequestSchema,
  type BulkCancelWorkflowRunResult, type BulkCancelWorkflowRunsRequest, type BulkCancelWorkflowRunsResult, type Storage,
} from '@workflow/world';
import { CANCEL_MANY_CONCURRENCY } from '../constants.js';
import { EventType, RunStatus } from '../enums.js';

/**
 * `runs.cancelMany`: writes `run_cancelled` to each run through `events.create`, a few runs at a
 * time, so each run gets the outcome its own cancellation would have had.
 */
export async function cancelMany(
  request: BulkCancelWorkflowRunsRequest, getRun: Storage['runs']['get'], create: Storage['events']['create'],
): Promise<BulkCancelWorkflowRunsResult> {
  const { runIds, cancelReason } = BulkCancelWorkflowRunsRequestSchema.parse(request);
  const cancelOne = async (runId: string): Promise<BulkCancelWorkflowRunResult> => {
    try {
      const { event } = await create(runId, {
        eventType: EventType.RunCancelled, ...(cancelReason === undefined ? {} : { eventData: { cancelReason } }),
      });
      // The store writes nothing for a run that is already cancelled.
      return { runId, outcome: event === undefined ? 'already_cancelled' : 'cancelled' };
    } catch (error) {
      if (WorkflowRunNotFoundError.is(error)) return { runId, outcome: 'not_found' };
      if (!EntityConflictError.is(error)) return failed(runId, error);
    }
    // Only a finished run refuses `run_cancelled`; report how it finished.
    try {
      const { status } = await getRun(runId, { resolveData: 'none' });
      return status === RunStatus.Cancelled ? { runId, outcome: 'already_cancelled' } : { runId, outcome: 'not_cancellable', status };
    } catch (error) {
      return failed(runId, error);
    }
  };
  const results: BulkCancelWorkflowRunResult[] = [];
  for (let start = 0; start < runIds.length; start += CANCEL_MANY_CONCURRENCY) {
    results.push(...await Promise.all(runIds.slice(start, start + CANCEL_MANY_CONCURRENCY).map(cancelOne)));
  }
  const count = (outcome: BulkCancelWorkflowRunResult['outcome']) => results.filter((result) => result.outcome === outcome).length;
  return {
    summary: {
      requested: runIds.length, cancelled: count('cancelled'), alreadyCancelled: count('already_cancelled'),
      notCancellable: count('not_cancellable'), notFound: count('not_found'), failed: count('failed'),
    },
    results,
  };
}

/** Like core's `cancelRuns` fallback, a failure is reported as retryable without classifying it. */
function failed(runId: string, error: unknown): BulkCancelWorkflowRunResult {
  console.error(`Failed to cancel run ${runId}`, error);
  return { runId, outcome: 'failed', code: 'internal_error', retryable: true };
}
