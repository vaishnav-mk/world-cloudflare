import { WorkflowWorldError } from '@workflow/errors';
import { expect, it } from 'vitest';
import { transientAsWorldError } from '../packages/world-cloudflare/src/lib/transient.js';

const rejecting = (flags: object) => transientAsWorldError({
  call: async (): Promise<void> => { throw Object.assign(new Error('storage reset'), flags); },
});

it('rejects transient Durable Object failures as retryable World errors', async () => {
  for (const flags of [{ retryable: true }, { overloaded: true }]) {
    const error: unknown = await rejecting(flags).call().catch((caught: unknown) => caught);
    expect(WorkflowWorldError.is(error)).toBe(true);
    expect(error).toMatchObject({ status: 503, message: 'storage reset' });
  }
});

it('passes errors from the object code through unchanged', async () => {
  const error: unknown = await rejecting({ remote: true }).call().catch((caught: unknown) => caught);
  expect(WorkflowWorldError.is(error)).toBe(false);
  expect(error).toMatchObject({ message: 'storage reset' });
});
