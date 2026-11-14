import { decodeSnapshotEnvelope, encodeSnapshotEnvelope, type Storage } from '@workflow/world';
import { objectOptions } from '../lib/shards.js';
import { transientAsWorldError } from '../lib/transient.js';
import type { WorldEnv } from '../types.js';

/**
 * The World's VM snapshot storage. Each run's snapshot lives in that run's `RunStreams` object as
 * one envelope of metadata and bytes, so the two are always saved and read together. Envelopes
 * travel as streams, because the SDK's 32 MiB snapshot cap plus framing can pass the RPC limit.
 */
export function createSnapshots(env: Pick<WorldEnv, 'RUN_STREAMS'>): Pick<Required<Storage>, 'experimental_snapshots'> {
  const streamsOf = (runId: string) => transientAsWorldError(env.RUN_STREAMS.getByName(runId, objectOptions()));
  return {
    experimental_snapshots: {
      save: (runId, data, metadata) => {
        // SAFETY: encodeSnapshotEnvelope returns a new Uint8Array over its own ArrayBuffer.
        const envelope = encodeSnapshotEnvelope(metadata, data) as Uint8Array<ArrayBuffer>;
        return streamsOf(runId).saveSnapshot(new Blob([envelope]).stream());
      },
      load: async (runId) => {
        const envelope = await streamsOf(runId).loadSnapshot();
        return envelope === null ? null : decodeSnapshotEnvelope(new Uint8Array(await new Response(envelope).arrayBuffer()));
      },
      delete: (runId) => streamsOf(runId).deleteSnapshot(),
    },
  };
}
