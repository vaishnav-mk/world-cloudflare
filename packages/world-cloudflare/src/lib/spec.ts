import { mintedSpecVersion, SPEC_VERSION_SINGLE_ORCHESTRATOR, type SpecVersion } from '@workflow/world';

/**
 * The spec version new runs get: 8 unless `WORKFLOW_SPEC_VERSION` asks for another. The World
 * implements the in-band fence, so `WORKFLOW_SPEC_VERSION=9` creates single-orchestrator runs.
 */
export function mintSpecVersion(): SpecVersion {
  return mintedSpecVersion(process.env, { inBandFence: true });
}

/** Spec 9 runs: one orchestrator decides, step and wait events are appends, and in-band writes are fenced. */
export function isSingleOrchestrator(specVersion: number | undefined): boolean {
  return specVersion !== undefined && specVersion >= SPEC_VERSION_SINGLE_ORCHESTRATOR;
}
