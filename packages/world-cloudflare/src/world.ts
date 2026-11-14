import { env as platformEnv } from 'cloudflare:workers';
import { WorkflowWorldError } from '@workflow/errors';
import type { World } from '@workflow/world';
import { MAX_EXECUTION_CONTEXT_BYTES } from './constants.js';
import { dynamicCodeKey } from './lib/payloads.js';
import { createGetEncryptionKeyForRun } from './lib/encryption.js';
import { currentDeadline } from './lib/request-context.js';
import { mintSpecVersion } from './lib/spec.js';
import { createQueue } from './queue/queue.js';
import { createStorage } from './store/storage.js';
import { createSnapshots } from './streams/snapshots.js';
import { createStreamer } from './streams/streamer.js';
import type { WorldEnv } from './types.js';

/** The Cloudflare World. Defaults to the Worker's own bindings. */
// SAFETY: the Worker declares the World's bindings (README, Setup); `Cloudflare.Env` lists them only in the Worker's own generated types.
export function createWorld(env: WorldEnv = platformEnv as WorldEnv): World {
  return {
    specVersion: mintSpecVersion(),
    capabilities: {
      // Hooks keep `tokenRetentionUntil` past their run's end; see `Db.isExpiredHook`.
      hookRetention: { active: true },
      // The run's store claims the token, then creates the run with its start Hook; see `admit` in `store/token-claims.ts`.
      atomicStartHook: { active: true },
      // A unique `(run_id, resume_id)` index, and the store returns the committed event to a retry.
      hookResumeDedup: true,
      // A run's workflow deliveries and invocations all reach its one `Runner`, which runs the
      // deliveries one at a time and the invocations beside them.
      invoke: true,
      // Every resume is deduplicated, so a cached hook's resume needs only its `resumeId`; a missing
      // or taken hook is refused before anything is written.
      resolvedHookResume: true,
      // Spec 9 runs count their in-band positions in `runs.seq_in_band`; see `checkFence`.
      inBandFence: true,
      // A spec 9 run's orchestrator messages share its queue shard, which delivers one at a time.
      maxConcurrency: true,
      // The token's shard takes the token over once the holder's store disposed of its hook; see
      // `WorldStore.claimHookTokenForced`.
      hookForceClaim: true,
      // The code is stored with the run like any payload, in R2 once it is large.
      dynamicWorkflowCode: true,
      // A deployment ID names one release, run by this Worker or by its own retained executor, so a
      // delivery that reached another deployment goes back to its run's.
      deploymentAffinity: true,
    },
    // A dynamic run's code too large for its run_created goes to R2 first, under the run's prefix.
    uploadDynamicWorkflowCode: async (runId, { code }) => {
      const key = dynamicCodeKey(runId);
      await env.PAYLOADS.put(key, code);
      return key;
    },
    validateRunExecutionContext: (context) => {
      const bytes = new TextEncoder().encode(JSON.stringify(context)).byteLength;
      if (bytes > MAX_EXECUTION_CONTEXT_BYTES) {
        throw new WorkflowWorldError(`The run's execution context is ${bytes} bytes, over the ${MAX_EXECUTION_CONTEXT_BYTES}-byte limit`, { status: 400 });
      }
    },
    // A delivery hands off after FLOW_DELIVERY_BUDGET_MS, well inside the CPU limit.
    getRuntimeDeadline: async () => currentDeadline(),
    // `start({ deploymentId: 'latest' })`: the main Worker's deployment, also from a retained executor.
    resolveLatestDeploymentId: () => env.CONSUMER.latestDeploymentId(),
    ...createStorage(env),
    ...createQueue(env),
    ...createStreamer(env),
    ...createSnapshots(env),
    // Without a secret the SDK stores plaintext, as before.
    ...(env.WORKFLOW_ENCRYPTION_SECRET === undefined
      ? {} : { getEncryptionKeyForRun: createGetEncryptionKeyForRun(env.WORKFLOW_ENCRYPTION_SECRET) }),
  };
}

/**
 * Makes `world` the SDK's World, as `setWorld()` from `workflow/runtime` does, by writing the
 * World cache the SDK reads from `globalThis`.
 * shortcut: `workflow/runtime` 5.5.0 draws a random ID as it loads, which Workers forbid at startup;
 * switch to `setWorld()` once a release defers it (upstream main does).
 */
export function registerWorld(world: World): void {
  // SAFETY: the keys `setWorld()` writes; every copy of the SDK in the Worker reads them.
  const cache = globalThis as unknown as Record<symbol, World | undefined>;
  cache[Symbol.for('@workflow/world//cache')] = world;
  cache[Symbol.for('@workflow/world//stubbedCache')] = world;
  cache[Symbol.for('@workflow/world//cachePromise')] = undefined;
  cache[Symbol.for('@workflow/world//stubbedCachePromise')] = undefined;
}
