import * as errors from '@workflow/errors';
import { mintedSpecVersion, type World, type WorkflowRun } from '@workflow/world';
import {
  decodeResult, encode, isRecord, RPC_CONTENT_TYPE, RPC_PATH, RpcMethod, type RpcError,
} from './protocol.js';

/**
 * A World for Node that forwards every call to a deployed Cloudflare World. Point the SDK's tools
 * at it with `WORKFLOW_TARGET_WORLD=@workflow/world-cloudflare/remote`, plus:
 * - `WORKFLOW_CLOUDFLARE_URL`: the Worker that serves `/world/rpc`;
 * - `WORKFLOW_CLOUDFLARE_TOKEN`: the Worker's `WORLD_RPC_TOKEN` secret.
 *
 * It cannot receive queue deliveries; workflows still run on Cloudflare.
 */
export function createWorld(options: { url?: string; token?: string } = {}): World {
  const url = options.url ?? process.env.WORKFLOW_CLOUDFLARE_URL;
  const token = options.token ?? process.env.WORKFLOW_CLOUDFLARE_TOKEN;
  if (!url || !token) throw new Error('Set WORKFLOW_CLOUDFLARE_URL and WORKFLOW_CLOUDFLARE_TOKEN');
  const endpoint = new URL(RPC_PATH, url);

  const post = async (method: RpcMethod, args: unknown[]): Promise<Response> => {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': RPC_CONTENT_TYPE },
      body: encode({ method, args: args.map(withoutLocalValues) }),
    });
    if (!response.ok) throw new Error(`World RPC ${method} failed: HTTP ${response.status} ${await response.text()}`);
    return response;
  };

  const call = (method: RpcMethod) => async (...args: unknown[]): Promise<unknown> => {
    const result = decodeResult(await (await post(method, args)).text());
    if (!result.ok) throw rebuild(result.error);
    return result.value;
  };

  const world = {
    specVersion: mintedSpecVersion(),
    runs: {
      get: call(RpcMethod.RunsGet),
      getMany: call(RpcMethod.RunsGetMany),
      list: call(RpcMethod.RunsList),
      waitForTerminalStatus: call(RpcMethod.RunsWaitForTerminalStatus),
      cancelMany: call(RpcMethod.RunsCancelMany),
    },
    steps: { get: call(RpcMethod.StepsGet), list: call(RpcMethod.StepsList) },
    events: {
      create: call(RpcMethod.EventsCreate),
      get: call(RpcMethod.EventsGet),
      list: call(RpcMethod.EventsList),
      listByCorrelationId: call(RpcMethod.EventsListByCorrelationId),
    },
    hooks: { get: call(RpcMethod.HooksGet), getByToken: call(RpcMethod.HooksGetByToken), list: call(RpcMethod.HooksList) },
    streams: {
      write: call(RpcMethod.StreamsWrite),
      writeMulti: call(RpcMethod.StreamsWriteMulti),
      close: call(RpcMethod.StreamsClose),
      list: call(RpcMethod.StreamsList),
      getChunks: call(RpcMethod.StreamsGetChunks),
      getInfo: call(RpcMethod.StreamsGetInfo),
      get: async (...args: unknown[]) => (await post(RpcMethod.StreamsGet, args)).body,
    },
    experimental_snapshots: {
      save: call(RpcMethod.SnapshotsSave),
      load: call(RpcMethod.SnapshotsLoad),
      delete: call(RpcMethod.SnapshotsDelete),
    },
    queue: call(RpcMethod.Queue),
    getDeploymentId: call(RpcMethod.GetDeploymentId),
    // The key depends only on the run ID, so send that rather than the run and its payloads.
    getEncryptionKeyForRun: (run: WorkflowRun | string) =>
      call(RpcMethod.GetEncryptionKeyForRun)(typeof run === 'string' ? run : run.runId),
    createQueueHandler: () => {
      throw new Error('The remote Cloudflare World cannot receive queue deliveries');
    },
  };
  // SAFETY: each method forwards to the deployed World's method of the same name, which returns
  // the World's own types. The overloaded signatures cannot be expressed by `call`.
  return world as unknown as World;
}

/** Drops values that only mean something in this process, such as an `AbortSignal` or a callback. */
function withoutLocalValues(arg: unknown): unknown {
  if (!isRecord(arg) || Array.isArray(arg) || arg instanceof Uint8Array || arg instanceof Date) return arg;
  return Object.fromEntries(Object.entries(arg).filter(([, value]) =>
    typeof value !== 'function' && !(value instanceof AbortSignal)));
}

/** Rebuilds the server's error with its SDK class, so `WorkflowRunNotFoundError.is()` and `instanceof` work. */
function rebuild({ name, message, props }: RpcError): Error {
  const error = Object.assign(new Error(message), props, { name });
  const ErrorClass = (errors as Record<string, unknown>)[name];
  if (typeof ErrorClass === 'function') Object.setPrototypeOf(error, ErrorClass.prototype);
  return error;
}

export default createWorld;
