import { parse, stringify } from 'devalue';

/**
 * Wire protocol for driving a deployed World from Node, as the SDK's web UI does. Requests and
 * responses are devalue-encoded, so dates, bytes and `undefined` survive the trip.
 */

export const RPC_PATH = '/world/rpc';
export const RPC_CONTENT_TYPE = 'application/x-devalue';

/** World methods the endpoint serves. Anything else is rejected. */
export enum RpcMethod {
  RunsGet = 'runs.get',
  RunsGetMany = 'runs.getMany',
  RunsList = 'runs.list',
  RunsWaitForTerminalStatus = 'runs.waitForTerminalStatus',
  RunsCancelMany = 'runs.cancelMany',
  StepsGet = 'steps.get',
  StepsList = 'steps.list',
  EventsCreate = 'events.create',
  EventsGet = 'events.get',
  EventsList = 'events.list',
  EventsListByCorrelationId = 'events.listByCorrelationId',
  HooksGet = 'hooks.get',
  HooksGetByToken = 'hooks.getByToken',
  HooksList = 'hooks.list',
  StreamsWrite = 'streams.write',
  StreamsWriteMulti = 'streams.writeMulti',
  StreamsClose = 'streams.close',
  StreamsList = 'streams.list',
  StreamsGetChunks = 'streams.getChunks',
  StreamsGetInfo = 'streams.getInfo',
  /** Answered with the stream's bytes as the response body instead of an encoded result. */
  StreamsGet = 'streams.get',
  SnapshotsSave = 'experimental_snapshots.save',
  SnapshotsLoad = 'experimental_snapshots.load',
  SnapshotsDelete = 'experimental_snapshots.delete',
  Queue = 'queue',
  GetDeploymentId = 'getDeploymentId',
  GetEncryptionKeyForRun = 'getEncryptionKeyForRun',
}

const METHODS: ReadonlySet<string> = new Set(Object.values(RpcMethod));

type RpcRequest = { method: RpcMethod; args: unknown[] };

/** A thrown error, flattened so its class can be rebuilt on the other side. */
export type RpcError = { name: string; message: string; props: Record<string, unknown> };

export type RpcResult = { ok: true; value: unknown } | { ok: false; error: RpcError };

export const encode = (value: RpcRequest | RpcResult): string => stringify(value);

export function decodeRequest(body: string): RpcRequest {
  const request: unknown = parse(body);
  if (!isRecord(request) || typeof request.method !== 'string' || !METHODS.has(request.method) || !Array.isArray(request.args)) {
    throw new TypeError('Invalid World RPC request');
  }
  // SAFETY: the method was checked against the RpcMethod values just above.
  return { method: request.method as RpcMethod, args: request.args };
}

export function decodeResult(body: string): RpcResult {
  // SAFETY: results come from our own endpoint, which only writes RpcResult values.
  return parse(body) as RpcResult;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
