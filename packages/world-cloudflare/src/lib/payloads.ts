import type { AnyEventRequest } from '@workflow/world';
import { PAYLOAD_INLINE_MAX_BYTES, PAYLOAD_REF_PREFIX } from '../constants.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Payloads this call wrote to R2, by key, so its result can reuse them instead of reading R2 back. */
type Written = Map<string, Uint8Array>;

/** Whether `bytes` are a ref to a payload in R2 rather than the payload itself. */
export function isPayloadRef(bytes: Uint8Array): boolean {
  return bytes.byteLength >= PAYLOAD_REF_PREFIX.byteLength && PAYLOAD_REF_PREFIX.every((byte, index) => bytes[index] === byte);
}

/** The R2 key a ref points at. */
export function payloadRefKey(ref: Uint8Array): string {
  return decoder.decode(ref.subarray(PAYLOAD_REF_PREFIX.byteLength));
}

/**
 * Moves large payloads to R2 before they reach a Durable Object, and back after. It runs in the
 * calling Worker, so large values never cross Durable Object RPC or sit in SQLite. The objects
 * store a ref in their place: `PAYLOAD_REF_PREFIX` followed by the R2 key.
 */
export function createPayloads(bucket: R2Bucket) {
  /** What a Durable Object should store: `bytes` themselves, or a ref once they are in R2. */
  async function offload(runId: string, bytes: Uint8Array, written?: Written): Promise<Uint8Array> {
    // Small bytes that look like a ref go to R2 too, so a stored ref is never user data.
    if (bytes.byteLength <= PAYLOAD_INLINE_MAX_BYTES && !isPayloadRef(bytes)) return bytes;
    const key = `runs/${runId}/${crypto.randomUUID()}`;
    await bucket.put(key, bytes);
    written?.set(key, bytes);
    return Uint8Array.of(...PAYLOAD_REF_PREFIX, ...encoder.encode(key));
  }

  async function resolve(bytes: Uint8Array, written?: Written): Promise<Uint8Array> {
    if (!isPayloadRef(bytes)) return bytes;
    const key = payloadRefKey(bytes);
    const known = written?.get(key);
    if (known !== undefined) return known;
    const object = await bucket.get(key);
    if (object === null) throw new Error(`Payload ${key} is missing from R2`);
    return new Uint8Array(await object.arrayBuffer());
  }

  /** A copy of the request with each binary `eventData` field offloaded: the payload, and dynamic workflow code. */
  async function offloadEvent<T extends AnyEventRequest>(runId: string, request: T, written: Written): Promise<T> {
    if (!('eventData' in request) || request.eventData === undefined) return request;
    const fields = await Promise.all(Object.entries(request.eventData).map(async ([key, value]: [string, unknown]) =>
      [key, value instanceof Uint8Array ? await offload(runId, value, written) : value]));
    // SAFETY: only Uint8Array fields change, and each stays a Uint8Array.
    return { ...request, eventData: Object.fromEntries(fields) } as T;
  }

  /** Replaces every ref in a Durable Object's result with its payload, in place. */
  async function resolveAll<T>(result: T, written?: Written): Promise<T> {
    const pending: Promise<void>[] = [];
    const visit = (node: unknown) => {
      if (typeof node !== 'object' || node === null) return;
      // SAFETY: RPC results are plain records and arrays this call owns, so writing to them is safe.
      const record = node as Record<string, unknown>;
      for (const [key, value] of Object.entries(record)) {
        if (!(value instanceof Uint8Array)) visit(value);
        else if (isPayloadRef(value)) pending.push(resolve(value, written).then((bytes) => { record[key] = bytes; }));
      }
    };
    visit(result);
    await Promise.all(pending);
    return result;
  }

  return { offload, resolve, offloadEvent, resolveAll };
}
