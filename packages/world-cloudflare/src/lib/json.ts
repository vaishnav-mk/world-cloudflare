import { isPayloadRef, payloadRefKey } from './payloads.js';

/**
 * JSON with binary values as `{ "$bytes": "<base64>" }`. Plain `JSON.stringify` writes a
 * `Uint8Array` as an index map, about 12 times its size. A payload kept in R2 is written as
 * `{ "$ref": "<key>" }`: the store's event subscription sees only the ref.
 */
export function toJson(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) => {
    if (!(item instanceof Uint8Array)) return item;
    return isPayloadRef(item) ? { $ref: payloadRefKey(item) } : { $bytes: Buffer.from(item).toString('base64') };
  });
}
