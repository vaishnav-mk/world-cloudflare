/**
 * JSON with binary values as `{ "$bytes": "<base64>" }`. Plain `JSON.stringify` writes a
 * `Uint8Array` as an index map, about 12 times its size.
 */
export function toJson(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) =>
    item instanceof Uint8Array ? { $bytes: Buffer.from(item).toString('base64') } : item);
}
