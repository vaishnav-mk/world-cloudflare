import { parse, stringify } from 'devalue';
import { ENCODING_PREFIX } from '../constants.js';

/** Date fields written as ISO strings by the original JSON codec. */
const LEGACY_DATE_KEYS = new Set([
  'createdAt', 'updatedAt', 'startedAt', 'completedAt', 'expiredAt',
  'retryAfter', 'resumeAt', 'tokenRetentionUntil',
]);

/** Lossless encoding for rows: keeps `Date`, `Uint8Array` and `undefined` intact. */
export function encode(value: unknown): string {
  return `${ENCODING_PREFIX}${stringify(value)}`;
}

export function decode<T>(value: string): T {
  if (value.startsWith(ENCODING_PREFIX)) return parse(value.slice(ENCODING_PREFIX.length)) as T;
  return JSON.parse(value, reviveLegacy) as T;
}

function reviveLegacy(key: string, item: unknown): unknown {
  if (item && typeof item === 'object' && '__workflowBytes' in item) {
    return new Uint8Array((item as { __workflowBytes: number[] }).__workflowBytes);
  }
  return LEGACY_DATE_KEYS.has(key) && typeof item === 'string' ? new Date(item) : item;
}
