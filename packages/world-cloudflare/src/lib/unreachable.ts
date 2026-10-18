/**
 * Ends a switch over an SDK event type. TypeScript does not treat enum cases as exhausting a
 * string-literal union, so this marks the end and fails to compile if a case is missing.
 */
export function unreachable(value: never): never {
  throw new Error(`Unhandled case: ${JSON.stringify(value)}`);
}
