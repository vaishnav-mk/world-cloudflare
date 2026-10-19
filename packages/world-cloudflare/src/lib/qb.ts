/**
 * Result helpers for synchronous workers-qb queries. workers-qb types `results` as optional, so
 * the absence is handled here once instead of at every call site.
 */
type Query<T> = { execute(): { results?: T } };

/** All rows; no result means no rows. */
export function allRows<T>(query: Query<T[]>): T[] {
  return query.execute().results ?? [];
}

/** The first row, when the query may legitimately match nothing. */
export function findRow<T>(query: Query<T>): T | undefined {
  return query.execute().results;
}

/** A row the query always returns, such as an aggregate or an upsert with `RETURNING`. */
export function requireRow<T>(query: Query<T>): T {
  const row = query.execute().results;
  if (row === undefined) throw new Error('Query returned no row');
  return row;
}
