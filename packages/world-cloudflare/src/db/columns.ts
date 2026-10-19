import { customType } from 'drizzle-orm/sqlite-core';

/** A BLOB column read and written as an `ArrayBuffer`, the type Durable Object SQLite binds. */
export const bytes = customType<{ data: ArrayBuffer }>({ dataType: () => 'blob' });
