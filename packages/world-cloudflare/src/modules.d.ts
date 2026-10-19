/** Migration SQL is bundled as text by the Wrangler `Text` rule for `*.sql`. */
declare module '*.sql' {
  const sql: string;
  export default sql;
}

/** Migration bundles written by `drizzle-kit generate` for the `durable-sqlite` driver. */
declare module '*/migrations.js' {
  const config: Parameters<typeof import('drizzle-orm/durable-sqlite/migrator').migrate>[1];
  export default config;
}
