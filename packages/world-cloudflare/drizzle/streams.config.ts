import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  dialect: 'sqlite',
  driver: 'durable-sqlite',
  schema: './src/streams/tables.ts',
  out: './drizzle/streams',
});
