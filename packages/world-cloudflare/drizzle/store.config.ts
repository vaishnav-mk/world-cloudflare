import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  dialect: 'sqlite',
  driver: 'durable-sqlite',
  schema: './src/store/tables.ts',
  out: './drizzle/store',
});
