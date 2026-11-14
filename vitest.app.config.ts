import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { workerAliases } from '@workflow/world-cloudflare/build';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.app.test.jsonc' } })],
  resolve: { alias: workerAliases },
  test: { include: ['test/e2e.test.ts'], testTimeout: 30_000 },
});
