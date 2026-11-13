import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { workerAliases } from '@workflow/world-cloudflare/build';
import { defineConfig } from 'vitest/config';

// The SDK end-to-end suite again, with new runs on spec 9 (WORKFLOW_SPEC_VERSION=9).
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.spec9.test.jsonc' } })],
  resolve: { alias: workerAliases },
  test: { include: ['test/e2e.test.ts', 'test/spec9-e2e.test.ts'], testTimeout: 30_000 },
});
