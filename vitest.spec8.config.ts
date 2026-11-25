import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { workerAliases } from 'world-cloudflare/build';
import { defineConfig } from 'vitest/config';

// The SDK end-to-end suite again, with new runs on spec 8 (WORKFLOW_SPEC_VERSION=8).
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.spec8.test.jsonc' } })],
  resolve: { alias: workerAliases },
  ssr: { resolve: { conditions: ['worlds-source'] } },
  test: { include: ['test/e2e.test.ts', 'test/world-testing.test.ts'], testTimeout: 30_000 },
});
