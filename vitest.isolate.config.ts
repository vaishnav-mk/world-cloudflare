import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { defineConfig } from 'vitest/config';

// The SDK end-to-end suite again, with new runs on the isolate engine (WORKFLOW_VM=isolate).
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.isolate.test.jsonc' } })],
  test: { include: ['test/e2e.test.ts', 'test/isolate.test.ts'], testTimeout: 30_000 },
});
