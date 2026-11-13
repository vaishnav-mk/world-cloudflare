import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { defineConfig } from 'vitest/config';

// The SDK end-to-end suite again, with new runs on the isolate engine (WORKFLOW_VM=isolate), and the
// stress suite, which runs each workflow on both engines and compares them.
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.isolate.test.jsonc' } })],
  test: { include: ['test/e2e.test.ts', 'test/isolate.test.ts', 'test/stress.test.ts'], testTimeout: 30_000 },
});
