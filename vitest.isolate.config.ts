import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { workerAliases } from '@workflow/world-cloudflare/build';
import { defineConfig } from 'vitest/config';

// The SDK end-to-end suite again, with new runs on the native engine (the SDK's node engine on vm.ts,
// WORKFLOW_VM=node), and the stress suite, which runs each workflow on both engines and compares them.
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.isolate.test.jsonc' } })],
  resolve: { alias: workerAliases },
  test: { include: ['test/e2e.test.ts', 'test/isolate.test.ts', 'test/stress.test.ts'], testTimeout: 30_000 },
});
