import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { workerAliases } from '@workflow/world-cloudflare/build';
import { defineConfig } from 'vitest/config';

// The SDK end-to-end suite again, with new runs on the node engine (WORKFLOW_VM=node, on vm.ts), and
// the stress suite, which runs each workflow on both engines and compares them.
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.node.test.jsonc' } })],
  resolve: { alias: workerAliases },
  ssr: { resolve: { conditions: ['worlds-source'] } },
  test: { include: ['test/e2e.test.ts', 'test/world-testing.test.ts', 'test/node-engine.test.ts', 'test/stress.test.ts'], testTimeout: 30_000 },
});
