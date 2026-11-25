import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { workerAliases } from 'world-cloudflare/build';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.app.test.jsonc' } })],
  resolve: { alias: workerAliases },
  ssr: { resolve: { conditions: ['worlds-source'] } },
  test: { include: ['test/e2e.test.ts', 'test/world-testing.test.ts', 'test/spec9-e2e.test.ts'], testTimeout: 30_000 },
});
