import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { workerAliases } from 'world-cloudflare/build';
import { defineConfig } from 'vitest/config';

// SDK runs with WORKFLOW_ENCRYPTION_SECRET set. The other end-to-end configs keep covering plaintext.
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.encrypted.test.jsonc' } })],
  resolve: { alias: workerAliases },
  ssr: { resolve: { conditions: ['worlds-source'] } },
  test: { include: ['test/encrypted.test.ts'], testTimeout: 30_000 },
});
