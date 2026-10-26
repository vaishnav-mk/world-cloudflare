import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { defineConfig } from 'vitest/config';

// SDK runs with WORKFLOW_ENCRYPTION_SECRET set. The other end-to-end configs keep covering plaintext.
export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.encrypted.test.jsonc' } })],
  test: { include: ['test/encrypted.test.ts'], testTimeout: 30_000 },
});
