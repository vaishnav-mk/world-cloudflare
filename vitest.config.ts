import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.test.jsonc' } })],
  test: { include: ['test/**/*.test.ts'], exclude: ['test/e2e.test.ts', 'test/isolate.test.ts', 'test/encrypted.test.ts', 'test/stress.test.ts', 'test/spec9-e2e.test.ts'] },
});
