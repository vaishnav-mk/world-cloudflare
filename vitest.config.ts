import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { defineConfig } from 'vitest/config';

// `gc()` for the stream memory test in test/streams.test.ts.
process.env.MINIFLARE_WORKERD_V8_FLAGS = [process.env.MINIFLARE_WORKERD_V8_FLAGS, '--expose-gc'].filter(Boolean).join(' ');

export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: './wrangler.test.jsonc' } })],
  test: { include: ['test/**/*.test.ts'], exclude: ['test/e2e.test.ts', 'test/node-engine.test.ts', 'test/encrypted.test.ts', 'test/stress.test.ts', 'test/spec9-e2e.test.ts', 'test/world-testing.test.ts', 'test/replays.test.ts'] },
});
