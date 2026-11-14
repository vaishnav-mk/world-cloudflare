import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { workerAliases } from '@workflow/world-cloudflare/build';
import { defineConfig } from 'vitest/config';

// Replay measurements, not part of any suite:
//   WORKFLOW_VM=node|quickjs [REPLAY_COUNTS=200,1000] pnpm exec vitest run --config vitest.replays.config.ts
export default defineConfig({
  plugins: [cloudflareTest({
    wrangler: { configPath: process.env.WORKFLOW_VM === 'node' ? './wrangler.node.test.jsonc' : './wrangler.app.test.jsonc' },
    miniflare: { bindings: { REPLAY_COUNTS: process.env.REPLAY_COUNTS ?? '200,1000' } },
  })],
  resolve: { alias: workerAliases },
  ssr: { resolve: { conditions: ['worlds-source'] } },
  test: { include: ['test/replays.test.ts'], testTimeout: 660_000 },
});
