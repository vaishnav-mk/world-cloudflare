import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './test',
  use: { baseURL: 'http://127.0.0.1:4173', browserName: 'chromium', channel: 'chrome' },
  webServer: { command: 'pnpm preview --port 4173', url: 'http://127.0.0.1:4173', reuseExistingServer: !process.env.CI },
});
