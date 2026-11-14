import { expect, test } from '@playwright/test';

const paths = ['/', ...['architecture', 'lifecycle', 'services', 'state-and-delivery', 'engines', 'scaling', 'streams', 'vercel', 'benchmarks', 'security', 'operations'].map((slug) => `/docs/${slug}`)];

test('every page server-renders with working section targets', async ({ browser }) => {
  const context = await browser.newContext({ javaScriptEnabled: false });
  const page = await context.newPage();
  for (const path of paths) {
    const response = await page.goto(`http://127.0.0.1:4173${path}`);
    expect(response?.status()).toBe(200);
    await expect(page.locator('h1')).toHaveCount(1);
    expect(await page.locator('article').innerText()).toContain('Sources and implementation');
    const broken = await page.locator('article a[href^="#"]').evaluateAll((links) => links.filter((link) => document.getElementById((link as HTMLAnchorElement).hash.slice(1)) === null).map((link) => link.getAttribute('href')));
    expect(broken).toEqual([]);
  }
  await context.close();
});

test('diagram phases, source navigation, history and search work', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('/');
  await page.getByRole('button', { name: 'Deliver', exact: true }).click();
  await expect(page.locator('.diagram-caption')).toContainText('alarm-driven');
  await page.getByLabel('Read about Store shards').click();
  await expect(page).toHaveURL(/\/docs\/services#store$/);
  await expect(page.locator('h1')).toHaveText('Every service, explained');
  await page.goBack();
  await expect(page.locator('h1')).toHaveText('A workflow is a log you can replay.');
  await page.getByRole('button', { name: 'Search documentation', exact: true }).click();
  await page.getByRole('searchbox').fill('writer');
  await expect(page.getByRole('dialog')).toContainText('Payloads and byte streams');
  await page.getByRole('searchbox').fill('zzzz-no-such-topic');
  await expect(page.getByRole('dialog')).toContainText('No matching topic');
  await page.keyboard.press('Escape');
  await expect(page.getByRole('dialog')).not.toBeVisible();
  expect(errors).toEqual([]);
});

test('replay, epoch controls and theme persist across navigation', async ({ page }) => {
  await page.goto('/docs/engines');
  await page.getByRole('button', { name: 'Show reads' }).click();
  await expect(page.locator('.tape .read')).toHaveCount(24);
  await page.goto('/docs/scaling');
  await page.getByLabel(/New epoch/).selectOption('64');
  await expect(page.locator('.shard-cell')).toHaveCount(64);
  await expect(page.locator('.epoch-caption')).toContainText('109 % 64 = 45');
  await page.getByRole('button', { name: 'Use dark theme' }).click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  await page.reload();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
});

test('mobile navigation works and no page overflows the viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await page.getByRole('button', { name: 'Open navigation' }).click();
  await page.getByRole('dialog').getByRole('link', { name: 'Every service, explained' }).click();
  await expect(page).toHaveURL(/\/docs\/services$/);
  await expect(page.getByRole('dialog')).not.toBeVisible();
  for (const path of paths) {
    await page.goto(path);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), path).toBe(true);
  }
});

test('unknown documentation page returns 404', async ({ page }) => {
  const response = await page.goto('/docs/does-not-exist');
  expect(response?.status()).toBe(404);
  await expect(page.getByText('Page not found', { exact: true })).toBeVisible();
});
