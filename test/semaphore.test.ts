import { expect, it } from 'vitest';
import { Semaphore } from '../packages/world-cloudflare/src/lib/semaphore.js';

it('runs at most its permits at once, and the rest in arrival order', async () => {
  const semaphore = new Semaphore(2);
  let running = 0;
  let peak = 0;
  const order: number[] = [];
  await Promise.all(Array.from({ length: 5 }, (_, index) => semaphore.run(async () => {
    peak = Math.max(peak, ++running);
    order.push(index);
    await new Promise((resolve) => setTimeout(resolve, 10));
    running--;
  })));
  expect(peak).toBe(2);
  expect(order).toEqual([0, 1, 2, 3, 4]);
});

it('frees a permit when its work throws', async () => {
  const semaphore = new Semaphore(1);
  await expect(semaphore.run(async () => { throw new Error('boom'); })).rejects.toThrow('boom');
  expect(await semaphore.run(async () => 'next')).toBe('next');
});
