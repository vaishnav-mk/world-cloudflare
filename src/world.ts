import { setWorld } from 'workflow/runtime';
import { createWorld } from '@workflow/world-cloudflare';

/** The World this Worker uses, registered with the SDK once per isolate. */
export const world = createWorld();
setWorld(world);
