import { createWorld, registerWorld } from 'world-cloudflare';

/** The World this Worker uses, registered with the SDK once per isolate. */
export const world = createWorld();
registerWorld(world);
