import { createQueueConsumer } from '@workflow/world-cloudflare';
// Loaded before `./dispatcher.js` registers the World: loading the bundle after it lost VM snapshots in tests.
import { POST as flow } from '../.well-known/workflow/v1/flow.mjs';

export { default, QueueShard, Runner, RunStreams, WorldStore } from './dispatcher.js';
export const QueueConsumer = createQueueConsumer(flow);
