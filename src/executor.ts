import { createExecutor, createWorld, registerWorld } from 'world-cloudflare';
import { POST as flowPOST } from '../.well-known/workflow/v1/flow.mjs';

registerWorld(createWorld());

export default createExecutor(flowPOST);
