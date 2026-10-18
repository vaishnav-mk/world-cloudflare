import { setWorld } from 'workflow/runtime';
import { createExecutor, createWorld } from '@workflow/world-cloudflare';
import { POST as flowPOST } from '../.well-known/workflow/v1/flow.mjs';

setWorld(createWorld());

export default createExecutor(flowPOST);
