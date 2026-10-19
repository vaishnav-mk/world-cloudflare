import { handleWorldRpc } from '@workflow/world-cloudflare';
import type { RouteContext } from '../router.js';
import { world } from '../world.js';

/** `POST /world/rpc`: the World over HTTP for `workflow web`. Disabled unless `WORLD_RPC_TOKEN` is set. */
export function worldRpc({ request, env }: RouteContext): Promise<Response> {
  return handleWorldRpc(request, world, env.WORLD_RPC_TOKEN);
}
