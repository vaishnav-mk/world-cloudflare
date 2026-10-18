import { resumeHook } from 'workflow/api';
import { readJson } from '../http.js';
import type { RouteContext } from '../router.js';

/** `POST /hooks/:token`: resumes the run waiting on this hook token with the JSON body. */
export async function resumeHookByToken({ request, params }: RouteContext): Promise<Response> {
  return Response.json(await resumeHook(params.token, await readJson(request)));
}
