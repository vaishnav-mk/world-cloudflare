import { eventIdToSlot, EventTypeSchema } from '@workflow/world';
import { nextEvents, ResolveData } from '@workflow/world-cloudflare';
import { HttpError } from '../http.js';
import type { RouteContext } from '../router.js';
import { world } from '../world.js';

const encoder = new TextEncoder();

/**
 * `GET /runs/:runId/events`: server-sent events. Replays the run's log, then follows new commits
 * and closes once the run finishes. Resume with `?cursor=evnt_…` or `Last-Event-ID`; filter with
 * `?types=run_completed,run_failed`.
 */
export async function subscribeEvents({ request, url, env, params }: RouteContext): Promise<Response> {
  const { runId } = params;
  await world.runs.get(runId, { resolveData: ResolveData.None });
  let cursor = parseCursor(url.searchParams.get('cursor') ?? request.headers.get('last-event-id') ?? '0');
  const types = parseTypes(url.searchParams.get('types'));
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const page = await nextEvents(env, runId, cursor, types);
        for (const event of page.events) {
          controller.enqueue(encoder.encode(`id: ${event.eventId}\nevent: ${event.eventType}\ndata: ${event.data}\n\n`));
        }
        cursor = page.cursor;
        if (page.done) controller.close();
        else if (!page.events.length) controller.enqueue(encoder.encode(': keepalive\n\n'));
      } catch (error) {
        controller.error(error);
      }
    },
  });
  return new Response(body, { headers: { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache' } });
}

function parseCursor(text: string): number {
  const slot = text === '0' ? 0 : eventIdToSlot(text);
  if (slot === null) throw new HttpError(400, 'Invalid event cursor');
  return slot;
}

function parseTypes(text: string | null): string[] | undefined {
  const types = text?.split(',').filter(Boolean);
  if (types?.some((type) => !EventTypeSchema.safeParse(type).success)) throw new HttpError(400, 'Invalid event type');
  return types;
}
