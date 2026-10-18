import { getRun, start, resumeHook } from 'workflow/api';
import { HookNotFoundError, WorkflowRunNotFoundError } from '@workflow/errors';
import { eventIdToSlot, EventTypeSchema } from '@workflow/world';
import { setWorld } from 'workflow/runtime';
import { POST as flowPOST } from '../.well-known/workflow/v1/flow.mjs';
import { approval, delayed, demo, failPermanently, longDelay, progress, retryOnce } from '../workflows/demo.js';
import { createQueueConsumer, createWorld, drainOutbox, type WorldEnv } from '@workflow/world-cloudflare';

export { QueueShard, RunStreams, WorldStore } from '@workflow/world-cloudflare';
export const QueueConsumer = createQueueConsumer(flowPOST);

const world = createWorld();
setWorld(world);

const examples = {
  demo: Object.assign(demo, { workflowId: 'workflow//./workflows/demo//demo' }),
  delayed: Object.assign(delayed, { workflowId: 'workflow//./workflows/demo//delayed' }),
  longDelay: Object.assign(longDelay, { workflowId: 'workflow//./workflows/demo//longDelay' }),
  approval: Object.assign(approval, { workflowId: 'workflow//./workflows/demo//approval' }),
  progress: Object.assign(progress, { workflowId: 'workflow//./workflows/demo//progress' }),
  retry: Object.assign(retryOnce, { workflowId: 'workflow//./workflows/demo//retryOnce' }),
  fatal: Object.assign(failPermanently, { workflowId: 'workflow//./workflows/demo//failPermanently' }),
};

async function readJsonWithinLimit(request: Request): Promise<{ value: unknown } | Response> {
  if (Number(request.headers.get('content-length') ?? 0) > 1_048_576) {
    return new Response('Request too large', { status: 413 });
  }
  const reader = request.body?.getReader();
  if (!reader) return new Response('Invalid JSON', { status: 400 });
  try {
    const chunks: Uint8Array[] = [];
    let length = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > 1_048_576) {
        await reader.cancel().catch(() => {});
        return new Response('Request too large', { status: 413 });
      }
      chunks.push(value);
    }
    const body = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
    return { value: JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(body)) };
  } catch {
    return new Response('Invalid JSON', { status: 400 });
  } finally {
    reader.releaseLock();
  }
}

export default {
  async scheduled(_controller: ScheduledController, env: WorldEnv): Promise<void> {
    await drainOutbox(env);
  },
  async fetch(request: Request, env: WorldEnv): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    if (path === '/runs' && request.method === 'POST') {
      const parsed = await readJsonWithinLimit(request);
      if (parsed instanceof Response) return parsed;
      const payload = parsed.value;
      if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return new Response('Invalid JSON', { status: 400 });
      const { value, name: requestedName } = payload as { value?: unknown; name?: unknown };
      if (typeof value !== 'number' || !Number.isFinite(value)) return new Response('Invalid value', { status: 400 });
      const name = requestedName ?? 'demo';
      if (typeof name !== 'string' || !Object.hasOwn(examples, name)) return new Response('Unknown workflow', { status: 400 });
      const run = await start(examples[name as keyof typeof examples], [value]);
      return Response.json({ runId: run.runId }, { status: 202 });
    }
    const run = path.match(/^\/runs\/([^/]+)$/);
    if (run && request.method === 'GET') {
      try { return Response.json(await world.runs.get(run[1])); }
      catch (error) {
        if (WorkflowRunNotFoundError.is(error)) return new Response('Run not found', { status: 404 });
        throw error;
      }
    }
    const result = path.match(/^\/runs\/([^/]+)\/result$/);
    if (result && request.method === 'GET') {
      try { return Response.json({ value: await getRun(result[1]).returnValue }); }
      catch (error) {
        if (WorkflowRunNotFoundError.is(error)) return new Response('Run not found', { status: 404 });
        throw error;
      }
    }
    const hooks = path.match(/^\/runs\/([^/]+)\/hooks$/);
    if (hooks && request.method === 'GET') {
      return Response.json(await world.hooks.list({ runId: hooks[1] }));
    }
    const subscription = path.match(/^\/runs\/([^/]+)\/events$/);
    if (subscription && request.method === 'GET') {
      try { await world.runs.get(subscription[1], { resolveData: 'none' }); }
      catch (error) {
        if (WorkflowRunNotFoundError.is(error)) return new Response('Run not found', { status: 404 });
        throw error;
      }
      const cursorText = url.searchParams.get('cursor') ?? request.headers.get('last-event-id') ?? '0';
      let cursor: number;
      const parsedCursor = cursorText === '0' ? 0 : eventIdToSlot(cursorText);
      if (parsedCursor === null) return new Response('Invalid event cursor', { status: 400 });
      cursor = parsedCursor;
      const requestedTypes = url.searchParams.get('types')?.split(',').filter(Boolean);
      if (requestedTypes && requestedTypes.some((type) => !EventTypeSchema.safeParse(type).success)) {
        return new Response('Invalid event type', { status: 400 });
      }
      const stub = env.WORLD_STORE.getByName('global');
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        async pull(controller) {
          try {
            const page = await stub.nextEvents(subscription[1], cursor, requestedTypes);
            for (const event of page.events) {
              controller.enqueue(encoder.encode(`id: ${event.eventId}\nevent: ${event.eventType}\ndata: ${event.data}\n\n`));
            }
            cursor = page.cursor;
            if (page.done) controller.close();
            else if (!page.events.length) controller.enqueue(encoder.encode(': keepalive\n\n'));
          } catch (error) { controller.error(error); }
        },
      }), { headers: { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache' } });
    }
    const streamList = path.match(/^\/runs\/([^/]+)\/streams$/);
    if (streamList && request.method === 'GET') {
      return Response.json({ names: await world.streams.list(streamList[1]) });
    }
    const stream = path.match(/^\/runs\/([^/]+)\/streams\/([^/]+)$/);
    if (stream && request.method === 'GET') {
      try { await world.runs.get(stream[1], { resolveData: 'none' }); }
      catch (error) {
        if (WorkflowRunNotFoundError.is(error)) return new Response('Run not found', { status: 404 });
        throw error;
      }
      return new Response(getRun(stream[1]).getReadable<Uint8Array>({ namespace: stream[2] }), {
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    const hook = path.match(/^\/hooks\/([^/]+)$/);
    if (hook && request.method === 'POST') {
      const parsed = await readJsonWithinLimit(request);
      if (parsed instanceof Response) return parsed;
      try { return Response.json(await resumeHook(hook[1], parsed.value)); }
      catch (error) {
        if (HookNotFoundError.is(error)) return new Response('Hook not found', { status: 404 });
        throw error;
      }
    }
    return new Response('Not found', { status: 404 });
  },
};
