import { DurableObject, WorkerEntrypoint } from 'cloudflare:workers';
import { parse } from 'devalue';
import { DeliveryResult, type Delivery, type DeliveryOutcome } from '@workflow/world-cloudflare';
export { QueueShard, Runner, RunStreams, WorldStore } from '@workflow/world-cloudflare';

/** One delivery as a `_timed` or `_handoff` queue saw it. */
export type DeliveryLog = { id: string; runId: string; attempt: number; deliveryCount?: number; start: number; end: number };

type Env = { SINK: DurableObjectNamespace<Sink> };
export class Sink extends DurableObject<Env> {
  async record(id: string) {
    const entries = await this.ctx.storage.get<string[]>('received') ?? [];
    await this.ctx.storage.put('received', [...entries, id]);
  }
  async received() { return await this.ctx.storage.get<string[]>('received') ?? []; }
  async log(entry: DeliveryLog) {
    const entries = await this.ctx.storage.get<DeliveryLog[]>('log') ?? [];
    await this.ctx.storage.put('log', [...entries, entry]);
  }
  async logged() { return await this.ctx.storage.get<DeliveryLog[]>('log') ?? []; }
}
export class QueueConsumer extends WorkerEntrypoint<Env> {
  latestDeploymentId() { return 'test-v1'; }
  async consume(deliveries: Delivery[]): Promise<DeliveryOutcome[]> {
    const wake: DeliveryOutcome = { kind: DeliveryResult.Wake, seconds: 0.1 };
    if (deliveries.some((delivery) => delivery.body.queueName.endsWith('_poison'))) return deliveries.map(() => wake);
    if (deliveries.some((delivery) => delivery.body.queueName.endsWith('_retry') && delivery.attempt === 1)) {
      return deliveries.map(() => wake);
    }
    // A `_flaky` queue fails each message's first attempt and records every attempt as `<id>#<attempt>`.
    if (deliveries.some((delivery) => delivery.body.queueName.endsWith('_flaky'))) {
      for (const { id, attempt } of deliveries) await this.env.SINK.getByName('sink').record(`${id}#${attempt}`);
      return deliveries.map(({ attempt }): DeliveryOutcome => attempt === 1 ? { kind: DeliveryResult.Failed, error: 'flaky' } : { kind: DeliveryResult.Done });
    }
    // A `_timed` queue holds each delivery open for 300 ms and logs when it ran; a `_handoff` queue
    // hands each message off once, on its first delivery.
    if (deliveries.some(({ body }) => body.queueName.endsWith('_timed') || body.queueName.endsWith('_handoff'))) {
      return Promise.all(deliveries.map(async ({ id, attempt, deliveryCount, body }) => {
        const start = Date.now();
        if (body.queueName.endsWith('_timed')) await scheduler.wait(300);
        const { runId } = parse(body.encoded) as { runId: string };
        await this.env.SINK.getByName('sink').log({ id, runId, attempt, deliveryCount, start, end: Date.now() });
        const handoff = body.queueName.endsWith('_handoff') && deliveryCount === 1;
        return handoff ? { kind: DeliveryResult.Wake, seconds: 0 } : { kind: DeliveryResult.Done };
      }));
    }
    // A `_slow` queue holds its delivery open, like a long flow invocation.
    if (deliveries.some((delivery) => delivery.body.queueName.endsWith('_slow'))) await scheduler.wait(1_500);
    for (const delivery of deliveries) await this.env.SINK.getByName('sink').record(delivery.id);
    return deliveries.map(() => ({ kind: DeliveryResult.Done }));
  }
}
export default { fetch: () => new Response('ok') };
