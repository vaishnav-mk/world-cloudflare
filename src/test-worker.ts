import { DurableObject, WorkerEntrypoint } from 'cloudflare:workers';
import { DeliveryResult, type Delivery, type DeliveryOutcome } from '@workflow/world-cloudflare';
export { QueueShard, RunStreams, WorldStore } from '@workflow/world-cloudflare';

type Env = { SINK: DurableObjectNamespace<Sink> };
export class Sink extends DurableObject<Env> {
  async record(id: string) {
    const entries = await this.ctx.storage.get<string[]>('received') ?? [];
    await this.ctx.storage.put('received', [...entries, id]);
  }
  async received() { return await this.ctx.storage.get<string[]>('received') ?? []; }
}
export class QueueConsumer extends WorkerEntrypoint<Env> {
  async consume(deliveries: Delivery[]): Promise<DeliveryOutcome[]> {
    const wake: DeliveryOutcome = { kind: DeliveryResult.Wake, seconds: 0.1 };
    if (deliveries.some((delivery) => delivery.body.queueName.endsWith('_poison'))) return deliveries.map(() => wake);
    if (deliveries.some((delivery) => delivery.body.queueName.endsWith('_retry') && delivery.attempt === 1)) {
      return deliveries.map(() => wake);
    }
    // A `_slow` queue holds its delivery open, like a long flow invocation.
    if (deliveries.some((delivery) => delivery.body.queueName.endsWith('_slow'))) await scheduler.wait(1_500);
    for (const delivery of deliveries) await this.env.SINK.getByName('sink').record(delivery.id);
    return deliveries.map(() => ({ kind: DeliveryResult.Done }));
  }
}
export default { fetch: () => new Response('ok') };
