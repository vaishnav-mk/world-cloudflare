import { DurableObject, WorkerEntrypoint } from 'cloudflare:workers';
import type { Delivery } from '@workflow/world-cloudflare';
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
  async consume(deliveries: Delivery[]): Promise<(number | null)[]> {
    if (deliveries.some((delivery) => delivery.body.queueName.endsWith('_poison'))) {
      return deliveries.map(() => 0.1);
    }
    if (deliveries.some((delivery) => delivery.body.queueName.endsWith('_retry') && delivery.attempt === 1)) {
      return deliveries.map(() => 0.1);
    }
    for (const delivery of deliveries) await this.env.SINK.getByName('sink').record(delivery.id);
    return deliveries.map(() => null);
  }
}
export default { fetch: () => new Response('ok') };
