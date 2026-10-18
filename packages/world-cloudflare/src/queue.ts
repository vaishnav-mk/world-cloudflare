import { DurableObject } from 'cloudflare:workers';
import { DurabilityFanout, DurabilityScheduler, type DurabilityFanoutMessage } from 'durability';
import { parse, stringify } from 'devalue';
import {
  MessageId, QueuePrefix, ValidQueueName,
  type Queue, type QueuePayload,
} from '@workflow/world';
import type { WorldStore } from './store.js';

type Envelope = {
  queueName: string;
  encoded: string;
  targetDeployment?: string;
  delayMs?: number;
  due?: number; // Queued before delayMs replaced the absolute deadline.
};
type Delivery = { id: string; attempt: number; enqueuedAt: number; body: Envelope };
type DedupRow = {
  id: string;
  queue_name: string;
  encoded: string | null;
  payload_digest: string;
  delay_ms: number;
  target_deployment: string | null;
  accepted: number;
};
export type QueueEnv = {
  QUEUE_SHARD: DurableObjectNamespace<QueueShard>;
  WORLD_STORE: DurableObjectNamespace<WorldStore>;
  CONSUMER: { consume(deliveries: Delivery[]): Promise<(number | null)[]> };
  DLQ: R2Bucket;
  DEPLOYMENT_ID: string;
  EXECUTOR_ROUTES?: string;
  QUEUE_MAX_ATTEMPTS?: string;
};

export function executorFor(env: Pick<QueueEnv, 'EXECUTOR_ROUTES'>, deploymentId: string): string | undefined {
  if (!env.EXECUTOR_ROUTES) return undefined;
  const routes: unknown = JSON.parse(env.EXECUTOR_ROUTES);
  if (!routes || typeof routes !== 'object' || Array.isArray(routes)) throw new Error('Invalid executor registry');
  const script = Object.hasOwn(routes, deploymentId)
    ? (routes as Record<string, unknown>)[deploymentId] : undefined;
  return typeof script === 'string' && /^[a-zA-Z0-9_-]{1,63}$/.test(script) ? script : undefined;
}

export class QueueShard extends DurableObject<QueueEnv> {
  constructor(ctx: DurableObjectState, env: QueueEnv) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS queue_dedup (
      key TEXT PRIMARY KEY, id TEXT NOT NULL, queue_name TEXT NOT NULL,
      encoded TEXT, payload_digest TEXT NOT NULL, delay_ms INTEGER NOT NULL,
      target_deployment TEXT, accepted INTEGER NOT NULL DEFAULT 0
    )`);
    if (!this.ctx.storage.sql.exec<{ name: string }>('PRAGMA table_info(queue_dedup)').toArray()
      .some(({ name }) => name === 'encoded')) {
      this.ctx.storage.sql.exec('ALTER TABLE queue_dedup ADD COLUMN encoded TEXT');
    }
  }

  private readonly scheduler = new DurabilityScheduler({ context: this.ctx });
  private readonly fanout = new DurabilityFanout<Envelope>({
    scheduler: this.scheduler,
    retries: { maxAttempts: Number(this.env.QUEUE_MAX_ATTEMPTS ?? 64), delay: (attempt) => Math.min(30_000, attempt * 1000) },
    targets: {
      flow: {
        ackOnReturn: true,
        deliver: async (messages) => {
          const due: DurabilityFanoutMessage<Envelope>[] = [];
          for (const message of messages) {
            const deadline = message.body.due ?? (message.enqueuedAt + (message.body.delayMs ?? Number.NaN));
            if (!Number.isFinite(deadline)) throw new Error('Invalid persisted queue deadline');
            const remaining = deadline - Date.now();
            if (remaining > 0) await message.retry(remaining);
            else due.push(message);
          }
          if (!due.length) return;
          const deliveries: Delivery[] = due.map(({ id, attempt, enqueuedAt, body }) => ({ id, attempt, enqueuedAt, body }));
          const results = await this.env.CONSUMER.consume(deliveries);
          if (results.length !== due.length) throw new Error('Incomplete queue delivery response');
          for (let i = 0; i < due.length; i++) {
            const timeout = results[i];
            if (typeof timeout === 'number') await due[i].retry(timeout * 1000);
          }
        },
      },
    },
    dlq: async (message) => {
      await this.env.DLQ.put(`workflow/${message.id}`, JSON.stringify(message), {
        onlyIf: { etagDoesNotMatch: '*' },
      });
      // A queue-accepted wake is not delivered when its retries exhaust.
      let payload: unknown;
      try { payload = parse(message.body.encoded); }
      catch (error) {
        console.error('Cannot recover corrupt DLQ payload', { messageId: message.id, error });
        return;
      }
      if (payload && typeof payload === 'object' && 'runId' in payload && typeof payload.runId === 'string') {
        const store = this.env.WORLD_STORE.getByName('global');
        if ('stepId' in payload && typeof payload.stepId === 'string') {
          if ('stepName' in payload && typeof payload.stepName === 'string') {
            await store.reopenPublishedStepDispatch(payload.runId, payload.stepId, payload.stepName);
          }
        } else if ('waitContinuation' in payload) {
          const continuation = payload.waitContinuation;
          if (continuation && typeof continuation === 'object' && 'correlationId' in continuation &&
              typeof continuation.correlationId === 'string') {
            await store.reopenPublishedWaitDispatch(payload.runId, continuation.correlationId);
          }
        } else {
          await store.reopenPublishedHookWakes(payload.runId);
        }
      }
    },
  });

  async enqueue(
    id: string, queueName: string, encoded: string, delayMs: number,
    targetDeployment?: string, deduplicationKey?: string, payloadDigest?: string,
  ) {
    let fresh = true;
    if (deduplicationKey) {
      if (!payloadDigest) throw new Error('Missing queue payload digest');
      const sql = this.ctx.storage.sql;
      const existing = sql.exec<DedupRow>('SELECT * FROM queue_dedup WHERE key = ?', deduplicationKey).toArray()[0];
      if (existing) {
        fresh = false;
        // First accepted message wins even if retry telemetry or requestedAt changed.
        if (existing.accepted) return { id: existing.id, fresh: false };
        if (existing.encoded === null && existing.payload_digest !== payloadDigest) {
          throw new Error('Cannot replay a pre-upgrade queue claim with different bytes');
        }
        id = existing.id;
        encoded = existing.encoded ?? encoded;
        delayMs = existing.delay_ms;
        targetDeployment = existing.target_deployment ?? undefined;
      } else {
        sql.exec('INSERT INTO queue_dedup (key,id,queue_name,encoded,payload_digest,delay_ms,target_deployment) VALUES (?,?,?,?,?,?,?)',
          deduplicationKey, id, queueName, encoded, payloadDigest, delayMs, targetDeployment ?? null);
      }
    }
    const result = await this.fanout.enqueue({ id, body: { queueName, encoded, delayMs, targetDeployment }, deduplicationKey });
    if (!result.success) throw new Error(result.error.message);
    if (deduplicationKey) this.ctx.storage.sql.exec('UPDATE queue_dedup SET accepted = 1 WHERE key = ?', deduplicationKey);
    return { id: result.value[0], fresh };
  }

  alarm(info?: AlarmInvocationInfo) {
    return this.scheduler.alarm(info);
  }
}

export function createQueue(env: QueueEnv): Queue {
  return {
    getDeploymentId: async () => env.DEPLOYMENT_ID,
    queue: async (queueName, message, opts) => {
      ValidQueueName.parse(queueName);
      if (opts?.deploymentId && opts.deploymentId !== env.DEPLOYMENT_ID &&
          !executorFor(env, opts.deploymentId)) {
        throw new Error(`Deployment ${opts.deploymentId} is unavailable`);
      }
      const delay = opts?.delaySeconds ?? 0;
      if (!Number.isFinite(delay) || delay < 0) throw new RangeError('Invalid queue delay');
      const key = opts?.idempotencyKey ? `${queueName}:${opts.idempotencyKey}` : undefined;
      const store = env.WORLD_STORE.getByName('global');
      const runMessage = '__healthCheck' in message ? undefined : message;
      const stepSlot = runMessage?.stepId && runMessage.stepName
        ? await store.pendingStepDispatchSlot(runMessage.runId, runMessage.stepId, runMessage.stepName) : 0;
      const startSlot = !runMessage?.stepId && runMessage?.runInput
        ? await store.pendingRunStartSlot(runMessage.runId) : 0;
      const wakeSlot = !runMessage?.stepId && runMessage?.hookResumeTiming
        ? await store.pendingHookWakeSlot(runMessage.runId) : 0;
      const waitSlot = !runMessage?.stepId && !runMessage?.runInput && !runMessage?.hookResumeTiming && runMessage?.waitContinuation
        ? await store.pendingWaitDispatchSlot(runMessage.runId, runMessage.waitContinuation.correlationId) : 0;
      const id = key
        ? `qmsg_${Buffer.from(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(key))).toString('hex')}`
        : crypto.randomUUID();
      const encoded = stringify(message);
      const digest = key
        ? Buffer.from(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(encoded))).toString('hex')
        : undefined;
      // shortcut: one queue shard; partition by stable run/key after measuring delivery throughput.
      const result = await env.QUEUE_SHARD.getByName('flow').enqueue(
        id, queueName, encoded, Math.ceil(delay * 1000), opts?.deploymentId, key, digest,
      );
      if (result.fresh && runMessage) {
        if (stepSlot) await store.markStepDispatchPublished(runMessage.runId, stepSlot);
        if (startSlot) await store.markRunStartsPublished(runMessage.runId, startSlot);
        if (wakeSlot) await store.markHookWakesPublished(runMessage.runId, wakeSlot);
        if (waitSlot) await store.markWaitDispatchPublished(runMessage.runId, waitSlot);
      }
      return { messageId: MessageId.parse(result.id) };
    },
    createQueueHandler: (prefix, handler) => async (request) => {
      const queueName = request.headers.get('x-vqs-queue-name');
      const messageId = request.headers.get('x-vqs-message-id');
      const attempt = Number(request.headers.get('x-vqs-message-attempt'));
      if (!queueName || !ValidQueueName.safeParse(queueName).success || !queueName.startsWith(QueuePrefix.parse(prefix)) ||
          !messageId || !Number.isSafeInteger(attempt) || attempt < 1) {
        return Response.json({ error: 'Invalid queue delivery' }, { status: 400 });
      }
      try {
        const payload = parse(await request.text()) as QueuePayload;
        const result = await handler(payload, {
          queueName: ValidQueueName.parse(queueName), messageId: MessageId.parse(messageId), attempt,
          deliveryCount: attempt,
          createdAt: new Date(Number(request.headers.get('x-vqs-created-at'))),
        });
        if (typeof result === 'object' && result !== null && 'timeoutSeconds' in result) {
          return Response.json({ timeoutSeconds: result.timeoutSeconds });
        }
        return Response.json({ ok: true });
      } catch (error) {
        console.error('Workflow delivery failed', error);
        return Response.json({ error: 'Delivery failed' }, { status: 500 });
      }
    },
  };
}

export type { Delivery };
