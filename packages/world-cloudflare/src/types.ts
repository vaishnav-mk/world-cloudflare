import type { AnyEventRequest } from '@workflow/world';
import type { EventType, ResolveData } from './enums.js';
import type { QueueShard } from './queue/queue-shard.js';
import type { WorldStore } from './store/world-store.js';
import type { RunStreams } from './streams/run-streams.js';

/** Bindings the World needs. Every Worker that hosts or calls the World declares the same names. */
export type WorldEnv = {
  WORLD_STORE: DurableObjectNamespace<WorldStore>;
  QUEUE_SHARD: DurableObjectNamespace<QueueShard>;
  RUN_STREAMS: DurableObjectNamespace<RunStreams>;
  /** Service binding to the Worker's own `QueueConsumer` entrypoint. */
  CONSUMER: { consume(deliveries: Delivery[]): Promise<(number | null)[]> };
  DLQ: R2Bucket;
  DEPLOYMENT_ID: string;
  /** JSON map from deployment ID to a retained executor script name. */
  EXECUTOR_ROUTES?: string;
  QUEUE_MAX_ATTEMPTS?: string;
};

/** The `POST` export of the app's generated `.well-known/workflow/v1/flow.mjs`. */
export type FlowHandler = (request: Request) => Promise<Response>;

/** What the queue shard stores for each message. */
export type Envelope = {
  queueName: string;
  encoded: string;
  targetDeployment?: string;
  delayMs?: number;
  /** Absolute deadline written by messages queued before `delayMs` existed. */
  due?: number;
};

/** One message handed to `QueueConsumer.consume`. */
export type Delivery = { id: string; attempt: number; enqueuedAt: number; body: Envelope };

/** Props the dispatching consumer passes to a retained executor. */
export type ExecutorProps = { privateDispatch?: boolean; targetDeployment?: string };

/** Events whose commit owes a queue message, recorded in the dispatch outbox. */
export type DispatchKind = EventType.RunCreated | EventType.HookReceived | EventType.StepCreated | EventType.WaitCreated;

/** The SDK passes `resolveData` as a plain string, which an enum parameter would not accept. */
export type ResolveDataMode = `${ResolveData}`;

/** A validated event request of the given types. */
export type EventOf<T extends EventType> = Extract<AnyEventRequest, { eventType: `${T}` }>;
