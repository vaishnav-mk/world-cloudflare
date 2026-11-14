import type { AnyEventRequest, Hook } from '@workflow/world';
import type { DeliveryResult, EventType, MessageKind, ResolveData } from './enums.js';
import type { QueueShard } from './queue/queue-shard.js';
import type { WorldStore } from './store/world-store.js';
import type { RunStreams } from './streams/run-streams.js';

/** Bindings the World needs. Every Worker that hosts or calls the World declares the same names. */
export type WorldEnv = {
  WORLD_STORE: DurableObjectNamespace<WorldStore>;
  QUEUE_SHARD: DurableObjectNamespace<QueueShard>;
  RUN_STREAMS: DurableObjectNamespace<RunStreams>;
  /** Service binding to the main Worker's `QueueConsumer` entrypoint, from it and from its retained executors. */
  CONSUMER: { consume(deliveries: Delivery[]): Promise<DeliveryOutcome[]>; latestDeploymentId(): Promise<string> };
  DLQ: R2Bucket;
  /** Payloads and stream chunks too large to keep in a Durable Object, keyed `runs/<runId>/<id>`. */
  PAYLOADS: R2Bucket;
  DEPLOYMENT_ID: string;
  /** JSON map from deployment ID to a retained executor script name. */
  EXECUTOR_ROUTES?: string;
  QUEUE_MAX_ATTEMPTS?: string;
  /** JSON list of `ShardEpoch`, oldest first. Append an epoch starting in the future to add shards. */
  SHARD_EPOCHS?: string;
  /** 32 random bytes as base64. When set, the SDK encrypts payloads and VM snapshots with per-run keys derived from it. */
  WORKFLOW_ENCRYPTION_SECRET?: string;
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
  /** Set on a copy of a delivery a reset cut off: the message it continues, whose ID it keeps. */
  messageId?: string;
};

/** The outcome of one delivery, returned by `QueueConsumer.consume` in delivery order. */
export type DeliveryOutcome =
  | { kind: DeliveryResult.Done }
  | { kind: DeliveryResult.Wake; seconds: number }
  | { kind: DeliveryResult.Failed; error: string };

/**
 * One message handed to `QueueConsumer.consume`. `id` is stable across redeliveries;
 * `deliveryCount` is 1 only on the message's first delivery, and `attempt` leaves out handoffs.
 */
export type Delivery = { id: string; attempt: number; deliveryCount?: number; enqueuedAt: number; body: Envelope };

/** The outbox row a queue message settles once the queue accepts it. */
export type OutboxClaim =
  | { kind: MessageKind.Step; runId: string; stepId: string; stepName: string }
  | { kind: MessageKind.Start; runId: string }
  | { kind: MessageKind.HookWake; runId: string }
  | { kind: MessageKind.WaitContinuation; runId: string; correlationId: string };

/** One message for `QueueShard.enqueueMany`, as `enqueue` takes it. */
export type EnqueueRequest = {
  id: string; queueName: string; encoded: string; delayMs: number;
  targetDeployment?: string; deduplicationKey?: string; payloadDigest?: string;
  /** The message a copy continues; see `Envelope.messageId`. */
  messageId?: string;
};

/** `QueueShard.enqueueMany`'s outcome for one message. */
export type EnqueueResult = { id: string; fresh: boolean } | { error: string };
/** From `from` (ms since the epoch, run ULID time), new runs use `store` store shards and `queue` queue shards. */
export type ShardEpoch = { from: number; store: number; queue: number };
/** A queue message that settles an outbox row, routed through its run's store. */
export type QueueDispatch = { request: EnqueueRequest; claim: OutboxClaim; shard: string };

/**
 * A release a store sends a token's shard: the hook was disposed, which frees the token now, or its
 * run finished, which frees it once the hook's retention ends.
 */
export type TokenRelease = { token: string; hookId: string; disposed: boolean };

/** A token a legacy hook holds, as the legacy store hands it to the token's shard. */
export type HeldToken = { hook: Hook; finished: boolean };

/** Props the dispatching consumer passes to a retained executor. */
export type ExecutorProps = { privateDispatch?: boolean; targetDeployment?: string };

/** Events whose commit owes a queue message, recorded in the dispatch outbox. */
export type DispatchKind = EventType.RunCreated | EventType.HookReceived | EventType.StepCreated | EventType.WaitCreated;

/**
 * The SDK passes `resolveData` as a plain string, which an enum parameter would not accept. Entity
 * reads take `none` or `all`; only event reads take `skip-step-inputs`.
 */
export type ResolveDataMode = `${ResolveData.None | ResolveData.All}`;

/** A validated event request of the given types. */
export type EventOf<T extends EventType> = Extract<AnyEventRequest, { eventType: `${T}` }>;
