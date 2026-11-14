/** SDK event types this World handles by name. Values are the SDK's `eventType` strings. */
export enum EventType {
  RunCreated = 'run_created',
  RunStarted = 'run_started',
  RunCompleted = 'run_completed',
  RunFailed = 'run_failed',
  RunCancelled = 'run_cancelled',
  AttrSet = 'attr_set',
  StepCreated = 'step_created',
  StepStarted = 'step_started',
  StepCompleted = 'step_completed',
  StepFailed = 'step_failed',
  StepRetrying = 'step_retrying',
  HookCreated = 'hook_created',
  HookReceived = 'hook_received',
  HookDisposed = 'hook_disposed',
  HookConflict = 'hook_conflict',
  WaitCreated = 'wait_created',
  WaitCompleted = 'wait_completed',
}

export enum RunStatus {
  Pending = 'pending',
  Running = 'running',
  Completed = 'completed',
  Failed = 'failed',
  Cancelled = 'cancelled',
}

export enum StepStatus {
  Pending = 'pending',
  Running = 'running',
  Completed = 'completed',
  Failed = 'failed',
  Cancelled = 'cancelled',
}

export enum WaitStatus {
  Waiting = 'waiting',
  Completed = 'completed',
}

/** Whether a list or read returns payloads (`all`) or metadata only (`none`). */
export enum ResolveData {
  None = 'none',
  All = 'all',
  /** Events only: as `all`, but `step_created` may leave out its input, which replay never reads. */
  SkipStepInputs = 'skip-step-inputs',
}

export enum SortOrder {
  Asc = 'asc',
  Desc = 'desc',
}

/** Lifecycle of a dispatch outbox row. */
export enum OutboxStatus {
  Pending = 'pending',
  Published = 'published',
}

/** What the outbox drain does with a due row. */
export enum DispatchAction {
  /** The message is no longer needed. */
  Settle = 'settle',
  /** Not yet: the run has not started. */
  Postpone = 'postpone',
  Publish = 'publish',
}

/** The run wake a queue message carries, used to settle or reopen its outbox row. */
export enum MessageKind {
  Start = 'start',
  HookWake = 'hook-wake',
  Step = 'step',
  WaitContinuation = 'wait',
}

/** Result of claiming a queue idempotency key. */
export enum ClaimStatus {
  /** First use of the key. */
  New = 'new',
  /** Claimed before but never accepted by the fanout: replay the stored message. */
  Replay = 'replay',
  /** Already accepted: the first message wins. */
  Accepted = 'accepted',
}

/** What an event changes besides its run. Each kind carries exactly the entities it writes. */
export enum ChangeKind {
  /** Only the run changes, if anything. */
  Run = 'run',
  Step = 'step',
  /** A lazy `step_started` creates and starts its step in one write. */
  LazyStepStart = 'lazy-step-start',
  HookCreated = 'hook-created',
  /** Another run holds the token: journal `hook_conflict` instead. */
  HookConflict = 'hook-conflict',
  HookDisposed = 'hook-disposed',
  Wait = 'wait',
  /** Spec 9: a step event is an append; the step record follows it but is not returned. */
  StepAppend = 'step-append',
  /** Spec 9: a wait event is an append; the wait record follows it but is not returned. */
  WaitAppend = 'wait-append',
}

/** How `hook_created` learns whether its token, unique across runs, is free. */
export enum TokenClaim {
  /** The token's shard granted the token to this hook. */
  Granted = 'granted',
  /** The token's shard reports that another run holds the token. */
  Held = 'held',
  /** A forced claim the holder's run cannot take: it predates forced disposals, so it would never read one. */
  Refused = 'refused',
  /** No claim was made, so the write cannot create a hook. */
  None = 'none',
}

/** What one flow delivery asks of the queue shard. */
export enum DeliveryResult {
  Done = 'done',
  /** The SDK asked to be woken again after a delay. */
  Wake = 'wake',
  Failed = 'failed',
}

/** Where a store's one-time backfill of the index shards stands. */
export enum BackfillStage {
  /** Owing the index its runs, a batch per alarm. */
  Scanning = 'scanning',
  /** Every run owed; it reports once they are pushed. */
  Scanned = 'scanned',
  /** Every index shard it feeds knows it is backfilled. */
  Reported = 'reported',
}
