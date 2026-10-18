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
