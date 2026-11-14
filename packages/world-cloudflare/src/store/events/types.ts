import type { AnyEventRequest, Hook, Step, Wait, WorkflowRun } from '@workflow/world';
import type { ChangeKind, EventType, TokenClaim } from '../../enums.js';
import type { EventOf } from '../../types.js';
import type { Db } from '../db.js';
import type { IndexOutbox } from '../index-outbox.js';
import type { Outbox } from '../outbox.js';
import type { TokenOutbox } from '../token-outbox.js';

export type StepEvent = EventOf<
  EventType.StepCreated | EventType.StepStarted | EventType.StepCompleted | EventType.StepFailed | EventType.StepRetrying
>;
export type HookEvent = EventOf<EventType.HookCreated | EventType.HookReceived | EventType.HookDisposed>;
export type WaitEvent = EventOf<EventType.WaitCreated | EventType.WaitCompleted>;
export type RunUpdateEvent = EventOf<
  EventType.RunStarted | EventType.RunCompleted | EventType.RunFailed | EventType.RunCancelled | EventType.AttrSet
>;

/** Written instead of `hook_created` when another run holds the token. */
export type HookConflictEvent = {
  eventType: EventType.HookConflict;
  correlationId: string;
  specVersion: number;
  eventData: { token: string; conflictingRunId: string; forceRefusedReason?: 'victim-spec-version' };
};

/**
 * The answer to a hook-token claim. A store asks the token's shard before its transaction, because
 * a token is unique across runs. A granted forced claim's hook names the hook it took the token
 * from in `claimedFrom`.
 */
export type Tokens =
  | { kind: TokenClaim.Granted; hook: Hook }
  | { kind: TokenClaim.Held; holderRunId: string }
  | { kind: TokenClaim.Refused; holderRunId: string }
  | { kind: TokenClaim.None };

/** What a token's shard answers a claim with. */
export type TokenAnswer = Exclude<Tokens, { kind: TokenClaim.None }>;

/** Everything one event write needs. It all runs inside a single synchronous transaction. */
export type Tx = {
  db: Db;
  outbox: Outbox;
  releases: TokenOutbox;
  index: IndexOutbox;
  tokens: Tokens;
  runId: string;
  now: Date;
  specVersion: number;
};

/**
 * What an event writes besides its run, before it is written. Each kind carries exactly the
 * entities it touches, so writers never test for optional fields.
 */
export type Change =
  | { kind: ChangeKind.Run; run: WorkflowRun }
  | { kind: ChangeKind.Step; run: WorkflowRun; step: Step }
  | {
    kind: ChangeKind.LazyStepStart;
    run: WorkflowRun;
    step: Step;
    /** Journaled first; it holds the input. */
    implied: EventOf<EventType.StepCreated>;
    /** The `step_started` as journaled, without the input. */
    journal: AnyEventRequest;
  }
  | { kind: ChangeKind.HookCreated; run: WorkflowRun; hook: Hook }
  | { kind: ChangeKind.HookConflict; run: WorkflowRun; conflict: HookConflictEvent }
  | { kind: ChangeKind.HookDisposed; run: WorkflowRun; hook: Hook }
  | { kind: ChangeKind.Wait; run: WorkflowRun; wait: Wait; correlationId: string }
  | { kind: ChangeKind.StepAppend; run: WorkflowRun; step: Step }
  | { kind: ChangeKind.WaitAppend; run: WorkflowRun; wait: Wait; correlationId: string };
