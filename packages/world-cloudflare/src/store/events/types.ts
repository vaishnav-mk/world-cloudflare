import type { AnyEventRequest, Hook, Step, Wait, WorkflowRun } from '@workflow/world';
import type { EventType } from '../../enums.js';
import type { EventOf } from '../../types.js';
import type { Db } from '../db.js';
import type { Outbox } from '../outbox.js';

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
  eventData: { token: string; conflictingRunId: string };
};

/** Everything one event write needs. It all runs inside a single synchronous transaction. */
export type Tx = {
  db: Db;
  outbox: Outbox;
  runId: string;
  now: Date;
  specVersion: number;
};

/** The state an event leaves behind, before it is written. */
export type Change = {
  run: WorkflowRun;
  step?: Step;
  /** Set only by `hook_created`. */
  createdHook?: Hook;
  wait?: Wait;
  /** An event this one implies, journaled first: `step_created` for a lazy `step_started`. */
  implied?: AnyEventRequest;
  stepCreated?: true;
  /** Set when `hook_created` lost its token to another run. */
  hookConflict?: HookConflictEvent;
};
