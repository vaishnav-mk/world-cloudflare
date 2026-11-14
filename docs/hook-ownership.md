# Hook ownership

Hook acquisition starts with an accepted event on the run's store, not a blind registry mutation. The run store, token shard and previous owner's store still have separate transactions.

## Admission and recovery

1. Event writers share a per-run gate, including batches, unfenced writes and terminal events. Queued writers count toward the gate's lifetime. Token, disposal and evidence RPCs never enter it.
2. The actual event reducer validates a provisional grant in a rolled-back savepoint, fence first. A local transaction then reserves the hook ID, persists the request, parameters, Hook and operation ID, and arms recovery. An alarm failure rolls admission back.
3. The token shard checks the operation against the source's immutable SHA-256 argument digest. Compact receipts retain the canonical grant, conflict, refusal or identity rejection. Large metadata is not copied into permanent receipts.
4. A pending takeover reserves its token before either ordinary or forced claims can acquire it, even after the old owner releases. Transfer checks the reservation's operation identity and expected owner, or that owner's released absence. Late callbacks cannot overwrite a newer owner, remove a newer reservation or change its backoff.
5. Accepted events roll forward. Acquisition and local event/state/index/outbox commit run under the gate; intent completion commits with those local changes. Persistent intents precede later run mutations after reset. Startup schedules recovery, and alarms and `drainOutbox()` replay it. Index flushing and release delivery do not hold the run gate.

A completed-hook-ID marker prevents public retries from becoming new acquisitions after disposal. Start candidates also retain a token digest so a delayed disposal can identify an accepted candidate after its intent is gone. Argument digests, decisions, cancellation receipts and disposal tombstones remain stored indefinitely. Safe receipt collection needs a separate protocol.

## Forced disposal and start candidates

Disposal stays outside run gates so same-store and reciprocal takeovers can finish. A takeover can reach an accepted hook before it materializes locally. The owner's store records the disposal tombstone; recovery journals creation followed by disposal instead of inserting the hook. Its conditional release cannot free the next owner's token.

An accepted start candidate may not have a run yet. Disposal requires its exact accepted identity and returns no wake target until a run exists. If that candidate later materializes, its creation records the earlier disposal as an out-of-band event. If it expires first, recovery obtains a durable token-side cancellation receipt, then atomically owes its conditional release and completes the local intent. Delayed acquisitions check cancellation before mutation. Neither elapsed time alone nor a missing local hook proves that a registry grant may be released.

An accepted operation can delay that run's later writes while a peer is unavailable. Other runs have separate gates. Alarm scheduling failures propagate rather than acknowledging work without recovery. This protocol does not reconstruct historical unlogged grants or repair unrelated retention debt.

## Compatibility and rollout

Existing RPCs remain callable with their old arguments. Retained executors call `createEvent()` on the current store and do not need the internal acquisition RPC.

Use an additive receiver-first, writer-next rollout for an existing installation:

1. Install the migration, receiver methods and recovery code on all stores, including token-only and legacy stores. Preserve old pending takeover rows; their null operation identity identifies the earlier protocol. Prepare this receiver-only artifact with the old event writer and its admission helper still selected.
2. After every receiver supports `claimAcceptedHook`, `cancelAcceptedHook` and `hookClaimEvidence`, switch event writers to durable intent admission. This release contains both sides, not a rollout toggle; preparing the receiver-only artifact is an operator step.
3. Keep receiver support and receipts across later deployments. Do not roll stores back to blind acquisition or delete receipts by age.

Old direct token callers retain safe idempotent and conflict answers. They cannot initiate acquisitions without durable admission evidence; those calls return retryable 503, including fresh forced takeovers. Competing callers also get 503 while a takeover reserves their token. Old store writers can therefore need retries during the receiver-first stage. Falling back to irreversible acquisition is unsafe.

Historical orphan grants need separate owner-conditional reconciliation after old claim producers retire. No backfill is implied by this migration.

## Local proof

`test/hook-recovery.test.ts` uses RPC barriers, lost replies, object resets and explicit alarms. It covers requester validation, fence advancement, hook ID reservations, owner release during takeover, delayed callbacks, grant/commit recovery, start-candidate expiry and disposal, bounded receipts, scheduling failures and same-store/reciprocal takeovers. A paused victim-index push does not prevent the requester from cancelling. Migration tests preserve an old pending takeover during upgrade.
