# Workflow SDK v5 World parity ledger

Target: the pinned `@workflow/world` 5.x interface in `.vendor/workflow-sdk/packages/world/src/interfaces.ts` and the capabilities currently advertised by `.vendor/workflow-sdk/packages/world-vercel/src/index.ts`. A passing typecheck is not behavioral conformance. `docs/staging-e2e.md` records tested remote behavior. Do not mark an item complete without a workerd contract test and, where it involves delivery or resumption, a remote staging E2E test.

| Contract | Current state | Required gate |
| --- | --- | --- |
| Run, step, hook, event get/list; atomic event and entity update | Basic paths pass; no full conformance suite | Port World testing fixtures into workerd, including pagination, terminal races, retention, `resolveData` |
| Dense per-run event IDs and stale-writer reports | Workerd tests pass | Concurrent writers, cursor snapshots, synthetic events, protocol-version matrix |
| `run_created(null)` and old-run spec stamping | Workerd tests pass locally; not yet deployed to staging | Remote run creation with null ID and replay under mixed spec versions |
| Queue dispatch, delay, retry, message identity, idempotency | SDK E2E passes; registered keys dedup after ack locally | Fault injection between claim/fanout/accepted; timed waits beyond a day; general DLQ/redrive; replay after duplicate delivery |
| SDK starts, hooks, waits, steps and return value | Local/remote paths pass; `run_created`, eager `step_created`, `wait_created`, and `hook_received` have transactional dispatch outbox rows. A committed hook wake recovered remotely; pending-step/timer backup and fresh-key DLQ redrive passed in workerd; 45-second sleep passed remotely after backup | Remote start/step/wait publish faults, already-running step owner lease, remote DLQ fault and multi-resume dedup |
| Named binary streams, chunks, live read, close | Workerd and remote early-reader test pass | Writer crash/eviction, cursor/negative offsets, large binary and reconnect tests |
| `runs.waitForTerminalStatus` | Implemented, workerd timeout/wake test passes | Remote timeout, disconnect and terminal races |
| `createBatch`, `getMany`, `cancelMany`, `queueBatch`, stream writer sessions/`writeMulti` | `getMany` and atomic ordered `writeMulti` pass workerd tests. Others absent | Compare remaining Vercel methods; add atomicity, ordering and failure tests |
| Attributes (`attr_set`, `experimentalSetAttributes`) | `attr_set` works with the SDK's 64-key and 8 KiB write constraints, including merges; direct mutation API missing | SDK attribute calls and concurrent merges; higher limits require coordinated SDK changes |
| `hookRetention`, `hookForceClaim`, `hookResumeDedup` | Not advertised; partial hook-token logic | Retained-token expiry, forced takeover and dedup conformance |
| `inBandFence`, `maxConcurrency`, `deploymentAffinity` | Not advertised; D→E routing passes only with retained executors | Fence counters and refusal, serialized runners, registry/retirement and genuinely different A/B code |
| Dynamic code / upload / snapshots | Not implemented | Code size, cold-reload, snapshot version changes and cleanup |
| SDK encryption, key rotation, environment checks | Not implemented; storage and DLQ contain plaintext | Per-run stable keys across deployments, sealed payload E2E, rotate/revoke tests |
| Analytics, telemetry, descriptive IDs, latest deployment resolution | Not implemented | Data parity, latency/usage traces, version selection tests |
| Retention and cleanup | Not implemented; staging data persists | `retention: 0`, terminal expiry, active/retained hook tokens, queue/DLQ cleanup |
| Multi-region behavior | Not implemented; one store DO and one fanout shard | Explicit placement plus region-consistent state, queue, streams and hook lookup |
| Package/SDK ecosystem validation | Private staging app, patched SDK CLI; not published | Export installable `createWorld()`, land edge SDK support upstream, register in `worlds-manifest.json`, pass SDK CI E2E |

**Routing progress, not parity:** the earlier A→B hook failure committed `hook_received`, returned 500 and required a second POST, yielding two events. See `docs/staging-e2e.md`. The stable state/queue owner now dispatches through separately retained D/E executors in a Workers for Platforms [dispatch namespace](https://developers.cloudflare.com/cloudflare-for-platforms/workers-for-platforms/configuration/dynamic-dispatch/): a D hook and 45-second sleep both completed after E took new runs, with one hook event. The executor registry is still configuration text. Hook-wake outbox recovery passed a staged failed-publish test; run-start backstop and DLQ reopen passed in workerd. The cross-DO accepted-before-ack window, already-running step ownership, `wait_completed` handoffs, artifact retirement and remote start/step/wait/DLQ fault gates remain. Do not advertise `deploymentAffinity` yet. Cloudflare [version overrides](https://developers.cloudflare.com/workers/versions-and-deployments/version-overrides/) alone cannot retain arbitrary older builds.

Vercel OIDC, billing, dashboard and managed regional infrastructure cannot be copied literally. This ledger targets equivalent **SDK-visible behavior**, not identical platform services. No item above implies 1:1 parity yet.
