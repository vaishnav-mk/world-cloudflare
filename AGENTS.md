# Agent guide

Rules for anyone, human or agent, changing this repo. The World package is `packages/world-cloudflare`; the root is an example app.

## Handling `undefined`

Check a value once, where it enters, and let the types carry the result. Don't re-check what the types already guarantee.

- **Trust the types.** No `if (!x)`, `?.` or `??` on a value whose type is not nullable.
- **Lookups come in pairs.** `db.getRun()` throws the contract's not-found error; `db.findRun()` returns `T | undefined`. Use `find*` only where a missing record is a normal outcome. Never follow a lookup with `if (!x) throw`.
- **Parse untrusted input once.** RPC arguments, queue payloads, cursors, request bodies and stored JSON are parsed or validated at entry. Code past that point takes the parsed type.
- **Apply defaults once.** Destructure optional options with defaults where they enter (`{ limit = DEFAULT_PAGE_LIMIT } = {}`), or give the parameter a default. No `?? {}` or `?? []` deeper in.
- **Model "X is set only when Y" as a discriminated union,** not optional fields. `Change` in `store/events/types.ts` is the example: each kind carries exactly the entities it writes.
- **No `!`.** Don't replace it with `?.` either; that hides the bug. Narrow, restructure, or call a throwing helper.
- **Library optionality is handled in one place.** workers-qb types `results` as optional; use `allRows`, `findRow` or `requireRow` from `lib/qb.ts`.
- **Compare explicitly** (`=== undefined`, `=== null`) where `0`, `''` or `false` are valid values.
- **A cast of trusted data gets a `SAFETY:` comment** saying why it holds, as on `Outbox.due`.

`?.` and `??` are right for values that really are optional: SDK parameters and fields, nullable SQL columns, `Map.get`.

## Structure

- Name SDK string values with the enums in `src/enums.ts` and use `switch` over them. End a switch over an SDK event type with `default: return unreachable(data)`, because TypeScript doesn't treat enum cases as covering a string-literal union.
- Shared types live in `src/types.ts`, tunables in `src/constants.ts`.
- Durable Object methods are RPC. Keep their names and signatures stable, because retained executors call them. Use `#private` members, not TypeScript `private`, so helpers aren't exposed over RPC.
- Keep Durable Object classes thin. `WorldStore` methods delegate to the module for their role (`store/reads.ts`, `store/writes.ts`, `store/token-shard.ts`...), which takes the store's `StoreContext`; `QueueShard` delivers through `queue/deliver.ts`.
- The package compiles to `dist` with `tsc -p tsconfig.build.json` (`pnpm build` in the package; `pnpm run build:workflow` runs it). The root resolves `world-cloudflare` to `src` through the `worlds-source` export condition (`customConditions` in `tsconfig.json`, `ssr.resolve.conditions` in each vitest config), so typecheck and tests run on source. Wrangler and Node load `dist`.
- Tables are Drizzle definitions in each object's `tables.ts`. After changing one, run `pnpm db:generate` in the package and commit the migration. Keep schema changes additive.
- Queries use workers-qb. Use `raw` only for aggregates, joins and arithmetic updates its typed API can't express.

## Before you finish

```sh
pnpm run typecheck && pnpm test && pnpm run test:e2e
```
