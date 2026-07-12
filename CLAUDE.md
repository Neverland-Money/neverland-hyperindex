# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An [Envio HyperIndex](https://docs.envio.dev/) indexer for the Neverland DeFi protocol on **Monad mainnet (chain id 143)**. It ingests on-chain events, runs TypeScript handlers over them, and writes entities to Postgres exposed via a Hasura/Envio GraphQL API. It indexes an Aave-V3-style lending market, DUST tokenomics + veDUST (`DustLock`) voting escrow, a points/leaderboard system, NFT partnership multipliers, LP-position points, and a profile-item shop.

Runtime is Node **22.18.0** (`.nvmrc`), package manager **pnpm 10** (see `packageManager` in `package.json`). `dependencies` are only `envio` and `viem`.

## The codegen loop (read this first)

Three files are the source of truth, and they must stay in sync:

- **`config.yaml`** — which networks/contracts/events to index, start blocks, `preload_handlers`, `field_selection`. Maps each contract to a `handler:` file.
- **`schema.graphql`** — the entity/enum definitions that become the DB tables and the GraphQL API.
- **`src/handlers/*.ts`** — the event-handler logic.

`pnpm run codegen` reads `config.yaml` + `schema.graphql` and regenerates the `generated/` directory (typed bindings, `handlerContext`, `TestHelpers`). **Handlers import types and the registration API from `'generated'` / `'../../generated'`.** After editing `config.yaml` or `schema.graphql` you must re-run codegen or types/handlers won't compile. `pnpm run type-check` runs `envio codegen` before `tsc --noEmit` for exactly this reason.

## Common commands

```bash
pnpm install                       # install deps
pnpm run local:docker:up           # start local Postgres (Docker Desktop required)
pnpm run codegen                   # regenerate generated/ after config.yaml or schema.graphql edits
pnpm run dev                       # run local indexer with hot reload
pnpm run type-check                # codegen + tsc --noEmit
pnpm run format / lint / lint:fix  # prettier / eslint (see eslint.config.js)
```

Testing (Node's built-in `node:test`, compiled first via `tsc`):

```bash
pnpm run test                      # tsc -> dist-test, then node --test dist-test/src/__tests__/*.test.js
pnpm run test:build                # compile only
pnpm run test:coverage             # c8 coverage report
pnpm run test:coverage:check       # enforce 100% lines/functions/branches/statements

# Run a single test file (build first, then point node --test at the compiled .js):
pnpm run test:build && node --test dist-test/src/__tests__/pool-events.test.js
# Filter by test name:
pnpm run test:build && node --test --test-name-pattern="colliding" dist-test/src/__tests__/config-events.test.js
```

Coverage is enforced at **100%** across the board, **except `src/handlers/lp.ts`**, which is excluded in the `c8` block of `package.json`. The **husky `pre-commit` hook** runs the full gate: `codegen` → git-diff check → `format:check` → `lint` → `type-check` → `test:coverage`. CI (`.github/workflows/ci.yml`) runs `format:check`, `lint`, and a source-cleanliness diff. Prod/staging Docker Compose lifecycle lives in the `prod:*` / `staging:*` npm scripts and the `README.md`.

## Architecture / concepts that span multiple files

### Two-phase preload execution (`preload_handlers: true`)
Every handler runs **twice per event**: first a concurrent *preload* pass to discover and batch DB reads (writes are no-ops), then a sequential pass with in-memory data. Consequences when editing handlers:
- Guard side effects with `isPreload(context)` (`context.isPreload === true`). In `src/handlers/shared.ts`, cache-invalidation and other write-time-only logic is gated behind `if (isPreload(context)) return;`.
- There is a **block-scoped read cache** in `shared.ts` for global singletons (`LeaderboardConfig`, `LeaderboardState`, active epoch, NFT registry, VP tiers). Mutation handlers must invalidate it; the cache also resets when the `context` identity changes (so test mocks don't leak across cases).

### Dynamic contract registration is forward-only
Handlers register newly discovered contracts via `SomeEvent.contractRegister(...)` + `context.add<Name>(address)` (e.g. `PoolAddressesProvider.ProxyCreated → addPool/addPoolConfigurator`, `PoolConfigurator.ReserveInitialized → addAToken/…`, `LeaderboardConfig.LPPoolConfigured → addUniswapV2Pair/…`, `NFTPartnershipRegistry.PartnershipAdded → addPartnerNFT`). A dynamically added contract is indexed **only from the triggering block onward — no backfill.** When history predates the registration event, statically bootstrap the address in `config.yaml`; Envio dedupes on `(contractName, address)`, so the later registration event becomes a harmless no-op. See `docs/isolated-pool-indexing.md`.

⚠️ Envio does **not** dedupe across *different* contract names. Statically-configured NFT collections (`STATIC_NFT_COLLECTION_ADDRESSES` in `helpers/constants.ts`) must never be re-registered as the dynamic `PartnerNFT`, or each `Transfer` fires two handlers and double-counts NFT balances. Keep that list in sync with the static NFT entries in `config.yaml`.

### Entity keying (pool-parametric, DRY across markets)
The lending layer is written to support multiple Aave markets from the same handlers. Reserves/positions/points key by `${asset}-${poolId}` (poolId = `PoolAddressesProvider` address) and resolve token roles via per-token `SubToken` rows. This is why the isolated `neverland-pendle-ausd` pool (a second market sharing the AUSD asset) coexists without collisions and its lending activity rolls into the **same** leaderboard automatically — scoring never filters by market. Regression-pinned by `config-events.test.ts`.

### Leaderboard / points / settlement
Points scoring is epoch-based and pool-agnostic: `settlePointsForUser` walks a user's flat `UserReserveList` and aggregates into one `UserEpochStats` keyed `${user}:${epoch}`. Config-driven per-hour rates (deposit/borrow/LP/VP) live in `helpers/points.ts`; combined multipliers (NFT decay + VP tiers, capped) in `shared.ts` and `helpers/leaderboard.ts`. The `LeaderboardKeeper` contract drives on-chain settlement/sync events (`leaderboardKeeper.ts`). Some epoch-1 values are **bootstrapped** from `helpers/constants.ts` (`EPOCH_1_*_OVERRIDE`, `BOOTSTRAP_*`) rather than events; this is gated by `ENVIO_DISABLE_BOOTSTRAP`.

### LP points "eras" (cutovers)
The active LP-points pool has changed over time and is gated by block-number cutovers in `src/handlers/lp.ts` (`applyStaticLPPoolCutover`), using the `LP_*_CUTOVER_BLOCK`/`_TIMESTAMP` constants: UniswapV3 → UniswapV2 pair → Balancer AutoRange V3 → back to the UniswapV2 pair. All four LP contracts stay registered in `config.yaml`; the handler decides which era accrues points for a given block. `lp.ts` is large and coverage-excluded — tread carefully and lean on `lp-events.test.ts` / `lp-coverage.test.ts`.

### Event-only in production — no external RPC reads
`shouldUseEthCalls()` in `shared.ts` is **hardcoded to `false`**: Monad full nodes can't serve archive-style historic state, so handlers must never depend on external chain reads. A `viem` public client exists (`helpers/viem.ts`) and there are opt-in env gates (`ENVIO_ENABLE_EXTERNAL_CALLS`, `ENVIO_ENABLE_ETH_CALLS`, `ENVIO_ENABLE_NFT_CHAIN_SYNC`, `ENVIO_ENABLE_LP_CHAIN_SYNC`), but they are effectively dead while `shouldUseEthCalls()` returns false. Do not introduce handler logic that requires live `eth_call`. `DEBUG_LP_POINTS=true` enables verbose LP tracing.

## Handler map
`src/handlers/`: `config.ts` (addresses-provider registry, pool/configurator/vault discovery, EMode), `pool.ts` (lending events), `tokenization.ts` (aToken/debt-token balances), `rewards.ts` (RewardsController, DustToken, RevenueReward), `dustlock.ts` (veDUST locks), `leaderboard.ts` + `leaderboardKeeper.ts` (epochs, config, settlement), `nft.ts` (partnership multipliers), `lp.ts` (LP positions/points), `profileShop.ts`, `specialEditions.ts`, and `shared.ts` (the shared engine: caching, settlement, multipliers, protocol aggregation glue). `src/helpers/` holds pure logic (`math.ts` ray/wad, `points.ts`, `leaderboard.ts`, `uniswapV3.ts`, `protocolAggregation.ts`, `constants.ts`, `entityHelpers.ts`, `viem.ts`, `testnetTiers.ts`).

## Testing pattern
Tests use the **native generated `TestHelpers`** loaded through the compatibility seam `src/__tests__/v3-test-helpers.ts` (it symlinks `generated/` into `dist-test/` and requires all handler modules so their `Contract.Event.handler(...)` registrations run before any `processEvent`). Pattern: `TestHelpers.MockDb.createMockDb()` → `TestHelpers.<Contract>.<Event>.createMockEvent({...})` → `processEvent({ event, mockDb })` → assert on `mockDb.entities.<Entity>.get(id)`. Tests set env gates (`ENVIO_ENABLE_EXTERNAL_CALLS='false'`, etc.) at the top of the file. Because tests run against compiled JS in `dist-test`, **run `test:build` (or `test`) after any source change** — stale `dist-test` output will silently test old code.
