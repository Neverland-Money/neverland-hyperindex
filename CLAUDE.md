# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An [Envio HyperIndex](https://docs.envio.dev/) indexer for the Neverland DeFi protocol on **Monad mainnet (chain id 143)**. It ingests on-chain events, runs TypeScript handlers over them, and writes entities to Postgres exposed via a Hasura/Envio GraphQL API. It indexes an Aave-V3-style lending market, DUST tokenomics + veDUST (`DustLock`) voting escrow, a points/leaderboard system, NFT partnership multipliers, LP-position points, and a profile-item shop.

Runtime is Node **22.18.0** (`.nvmrc`), package manager **pnpm 10** (see `packageManager` in `package.json`). `dependencies` are only `envio` and `viem`.

## The codegen loop (read this first)

Three files are the source of truth, and they must stay in sync:

- **`config.yaml`** — which chains/contracts/events to index, start blocks, batch/reorg settings, and `field_selection`. Maps each contract to a `handler:` file.
- **`schema.graphql`** — the entity/enum definitions that become the DB tables and the GraphQL API.
- **`src/handlers/*.ts`** — the event-handler logic.

`pnpm run codegen` reads `config.yaml` + `schema.graphql` and refreshes V3's ignored `.envio/types.d.ts`, wired into the `envio` module by `envio-env.d.ts`. **Handlers import `indexer` from `envio` and entity aliases from `src/types/envio.ts`.** After editing `config.yaml` or `schema.graphql`, re-run codegen or types/handlers will be stale. `pnpm run type-check` runs `envio codegen` before `tsc --noEmit` for exactly this reason.

## Common commands

```bash
pnpm install                       # install deps
pnpm run local:docker:up           # start local Postgres (Docker Desktop required)
pnpm run codegen                   # refresh V3 types after config.yaml or schema.graphql edits
pnpm run dev                       # run local indexer with hot reload
pnpm run dev:restart               # clear local state and reindex from scratch
pnpm run type-check                # codegen + tsc --noEmit
pnpm run format / lint / lint:fix  # prettier / eslint (see eslint.config.js)
```

Testing (Node's built-in `node:test`, running TypeScript through `tsx`):

```bash
pnpm run test                      # type-check, then run src/__tests__/*.test.ts
pnpm run test:build                # type-check without emitting files
pnpm run test:coverage             # c8 coverage report
pnpm run test:coverage:check       # enforce 100% lines/functions/branches/statements

# Run a single test file through the V3 simulator.
# `--import ./src/__tests__/test-env-preload.ts` is REQUIRED: it pins the operator settings
# (prefill off, fixture-only data dir) that `envio`'s dotenv would otherwise take from `.env`.
# Without it a test can read the 31 MB production `data/` directory.
ENVIO_TEST_HANDLER_WILDCARD=true node --import tsx --import ./src/__tests__/test-env-preload.ts --test src/__tests__/pool-events.test.ts
# Filter by test name:
ENVIO_TEST_HANDLER_WILDCARD=true node --import tsx --import ./src/__tests__/test-env-preload.ts --test --test-name-pattern="colliding" src/__tests__/config-events.test.ts
```

The **husky `pre-commit` hook** runs `codegen` → git-diff check → `format:check` → `lint` → `type-check` → enforcing `test:coverage:check`. Coverage is required to remain at 100% lines/functions/branches/statements. CI (`.github/workflows/ci.yml`) runs `format:check`, `lint`, and a source-cleanliness diff. Prod/staging Docker Compose lifecycle lives in the `prod:*` / `staging:*` npm scripts and the `README.md`.

## Architecture / concepts that span multiple files

### V3 always-on two-phase preload execution

Every handler runs **twice per event**: first a concurrent _preload_ pass to discover and batch DB reads (writes are no-ops), then a sequential pass with in-memory data. Consequences when editing handlers:

- Guard side effects with `isPreload(context)` (`context.isPreload === true`). In `src/handlers/shared.ts`, cache-invalidation and other write-time-only logic is gated behind `if (isPreload(context)) return;`.
- There is a **block-scoped read cache** in `shared.ts` for global singletons (`LeaderboardConfig`, `LeaderboardState`, active epoch, NFT registry, VP tiers). Mutation handlers must invalidate it; the cache also resets when the `context` identity changes (so test mocks don't leak across cases).

### Dynamic contract registration is forward-only

Handlers register newly discovered contracts via `indexer.contractRegister(...)` + `context.chain.<Contract>.add(address)` (e.g. `PoolAddressesProvider.ProxyCreated → Pool/PoolConfigurator`, `PoolConfigurator.ReserveInitialized → AToken/debt tokens`, `LeaderboardConfig.LPPoolConfigured → LP contracts`, `NFTPartnershipRegistry.PartnershipAdded → PartnerNFT`). A dynamically added contract is indexed **only from the triggering block onward — no backfill.** When history predates the registration event, statically bootstrap the address in `config.yaml`; Envio dedupes on `(contractName, address)`, so the later registration event becomes a harmless no-op. See `docs/isolated-pool-indexing.md`.

⚠️ Envio does **not** dedupe across _different_ contract names. Statically-configured NFT collections (`STATIC_NFT_COLLECTION_ADDRESSES` in `helpers/constants.ts`) must never be re-registered as the dynamic `PartnerNFT`, or each `Transfer` fires two handlers and double-counts NFT balances. Keep that list in sync with the static NFT entries in `config.yaml`.

### Entity keying (pool-parametric, DRY across markets)

The lending layer is written to support multiple Aave markets from the same handlers. Reserves/positions/points key by `${asset}-${poolId}` (poolId = `PoolAddressesProvider` address) and resolve token roles via per-token `SubToken` rows. This is why the isolated pools — `neverland-pendle-ausd` (shares the AUSD asset with canonical) and `neverland-pendle-shmon` (shares WMON) — coexist without collisions and their lending activity rolls into the **same** leaderboard automatically; scoring never filters by market. Each isolated market's `PoolAddressesProvider` is statically bootstrapped in `config.yaml` because neither is registered in the on-chain registry yet and dynamic registration is forward-only. Regression-pinned by `config-events.test.ts`; runbook in `docs/isolated-pool-indexing.md`.

### Leaderboard / points / settlement

Points scoring is epoch-based and pool-agnostic: `settlePointsForUser` walks a user's flat `UserReserveList` and aggregates into one `UserEpochStats` keyed `${user}:${epoch}`. Config-driven per-hour rates (deposit/borrow/LP/VP) live in `helpers/points.ts`; combined multipliers (NFT decay + VP tiers, capped) in `shared.ts` and `helpers/leaderboard.ts`. The `LeaderboardKeeper` contract drives on-chain settlement/sync events (`leaderboardKeeper.ts`). Some epoch-1 values are **bootstrapped** from `helpers/constants.ts` (`EPOCH_1_*_OVERRIDE`, `BOOTSTRAP_*`) rather than events; this is gated by `ENVIO_DISABLE_BOOTSTRAP`.

### LP points "eras" (cutovers)

The active LP-points pool has changed over time and is gated by block-number cutovers in `src/handlers/lp.ts` (`applyStaticLPPoolCutover`), using the `LP_*_CUTOVER_BLOCK`/`_TIMESTAMP` constants: UniswapV3 → UniswapV2 pair → Balancer AutoRange V3 → back to the UniswapV2 pair. All four LP contracts stay registered in `config.yaml`; the handler decides which era accrues points for a given block. `lp.ts` is large and coverage-excluded — tread carefully and lean on `lp-events.test.ts` / `lp-coverage.test.ts`.

Two different accrual engines run side by side, chosen by pool shape (`isFungibleLPPoolConfig`, i.e. `positionManager === pool`):

- **Concentrated range (Uniswap V3)** — per-swap fanout. `UniswapV3Pool.Swap` walks the pool's position index (`updatePositionsInRangeStatus`), and every mutation settles the touched position first (`settleLPPosition`). Accrual state lives on the position row itself (`lastInRangeTimestamp` / `accumulatedInRangeSeconds`); points come from `valueUsd x rate x in-range seconds`. There is no pool-wide growth header — `advanceLPPoolGrowth` returns immediately for these pools.
- **Fungible share (UniswapV2 pair, Balancer AutoRange)** — lazy scalar growth. One `LPPoolEpochGrowth.scalarGrowthX128` clock per pool per Tide, advanced in O(1), with each holder reading its share off a `UserLPEpochCursor` when touched.

### Event-only in production — no external RPC reads

`shouldUseEthCalls()` in `shared.ts` is **hardcoded to `false`**: Monad full nodes can't serve archive-style historic state, so handlers must never depend on external chain reads. A `viem` public client exists (`helpers/viem.ts`), but handler paths remain event-only even if external-call environment variables are set. Do not introduce handler logic that requires live `eth_call`. `DEBUG_LP_POINTS=true` enables verbose LP tracing.

## Handler map

`src/handlers/`: `config.ts` (addresses-provider registry, pool/configurator/vault discovery, EMode), `pool.ts` (lending events), `tokenization.ts` (aToken/debt-token balances), `rewards.ts` (RewardsController, DustToken, RevenueReward), `dustlock.ts` (veDUST locks), `leaderboard.ts` + `leaderboardKeeper.ts` (epochs, config, settlement), `nft.ts` (partnership multipliers), `lp.ts` (LP positions/points), `profileShop.ts`, `specialEditions.ts`, and `shared.ts` (the shared engine: caching, settlement, multipliers, protocol aggregation glue). `src/helpers/` holds pure logic (`math.ts` ray/wad, `points.ts`, `leaderboard.ts`, `uniswapV3.ts`, `protocolAggregation.ts`, `constants.ts`, `entityHelpers.ts`, `viem.ts`, `testnetTiers.ts`).

## Testing pattern

Tests use Envio V3's public `createTestIndexer()` through the compatibility seam `src/__tests__/v3-test-helpers.ts`. It preserves the suite's `TestHelpers.MockDb.createMockDb()` → `TestHelpers.<Contract>.<Event>.createMockEvent({...})` → `processEvent({ event, mockDb })` shape while routing events through the real V3 simulator. `ENVIO_TEST_HANDLER_WILDCARD=true` is test-only: it lets synthetic contract addresses reach handlers and suppresses dynamic registrations that would otherwise ask the one-event simulator to fetch another contract. Production leaves it unset. Tests run directly from TypeScript, so there is no `dist-test` output to refresh.
