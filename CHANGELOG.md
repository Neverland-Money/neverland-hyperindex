# Changelog

Notable changes to the indexer, and — where a change moves stored numbers — what it
affects and why. Entries that alter scoring or the schema carry a **Data impact** note:
whether a reindex is required, and which tides shift.

Newest first.

## Unreleased

### Schema: stable-rate borrowing removed entirely

Stable-rate borrowing is disabled across this market: all 18 deployed sTokens have never
emitted a stored event, and `STokenBalanceHistoryItem` and `StableTokenDelegatedAllowance`
both held zero rows. The whole surface is gone rather than gated — the `StableDebtToken`
contract and its five handlers, the `ReserveStableRateBorrowing` and `StableDebtTokenUpgraded`
events, both entities, 22 fields across `Reserve`, `UserReserve`, `Borrow`,
`ReserveParamsHistoryItem`, `ReserveConfigurationHistoryItem` and `ReserveRateSnapshot`, the
`sToken` `SubToken` row, and `abis/lending/StableDebtToken.json`.

`totalDebt` in settlement and protocol aggregation drops its stable term. `ReserveDataUpdated`
keeps its `stableBorrowRate` parameter — it is an ABI field, simply no longer stored. The debt
fields left behind are normalized in the next entry.

**Data impact:** breaking for GraphQL consumers reading any removed field, and a full reindex
is required (Envio has no in-place migration; `envio dev --restart` clears the database).
Stored point totals are unaffected — no scoring path read stable debt.

### Schema: debt fields normalized to one name

With stable-rate borrowing gone there is only one kind of debt, so the schema no longer
distinguishes it. `currentTotalDebt` is deleted — it was assigned the identical expression as
`currentVariableDebt` at every write site — and the remaining amount fields drop the now
meaningless `Variable` qualifier, mirroring how the supply side names one concept:

| Before                        | After                 |
| ----------------------------- | --------------------- |
| `scaledVariableDebt`          | `scaledDebt`          |
| `currentVariableDebt`         | `currentDebt`         |
| `currentTotalDebt`            | _removed_             |
| `totalScaledVariableDebt`     | `totalScaledDebt`     |
| `totalCurrentVariableDebt`    | `totalCurrentDebt`    |
| `lifetimeScaledVariableDebt`  | `lifetimeScaledDebt`  |
| `lifetimeCurrentVariableDebt` | `lifetimeCurrentDebt` |

The same collapse applies wherever a name qualified against a stable counterpart that no
longer exists:

- `getCurrentBalancesFromScaled` returned `{ supply, variableDebt, totalDebt }` with the last
  two always equal; it now returns `{ supply, debt }`.
- `VariableTokenDelegatedAllowance` → `DelegatedAllowance`.
- `ReserveRateSnapshot.variableBorrowAPRRay` → `borrowAPRRay`, pairing with `liquidityAPRRay`.
- `Borrow.variableTokenDebt` → `scaledDebt`, which is what it always held.
- The `-variable` suffix on `BorrowAllowance` ids and the `variable` prefix on
  delegated-allowance ids are dropped.

Names that mirror on-chain identifiers are deliberately kept: `variableBorrowRate`,
`variableBorrowIndex`, `baseVariableBorrowRate` and `variableRateSlope1/2` appear in the
`ReserveDataUpdated` event and the `DefaultReserveInterestRateStrategy` ABI, and
`IsolationModeTotalDebtUpdated.totalDebt` is an event parameter. Renaming those would put the
schema out of step with the chain.

**Data impact:** breaking for GraphQL consumers reading any renamed field or entity, and
`BorrowAllowance` / `DelegatedAllowance` row ids change shape. Full reindex required. No
stored value changes — every renamed field carries the same number it did before, and the
deleted one was a duplicate.

### Schema: four dead Pool events and seven orphan entities removed

`MintUnbacked`, `BackUnbacked`, `SwapBorrowRateMode` and `RebalanceStableBorrowRate` had
handlers but no live use; their four entity types and two `Action` enum members are gone.
Separately, seven entity types with no writer anywhere in the codebase are deleted:
`EpochLeaderboardStats`, `SelfTransferredToken`, `SwapHistory`, `TopK`, `TopKEntry`,
`UsdEthPriceHistoryItem`, `UserLPBaseline`.

`IsolationModeTotalDebtUpdated` and `DustLock.Split` are deliberately kept. Ranking already
uses ORDER BY, so removing the materialized `TopK` types changes no result.

**Data impact:** breaking for consumers querying those types; full reindex required. No
stored value changes.

**Not a request-rate change.** Measured after a 17% filter reduction: mean 219.5 getLogs/min
across six windows against a 169/min baseline. These removals are justified by cleanliness,
not throughput.

### Tooling: `scripts/export-tides.ts`

Exports settled Tides to `data/tide-<n>.json` with every BigInt written as a quoted JSON
string, read out of Postgres through `COPY ... FORMAT csv` with every column cast to `::text`
so a `numeric` never round-trips through a float. `--verify` re-reads and compares field for
field with zero tolerance. Adds no dependency — `psql` is invoked directly.

### LP accrual: Uniswap V3 back on the per-swap fanout

Concentrated-range pools (Uniswap V3) settle each position on every swap and mutation
again, as production does. The lazy Fenwick moment accumulator that replaced it is gone:
`LPV3GrowthNode`, the `LPPoolGrowthKind` enum and `LPPoolEpochGrowth.kind` are deleted,
and `lpGrowthMath.ts` drops from 217 lines to 37.

Fungible pools (the UniswapV2 pair, Balancer AutoRange) keep the lazy scalar growth
clock, which was measured exact and is the live path. The two engines are chosen by pool
shape through `isFungibleLPPoolConfig`; a concentrated pool now owns no growth header and
no `UserLPEpochCursor` at all.

Why: lazy growth bought nothing in production for V3 — that pool's era ended at the
UniswapV2 cutover and every tide it touched is prefilled — while carrying the most
complex code in the indexer, and it measurably drifted from production on Tides 1-2.

**Data impact: requires a reindex.** Dropping `LPV3GrowthNode` and the `kind` column is a
breaking schema change; any environment holding V3-era growth rows must resync.

Fixed along the way, each independently able to corrupt LP points:

- Five V3 mutation sites settled correctly and then persisted the **stale**
  `position.accumulatedInRangeSeconds` instead of `settlement.newAccumulatedSeconds`.
  In-range seconds under-reported permanently; the burn path lost the final in-range span
  of a position's life irrecoverably.
- The legacy-V3 cutover advanced a Fenwick scalar clock where it must call
  `settleLPPoolPositions` — the single moment every V3 position is frozen for good.
- `settleUserLPPositions` settled every pool through the lazy path regardless of shape,
  so the entire V3 epoch close still ran on Fenwick.
- A position whose pool has no `LPPoolConfig` was scored against an unrelated market:
  `getEffectiveLPPoolConfig` falls back to the single active pool, and the ranged engine
  resolves its rate through it. Such positions now settle to nothing.
- Six V3 sites lost their `updatePoolLPStats` call, so pool TVL and position counts only
  refreshed on a tick-moving swap.

### Leaderboard: `ENVIO_LEADERBOARD_LIVE_EPOCH` removed

The backfill gate is gone from the code, `.env` and `.env.example`, along with the
`HISTORICAL_ACTIVE_FINAL_ONLY` keeper mode it drove. It skipped mid-epoch keeper reserve
sweeps for closed tides to speed up backfills, at the cost of corrupting the tides it
skipped.

**Data impact:** any environment that backfilled with it set has wrong values for every
tide below its floor and must resync.

### Reserve metadata: `KNOWN_TOKENS` is authoritative

For a listed asset the indexer takes symbol, name **and** decimals from the table and
ignores what the chain reports — a curated entry is a deliberate statement, and on-chain
discovery must not silently overwrite it. An asset missing from the table is derived from
`AToken.Initialized`, where name and symbol coincide.

Two bugs fixed:

- Symbol derivation matched only the canonical market's prefixes (`n` /
  `Neverland Interest Bearing`), so all three isolated Pendle markets came through
  unstripped: `npSHMON` resolved to `pSHMON` with name `Neverland Pendle SHMON`.
  Derivation is now prefix-agnostic.
- `PoolConfigurator.initReserves` initializes the aToken proxy **before** emitting
  `ReserveInitialized`, so `config.ts` writes last and was overwriting the derived values
  with its `'ERC20'`/`'Token ERC20'`/18 placeholders. An unlisted 6-decimal asset stayed
  at 18 decimals, mispricing it in TVL and LP value.

### Point-accrual blacklist from Tide 9

25 addresses — the 14 in `neverland-tide-draw/blacklist.json` plus the 12 Neverland
Foundation multisigs, with one address in both — stop accruing points entirely from the
start of Tide 9. Gated at six call sites covering every automatic accrual path. Manual
on-chain admin awards are deliberately **not** gated. See `ISSUES.md` §5.

**Data impact:** Tides 1-8 unaffected. These addresses hold points accrued between the
Tide 9 open and this landing; the next full resync corrects them.

### `PREFILL_HISTORIC_EPOCHS` — settled Tides are written, not recomputed

Off by default. When on, every `data/tide-<n>.json` present is written verbatim and the
leaderboard performs **no settlement** for the span those tides cover. Discovery is the file
listing, so adding or removing a tide file is the whole control surface.

Coverage is keyed on timestamp, running to the last prefilled tide's `endTime` inclusive.
Seven guards enforce it: `settlePointsForUser`, the four `awardDaily*Points`,
`updateUserEpochLPPoints`, and the Tide close in `freezeLPForEpochEnd` — the last one
matters because the LP holder sweep and the growth freeze would otherwise rewrite stored
values at the boundary. `PREFILL_DATA_DIR` relocates the directory for containers.

Verified against the real export: 8 tides, 28,403 rows, covered through 2026-08-28 04:00
UTC, with the boundary exact to the second.

Measured on a full resync with the flag on: all 8 tides written at block 37.3M — before the
leaderboard start block — and `UserEpochStats` for Tides 1-8 held at exactly 28,403 rows for
the entire span, with the sync reaching block 85.7M in 600s against ~2,200s for the same
point without prefill (~3.7x).

**Known limit: prefilled `LeaderboardEpoch` rows are not immutable.** The guards cover
settlement, not the epoch lifecycle, so `EpochStart` reopens each prefilled tide as the sync
enters its window (`isActive` true, `endTime` cleared) and `EpochEnd` closes it again. In the
measured run every tide converged back to precisely the value in `data/`, because the
on-chain timestamps agree with the export. They would not converge for a tide whose on-chain
dates are wrong — the case `EPOCH_DATES_OVERRIDES` exists to correct — and the stored epoch
envelope would then follow the chain rather than the file. Points are unaffected either way.

### Stable-debt tokens are no longer registered

`INDEX_STABLE_DEBT_TOKENS = false`. Stable-rate borrowing is disabled across this market —
no reserve has `stableBorrowRateEnabled`, none carries stable debt, no user holds any — yet
all 18 deployed sTokens were registered, putting **90 of 542 address/topic pairs (17%)** in
front of HyperSync on every block for data that cannot exist. The handlers remain in place;
flip the flag and resync to index stable debt again.

Takes effect on a fresh sync only, since registrations already persisted in
`envio_addresses` are not revoked.

### Maintenance

Every manual figure now lives in `src/helpers/constants.ts` under section separators: LP
era pool identities and cutover transitions, fee and volume figures, multiplier and
cooldown caps, decimal fallbacks, leaderboard bucket sizing, shop literals. Four
duplicated definitions collapsed to one (`ZERO_ADDRESS`, `BASIS_POINTS`, `RAY`,
`DUST_TOKEN_ADDRESS`). Derived mathematical primitives stay with the math they serve.

---

## Reconciliation: why recomputed history no longer matches the draws

Comparing a clean full resync against the `neverland-tide-draw` leaderboards for Tides
1-8 shows a sharp split, and it is **not** a defect in the current code.

| tides | agreement                   | material differences                       |
| ----- | --------------------------- | ------------------------------------------ |
| 6-8   | 92% within float noise      | 40 total — 38 LP-model, 2 sub-1-point dust |
| 1-5   | median identical, long tail | 520-651 users per tide                     |

**Root cause of the 1-5 tail: `57e39c2` (2026-06-20) added `settleVpOwnerBeforeMutate`.**
Before it, a veDUST lock increase credited the whole elapsed period since the last
settlement at the **new, higher** VP. The fix settles first, so the old period is credited
at the old VP. It legitimately _reduces_ points for anyone who grew a lock mid-tide.

That commit landed between the Tide 5 draw (2026-05-31) and the Tide 6 draw (2026-06-30).
Tides 1-5 were drawn under the old over-crediting behavior; Tides 6-8 were drawn under
the current one. Hence VP-only differences for 303-406 users per tide in 1-5 (ours lower
in ~75-80% of cases, p10 ratio 0.74-0.83) and none at all in 6-8.

The Tide 6-8 differences have two separate causes, neither of them the Tide-close holder
sweep (zero of the users present only in our data are LP-only holders):

- **Row-count gap** — 5, 5 and 9 addresses appear in our data but not the draw. Almost all
  are blacklisted (4/5, 4/5, 9/9): `neverland-tide-draw` filters `blacklist.json`, and the
  indexer does not, since the new point-accrual blacklist only begins at Tide 9.
- **LP value differences** — 324, 274 and 145 common users. Production has never run the
  lazy scalar growth model at all: `LPPoolEpochGrowth` and `UserLPEpochCursor` do not
  exist in its schema. Tides 6-8 sit entirely in fungible eras (UniswapV2 pair, then
  Balancer AutoRange), which are exactly the pools this change moved from per-position
  settlement onto that model. Two different algorithms, so small bidirectional moves:
  median 7e-4 in Tides 6-7 and 2e-8 in Tide 8, consistent with the 0.2-1.5% mid-tail churn
  measured during rollout.

**Consequence.** Tides 1-8 are settled and paid. Recomputing them with today's code
produces _more correct_ numbers that are **not** the numbers distributed, and every future
correctness fix widens that gap. History must therefore be prefilled rather than
recomputed — this is the case for `PREFILL_HISTORIC_EPOCHS`.

**Decision:** `data/tide-1..8.json` is exported from a clean local resync — the current
engine's truth — not from the draw archives. A resync reproduces what the indexer believes
today, consistently and repeatably, and this changelog is the record of why those figures
differ from what was distributed. The paid rankings remain in
`neverland-tide-draw/tides/N/leaderboard.json`.

Two earlier attributions of this split were wrong and are recorded so they are not
retried: it is not `ENVIO_LEADERBOARD_LIVE_EPOCH` (introduced at epoch 7, so draws 1-6
predate it), and it is not the additive-multiplier change of `2112d22` (which explains
only 20-29 users per tide, against 303-406 for VP).
