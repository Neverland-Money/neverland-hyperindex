# Known issues

Deferred defects with enough detail to act on without re-deriving the analysis.
Each entry states what the code does, what the deployed contract does, what the
indexed data looks like today, and what changes once it is fixed.

---

## 1. Flash-loan premium accounting contradicts the deployed contract

**Severity:** high — wrong aggregates, and a double-counted supply.
**Status:** open, deliberately deferred.
**Where:** `src/handlers/pool.ts:446` (`Pool.FlashLoan.handler`), writes at `:475-481`.

### What the indexer does

```ts
if (pool && pool.flashloanPremiumToProtocol) {
  protocolFee = (premium * pool.flashloanPremiumToProtocol + 5000n) / 10000n;
  lpFee = premium - protocolFee;                     // LP / protocol split
}
context.Reserve.set({
  availableLiquidity: reserve.availableLiquidity + premium,     // <-- immediate
  totalATokenSupply: reserve.totalATokenSupply + premium,       // <-- immediate
  lifetimeFlashLoanLPPremium: ... + lpFee,
  lifetimeFlashLoanProtocolPremium: ... + protocolFee,
});
```

### What the deployed contract does

`contracts/neverland-lending/contracts/protocol/libraries/logic/FlashLoanLogic.sol`,
`_handleFlashLoanRepayment`, verbatim:

> The entire flash-loan premium accrues to the treasury (Aave v3.5 behaviour): no
> LP/protocol split and no liquidityIndex bump. `params.flashLoanPremiumToProtocol`
> is retained for ABI / storage stability but no longer routes the premium.

```solidity
reserve.accruedToTreasury += params.totalPremium
  .getATokenMintScaledAmount(reserveCache.nextLiquidityIndex).toUint128();
```

100% of the premium goes to `accruedToTreasury`. No index bump. The aTokens are
minted **later**, by `mintToTreasury`, which emits `AToken.Mint(caller = Pool)`
plus `Pool.MintedToTreasury`.

### Consequences

1. **`totalATokenSupply` is counted twice** — once here on `FlashLoan`, then
   again when the deferred treasury mint lands. `suppliesUsd` / `tvlUsd` read
   `totalATokenSupply`, so both inherit the error.
2. **`lifetimeFlashLoanLPPremium` is fiction.** `flashloanPremiumToProtocol` is
   retained only for ABI stability; there is no LP share any more. Any analytics
   splitting flash-loan premium LP-vs-protocol is reporting a split that the
   contract stopped making.

**Not** a defect, despite looking like one: `availableLiquidity += premium` at
flash-loan time is CORRECT. `_handleFlashLoanRepayment` does
`safeTransferFrom(receiver, aTokenAddress, amountPlusPremium)` and
`updateInterestRates(..., liquidityAdded = amountPlusPremium, 0)`, so the
underlying really does arrive in that transaction. Only the aToken *minting* is
deferred. Do not "fix" this line.

This is pre-existing, not introduced by any recent change — but a from-genesis
resync replays it deterministically into all of history.

### Fix sketch

- Drop the `lpFee` / `protocolFee` split; the premium is entirely protocol
  revenue. Keep `lifetimeFlashLoanPremium`; retire or zero the LP-share field.
- Keep `availableLiquidity += premium` — the underlying arrives now (see above).
- Do **not** move `totalATokenSupply` here. The aTokens are minted later by
  `mintToTreasury`, which the treasury path already handles correctly
  (`AToken.Mint` with `caller = Pool` moves supply, `Pool.MintedToTreasury` books
  the revenue through the shared aggregator).
- Regression test: a `FlashLoan` followed by the paired
  `AToken.Mint(caller = Pool)` + `MintedToTreasury`, asserting `totalATokenSupply`
  rises exactly once and revenue is booked exactly once.

### Data difference after the fix

| Field | Today | After |
|---|---|---|
| `Reserve.totalATokenSupply` | premium counted twice per flash loan | counted once, at the treasury mint |
| `suppliesUsd` / `tvlUsd` | inflated by the duplicate | corrected |
| `availableLiquidity` | correct already | unchanged |
| `lifetimeFlashLoanLPPremium` | non-zero, meaningless | zero / removed |
| `lifetimeFlashLoanProtocolPremium` | partial share | full premium |

Verify by replaying a block range containing a flash loan on both branches and
diffing `Reserve`, `ProtocolStats`, `PoolStats` and `entity_history`.

---

## 2. Epoch-9 gap settlement is best-effort if prices move (accepted)

**Severity:** medium, epoch-9 specific. **Status:** accepted, no fix planned.
**Where:** `src/handlers/shared.ts`, the cumulative price-index cap in the
accrual path.

The manual settlement sweep run during the one-hour gap caps the cumulative price
index by subtracting the latest price across the whole post-epoch gap. If the
price of a reserve moves between epoch 9's end (`1790442000`) and the moment a
user is settled, that user's epoch-9 points are credited at the later price and
are therefore slightly over- or under-stated.

Exact settlement would require persisting the cumulative price index **at** the
epoch boundary and settling against that snapshot; timing the sweep inside the
gap narrows the window but cannot close it.

**Accepted deliberately:** the drift is proportional to price movement over a
one-hour window, we do not mint against these points, and best-effort is good
enough. Recorded so nobody re-derives it as a new finding.

---

## 3. Quiet multi-epoch rollover collapses epoch block numbers

**Severity:** low. **Status:** open.
**Where:** `src/handlers/shared.ts:757`, `applyScheduledEpochTransitions`
(`MAX_SCHEDULED_TRANSITIONS = 5`).

Epoch transitions only run when an event arrives, and at most five boundaries are
crossed per call. Two consequences when the protocol goes quiet across one or
more whole epochs:

- **Deferred catch-up.** If more than five boundaries elapse between two indexed
  events, the remainder wait for the following events. Self-correcting, since
  every protocol transaction drives the loop.
- **Block numbers collapse.** `startBlock` / `endBlock` for every epoch crossed
  during the catch-up are stamped with the *catch-up event's* block, because that
  is the only block in hand. The timestamps stay correct (they come from the
  stored schedule), so points and durations are unaffected — but a consumer
  reading epoch block ranges for a quiet span gets a degenerate range.

LP settlement is invoked per boundary with that boundary's `endTime`, so LP
points remain time-correct; only the block attribution degrades.

Fix would mean resolving the block at a boundary timestamp, which the indexer has
no cheap way to do. Low value unless block ranges become load-bearing.

---

## 4. Treasury mints drop the interest on the treasury's own position

**Severity:** low. **Status:** open.
**Where:** `src/handlers/tokenization.ts:112` and the treasury branch below it.

`userBalanceChange = event.params.value - event.params.balanceIncrease`, and
`totalATokenSupply` moves by that figure. For ordinary users the excluded
`balanceIncrease` is recovered later from the index-based balance recomputation
on their `UserReserve`. The treasury branch writes no `UserReserve`, so nothing
recovers it there.

So when `mintToTreasury` fires while the treasury *also* holds a real aToken
position — which happens, since OTC returns are supplied `onBehalfOf` the
treasury — the interest accrued on that position is dropped from
`totalATokenSupply` and is never credited anywhere.

Impact is bounded by interest on the treasury's own balance between two treasury
mints, and it under-reports rather than inflates. Fixing it means tracking the
treasury's position properly rather than skipping user accounting for it, which
reopens the question of whether the treasury should appear as a supplier at all.

---

## 5. Also deferred

Smaller items consciously left alone; none blocks a resync.

- **Multiplier is time-weighted, not points-weighted.**
  `calculateAverageCombinedMultiplierBps` (`src/handlers/shared.ts`) averages the
  combined multiplier across the window by duration, so a segment in which a user
  held nothing still drags their multiplier down. Visible in
  `vp-ownership-weighting.test.ts`: a mid-window acquirer scores 0.375 of a full
  holder where the pure point-integral would be 0.5. Pre-existing, affects every
  user; changing it moves everyone's points.

- **Snapshot capture points disagree within a transaction.**
  `ProtocolStatsSnapshot` is written before handler accounting
  (`src/handlers/shared.ts`, `recordProtocolTransaction`), `PoolStatsSnapshot`
  after (`src/helpers/protocolAggregation.ts`). At one timestamp the protocol row
  is pre-transaction while the pool row is post-transaction, so window
  calculations can place the last transaction of a period on the wrong side.

- **`lastPreloadEpochSweep` is a single slot.**
  `src/handlers/shared.ts`. If two epoch boundaries close in one preload batch the
  dedupe can thrash, and nothing clears it after a reorg replay. Performance only
  — never worse than the pre-change baseline, and it cannot affect stored data
  because preload writes are `noopSet`. A per-boundary `Set` would close it.

- **`test:coverage:check` now fails honestly.**
  The thresholds used to be passed after `node --test`, so c8 never saw them and
  the gate always passed. Fixed to enforce; it now reports the real number
  (~96.4% statements), dragged mainly by `src/handlers/specialEditions.ts` at
  16.4%. CI does not run it. `.husky/pre-commit` still runs the report-only
  `test:coverage`, not the enforcing variant.

- **`optimalUtilisationRate` → `optimalUtilizationRate`** is an unversioned public
  GraphQL/DB column rename. No in-repo consumer; external consumers were accepted
  as a deliberate call.
