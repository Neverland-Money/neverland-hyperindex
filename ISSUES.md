# Known issues

Deferred defects with enough detail to act on without re-deriving the analysis.
Each entry states what the code does, what the deployed contract does, what the
indexed data looks like today, and what changes once it is fixed.

---

## 1. Epoch-9 gap settlement is best-effort if prices move (accepted)

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

## 2. Quiet multi-epoch rollover collapses epoch block numbers

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
  during the catch-up are stamped with the _catch-up event's_ block, because that
  is the only block in hand. The timestamps stay correct (they come from the
  stored schedule), so points and durations are unaffected — but a consumer
  reading epoch block ranges for a quiet span gets a degenerate range.

LP settlement is invoked per boundary with that boundary's `endTime`, so LP
points remain time-correct; only the block attribution degrades.

Fix would mean resolving the block at a boundary timestamp, which the indexer has
no cheap way to do. Low value unless block ranges become load-bearing.

---

## 3. Treasury mints drop the interest on the treasury's own position

**Severity:** low. **Status:** open.
**Where:** `src/handlers/tokenization.ts:112` and the treasury branch below it.

`userBalanceChange = event.params.value - event.params.balanceIncrease`, and
`totalATokenSupply` moves by that figure. For ordinary users the excluded
`balanceIncrease` is recovered later from the index-based balance recomputation
on their `UserReserve`. The treasury branch writes no `UserReserve`, so nothing
recovers it there.

So when `mintToTreasury` fires while the treasury _also_ holds a real aToken
position — which happens, since OTC returns are supplied `onBehalfOf` the
treasury — the interest accrued on that position is dropped from
`totalATokenSupply` and is never credited anywhere.

Impact is bounded by interest on the treasury's own balance between two treasury
mints, and it under-reports rather than inflates. Fixing it means tracking the
treasury's position properly rather than skipping user accounting for it, which
reopens the question of whether the treasury should appear as a supplier at all.

---

## 4. Also deferred

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

## 5. Point-accrual blacklist from Tide 9 (implemented)

25 addresses stop accruing points entirely from the start of Tide 9. Tides 1-8 keep
the values they were scored and paid under.

**Sources**, deduplicated in `POINT_ACCRUAL_BLACKLIST` (`src/helpers/constants.ts`):
the 14 entries of `neverland-tide-draw/blacklist.json` plus the 12 Neverland
Foundation multisigs. `0x909b176220b7e782C0f3cEccaB4b19D2c433c6BB` appears in both,
so the union is 25, not 26. Stored lowercase; every call site normalizes first.

**Boundary.** `POINT_ACCRUAL_BLACKLIST_FROM` is derived from
`EPOCH_DATES_OVERRIDES['9'].startTime` (1787893200 = 2026-08-28 05:00 UTC) rather
than duplicated, so the two can never drift apart.

Gated on timestamp only, not on a block, unlike the LP cutovers which pin both. The
LP eras need a block because they are also compared against block numbers elsewhere;
this has a single comparison site, and under sub-second block production two
independent signals can disagree at the boundary — the exact failure
`applyStaticLPPoolCutover` documents. One signal cannot disagree with itself.

**Gated call sites** — six, covering every automatic accrual path:
`settlePointsForUser` and the four `awardDaily*Points` functions in
`src/handlers/shared.ts`, and `updateUserEpochLPPoints` in `src/handlers/lp.ts`.
`settlePointsForUser` returns before touching any store, including the voting-power
refresh: multiplier state only feeds points these addresses cannot earn.

**Deliberately NOT gated: manual admin awards.** `LeaderboardConfig`'s manual
points-added / points-removed handlers (`src/handlers/leaderboard.ts`) still apply to
blacklisted addresses. Those are explicit, signed, on-chain admin actions; silently
voiding one would hide an operator's intent rather than enforce a policy. Revisit if
that is not wanted.

**Note on the pre-existing blacklist.** `LeaderboardBlacklist`, driven by the
on-chain `AddressBlacklisted` / `AddressUnblacklisted` events, is a separate
mechanism and still suppresses _ranking only_ — a user on it continues to accrue and
is merely hidden from the board. It was left as-is.

**No resync needed.** The change only affects Tide 9 onward, and the data being
validated against production is Tides 1-8. Points these addresses accrued between the
Tide 9 open and this landing will be corrected by the next full resync.
