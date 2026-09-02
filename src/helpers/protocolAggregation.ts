/**
 * Protocol USD Aggregation Helper
 * Calculates protocol-wide USD totals from Reserve data
 */

import type { handlerContext } from '../../generated';
import { ensureAssetPrice } from '../handlers/shared';

/**
 * Updates ProtocolStats USD aggregates by summing across all Reserves
 * Called whenever Reserve data changes (ReserveDataUpdated, Mint, Burn, etc.)
 */
export async function updateProtocolUsdAggregates(
  _context: handlerContext,
  _timestamp: number
): Promise<void> {
  // Aggregation happens incrementally in updateReserveUsdValues
  // This function is reserved for future full recalculation if needed
}

/**
 * One reserve's before/after figures for a single aggregation step.
 *
 * The protocol row and the pool row are moved by exactly this payload, so they
 * share one named type rather than two parallel positional signatures. With
 * nineteen same-typed numbers a transposed pair of arguments was invisible to
 * the compiler; as named fields it is a type error.
 */
export interface ReserveUsdDelta {
  oldSuppliesUsd: number;
  oldBorrowsUsd: number;
  oldAvailableUsd: number;
  newSuppliesUsd: number;
  newBorrowsUsd: number;
  newAvailableUsd: number;
  oldSuppliesE8: bigint;
  oldBorrowsE8: bigint;
  oldAvailableE8: bigint;
  newSuppliesE8: bigint;
  newBorrowsE8: bigint;
  newAvailableE8: bigint;
  oldSuppliersInterestEarned: bigint;
  oldProtocolAccrued: bigint;
  newSuppliersInterestEarned: bigint;
  newProtocolAccrued: bigint;
  priceE8: bigint;
  decimals: number;
  /**
   * Revenue deltas derived once by updateReserveUsdValues. The protocol row and
   * the pool row both need them and both used to recompute them from the same
   * six inputs; deriving once also guarantees the two round identically. Left
   * optional so either updater stays callable on its own.
   */
  revenueUsd?: { deltaProtocolUsd: number; deltaSupplyUsd: number };
}

/**
 * Updates USD values for a specific Reserve and its aggregate
 * Called from ReserveDataUpdated and other Reserve-modifying handlers
 */
export async function updateReserveUsdValues(
  context: handlerContext,
  reserveId: string,
  underlyingAsset: string,
  timestamp: number
): Promise<void> {
  const reserve = await context.Reserve.get(reserveId);
  if (!reserve) return;

  await ensureAssetPrice(context, underlyingAsset, timestamp);
  const priceOracle = await context.PriceOracleAsset.get(underlyingAsset);
  if (!priceOracle) return;
  const priceE8 = priceOracle.priceInEth;
  const priceUsd = Number(priceE8) / 1e8;
  const decimals = reserve.decimals;

  // Calculate utilization rate: 1 - (available / total)
  let utilizationRate = 0;
  if (reserve.totalLiquidity > 0n) {
    const utilized = reserve.totalLiquidity - reserve.availableLiquidity;
    const utilizationE8 = (utilized * 100000000n) / reserve.totalLiquidity;
    utilizationRate = Number(utilizationE8) / 1e8;
  }

  // Update Reserve price and utilization fields
  context.Reserve.set({
    ...reserve,
    priceInUsd: priceUsd,
    priceInUsdE8: priceE8,
    utilizationRate,
  });

  // Calculate USD values - EXACT match to subgraph logic
  // Formula: (amount * priceE8) / decimals / 1e8
  const decimalsBI = 10n ** BigInt(decimals);

  // Use totalATokenSupply for supplies (not totalLiquidity)
  const suppliesE8 = (reserve.totalATokenSupply * priceE8) / decimalsBI;
  const suppliesUsd = Number(suppliesE8) / 1e8;

  const debt = reserve.totalCurrentDebt;
  const borrowsE8 = (debt * priceE8) / decimalsBI;
  const borrowsUsd = Number(borrowsE8) / 1e8;

  const availableE8 = (reserve.availableLiquidity * priceE8) / decimalsBI;
  const availableUsd = Number(availableE8) / 1e8;

  // Track old values BEFORE updating aggregate
  let aggregate = await context.ReserveAggregate.get(reserveId);
  const oldSuppliesUsd = aggregate?.suppliesUsd || 0;
  const oldBorrowsUsd = aggregate?.borrowsUsd || 0;
  const oldAvailableUsd = aggregate?.availableUsd || 0;
  const oldSuppliesE8 = aggregate?.suppliesE8 || 0n;
  const oldBorrowsE8 = aggregate?.borrowsE8 || 0n;
  const oldAvailableE8 = aggregate?.availableE8 || 0n;
  const oldSuppliersInterestEarned = aggregate?.lastSuppliersInterestEarnedToken || 0n;
  const oldProtocolAccrued = aggregate?.lastProtocolAccruedToken || 0n;

  // Create or update aggregate with NEW values
  aggregate = {
    id: reserveId,
    suppliesUsd,
    borrowsUsd,
    availableUsd,
    suppliesE8,
    borrowsE8,
    availableE8,
    priceE8,
    lastSuppliersInterestEarnedToken: reserve.lifetimeSuppliersInterestEarned,
    lastProtocolAccruedToken: reserve.lifetimeReserveFactorAccrued,
    updatedAt: timestamp,
  };

  context.ReserveAggregate.set(aggregate);

  // Both rows move by exactly the same figures; building the payload once is
  // what keeps them from drifting apart.
  const delta: ReserveUsdDelta = {
    oldSuppliesUsd,
    oldBorrowsUsd,
    oldAvailableUsd,
    newSuppliesUsd: suppliesUsd,
    newBorrowsUsd: borrowsUsd,
    newAvailableUsd: availableUsd,
    oldSuppliesE8,
    oldBorrowsE8,
    oldAvailableE8,
    newSuppliesE8: suppliesE8,
    newBorrowsE8: borrowsE8,
    newAvailableE8: availableE8,
    oldSuppliersInterestEarned,
    oldProtocolAccrued,
    newSuppliersInterestEarned: reserve.lifetimeSuppliersInterestEarned,
    newProtocolAccrued: reserve.lifetimeReserveFactorAccrued,
    priceE8,
    decimals,
  };
  delta.revenueUsd = computeRevenueDeltasUsd(
    oldSuppliersInterestEarned,
    oldProtocolAccrued,
    reserve.lifetimeSuppliersInterestEarned,
    reserve.lifetimeReserveFactorAccrued,
    priceE8,
    decimals
  );

  const appliedToProtocol = await updateProtocolStatsIncremental(context, delta, timestamp);

  // Per-pool (per-market) breakdown: bucket the same deltas by pool_id. Skipped
  // when the protocol row did not take them, because a delta booked to one row
  // and not the other is exactly the drift the per-pool numbers are trusted not
  // to have.
  //
  // In practice this never skips: every caller of updateReserveUsdValues runs
  // recordProtocolTransaction first, which creates the ProtocolStats row. The
  // branch is a safety net against a future caller that does not, not a live
  // path where the per-pool rows deliberately lag the protocol row.
  if (appliedToProtocol) {
    await updatePoolStatsIncremental(context, reserve.pool_id, delta, timestamp);
  }
}

/**
 * Borrows over supplies for an aggregate scope, as a 0..1 fraction.
 *
 * Uses the E8 integers rather than the USD floats, so PoolStats and
 * ProtocolStats derive their rate identically and a market's rate is directly
 * comparable to the protocol's. Doing it in float USD instead would drift from
 * the integer totals as a market grows.
 *
 * Not comparable to `Reserve.utilizationRate`, which divides
 * `totalLiquidity - availableLiquidity` by `totalLiquidity` while this divides
 * debt by aToken supply. Those bases genuinely diverge — a flash loan credits
 * `availableLiquidity` and `totalATokenSupply` but not `totalLiquidity` — so
 * the two must not be mixed in one chart or reconciled against each other.
 *
 * Either side at or below zero yields 0: an empty market is 0% utilized, and
 * the incremental accounting can leave a dust-level negative total when the
 * last position in a market is closed. That applies to borrows as much as to
 * supplies, and a negative rate is never a meaningful answer.
 */
export function computeUtilizationRate(borrowsE8: bigint, suppliesE8: bigint): number {
  if (suppliesE8 <= 0n || borrowsE8 <= 0n) return 0;
  return Number((borrowsE8 * 100000000n) / suppliesE8) / 1e8;
}

/**
 * Converts a reserve's lifetime interest/fee movement into USD revenue deltas.
 *
 * Extracted so ProtocolStats and PoolStats derive revenue from one
 * implementation rather than two that can drift apart — the per-pool rows must
 * sum back to the protocol row, and that only holds if both round identically.
 *
 * Lifetime counters only ever grow, so a non-positive delta means "no new
 * revenue this event" (a re-org replay or an unchanged reserve), never negative
 * revenue.
 */
export function computeRevenueDeltasUsd(
  oldSuppliersInterestEarned: bigint,
  oldProtocolAccrued: bigint,
  newSuppliersInterestEarned: bigint,
  newProtocolAccrued: bigint,
  priceE8: bigint,
  decimals: number
): { deltaProtocolUsd: number; deltaSupplyUsd: number } {
  const decimalsBI = 10n ** BigInt(decimals);

  const deltaProtocolToken = newProtocolAccrued - oldProtocolAccrued;
  const deltaSupplyToken = newSuppliersInterestEarned - oldSuppliersInterestEarned;

  let deltaProtocolUsd = 0;
  if (deltaProtocolToken > 0n) {
    const deltaProtocolUsdE8 = (deltaProtocolToken * priceE8) / decimalsBI;
    deltaProtocolUsd = Number(deltaProtocolUsdE8) / 1e8;
  }

  let deltaSupplyUsd = 0;
  if (deltaSupplyToken > 0n) {
    const deltaSupplyUsdE8 = (deltaSupplyToken * priceE8) / decimalsBI;
    deltaSupplyUsd = Number(deltaSupplyUsdE8) / 1e8;
  }

  return { deltaProtocolUsd, deltaSupplyUsd };
}

/**
 * Incremental update to ProtocolStats when a Reserve's USD values change.
 *
 * Returns whether the delta was applied. A caller that mirrors the same delta
 * into another row must skip it when this returns false, or the two rows stop
 * agreeing.
 */
export async function updateProtocolStatsIncremental(
  context: handlerContext,
  delta: ReserveUsdDelta,
  timestamp: number
): Promise<boolean> {
  const {
    oldSuppliesUsd,
    oldBorrowsUsd,
    oldAvailableUsd,
    newSuppliesUsd,
    newBorrowsUsd,
    newAvailableUsd,
    oldSuppliesE8,
    oldBorrowsE8,
    oldAvailableE8,
    newSuppliesE8,
    newBorrowsE8,
    newAvailableE8,
    oldSuppliersInterestEarned,
    oldProtocolAccrued,
    newSuppliersInterestEarned,
    newProtocolAccrued,
    priceE8,
    decimals,
  } = delta;

  const ps = await context.ProtocolStats.get('1');
  if (!ps) return false;

  const suppliesDelta = newSuppliesUsd - oldSuppliesUsd;
  const borrowsDelta = newBorrowsUsd - oldBorrowsUsd;
  const availableDelta = newAvailableUsd - oldAvailableUsd;

  const suppliesE8Delta = newSuppliesE8 - oldSuppliesE8;
  const borrowsE8Delta = newBorrowsE8 - oldBorrowsE8;
  const availableE8Delta = newAvailableE8 - oldAvailableE8;

  const updatedSuppliesUsd = ps.suppliesUsd + suppliesDelta;
  const updatedBorrowsUsd = ps.borrowsUsd + borrowsDelta;
  const updatedAvailableUsd = ps.availableUsd + availableDelta;
  const updatedSuppliesE8 = ps.suppliesE8 + suppliesE8Delta;
  const updatedBorrowsE8 = ps.borrowsE8 + borrowsE8Delta;
  const updatedAvailableE8 = ps.availableE8 + availableE8Delta;
  const updatedCombinedSuppliesUsd = ps.combinedSuppliesUsd + suppliesDelta;
  const updatedCombinedBorrowsUsd = ps.combinedBorrowsUsd + borrowsDelta;
  const updatedCombinedAvailableUsd = ps.combinedAvailableUsd + availableDelta;
  const updatedCombinedSuppliesE8 = ps.combinedSuppliesE8 + suppliesE8Delta;
  const updatedCombinedBorrowsE8 = ps.combinedBorrowsE8 + borrowsE8Delta;
  const updatedCombinedAvailableE8 = ps.combinedAvailableE8 + availableE8Delta;

  // Calculate revenue deltas from lifetime values
  const { deltaProtocolUsd, deltaSupplyUsd } =
    delta.revenueUsd ??
    computeRevenueDeltasUsd(
      oldSuppliersInterestEarned,
      oldProtocolAccrued,
      newSuppliersInterestEarned,
      newProtocolAccrued,
      priceE8,
      decimals
    );

  const updatedProtocolRevenueUsd = ps.protocolRevenueUsd + deltaProtocolUsd;
  const updatedSupplyRevenueUsd = ps.supplyRevenueUsd + deltaSupplyUsd;
  const updatedTotalRevenueUsd = updatedProtocolRevenueUsd + updatedSupplyRevenueUsd;

  context.ProtocolStats.set({
    ...ps,
    suppliesUsd: updatedSuppliesUsd,
    borrowsUsd: updatedBorrowsUsd,
    availableUsd: updatedAvailableUsd,
    tvlUsd: updatedSuppliesUsd,
    combinedSuppliesUsd: updatedCombinedSuppliesUsd,
    combinedBorrowsUsd: updatedCombinedBorrowsUsd,
    combinedAvailableUsd: updatedCombinedAvailableUsd,
    combinedTvlUsd: updatedCombinedSuppliesUsd,
    suppliesE8: updatedSuppliesE8,
    borrowsE8: updatedBorrowsE8,
    availableE8: updatedAvailableE8,
    tvlE8: updatedSuppliesE8,
    combinedSuppliesE8: updatedCombinedSuppliesE8,
    combinedBorrowsE8: updatedCombinedBorrowsE8,
    combinedAvailableE8: updatedCombinedAvailableE8,
    combinedTvlE8: updatedCombinedSuppliesE8,
    utilizationRate: computeUtilizationRate(updatedBorrowsE8, updatedSuppliesE8),
    protocolRevenueUsd: updatedProtocolRevenueUsd,
    supplyRevenueUsd: updatedSupplyRevenueUsd,
    totalRevenueUsd: updatedTotalRevenueUsd,
    updatedAt: timestamp,
  });

  return true;
}

/**
 * Incremental update to per-pool PoolStats (one row per market, keyed by
 * pool_id). Applies the same per-reserve deltas as the protocol-wide stats, but
 * bucketed to the reserve's own pool — so ProtocolStats stays the all-pool total
 * while PoolStats gives the per-market breakdown (Global Markets, Isolated
 * Pendle AUSD, ...). tvl == supplies, matching ProtocolStats.
 */
export async function updatePoolStatsIncremental(
  context: handlerContext,
  poolId: string,
  delta: ReserveUsdDelta,
  timestamp: number
): Promise<void> {
  const {
    oldSuppliesUsd,
    oldBorrowsUsd,
    oldAvailableUsd,
    newSuppliesUsd,
    newBorrowsUsd,
    newAvailableUsd,
    oldSuppliesE8,
    oldBorrowsE8,
    oldAvailableE8,
    newSuppliesE8,
    newBorrowsE8,
    newAvailableE8,
    oldSuppliersInterestEarned,
    oldProtocolAccrued,
    newSuppliersInterestEarned,
    newProtocolAccrued,
    priceE8,
    decimals,
  } = delta;

  const id = poolId.toLowerCase();
  const existing = await context.PoolStats.get(id);
  const base = existing ?? emptyPoolStats(id, timestamp);

  const suppliesUsd = base.suppliesUsd + (newSuppliesUsd - oldSuppliesUsd);
  const borrowsUsd = base.borrowsUsd + (newBorrowsUsd - oldBorrowsUsd);
  const availableUsd = base.availableUsd + (newAvailableUsd - oldAvailableUsd);
  const suppliesE8 = base.suppliesE8 + (newSuppliesE8 - oldSuppliesE8);
  const borrowsE8 = base.borrowsE8 + (newBorrowsE8 - oldBorrowsE8);
  const availableE8 = base.availableE8 + (newAvailableE8 - oldAvailableE8);

  // Same derivation the protocol row uses, so the per-pool rows sum back to it.
  const { deltaProtocolUsd, deltaSupplyUsd } =
    delta.revenueUsd ??
    computeRevenueDeltasUsd(
      oldSuppliersInterestEarned,
      oldProtocolAccrued,
      newSuppliersInterestEarned,
      newProtocolAccrued,
      priceE8,
      decimals
    );

  const protocolRevenueUsd = base.protocolRevenueUsd + deltaProtocolUsd;
  const supplyRevenueUsd = base.supplyRevenueUsd + deltaSupplyUsd;
  const totalRevenueUsd = protocolRevenueUsd + supplyRevenueUsd;

  const updated = {
    ...base,
    suppliesUsd,
    borrowsUsd,
    availableUsd,
    tvlUsd: suppliesUsd,
    suppliesE8,
    borrowsE8,
    availableE8,
    tvlE8: suppliesE8,
    utilizationRate: computeUtilizationRate(borrowsE8, suppliesE8),
    protocolRevenueUsd,
    supplyRevenueUsd,
    totalRevenueUsd,
    updatedAt: timestamp,
  };

  context.PoolStats.set(updated);
  writePoolStatsSnapshot(context, updated, timestamp);
}

/** A zeroed PoolStats row, so every creation site agrees on the defaults. */
function emptyPoolStats(id: string, timestamp: number) {
  return {
    id,
    suppliesUsd: 0,
    borrowsUsd: 0,
    availableUsd: 0,
    tvlUsd: 0,
    suppliesE8: 0n,
    borrowsE8: 0n,
    availableE8: 0n,
    tvlE8: 0n,
    utilizationRate: 0,
    totalRevenueUsd: 0,
    supplyRevenueUsd: 0,
    protocolRevenueUsd: 0,
    reserveCount: 0,
    updatedAt: timestamp,
  };
}

/**
 * Append this market's current aggregates to its time series.
 *
 * Keyed by second so several updates within one block collapse to a single row,
 * exactly as ProtocolStatsSnapshot does.
 */
function writePoolStatsSnapshot(
  context: handlerContext,
  stats: ReturnType<typeof emptyPoolStats>,
  timestamp: number
): void {
  context.PoolStatsSnapshot.set({
    id: `${stats.id}-${timestamp}`,
    pool_id: stats.id,
    timestamp,
    suppliesUsd: stats.suppliesUsd,
    borrowsUsd: stats.borrowsUsd,
    availableUsd: stats.availableUsd,
    tvlUsd: stats.tvlUsd,
    suppliesE8: stats.suppliesE8,
    borrowsE8: stats.borrowsE8,
    availableE8: stats.availableE8,
    tvlE8: stats.tvlE8,
    utilizationRate: stats.utilizationRate,
    totalRevenueUsd: stats.totalRevenueUsd,
    supplyRevenueUsd: stats.supplyRevenueUsd,
    protocolRevenueUsd: stats.protocolRevenueUsd,
    reserveCount: stats.reserveCount,
  });
}

/**
 * Adjust a market's live reserve count and snapshot the result.
 *
 * Called from the reserve lifecycle rather than the USD path: a reserve can be
 * listed or dropped without any balance moving, and that still changes the shape
 * of the market. Creates the PoolStats row when a market's first reserve is
 * listed before any USD aggregation has run for it.
 */
export async function adjustPoolReserveCount(
  context: handlerContext,
  poolId: string,
  delta: number,
  timestamp: number
): Promise<void> {
  const id = poolId.toLowerCase();
  const existing = await context.PoolStats.get(id);
  const base = existing ?? emptyPoolStats(id, timestamp);

  // Never let a double-drop or an out-of-order replay push the count negative.
  // The isCountedReserve transition test in config.ts should make that
  // impossible, so a clamp that actually fires means the count has desynced
  // from the reserve rows - log it rather than absorb it silently, because a
  // wrong count is invisible once it is in the historical series.
  const rawCount = base.reserveCount + delta;
  const reserveCount = Math.max(0, rawCount);
  if (rawCount < 0) {
    context.log.warn(
      `PoolStats.reserveCount for ${id} went negative (${rawCount}) applying delta ${delta}; clamped to 0`
    );
  }

  const updated = { ...base, reserveCount, updatedAt: timestamp };

  context.PoolStats.set(updated);
  writePoolStatsSnapshot(context, updated, timestamp);
}
