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

  const totalDebt = reserve.totalCurrentVariableDebt + reserve.totalPrincipalStableDebt;
  const borrowsE8 = (totalDebt * priceE8) / decimalsBI;
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

  // Update ProtocolStats incrementally with revenue tracking
  await updateProtocolStatsIncremental(
    context,
    oldSuppliesUsd,
    oldBorrowsUsd,
    oldAvailableUsd,
    suppliesUsd,
    borrowsUsd,
    availableUsd,
    oldSuppliesE8,
    oldBorrowsE8,
    oldAvailableE8,
    suppliesE8,
    borrowsE8,
    availableE8,
    oldSuppliersInterestEarned,
    oldProtocolAccrued,
    reserve.lifetimeSuppliersInterestEarned,
    reserve.lifetimeReserveFactorAccrued,
    priceE8,
    decimals,
    timestamp
  );
}

/**
 * Incremental update to ProtocolStats when a Reserve's USD values change
 */
export async function updateProtocolStatsIncremental(
  context: handlerContext,
  oldSuppliesUsd: number,
  oldBorrowsUsd: number,
  oldAvailableUsd: number,
  newSuppliesUsd: number,
  newBorrowsUsd: number,
  newAvailableUsd: number,
  oldSuppliesE8: bigint,
  oldBorrowsE8: bigint,
  oldAvailableE8: bigint,
  newSuppliesE8: bigint,
  newBorrowsE8: bigint,
  newAvailableE8: bigint,
  oldSuppliersInterestEarned: bigint,
  oldProtocolAccrued: bigint,
  newSuppliersInterestEarned: bigint,
  newProtocolAccrued: bigint,
  priceE8: bigint,
  decimals: number,
  timestamp: number
): Promise<void> {
  let ps = await context.ProtocolStats.get('1');
  if (!ps) return;

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

  // Calculate revenue deltas from lifetime values
  const deltaProtocolToken = newProtocolAccrued - oldProtocolAccrued;
  const deltaSupplyToken = newSuppliersInterestEarned - oldSuppliersInterestEarned;

  const decimalsBI = 10n ** BigInt(decimals);

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

  const updatedProtocolRevenueUsd = ps.protocolRevenueUsd + deltaProtocolUsd;
  const updatedSupplyRevenueUsd = ps.supplyRevenueUsd + deltaSupplyUsd;
  const updatedTotalRevenueUsd = updatedProtocolRevenueUsd + updatedSupplyRevenueUsd;

  context.ProtocolStats.set({
    ...ps,
    suppliesUsd: updatedSuppliesUsd,
    borrowsUsd: updatedBorrowsUsd,
    availableUsd: updatedAvailableUsd,
    tvlUsd: updatedSuppliesUsd,
    suppliesE8: updatedSuppliesE8,
    borrowsE8: updatedBorrowsE8,
    availableE8: updatedAvailableE8,
    tvlE8: updatedSuppliesE8,
    protocolRevenueUsd: updatedProtocolRevenueUsd,
    supplyRevenueUsd: updatedSupplyRevenueUsd,
    totalRevenueUsd: updatedTotalRevenueUsd,
    updatedAt: timestamp,
  });
}
