/**
 * Pool Event Handlers
 * Supply, Borrow, Repay, Withdraw, Liquidation, FlashLoan, etc.
 */

import type { handlerContext } from '../../generated';
import { Pool } from '../../generated';
import {
  recordProtocolTransaction,
  getOrCreateUser,
  getAssetPriceUSD,
  getReserveNormalizedIncome,
  getReserveNormalizedVariableDebt,
} from './shared';
import { calculateGrowth } from '../helpers/math';
import { getHistoryEntityId } from '../helpers/entityHelpers';
import { updateReserveUsdValues } from '../helpers/protocolAggregation';
import { normalizeAddress } from '../helpers/constants';

async function resolvePoolId(context: handlerContext, contractAddress: string): Promise<string> {
  const normalized = normalizeAddress(contractAddress);
  const mapping = await context.ContractToPoolMapping.get(normalized);
  return mapping?.pool_id || normalized;
}

async function maybeStoreEpochEndReserveSnapshot(
  context: handlerContext,
  reserve: {
    variableBorrowRate: bigint;
    variableBorrowIndex: bigint;
    utilizationRate: number;
    stableBorrowRate: bigint;
    averageStableRate: bigint;
    liquidityIndex: bigint;
    liquidityRate: bigint;
    totalLiquidity: bigint;
    totalATokenSupply: bigint;
    totalLiquidityAsCollateral: bigint;
    availableLiquidity: bigint;
    priceInUsdE8: bigint;
    priceInUsd: number;
    accruedToTreasury: bigint;
    totalScaledVariableDebt: bigint;
    totalCurrentVariableDebt: bigint;
    totalPrincipalStableDebt: bigint;
    lifetimePrincipalStableDebt: bigint;
    lifetimeScaledVariableDebt: bigint;
    lifetimeCurrentVariableDebt: bigint;
    lifetimeLiquidity: bigint;
    lifetimeRepayments: bigint;
    lifetimeWithdrawals: bigint;
    lifetimeBorrows: bigint;
    lifetimeLiquidated: bigint;
    lifetimeFlashLoans: bigint;
    lifetimeFlashLoanPremium: bigint;
    lifetimeFlashLoanLPPremium: bigint;
    lifetimeFlashLoanProtocolPremium: bigint;
    lifetimeReserveFactorAccrued: bigint;
    lifetimePortalLPFee: bigint;
    lifetimePortalProtocolFee: bigint;
    lifetimeSuppliersInterestEarned: bigint;
    lastUpdateTimestamp: number;
  },
  reserveId: string,
  timestamp: number
): Promise<void> {
  const state = await context.LeaderboardState.get('current');
  if (!state || state.currentEpochNumber === 0n || state.isActive) {
    return;
  }

  const epoch = await context.LeaderboardEpoch.get(state.currentEpochNumber.toString());
  if (!epoch || epoch.endTime === undefined) return;

  const epochEndTime = epoch.endTime;
  if (timestamp <= epochEndTime) return;
  if (reserve.lastUpdateTimestamp > epochEndTime) return;

  const snapshotId = `epochEnd:${state.currentEpochNumber.toString()}:${reserveId}`;
  const existing = await context.ReserveParamsHistoryItem.get(snapshotId);
  if (existing) return;

  const liquidityIndexAtEnd = getReserveNormalizedIncome(reserve, epochEndTime);
  const variableBorrowIndexAtEnd = getReserveNormalizedVariableDebt(reserve, epochEndTime);

  // Snapshot reserve indices at epoch end for accurate gap settlements.
  context.ReserveParamsHistoryItem.set({
    id: snapshotId,
    reserve_id: reserveId,
    variableBorrowRate: reserve.variableBorrowRate,
    variableBorrowIndex: variableBorrowIndexAtEnd,
    utilizationRate: reserve.utilizationRate,
    stableBorrowRate: reserve.stableBorrowRate,
    averageStableBorrowRate: reserve.averageStableRate,
    liquidityIndex: liquidityIndexAtEnd,
    liquidityRate: reserve.liquidityRate,
    totalLiquidity: reserve.totalLiquidity,
    totalATokenSupply: reserve.totalATokenSupply,
    totalLiquidityAsCollateral: reserve.totalLiquidityAsCollateral,
    availableLiquidity: reserve.availableLiquidity,
    priceInEth: reserve.priceInUsdE8,
    priceInUsd: reserve.priceInUsd,
    timestamp: epochEndTime,
    accruedToTreasury: reserve.accruedToTreasury,
    totalScaledVariableDebt: reserve.totalScaledVariableDebt,
    totalCurrentVariableDebt: reserve.totalCurrentVariableDebt,
    totalPrincipalStableDebt: reserve.totalPrincipalStableDebt,
    lifetimePrincipalStableDebt: reserve.lifetimePrincipalStableDebt,
    lifetimeScaledVariableDebt: reserve.lifetimeScaledVariableDebt,
    lifetimeCurrentVariableDebt: reserve.lifetimeCurrentVariableDebt,
    lifetimeLiquidity: reserve.lifetimeLiquidity,
    lifetimeRepayments: reserve.lifetimeRepayments,
    lifetimeWithdrawals: reserve.lifetimeWithdrawals,
    lifetimeBorrows: reserve.lifetimeBorrows,
    lifetimeLiquidated: reserve.lifetimeLiquidated,
    lifetimeFlashLoans: reserve.lifetimeFlashLoans,
    lifetimeFlashLoanPremium: reserve.lifetimeFlashLoanPremium,
    lifetimeFlashLoanLPPremium: reserve.lifetimeFlashLoanLPPremium,
    lifetimeFlashLoanProtocolPremium: reserve.lifetimeFlashLoanProtocolPremium,
    lifetimeReserveFactorAccrued: reserve.lifetimeReserveFactorAccrued,
    lifetimePortalLPFee: reserve.lifetimePortalLPFee,
    lifetimePortalProtocolFee: reserve.lifetimePortalProtocolFee,
    lifetimeSuppliersInterestEarned: reserve.lifetimeSuppliersInterestEarned,
  });
}

Pool.Supply.handler(async ({ event, context }) => {
  try {
    // context.log.debug(`Processing Supply event for user ${event.params.onBehalfOf}`);

    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );

    const poolId = await resolvePoolId(context, event.srcAddress);
    const reserveAddress = normalizeAddress(event.params.reserve);
    const reserveId = `${reserveAddress}-${poolId}`;
    const userId = normalizeAddress(event.params.onBehalfOf);
    const userReserveId = `${userId}-${reserveId}`;

    await getOrCreateUser(context, userId);
    await getOrCreateUser(context, normalizeAddress(event.params.user));

    if (event.params.referralCode > 0) {
      const referrerId = event.params.referralCode.toString();
      const referrer = await context.Referrer.get(referrerId);
      if (!referrer) {
        context.Referrer.set({ id: referrerId });
      }
    }

    const assetPriceUSD = await getAssetPriceUSD(
      context,
      reserveAddress,
      Number(event.block.timestamp)
    );

    const id = `${event.transaction.hash}-${event.logIndex}`;
    context.Supply.set({
      id,
      txHash: event.transaction.hash,
      action: 'Supply',
      pool_id: poolId,
      user_id: userId,
      caller_id: normalizeAddress(event.params.user),
      reserve_id: reserveId,
      referrer_id: event.params.referralCode > 0 ? event.params.referralCode.toString() : undefined,
      userReserve_id: userReserveId,
      amount: event.params.amount,
      timestamp: Number(event.block.timestamp),
      assetPriceUSD,
    });
    /* c8 ignore start */
  } catch (error) {
    context.log.error(`Failed to process Supply event: ${error}`);
    throw error;
  }
  /* c8 ignore end */
});

Pool.Withdraw.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  // We intentionally avoid creating RedeemUnderlying here because:
  // - event.params.user can be the caller, not the actual aToken holder.
  // - gateway withdrawals emit the gateway as user.
  // The AToken.Burn handler creates RedeemUnderlying with the correct owner.
});

Pool.Borrow.handler(async ({ event, context }) => {
  try {
    // context.log.debug(`Processing Borrow event for user ${event.params.onBehalfOf}`);

    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );

    const poolId = await resolvePoolId(context, event.srcAddress);
    const reserveAddress = normalizeAddress(event.params.reserve);
    const reserveId = `${reserveAddress}-${poolId}`;
    const userId = normalizeAddress(event.params.onBehalfOf);
    const userReserveId = `${userId}-${reserveId}`;

    await getOrCreateUser(context, userId);
    await getOrCreateUser(context, normalizeAddress(event.params.user));

    if (event.params.referralCode > 0) {
      const referrerId = event.params.referralCode.toString();
      const referrer = await context.Referrer.get(referrerId);
      if (!referrer) {
        context.Referrer.set({ id: referrerId });
      }
    }

    const userReserve = await context.UserReserve.get(userReserveId);
    const stableTokenDebt = userReserve?.principalStableDebt || 0n;
    const variableTokenDebt = userReserve?.scaledVariableDebt || 0n;
    const assetPriceUSD = await getAssetPriceUSD(
      context,
      reserveAddress,
      Number(event.block.timestamp)
    );

    const id = `${event.transaction.hash}-${event.logIndex}`;
    context.Borrow.set({
      id,
      txHash: event.transaction.hash,
      action: 'Borrow',
      pool_id: poolId,
      user_id: userId,
      caller_id: normalizeAddress(event.params.user),
      reserve_id: reserveId,
      userReserve_id: userReserveId,
      amount: event.params.amount,
      borrowRate: event.params.borrowRate,
      borrowRateMode: Number(event.params.interestRateMode),
      referrer_id: event.params.referralCode ? event.params.referralCode.toString() : undefined,
      stableTokenDebt,
      variableTokenDebt,
      assetPriceUSD,
      timestamp: Number(event.block.timestamp),
    });
    /* c8 ignore start */
  } catch (error) {
    context.log.error(`Failed to process Borrow event: ${error}`);
    throw error;
  }
  /* c8 ignore end */
});

Pool.Repay.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.reserve);
  const reserveId = `${reserveAddress}-${poolId}`;
  const userId = normalizeAddress(event.params.user);
  const userReserveId = `${userId}-${reserveId}`;

  await getOrCreateUser(context, userId);
  await getOrCreateUser(context, normalizeAddress(event.params.repayer));

  const assetPriceUSD = await getAssetPriceUSD(
    context,
    reserveAddress,
    Number(event.block.timestamp)
  );

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.Repay.set({
    id,
    txHash: event.transaction.hash,
    action: 'Repay',
    pool_id: poolId,
    user_id: userId,
    repayer_id: normalizeAddress(event.params.repayer),
    reserve_id: reserveId,
    userReserve_id: userReserveId,
    amount: event.params.amount,
    useATokens: event.params.useATokens,
    assetPriceUSD,
    timestamp: Number(event.block.timestamp),
  });
});

Pool.FlashLoan.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.asset);
  const reserveId = `${reserveAddress}-${poolId}`;

  await getOrCreateUser(context, normalizeAddress(event.params.initiator));

  const reserve = await context.Reserve.get(reserveId);
  const pool = await context.Pool.get(poolId);

  const premium = event.params.premium;
  let lpFee = 0n;
  let protocolFee = 0n;

  if (pool && pool.flashloanPremiumToProtocol) {
    protocolFee = (premium * pool.flashloanPremiumToProtocol + 5000n) / 10000n;
    lpFee = premium - protocolFee;
  }

  if (reserve) {
    context.Reserve.set({
      ...reserve,
      availableLiquidity: reserve.availableLiquidity + premium,
      totalATokenSupply: reserve.totalATokenSupply + premium,
      lifetimeFlashLoans: reserve.lifetimeFlashLoans + event.params.amount,
      lifetimeFlashLoanPremium: reserve.lifetimeFlashLoanPremium + premium,
      lifetimeFlashLoanLPPremium: reserve.lifetimeFlashLoanLPPremium + lpFee,
      lifetimeFlashLoanProtocolPremium: reserve.lifetimeFlashLoanProtocolPremium + protocolFee,
    });
  }

  const assetPriceUSD = await getAssetPriceUSD(
    context,
    reserveAddress,
    Number(event.block.timestamp)
  );

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.FlashLoan.set({
    id,
    pool_id: poolId,
    reserve_id: reserveId,
    target: normalizeAddress(event.params.target),
    initiator_id: normalizeAddress(event.params.initiator),
    amount: event.params.amount,
    totalFee: event.params.premium,
    lpFee,
    protocolFee,
    assetPriceUSD,
    timestamp: Number(event.block.timestamp),
  });
});

Pool.LiquidationCall.handler(async ({ event, context }) => {
  try {
    // context.log.info(
    //   `Processing Liquidation for user ${event.params.user} by ${event.params.liquidator}`
    // );

    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );

    const poolId = await resolvePoolId(context, event.srcAddress);
    const collateralAsset = normalizeAddress(event.params.collateralAsset);
    const debtAsset = normalizeAddress(event.params.debtAsset);
    const collateralReserveId = `${collateralAsset}-${poolId}`;
    const debtReserveId = `${debtAsset}-${poolId}`;
    const userId = normalizeAddress(event.params.user);

    const collateralReserve = await context.Reserve.get(collateralReserveId);
    if (collateralReserve) {
      context.Reserve.set({
        ...collateralReserve,
        lifetimeLiquidated:
          collateralReserve.lifetimeLiquidated + event.params.liquidatedCollateralAmount,
      });
    }

    await getOrCreateUser(context, userId);
    await getOrCreateUser(context, normalizeAddress(event.params.liquidator));

    const collateralAssetPriceUSD = await getAssetPriceUSD(
      context,
      collateralAsset,
      Number(event.block.timestamp)
    );
    const borrowAssetPriceUSD = await getAssetPriceUSD(
      context,
      debtAsset,
      Number(event.block.timestamp)
    );

    const id = `${event.transaction.hash}-${event.logIndex}`;
    context.LiquidationCall.set({
      id,
      txHash: event.transaction.hash,
      action: 'LiquidationCall',
      pool_id: poolId,
      user_id: userId,
      collateralReserve_id: collateralReserveId,
      principalReserve_id: debtReserveId,
      collateralUserReserve_id: `${userId}-${collateralReserveId}`,
      principalUserReserve_id: `${userId}-${debtReserveId}`,
      collateralAmount: event.params.liquidatedCollateralAmount,
      principalAmount: event.params.debtToCover,
      liquidator: normalizeAddress(event.params.liquidator),
      collateralAssetPriceUSD,
      borrowAssetPriceUSD,
      timestamp: Number(event.block.timestamp),
    });
    /* c8 ignore start */
  } catch (error) {
    context.log.error(`Failed to process LiquidationCall event: ${error}`);
    throw error;
  }
  /* c8 ignore end */
});

Pool.ReserveUsedAsCollateralEnabled.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.reserve);
  const reserveId = `${reserveAddress}-${poolId}`;
  const userId = normalizeAddress(event.params.user);
  const userReserveId = `${userId}-${reserveId}`;

  await getOrCreateUser(context, userId);
  let userReserve = await context.UserReserve.get(userReserveId);
  if (userReserve) {
    const historyId = getHistoryEntityId(event.transaction.hash, Number(event.logIndex));
    context.UsageAsCollateral.set({
      id: historyId,
      txHash: event.transaction.hash,
      action: 'UsageAsCollateral',
      pool_id: poolId,
      user_id: userId,
      reserve_id: reserveId,
      userReserve_id: userReserveId,
      fromState: userReserve.usageAsCollateralEnabledOnUser,
      toState: true,
      timestamp: Number(event.block.timestamp),
    });

    context.UserReserve.set({
      ...userReserve,
      usageAsCollateralEnabledOnUser: true,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });

    // Update Reserve.totalLiquidityAsCollateral when collateral is enabled
    if (!userReserve.usageAsCollateralEnabledOnUser) {
      const reserve = await context.Reserve.get(reserveId);
      if (reserve) {
        const userBalance = userReserve.currentATokenBalance;
        context.Reserve.set({
          ...reserve,
          totalLiquidityAsCollateral: reserve.totalLiquidityAsCollateral + userBalance,
        });
      }
    }
  }
});

Pool.ReserveUsedAsCollateralDisabled.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.reserve);
  const reserveId = `${reserveAddress}-${poolId}`;
  const userId = normalizeAddress(event.params.user);
  const userReserveId = `${userId}-${reserveId}`;

  await getOrCreateUser(context, userId);
  let userReserve = await context.UserReserve.get(userReserveId);
  if (userReserve) {
    const historyId = getHistoryEntityId(event.transaction.hash, Number(event.logIndex));
    context.UsageAsCollateral.set({
      id: historyId,
      txHash: event.transaction.hash,
      action: 'UsageAsCollateral',
      pool_id: poolId,
      user_id: userId,
      reserve_id: reserveId,
      userReserve_id: userReserveId,
      fromState: userReserve.usageAsCollateralEnabledOnUser,
      toState: false,
      timestamp: Number(event.block.timestamp),
    });

    context.UserReserve.set({
      ...userReserve,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });

    // Update Reserve.totalLiquidityAsCollateral when collateral is disabled
    if (userReserve.usageAsCollateralEnabledOnUser) {
      const reserve = await context.Reserve.get(reserveId);
      if (reserve) {
        const userBalance = userReserve.currentATokenBalance;
        context.Reserve.set({
          ...reserve,
          totalLiquidityAsCollateral:
            reserve.totalLiquidityAsCollateral > userBalance
              ? reserve.totalLiquidityAsCollateral - userBalance
              : 0n,
        });
      }
    }
  }
});

Pool.ReserveDataUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.reserve);
  const reserveId = `${reserveAddress}-${poolId}`;

  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    await maybeStoreEpochEndReserveSnapshot(
      context,
      reserve,
      reserveId,
      Number(event.block.timestamp)
    );

    // Calculate interest growth between updates
    const prevTimestamp = BigInt(reserve.lastUpdateTimestamp);
    const currentTimestamp = BigInt(Number(event.block.timestamp));

    let growth = 0n;
    if (currentTimestamp > prevTimestamp) {
      growth = calculateGrowth(
        reserve.totalATokenSupply,
        reserve.liquidityRate,
        prevTimestamp,
        currentTimestamp
      );
    }

    const newTotalATokenSupply = reserve.totalATokenSupply + growth;
    const newLifetimeSuppliersInterest = reserve.lifetimeSuppliersInterestEarned + growth;

    context.Reserve.set({
      ...reserve,
      liquidityRate: event.params.liquidityRate,
      stableBorrowRate: event.params.stableBorrowRate,
      variableBorrowRate: event.params.variableBorrowRate,
      liquidityIndex: event.params.liquidityIndex,
      variableBorrowIndex: event.params.variableBorrowIndex,
      totalATokenSupply: newTotalATokenSupply,
      lifetimeSuppliersInterestEarned: newLifetimeSuppliersInterest,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });

    // Update USD aggregates with new accrued interest
    await updateReserveUsdValues(context, reserveId, reserveAddress, Number(event.block.timestamp));
  }
});

Pool.MintedToTreasury.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.reserve);
  const reserveId = `${reserveAddress}-${poolId}`;

  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    const historyId = getHistoryEntityId(event.transaction.hash, Number(event.logIndex));
    context.MintedToTreasury.set({
      id: historyId,
      pool_id: poolId,
      reserve_id: reserveId,
      amount: event.params.amountMinted,
      timestamp: Number(event.block.timestamp),
    });

    const newProtocolAccrued = reserve.lifetimeReserveFactorAccrued + event.params.amountMinted;

    context.Reserve.set({
      ...reserve,
      lifetimeReserveFactorAccrued: newProtocolAccrued,
    });

    const ps = await context.ProtocolStats.get('1');
    if (ps) {
      const decimalsBI = 10n ** BigInt(reserve.decimals);
      const priceUsd =
        reserve.priceInUsd ||
        (await getAssetPriceUSD(context, reserveAddress, Number(event.block.timestamp)));
      const deltaProtocolUsd = (Number(event.params.amountMinted) / Number(decimalsBI)) * priceUsd;
      const updatedProtocolRevenueUsd = ps.protocolRevenueUsd + deltaProtocolUsd;
      context.ProtocolStats.set({
        ...ps,
        protocolRevenueUsd: updatedProtocolRevenueUsd,
        totalRevenueUsd: updatedProtocolRevenueUsd + ps.supplyRevenueUsd,
        updatedAt: Number(event.block.timestamp),
      });
    }

    const aggregate = await context.ReserveAggregate.get(reserveId);
    if (aggregate) {
      context.ReserveAggregate.set({
        ...aggregate,
        lastProtocolAccruedToken: newProtocolAccrued,
        updatedAt: Number(event.block.timestamp),
      });
    } else {
      context.ReserveAggregate.set({
        id: reserveId,
        suppliesUsd: 0,
        borrowsUsd: 0,
        availableUsd: 0,
        suppliesE8: 0n,
        borrowsE8: 0n,
        availableE8: 0n,
        priceE8: 0n,
        lastSuppliersInterestEarnedToken: 0n,
        lastProtocolAccruedToken: newProtocolAccrued,
        updatedAt: Number(event.block.timestamp),
      });
    }
  }
});

Pool.UserEModeSet.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const userId = normalizeAddress(event.params.user);
  await getOrCreateUser(context, userId);
  let user = await context.User.get(userId);
  if (user) {
    context.User.set({
      ...user,
      eModeCategoryId_id: event.params.categoryId.toString(),
    });
  }

  const historyId = getHistoryEntityId(event.transaction.hash, Number(event.logIndex));
  context.UserEModeSet.set({
    id: historyId,
    txHash: event.transaction.hash,
    action: 'UserEModeSet',
    user_id: userId,
    timestamp: Number(event.block.timestamp),
    categoryId: Number(event.params.categoryId),
  });
});

Pool.MintUnbacked.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.reserve);
  const reserveId = `${reserveAddress}-${poolId}`;
  const userId = normalizeAddress(event.params.onBehalfOf);

  await getOrCreateUser(context, userId);
  await getOrCreateUser(context, normalizeAddress(event.params.user));

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.MintUnbacked.set({
    id,
    pool_id: poolId,
    user_id: userId,
    caller_id: normalizeAddress(event.params.user),
    reserve_id: reserveId,
    amount: event.params.amount,
    referral: Number(event.params.referralCode),
    userReserve_id: `${userId}-${reserveId}`,
    timestamp: Number(event.block.timestamp),
  });

  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    context.Reserve.set({
      ...reserve,
      unbacked: reserve.unbacked + event.params.amount,
    });
  }
});

Pool.BackUnbacked.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.reserve);
  const reserveId = `${reserveAddress}-${poolId}`;

  await getOrCreateUser(context, normalizeAddress(event.params.backer));

  const pool = await context.Pool.get(poolId);
  const premium = event.params.fee;
  const bridgeFee = pool?.bridgeProtocolFee || 0n;
  const protocolFee = (premium * bridgeFee + 5000n) / 10000n;
  const lpFee = premium - protocolFee;

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.BackUnbacked.set({
    id,
    pool_id: poolId,
    reserve_id: reserveId,
    backer_id: normalizeAddress(event.params.backer),
    amount: event.params.amount,
    fee: event.params.fee,
    lpFee,
    protocolFee,
    userReserve_id: `${normalizeAddress(event.params.backer)}-${reserveId}`,
    timestamp: Number(event.block.timestamp),
  });

  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    const newUnbacked =
      reserve.unbacked > event.params.amount ? reserve.unbacked - event.params.amount : 0n;
    context.Reserve.set({
      ...reserve,
      unbacked: newUnbacked,
      lifetimePortalLPFee: reserve.lifetimePortalLPFee + lpFee,
      lifetimePortalProtocolFee: reserve.lifetimePortalProtocolFee + protocolFee,
    });
  }
});

Pool.IsolationModeTotalDebtUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const assetAddress = normalizeAddress(event.params.asset);
  const reserveId = `${assetAddress}-${poolId}`;
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.IsolationModeTotalDebtUpdated.set({
    id,
    isolatedDebt: event.params.totalDebt,
    pool_id: poolId,
    reserve_id: reserveId,
    timestamp: Number(event.block.timestamp),
  });
});

Pool.SwapBorrowRateMode.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.reserve);
  const reserveId = `${reserveAddress}-${poolId}`;
  const userId = normalizeAddress(event.params.user);
  const userReserveId = `${userId}-${reserveId}`;

  await getOrCreateUser(context, userId);

  const reserve = await context.Reserve.get(reserveId);

  const id = `${event.transaction.hash}-${event.logIndex}`;

  // Determine swap direction: 1 = stable, 2 = variable
  const borrowRateModeFrom = Number(event.params.interestRateMode);
  const borrowRateModeTo = borrowRateModeFrom === 1 ? 2 : 1;

  context.SwapBorrowRate.set({
    id,
    txHash: event.transaction.hash,
    action: 'SwapBorrowRate',
    pool_id: poolId,
    user_id: userId,
    reserve_id: reserveId,
    userReserve_id: userReserveId,
    borrowRateModeFrom,
    borrowRateModeTo,
    stableBorrowRate: reserve?.stableBorrowRate || 0n,
    variableBorrowRate: reserve?.variableBorrowRate || 0n,
    timestamp: Number(event.block.timestamp),
  });
});

Pool.RebalanceStableBorrowRate.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const poolId = await resolvePoolId(context, event.srcAddress);
  const reserveAddress = normalizeAddress(event.params.reserve);
  const reserveId = `${reserveAddress}-${poolId}`;
  const userId = normalizeAddress(event.params.user);
  const userReserveId = `${userId}-${reserveId}`;

  await getOrCreateUser(context, userId);

  const userReserve = await context.UserReserve.get(userReserveId);

  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.RebalanceStableBorrowRate.set({
    id,
    txHash: event.transaction.hash,
    action: 'RebalanceStableBorrowRate',
    pool_id: poolId,
    user_id: userId,
    reserve_id: reserveId,
    userReserve_id: userReserveId,
    borrowRateFrom: userReserve?.oldStableBorrowRate || 0n,
    borrowRateTo: userReserve?.stableBorrowRate || 0n,
    timestamp: Number(event.block.timestamp),
  });
});
