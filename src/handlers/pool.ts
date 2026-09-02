/**
 * Pool Event Handlers
 * Supply, Borrow, Repay, Withdraw, Liquidation, FlashLoan, etc.
 */

import { Pool } from '../../generated';
import type { handlerContext } from '../../generated';
import {
  recordProtocolTransaction,
  getOrCreateUser,
  getAssetPriceUSD,
  getReserveNormalizedIncome,
  getReserveNormalizedVariableDebt,
  addReserveToUserList,
  getOrCreateProtocolStats,
} from './shared';
import { calculateGrowth } from '../helpers/math';
import { getHistoryEntityId } from '../helpers/entityHelpers';
import { updateReserveUsdValues } from '../helpers/protocolAggregation';
import { normalizeAddress, FLASH_LOAN_PREMIUM_TO_TREASURY_BLOCK } from '../helpers/constants';

async function resolvePoolId(context: handlerContext, contractAddress: string): Promise<string> {
  const normalized = normalizeAddress(contractAddress);
  const mapping = await context.ContractToPoolMapping.get(normalized);
  return mapping?.pool_id || normalized;
}

// The Pool emits ReserveUsedAsCollateralEnabled BEFORE the aToken's
// BalanceTransfer when a user's first aTokens arrive via transfer, so the
// toggle handlers cannot assume the UserReserve row already exists — skipping
// would silently lose the collateral flag forever (the later BalanceTransfer
// creates the row with the flag defaulted to false).
async function getOrCreateUserReserveForCollateralToggle(
  context: handlerContext,
  userReserveId: string,
  poolId: string,
  userId: string,
  reserveId: string,
  timestamp: number
) {
  const existing = await context.UserReserve.get(userReserveId);
  if (existing) return existing;

  await addReserveToUserList(context, userId, reserveId, timestamp);
  return {
    id: userReserveId,
    pool_id: poolId,
    user_id: userId,
    reserve_id: reserveId,
    scaledATokenBalance: 0n,
    currentATokenBalance: 0n,
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: timestamp,
  };
}

async function updateUsageAsCollateral(
  context: handlerContext,
  params: {
    txHash: string;
    logIndex: number;
    timestamp: number;
    poolId: string;
    reserveId: string;
    userId: string;
    userReserveId: string;
    toState: boolean;
  }
) {
  const { txHash, logIndex, timestamp, poolId, reserveId, userId, userReserveId, toState } = params;

  await getOrCreateUser(context, userId);
  const userReserve = await getOrCreateUserReserveForCollateralToggle(
    context,
    userReserveId,
    poolId,
    userId,
    reserveId,
    timestamp
  );

  const historyId = getHistoryEntityId(txHash, logIndex);
  context.UsageAsCollateral.set({
    id: historyId,
    txHash,
    action: 'UsageAsCollateral',
    pool_id: poolId,
    user_id: userId,
    reserve_id: reserveId,
    userReserve_id: userReserveId,
    fromState: userReserve.usageAsCollateralEnabledOnUser,
    toState,
    timestamp,
  });

  context.UserReserve.set({
    ...userReserve,
    usageAsCollateralEnabledOnUser: toState,
    lastUpdateTimestamp: timestamp,
  });

  // Only adjust Reserve.totalLiquidityAsCollateral when the state actually flips.
  if (userReserve.usageAsCollateralEnabledOnUser !== toState) {
    const reserve = await context.Reserve.get(reserveId);
    if (reserve) {
      const userBalance = userReserve.currentATokenBalance;
      context.Reserve.set({
        ...reserve,
        totalLiquidityAsCollateral: toState
          ? reserve.totalLiquidityAsCollateral + userBalance
          : reserve.totalLiquidityAsCollateral > userBalance
            ? reserve.totalLiquidityAsCollateral - userBalance
            : 0n,
      });
    }
  }
}

async function maybeStoreEpochEndReserveSnapshot(
  context: handlerContext,
  reserve: {
    variableBorrowRate: bigint;
    variableBorrowIndex: bigint;
    utilizationRate: number;
    liquidityIndex: bigint;
    liquidityRate: bigint;
    totalLiquidity: bigint;
    totalATokenSupply: bigint;
    totalLiquidityAsCollateral: bigint;
    availableLiquidity: bigint;
    priceInUsdE8: bigint;
    priceInUsd: number;
    accruedToTreasury: bigint;
    totalScaledDebt: bigint;
    totalCurrentDebt: bigint;
    lifetimeScaledDebt: bigint;
    lifetimeCurrentDebt: bigint;
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
    totalScaledDebt: reserve.totalScaledDebt,
    totalCurrentDebt: reserve.totalCurrentDebt,
    lifetimeScaledDebt: reserve.lifetimeScaledDebt,
    lifetimeCurrentDebt: reserve.lifetimeCurrentDebt,
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
    const callerId = normalizeAddress(event.params.user);
    const userReserveId = `${userId}-${reserveId}`;

    await getOrCreateUser(context, userId);
    await getOrCreateUser(context, callerId);

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
      caller_id: callerId,
      reserve_id: reserveId,
      referrer_id: event.params.referralCode > 0 ? event.params.referralCode.toString() : undefined,
      userReserve_id: userReserveId,
      amount: event.params.amount,
      timestamp: Number(event.block.timestamp),
      assetPriceUSD,
    });

    context.ReserveTx.set({
      id,
      txHash: event.transaction.hash,
      kind: 'Supply',
      reserve: reserveId,
      user: userId,
      counterparty: callerId,
      onBehalfOf: userId,
      amount: event.params.amount,
      timestamp: Number(event.block.timestamp),
      blockNumber: Number(event.block.number),
      logIndex: Number(event.logIndex),
    });

    const vault = await context.UserVaultEntity.get(callerId);
    if (vault?.owner === userId) {
      context.AutoDeposit.set({
        id,
        vault: callerId,
        user: userId,
        poolAddressesProvider: poolId,
        asset: reserveAddress,
        reserve_id: reserveId,
        amount: event.params.amount,
        assetPriceUSD,
        timestamp: Number(event.block.timestamp),
        txHash: event.transaction.hash,
      });
    }
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
    const scaledDebt = userReserve?.scaledDebt || 0n;
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
      scaledDebt,
      assetPriceUSD,
      timestamp: Number(event.block.timestamp),
    });

    context.ReserveTx.set({
      id,
      txHash: event.transaction.hash,
      kind: 'Borrow',
      reserve: reserveId,
      user: userId,
      counterparty: normalizeAddress(event.params.user),
      onBehalfOf: userId,
      amount: event.params.amount,
      timestamp: Number(event.block.timestamp),
      blockNumber: Number(event.block.number),
      logIndex: Number(event.logIndex),
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

  context.ReserveTx.set({
    id,
    txHash: event.transaction.hash,
    kind: 'Repay',
    reserve: reserveId,
    user: userId,
    counterparty: normalizeAddress(event.params.repayer),
    onBehalfOf: undefined,
    amount: event.params.amount,
    timestamp: Number(event.block.timestamp),
    blockNumber: Number(event.block.number),
    logIndex: Number(event.logIndex),
  });

  const vaultAddress = normalizeAddress(event.params.repayer);
  const vault = await context.UserVaultEntity.get(vaultAddress);
  if (!vault || vault.owner !== userId) return;

  const timestamp = Number(event.block.timestamp);
  context.LoanSelfRepayment.set({
    id,
    vault: vaultAddress,
    user: userId,
    poolAddressesProvider: poolId,
    debtAsset: reserveAddress,
    reserve_id: reserveId,
    amount: event.params.amount,
    assetPriceUSD,
    timestamp,
    txHash: event.transaction.hash,
  });

  let vaultSummary = await context.UserVault.get(vaultAddress);
  if (!vaultSummary) {
    vaultSummary = {
      id: vaultAddress,
      user: userId,
      createdAt: timestamp,
      totalRepayVolume: 0n,
      repayCount: 0n,
      lastRepayAt: 0,
    };
  }
  context.UserVault.set({
    ...vaultSummary,
    totalRepayVolume: vaultSummary.totalRepayVolume + event.params.amount,
    repayCount: vaultSummary.repayCount + 1n,
    lastRepayAt: timestamp,
  });

  context.UserVaultEntity.set({
    ...vault,
    totalSelfRepayVolume: vault.totalSelfRepayVolume + event.params.amount,
    totalSelfRepayCount: vault.totalSelfRepayCount + 1n,
    lastUpdate: timestamp,
  });

  const user = await context.User.get(userId);
  if (user) {
    context.User.set({
      ...user,
      totalSelfRepaymentsReceived: user.totalSelfRepaymentsReceived + event.params.amount,
    });
  }

  const ps = await getOrCreateProtocolStats(context, timestamp);
  context.ProtocolStats.set({
    ...ps,
    totalSelfRepayVolume: ps.totalSelfRepayVolume + event.params.amount,
    totalSelfRepayCount: ps.totalSelfRepayCount + 1n,
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

  // The premium is split the way the deployed contract split it AT THIS BLOCK.
  // Before the upgrade the LP share was real, distributed through
  // cumulateToLiquidityIndex; from the upgrade on the whole premium accrues to
  // the treasury with no liquidityIndex bump. Applying either rule to the whole
  // history misreports one era, and a from-genesis resync replays both.
  const splitPremium = Number(event.block.number) < FLASH_LOAN_PREMIUM_TO_TREASURY_BLOCK;
  let protocolFee = premium;
  let lpFee = 0n;
  if (splitPremium) {
    protocolFee = pool?.flashloanPremiumToProtocol
      ? (premium * pool.flashloanPremiumToProtocol + 5000n) / 10000n
      : 0n;
    lpFee = premium - protocolFee;
  }

  if (reserve) {
    context.Reserve.set({
      ...reserve,
      // The underlying arrives now: the repayment transfers amount + premium to
      // the aToken and updates rates with liquidityAdded = amountPlusPremium.
      availableLiquidity: reserve.availableLiquidity + premium,
      // Only the LP share moves the supply here, and only in the era that had
      // one. That share reached holders through a liquidityIndex bump, which
      // emits no Mint, so nothing else would ever record it. The protocol share
      // is NOT added: those aTokens are minted later by mintToTreasury and
      // arrive as AToken.Mint(caller = Pool). Adding the whole premium here
      // counted the protocol share twice and inflated suppliesUsd and tvlUsd.
      totalATokenSupply: reserve.totalATokenSupply + lpFee,
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

    // Feed row attaches to the principal (debt) reserve with the repaid amount.
    context.ReserveTx.set({
      id,
      txHash: event.transaction.hash,
      kind: 'LiquidationCall',
      reserve: debtReserveId,
      user: userId,
      counterparty: normalizeAddress(event.params.liquidator),
      onBehalfOf: undefined,
      amount: event.params.debtToCover,
      timestamp: Number(event.block.timestamp),
      blockNumber: Number(event.block.number),
      logIndex: Number(event.logIndex),
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

  await updateUsageAsCollateral(context, {
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
    timestamp: Number(event.block.timestamp),
    poolId,
    reserveId,
    userId,
    userReserveId,
    toState: true,
  });
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

  await updateUsageAsCollateral(context, {
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
    timestamp: Number(event.block.timestamp),
    poolId,
    reserveId,
    userId,
    userReserveId,
    toState: false,
  });
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
      variableBorrowRate: event.params.variableBorrowRate,
      liquidityIndex: event.params.liquidityIndex,
      variableBorrowIndex: event.params.variableBorrowIndex,
      totalATokenSupply: newTotalATokenSupply,
      lifetimeSuppliersInterestEarned: newLifetimeSuppliersInterest,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });

    // Hourly rate bucket: deterministic id makes the last update in an hour win.
    const hourStart = Math.floor(Number(event.block.timestamp) / 3600) * 3600;
    context.ReserveRateSnapshot.set({
      id: `${reserveId}-${hourStart}`,
      reserve: reserveId,
      hourStart,
      liquidityAPRRay: event.params.liquidityRate,
      borrowAPRRay: event.params.variableBorrowRate,
      liquidityIndexRay: event.params.liquidityIndex,
      variableBorrowIndexRay: event.params.variableBorrowIndex,
      lastUpdateTs: Number(event.block.timestamp),
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

    // Revenue is booked through the shared aggregator rather than inline. The
    // old inline path advanced ReserveAggregate.lastProtocolAccruedToken itself
    // and wrote straight to ProtocolStats, which meant PoolStats - whose only
    // feed is this aggregator - never saw a single unit of treasury revenue.
    // updateReserveUsdValues reads that same watermark, books the delta to both
    // rows and advances it, so the amount is still counted exactly once.
    await updateReserveUsdValues(context, reserveId, reserveAddress, Number(event.block.timestamp));
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
