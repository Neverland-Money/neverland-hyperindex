/**
 * Tokenization Event Handlers
 * AToken, VariableDebtToken, StableDebtToken
 */

import type { handlerContext } from '../../generated';
import { AToken, VariableDebtToken, StableDebtToken } from '../../generated';
import { rayDiv, rayMul, toDecimal } from '../helpers/math';
import {
  isGatewayAddress,
  KNOWN_GATEWAYS,
  ZERO_ADDRESS,
  getTokenMetadata,
  normalizeAddress,
} from '../helpers/constants';
import { tryReadTokenMetadata } from '../helpers/viem';
import {
  recordProtocolTransaction,
  addReserveToUserList,
  getOrCreateUser,
  getAssetPriceUSD,
  getOrCreateUserDailyActivity,
  settlePointsForUser,
  syncUserReservePointsBaseline,
  updatePriceOracleIndex,
  awardDailySupplyPoints,
  awardDailyBorrowPoints,
  awardDailyWithdrawPoints,
  awardDailyRepayPoints,
} from './shared';
import { updateReserveUsdValues } from '../helpers/protocolAggregation';
import {
  createDefaultReserve,
  getHistoryEntityId,
  isTreasuryAddress,
} from '../helpers/entityHelpers';

async function getOrCreateUserReserveForAllowance(
  context: handlerContext,
  userAddress: string,
  reserveId: string,
  poolId: string,
  timestamp: number
): Promise<string> {
  const normalizedUser = normalizeAddress(userAddress);
  const normalizedReserveId = reserveId.toLowerCase();
  const normalizedPoolId = normalizeAddress(poolId);
  const userReserveId = `${normalizedUser}-${normalizedReserveId}`;
  let userReserve = await context.UserReserve.get(userReserveId);
  if (!userReserve) {
    userReserve = {
      id: userReserveId,
      pool_id: normalizedPoolId,
      user_id: normalizedUser,
      reserve_id: normalizedReserveId,
      scaledATokenBalance: 0n,
      currentATokenBalance: 0n,
      scaledVariableDebt: 0n,
      currentVariableDebt: 0n,
      principalStableDebt: 0n,
      currentStableDebt: 0n,
      currentTotalDebt: 0n,
      stableBorrowRate: 0n,
      oldStableBorrowRate: 0n,
      liquidityRate: 0n,
      variableBorrowIndex: 0n,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: timestamp,
      stableBorrowLastUpdateTimestamp: 0,
    };
    await addReserveToUserList(context, normalizedUser, normalizedReserveId, timestamp);
    context.UserReserve.set(userReserve);
  }
  return userReserveId;
}

// ============================================
// AToken Handlers
// ============================================

AToken.Mint.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenAddress = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(tokenAddress);
  if (!subToken) return;

  const underlyingAsset = subToken.underlyingAssetAddress;
  const poolId = subToken.pool_id;
  const reserveId = `${underlyingAsset}-${poolId}`;
  const userAddress = normalizeAddress(event.params.onBehalfOf);
  const userReserveId = `${userAddress}-${reserveId}`;

  const isTreasury = isTreasuryAddress(userAddress);
  if (!isTreasury) {
    await getOrCreateUser(context, userAddress);
  }

  const reserve = await context.Reserve.get(reserveId);

  // Subgraph logic: userBalanceChange = value - balanceIncrease (actual new deposit)
  // Then scale down by index to get scaled balance
  const userBalanceChange = event.params.value - event.params.balanceIncrease;
  let userReserve = await context.UserReserve.get(userReserveId);
  if (!isTreasury) {
    if (!userReserve) {
      userReserve = {
        id: userReserveId,
        pool_id: poolId,
        user_id: userAddress,
        reserve_id: reserveId,
        scaledATokenBalance: 0n,
        currentATokenBalance: 0n,
        scaledVariableDebt: 0n,
        currentVariableDebt: 0n,
        principalStableDebt: 0n,
        currentStableDebt: 0n,
        currentTotalDebt: 0n,
        stableBorrowRate: 0n,
        oldStableBorrowRate: 0n,
        liquidityRate: 0n,
        variableBorrowIndex: 0n,
        usageAsCollateralEnabledOnUser: false,
        lastUpdateTimestamp: Number(event.block.timestamp),
        stableBorrowLastUpdateTimestamp: 0,
      };
      await addReserveToUserList(context, userAddress, reserveId, Number(event.block.timestamp));
    }

    await settlePointsForUser(
      context,
      userAddress,
      reserveId,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );

    const calculatedAmount = rayDiv(userBalanceChange, event.params.index);

    const newScaledBalance = userReserve.scaledATokenBalance + calculatedAmount;
    const newCurrentBalance = rayMul(newScaledBalance, event.params.index);

    context.UserReserve.set({
      ...userReserve,
      scaledATokenBalance: newScaledBalance,
      currentATokenBalance: newCurrentBalance,
      liquidityRate: reserve?.liquidityRate || 0n,
      variableBorrowIndex: reserve?.variableBorrowIndex || 0n,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });

    const historyId = `${userReserveId}:${event.transaction.hash}:${event.logIndex}`;
    context.ATokenBalanceHistoryItem.set({
      id: historyId,
      userReserve_id: userReserveId,
      timestamp: Number(event.block.timestamp),
      scaledATokenBalance: newScaledBalance,
      currentATokenBalance: newCurrentBalance,
      index: event.params.index,
    });
  }
  if (reserve) {
    const newTotalATokenSupply = reserve.totalATokenSupply + userBalanceChange;

    if (!isTreasury) {
      const liquidityAsCollateral = userReserve?.usageAsCollateralEnabledOnUser
        ? reserve.totalLiquidityAsCollateral + userBalanceChange
        : reserve.totalLiquidityAsCollateral;

      context.Reserve.set({
        ...reserve,
        totalATokenSupply: newTotalATokenSupply,
        totalLiquidity: reserve.totalLiquidity + userBalanceChange,
        availableLiquidity: reserve.availableLiquidity + userBalanceChange,
        lifetimeLiquidity: reserve.lifetimeLiquidity + userBalanceChange,
        totalSupplies: reserve.totalSupplies + userBalanceChange,
        totalLiquidityAsCollateral: liquidityAsCollateral,
      });

      // Update USD aggregates
      await updateReserveUsdValues(
        context,
        reserveId,
        underlyingAsset,
        Number(event.block.timestamp)
      );

      await recordReserveParamsHistory(
        context,
        reserveId,
        Number(event.block.timestamp),
        event.transaction.hash,
        Number(event.logIndex)
      );
    } else {
      // Treasury mint - this is protocol revenue
      context.Reserve.set({
        ...reserve,
        totalATokenSupply: newTotalATokenSupply,
        lifetimeReserveFactorAccrued: reserve.lifetimeReserveFactorAccrued + userBalanceChange,
      });

      await updateReserveUsdValues(
        context,
        reserveId,
        underlyingAsset,
        Number(event.block.timestamp)
      );

      await recordReserveParamsHistory(
        context,
        reserveId,
        Number(event.block.timestamp),
        event.transaction.hash,
        Number(event.logIndex)
      );
    }
  }

  if (!isTreasury) {
    await syncUserReservePointsBaseline(
      context,
      userAddress,
      reserveId,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );
    await updateDailySupplyHighwater(
      context,
      userAddress,
      reserveId,
      userBalanceChange,
      Number(event.block.timestamp)
    );
    await awardDailySupplyPoints(
      context,
      userAddress,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );
  }
});

AToken.Burn.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const aTokenAddress = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(aTokenAddress);
  if (!subToken) return;

  const underlyingAsset = subToken.underlyingAssetAddress;
  const pool = await context.Pool.get(subToken.pool_id);
  if (!pool) return;

  const poolId = pool.id;
  const reserveId = `${underlyingAsset}-${poolId}`;

  // Check if this is a gateway withdrawal (use lowercase for consistent ID matching)
  const burnFrom = normalizeAddress(event.params.from);
  const pendingId = `${event.transaction.hash}:${underlyingAsset}:${burnFrom}`;
  const pendingWithdrawal = await context.PendingGatewayWithdrawal.get(pendingId);

  // Use actual user from pending withdrawal, or fall back to event params
  const userAddress = normalizeAddress(pendingWithdrawal?.actualUser ?? burnFrom);

  await getOrCreateUser(context, userAddress);

  const userReserveId = `${userAddress}-${reserveId}`;

  const reserve = await context.Reserve.get(reserveId);
  let userReserve = await context.UserReserve.get(userReserveId);
  let newScaledBalance = userReserve?.scaledATokenBalance || 0n;
  let newCurrentBalance = userReserve?.currentATokenBalance || 0n;

  await settlePointsForUser(
    context,
    userAddress,
    reserveId,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  if (userReserve) {
    // Subgraph: userBalanceChange = value + balanceIncrease (total withdrawn)
    const userBalanceChange = event.params.value + event.params.balanceIncrease;
    const calculatedAmount = rayDiv(userBalanceChange, event.params.index);

    newScaledBalance = userReserve.scaledATokenBalance - calculatedAmount;
    newCurrentBalance = rayMul(newScaledBalance, event.params.index);

    context.UserReserve.set({
      ...userReserve,
      scaledATokenBalance: newScaledBalance,
      currentATokenBalance: newCurrentBalance,
      liquidityRate: reserve?.liquidityRate || userReserve.liquidityRate,
      variableBorrowIndex: reserve?.variableBorrowIndex || userReserve.variableBorrowIndex,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });
  }

  await syncUserReservePointsBaseline(
    context,
    userAddress,
    reserveId,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const historyId = `${userReserveId}:${event.transaction.hash}:${event.logIndex}`;
  context.ATokenBalanceHistoryItem.set({
    id: historyId,
    userReserve_id: userReserveId,
    timestamp: Number(event.block.timestamp),
    scaledATokenBalance: newScaledBalance,
    currentATokenBalance: newCurrentBalance,
    index: event.params.index,
  });

  if (reserve) {
    // Subgraph: userBalanceChange = value + balanceIncrease
    const userBalanceChange = event.params.value + event.params.balanceIncrease;
    const liquidityAsCollateral = userReserve?.usageAsCollateralEnabledOnUser
      ? reserve.totalLiquidityAsCollateral - userBalanceChange
      : reserve.totalLiquidityAsCollateral;

    context.Reserve.set({
      ...reserve,
      totalATokenSupply: reserve.totalATokenSupply - userBalanceChange,
      availableLiquidity: reserve.availableLiquidity - userBalanceChange,
      totalLiquidity: reserve.totalLiquidity - userBalanceChange,
      lifetimeWithdrawals: reserve.lifetimeWithdrawals + userBalanceChange,
      totalSupplies: reserve.totalSupplies - userBalanceChange,
      totalLiquidityAsCollateral: liquidityAsCollateral,
    });

    // Update USD aggregates
    await updateReserveUsdValues(
      context,
      reserveId,
      underlyingAsset,
      Number(event.block.timestamp)
    );

    await recordReserveParamsHistory(
      context,
      reserveId,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }

  // Create RedeemUnderlying here for accurate user attribution
  // This ensures gateway withdrawals are correctly attributed to the actual user whose aTokens were burned
  const redeemId = `${event.transaction.hash}:${event.logIndex}:${userReserveId}`;
  const existingRedeem = await context.RedeemUnderlying.get(redeemId);
  if (!existingRedeem) {
    const assetPriceUSD = await getAssetPriceUSD(
      context,
      underlyingAsset,
      Number(event.block.timestamp)
    );
    context.RedeemUnderlying.set({
      id: redeemId,
      txHash: event.transaction.hash,
      action: 'RedeemUnderlying',
      pool_id: poolId,
      user_id: userAddress,
      to_id: userAddress,
      reserve_id: reserveId,
      userReserve_id: userReserveId,
      amount: event.params.value + event.params.balanceIncrease,
      timestamp: Number(event.block.timestamp),
      assetPriceUSD,
    });

    await updateDailyWithdrawHighwater(
      context,
      userAddress,
      reserveId,
      event.params.value + event.params.balanceIncrease,
      Number(event.block.timestamp)
    );
    await awardDailyWithdrawPoints(
      context,
      userAddress,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );

    // Clean up pending gateway withdrawal
    if (pendingWithdrawal) {
      context.PendingGatewayWithdrawal.deleteUnsafe(pendingId);
    }
  }
});

AToken.BalanceTransfer.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenAddress = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(tokenAddress);
  if (!subToken) return;

  const reserveId = `${subToken.underlyingAssetAddress}-${subToken.pool_id}`;
  const fromAddress = normalizeAddress(event.params.from);
  const toAddress = normalizeAddress(event.params.to);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);

  // Check if this is a gateway transfer
  const isGatewayTransfer = KNOWN_GATEWAYS.includes(toAddress);
  if (isGatewayTransfer) {
    context.PendingGatewayWithdrawal.set({
      id: `${event.transaction.hash}:${subToken.underlyingAssetAddress}:${toAddress}`,
      txHash: event.transaction.hash,
      reserve: subToken.underlyingAssetAddress,
      gateway: toAddress,
      actualUser: fromAddress,
    });
    return;
  }

  const fromUserReserveId = `${fromAddress}-${reserveId}`;
  const toUserReserveId = `${toAddress}-${reserveId}`;

  const isFromGateway = isGatewayAddress(fromAddress);
  const isToGateway = isGatewayAddress(toAddress);

  let fromUserReserve = await context.UserReserve.get(fromUserReserveId);
  if (!fromUserReserve && fromAddress !== ZERO_ADDRESS && !isFromGateway) {
    await getOrCreateUser(context, fromAddress);
    fromUserReserve = {
      id: fromUserReserveId,
      pool_id: subToken.pool_id,
      user_id: fromAddress,
      reserve_id: reserveId,
      scaledATokenBalance: 0n,
      currentATokenBalance: 0n,
      scaledVariableDebt: 0n,
      currentVariableDebt: 0n,
      principalStableDebt: 0n,
      currentStableDebt: 0n,
      currentTotalDebt: 0n,
      stableBorrowRate: 0n,
      oldStableBorrowRate: 0n,
      liquidityRate: 0n,
      variableBorrowIndex: 0n,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: timestamp,
      stableBorrowLastUpdateTimestamp: 0,
    };
    await addReserveToUserList(context, fromAddress, reserveId, timestamp);
  }

  let toUserReserve = await context.UserReserve.get(toUserReserveId);
  // Skip creating user/reserve for gateway addresses - they are intermediaries
  if (!toUserReserve && toAddress !== ZERO_ADDRESS && !isToGateway) {
    await getOrCreateUser(context, toAddress);
    toUserReserve = {
      id: toUserReserveId,
      pool_id: subToken.pool_id,
      user_id: toAddress,
      reserve_id: reserveId,
      scaledATokenBalance: 0n,
      currentATokenBalance: 0n,
      scaledVariableDebt: 0n,
      currentVariableDebt: 0n,
      principalStableDebt: 0n,
      currentStableDebt: 0n,
      currentTotalDebt: 0n,
      stableBorrowRate: 0n,
      oldStableBorrowRate: 0n,
      liquidityRate: 0n,
      variableBorrowIndex: 0n,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: timestamp,
      stableBorrowLastUpdateTimestamp: 0,
    };
    await addReserveToUserList(context, toAddress, reserveId, timestamp);
  }

  // Settle points for real users, not gateway contracts
  if (fromAddress !== ZERO_ADDRESS && !isFromGateway) {
    await settlePointsForUser(context, fromAddress, reserveId, timestamp, blockNumber);
  }
  if (toAddress !== ZERO_ADDRESS && !isToGateway) {
    await settlePointsForUser(context, toAddress, reserveId, timestamp, blockNumber);
  }

  const scaledAmount = event.params.value;
  const currentAmount = rayMul(event.params.value, event.params.index);

  if (toUserReserve) {
    context.UserReserve.set({
      ...toUserReserve,
      scaledATokenBalance: toUserReserve.scaledATokenBalance + scaledAmount,
      currentATokenBalance: toUserReserve.currentATokenBalance + currentAmount,
      lastUpdateTimestamp: timestamp,
    });
  }

  if (fromUserReserve) {
    const newScaledBalance =
      fromUserReserve.scaledATokenBalance > scaledAmount
        ? fromUserReserve.scaledATokenBalance - scaledAmount
        : 0n;
    const newCurrentBalance =
      fromUserReserve.currentATokenBalance > currentAmount
        ? fromUserReserve.currentATokenBalance - currentAmount
        : 0n;

    context.UserReserve.set({
      ...fromUserReserve,
      scaledATokenBalance: newScaledBalance,
      currentATokenBalance: newCurrentBalance,
      lastUpdateTimestamp: timestamp,
    });
  }

  if (toUserReserve) {
    const toHistoryId = `${toUserReserveId}:${event.transaction.hash}:${event.logIndex}`;
    context.ATokenBalanceHistoryItem.set({
      id: toHistoryId,
      userReserve_id: toUserReserveId,
      timestamp,
      scaledATokenBalance: toUserReserve.scaledATokenBalance + scaledAmount,
      currentATokenBalance: toUserReserve.currentATokenBalance + currentAmount,
      index: event.params.index,
    });
  }

  if (fromUserReserve) {
    const fromHistoryId = `${fromUserReserveId}:${event.transaction.hash}:${event.logIndex}`;
    context.ATokenBalanceHistoryItem.set({
      id: fromHistoryId,
      userReserve_id: fromUserReserveId,
      timestamp,
      scaledATokenBalance:
        fromUserReserve.scaledATokenBalance > scaledAmount
          ? fromUserReserve.scaledATokenBalance - scaledAmount
          : 0n,
      currentATokenBalance:
        fromUserReserve.currentATokenBalance > currentAmount
          ? fromUserReserve.currentATokenBalance - currentAmount
          : 0n,
      index: event.params.index,
    });
  }

  if (fromAddress !== ZERO_ADDRESS) {
    await syncUserReservePointsBaseline(context, fromAddress, reserveId, timestamp, blockNumber);
  }

  if (toAddress !== ZERO_ADDRESS) {
    await syncUserReservePointsBaseline(context, toAddress, reserveId, timestamp, blockNumber);
    await updateDailySupplyHighwater(context, toAddress, reserveId, currentAmount, timestamp);
    await awardDailySupplyPoints(context, toAddress, timestamp, blockNumber);
  }

  const reserve = await context.Reserve.get(reserveId);
  if (reserve && fromUserReserve && toUserReserve) {
    // Use currentAmount for collateral totals to match actual liquidity
    if (
      fromUserReserve.usageAsCollateralEnabledOnUser &&
      !toUserReserve.usageAsCollateralEnabledOnUser
    ) {
      context.Reserve.set({
        ...reserve,
        totalLiquidityAsCollateral:
          reserve.totalLiquidityAsCollateral > currentAmount
            ? reserve.totalLiquidityAsCollateral - currentAmount
            : 0n,
      });
    } else if (
      !fromUserReserve.usageAsCollateralEnabledOnUser &&
      toUserReserve.usageAsCollateralEnabledOnUser
    ) {
      context.Reserve.set({
        ...reserve,
        totalLiquidityAsCollateral: reserve.totalLiquidityAsCollateral + currentAmount,
      });
    } else {
      return;
    }

    await updateReserveUsdValues(context, reserveId, subToken.underlyingAssetAddress, timestamp);

    await recordReserveParamsHistory(
      context,
      reserveId,
      timestamp,
      event.transaction.hash,
      Number(event.logIndex)
    );
  }
});

AToken.Initialized.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const tokenId = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(tokenId);

  if (subToken) {
    context.SubToken.set({
      ...subToken,
      underlyingAssetAddress: normalizeAddress(event.params.underlyingAsset),
      underlyingAssetDecimals: Number(event.params.aTokenDecimals),
    });
  }

  // Update Reserve with symbol, name, and decimals
  // Use mapped pool ID to match how Reserve was created in ReserveInitialized
  const poolId = normalizeAddress(event.params.pool);
  const mapping = await context.ContractToPoolMapping.get(poolId);
  const actualPoolId = mapping?.pool_id || poolId;
  const underlyingAsset = normalizeAddress(event.params.underlyingAsset);
  const reserveId = `${underlyingAsset}-${actualPoolId}`;
  let reserve = await context.Reserve.get(reserveId);
  if (!reserve) {
    reserve = createDefaultReserve(reserveId, actualPoolId, underlyingAsset);
    context.Reserve.set(reserve);
  }

  if (mapping) {
    context.MapAssetPool.set({
      id: tokenId,
      pool: mapping.pool_id,
      underlyingAsset,
    });
  }

  if (reserve) {
    const tokenInfo = getTokenMetadata(underlyingAsset);
    const isKnownToken = tokenInfo !== null;

    let symbol = '';
    let name = '';
    let decimals = Number(event.params.aTokenDecimals);

    /* c8 ignore start */
    if (isKnownToken) {
      symbol = tokenInfo.symbol;
      name = tokenInfo.name;
      decimals = tokenInfo.decimals;
    } else {
      /* c8 ignore end */
      const chainMetadata = await tryReadTokenMetadata(underlyingAsset, BigInt(event.block.number));
      if (chainMetadata) {
        if (chainMetadata.symbol) symbol = chainMetadata.symbol;
        if (chainMetadata.name) name = chainMetadata.name;
        if (chainMetadata.decimals !== undefined) decimals = chainMetadata.decimals;
      }

      if (!symbol && !name) {
        // Extract symbol from aToken symbol by stripping "n" prefix
        const aTokenSymbol = event.params.aTokenSymbol;
        if (aTokenSymbol.length > 1 && aTokenSymbol.charAt(0) === 'n') {
          symbol = aTokenSymbol.substring(1);
        } else {
          symbol = aTokenSymbol;
        }

        // Extract name from aToken name by stripping prefix
        const aTokenName = event.params.aTokenName;
        const prefix = 'Neverland Interest Bearing ';
        if (aTokenName.startsWith(prefix)) {
          name = aTokenName.substring(prefix.length);
        } else {
          name = aTokenName;
        }
      }
    }

    if (!name && symbol) {
      name = symbol;
    }

    if (!symbol && name) {
      symbol = name;
    }

    context.Reserve.set({
      ...reserve,
      symbol,
      name,
      decimals,
    });
  }
});

AToken.PriceObserved.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  await recordPriceObserved(
    context,
    event.params.asset,
    event.params.price,
    event.params.baseUnit,
    event.params.oracle,
    event.params.ok,
    Number(event.block.timestamp),
    Number(event.block.number),
    Number(event.logIndex)
  );
});

VariableDebtToken.PriceObserved.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  await recordPriceObserved(
    context,
    event.params.asset,
    event.params.price,
    event.params.baseUnit,
    event.params.oracle,
    event.params.ok,
    Number(event.block.timestamp),
    Number(event.block.number),
    Number(event.logIndex)
  );
});

StableDebtToken.PriceObserved.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const params = event.params as typeof event.params & {
    baseUnit: bigint;
    ok: boolean;
  };
  await recordPriceObserved(
    context,
    params.asset,
    params.price,
    params.baseUnit,
    params.oracle,
    params.ok,
    Number(event.block.timestamp),
    Number(event.block.number),
    Number(event.logIndex)
  );
});

// ============================================
// VariableDebtToken Handlers
// ============================================

VariableDebtToken.Mint.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenAddress = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(tokenAddress);
  if (!subToken) return;

  const underlyingAsset = subToken.underlyingAssetAddress;
  const poolId = subToken.pool_id;
  const reserveId = `${underlyingAsset}-${poolId}`;
  const userAddress = normalizeAddress(event.params.onBehalfOf);
  const userReserveId = `${userAddress}-${reserveId}`;

  await getOrCreateUser(context, userAddress);

  const reserve = await context.Reserve.get(reserveId);
  let userReserve = await context.UserReserve.get(userReserveId);
  if (!userReserve) {
    userReserve = {
      id: userReserveId,
      pool_id: poolId,
      user_id: userAddress,
      reserve_id: reserveId,
      scaledATokenBalance: 0n,
      currentATokenBalance: 0n,
      scaledVariableDebt: 0n,
      currentVariableDebt: 0n,
      principalStableDebt: 0n,
      currentStableDebt: 0n,
      currentTotalDebt: 0n,
      stableBorrowRate: 0n,
      oldStableBorrowRate: 0n,
      liquidityRate: 0n,
      variableBorrowIndex: 0n,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: Number(event.block.timestamp),
      stableBorrowLastUpdateTimestamp: 0,
    };
    await addReserveToUserList(context, userAddress, reserveId, Number(event.block.timestamp));
  }

  await settlePointsForUser(
    context,
    userAddress,
    reserveId,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  // Subgraph: userBalanceChange = value - balanceIncrease (actual borrow)
  const userBalanceChange = event.params.value - event.params.balanceIncrease;
  const calculatedAmount = rayDiv(userBalanceChange, event.params.index);

  const newScaledDebt = userReserve.scaledVariableDebt + calculatedAmount;
  const newCurrentDebt = rayMul(newScaledDebt, event.params.index);

  context.UserReserve.set({
    ...userReserve,
    scaledVariableDebt: newScaledDebt,
    currentVariableDebt: newCurrentDebt,
    currentTotalDebt: userReserve.currentStableDebt + newCurrentDebt,
    liquidityRate: reserve?.liquidityRate || 0n,
    variableBorrowIndex: reserve?.variableBorrowIndex || event.params.index,
    lastUpdateTimestamp: Number(event.block.timestamp),
  });

  if (reserve) {
    const newReserveScaledDebt = reserve.totalScaledVariableDebt + calculatedAmount;
    const newReserveCurrentDebt = rayMul(newReserveScaledDebt, event.params.index);
    const newLifetimeScaledDebt = reserve.lifetimeScaledVariableDebt + calculatedAmount;
    const newLifetimeCurrentDebt = rayMul(newLifetimeScaledDebt, event.params.index);

    context.Reserve.set({
      ...reserve,
      totalScaledVariableDebt: newReserveScaledDebt,
      totalCurrentVariableDebt: newReserveCurrentDebt,
      lifetimeScaledVariableDebt: newLifetimeScaledDebt,
      lifetimeCurrentVariableDebt: newLifetimeCurrentDebt,
      lifetimeBorrows: reserve.lifetimeBorrows + userBalanceChange,
      availableLiquidity: reserve.availableLiquidity - userBalanceChange,
    });

    // Update USD aggregates
    await updateReserveUsdValues(
      context,
      reserveId,
      underlyingAsset,
      Number(event.block.timestamp)
    );

    await recordReserveParamsHistory(
      context,
      reserveId,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }

  const user = await context.User.get(userAddress);
  if (user && userReserve.scaledVariableDebt === 0n && userReserve.principalStableDebt === 0n) {
    context.User.set({
      ...user,
      borrowedReservesCount: user.borrowedReservesCount + 1,
    });
  }

  const historyId = `${userReserveId}:${event.transaction.hash}:${event.logIndex}`;
  context.VTokenBalanceHistoryItem.set({
    id: historyId,
    userReserve_id: userReserveId,
    scaledVariableDebt: newScaledDebt,
    currentVariableDebt: newCurrentDebt,
    timestamp: Number(event.block.timestamp),
    index: event.params.index,
  });

  await syncUserReservePointsBaseline(
    context,
    userAddress,
    reserveId,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  await updateDailyBorrowHighwater(
    context,
    userAddress,
    reserveId,
    userBalanceChange,
    Number(event.block.timestamp)
  );
  await awardDailyBorrowPoints(
    context,
    userAddress,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
});

VariableDebtToken.Burn.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenAddress = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(tokenAddress);
  if (!subToken) return;

  const underlyingAsset = subToken.underlyingAssetAddress;
  const poolId = subToken.pool_id;
  const reserveId = `${underlyingAsset}-${poolId}`;
  const userAddress = normalizeAddress(event.params.from);
  const userReserveId = `${userAddress}-${reserveId}`;

  const reserve = await context.Reserve.get(reserveId);
  let userReserve = await context.UserReserve.get(userReserveId);
  if (!userReserve) return;

  await settlePointsForUser(
    context,
    userAddress,
    reserveId,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  // Subgraph: userBalanceChange = value + balanceIncrease (total repayment)
  const userBalanceChange = event.params.value + event.params.balanceIncrease;
  const calculatedAmount = rayDiv(userBalanceChange, event.params.index);

  const newScaledDebt = userReserve.scaledVariableDebt - calculatedAmount;
  const newCurrentDebt = rayMul(newScaledDebt, event.params.index);

  context.UserReserve.set({
    ...userReserve,
    scaledVariableDebt: newScaledDebt,
    currentVariableDebt: newCurrentDebt,
    currentTotalDebt: userReserve.currentStableDebt + newCurrentDebt,
    liquidityRate: reserve?.liquidityRate || userReserve.liquidityRate,
    variableBorrowIndex: reserve?.variableBorrowIndex || event.params.index,
    lastUpdateTimestamp: Number(event.block.timestamp),
  });

  if (reserve) {
    const newReserveScaledDebt = reserve.totalScaledVariableDebt - calculatedAmount;
    const newReserveCurrentDebt = rayMul(newReserveScaledDebt, event.params.index);

    context.Reserve.set({
      ...reserve,
      totalScaledVariableDebt: newReserveScaledDebt,
      totalCurrentVariableDebt: newReserveCurrentDebt,
      lifetimeRepayments: reserve.lifetimeRepayments + userBalanceChange,
      availableLiquidity: reserve.availableLiquidity + userBalanceChange,
    });

    // Update USD aggregates
    await updateReserveUsdValues(
      context,
      reserveId,
      underlyingAsset,
      Number(event.block.timestamp)
    );

    await recordReserveParamsHistory(
      context,
      reserveId,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }

  const historyId = `${userReserveId}:${event.transaction.hash}:${event.logIndex}`;
  context.VTokenBalanceHistoryItem.set({
    id: historyId,
    userReserve_id: userReserveId,
    scaledVariableDebt: newScaledDebt,
    currentVariableDebt: newCurrentDebt,
    timestamp: Number(event.block.timestamp),
    index: event.params.index,
  });

  await syncUserReservePointsBaseline(
    context,
    userAddress,
    reserveId,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  if (newScaledDebt === 0n && userReserve.principalStableDebt === 0n) {
    const user = await context.User.get(userAddress);
    if (user && user.borrowedReservesCount > 0) {
      context.User.set({
        ...user,
        borrowedReservesCount: user.borrowedReservesCount - 1,
      });
    }
  }

  await updateDailyRepayHighwater(
    context,
    userAddress,
    reserveId,
    userBalanceChange,
    Number(event.block.timestamp)
  );
  await awardDailyRepayPoints(
    context,
    userAddress,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
});
// ============================================
// StableDebtToken Handlers
// ============================================

StableDebtToken.Mint.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenAddress = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(tokenAddress);
  if (!subToken) return;

  const underlyingAsset = subToken.underlyingAssetAddress;
  const poolId = subToken.pool_id;
  const reserveId = `${underlyingAsset}-${poolId}`;
  const userAddress = normalizeAddress(event.params.onBehalfOf);
  const userReserveId = `${userAddress}-${reserveId}`;

  await getOrCreateUser(context, userAddress);

  let userReserve = await context.UserReserve.get(userReserveId);
  if (!userReserve) {
    userReserve = {
      id: userReserveId,
      pool_id: poolId,
      user_id: userAddress,
      reserve_id: reserveId,
      scaledATokenBalance: 0n,
      currentATokenBalance: 0n,
      scaledVariableDebt: 0n,
      currentVariableDebt: 0n,
      principalStableDebt: 0n,
      currentStableDebt: 0n,
      currentTotalDebt: 0n,
      stableBorrowRate: 0n,
      oldStableBorrowRate: 0n,
      liquidityRate: 0n,
      variableBorrowIndex: 0n,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: Number(event.block.timestamp),
      stableBorrowLastUpdateTimestamp: 0,
    };
    await addReserveToUserList(context, userAddress, reserveId, Number(event.block.timestamp));
  }

  await settlePointsForUser(
    context,
    userAddress,
    reserveId,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const reserve = await context.Reserve.get(reserveId);

  // Subgraph: borrowedAmount = amount - balanceIncrease (actual new principal)
  const borrowedAmount = event.params.amount - event.params.balanceIncrease;
  const balanceChangeIncludingInterest = event.params.amount;
  const newPrincipalDebt = userReserve.principalStableDebt + balanceChangeIncludingInterest;

  context.UserReserve.set({
    ...userReserve,
    principalStableDebt: newPrincipalDebt,
    currentStableDebt: newPrincipalDebt,
    currentTotalDebt: userReserve.currentVariableDebt + newPrincipalDebt,
    oldStableBorrowRate: userReserve.stableBorrowRate,
    stableBorrowRate: event.params.newRate,
    liquidityRate: reserve?.liquidityRate || 0n,
    variableBorrowIndex: reserve?.variableBorrowIndex || 0n,
    stableBorrowLastUpdateTimestamp: Number(event.block.timestamp),
    lastUpdateTimestamp: Number(event.block.timestamp),
  });

  if (reserve) {
    context.Reserve.set({
      ...reserve,
      totalPrincipalStableDebt: event.params.newTotalSupply,
      averageStableRate: event.params.avgStableRate,
      stableDebtLastUpdateTimestamp: Number(event.block.timestamp),
      lifetimePrincipalStableDebt:
        reserve.lifetimePrincipalStableDebt + balanceChangeIncludingInterest,
      lifetimeBorrows: reserve.lifetimeBorrows + borrowedAmount,
      availableLiquidity: reserve.availableLiquidity - borrowedAmount,
      totalLiquidity: reserve.totalLiquidity + event.params.balanceIncrease,
    });

    // Update USD aggregates
    await updateReserveUsdValues(
      context,
      reserveId,
      underlyingAsset,
      Number(event.block.timestamp)
    );

    await recordReserveParamsHistory(
      context,
      reserveId,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }

  const user = await context.User.get(userAddress);
  if (user && userReserve.scaledVariableDebt === 0n && userReserve.principalStableDebt === 0n) {
    context.User.set({
      ...user,
      borrowedReservesCount: user.borrowedReservesCount + 1,
    });
  }

  const historyId = `${event.transaction.hash}-${event.logIndex}`;
  context.STokenBalanceHistoryItem.set({
    id: historyId,
    userReserve_id: userReserveId,
    principalStableDebt: newPrincipalDebt,
    currentStableDebt: newPrincipalDebt,
    timestamp: Number(event.block.timestamp),
    avgStableBorrowRate: event.params.avgStableRate,
  });
});

StableDebtToken.Burn.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenAddress = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(tokenAddress);
  if (!subToken) return;

  const underlyingAsset = subToken.underlyingAssetAddress;
  const poolId = subToken.pool_id;
  const reserveId = `${underlyingAsset}-${poolId}`;
  const userAddress = normalizeAddress(event.params.from);
  const userReserveId = `${userAddress}-${reserveId}`;

  let userReserve = await context.UserReserve.get(userReserveId);
  if (!userReserve) return;

  const reserve = await context.Reserve.get(reserveId);

  // Subgraph uses amount directly for stable debt
  const amount = event.params.amount;
  const balanceIncrease = event.params.balanceIncrease;
  const newPrincipalDebt = userReserve.principalStableDebt - amount;

  context.UserReserve.set({
    ...userReserve,
    principalStableDebt: newPrincipalDebt,
    currentStableDebt: newPrincipalDebt,
    currentTotalDebt: userReserve.currentVariableDebt + newPrincipalDebt,
    liquidityRate: reserve?.liquidityRate || userReserve.liquidityRate,
    variableBorrowIndex: reserve?.variableBorrowIndex || userReserve.variableBorrowIndex,
    stableBorrowLastUpdateTimestamp: Number(event.block.timestamp),
    lastUpdateTimestamp: Number(event.block.timestamp),
  });

  if (userReserve.scaledVariableDebt === 0n && newPrincipalDebt === 0n) {
    const user = await context.User.get(userAddress);
    if (user && user.borrowedReservesCount > 0) {
      context.User.set({
        ...user,
        borrowedReservesCount: user.borrowedReservesCount - 1,
      });
    }
  }

  if (reserve) {
    // Subgraph: availableLiquidity increases by amount + balanceIncrease
    const totalRepaid = amount + balanceIncrease;

    context.Reserve.set({
      ...reserve,
      totalPrincipalStableDebt: event.params.newTotalSupply,
      availableLiquidity: reserve.availableLiquidity + totalRepaid,
      totalLiquidity: reserve.totalLiquidity + balanceIncrease,
      totalATokenSupply: reserve.totalATokenSupply + balanceIncrease,
      lifetimeRepayments: reserve.lifetimeRepayments + amount,
      averageStableRate: event.params.avgStableRate,
      stableDebtLastUpdateTimestamp: Number(event.block.timestamp),
    });

    // Update USD aggregates
    await updateReserveUsdValues(
      context,
      reserveId,
      underlyingAsset,
      Number(event.block.timestamp)
    );

    await recordReserveParamsHistory(
      context,
      reserveId,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }

  await settlePointsForUser(
    context,
    userAddress,
    reserveId,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const historyId = `${event.transaction.hash}-${event.logIndex}`;
  context.STokenBalanceHistoryItem.set({
    id: historyId,
    userReserve_id: userReserveId,
    principalStableDebt: newPrincipalDebt,
    currentStableDebt: newPrincipalDebt,
    timestamp: Number(event.block.timestamp),
    avgStableBorrowRate: event.params.avgStableRate,
  });

  await awardDailyRepayPoints(
    context,
    userAddress,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
});

StableDebtToken.BorrowAllowanceDelegated.handler(async ({ event, context }) => {
  const fromUser = normalizeAddress(event.params.fromUser);
  const toUser = normalizeAddress(event.params.toUser);
  const asset = normalizeAddress(event.params.asset);
  const id = `${fromUser}-${toUser}-${asset}-stable`;

  await getOrCreateUser(context, fromUser);

  context.BorrowAllowance.set({
    id,
    fromUser,
    toUser,
    asset,
    amount: event.params.amount,
    lastUpdate: Number(event.block.timestamp),
  });

  const subToken = await context.SubToken.get(normalizeAddress(event.srcAddress));
  if (!subToken) return;
  const reserveId = `${subToken.underlyingAssetAddress}-${subToken.pool_id}`;
  const userReserveId = await getOrCreateUserReserveForAllowance(
    context,
    fromUser,
    reserveId,
    subToken.pool_id,
    Number(event.block.timestamp)
  );
  const delegatedId = `stable${fromUser}${toUser}${asset}`;
  context.StableTokenDelegatedAllowance.set({
    id: delegatedId,
    fromUser_id: fromUser,
    toUser_id: toUser,
    amountAllowed: event.params.amount,
    userReserve_id: userReserveId,
  });
});

VariableDebtToken.BorrowAllowanceDelegated.handler(async ({ event, context }) => {
  const fromUser = normalizeAddress(event.params.fromUser);
  const toUser = normalizeAddress(event.params.toUser);
  const asset = normalizeAddress(event.params.asset);
  const id = `${fromUser}-${toUser}-${asset}-variable`;

  await getOrCreateUser(context, fromUser);

  context.BorrowAllowance.set({
    id,
    fromUser,
    toUser,
    asset,
    amount: event.params.amount,
    lastUpdate: Number(event.block.timestamp),
  });

  const subToken = await context.SubToken.get(normalizeAddress(event.srcAddress));
  if (!subToken) return;
  const reserveId = `${subToken.underlyingAssetAddress}-${subToken.pool_id}`;
  const userReserveId = await getOrCreateUserReserveForAllowance(
    context,
    fromUser,
    reserveId,
    subToken.pool_id,
    Number(event.block.timestamp)
  );
  const delegatedId = `variable${fromUser}${toUser}${asset}`;
  context.VariableTokenDelegatedAllowance.set({
    id: delegatedId,
    fromUser_id: fromUser,
    toUser_id: toUser,
    amountAllowed: event.params.amount,
    userReserve_id: userReserveId,
  });
});

StableDebtToken.Initialized.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const tokenId = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(tokenId);
  if (subToken) {
    context.SubToken.set({
      ...subToken,
      underlyingAssetAddress: normalizeAddress(event.params.underlyingAsset),
      underlyingAssetDecimals: Number(event.params.debtTokenDecimals),
    });
  }

  const mapping = await context.ContractToPoolMapping.get(normalizeAddress(event.params.pool));
  if (mapping) {
    context.MapAssetPool.set({
      id: tokenId,
      pool: mapping.pool_id,
      underlyingAsset: normalizeAddress(event.params.underlyingAsset),
    });
  }
});

VariableDebtToken.Initialized.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const tokenId = normalizeAddress(event.srcAddress);
  const subToken = await context.SubToken.get(tokenId);
  if (subToken) {
    context.SubToken.set({
      ...subToken,
      underlyingAssetAddress: normalizeAddress(event.params.underlyingAsset),
      underlyingAssetDecimals: Number(event.params.debtTokenDecimals),
    });
  }

  const mapping = await context.ContractToPoolMapping.get(normalizeAddress(event.params.pool));
  if (mapping) {
    context.MapAssetPool.set({
      id: tokenId,
      pool: mapping.pool_id,
      underlyingAsset: normalizeAddress(event.params.underlyingAsset),
    });
  }
});

async function recordReserveParamsHistory(
  context: handlerContext,
  reserveId: string,
  timestamp: number,
  txHash: string,
  logIndex: number
): Promise<void> {
  const reserve = await context.Reserve.get(reserveId);
  if (!reserve) return;

  const id = getHistoryEntityId(txHash, logIndex);
  context.ReserveParamsHistoryItem.set({
    id,
    reserve_id: reserveId,
    variableBorrowRate: reserve.variableBorrowRate,
    variableBorrowIndex: reserve.variableBorrowIndex,
    utilizationRate: reserve.utilizationRate,
    stableBorrowRate: reserve.stableBorrowRate,
    averageStableBorrowRate: reserve.averageStableRate,
    liquidityIndex: reserve.liquidityIndex,
    liquidityRate: reserve.liquidityRate,
    totalLiquidity: reserve.totalLiquidity,
    totalATokenSupply: reserve.totalATokenSupply,
    totalLiquidityAsCollateral: reserve.totalLiquidityAsCollateral,
    availableLiquidity: reserve.availableLiquidity,
    priceInEth: reserve.priceInUsdE8,
    priceInUsd: reserve.priceInUsd,
    timestamp,
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

async function updateDailySupplyHighwater(
  context: handlerContext,
  userId: string,
  reserveId: string,
  amount: bigint,
  timestamp: number
): Promise<void> {
  const reserve = await context.Reserve.get(reserveId);
  if (!reserve) return;

  const amountTokens = toDecimal(amount, reserve.decimals);
  const priceUsd = await getAssetPriceUSD(context, reserve.underlyingAsset, timestamp);
  const amountUsd = amountTokens * priceUsd;
  const day = Math.floor(timestamp / 86400);
  const activity = await getOrCreateUserDailyActivity(context, userId, day, timestamp);

  context.UserDailyActivity.set({
    ...activity,
    dailySupplyUsdHighwater: activity.dailySupplyUsdHighwater + amountUsd,
    updatedAt: timestamp,
  });
}

async function updateDailyBorrowHighwater(
  context: handlerContext,
  userId: string,
  reserveId: string,
  amount: bigint,
  timestamp: number
): Promise<void> {
  const reserve = await context.Reserve.get(reserveId);
  if (!reserve) return;

  const amountTokens = toDecimal(amount, reserve.decimals);
  const priceUsd = await getAssetPriceUSD(context, reserve.underlyingAsset, timestamp);
  const amountUsd = amountTokens * priceUsd;
  const day = Math.floor(timestamp / 86400);
  const activity = await getOrCreateUserDailyActivity(context, userId, day, timestamp);

  context.UserDailyActivity.set({
    ...activity,
    dailyBorrowUsdHighwater: activity.dailyBorrowUsdHighwater + amountUsd,
    updatedAt: timestamp,
  });
}

async function updateDailyRepayHighwater(
  context: handlerContext,
  userId: string,
  reserveId: string,
  amount: bigint,
  timestamp: number
): Promise<void> {
  const reserve = await context.Reserve.get(reserveId);
  if (!reserve) return;

  const amountTokens = toDecimal(amount, reserve.decimals);
  const priceUsd = await getAssetPriceUSD(context, reserve.underlyingAsset, timestamp);
  const amountUsd = amountTokens * priceUsd;
  const day = Math.floor(timestamp / 86400);
  const activity = await getOrCreateUserDailyActivity(context, userId, day, timestamp);

  context.UserDailyActivity.set({
    ...activity,
    dailyRepayUsdHighwater: activity.dailyRepayUsdHighwater + amountUsd,
    updatedAt: timestamp,
  });
}

async function updateDailyWithdrawHighwater(
  context: handlerContext,
  userId: string,
  reserveId: string,
  amount: bigint,
  timestamp: number
): Promise<void> {
  const reserve = await context.Reserve.get(reserveId);
  if (!reserve) return;

  const amountTokens = toDecimal(amount, reserve.decimals);
  const priceUsd = await getAssetPriceUSD(context, reserve.underlyingAsset, timestamp);
  const amountUsd = amountTokens * priceUsd;
  const day = Math.floor(timestamp / 86400);
  const activity = await getOrCreateUserDailyActivity(context, userId, day, timestamp);

  context.UserDailyActivity.set({
    ...activity,
    dailyWithdrawUsdHighwater: activity.dailyWithdrawUsdHighwater + amountUsd,
    updatedAt: timestamp,
  });
}

async function recordPriceObserved(
  context: handlerContext,
  asset: string,
  price: bigint,
  baseUnit: bigint,
  oracle: string,
  ok: boolean,
  timestamp: number,
  blockNumber: number,
  logIndex: number
): Promise<void> {
  const assetAddress = normalizeAddress(asset);
  const oracleAddress = normalizeAddress(oracle);
  const scale = 100000000n;
  const normalized = baseUnit === scale ? price : (price * scale) / baseUnit;
  const priceUsd = Number(normalized) / 1e8;

  const existing = await context.PriceOracleAsset.get(assetAddress);
  const updatedOracle = existing
    ? (await updatePriceOracleIndex(context, existing, timestamp)).updated
    : null;
  const base = updatedOracle ?? existing;

  context.PriceOracleAsset.set({
    id: assetAddress,
    oracle_id: oracleAddress,
    priceSource: oracleAddress,
    dependentAssets: base?.dependentAssets || [],
    priceType: base?.priceType || '',
    platform: base?.platform || '',
    priceInEth: normalized,
    isFallbackRequired: !ok,
    lastUpdateTimestamp: timestamp,
    priceCacheExpiry: base?.priceCacheExpiry || 0,
    fromChainlinkSourcesRegistry: base?.fromChainlinkSourcesRegistry || false,
    lastPriceUsd: priceUsd,
    cumulativeUsdPriceHours: base?.cumulativeUsdPriceHours || 0,
    resetTimestamp: base?.resetTimestamp || 0,
    resetCumulativeUsdPriceHours: base?.resetCumulativeUsdPriceHours || 0,
  });

  const historyId = `${assetAddress}-${blockNumber}-${logIndex}`;
  context.PriceHistoryItem.set({
    id: historyId,
    asset: assetAddress,
    price: normalized,
    timestamp,
  });
}
