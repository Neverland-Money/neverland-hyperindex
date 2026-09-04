/**
 * Tokenization Event Handlers
 * AToken, VariableDebtToken
 */

import { rayDiv, rayMul, toDecimal } from '../helpers/math';
import {
  isGatewayAddress,
  KNOWN_GATEWAYS,
  ZERO_ADDRESS,
  getTokenMetadata,
  deriveReserveSymbolFromAToken,
  normalizeAddress,
} from '../helpers/constants';
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
import { createDefaultReserve, getHistoryEntityId } from '../helpers/entityHelpers';

import type { EvmOnEventContext as handlerContext } from 'envio';
import { indexer } from './registry';
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
      scaledDebt: 0n,
      currentDebt: 0n,
      liquidityRate: 0n,
      variableBorrowIndex: 0n,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: timestamp,
    };
    await addReserveToUserList(context, normalizedUser, normalizedReserveId, timestamp);
    context.UserReserve.set(userReserve);
  }
  return userReserveId;
}

// ============================================
// AToken Handlers
// ============================================

indexer.onEvent({ contract: 'AToken', event: 'Mint' }, async ({ event, context }) => {
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

  // A treasury mint is Mint(caller = the Pool contract, onBehalfOf = treasury),
  // emitted by AToken.mintToTreasury. An ordinary deposit - including one made
  // BY the treasury address - carries the supplier as caller, and an aToken
  // transfer is not a Mint at all, so neither is misread as revenue here.
  const aTokenTreasury = await context.ATokenTreasury.get(tokenAddress);
  const isTreasury =
    aTokenTreasury !== undefined &&
    normalizeAddress(event.params.caller) === aTokenTreasury.poolContract &&
    userAddress === aTokenTreasury.treasury;
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
        scaledDebt: 0n,
        currentDebt: 0n,
        liquidityRate: 0n,
        variableBorrowIndex: 0n,
        usageAsCollateralEnabledOnUser: false,
        lastUpdateTimestamp: Number(event.block.timestamp),
      };
      await addReserveToUserList(context, userAddress, reserveId, Number(event.block.timestamp));
    }

    await settlePointsForUser(
      context,
      userAddress,
      reserveId,
      Number(event.block.timestamp),
      BigInt(event.block.number),
      { ignoreCooldown: true }
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
      // Treasury mint. Only the aToken supply moves: no underlying entered the
      // pool, so liquidity must not grow, and no user position is created.
      // Revenue is booked by Pool.MintedToTreasury, which fires later in this
      // same transaction and owns lifetimeReserveFactorAccrued - crediting it
      // here as well would count the same premium twice.
      context.Reserve.set({
        ...reserve,
        totalATokenSupply: newTotalATokenSupply,
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

indexer.onEvent({ contract: 'AToken', event: 'Burn' }, async ({ event, context }) => {
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
    BigInt(event.block.number),
    { ignoreCooldown: true }
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

    context.ReserveTx.set({
      id: redeemId,
      txHash: event.transaction.hash,
      kind: 'RedeemUnderlying',
      reserve: reserveId,
      user: userAddress,
      // counterparty carries the burn recipient (event.params.target) to match
      // the retired charts feed, which showed the withdrawal receiver.
      counterparty: normalizeAddress(event.params.target),
      onBehalfOf: undefined,
      amount: event.params.value + event.params.balanceIncrease,
      timestamp: Number(event.block.timestamp),
      blockNumber: Number(event.block.number),
      logIndex: Number(event.logIndex),
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

indexer.onEvent({ contract: 'AToken', event: 'BalanceTransfer' }, async ({ event, context }) => {
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
      scaledDebt: 0n,
      currentDebt: 0n,
      liquidityRate: 0n,
      variableBorrowIndex: 0n,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: timestamp,
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
      scaledDebt: 0n,
      currentDebt: 0n,
      liquidityRate: 0n,
      variableBorrowIndex: 0n,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: timestamp,
    };
    await addReserveToUserList(context, toAddress, reserveId, timestamp);
  }

  // Settle points for real users, not gateway contracts
  if (fromAddress !== ZERO_ADDRESS && !isFromGateway) {
    await settlePointsForUser(context, fromAddress, reserveId, timestamp, blockNumber, {
      ignoreCooldown: true,
    });
  }
  if (toAddress !== ZERO_ADDRESS && !isToGateway) {
    await settlePointsForUser(context, toAddress, reserveId, timestamp, blockNumber, {
      ignoreCooldown: true,
    });
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

indexer.onEvent({ contract: 'AToken', event: 'Initialized' }, async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const tokenId = normalizeAddress(event.srcAddress);

  // Written before the SubToken guard below: AToken.Initialized fires earlier
  // than ReserveInitialized in the same transaction, so the SubToken row may not
  // exist yet and this wiring must not be lost with it.
  context.ATokenTreasury.set({
    id: tokenId,
    treasury: normalizeAddress(event.params.treasury),
    poolContract: normalizeAddress(event.params.pool),
    updatedAt: Number(event.block.timestamp),
  });

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

    // KNOWN_TOKENS is authoritative and is taken whole -- symbol, name AND decimals. A
    // curated entry is a deliberate statement about the asset (canonical display casing
    // the aToken metadata uppercases away: `loAZND`, `XAUt0`, `shMON`), so on-chain
    // discovery must not silently overwrite it. Correcting a listed reserve means editing
    // the table, not letting an event win.
    //
    // Only an asset MISSING from the table is derived from the event. That is the case
    // that matters: PoolConfigurator.ReserveInitialized carries no metadata at all, so
    // config.ts can only seed 'ERC20'/'Token ERC20'/18 placeholders for it, and leaving
    // an unlisted 6-decimal asset at 18 is how XAUt0 was mispriced in TVL and LP value.
    // `aTokenName` is just `${prefix} ${symbol}`, so name and symbol coincide there.
    const derived = deriveReserveSymbolFromAToken(
      event.params.aTokenName,
      event.params.aTokenSymbol
    );
    const { symbol, name, decimals } = tokenInfo ?? {
      symbol: derived,
      name: derived,
      decimals: Number(event.params.aTokenDecimals),
    };

    context.Reserve.set({
      ...reserve,
      symbol,
      name,
      decimals,
    });

    // Correct the TokenInfo row config.ts seeded from ReserveInitialized, which had no
    // decimals available. TokenInfo.decimals is read by LP valuation (lpEntityHelpers,
    // lp.ts), so a stale 18 here silently misprices the asset everywhere.
    context.TokenInfo.set({
      id: underlyingAsset,
      address: underlyingAsset,
      decimals,
      symbol,
      name,
      lastUpdate: Number(event.block.timestamp),
    });
  }
});

indexer.onEvent({ contract: 'AToken', event: 'PriceObserved' }, async ({ event, context }) => {
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

indexer.onEvent(
  { contract: 'VariableDebtToken', event: 'PriceObserved' },
  async ({ event, context }) => {
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
  }
);

// ============================================
// VariableDebtToken Handlers
// ============================================

indexer.onEvent({ contract: 'VariableDebtToken', event: 'Mint' }, async ({ event, context }) => {
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
      scaledDebt: 0n,
      currentDebt: 0n,
      liquidityRate: 0n,
      variableBorrowIndex: 0n,
      usageAsCollateralEnabledOnUser: false,
      lastUpdateTimestamp: Number(event.block.timestamp),
    };
    await addReserveToUserList(context, userAddress, reserveId, Number(event.block.timestamp));
  }

  await settlePointsForUser(
    context,
    userAddress,
    reserveId,
    Number(event.block.timestamp),
    BigInt(event.block.number),
    { ignoreCooldown: true }
  );

  // Subgraph: userBalanceChange = value - balanceIncrease (actual borrow)
  const userBalanceChange = event.params.value - event.params.balanceIncrease;
  const calculatedAmount = rayDiv(userBalanceChange, event.params.index);

  const newScaledDebt = userReserve.scaledDebt + calculatedAmount;
  const newCurrentDebt = rayMul(newScaledDebt, event.params.index);

  context.UserReserve.set({
    ...userReserve,
    scaledDebt: newScaledDebt,
    currentDebt: newCurrentDebt,
    liquidityRate: reserve?.liquidityRate || 0n,
    variableBorrowIndex: reserve?.variableBorrowIndex || event.params.index,
    lastUpdateTimestamp: Number(event.block.timestamp),
  });

  if (reserve) {
    const newReserveScaledDebt = reserve.totalScaledDebt + calculatedAmount;
    const newReserveCurrentDebt = rayMul(newReserveScaledDebt, event.params.index);
    const newLifetimeScaledDebt = reserve.lifetimeScaledDebt + calculatedAmount;
    const newLifetimeCurrentDebt = rayMul(newLifetimeScaledDebt, event.params.index);

    context.Reserve.set({
      ...reserve,
      totalScaledDebt: newReserveScaledDebt,
      totalCurrentDebt: newReserveCurrentDebt,
      lifetimeScaledDebt: newLifetimeScaledDebt,
      lifetimeCurrentDebt: newLifetimeCurrentDebt,
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
  if (user && userReserve.scaledDebt === 0n) {
    context.User.set({
      ...user,
      borrowedReservesCount: user.borrowedReservesCount + 1,
    });
  }

  const historyId = `${userReserveId}:${event.transaction.hash}:${event.logIndex}`;
  context.VTokenBalanceHistoryItem.set({
    id: historyId,
    userReserve_id: userReserveId,
    scaledDebt: newScaledDebt,
    currentDebt: newCurrentDebt,
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

indexer.onEvent({ contract: 'VariableDebtToken', event: 'Burn' }, async ({ event, context }) => {
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
    BigInt(event.block.number),
    { ignoreCooldown: true }
  );

  // Subgraph: userBalanceChange = value + balanceIncrease (total repayment)
  const userBalanceChange = event.params.value + event.params.balanceIncrease;
  const calculatedAmount = rayDiv(userBalanceChange, event.params.index);

  const newScaledDebt = userReserve.scaledDebt - calculatedAmount;
  const newCurrentDebt = rayMul(newScaledDebt, event.params.index);

  context.UserReserve.set({
    ...userReserve,
    scaledDebt: newScaledDebt,
    currentDebt: newCurrentDebt,
    liquidityRate: reserve?.liquidityRate || userReserve.liquidityRate,
    variableBorrowIndex: reserve?.variableBorrowIndex || event.params.index,
    lastUpdateTimestamp: Number(event.block.timestamp),
  });

  if (reserve) {
    const newReserveScaledDebt = reserve.totalScaledDebt - calculatedAmount;
    const newReserveCurrentDebt = rayMul(newReserveScaledDebt, event.params.index);

    context.Reserve.set({
      ...reserve,
      totalScaledDebt: newReserveScaledDebt,
      totalCurrentDebt: newReserveCurrentDebt,
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
    scaledDebt: newScaledDebt,
    currentDebt: newCurrentDebt,
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

  if (newScaledDebt === 0n) {
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
// ============================================

indexer.onEvent(
  { contract: 'VariableDebtToken', event: 'BorrowAllowanceDelegated' },
  async ({ event, context }) => {
    const fromUser = normalizeAddress(event.params.fromUser);
    const toUser = normalizeAddress(event.params.toUser);
    const asset = normalizeAddress(event.params.asset);
    const id = `${fromUser}-${toUser}-${asset}`;

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
    const delegatedId = `${fromUser}${toUser}${asset}`;
    context.DelegatedAllowance.set({
      id: delegatedId,
      fromUser_id: fromUser,
      toUser_id: toUser,
      amountAllowed: event.params.amount,
      userReserve_id: userReserveId,
    });
  }
);

indexer.onEvent(
  { contract: 'VariableDebtToken', event: 'Initialized' },
  async ({ event, context }) => {
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
  }
);

async function recordReserveParamsHistory(
  context: handlerContext,
  reserveId: string,
  timestamp: number,
  txHash: string,
  logIndex: number
): Promise<void> {
  const reserve = await context.Reserve.get(reserveId);
  // Defensive only: all eight call sites are already inside an `if (reserve)` block, so this
  // never fires today. Kept so the helper stays safe if it is ever called from an unguarded
  // path, and excluded from coverage because no test can reach it without dead-code callers.
  /* c8 ignore next */
  if (!reserve) return;

  const id = getHistoryEntityId(txHash, logIndex);
  context.ReserveParamsHistoryItem.set({
    id,
    reserve_id: reserveId,
    variableBorrowRate: reserve.variableBorrowRate,
    variableBorrowIndex: reserve.variableBorrowIndex,
    utilizationRate: reserve.utilizationRate,
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
