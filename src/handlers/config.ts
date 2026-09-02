/**
 * Configuration Event Handlers
 * PoolAddressesProviderRegistry, PoolAddressesProvider, PoolConfigurator, UserVaultFactory
 */

import {
  PoolAddressesProvider,
  PoolAddressesProviderRegistry,
  PoolConfigurator,
  UserVaultFactory,
} from '../../generated';
import { recordProtocolTransaction, getOrCreateUser } from './shared';
import { adjustPoolReserveCount } from '../helpers/protocolAggregation';
import { getHistoryEntityId } from '../helpers/entityHelpers';
import {
  POOL_ID,
  POOL_CONFIGURATOR_ID,
  POOL_ADMIN_ID,
  EMERGENCY_ADMIN_ID,
  getTokenMetadata,
  normalizeAddress,
} from '../helpers/constants';

import type { handlerContext } from '../../generated';

function recordReserveConfigurationHistory(
  context: handlerContext,
  reserve: {
    id: string;
    usageAsCollateralEnabled: boolean;
    borrowingEnabled: boolean;
    isActive: boolean;
    isFrozen: boolean;
    reserveInterestRateStrategy: string;
    baseLTVasCollateral: bigint;
    reserveLiquidationThreshold: bigint;
    reserveLiquidationBonus: bigint;
  },
  timestamp: number,
  txHash: string,
  logIndex: number
): void {
  context.ReserveConfigurationHistoryItem.set({
    id: getHistoryEntityId(txHash, logIndex),
    reserve_id: reserve.id,
    usageAsCollateralEnabled: reserve.usageAsCollateralEnabled,
    borrowingEnabled: reserve.borrowingEnabled,
    isActive: reserve.isActive,
    isFrozen: reserve.isFrozen,
    reserveInterestRateStrategy: reserve.reserveInterestRateStrategy,
    baseLTVasCollateral: reserve.baseLTVasCollateral,
    reserveLiquidationThreshold: reserve.reserveLiquidationThreshold,
    reserveLiquidationBonus: reserve.reserveLiquidationBonus,
    timestamp,
  });
}

/**
 * Whether a reserve counts toward PoolStats.reserveCount.
 *
 * A reserve counts once the configurator has listed it and until it is dropped.
 * Row existence is deliberately not the test: AToken.Initialized creates an
 * unlisted stub earlier in the same listing transaction, and a dropped reserve
 * keeps its row. Every count adjustment is the difference of this predicate
 * across one event, so replays and re-listings both land on the right number.
 */
function isCountedReserve(reserve: { isListed: boolean; isDropped: boolean } | undefined): boolean {
  return reserve !== undefined && reserve.isListed && !reserve.isDropped;
}

async function resolvePoolId(context: handlerContext, contractAddress: string): Promise<string> {
  const normalized = normalizeAddress(contractAddress);
  const mapping = await context.ContractToPoolMapping.get(normalized);
  return mapping?.pool_id || normalized;
}

async function getOrCreateAddressesProviderState(
  context: handlerContext,
  providerId: string,
  timestamp: number
) {
  let state = await context.PoolAddressesProviderState.get(providerId);
  if (!state) {
    state = {
      id: providerId,
      owner: undefined,
      aclAdmin: undefined,
      aclManager: undefined,
      poolAdmin: undefined,
      emergencyAdmin: undefined,
      priceOracleSentinel: undefined,
      marketId: undefined,
      lastUpdate: timestamp,
    };
    context.PoolAddressesProviderState.set(state);
  }

  return state;
}

async function getOrCreateProtocol(context: handlerContext): Promise<void> {
  const protocol = await context.Protocol.get('1');
  if (!protocol) {
    context.Protocol.set({ id: '1' });
  }
}

async function getOrCreatePool(context: handlerContext, providerId: string, timestamp: number) {
  await getOrCreateProtocol(context);

  let pool = await context.Pool.get(providerId);
  if (!pool) {
    pool = {
      id: providerId,
      addressProviderId: 0n,
      protocol_id: '1',
      pool: undefined,
      poolCollateralManager: undefined,
      poolConfiguratorImpl: undefined,
      poolConfigurator: undefined,
      poolDataProviderImpl: undefined,
      poolImpl: undefined,
      proxyPriceProvider: undefined,
      bridgeProtocolFee: undefined,
      flashloanPremiumToProtocol: undefined,
      flashloanPremiumTotal: undefined,
      active: true,
      paused: false,
      lastUpdateTimestamp: timestamp,
    };
    context.Pool.set(pool);
  }

  return pool;
}

// ============================================
// PoolAddressesProviderRegistry Handlers
// ============================================

PoolAddressesProviderRegistry.AddressesProviderRegistered.contractRegister(
  async ({ event, context }) => {
    context.addPoolAddressesProvider(normalizeAddress(event.params.addressesProvider));
  }
);

PoolAddressesProviderRegistry.AddressesProviderRegistered.handler(async ({ event, context }) => {
  const id = normalizeAddress(event.params.addressesProvider);
  const timestamp = Number(event.block.timestamp);

  const pool = await getOrCreatePool(context, id, timestamp);
  context.Pool.set({
    ...pool,
    addressProviderId: event.params.id,
    active: true,
    lastUpdateTimestamp: timestamp,
  });

  await getOrCreateAddressesProviderState(context, id, timestamp);
});

PoolAddressesProviderRegistry.AddressesProviderUnregistered.handler(async ({ event, context }) => {
  const id = normalizeAddress(event.params.addressesProvider);

  const pool = await context.Pool.get(id);
  if (pool) {
    context.Pool.set({
      ...pool,
      active: false,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });
  }
});

// ============================================
// PoolAddressesProvider Handlers
// ============================================

PoolAddressesProvider.ProxyCreated.contractRegister(async ({ event, context }) => {
  const contractId = event.params.id.toString();
  const proxyAddress = normalizeAddress(event.params.proxyAddress);

  if (contractId === POOL_ID) {
    context.addPool(proxyAddress);
  } else if (contractId === POOL_CONFIGURATOR_ID) {
    context.addPoolConfigurator(proxyAddress);
  }
});

PoolAddressesProvider.ProxyCreated.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    timestamp,
    BigInt(event.block.number)
  );

  const providerId = normalizeAddress(event.srcAddress);
  const proxyAddress = normalizeAddress(event.params.proxyAddress);
  const implementationAddress = normalizeAddress(event.params.implementationAddress);
  const pool = await getOrCreatePool(context, providerId, timestamp);

  context.ContractToPoolMapping.set({
    id: proxyAddress,
    pool_id: providerId,
  });

  // Pool.pool / Pool.poolConfigurator hold the PROXY addresses (aave-subgraph
  // contract; ops preflights compare them against descriptor addresses). The
  // *Updated events only carry implementation addresses — see those handlers.
  const contractId = event.params.id.toString();
  if (contractId === POOL_ID) {
    context.Pool.set({
      ...pool,
      pool: proxyAddress,
      poolImpl: implementationAddress,
      lastUpdateTimestamp: timestamp,
    });
  } else if (contractId === POOL_CONFIGURATOR_ID) {
    context.Pool.set({
      ...pool,
      poolConfigurator: proxyAddress,
      poolConfiguratorImpl: implementationAddress,
      lastUpdateTimestamp: timestamp,
    });
  }
});

PoolAddressesProvider.PoolUpdated.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    timestamp,
    BigInt(event.block.number)
  );
  const providerId = normalizeAddress(event.srcAddress);
  const pool = await getOrCreatePool(context, providerId, timestamp);
  // PoolUpdated.newAddress is the new IMPLEMENTATION behind the unchanged
  // proxy (Aave v3 setPoolImpl) — never overwrite Pool.pool with it.
  context.Pool.set({
    ...pool,
    poolImpl: normalizeAddress(event.params.newAddress),
    lastUpdateTimestamp: timestamp,
  });
});

PoolAddressesProvider.PoolConfiguratorUpdated.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    timestamp,
    BigInt(event.block.number)
  );
  const providerId = normalizeAddress(event.srcAddress);
  const pool = await getOrCreatePool(context, providerId, timestamp);
  // Same proxy-vs-impl split as PoolUpdated: newAddress is the implementation.
  context.Pool.set({
    ...pool,
    poolConfiguratorImpl: normalizeAddress(event.params.newAddress),
    lastUpdateTimestamp: timestamp,
  });
});

PoolAddressesProvider.PriceOracleUpdated.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    timestamp,
    BigInt(event.block.number)
  );
  const providerId = normalizeAddress(event.srcAddress);
  const pool = await getOrCreatePool(context, providerId, timestamp);
  context.Pool.set({
    ...pool,
    proxyPriceProvider: normalizeAddress(event.params.newAddress),
    lastUpdateTimestamp: timestamp,
  });

  context.PriceOracle.set({
    id: providerId,
    proxyPriceProvider: normalizeAddress(event.params.newAddress),
    usdPriceEth: 0n,
    usdPriceEthMainSource: '',
    usdPriceEthFallbackRequired: false,
    lastUpdateTimestamp: Number(event.block.timestamp),
    version: 1,
    baseCurrency: '',
    baseCurrencyUnit: 0n,
    fallbackPriceOracle: '',
    tokensWithFallback: [],
    usdDependentAssets: [],
  });
});

PoolAddressesProvider.PoolDataProviderUpdated.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    timestamp,
    BigInt(event.block.number)
  );
  const providerId = normalizeAddress(event.srcAddress);
  const pool = await getOrCreatePool(context, providerId, timestamp);
  context.Pool.set({
    ...pool,
    poolDataProviderImpl: normalizeAddress(event.params.newAddress),
    lastUpdateTimestamp: timestamp,
  });
});

PoolAddressesProvider.ACLAdminUpdated.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp);
  const providerId = normalizeAddress(event.srcAddress);

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.PoolAddressesProviderACLAdminUpdated.set({
    id,
    poolAddressesProvider: providerId,
    oldAddress: normalizeAddress(event.params.oldAddress),
    newAddress: normalizeAddress(event.params.newAddress),
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateAddressesProviderState(context, providerId, timestamp);
  context.PoolAddressesProviderState.set({
    ...state,
    aclAdmin: normalizeAddress(event.params.newAddress),
    lastUpdate: timestamp,
  });
});

PoolAddressesProvider.ACLManagerUpdated.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp);
  const providerId = normalizeAddress(event.srcAddress);

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.PoolAddressesProviderACLManagerUpdated.set({
    id,
    poolAddressesProvider: providerId,
    oldAddress: normalizeAddress(event.params.oldAddress),
    newAddress: normalizeAddress(event.params.newAddress),
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateAddressesProviderState(context, providerId, timestamp);
  context.PoolAddressesProviderState.set({
    ...state,
    aclManager: normalizeAddress(event.params.newAddress),
    lastUpdate: timestamp,
  });
});

PoolAddressesProvider.AddressSet.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp);
  const providerId = normalizeAddress(event.srcAddress);

  const addressId = event.params.id.toString();
  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.PoolAddressesProviderAddressSet.set({
    id,
    poolAddressesProvider: providerId,
    addressId,
    oldAddress: normalizeAddress(event.params.oldAddress),
    newAddress: normalizeAddress(event.params.newAddress),
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateAddressesProviderState(context, providerId, timestamp);
  const nextState = {
    ...state,
    lastUpdate: timestamp,
  };
  if (addressId === POOL_ADMIN_ID) {
    nextState.poolAdmin = normalizeAddress(event.params.newAddress);
  } else if (addressId === EMERGENCY_ADMIN_ID) {
    nextState.emergencyAdmin = normalizeAddress(event.params.newAddress);
  }
  context.PoolAddressesProviderState.set(nextState);
});

PoolAddressesProvider.AddressSetAsProxy.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp);
  const providerId = normalizeAddress(event.srcAddress);

  const addressId = event.params.id.toString();
  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.PoolAddressesProviderAddressSetAsProxy.set({
    id,
    poolAddressesProvider: providerId,
    addressId,
    proxyAddress: normalizeAddress(event.params.proxyAddress),
    oldImplementationAddress: normalizeAddress(event.params.oldImplementationAddress),
    newImplementationAddress: normalizeAddress(event.params.newImplementationAddress),
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateAddressesProviderState(context, providerId, timestamp);
  context.PoolAddressesProviderState.set({
    ...state,
    lastUpdate: timestamp,
  });
});

PoolAddressesProvider.MarketIdSet.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp);
  const providerId = normalizeAddress(event.srcAddress);

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.PoolAddressesProviderMarketIdSet.set({
    id,
    poolAddressesProvider: providerId,
    oldMarketId: event.params.oldMarketId,
    newMarketId: event.params.newMarketId,
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateAddressesProviderState(context, providerId, timestamp);
  context.PoolAddressesProviderState.set({
    ...state,
    marketId: event.params.newMarketId,
    lastUpdate: timestamp,
  });
});

PoolAddressesProvider.OwnershipTransferred.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp);
  const providerId = normalizeAddress(event.srcAddress);

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.PoolAddressesProviderOwnershipTransferred.set({
    id,
    poolAddressesProvider: providerId,
    previousOwner: normalizeAddress(event.params.previousOwner),
    newOwner: normalizeAddress(event.params.newOwner),
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateAddressesProviderState(context, providerId, timestamp);
  context.PoolAddressesProviderState.set({
    ...state,
    owner: normalizeAddress(event.params.newOwner),
    lastUpdate: timestamp,
  });
});

PoolAddressesProvider.PriceOracleSentinelUpdated.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp);
  const providerId = normalizeAddress(event.srcAddress);

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.PoolAddressesProviderPriceOracleSentinelUpdated.set({
    id,
    poolAddressesProvider: providerId,
    oldAddress: normalizeAddress(event.params.oldAddress),
    newAddress: normalizeAddress(event.params.newAddress),
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateAddressesProviderState(context, providerId, timestamp);
  context.PoolAddressesProviderState.set({
    ...state,
    priceOracleSentinel: normalizeAddress(event.params.newAddress),
    lastUpdate: timestamp,
  });
});

// ============================================
// PoolConfigurator Handlers
// ============================================

PoolConfigurator.ReserveInitialized.contractRegister(async ({ event, context }) => {
  context.addAToken(normalizeAddress(event.params.aToken));
  context.addVariableDebtToken(normalizeAddress(event.params.variableDebtToken));
});

PoolConfigurator.ReserveInitialized.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const actualPoolId = await resolvePoolId(context, event.srcAddress);

  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${actualPoolId}`;
  const tokenInfo = getTokenMetadata(asset);
  const timestamp = Number(event.block.timestamp);
  const tokenAddress = asset;
  const aToken = normalizeAddress(event.params.aToken);
  const vToken = normalizeAddress(event.params.variableDebtToken);
  const interestRateStrategy = normalizeAddress(event.params.interestRateStrategyAddress);

  // KNOWN_TOKENS is authoritative when it has the asset: it carries the curated
  // display casing and decimals, and nothing on chain should override it.
  //
  // Otherwise fall back to whatever is already recorded. ReserveInitialized carries
  // no metadata at all, so 'ERC20'/'Token ERC20'/18 are pure placeholders -- but
  // PoolConfigurator.initReserves initializes the aToken proxy BEFORE emitting this
  // event, so AToken.Initialized has already run and derived the real symbol and
  // decimals from `aTokenSymbol`/`aTokenName`/`aTokenDecimals`. Writing the
  // placeholders unconditionally clobbered that, which is how an unlisted 6-decimal
  // asset ended up valued as 18 decimals in TVL and LP pricing.
  const previousTokenInfo = await context.TokenInfo.get(tokenAddress);
  const symbol = tokenInfo?.symbol ?? previousTokenInfo?.symbol ?? 'ERC20';
  const name = tokenInfo?.name ?? previousTokenInfo?.name ?? 'Token ERC20';
  const decimals = tokenInfo?.decimals ?? previousTokenInfo?.decimals ?? 18;

  context.TokenInfo.set({
    id: tokenAddress,
    address: tokenAddress,
    decimals,
    symbol,
    name,
    lastUpdate: timestamp,
  });

  const reserveEntity = {
    id: reserveId,
    underlyingAsset: asset,
    pool_id: actualPoolId,
    symbol,
    name,
    decimals,
    usageAsCollateralEnabled: false,
    borrowingEnabled: false,
    isActive: true,
    isFrozen: false,
    isPaused: false,
    isListed: true,
    reserveFactor: 0n,
    baseLTVasCollateral: 0n,
    optimalUtilizationRate: 0n,
    reserveLiquidationThreshold: 0n,
    reserveLiquidationBonus: 0n,
    reserveInterestRateStrategy: interestRateStrategy,
    baseVariableBorrowRate: 0n,
    variableRateSlope1: 0n,
    variableRateSlope2: 0n,
    utilizationRate: 0,
    totalLiquidity: 0n,
    availableLiquidity: 0n,
    totalATokenSupply: 0n,
    totalLiquidityAsCollateral: 0n,
    totalSupplies: 0n,
    totalCurrentDebt: 0n,
    totalScaledDebt: 0n,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    liquidityIndex: 0n,
    variableBorrowIndex: 0n,
    aToken_id: aToken,
    vToken_id: vToken,
    lifetimeFlashLoans: 0n,
    lifetimeFlashLoanPremium: 0n,
    lifetimeFlashLoanLPPremium: 0n,
    lifetimeFlashLoanProtocolPremium: 0n,
    lifetimeSuppliersInterestEarned: 0n,
    lifetimeReserveFactorAccrued: 0n,
    lifetimePortalLPFee: 0n,
    lifetimePortalProtocolFee: 0n,
    lifetimeLiquidity: 0n,
    lifetimeBorrows: 0n,
    lifetimeRepayments: 0n,
    lifetimeLiquidated: 0n,
    lifetimeScaledDebt: 0n,
    lifetimeCurrentDebt: 0n,
    lifetimeWithdrawals: 0n,
    isDropped: false,
    lastUpdateTimestamp: timestamp,
    price: asset,
    priceInUsd: 0,
    priceInUsdE8: 0n,
    siloedBorrowing: false,
    debtCeiling: 0n,
    unbackedMintCap: 0n,
    liquidationProtocolFee: 0n,
    borrowCap: 0n,
    supplyCap: 0n,
    borrowableInIsolation: false,
    eMode_id: undefined,
    accruedToTreasury: 0n,
    unbacked: 0n,
  };

  // Count the reserve only when it actually enters the counted state. Row
  // existence is not the signal: AToken.Initialized fires earlier in this same
  // transaction and leaves an unlisted stub behind, and a dropped reserve keeps
  // its row too. Both must still be counted here, while a replay of an already
  // listed, undropped reserve must not inflate the count.
  const previous = await context.Reserve.get(reserveId);
  if (!isCountedReserve(previous)) {
    await adjustPoolReserveCount(context, actualPoolId, 1, timestamp);
  }

  context.Reserve.set(reserveEntity);

  recordReserveConfigurationHistory(
    context,
    reserveEntity,
    timestamp,
    event.transaction.hash,
    Number(event.logIndex)
  );

  context.SubToken.set({
    id: aToken,
    pool_id: actualPoolId,
    tokenContractImpl: undefined,
    underlyingAssetAddress: asset,
    underlyingAssetDecimals: decimals,
  });
  context.SubToken.set({
    id: vToken,
    pool_id: actualPoolId,
    tokenContractImpl: undefined,
    underlyingAssetAddress: asset,
    underlyingAssetDecimals: decimals,
  });
});

PoolConfigurator.ReserveBorrowing.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    const updated = {
      ...reserve,
      borrowingEnabled: event.params.enabled,
      lastUpdateTimestamp: Number(event.block.timestamp),
    };
    context.Reserve.set(updated);
    recordReserveConfigurationHistory(
      context,
      updated,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }
});

PoolConfigurator.CollateralConfigurationChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    const updated = {
      ...reserve,
      baseLTVasCollateral: event.params.ltv,
      reserveLiquidationThreshold: event.params.liquidationThreshold,
      reserveLiquidationBonus: event.params.liquidationBonus,
      usageAsCollateralEnabled: event.params.ltv > 0n,
      lastUpdateTimestamp: Number(event.block.timestamp),
    };
    context.Reserve.set(updated);
    recordReserveConfigurationHistory(
      context,
      updated,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }
});

PoolConfigurator.ReserveActive.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    const updated = {
      ...reserve,
      isActive: event.params.active,
      isDropped: event.params.active ? false : reserve.isDropped,
      lastUpdateTimestamp: Number(event.block.timestamp),
    };

    // Reactivating clears isDropped, which puts a previously dropped reserve
    // back into the counted state; without this the count would never recover
    // from the drop.
    const countDelta = Number(isCountedReserve(updated)) - Number(isCountedReserve(reserve));
    if (countDelta !== 0) {
      await adjustPoolReserveCount(context, poolId, countDelta, Number(event.block.timestamp));
    }

    context.Reserve.set(updated);
    recordReserveConfigurationHistory(
      context,
      updated,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }
});

PoolConfigurator.ReserveFrozen.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    const updated = {
      ...reserve,
      isFrozen: event.params.frozen,
      lastUpdateTimestamp: Number(event.block.timestamp),
    };
    context.Reserve.set(updated);
    recordReserveConfigurationHistory(
      context,
      updated,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }
});

PoolConfigurator.ReservePaused.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    context.Reserve.set({
      ...reserve,
      isPaused: event.params.paused,
    });
  }
});

PoolConfigurator.ReserveDropped.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    // Only the transition out of the counted state changes the count; dropping
    // an already-dropped reserve, or one the configurator never listed, is a
    // no-op. isListed stays true so a later re-listing counts again.
    if (isCountedReserve(reserve)) {
      await adjustPoolReserveCount(context, poolId, -1, Number(event.block.timestamp));
    }
    context.Reserve.set({
      ...reserve,
      isActive: false,
      isDropped: true,
    });
  }
});

PoolConfigurator.ReserveFactorChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    const updated = {
      ...reserve,
      reserveFactor: event.params.newReserveFactor,
      lastUpdateTimestamp: Number(event.block.timestamp),
    };
    context.Reserve.set(updated);
    recordReserveConfigurationHistory(
      context,
      updated,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }
});

PoolConfigurator.SupplyCapChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    context.Reserve.set({
      ...reserve,
      supplyCap: event.params.newSupplyCap,
    });
  }
});

PoolConfigurator.BorrowCapChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    context.Reserve.set({
      ...reserve,
      borrowCap: event.params.newBorrowCap,
    });
  }
});

PoolConfigurator.EModeAssetCategoryChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    context.Reserve.set({
      ...reserve,
      eMode_id: event.params.newCategoryId.toString(),
    });
  }
});

PoolConfigurator.EModeCategoryAdded.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = event.params.categoryId.toString();

  context.EModeCategory.set({
    id,
    ltv: event.params.ltv,
    liquidationThreshold: event.params.liquidationThreshold,
    liquidationBonus: event.params.liquidationBonus,
    oracle: normalizeAddress(event.params.oracle),
    label: event.params.label,
  });
});

PoolConfigurator.ATokenUpgraded.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const subToken = await context.SubToken.get(normalizeAddress(event.params.proxy));
  if (subToken) {
    context.SubToken.set({
      ...subToken,
      tokenContractImpl: normalizeAddress(event.params.implementation),
    });
  }
});

PoolConfigurator.VariableDebtTokenUpgraded.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const subToken = await context.SubToken.get(normalizeAddress(event.params.proxy));
  if (subToken) {
    context.SubToken.set({
      ...subToken,
      tokenContractImpl: normalizeAddress(event.params.implementation),
    });
  }
});

PoolConfigurator.BorrowableInIsolationChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    context.Reserve.set({
      ...reserve,
      borrowableInIsolation: event.params.borrowable,
    });
  }
});

PoolConfigurator.SiloedBorrowingChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    const updated = {
      ...reserve,
      siloedBorrowing: event.params.newState,
      lastUpdateTimestamp: Number(event.block.timestamp),
    };
    context.Reserve.set(updated);
    recordReserveConfigurationHistory(
      context,
      updated,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }
});

PoolConfigurator.DebtCeilingChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    context.Reserve.set({
      ...reserve,
      debtCeiling: event.params.newDebtCeiling,
    });
  }
});

PoolConfigurator.UnbackedMintCapChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    context.Reserve.set({
      ...reserve,
      unbackedMintCap: event.params.newUnbackedMintCap,
    });
  }
});

PoolConfigurator.LiquidationProtocolFeeChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    context.Reserve.set({
      ...reserve,
      liquidationProtocolFee: event.params.newFee,
    });
  }
});

PoolConfigurator.ReserveInterestRateStrategyChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const poolId = await resolvePoolId(context, event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const reserveId = `${asset}-${poolId}`;
  const reserve = await context.Reserve.get(reserveId);
  if (reserve) {
    const updated = {
      ...reserve,
      reserveInterestRateStrategy: normalizeAddress(event.params.newStrategy),
      lastUpdateTimestamp: Number(event.block.timestamp),
    };
    context.Reserve.set(updated);
    recordReserveConfigurationHistory(
      context,
      updated,
      Number(event.block.timestamp),
      event.transaction.hash,
      Number(event.logIndex)
    );
  }
});

PoolConfigurator.FlashloanPremiumTotalUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const configurator = normalizeAddress(event.srcAddress);
  const mapping = await context.ContractToPoolMapping.get(configurator);
  const poolId = mapping?.pool_id || configurator;

  const pool = await context.Pool.get(poolId);
  if (pool) {
    context.Pool.set({
      ...pool,
      flashloanPremiumTotal: event.params.newFlashloanPremiumTotal,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });
  }
});

PoolConfigurator.FlashloanPremiumToProtocolUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const configurator = normalizeAddress(event.srcAddress);
  const mapping = await context.ContractToPoolMapping.get(configurator);
  const poolId = mapping?.pool_id || configurator;

  const pool = await context.Pool.get(poolId);
  if (pool) {
    context.Pool.set({
      ...pool,
      flashloanPremiumToProtocol: event.params.newFlashloanPremiumToProtocol,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });
  }
});

PoolConfigurator.AssetCollateralInEModeChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const categoryId = event.params.categoryId.toString();
  const asset = normalizeAddress(event.params.asset);
  const configId = `${asset}-${categoryId}`;

  let config = await context.EModeCategoryConfig.get(configId);
  if (!config) {
    config = {
      id: configId,
      category_id: categoryId,
      asset,
      collateral: false,
      borrowable: false,
    };
  }

  context.EModeCategoryConfig.set({
    ...config,
    collateral: event.params.collateral,
  });
});

PoolConfigurator.AssetBorrowableInEModeChanged.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const categoryId = event.params.categoryId.toString();
  const asset = normalizeAddress(event.params.asset);
  const configId = `${asset}-${categoryId}`;

  let config = await context.EModeCategoryConfig.get(configId);
  if (!config) {
    config = {
      id: configId,
      category_id: categoryId,
      asset,
      collateral: false,
      borrowable: false,
    };
  }

  context.EModeCategoryConfig.set({
    ...config,
    borrowable: event.params.borrowable,
  });
});

PoolConfigurator.BridgeProtocolFeeUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const configurator = normalizeAddress(event.srcAddress);
  const mapping = await context.ContractToPoolMapping.get(configurator);
  const poolId = mapping?.pool_id || configurator;

  const pool = await context.Pool.get(poolId);
  if (pool) {
    context.Pool.set({
      ...pool,
      bridgeProtocolFee: event.params.newBridgeProtocolFee,
      lastUpdateTimestamp: Number(event.block.timestamp),
    });
  }
});

// ============================================
// UserVaultFactory Handlers
// ============================================

UserVaultFactory.UserVaultCreated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const userId = normalizeAddress(event.params.user);
  const vaultId = normalizeAddress(event.params.vault);
  const timestamp = Number(event.block.timestamp);

  let user = await context.User.get(userId);

  if (!user) {
    await getOrCreateUser(context, userId);
    user = await context.User.get(userId);
  }

  if (user) {
    context.User.set({
      ...user,
      userVault_id: vaultId,
    });
  }

  context.UserVaultEntity.set({
    id: vaultId,
    owner: userId,
    createdAt: timestamp,
    lastUpdate: timestamp,
    totalSelfRepayVolume: 0n,
    totalSelfRepayCount: 0n,
    isActive: true,
  });

  context.UserVault.set({
    id: vaultId,
    user: userId,
    createdAt: timestamp,
    totalRepayVolume: 0n,
    repayCount: 0n,
    lastRepayAt: 0,
  });
});
