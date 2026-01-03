/**
 * Configuration Event Handlers
 * PoolAddressesProviderRegistry, PoolAddressesProvider, PoolConfigurator, UserVaultFactory, UserVault
 */

import type { handlerContext } from '../../generated';
import {
  PoolAddressesProviderRegistry,
  PoolAddressesProvider,
  PoolConfigurator,
  UserVaultFactory,
  UserVault,
} from '../../generated';
import { recordProtocolTransaction, getOrCreateUser, getOrCreateProtocolStats } from './shared';
import { tryReadTokenMetadata } from '../helpers/viem';
import { getHistoryEntityId } from '../helpers/entityHelpers';
import {
  POOL_ID,
  POOL_CONFIGURATOR_ID,
  POOL_ADMIN_ID,
  EMERGENCY_ADMIN_ID,
  ZERO_ADDRESS,
  getTokenMetadata,
  normalizeAddress,
} from '../helpers/constants';

function recordReserveConfigurationHistory(
  context: handlerContext,
  reserve: {
    id: string;
    usageAsCollateralEnabled: boolean;
    borrowingEnabled: boolean;
    stableBorrowRateEnabled: boolean;
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
    stableBorrowRateEnabled: reserve.stableBorrowRateEnabled,
    isActive: reserve.isActive,
    isFrozen: reserve.isFrozen,
    reserveInterestRateStrategy: reserve.reserveInterestRateStrategy,
    baseLTVasCollateral: reserve.baseLTVasCollateral,
    reserveLiquidationThreshold: reserve.reserveLiquidationThreshold,
    reserveLiquidationBonus: reserve.reserveLiquidationBonus,
    timestamp,
  });
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

async function tryReadATokenMetadata(
  aTokenAddress: string,
  blockNumber?: bigint
): Promise<{ symbol?: string; name?: string; decimals?: number } | null> {
  const metadata = await tryReadTokenMetadata(aTokenAddress, blockNumber);
  if (!metadata) {
    return null;
  }

  let symbol = metadata.symbol || '';
  let name = metadata.name || '';
  const decimals = metadata.decimals;

  if (symbol.length > 1 && symbol.charAt(0) === 'n') {
    symbol = symbol.substring(1);
  }

  const prefix = 'Neverland Interest Bearing ';
  if (name.startsWith(prefix)) {
    name = name.substring(prefix.length);
  }

  if (!name && symbol) {
    name = symbol;
  }

  if (!symbol && name) {
    symbol = name;
  }

  /* c8 ignore start */
  if (!symbol && !name && decimals === undefined) {
    return null;
  }
  /* c8 ignore end */

  return {
    symbol,
    name: name || symbol,
    decimals,
  };
}

async function tryReadUnderlyingMetadata(
  assetAddress: string,
  blockNumber?: bigint
): Promise<{ symbol?: string; name?: string; decimals?: number } | null> {
  const metadata = await tryReadTokenMetadata(assetAddress, blockNumber);
  if (!metadata) {
    return null;
  }

  let symbol = metadata.symbol || '';
  let name = metadata.name || '';
  const decimals = metadata.decimals;

  if (!name && symbol) {
    name = symbol;
  }

  if (!symbol && name) {
    symbol = name;
  }

  /* c8 ignore next */
  if (!symbol && !name && decimals === undefined) {
    return null;
  }

  return {
    symbol,
    name: name || symbol,
    decimals,
  };
}

// ============================================
// PoolAddressesProviderRegistry Handlers
// ============================================

PoolAddressesProviderRegistry.AddressesProviderRegistered.contractRegister(({ event, context }) => {
  context.addPoolAddressesProvider(normalizeAddress(event.params.addressesProvider));
});

PoolAddressesProviderRegistry.AddressesProviderRegistered.handler(async ({ event, context }) => {
  const id = normalizeAddress(event.params.addressesProvider);
  const timestamp = Number(event.block.timestamp);

  let protocol = await context.Protocol.get('1');
  if (!protocol) {
    context.Protocol.set({
      id: '1',
    });
  }

  context.Pool.set({
    id: id,
    addressProviderId: event.params.id,
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

PoolAddressesProvider.ProxyCreated.contractRegister(({ event, context }) => {
  const contractId = event.params.id.toString();
  const proxyAddress = normalizeAddress(event.params.proxyAddress);

  if (contractId === POOL_ID) {
    context.addPool(proxyAddress);
  } else if (contractId === POOL_CONFIGURATOR_ID) {
    context.addPoolConfigurator(proxyAddress);
  }
});

PoolAddressesProvider.ProxyCreated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const proxyAddress = normalizeAddress(event.params.proxyAddress);

  context.ContractToPoolMapping.set({
    id: proxyAddress,
    pool_id: normalizeAddress(event.srcAddress),
  });
});

PoolAddressesProvider.PoolUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const providerId = normalizeAddress(event.srcAddress);
  const pool = await context.Pool.get(providerId);
  if (pool) {
    context.Pool.set({
      ...pool,
      pool: normalizeAddress(event.params.newAddress),
      lastUpdateTimestamp: Number(event.block.timestamp),
    });
  }
});

PoolAddressesProvider.PoolConfiguratorUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const providerId = normalizeAddress(event.srcAddress);
  const pool = await context.Pool.get(providerId);
  if (pool) {
    context.Pool.set({
      ...pool,
      poolConfigurator: normalizeAddress(event.params.newAddress),
      lastUpdateTimestamp: Number(event.block.timestamp),
    });
  }
});

PoolAddressesProvider.PriceOracleUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const providerId = normalizeAddress(event.srcAddress);
  const pool = await context.Pool.get(providerId);
  if (pool) {
    context.Pool.set({
      ...pool,
      proxyPriceProvider: normalizeAddress(event.params.newAddress),
      lastUpdateTimestamp: Number(event.block.timestamp),
    });
  }

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
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const providerId = normalizeAddress(event.srcAddress);
  const pool = await context.Pool.get(providerId);
  if (pool) {
    context.Pool.set({
      ...pool,
      poolDataProviderImpl: normalizeAddress(event.params.newAddress),
      lastUpdateTimestamp: Number(event.block.timestamp),
    });
  }
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

PoolConfigurator.ReserveInitialized.contractRegister(({ event, context }) => {
  context.addAToken(normalizeAddress(event.params.aToken));
  context.addVariableDebtToken(normalizeAddress(event.params.variableDebtToken));
  if (normalizeAddress(event.params.stableDebtToken) !== ZERO_ADDRESS) {
    context.addStableDebtToken(normalizeAddress(event.params.stableDebtToken));
  }
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
  const isKnownToken = tokenInfo !== null;
  const timestamp = Number(event.block.timestamp);
  const tokenAddress = asset;
  const aToken = normalizeAddress(event.params.aToken);
  const vToken = normalizeAddress(event.params.variableDebtToken);
  const sToken = normalizeAddress(event.params.stableDebtToken);
  const interestRateStrategy = normalizeAddress(event.params.interestRateStrategyAddress);

  let symbol = tokenInfo?.symbol ?? 'ERC20';
  let name = tokenInfo?.name ?? 'Token ERC20';
  let decimals = tokenInfo?.decimals ?? 18;

  if (!isKnownToken) {
    const blockNumber = BigInt(event.block.number);
    const underlyingMetadata = await tryReadUnderlyingMetadata(asset, blockNumber);
    if (underlyingMetadata) {
      if (underlyingMetadata.symbol) symbol = underlyingMetadata.symbol;
      if (underlyingMetadata.name) name = underlyingMetadata.name;
      if (underlyingMetadata.decimals !== undefined) decimals = underlyingMetadata.decimals;
    } else {
      const aTokenMetadata = await tryReadATokenMetadata(aToken, blockNumber);
      if (aTokenMetadata) {
        if (aTokenMetadata.symbol) symbol = aTokenMetadata.symbol;
        if (aTokenMetadata.name) name = aTokenMetadata.name;
        if (aTokenMetadata.decimals !== undefined) decimals = aTokenMetadata.decimals;
      }
    }

    /* c8 ignore start */
    if (!name && symbol) {
      name = symbol;
    }

    if (!symbol && name) {
      symbol = name;
    }
    /* c8 ignore end */
  }

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
    stableBorrowRateEnabled: false,
    isActive: true,
    isFrozen: false,
    isPaused: false,
    reserveFactor: 0n,
    baseLTVasCollateral: 0n,
    optimalUtilisationRate: 0n,
    reserveLiquidationThreshold: 0n,
    reserveLiquidationBonus: 0n,
    reserveInterestRateStrategy: interestRateStrategy,
    baseVariableBorrowRate: 0n,
    variableRateSlope1: 0n,
    variableRateSlope2: 0n,
    stableRateSlope1: 0n,
    stableRateSlope2: 0n,
    utilizationRate: 0,
    totalLiquidity: 0n,
    availableLiquidity: 0n,
    totalATokenSupply: 0n,
    totalLiquidityAsCollateral: 0n,
    totalSupplies: 0n,
    totalCurrentVariableDebt: 0n,
    totalScaledVariableDebt: 0n,
    totalPrincipalStableDebt: 0n,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    stableBorrowRate: 0n,
    averageStableRate: 0n,
    liquidityIndex: 0n,
    variableBorrowIndex: 0n,
    aToken_id: aToken,
    vToken_id: vToken,
    sToken_id: sToken,
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
    lifetimeScaledVariableDebt: 0n,
    lifetimeCurrentVariableDebt: 0n,
    lifetimePrincipalStableDebt: 0n,
    lifetimeWithdrawals: 0n,
    isDropped: false,
    stableDebtLastUpdateTimestamp: 0,
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
  if (sToken !== ZERO_ADDRESS) {
    context.SubToken.set({
      id: sToken,
      pool_id: actualPoolId,
      tokenContractImpl: undefined,
      underlyingAssetAddress: asset,
      underlyingAssetDecimals: decimals,
    });
  }
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

PoolConfigurator.ReserveStableRateBorrowing.handler(async ({ event, context }) => {
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
      stableBorrowRateEnabled: event.params.enabled,
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

PoolConfigurator.StableDebtTokenUpgraded.handler(async ({ event, context }) => {
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

UserVaultFactory.UserVaultCreated.contractRegister(({ event, context }) => {
  context.addUserVault(normalizeAddress(event.params.vault));
});

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

// ============================================
// UserVault Handlers
// ============================================

UserVault.LoanSelfRepaid.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const historyId = `${event.transaction.hash}-${event.logIndex}`;
  const vaultAddress = normalizeAddress(event.srcAddress);
  const userAddress = normalizeAddress(event.params.user);
  const timestamp = Number(event.block.timestamp);
  // ABI names poolAddressesProvider/debtToken but config uses debtAsset/collateralAsset.
  const poolAddressesProvider = normalizeAddress(event.params.debtAsset);
  const debtToken = normalizeAddress(event.params.collateralAsset);
  const debtTokenMeta = await context.SubToken.get(debtToken);
  const debtAsset = debtTokenMeta?.underlyingAssetAddress ?? debtToken;

  context.LoanSelfRepayment.set({
    id: historyId,
    vault: vaultAddress,
    user: userAddress,
    poolAddressesProvider,
    debtAsset,
    amount: event.params.amount,
    timestamp,
    txHash: event.transaction.hash,
  });

  let vaultSummary = await context.UserVault.get(vaultAddress);
  if (!vaultSummary) {
    vaultSummary = {
      id: vaultAddress,
      user: userAddress,
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

  const vault = await context.UserVaultEntity.get(vaultAddress);
  if (vault) {
    context.UserVaultEntity.set({
      ...vault,
      totalSelfRepayVolume: vault.totalSelfRepayVolume + event.params.amount,
      totalSelfRepayCount: vault.totalSelfRepayCount + 1n,
      lastUpdate: timestamp,
    });
  }

  const user = await context.User.get(userAddress);
  if (user) {
    context.User.set({
      ...user,
      totalSelfRepaymentsReceived: user.totalSelfRepaymentsReceived + event.params.amount,
    });
  }

  const ps = await getOrCreateProtocolStats(context, Number(event.block.timestamp));
  context.ProtocolStats.set({
    ...ps,
    totalSelfRepayVolume: ps.totalSelfRepayVolume + event.params.amount,
    totalSelfRepayCount: ps.totalSelfRepayCount + 1n,
  });
});
