/**
 * Entity helper functions for Neverland Protocol indexer
 * Provides get-or-initialize patterns for all entities
 */

import { ZERO_ADDRESS, TREASURY_ADDRESSES, normalizeAddress } from './constants';

// Types for entity creation
export interface ProtocolEntity {
  id: string;
}

export interface PoolEntity {
  id: string;
  addressProviderId: bigint;
  protocol_id: string;
  pool: string | undefined;
  poolCollateralManager: string | undefined;
  poolConfiguratorImpl: string | undefined;
  poolImpl: string | undefined;
  poolDataProviderImpl: string | undefined;
  poolConfigurator: string | undefined;
  proxyPriceProvider: string | undefined;
  lastUpdateTimestamp: number;
  bridgeProtocolFee: bigint | undefined;
  flashloanPremiumTotal: bigint | undefined;
  flashloanPremiumToProtocol: bigint | undefined;
  active: boolean;
  paused: boolean;
}

export interface UserEntity {
  id: string;
  borrowedReservesCount: number;
  eModeCategoryId_id: string | undefined;
  points_id: string | undefined;
  unclaimedRewards: bigint;
  rewardsLastUpdated: number;
  lifetimeRewards: bigint;
  userVault_id: string | undefined;
  totalSelfRepaymentsReceived: bigint;
}

export interface ReserveEntity {
  id: string;
  underlyingAsset: string;
  pool_id: string;
  symbol: string;
  name: string;
  decimals: number;
  usageAsCollateralEnabled: boolean;
  borrowingEnabled: boolean;
  stableBorrowRateEnabled: boolean;
  isActive: boolean;
  isFrozen: boolean;
  reserveInterestRateStrategy: string;
  optimalUtilisationRate: bigint;
  variableRateSlope1: bigint;
  variableRateSlope2: bigint;
  stableRateSlope1: bigint;
  stableRateSlope2: bigint;
  baseVariableBorrowRate: bigint;
  baseLTVasCollateral: bigint;
  reserveLiquidationThreshold: bigint;
  reserveLiquidationBonus: bigint;
  utilizationRate: number;
  totalLiquidity: bigint;
  totalATokenSupply: bigint;
  totalLiquidityAsCollateral: bigint;
  availableLiquidity: bigint;
  totalPrincipalStableDebt: bigint;
  totalScaledVariableDebt: bigint;
  totalCurrentVariableDebt: bigint;
  totalSupplies: bigint;
  liquidityRate: bigint;
  accruedToTreasury: bigint;
  averageStableRate: bigint;
  variableBorrowRate: bigint;
  stableBorrowRate: bigint;
  liquidityIndex: bigint;
  variableBorrowIndex: bigint;
  aToken_id: string;
  vToken_id: string;
  sToken_id: string;
  reserveFactor: bigint;
  lastUpdateTimestamp: number;
  stableDebtLastUpdateTimestamp: number;
  isPaused: boolean;
  isDropped: boolean;
  borrowCap: bigint | undefined;
  supplyCap: bigint | undefined;
  debtCeiling: bigint | undefined;
  unbackedMintCap: bigint | undefined;
  liquidationProtocolFee: bigint | undefined;
  borrowableInIsolation: boolean | undefined;
  eMode_id: string | undefined;
  siloedBorrowing: boolean;
  lifetimeLiquidity: bigint;
  lifetimePrincipalStableDebt: bigint;
  lifetimeScaledVariableDebt: bigint;
  lifetimeCurrentVariableDebt: bigint;
  lifetimeRepayments: bigint;
  lifetimeWithdrawals: bigint;
  lifetimeBorrows: bigint;
  lifetimeLiquidated: bigint;
  lifetimeFlashLoans: bigint;
  lifetimeFlashLoanPremium: bigint;
  lifetimeFlashLoanLPPremium: bigint;
  lifetimeFlashLoanProtocolPremium: bigint;
  lifetimePortalLPFee: bigint;
  lifetimePortalProtocolFee: bigint;
  lifetimeSuppliersInterestEarned: bigint;
  lifetimeReserveFactorAccrued: bigint;
  unbacked: bigint;
  price: string;
  priceInUsd: number;
  priceInUsdE8: bigint;
}

/**
 * Create default User entity
 */
export function createDefaultUser(userId: string): UserEntity {
  return {
    id: userId,
    borrowedReservesCount: 0,
    eModeCategoryId_id: undefined,
    points_id: undefined,
    unclaimedRewards: 0n,
    rewardsLastUpdated: 0,
    lifetimeRewards: 0n,
    userVault_id: undefined,
    totalSelfRepaymentsReceived: 0n,
  };
}

/**
 * Create default Reserve entity
 */
export function createDefaultReserve(
  reserveId: string,
  poolId: string,
  underlyingAsset: string
): ReserveEntity {
  return {
    id: reserveId,
    underlyingAsset,
    pool_id: poolId,
    symbol: '',
    name: '',
    decimals: 18,
    usageAsCollateralEnabled: false,
    borrowingEnabled: false,
    stableBorrowRateEnabled: false,
    isActive: false,
    isFrozen: false,
    reserveInterestRateStrategy: ZERO_ADDRESS,
    optimalUtilisationRate: 0n,
    variableRateSlope1: 0n,
    variableRateSlope2: 0n,
    stableRateSlope1: 0n,
    stableRateSlope2: 0n,
    baseVariableBorrowRate: 0n,
    baseLTVasCollateral: 0n,
    reserveLiquidationThreshold: 0n,
    reserveLiquidationBonus: 0n,
    utilizationRate: 0,
    totalLiquidity: 0n,
    totalATokenSupply: 0n,
    totalLiquidityAsCollateral: 0n,
    availableLiquidity: 0n,
    totalPrincipalStableDebt: 0n,
    totalScaledVariableDebt: 0n,
    totalCurrentVariableDebt: 0n,
    totalSupplies: 0n,
    liquidityRate: 0n,
    accruedToTreasury: 0n,
    averageStableRate: 0n,
    variableBorrowRate: 0n,
    stableBorrowRate: 0n,
    liquidityIndex: 0n,
    variableBorrowIndex: 0n,
    aToken_id: ZERO_ADDRESS,
    vToken_id: ZERO_ADDRESS,
    sToken_id: ZERO_ADDRESS,
    reserveFactor: 0n,
    lastUpdateTimestamp: 0,
    stableDebtLastUpdateTimestamp: 0,
    isPaused: false,
    isDropped: false,
    borrowCap: undefined,
    supplyCap: undefined,
    debtCeiling: undefined,
    unbackedMintCap: undefined,
    liquidationProtocolFee: undefined,
    borrowableInIsolation: undefined,
    eMode_id: undefined,
    siloedBorrowing: false,
    lifetimeLiquidity: 0n,
    lifetimePrincipalStableDebt: 0n,
    lifetimeScaledVariableDebt: 0n,
    lifetimeCurrentVariableDebt: 0n,
    lifetimeRepayments: 0n,
    lifetimeWithdrawals: 0n,
    lifetimeBorrows: 0n,
    lifetimeLiquidated: 0n,
    lifetimeFlashLoans: 0n,
    lifetimeFlashLoanPremium: 0n,
    lifetimeFlashLoanLPPremium: 0n,
    lifetimeFlashLoanProtocolPremium: 0n,
    lifetimePortalLPFee: 0n,
    lifetimePortalProtocolFee: 0n,
    lifetimeSuppliersInterestEarned: 0n,
    lifetimeReserveFactorAccrued: 0n,
    unbacked: 0n,
    price: underlyingAsset,
    priceInUsd: 0,
    priceInUsdE8: 0n,
  };
}

/**
 * Check if address is a treasury address
 */
export function isTreasuryAddress(address: string): boolean {
  return TREASURY_ADDRESSES.includes(normalizeAddress(address));
}

/**
 * Generate reserve ID from underlying asset and pool
 */
export function getReserveId(underlyingAsset: string, poolId: string): string {
  return `${underlyingAsset}-${poolId}`;
}

/**
 * Generate user reserve ID
 */
export function getUserReserveId(userId: string, reserveId: string): string {
  return `${userId}-${reserveId}`;
}

/**
 * Generate history entity ID from transaction hash and log index
 */
export function getHistoryEntityId(txHash: string, logIndex: number): string {
  return `${txHash}-${logIndex}`;
}
