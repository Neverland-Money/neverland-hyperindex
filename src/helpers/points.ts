/**
 * Points calculation system for Neverland Leaderboard
 * Migrated from TheGraph subgraph
 */

import {
  SECONDS_PER_DAY,
  HOURS_PER_DAY,
  BASIS_POINTS_FLOAT,
  MAX_MULTIPLIER,
  DEFAULT_DEPOSIT_RATE_BPS,
  DEFAULT_BORROW_RATE_BPS,
  MAX_LOCK_TIME,
} from './constants';

/**
 * Calculate deposit rate per hour from config
 */
export function getDepositRatePerHour(depositRateBps: bigint | undefined): number {
  const rate =
    depositRateBps === undefined ? Number(DEFAULT_DEPOSIT_RATE_BPS) : Number(depositRateBps);
  return rate / BASIS_POINTS_FLOAT / HOURS_PER_DAY;
}

/**
 * Calculate borrow rate per hour from config
 */
export function getBorrowRatePerHour(borrowRateBps: bigint | undefined): number {
  const rate =
    borrowRateBps === undefined ? Number(DEFAULT_BORROW_RATE_BPS) : Number(borrowRateBps);
  return rate / BASIS_POINTS_FLOAT / HOURS_PER_DAY;
}

/**
 * Calculate LP rate per hour from config
 * LP points accrue per USD of in-range liquidity per hour
 */
export function getLPRatePerHour(lpRateBps: bigint | undefined): number {
  const rate = lpRateBps === undefined ? 0 : Number(lpRateBps);
  return rate / BASIS_POINTS_FLOAT / HOURS_PER_DAY;
}

/**
 * Apply multipliers to raw points
 * Combined = NFT multiplier × VP multiplier / 10000
 */
export function applyMultipliers(
  rawPoints: number,
  nftMultiplier: bigint,
  vpMultiplier: bigint
): number {
  const combinedBps = (Number(nftMultiplier) * Number(vpMultiplier)) / BASIS_POINTS_FLOAT;
  let multiplier = combinedBps / BASIS_POINTS_FLOAT;

  // Cap at 10x
  if (multiplier > MAX_MULTIPLIER) {
    multiplier = MAX_MULTIPLIER;
  }

  return rawPoints * multiplier;
}

/**
 * Calculate voting power for a veNFT token
 * For permanent locks: VP = lockedAmount
 * For decaying locks: VP = lockedAmount * timeRemaining / MAX_LOCK_TIME
 */
export function calculateVotingPower(
  lockedAmount: bigint,
  lockEnd: number,
  isPermanent: boolean,
  currentTimestamp: number
): bigint {
  if (lockedAmount === 0n) return 0n;

  // Permanent locks have full voting power
  if (isPermanent) {
    return lockedAmount;
  }

  // Lock has expired
  if (lockEnd <= currentTimestamp) {
    return 0n;
  }

  // Calculate time-decayed voting power
  const timeRemaining = BigInt(lockEnd - currentTimestamp);
  const vp = (lockedAmount * timeRemaining) / MAX_LOCK_TIME;

  return vp;
}

/**
 * Calculate VP multiplier from voting power
 * Uses tier system from VotingPowerTier entities
 */
export function calculateVPMultiplier(
  votingPower: bigint,
  tiers: Array<{ minVotingPower: bigint; multiplierBps: bigint }>
): bigint {
  if (!tiers || tiers.length === 0) {
    return 10000n; // 1x multiplier if no tiers
  }

  // Sort tiers by minVotingPower descending
  const sortedTiers = [...tiers].sort((a, b) => Number(b.minVotingPower - a.minVotingPower));

  // Find highest tier user qualifies for
  for (const tier of sortedTiers) {
    if (votingPower >= tier.minVotingPower) {
      return tier.multiplierBps;
    }
  }

  return 10000n; // Base 1x multiplier
}

/**
 * Calculate NFT multiplier based on collection count using geometric decay
 * Formula: BASE + bonus × (1 + decay + decay² + ... + decay^(n-1))
 *
 * Example with firstBonus=1000 (10%), decayRatio=9000 (90%):
 * - 1 collection: 10000 + 1000 = 11000 (1.10x)
 * - 2 collections: 10000 + 1000 + 900 = 11900 (1.19x)
 * - 3 collections: 10000 + 1000 + 900 + 810 = 12710 (1.271x)
 */
export function calculateNFTMultiplier(
  collectionCount: number,
  firstBonus: bigint,
  decayRatio: bigint
): bigint {
  if (collectionCount === 0) {
    return 10000n; // 1x multiplier
  }

  const BASIS_POINTS = 10000n;
  let multiplier = BASIS_POINTS;
  let currentBonus = firstBonus;

  // Add decaying bonuses for each collection
  for (let i = 0; i < collectionCount; i++) {
    multiplier += currentBonus;
    // Decay the bonus for next collection
    currentBonus = (currentBonus * decayRatio) / BASIS_POINTS;
  }

  return multiplier;
}

/**
 * Check if daily bonus should be awarded (anti-dust threshold)
 */
export function shouldAwardDailyBonus(
  dailyUsdHighwater: number,
  minDailyBonusUsd: number
): boolean {
  return dailyUsdHighwater >= minDailyBonusUsd;
}

/**
 * Calculate points earned for a position
 * Points = tokens × priceIndex × rate
 */
export function calculatePositionPoints(
  tokens: number,
  indexDelta: number,
  ratePerHour: number
): number {
  return tokens * indexDelta * ratePerHour;
}

/**
 * Calculate time-weighted deposit points for a user reserve
 * Called during settlePointsForAllReserves
 */
export function calculateDepositPoints(
  scaledBalance: bigint,
  currentIndex: bigint,
  lastIndex: bigint,
  priceUsdE8: bigint,
  decimals: number,
  depositRateBps: bigint,
  timeDeltaSeconds: number
): number {
  if (scaledBalance === 0n || lastIndex === 0n) return 0;

  const balanceTokens = Number(scaledBalance) / Math.pow(10, decimals);
  const priceUsd = Number(priceUsdE8) / 1e8;

  // Position value in USD
  const positionUsd = balanceTokens * priceUsd;

  // Time in hours
  const hours = timeDeltaSeconds / 3600;

  // Rate per hour (basis points to decimal)
  const ratePerHour = Number(depositRateBps) / 10000 / 24;

  // Points = position × rate × hours
  return positionUsd * ratePerHour * hours;
}

/**
 * Calculate time-weighted borrow points for a user reserve
 */
export function calculateBorrowPoints(
  scaledDebt: bigint,
  currentIndex: bigint,
  lastIndex: bigint,
  priceUsdE8: bigint,
  decimals: number,
  borrowRateBps: bigint,
  timeDeltaSeconds: number
): number {
  if (scaledDebt === 0n || lastIndex === 0n) return 0;

  const debtTokens = Number(scaledDebt) / Math.pow(10, decimals);
  const priceUsd = Number(priceUsdE8) / 1e8;

  const positionUsd = debtTokens * priceUsd;
  const hours = timeDeltaSeconds / 3600;
  const ratePerHour = Number(borrowRateBps) / 10000 / 24;

  return positionUsd * ratePerHour * hours;
}

/**
 * Get current day number from timestamp
 */
export function getCurrentDay(timestamp: number): number {
  return Math.floor(timestamp / SECONDS_PER_DAY);
}
