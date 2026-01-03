/**
 * Math utilities for the Neverland Protocol indexer
 * Ray math for Aave V3 interest calculations
 */

// Ray = 1e27
const RAY = 10n ** 27n;
const HALF_RAY = RAY / 2n;
const WAD_RAY_RATIO = 10n ** 9n;
const SECONDS_PER_YEAR = 31556952n;

/**
 * Ray multiplication: (a * b + HALF_RAY) / RAY
 */
export function rayMul(a: bigint, b: bigint): bigint {
  if (a === 0n || b === 0n) return 0n;
  return (a * b + HALF_RAY) / RAY;
}

/**
 * Ray division: (a * RAY + b/2) / b
 */
export function rayDiv(a: bigint, b: bigint): bigint {
  if (b === 0n) return 0n;
  const halfB = b / 2n;
  return (a * RAY + halfB) / b;
}

export function rayToWad(a: bigint): bigint {
  const halfRatio = WAD_RAY_RATIO / 2n;
  return (a + halfRatio) / WAD_RAY_RATIO;
}

export function wadToRay(a: bigint): bigint {
  return a * WAD_RAY_RATIO;
}

export function calculateLinearInterest(
  rate: bigint,
  lastUpdatedTimestamp: bigint,
  nowTimestamp: bigint
): bigint {
  const timeDifference = nowTimestamp - lastUpdatedTimestamp;
  const timeDelta = rayDiv(wadToRay(timeDifference), wadToRay(SECONDS_PER_YEAR));
  return rayMul(rate, timeDelta);
}

export function calculateCompoundedInterest(
  rate: bigint,
  lastUpdatedTimestamp: bigint,
  nowTimestamp: bigint
): bigint {
  if (nowTimestamp < lastUpdatedTimestamp) {
    return RAY;
  }

  const timeDiff = nowTimestamp - lastUpdatedTimestamp;
  if (timeDiff === 0n) {
    return RAY;
  }

  const expMinusOne = timeDiff - 1n;
  const expMinusTwo = timeDiff > 2n ? timeDiff - 2n : 0n;
  const ratePerSecond = rate / SECONDS_PER_YEAR;

  const basePowerTwo = rayMul(ratePerSecond, ratePerSecond);
  const basePowerThree = rayMul(basePowerTwo, ratePerSecond);

  const secondTerm = (timeDiff * expMinusOne * basePowerTwo) / 2n;
  const thirdTerm = (timeDiff * expMinusOne * expMinusTwo * basePowerThree) / 6n;

  return RAY + ratePerSecond * timeDiff + secondTerm + thirdTerm;
}

/**
 * Calculate interest growth between two timestamps
 * growth = principal * rate * timeDelta / SECONDS_PER_YEAR
 */
export function calculateGrowth(
  totalATokenSupply: bigint,
  liquidityRate: bigint,
  prevTimestamp: bigint,
  currentTimestamp: bigint
): bigint {
  if (currentTimestamp <= prevTimestamp) return 0n;

  const growthRate = calculateLinearInterest(liquidityRate, prevTimestamp, currentTimestamp);
  const growth = rayMul(wadToRay(totalATokenSupply), growthRate);
  return rayToWad(growth);
}

/**
 * Calculate utilization rate
 * utilization = totalBorrows / (totalBorrows + availableLiquidity)
 */
export function calculateUtilizationRate(totalBorrows: bigint, availableLiquidity: bigint): number {
  const total = totalBorrows + availableLiquidity;
  if (total === 0n) return 0;
  return Number((totalBorrows * 10000n) / total) / 10000;
}

/**
 * Convert BigInt with decimals to number
 */
export function toDecimal(value: bigint, decimals: number): number {
  if (value === 0n) return 0;
  const negative = value < 0n;
  const absValue = negative ? -value : value;
  const valueString = absValue.toString();
  if (decimals === 0) {
    const whole = Number(valueString);
    return negative ? -whole : whole;
  }
  const padded = valueString.padStart(decimals + 1, '0');
  const whole = padded.slice(0, -decimals);
  const fraction = padded.slice(-decimals).replace(/0+$/, '');
  const numberString = fraction ? `${whole}.${fraction}` : whole;
  const result = Number(numberString);
  return negative ? -result : result;
}

/**
 * Exponent to BigInt (10^n)
 */
export function exponentToBigInt(n: number): bigint {
  return 10n ** BigInt(n);
}
