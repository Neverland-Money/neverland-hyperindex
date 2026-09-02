import { BASIS_POINTS, POINTS_SCALE, SECONDS_PER_DAY } from './constants';
import { pow10 } from './math';

export const LP_GROWTH_Q128 = 1n << 128n;

export function fungibleUnitValueGrowthX128(input: {
  reserve0: bigint;
  reserve1: bigint;
  token0PriceE8: bigint;
  token1PriceE8: bigint;
  token0Decimals: number;
  token1Decimals: number;
  totalSupply: bigint;
  elapsedSeconds: number;
  lpRateBps: bigint;
}): bigint {
  if (input.elapsedSeconds < 0) {
    throw new Error(`elapsed seconds cannot be negative: ${input.elapsedSeconds}`);
  }
  if (input.elapsedSeconds === 0 || input.lpRateBps <= 0n || input.totalSupply <= 0n) {
    return 0n;
  }

  const poolValueUsdE8 =
    (input.reserve0 * input.token0PriceE8) / pow10(input.token0Decimals) +
    (input.reserve1 * input.token1PriceE8) / pow10(input.token1Decimals);
  const unitValueUsdE8X128 = (poolValueUsdE8 * LP_GROWTH_Q128) / input.totalSupply;

  return unitValueUsdE8X128 * input.lpRateBps * BigInt(input.elapsedSeconds);
}

export function growthToPoints(liquidity: bigint, growthDeltaX128: bigint): bigint {
  const denominator = LP_GROWTH_Q128 * 100_000_000n * BASIS_POINTS * BigInt(SECONDS_PER_DAY);

  return (liquidity * growthDeltaX128 * POINTS_SCALE) / denominator;
}
