// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. This file does not import the `v3-test-helpers` seam, so without this
// a bare `node --test` invocation would inherit them from the repo `.env` via envio's
// dotenv. Redundant under `pnpm run test`, which loads the same module via `--import`.
import './test-env-preload';

import assert from 'node:assert/strict';
import test from 'node:test';
import {
  LP_GROWTH_Q128,
  fungibleUnitValueGrowthX128,
  growthToPoints,
} from '../helpers/lpGrowthMath';

const PRICE_E8 = 100_000_000n;
const BASIS_POINTS = 10_000n;
const SECONDS_PER_DAY = 86_400n;
const POINTS_SCALE = 10n ** 18n;

function ceilDiv(numerator: bigint, denominator: bigint): bigint {
  return numerator === 0n ? 0n : (numerator + denominator - 1n) / denominator;
}

function absoluteDifference(left: bigint, right: bigint): bigint {
  return left >= right ? left - right : right - left;
}

const TASK8_RANDOM_SEED = 0x6d2b79f5;

function createTask8Xorshift32() {
  let state = TASK8_RANDOM_SEED;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
}

function serializeTask8Fixture(value: unknown): string {
  return JSON.stringify(value, (_key, item) => (typeof item === 'bigint' ? item.toString() : item));
}

type FungibleDifferentialInput = {
  reserve0: bigint;
  reserve1: bigint;
  token0PriceE8: bigint;
  token1PriceE8: bigint;
  token0Decimals: number;
  token1Decimals: number;
  totalSupply: bigint;
  balance: bigint;
  seconds: number;
  rateBps: bigint;
};

function referenceFungiblePoints(input: FungibleDifferentialInput): bigint {
  const amount0 = (input.reserve0 * input.balance) / input.totalSupply;
  const amount1 = (input.reserve1 * input.balance) / input.totalSupply;
  const valueE8 =
    (amount0 * input.token0PriceE8) / 10n ** BigInt(input.token0Decimals) +
    (amount1 * input.token1PriceE8) / 10n ** BigInt(input.token1Decimals);

  return (
    (valueE8 * input.rateBps * BigInt(input.seconds) * POINTS_SCALE) /
    (PRICE_E8 * BASIS_POINTS * SECONDS_PER_DAY)
  );
}

/**
 * Floors are bounded against the same unfloored proportional-reserve value.
 * The reference loses less than one raw token before each token-price floor;
 * the lazy path loses less than one price-E8 unit per reserve leg and one Q128
 * unit when dividing pool value by supply. Each settlement interval also gets
 * one final integer-division unit on either side.
 */
function fungibleDifferentialBound(input: FungibleDifferentialInput): bigint {
  const tokenValueFloorBoundE8 =
    ceilDiv(input.token0PriceE8, 10n ** BigInt(input.token0Decimals)) +
    ceilDiv(input.token1PriceE8, 10n ** BigInt(input.token1Decimals)) +
    4n;
  const weight = input.rateBps * BigInt(input.seconds) * POINTS_SCALE;
  const pointsDenominator = PRICE_E8 * BASIS_POINTS * SECONDS_PER_DAY;
  const tokenFloorPointsBound = ceilDiv(tokenValueFloorBoundE8 * weight, pointsDenominator) + 1n;
  const q128PointsBound = ceilDiv(input.balance * weight, LP_GROWTH_Q128 * pointsDenominator) + 1n;
  return tokenFloorPointsBound + q128PointsBound;
}

function runTask8RandomizedDifferentialProof() {
  const next = createTask8Xorshift32();
  const coverage = {
    fungibleCases: 0,
    fungibleIntervals: 0,
    poolLocalRateChanges: false,
    decimals6x18: false,
    decimals18x6: false,
  };

  for (let caseIndex = 0; caseIndex < 500; caseIndex += 1) {
    const [token0Decimals, token1Decimals] =
      caseIndex % 2 === 0 ? ([6, 18] as const) : ([18, 6] as const);
    const totalSupply = 10n ** 24n + BigInt(next()) * 10n ** 12n;
    const balance = totalSupply / BigInt(2 + (next() % 19));
    const baseRate = 250n + BigInt(next() % 5_000);
    const rates = [baseRate, baseRate + 211n, baseRate + 733n];
    const baseReserve0 = (100_000n + BigInt(next() % 900_001)) * 10n ** BigInt(token0Decimals);
    const baseReserve1 = (100_000n + BigInt(next() % 900_001)) * 10n ** BigInt(token1Decimals);
    let lazyGrowth = 0n;
    let reference = 0n;
    let roundingBound = 0n;
    const intervals: FungibleDifferentialInput[] = [];

    for (let intervalIndex = 0; intervalIndex < rates.length; intervalIndex += 1) {
      const input: FungibleDifferentialInput = {
        reserve0: baseReserve0 + BigInt(next() % 10_000) * 10n ** BigInt(token0Decimals),
        reserve1: baseReserve1 + BigInt(next() % 10_000) * 10n ** BigInt(token1Decimals),
        token0PriceE8: 80_000_000n + BigInt(next() % 40_000_001),
        token1PriceE8: 80_000_000n + BigInt(next() % 40_000_001),
        token0Decimals,
        token1Decimals,
        totalSupply,
        balance,
        seconds: 1 + (next() % 7_200),
        rateBps: rates[intervalIndex],
      };
      intervals.push(input);
      lazyGrowth += fungibleUnitValueGrowthX128({
        reserve0: input.reserve0,
        reserve1: input.reserve1,
        token0PriceE8: input.token0PriceE8,
        token1PriceE8: input.token1PriceE8,
        token0Decimals: input.token0Decimals,
        token1Decimals: input.token1Decimals,
        totalSupply: input.totalSupply,
        elapsedSeconds: input.seconds,
        lpRateBps: input.rateBps,
      });
      reference += referenceFungiblePoints(input);
      roundingBound += fungibleDifferentialBound(input);
      coverage.fungibleIntervals += 1;
    }

    const lazy = growthToPoints(balance, lazyGrowth);
    assert.ok(
      absoluteDifference(lazy, reference) <= roundingBound,
      `seed=0x${TASK8_RANDOM_SEED.toString(16)} kind=FUNGIBLE case=${caseIndex} intervals=${serializeTask8Fixture(intervals)} lazy=${lazy} reference=${reference} bound=${roundingBound}`
    );
    coverage.fungibleCases += 1;
    coverage.poolLocalRateChanges ||= new Set(rates).size > 1;
    coverage.decimals6x18 ||= token0Decimals === 6 && token1Decimals === 18;
    coverage.decimals18x6 ||= token0Decimals === 18 && token1Decimals === 6;
  }

  return coverage;
}

test('fungible growth credits the exact time-weighted share through the generic denominator', () => {
  const growth = fungibleUnitValueGrowthX128({
    reserve0: 50n,
    reserve1: 50n,
    token0PriceE8: 100_000_000n,
    token1PriceE8: 100_000_000n,
    token0Decimals: 0,
    token1Decimals: 0,
    totalSupply: 1_000n,
    elapsedSeconds: 28_800,
    lpRateBps: 10_000n,
  });

  assert.equal(growthToPoints(100n, growth), 3_333_333_333_333_333_333n);
  assert.equal(
    growthToPoints(1n, LP_GROWTH_Q128 * 100_000_000n * 10_000n * 86_400n),
    1_000_000_000_000_000_000n
  );
});

test('fungible growth rejects backward time and zeroes non-accruing intervals', () => {
  const baseInput = {
    reserve0: 50n,
    reserve1: 50n,
    token0PriceE8: 100_000_000n,
    token1PriceE8: 100_000_000n,
    token0Decimals: 0,
    token1Decimals: 0,
    totalSupply: 1_000n,
    elapsedSeconds: 1,
    lpRateBps: 1n,
  };

  assert.equal(fungibleUnitValueGrowthX128({ ...baseInput, totalSupply: 0n }), 0n);
  assert.equal(fungibleUnitValueGrowthX128({ ...baseInput, elapsedSeconds: 0 }), 0n);
  assert.equal(fungibleUnitValueGrowthX128({ ...baseInput, lpRateBps: 0n }), 0n);
  assert.throws(
    () => fungibleUnitValueGrowthX128({ ...baseInput, elapsedSeconds: -1 }),
    /elapsed seconds cannot be negative/
  );
});

// The V3 half of this proof went away with the Fenwick accumulator; the fungible half is
// the live Uniswap-V2 / Balancer path and is preserved verbatim. Dropping the V3 loop
// shifts what the shared xorshift32 stream feeds these cases, so the counts and coverage
// flags below are re-derived against the new deterministic set, not carried over.
test('fungible growth matches an independently floored reference across randomized intervals', () => {
  const task8Coverage = runTask8RandomizedDifferentialProof();

  assert.deepEqual(
    [task8Coverage.fungibleCases, task8Coverage.fungibleIntervals],
    [500, 1_500],
    `seed=0x${TASK8_RANDOM_SEED.toString(16)} deterministic randomized coverage counts`
  );
  assert.deepEqual(
    {
      poolLocalRateChanges: task8Coverage.poolLocalRateChanges,
      decimals6x18: task8Coverage.decimals6x18,
      decimals18x6: task8Coverage.decimals18x6,
    },
    { poolLocalRateChanges: true, decimals6x18: true, decimals18x6: true },
    `seed=0x${TASK8_RANDOM_SEED.toString(16)} coverage=${JSON.stringify(task8Coverage)}`
  );
});
