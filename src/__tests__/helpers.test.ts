import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  DEFAULT_BORROW_RATE_BPS,
  DEFAULT_DEPOSIT_RATE_BPS,
  TREASURY_ADDRESSES,
  WMON_ADDRESS,
  fromScaledPoints,
  getTokenMetadata,
  toScaledPoints,
} from '../helpers/constants';
import {
  getHistoryEntityId,
  getReserveId,
  getUserReserveId,
  isTreasuryAddress,
} from '../helpers/entityHelpers';
import {
  calculateCompoundedInterest,
  calculateGrowth,
  calculateLinearInterest,
  calculateUtilizationRate,
  exponentToBigInt,
  rayDiv,
  rayMul,
  rayToWad,
  toDecimal,
  wadToRay,
} from '../helpers/math';
import {
  applyMultipliers,
  calculateBorrowPoints,
  calculateDepositPoints,
  calculateNFTMultiplier,
  calculatePositionPoints,
  calculateVPMultiplier,
  calculateVotingPower,
  getBorrowRatePerHour,
  getCurrentDay,
  getDepositRatePerHour,
  getLPRatePerHour,
  shouldAwardDailyBonus,
} from '../helpers/points';
import { updateProtocolUsdAggregates } from '../helpers/protocolAggregation';
import { updateLeaderboard } from '../helpers/leaderboard';
import {
  publicClient,
  readActivePartnerships,
  readContract,
  readLPBalance,
  readLPPosition,
  readLPTokenOfOwnerByIndex,
  readNFTBalance,
  readPoolFee,
  readPoolSlot0,
  readTokenBalance,
  readTokenDecimals,
  readTokenName,
  readTokenSymbol,
  tryReadTokenMetadata,
} from '../helpers/viem';
import { getAmountsForLiquidity } from '../helpers/uniswapV3';
import {
  VIEM_ERROR_ADDRESS,
  VIEM_FALLBACK_ADDRESS,
  VIEM_PARTIAL_ADDRESS,
  installViemMock,
} from './viem-mock';
import type { handlerContext } from '../../generated';

const RAY = 10n ** 27n;
const WAD = 10n ** 18n;
const TEST_ADDRESS = '0x0000000000000000000000000000000000000abc';
installViemMock();

test('constants and entity helpers return expected defaults', () => {
  // Unknown token returns null
  const unknownMetadata = getTokenMetadata(TEST_ADDRESS);
  assert.equal(unknownMetadata, null);

  // Known token returns metadata
  const knownMetadata = getTokenMetadata(WMON_ADDRESS);
  assert.ok(knownMetadata);
  assert.equal(knownMetadata.symbol, 'WMON');
  assert.equal(knownMetadata.name, 'Wrapped MON');
  assert.equal(knownMetadata.decimals, 18);

  assert.ok(isTreasuryAddress(TREASURY_ADDRESSES[0]));
  assert.equal(isTreasuryAddress(TEST_ADDRESS), false);

  assert.equal(getReserveId('asset', 'pool'), 'asset-pool');
  assert.equal(getUserReserveId('user', 'reserve'), 'user-reserve');
  assert.equal(getHistoryEntityId('0xabc', 7), '0xabc-7');
});

test('constants scale helpers round-trip', () => {
  const scaled = toScaledPoints(2.5);
  assert.equal(scaled, 2500000000000000000n);
  assert.equal(fromScaledPoints(scaled), 2.5);
});

test('math helpers cover branches and conversions', () => {
  assert.equal(rayMul(0n, RAY), 0n);
  assert.equal(rayMul(RAY, RAY), RAY);
  assert.equal(rayDiv(5n, 0n), 0n);
  assert.equal(rayDiv(RAY, RAY), RAY);

  assert.equal(rayToWad(RAY), WAD);
  assert.equal(wadToRay(WAD), RAY);

  assert.ok(calculateLinearInterest(RAY, 0n, 10n) > 0n);
  assert.equal(calculateCompoundedInterest(RAY, 100n, 100n), RAY);
  assert.equal(calculateCompoundedInterest(RAY, 200n, 100n), RAY);
  assert.ok(calculateCompoundedInterest(RAY, 0n, 10n) >= RAY);
  assert.ok(calculateCompoundedInterest(RAY, 0n, 2n) >= RAY);

  assert.equal(calculateGrowth(100n, RAY, 10n, 5n), 0n);
  assert.ok(calculateGrowth(100n, RAY, 0n, 10n) >= 0n);

  assert.equal(calculateUtilizationRate(0n, 0n), 0);
  assert.equal(calculateUtilizationRate(50n, 50n), 0.5);

  assert.equal(toDecimal(123000n, 3), 123);
  assert.equal(toDecimal(123n, 0), 123);
  assert.equal(toDecimal(-456n, 0), -456);
  assert.equal(toDecimal(-12345n, 2), -123.45);
  assert.equal(exponentToBigInt(6), 10n ** 6n);
});

type ReadStore<T> = {
  get: (id: string) => Promise<T | undefined>;
};

function createStore<T>(value?: T): ReadStore<T> {
  return {
    get: async () => value,
  };
}

test('leaderboard update returns early without state or epoch', async () => {
  const contextNoState = {
    LeaderboardState: createStore(undefined),
    LeaderboardEpoch: createStore(undefined),
  } as unknown as handlerContext;
  await updateLeaderboard(contextNoState, TEST_ADDRESS, 10, 0);

  const contextNoEpoch = {
    LeaderboardState: createStore({ id: 'current', currentEpochNumber: 1n }),
    LeaderboardEpoch: createStore(undefined),
  } as unknown as handlerContext;
  await updateLeaderboard(contextNoEpoch, TEST_ADDRESS, 10, 0);
});

test('points helpers cover default, caps, and thresholds', () => {
  assert.equal(getDepositRatePerHour(undefined), Number(DEFAULT_DEPOSIT_RATE_BPS) / 10000 / 24);
  assert.equal(getBorrowRatePerHour(undefined), Number(DEFAULT_BORROW_RATE_BPS) / 10000 / 24);
  assert.equal(getDepositRatePerHour(200n), 200 / 10000 / 24);
  assert.equal(getLPRatePerHour(undefined), 0);
  assert.equal(getLPRatePerHour(200n), 200 / 10000 / 24);

  assert.equal(applyMultipliers(10, 20000n, 20000n), 40);
  assert.equal(applyMultipliers(10, 100000n, 100000n), 100);

  assert.equal(calculateVotingPower(0n, 100, false, 0), 0n);
  assert.equal(calculateVotingPower(100n, 100, true, 0), 100n);
  assert.equal(calculateVotingPower(100n, 10, false, 20), 0n);
  assert.equal(calculateVotingPower(100n, 110, false, 10), (100n * 100n) / (365n * 86400n));

  assert.equal(calculateVPMultiplier(0n, []), 10000n);
  assert.equal(
    calculateVPMultiplier(200n, [
      { minVotingPower: 100n, multiplierBps: 15000n },
      { minVotingPower: 300n, multiplierBps: 20000n },
    ]),
    15000n
  );
  assert.equal(
    calculateVPMultiplier(50n, [
      { minVotingPower: 100n, multiplierBps: 15000n },
      { minVotingPower: 300n, multiplierBps: 20000n },
    ]),
    10000n
  );

  assert.equal(calculateNFTMultiplier(0, 1000n, 9000n), 10000n);
  // 3 collections with 10% first bonus and 90% decay: 10000 + 1000 + 900 + 810 = 12710
  assert.equal(calculateNFTMultiplier(3, 1000n, 9000n), 12710n);

  assert.equal(shouldAwardDailyBonus(9.9, 10), false);
  assert.equal(shouldAwardDailyBonus(10, 10), true);

  assert.equal(calculatePositionPoints(1.5, 2, 3), 9);
  assert.equal(calculateDepositPoints(0n, RAY, RAY, 100000000n, 6, 100n, 3600), 0);
  assert.equal(calculateBorrowPoints(0n, RAY, RAY, 100000000n, 6, 100n, 3600), 0);
  assert.ok(calculateDepositPoints(1000n, RAY, RAY, 100000000n, 6, 100n, 3600) > 0);
  assert.ok(calculateBorrowPoints(1000n, RAY, RAY, 100000000n, 6, 100n, 3600) > 0);

  const defaultBorrow = getBorrowRatePerHour(undefined);
  const customBorrow = getBorrowRatePerHour(600n);
  assert.notEqual(defaultBorrow, customBorrow);

  const defaultDeposit = getDepositRatePerHour(undefined);
  const customDeposit = getDepositRatePerHour(250n);
  assert.notEqual(defaultDeposit, customDeposit);

  assert.equal(getCurrentDay(86400), 1);
});

test('uniswap v3 amount math covers branches', () => {
  const sqrtAtZero = 2n ** 96n;
  const liquidity = 10n ** 18n;

  const belowRange = getAmountsForLiquidity(sqrtAtZero, 0, 100, liquidity);
  assert.ok(belowRange.amount0 > 0n);
  assert.equal(belowRange.amount1, 0n);

  const withinRange = getAmountsForLiquidity(sqrtAtZero, -100, 100, liquidity);
  assert.ok(withinRange.amount0 > 0n);
  assert.ok(withinRange.amount1 > 0n);

  const aboveRange = getAmountsForLiquidity(sqrtAtZero, -100, 0, liquidity);
  assert.equal(aboveRange.amount0, 0n);
  assert.ok(aboveRange.amount1 > 0n);

  const swapped = getAmountsForLiquidity(sqrtAtZero, 10, 0, liquidity);
  assert.ok(swapped.amount0 > 0n);
  assert.equal(swapped.amount1, 0n);

  const swappedAbove = getAmountsForLiquidity(sqrtAtZero * 2n, 10, 0, liquidity);
  assert.equal(swappedAbove.amount0, 0n);
  assert.ok(swappedAbove.amount1 > 0n);

  const highTickAmounts = getAmountsForLiquidity(sqrtAtZero, 0x7ffff, 0x80000, liquidity);
  assert.ok(highTickAmounts.amount0 >= 0n);
  assert.ok(highTickAmounts.amount1 >= 0n);

  assert.throws(
    () => getAmountsForLiquidity(sqrtAtZero, 887273, 10, liquidity),
    /TICK_OUT_OF_RANGE/
  );
});

test('protocol aggregation placeholder is callable', async () => {
  await updateProtocolUsdAggregates({} as never, 0);
});

test('viem helpers return values and fall back on errors', async () => {
  assert.equal(await readNFTBalance(TEST_ADDRESS, TEST_ADDRESS), 123n);
  assert.equal(await readNFTBalance(VIEM_ERROR_ADDRESS, TEST_ADDRESS), null);

  assert.deepEqual(await readActivePartnerships(TEST_ADDRESS), [VIEM_PARTIAL_ADDRESS]);
  assert.deepEqual(await readActivePartnerships(TEST_ADDRESS, 5n), [VIEM_PARTIAL_ADDRESS]);
  assert.equal(await readActivePartnerships(VIEM_ERROR_ADDRESS), null);

  assert.equal(await readTokenBalance(TEST_ADDRESS, TEST_ADDRESS), 123n);
  assert.equal(await readTokenDecimals(TEST_ADDRESS), 6);
  assert.equal(await readTokenSymbol(TEST_ADDRESS), 'TST');
  assert.equal(await readTokenName(TEST_ADDRESS), 'Test Token');

  const metadata = await tryReadTokenMetadata(TEST_ADDRESS);
  assert.deepEqual(metadata, { symbol: 'TST', name: 'Test Token', decimals: 6 });

  const fallbackMetadata = await tryReadTokenMetadata(VIEM_FALLBACK_ADDRESS, 1n);
  assert.deepEqual(fallbackMetadata, { symbol: 'TST', name: 'Test Token', decimals: 6 });

  const contractResult = await readContract<bigint>(TEST_ADDRESS, [], 'balanceOf', []);
  assert.equal(contractResult, 123n);
});

test('viem helpers log non-error failures', async () => {
  const originalRead = publicClient.readContract;
  const circular: Record<string, unknown> = {};
  circular.self = circular;

  publicClient.readContract = async () => {
    throw circular;
  };

  try {
    const errors: string[] = [];
    const logger = { error: (message: string) => errors.push(message) };
    assert.equal(await readLPBalance(VIEM_ERROR_ADDRESS, TEST_ADDRESS, undefined, logger), null);
    assert.equal(
      await readLPTokenOfOwnerByIndex(VIEM_ERROR_ADDRESS, TEST_ADDRESS, 0n, undefined, logger),
      null
    );
    assert.equal(await readLPPosition(VIEM_ERROR_ADDRESS, 1n, undefined, logger), null);
    assert.equal(await readPoolSlot0(VIEM_ERROR_ADDRESS, undefined, logger), null);
    assert.equal(await readPoolFee(VIEM_ERROR_ADDRESS, undefined, logger), null);
    assert.equal(await readLPBalance(VIEM_ERROR_ADDRESS, TEST_ADDRESS, 12n, logger), null);
    assert.equal(
      await readLPTokenOfOwnerByIndex(VIEM_ERROR_ADDRESS, TEST_ADDRESS, 0n, 12n, logger),
      null
    );
    assert.equal(await readPoolSlot0(VIEM_ERROR_ADDRESS, 12n, logger), null);
    assert.equal(await readPoolFee(VIEM_ERROR_ADDRESS, 12n, logger), null);
    assert.ok(errors.length >= 5);
  } finally {
    publicClient.readContract = originalRead;
  }
});

test('readPoolFee handles bigint results', async () => {
  const originalRead = publicClient.readContract;
  publicClient.readContract = async () => 3000n;

  try {
    const fee = await readPoolFee(TEST_ADDRESS);
    assert.equal(fee, 3000);
  } finally {
    publicClient.readContract = originalRead;
  }
});
