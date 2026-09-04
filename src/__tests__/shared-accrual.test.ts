import assert from 'node:assert/strict';
import { test } from 'node:test';

process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

import { AUSD_ADDRESS, LEADERBOARD_START_BLOCK } from '../helpers/constants';
import { createDefaultReserve } from '../helpers/entityHelpers';
import { LP_GROWTH_Q128 } from '../helpers/lpGrowthMath';
import { accruePointsForUserReserve, syncUserReservePointsBaseline } from '../handlers/shared';
import { TestHelpers, type MockDb } from './v3-test-helpers';

import type { EvmOnEventContext as handlerContext } from 'envio';
process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';

const RAY = 10n ** 27n;

const ADDRESSES = {
  pool: '0x000000000000000000000000000000000000f001',
  asset: '0x000000000000000000000000000000000000f002',
  user: '0x000000000000000000000000000000000000f003',
  aToken: '0x000000000000000000000000000000000000f004',
  lpPool: '0x000000000000000000000000000000000000f005',
  lpToken: '0x000000000000000000000000000000000000f006',
};

const LP_POSITION_ID = `v2:${ADDRESSES.lpPool}:${ADDRESSES.user}`;
const ONE_POINT_GROWTH_X128 = LP_GROWTH_Q128 * 100_000_000n * 10_000n * 86_400n;

function loadTestHelpers() {
  return TestHelpers;
}

function seedLeaderboardConfig(mockDb: MockDb): MockDb {
  return mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 100n,
    borrowRateBps: 100n,
    vpRateBps: 0n,
    lpRateBps: 0n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 0,
  });
}

test('accrual creates epoch stats when missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const timestamp = 10000;
  const blockNumber = BigInt(LEADERBOARD_START_BLOCK + 5);
  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: BigInt(LEADERBOARD_START_BLOCK),
    startTime: 1000,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.PriceOracleAsset.set({
    id: ADDRESSES.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: timestamp - 1000,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });
  mockDb = seedLeaderboardConfig(mockDb);

  const reserve = createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset);
  mockDb = mockDb.entities.Reserve.set({
    ...reserve,
    decimals: 6,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: timestamp + 100,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: `${ADDRESSES.user}-${reserveId}`,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.user,
    reserve_id: reserveId,
    scaledATokenBalance: 0n,
    currentATokenBalance: 1_000_000n,
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: timestamp,
  });

  await accruePointsForUserReserve(
    mockDb.entities as unknown as handlerContext,
    ADDRESSES.user,
    reserveId,
    timestamp,
    blockNumber
  );
});

test('baseline sync returns early during gaps with zero balances', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const timestamp = 2000;
  const blockNumber = BigInt(LEADERBOARD_START_BLOCK + 10);
  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 2n,
    isActive: false,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: BigInt(LEADERBOARD_START_BLOCK),
    startTime: 1500,
    endBlock: 999n,
    endTime: 1600,
    isActive: false,
    duration: 100n,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = seedLeaderboardConfig(mockDb);

  const reserve = createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset);
  mockDb = mockDb.entities.Reserve.set({
    ...reserve,
    decimals: 6,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: timestamp,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: `${ADDRESSES.user}-${reserveId}`,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.user,
    reserve_id: reserveId,
    scaledATokenBalance: 0n,
    currentATokenBalance: 0n,
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: timestamp,
  });

  await syncUserReservePointsBaseline(
    mockDb.entities as unknown as handlerContext,
    ADDRESSES.user,
    reserveId,
    timestamp,
    blockNumber
  );

  const epochStats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:2`);
  assert.equal(epochStats, undefined);
});

test('generated lending settlement consumes LP growth alongside deposit accrual', async () => {
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const timestamp = 200;
  const blockNumber = LEADERBOARD_START_BLOCK + 50;
  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: BigInt(LEADERBOARD_START_BLOCK),
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 100,
    scheduledEndTime: 300,
  });
  mockDb = mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 10_000n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    lpRateBps: 0n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: 6,
  });
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: 6,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: 100,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.UserReserve.set({
    id: `${ADDRESSES.user}-${reserveId}`,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.user,
    reserve_id: reserveId,
    scaledATokenBalance: 1_000_000n,
    currentATokenBalance: 1_000_000n,
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 100,
  });
  mockDb = mockDb.entities.PriceOracleAsset.set({
    id: ADDRESSES.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100_000_000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: 100,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [ADDRESSES.lpPool],
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    positionManager: ADDRESSES.lpPool,
    token0: AUSD_ADDRESS,
    token1: ADDRESSES.lpToken,
    fee: 3000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 100,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    currentTick: 0,
    sqrtPriceX96: 1n << 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    reserve0: 1n,
    reserve1: 1n,
    lpTotalSupply: 1n,
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.LPPoolEpochGrowth.set({
    id: `${ADDRESSES.lpPool}:1`,
    pool: ADDRESSES.lpPool,
    epochNumber: 1n,
    startTimestamp: 100,
    lastTimestamp: timestamp,
    scalarGrowthX128: ONE_POINT_GROWTH_X128,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: timestamp,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: 0,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.lpToken,
    address: ADDRESSES.lpToken,
    decimals: 0,
    symbol: 'LP1',
    name: 'LP token',
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: LP_POSITION_ID,
    tokenId: 0n,
    user_id: ADDRESSES.user,
    pool: ADDRESSES.lpPool,
    positionManager: ADDRESSES.lpPool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1n,
    amount0: 1n,
    amount1: 1n,
    isInRange: true,
    valueUsd: 200_000_000n,
    lastInRangeTimestamp: 100,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 100,
    settledLpPoints: 0n,
    createdAt: 99,
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    positionIds: [LP_POSITION_ID],
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.UserTokenList.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    tokenIds: [],
    lastUpdate: 100,
  });

  const mint = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1n,
    balanceIncrease: 0n,
    index: RAY,
    mockEventData: {
      block: { number: blockNumber, timestamp },
      logIndex: 1,
      srcAddress: ADDRESSES.aToken,
      transaction: { hash: `0x${'ab'.repeat(32)}` },
    },
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: mint, mockDb });

  const cursor = mockDb.entities.UserLPEpochCursor.get(`${LP_POSITION_ID}:1`);
  const stats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.equal(cursor?.lastSettledAt, timestamp);
  assert.equal(cursor?.growthBaselineX128, ONE_POINT_GROWTH_X128);
  assert.equal(stats?.lpPoints, 1_000_000_000_000_000_000n);
  assert.ok((stats?.depositPoints ?? 0n) > 0n);
});
