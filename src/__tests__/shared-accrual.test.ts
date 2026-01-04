import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

import { LEADERBOARD_START_BLOCK } from '../helpers/constants';
import { createDefaultReserve } from '../helpers/entityHelpers';
import { accruePointsForUserReserve, syncUserReservePointsBaseline } from '../handlers/shared';
import type { handlerContext } from '../../generated';
import type { t as MockDb } from '../../generated/src/TestHelpers_MockDb.gen';

process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'true';
process.env.ENVIO_DISABLE_ETH_CALLS = 'true';

const RAY = 10n ** 27n;

const ADDRESSES = {
  pool: '0x000000000000000000000000000000000000f001',
  asset: '0x000000000000000000000000000000000000f002',
  user: '0x000000000000000000000000000000000000f003',
};

function loadTestHelpers() {
  const cwd = process.cwd();
  const distTestRoot = path.join(cwd, 'dist-test');
  const generatedLink = path.join(distTestRoot, 'generated');

  const generatedIndex = path.join(generatedLink, 'index.js');
  if (!fs.existsSync(generatedIndex)) {
    if (fs.existsSync(generatedLink)) {
      fs.rmSync(generatedLink, { recursive: true, force: true });
    }
    fs.symlinkSync(path.join(cwd, 'generated'), generatedLink, 'dir');
  }

  const handlerModules = [
    'tokenization',
    'leaderboard',
    'leaderboardKeeper',
    'dustlock',
    'pool',
    'nft',
    'config',
    'rewards',
  ];
  for (const handler of handlerModules) {
    require(path.join(distTestRoot, 'src', 'handlers', `${handler}.js`));
  }

  return require(path.join(cwd, 'generated', 'src', 'TestHelpers.res.js'));
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
    scaledVariableDebt: 0n,
    currentVariableDebt: 0n,
    principalStableDebt: 0n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: timestamp,
    stableBorrowLastUpdateTimestamp: 0,
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
    scaledVariableDebt: 0n,
    currentVariableDebt: 0n,
    principalStableDebt: 0n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: timestamp,
    stableBorrowLastUpdateTimestamp: 0,
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
