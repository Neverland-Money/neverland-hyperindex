import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { installViemMock } from './viem-mock';

process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'true';
process.env.ENVIO_DISABLE_ETH_CALLS = 'true';
process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

const ADDRESSES = {
  epochManager: '0x000000000000000000000000000000000000c001',
  config: '0x000000000000000000000000000000000000c002',
  vpMultiplier: '0x000000000000000000000000000000000000c003',
  user: '0x000000000000000000000000000000000000c004',
  userTwo: '0x000000000000000000000000000000000000c005',
  positionManager: '0x000000000000000000000000000000000000c006',
  token0: '0x000000000000000000000000000000000000c007',
  token1: '0x000000000000000000000000000000000000c008',
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

function createEventDataFactory() {
  let counter = 1;
  return (blockNumber: number, timestamp: number, srcAddress: string) => {
    const txHash = `0x${counter.toString(16).padStart(64, '0')}`;
    const mockEventData = {
      block: { number: blockNumber, timestamp },
      logIndex: counter,
      srcAddress,
      transaction: { hash: txHash },
    };
    counter += 1;
    return { mockEventData };
  };
}

test('epochs and config updates apply leaderboard changes', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 2n,
    startTime: 200n,
    ...eventData(1, 200, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStart,
    mockDb,
  });

  const snapshot = TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
    depositRateBps: 10000n,
    borrowRateBps: 20000n,
    vpRateBps: 1000n,
    supplyDailyBonus: 1000000000000000000n,
    borrowDailyBonus: 2000000000000000000n,
    repayDailyBonus: 3000000000000000000n,
    withdrawDailyBonus: 4000000000000000000n,
    cooldownSeconds: 0n,
    minDailyBonusUsd: 0n,
    timestamp: 200n,
    ...eventData(2, 200, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: snapshot,
    mockDb,
  });

  const depositRate = TestHelpers.LeaderboardConfig.DepositRateUpdated.createMockEvent({
    oldRate: 0n,
    newRate: 500n,
    timestamp: 210n,
    ...eventData(3, 210, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.DepositRateUpdated.processEvent({
    event: depositRate,
    mockDb,
  });

  const borrowRate = TestHelpers.LeaderboardConfig.BorrowRateUpdated.createMockEvent({
    oldRate: 0n,
    newRate: 750n,
    timestamp: 220n,
    ...eventData(4, 220, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.BorrowRateUpdated.processEvent({
    event: borrowRate,
    mockDb,
  });

  const vpRate = TestHelpers.LeaderboardConfig.VpRateUpdated.createMockEvent({
    oldRate: 0n,
    newRate: 1500n,
    timestamp: 230n,
    ...eventData(5, 230, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.VpRateUpdated.processEvent({
    event: vpRate,
    mockDb,
  });

  const dailyBonus = TestHelpers.LeaderboardConfig.DailyBonusUpdated.createMockEvent({
    oldSupplyBonus: 0n,
    newSupplyBonus: 500000000000000000n,
    oldBorrowBonus: 0n,
    newBorrowBonus: 600000000000000000n,
    oldRepayBonus: 0n,
    newRepayBonus: 700000000000000000n,
    oldWithdrawBonus: 0n,
    newWithdrawBonus: 800000000000000000n,
    timestamp: 240n,
    ...eventData(6, 240, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.DailyBonusUpdated.processEvent({
    event: dailyBonus,
    mockDb,
  });

  const cooldown = TestHelpers.LeaderboardConfig.CooldownUpdated.createMockEvent({
    oldSeconds: 0n,
    newSeconds: 60n,
    timestamp: 250n,
    ...eventData(7, 250, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.CooldownUpdated.processEvent({
    event: cooldown,
    mockDb,
  });

  const minUsd = TestHelpers.LeaderboardConfig.MinDailyBonusUsdUpdated.createMockEvent({
    oldMin: 0n,
    newMin: 5n,
    timestamp: 260n,
    ...eventData(8, 260, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.MinDailyBonusUsdUpdated.processEvent({
    event: minUsd,
    mockDb,
  });

  mockDb = mockDb.entities.UserTokenList.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    tokenIds: [1n],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.DustLockToken.set({
    id: '1',
    owner: ADDRESSES.user,
    lockedAmount: 1000n,
    end: 0,
    isPermanent: true,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  mockDb = mockDb.entities.UserLeaderboardState.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    nftCount: 1n,
    nftMultiplier: 50000n,
    votingPower: 0n,
    vpTierIndex: 0n,
    vpMultiplier: 10000n,
    combinedMultiplier: 10000n,
    totalEpochsParticipated: 0n,
    lifetimePoints: 0n,
    currentEpochId: undefined,
    currentEpochRank: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.VotingPowerTier.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 50000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });
  mockDb = mockDb.entities.TopK.set({
    id: 'epoch:2',
    epochNumber: 2n,
    k: 100,
    entries: ['stale'],
    updatedAt: 0,
  });
  mockDb = mockDb.entities.TopK.set({
    id: 'global',
    epochNumber: 2n,
    k: 100,
    entries: ['global:stale'],
    updatedAt: 0,
  });

  const pointsAwarded = TestHelpers.LeaderboardConfig.PointsAwarded.createMockEvent({
    user: ADDRESSES.user,
    points: 100n * 10n ** 18n,
    reason: 'manual',
    timestamp: 260n,
    ...eventData(9, 260, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsAwarded.processEvent({
    event: pointsAwarded,
    mockDb,
  });

  const pointsRemoved = TestHelpers.LeaderboardConfig.PointsRemoved.createMockEvent({
    user: ADDRESSES.user,
    points: 20n * 10n ** 18n,
    reason: 'remove',
    timestamp: 270n,
    ...eventData(10, 270, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsRemoved.processEvent({
    event: pointsRemoved,
    mockDb,
  });

  const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 2n,
    endTime: 300n,
    ...eventData(11, 300, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEnd,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('1');
  assert.equal(epoch?.isActive, false);
  assert.ok(
    mockDb.entities.ManualPointsAward.get(
      `${pointsAwarded.transaction.hash}-${pointsAwarded.logIndex}`
    )
  );
});

test('epoch end initializes missing epoch state', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 5n,
    endTime: 0n,
    ...eventData(20, 500, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEnd,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('5');
  assert.ok(epoch);
  assert.equal(epoch?.epochNumber, 5n);
});

test('lp pool config handlers register pools and rates', async () => {
  const previousExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const previousEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
  process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
  installViemMock();

  try {
    const TestHelpers = loadTestHelpers();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();

    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });

    const pool = '0x000000000000000000000000000000000000c010';
    const manager = '0x000000000000000000000000000000000000c011';
    const token0 = '0x000000000000000000000000000000000000c012';
    const token1 = '0x000000000000000000000000000000000000c013';

    const configured = TestHelpers.LeaderboardConfig.LPPoolConfigured.createMockEvent({
      pool,
      positionManager: manager,
      token0,
      token1,
      lpRateBps: 2000n,
      timestamp: 100n,
      ...eventData(30, 100, ADDRESSES.config),
    });
    mockDb = await TestHelpers.LeaderboardConfig.LPPoolConfigured.processEvent({
      event: configured,
      mockDb,
    });

    const poolConfig = mockDb.entities.LPPoolConfig.get(pool.toLowerCase());
    assert.ok(poolConfig);
    assert.equal(poolConfig?.fee, 3000);
    assert.equal(poolConfig?.lpRateBps, 2000n);

    const registry = mockDb.entities.LPPoolRegistry.get('global');
    assert.ok(registry?.poolIds.includes(pool.toLowerCase()));

    const state = mockDb.entities.LPPoolState.get(pool.toLowerCase());
    assert.ok(state);

    const disabled = TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
      pool,
      timestamp: 200n,
      ...eventData(31, 200, ADDRESSES.config),
    });
    mockDb = await TestHelpers.LeaderboardConfig.LPPoolDisabled.processEvent({
      event: disabled,
      mockDb,
    });

    const disabledConfig = mockDb.entities.LPPoolConfig.get(pool.toLowerCase());
    assert.equal(disabledConfig?.isActive, false);

    const rateUpdated = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
      oldRate: 2000n,
      newRate: 1500n,
      timestamp: 210n,
      ...eventData(32, 210, ADDRESSES.config),
    });
    mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
      event: rateUpdated,
      mockDb,
    });

    const config = mockDb.entities.LeaderboardConfig.get('global');
    assert.equal(config?.lpRateBps, 1500n);
  } finally {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = previousExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = previousEth;
  }
});

test('config updates initialize missing leaderboard config', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const depositRate = TestHelpers.LeaderboardConfig.DepositRateUpdated.createMockEvent({
    oldRate: 0n,
    newRate: 750n,
    timestamp: 300n,
    ...eventData(1, 300, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.DepositRateUpdated.processEvent({
    event: depositRate,
    mockDb,
  });

  const config = mockDb.entities.LeaderboardConfig.get('global');
  assert.ok(config);
  assert.equal(config?.depositRateBps, 750n);
});

test('epoch end handles missing start time', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 2n,
    startTime: 200n,
    ...eventData(12, 200, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStart,
    mockDb,
  });

  const prevEpoch = mockDb.entities.LeaderboardEpoch.get('1');
  assert.equal(prevEpoch?.duration, undefined);
});

test('manual points updates skip when state or epoch missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const pointsAwarded = TestHelpers.LeaderboardConfig.PointsAwarded.createMockEvent({
    user: ADDRESSES.user,
    points: 10n * 10n ** 18n,
    reason: 'manual',
    timestamp: 500n,
    ...eventData(13, 500, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsAwarded.processEvent({
    event: pointsAwarded,
    mockDb,
  });

  const awardId = `${pointsAwarded.transaction.hash}-${pointsAwarded.logIndex}`;
  assert.equal(mockDb.entities.ManualPointsAward.get(awardId), undefined);

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 3n,
    isActive: true,
  });

  const pointsAwardedNoEpoch = TestHelpers.LeaderboardConfig.PointsAwarded.createMockEvent({
    user: ADDRESSES.user,
    points: 5n * 10n ** 18n,
    reason: 'manual',
    timestamp: 505n,
    ...eventData(13, 505, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsAwarded.processEvent({
    event: pointsAwardedNoEpoch,
    mockDb,
  });

  const awardNoEpochId = `${pointsAwardedNoEpoch.transaction.hash}-${pointsAwardedNoEpoch.logIndex}`;
  assert.equal(mockDb.entities.ManualPointsAward.get(awardNoEpochId), undefined);

  const pointsRemoved = TestHelpers.LeaderboardConfig.PointsRemoved.createMockEvent({
    user: ADDRESSES.user,
    points: 5n * 10n ** 18n,
    reason: 'remove',
    timestamp: 510n,
    ...eventData(14, 510, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsRemoved.processEvent({
    event: pointsRemoved,
    mockDb,
  });

  const removeId = `${pointsRemoved.transaction.hash}-${pointsRemoved.logIndex}`;
  assert.equal(mockDb.entities.ManualPointsAward.get(removeId), undefined);

  mockDb = TestHelpers.MockDb.createMockDb();
  const pointsRemovedNoState = TestHelpers.LeaderboardConfig.PointsRemoved.createMockEvent({
    user: ADDRESSES.user,
    points: 7n * 10n ** 18n,
    reason: 'remove',
    timestamp: 520n,
    ...eventData(15, 520, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsRemoved.processEvent({
    event: pointsRemovedNoState,
    mockDb,
  });

  const removedNoStateId = `${pointsRemovedNoState.transaction.hash}-${pointsRemovedNoState.logIndex}`;
  assert.equal(mockDb.entities.ManualPointsAward.get(removedNoStateId), undefined);
});

test('zero points update clears leaderboard buckets and totals', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.UserIndex.set({
    id: `${ADDRESSES.user}:1`,
    user: ADDRESSES.user,
    epochNumber: 1n,
    points: 10,
    bucketIndex: 1,
    updatedAt: 90,
  });
  mockDb = mockDb.entities.ScoreBucket.set({
    id: 'epoch:1:b:1',
    epochNumber: 1n,
    index: 1,
    lower: 0.1,
    upper: 0.5,
    count: 1,
    updatedAt: 90,
  });
  mockDb = mockDb.entities.LeaderboardTotals.set({
    id: 'epoch:1',
    epochNumber: 1n,
    totalUsers: 1,
    updatedAt: 90,
  });
  mockDb = mockDb.entities.UserEpochStats.set({
    id: `${ADDRESSES.user}:1`,
    user_id: ADDRESSES.user,
    epochNumber: 1n,
    depositPoints: 0n,
    borrowPoints: 0n,
    lpPoints: 0n,
    dailySupplyPoints: 0n,
    dailyBorrowPoints: 0n,
    dailyRepayPoints: 0n,
    dailyWithdrawPoints: 0n,
    dailyVPPoints: 0n,
    dailyLPPoints: 0n,
    manualAwardPoints: 10n * 10n ** 18n,
    depositMultiplierBps: 10000n,
    borrowMultiplierBps: 10000n,
    vpMultiplierBps: 10000n,
    lpMultiplierBps: 10000n,
    depositPointsWithMultiplier: 0n,
    borrowPointsWithMultiplier: 0n,
    vpPointsWithMultiplier: 0n,
    lpPointsWithMultiplier: 0n,
    lastSupplyPointsDay: -1,
    lastBorrowPointsDay: -1,
    lastRepayPointsDay: -1,
    lastWithdrawPointsDay: -1,
    lastVPPointsDay: -1,
    totalPoints: 10n * 10n ** 18n,
    totalPointsWithMultiplier: 10n * 10n ** 18n,
    totalMultiplierBps: 10000n,
    lastAppliedMultiplierBps: 10000n,
    testnetBonusBps: 0n,
    rank: 0,
    firstSeenAt: 0,
    lastUpdatedAt: 90,
  });

  const pointsRemoved = TestHelpers.LeaderboardConfig.PointsRemoved.createMockEvent({
    user: ADDRESSES.user,
    points: 10n * 10n ** 18n,
    reason: 'reset',
    timestamp: 600n,
    ...eventData(30, 600, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsRemoved.processEvent({
    event: pointsRemoved,
    mockDb,
  });

  const updatedIndex = mockDb.entities.UserIndex.get(`${ADDRESSES.user}:1`);
  assert.equal(updatedIndex?.bucketIndex, -1);
  assert.equal(updatedIndex?.points, 0);
  assert.equal(mockDb.entities.UserLeaderboardState.get(ADDRESSES.user)?.lastUpdate, 600);
});

test('negative points normalize to zero', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  const pointsRemoved = TestHelpers.LeaderboardConfig.PointsRemoved.createMockEvent({
    user: ADDRESSES.user,
    points: 5n * 10n ** 18n,
    reason: 'negative',
    timestamp: 700n,
    ...eventData(31, 700, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsRemoved.processEvent({
    event: pointsRemoved,
    mockDb,
  });

  const updatedIndex = mockDb.entities.UserIndex.get(`${ADDRESSES.user}:1`);
  assert.equal(updatedIndex?.points, 0);
});

test('topK sorts tied points by user id', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  const pointsA = TestHelpers.LeaderboardConfig.PointsAwarded.createMockEvent({
    user: ADDRESSES.user,
    points: 10n * 10n ** 18n,
    reason: 'tie',
    timestamp: 800n,
    ...eventData(32, 800, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsAwarded.processEvent({
    event: pointsA,
    mockDb,
  });

  const pointsB = TestHelpers.LeaderboardConfig.PointsAwarded.createMockEvent({
    user: ADDRESSES.userTwo,
    points: 10n * 10n ** 18n,
    reason: 'tie',
    timestamp: 810n,
    ...eventData(33, 810, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsAwarded.processEvent({
    event: pointsB,
    mockDb,
  });

  const topK = mockDb.entities.TopK.get('epoch:1');
  assert.ok(topK);
  const firstEntry = mockDb.entities.TopKEntry.get(topK?.entries[0] || '');
  const secondEntry = mockDb.entities.TopKEntry.get(topK?.entries[1] || '');
  assert.ok(firstEntry);
  assert.ok(secondEntry);
  assert.ok(firstEntry?.userId < secondEntry?.userId);
});

test('topK sorts higher points first', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  const pointsHigh = TestHelpers.LeaderboardConfig.PointsAwarded.createMockEvent({
    user: ADDRESSES.user,
    points: 20n * 10n ** 18n,
    reason: 'rank',
    timestamp: 900n,
    ...eventData(34, 900, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsAwarded.processEvent({
    event: pointsHigh,
    mockDb,
  });

  const pointsLow = TestHelpers.LeaderboardConfig.PointsAwarded.createMockEvent({
    user: ADDRESSES.userTwo,
    points: 10n * 10n ** 18n,
    reason: 'rank',
    timestamp: 910n,
    ...eventData(35, 910, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsAwarded.processEvent({
    event: pointsLow,
    mockDb,
  });

  const topK = mockDb.entities.TopK.get('epoch:1');
  const firstEntry = mockDb.entities.TopKEntry.get(topK?.entries[0] || '');
  assert.equal(firstEntry?.userId, ADDRESSES.user);
});

test('voting power tier events update tiers', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const tierAdded = TestHelpers.VotingPowerMultiplier.TierAdded.createMockEvent({
    tierIndex: 1n,
    minVotingPower: 100n,
    multiplierBps: 15000n,
    ...eventData(20, 400, ADDRESSES.vpMultiplier),
  });
  mockDb = await TestHelpers.VotingPowerMultiplier.TierAdded.processEvent({
    event: tierAdded,
    mockDb,
  });

  const tierUpdated = TestHelpers.VotingPowerMultiplier.TierUpdated.createMockEvent({
    tierIndex: 1n,
    newMinVotingPower: 200n,
    newMultiplierBps: 18000n,
    ...eventData(21, 410, ADDRESSES.vpMultiplier),
  });
  mockDb = await TestHelpers.VotingPowerMultiplier.TierUpdated.processEvent({
    event: tierUpdated,
    mockDb,
  });

  const tierRemoved = TestHelpers.VotingPowerMultiplier.TierRemoved.createMockEvent({
    tierIndex: 1n,
    ...eventData(22, 420, ADDRESSES.vpMultiplier),
  });
  mockDb = await TestHelpers.VotingPowerMultiplier.TierRemoved.processEvent({
    event: tierRemoved,
    mockDb,
  });

  const tier = mockDb.entities.VotingPowerTier.get('1');
  assert.equal(tier?.isActive, false);
});

test('blacklisted users are removed from leaderboard lists', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  const entryId = `epoch:1:${ADDRESSES.user}`;
  mockDb = mockDb.entities.TopKEntry.set({
    id: entryId,
    epochNumber: 1n,
    userId: ADDRESSES.user,
    points: 10,
    rank: 1,
  });
  mockDb = mockDb.entities.TopK.set({
    id: 'epoch:1',
    epochNumber: 1n,
    k: 100,
    entries: [entryId],
    updatedAt: 0,
  });

  const globalEntryId = `global:${ADDRESSES.user}`;
  mockDb = mockDb.entities.TopKEntry.set({
    id: globalEntryId,
    epochNumber: 1n,
    userId: ADDRESSES.user,
    points: 10,
    rank: 1,
  });
  mockDb = mockDb.entities.TopK.set({
    id: 'global',
    epochNumber: 1n,
    k: 100,
    entries: [globalEntryId],
    updatedAt: 0,
  });

  mockDb = mockDb.entities.ScoreBucket.set({
    id: 'epoch:1:b:3',
    epochNumber: 1n,
    index: 3,
    lower: 1,
    upper: 2,
    count: 1,
    updatedAt: 0,
  });
  mockDb = mockDb.entities.ScoreBucket.set({
    id: 'b:3',
    epochNumber: 1n,
    index: 3,
    lower: 1,
    upper: 2,
    count: 1,
    updatedAt: 0,
  });
  mockDb = mockDb.entities.LeaderboardTotals.set({
    id: 'epoch:1',
    epochNumber: 1n,
    totalUsers: 1,
    updatedAt: 0,
  });
  mockDb = mockDb.entities.LeaderboardTotals.set({
    id: 'global',
    epochNumber: 1n,
    totalUsers: 1,
    updatedAt: 0,
  });

  mockDb = mockDb.entities.UserIndex.set({
    id: `${ADDRESSES.user}:1`,
    user: ADDRESSES.user,
    epochNumber: 1n,
    points: 10,
    bucketIndex: 3,
    updatedAt: 0,
  });
  mockDb = mockDb.entities.UserIndex.set({
    id: ADDRESSES.user,
    user: ADDRESSES.user,
    epochNumber: 1n,
    points: 10,
    bucketIndex: 3,
    updatedAt: 0,
  });

  const blacklisted = TestHelpers.LeaderboardConfig.AddressBlacklisted.createMockEvent({
    account: ADDRESSES.user,
    timestamp: 500n,
    ...eventData(10, 500, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.AddressBlacklisted.processEvent({
    event: blacklisted,
    mockDb,
  });

  const blacklist = mockDb.entities.LeaderboardBlacklist.get(ADDRESSES.user);
  assert.ok(blacklist?.isBlacklisted);

  assert.equal(mockDb.entities.UserIndex.get(`${ADDRESSES.user}:1`), undefined);
  assert.equal(mockDb.entities.UserIndex.get(ADDRESSES.user), undefined);
  assert.equal(mockDb.entities.TopKEntry.get(entryId), undefined);
  assert.equal(mockDb.entities.TopKEntry.get(globalEntryId), undefined);

  const bucket = mockDb.entities.ScoreBucket.get('epoch:1:b:3');
  assert.equal(bucket?.count, 0);
  const globalBucket = mockDb.entities.ScoreBucket.get('b:3');
  assert.equal(globalBucket?.count, 0);

  const totals = mockDb.entities.LeaderboardTotals.get('epoch:1');
  assert.equal(totals?.totalUsers, 0);
  const globalTotals = mockDb.entities.LeaderboardTotals.get('global');
  assert.equal(globalTotals?.totalUsers, 0);

  const unblacklisted = TestHelpers.LeaderboardConfig.AddressUnblacklisted.createMockEvent({
    account: ADDRESSES.user,
    timestamp: 600n,
    ...eventData(11, 600, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.AddressUnblacklisted.processEvent({
    event: unblacklisted,
    mockDb,
  });

  const cleared = mockDb.entities.LeaderboardBlacklist.get(ADDRESSES.user);
  assert.ok(cleared && cleared.isBlacklisted === false);
});

test('epoch start preserves existing start block and skips future start', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '3',
    epochNumber: 3n,
    startBlock: 77n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 3n,
    startTime: 200n,
    ...eventData(20, 100, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStart,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('3');
  assert.equal(epoch?.startBlock, 77n);
});

test('epoch start sets zero when scheduled start is in the future', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 4n,
    startTime: 500n,
    ...eventData(21, 100, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStart,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('4');
  assert.equal(epoch?.startBlock, 0n);
});

test('epoch end preserves existing end block and skips future end', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '5',
    epochNumber: 5n,
    startBlock: 0n,
    startTime: 0,
    endBlock: 123n,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 5n,
    endTime: 500n,
    ...eventData(22, 100, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEnd,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('5');
  assert.equal(epoch?.endBlock, 123n);
});

test('epoch end leaves end block undefined when scheduled end is in the future', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 6n,
    endTime: 500n,
    ...eventData(23, 100, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEnd,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('6');
  assert.equal(epoch?.endBlock, undefined);
});

test('config snapshot preserves existing lp rate', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    lpRateBps: 777n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 0,
  });

  const snapshot = TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
    depositRateBps: 100n,
    borrowRateBps: 200n,
    vpRateBps: 300n,
    supplyDailyBonus: 0n,
    borrowDailyBonus: 0n,
    repayDailyBonus: 0n,
    withdrawDailyBonus: 0n,
    cooldownSeconds: 0n,
    minDailyBonusUsd: 0n,
    timestamp: 400n,
    ...eventData(24, 400, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: snapshot,
    mockDb,
  });

  const config = mockDb.entities.LeaderboardConfig.get('global');
  assert.equal(config?.lpRateBps, 777n);
});

test('lp pool config uses default epoch and skips registry update when already registered', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const pool = '0x000000000000000000000000000000000000d001';
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [pool],
    lastUpdate: 5,
  });

  const configured = TestHelpers.LeaderboardConfig.LPPoolConfigured.createMockEvent({
    pool,
    positionManager: ADDRESSES.positionManager,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    lpRateBps: 100n,
    timestamp: 500n,
    ...eventData(25, 500, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolConfigured.processEvent({
    event: configured,
    mockDb,
  });

  const poolConfig = mockDb.entities.LPPoolConfig.get(pool.toLowerCase());
  assert.equal(poolConfig?.enabledAtEpoch, 1n);

  const registry = mockDb.entities.LPPoolRegistry.get('global');
  assert.equal(registry?.lastUpdate, 5);
});

test('lp pool disabled defaults to epoch 1 when leaderboard state missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const pool = '0x000000000000000000000000000000000000d010';
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager: ADDRESSES.positionManager,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 100n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });

  const disabled = TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
    pool,
    timestamp: 600n,
    ...eventData(26, 600, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolDisabled.processEvent({
    event: disabled,
    mockDb,
  });

  const poolConfig = mockDb.entities.LPPoolConfig.get(pool.toLowerCase());
  assert.equal(poolConfig?.disabledAtEpoch, 1n);
});
