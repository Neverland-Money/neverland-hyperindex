import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

process.env.DISABLE_EXTERNAL_CALLS = 'true';
process.env.DISABLE_ETH_CALLS = 'true';

const ADDRESSES = {
  keeper: '0x000000000000000000000000000000000000b001',
  owner: '0x000000000000000000000000000000000000b002',
  user: '0x000000000000000000000000000000000000b003',
  collection: '0x000000000000000000000000000000000000b004',
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

test('keeper events update leaderboard state and ownership', async () => {
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
  mockDb = mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 1000n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 60000n,
    decayRatio: 10000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.VotingPowerTier.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 60000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
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
    lockedAmount: 1000n * 10n ** 18n,
    end: 0,
    isPermanent: true,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
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
    manualAwardPoints: 0n,
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
    totalPoints: 0n,
    totalPointsWithMultiplier: 0n,
    totalMultiplierBps: 10000n,
    lastAppliedMultiplierBps: 10000n,
    testnetBonusBps: 0n,
    rank: 0,
    firstSeenAt: 0,
    lastUpdatedAt: 0,
  });

  const vpSynced = TestHelpers.LeaderboardKeeper.VotingPowerSynced.createMockEvent({
    user: ADDRESSES.user,
    votingPower: 1000n * 10n ** 18n,
    timestamp: 100n,
    ...eventData(10, 100, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.VotingPowerSynced.processEvent({
    event: vpSynced,
    mockDb,
  });

  const nftSynced = TestHelpers.LeaderboardKeeper.NFTBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    collection: ADDRESSES.collection,
    balance: 1n,
    timestamp: 101n,
    ...eventData(10, 101, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.NFTBalanceSynced.processEvent({
    event: nftSynced,
    mockDb,
  });

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.ok(state);
  assert.equal(state?.combinedMultiplier, 100000n);

  const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: 86400n,
    ...eventData(11, 86400, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: settle,
    mockDb,
  });

  const stats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(stats);
  assert.ok(stats?.dailyVPPoints && stats.dailyVPPoints > 0);

  const balance = TestHelpers.LeaderboardKeeper.NFTBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    collection: ADDRESSES.collection,
    balance: 1n,
    timestamp: 90000n,
    ...eventData(12, 90000, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.NFTBalanceSynced.processEvent({
    event: balance,
    mockDb,
  });

  const cleared = TestHelpers.LeaderboardKeeper.NFTBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    collection: ADDRESSES.collection,
    balance: 0n,
    timestamp: 90010n,
    ...eventData(13, 90010, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.NFTBalanceSynced.processEvent({
    event: cleared,
    mockDb,
  });

  const batch = TestHelpers.LeaderboardKeeper.BatchComplete.createMockEvent({
    operation: 'settle',
    count: 10n,
    timestamp: 90020n,
    ...eventData(14, 90020, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.BatchComplete.processEvent({
    event: batch,
    mockDb,
  });

  const keeperUpdate = TestHelpers.LeaderboardKeeper.KeeperUpdated.createMockEvent({
    oldKeeper: ADDRESSES.owner,
    newKeeper: ADDRESSES.keeper,
    ...eventData(15, 90030, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.KeeperUpdated.processEvent({
    event: keeperUpdate,
    mockDb,
  });

  const interval = TestHelpers.LeaderboardKeeper.MinSettlementIntervalUpdated.createMockEvent({
    oldInterval: 10n,
    newInterval: 20n,
    ...eventData(16, 90040, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.MinSettlementIntervalUpdated.processEvent({
    event: interval,
    mockDb,
  });

  const cooldown = TestHelpers.LeaderboardKeeper.SelfSyncCooldownUpdated.createMockEvent({
    oldCooldown: 5n,
    newCooldown: 6n,
    ...eventData(17, 90050, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.SelfSyncCooldownUpdated.processEvent({
    event: cooldown,
    mockDb,
  });

  const owner = TestHelpers.LeaderboardKeeper.OwnershipTransferred.createMockEvent({
    previousOwner: ADDRESSES.owner,
    newOwner: ADDRESSES.keeper,
    ...eventData(18, 90060, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.OwnershipTransferred.processEvent({
    event: owner,
    mockDb,
  });

  const initialized = TestHelpers.LeaderboardKeeper.Initialized.createMockEvent({
    version: 1n,
    ...eventData(19, 90070, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.Initialized.processEvent({
    event: initialized,
    mockDb,
  });

  assert.ok(
    mockDb.entities.LeaderboardKeeperBatchComplete.get(
      `${batch.transaction.hash}-${batch.logIndex}`
    )
  );
  assert.ok(
    mockDb.entities.LeaderboardKeeperInitialized.get(
      `${initialized.transaction.hash}-${initialized.logIndex}`
    )
  );
});

test('nft balance sync clamps when state count is already zero', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.UserLeaderboardState.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    nftCount: 0n,
    nftMultiplier: 10000n,
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
  mockDb = mockDb.entities.UserNFTOwnership.set({
    id: `${ADDRESSES.user}:${ADDRESSES.collection}`,
    user_id: ADDRESSES.user,
    partnership_id: ADDRESSES.collection,
    balance: 1n,
    hasNFT: true,
    lastCheckedAt: 0,
    lastCheckedBlock: 0n,
  });

  const cleared = TestHelpers.LeaderboardKeeper.NFTBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    collection: ADDRESSES.collection,
    balance: 0n,
    timestamp: 200n,
    ...eventData(99, 200, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.NFTBalanceSynced.processEvent({
    event: cleared,
    mockDb,
  });

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(state?.nftCount, 0n);
});

test('user settled uses block timestamp when event timestamp missing', async () => {
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
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: undefined as unknown as bigint,
    ...eventData(120, 777, ADDRESSES.keeper),
  });
  (settle.params as { timestamp?: bigint }).timestamp = undefined;
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: settle,
    mockDb,
  });

  const recordId = `${settle.transaction.hash}-${settle.logIndex}`;
  const record = mockDb.entities.LeaderboardKeeperUserSettled.get(recordId);
  assert.equal(record?.timestamp, 777);
});

test('voting power synced caps combined multiplier', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 50000n,
    decayRatio: 10000n,
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
  mockDb = mockDb.entities.UserLeaderboardState.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    nftCount: 1n,
    nftMultiplier: 10000n,
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

  const vpSynced = TestHelpers.LeaderboardKeeper.VotingPowerSynced.createMockEvent({
    user: ADDRESSES.user,
    votingPower: 1000n,
    timestamp: 100n,
    ...eventData(20, 100, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.VotingPowerSynced.processEvent({
    event: vpSynced,
    mockDb,
  });

  const updated = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(updated?.combinedMultiplier, 100000n);
});

test('lp balance synced records event without chain sync', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const pool = '0x000000000000000000000000000000000000b010';
  const manager = '0x000000000000000000000000000000000000b011';
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager: manager,
    token0: '0x000000000000000000000000000000000000b012',
    token1: '0x000000000000000000000000000000000000b013',
    fee: 3000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });

  const syncedMeta = eventData(21, 100, ADDRESSES.keeper);
  const synced = TestHelpers.LeaderboardKeeper.LPBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    pool,
    liquidity: 123n,
    timestamp: 100n,
    ...syncedMeta,
  });
  mockDb = await TestHelpers.LeaderboardKeeper.LPBalanceSynced.processEvent({
    event: synced,
    mockDb,
  });

  const baseline = mockDb.entities.UserLPBaseline.get(`${ADDRESSES.user}:${manager}`);
  assert.equal(baseline, undefined);

  const record = mockDb.entities.LeaderboardKeeperLPBalanceSynced.get(
    `${syncedMeta.mockEventData.transaction.hash}-${syncedMeta.mockEventData.logIndex}`
  );
  assert.ok(record);
});
