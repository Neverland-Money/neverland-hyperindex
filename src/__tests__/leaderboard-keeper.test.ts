import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TestHelpers } from './v3-test-helpers';
import { LEADERBOARD_START_BLOCK } from '../helpers/constants';
import {
  getConfiguredLiveEpoch,
  shouldSkipMidEpochKeeperSettle,
} from '../handlers/leaderboardKeeper';
import type { handlerContext } from '../../generated';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';

const ADDRESSES = {
  keeper: '0x000000000000000000000000000000000000b001',
  owner: '0x000000000000000000000000000000000000b002',
  user: '0x000000000000000000000000000000000000b003',
  collection: '0x000000000000000000000000000000000000b004',
};

function loadTestHelpers() {
  return TestHelpers;
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
    startBlock: BigInt(LEADERBOARD_START_BLOCK),
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
    lastVPAccrualTimestamp: 0,
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
    ...eventData(LEADERBOARD_START_BLOCK + 10, 100, ADDRESSES.keeper),
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
    ...eventData(LEADERBOARD_START_BLOCK + 10, 101, ADDRESSES.keeper),
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
    ...eventData(LEADERBOARD_START_BLOCK + 11, 86400, ADDRESSES.keeper),
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

test('keeper sync events preserve special edition multiplier in combined state', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 1000n,
    decayRatio: 10000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.VotingPowerTier.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 20000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });
  mockDb = mockDb.entities.UserLeaderboardState.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    nftCount: 0n,
    nftMultiplier: 10000n,
    specialEditionCount: 1n,
    specialEditionMultiplier: 15000n,
    votingPower: 0n,
    vpTierIndex: 0n,
    vpMultiplier: 10000n,
    combinedMultiplier: 15000n,
    totalEpochsParticipated: 0n,
    lifetimePoints: 0n,
    currentEpochId: undefined,
    currentEpochRank: undefined,
    lastUpdate: 0,
  });

  const vpSynced = TestHelpers.LeaderboardKeeper.VotingPowerSynced.createMockEvent({
    user: ADDRESSES.user,
    votingPower: 1n,
    timestamp: 100n,
    ...eventData(LEADERBOARD_START_BLOCK + 20, 100, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.VotingPowerSynced.processEvent({
    event: vpSynced,
    mockDb,
  });

  let state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(state?.combinedMultiplier, 30000n);

  const nftSynced = TestHelpers.LeaderboardKeeper.NFTBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    collection: ADDRESSES.collection,
    balance: 1n,
    timestamp: 101n,
    ...eventData(LEADERBOARD_START_BLOCK + 21, 101, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.NFTBalanceSynced.processEvent({
    event: nftSynced,
    mockDb,
  });

  state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(state?.nftMultiplier, 11000n);
  assert.equal(state?.combinedMultiplier, 33000n);
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
    specialEditionCount: 0n,
    specialEditionMultiplier: 10000n,
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

function keeperGateContext(
  state: { currentEpochNumber: bigint; isActive: boolean } | undefined
): handlerContext {
  return { LeaderboardState: { get: async () => state } } as unknown as handlerContext;
}

test('keeper backfill gate skips only closed past epochs outside the gap', async () => {
  const prev = process.env.ENVIO_LEADERBOARD_LIVE_EPOCH;
  try {
    delete process.env.ENVIO_LEADERBOARD_LIVE_EPOCH;
    // gate disabled by default -> never skips
    assert.equal(getConfiguredLiveEpoch(), 0n);
    assert.equal(
      await shouldSkipMidEpochKeeperSettle(
        keeperGateContext({ currentEpochNumber: 2n, isActive: true })
      ),
      false
    );

    process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = '5';
    assert.equal(getConfiguredLiveEpoch(), 5n);
    // closed past epoch, mid-epoch (active) -> SKIP
    assert.equal(
      await shouldSkipMidEpochKeeperSettle(
        keeperGateContext({ currentEpochNumber: 2n, isActive: true })
      ),
      true
    );
    // closed past epoch, GAP (inactive) -> keep (it finalizes the epoch)
    assert.equal(
      await shouldSkipMidEpochKeeperSettle(
        keeperGateContext({ currentEpochNumber: 2n, isActive: false })
      ),
      false
    );
    // live epoch -> keep
    assert.equal(
      await shouldSkipMidEpochKeeperSettle(
        keeperGateContext({ currentEpochNumber: 5n, isActive: true })
      ),
      false
    );
    // future epoch -> keep
    assert.equal(
      await shouldSkipMidEpochKeeperSettle(
        keeperGateContext({ currentEpochNumber: 6n, isActive: true })
      ),
      false
    );
    // missing leaderboard state -> keep
    assert.equal(await shouldSkipMidEpochKeeperSettle(keeperGateContext(undefined)), false);

    // malformed / zero values disable the gate
    process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = 'not-a-number';
    assert.equal(getConfiguredLiveEpoch(), 0n);
    process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = '0';
    assert.equal(getConfiguredLiveEpoch(), 0n);
  } finally {
    if (prev === undefined) delete process.env.ENVIO_LEADERBOARD_LIVE_EPOCH;
    else process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = prev;
  }
});

test('keeper user settled: reserve user heavy sweep is skipped for a closed past epoch when the live-epoch gate is set', async () => {
  const prev = process.env.ENVIO_LEADERBOARD_LIVE_EPOCH;
  process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = '5';
  try {
    const TestHelpers = loadTestHelpers();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();

    // epoch 2 (< live 5), active (mid-epoch, not the gap) -> gate applies
    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 2n,
      isActive: true,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: '2',
      epochNumber: 2n,
      startBlock: 0n,
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });

    // The gate only protects the HEAVY reserve sweep, so this user must HAVE a
    // reserve to exercise the skip. (Pure-VP users now fall through — see the
    // companion test below.)
    mockDb = mockDb.entities.UserReserveList.set({
      id: ADDRESSES.user,
      user_id: ADDRESSES.user,
      reserveIds: ['0x00000000000000000000000000000000000000aa'],
      lastUpdate: 0,
    });

    // block past the leaderboard start, so a NON-gated settle would create UserEpochStats
    const block = Number(LEADERBOARD_START_BLOCK) + 1000;
    const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: ADDRESSES.user,
      timestamp: 1767000000n,
      ...eventData(block, 1767000000, ADDRESSES.keeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: settle,
      mockDb,
    });

    // the raw keeper event is still recorded (the gate keeps it)...
    const recordId = `${settle.transaction.hash}-${settle.logIndex}`;
    assert.ok(
      mockDb.entities.LeaderboardKeeperUserSettled.get(recordId),
      'raw keeper-settled event is still recorded'
    );
    // ...but the heavy reserve sweep was skipped -> no per-user epoch stats were created
    assert.equal(
      mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:2`),
      undefined,
      'reserve-user mid-epoch keeper settle skipped for a closed past epoch'
    );
  } finally {
    if (prev === undefined) delete process.env.ENVIO_LEADERBOARD_LIVE_EPOCH;
    else process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = prev;
  }
});

test('keeper user settled: pure-VP user still settles for a closed past epoch (gate does not drop VP decay tail)', async () => {
  const prev = process.env.ENVIO_LEADERBOARD_LIVE_EPOCH;
  process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = '5';
  try {
    const TestHelpers = loadTestHelpers();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();

    // epoch 2 (< live 5), active (mid-epoch) -> gate would skip a reserve user
    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 2n,
      isActive: true,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: '2',
      epochNumber: 2n,
      startBlock: 0n,
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });
    // a VP rate so VP points actually accrue
    mockDb = mockDb.entities.LeaderboardConfig.set({
      id: 'global',
      depositRateBps: 0n,
      borrowRateBps: 0n,
      vpRateBps: 10000n,
      lpRateBps: 0n,
      supplyDailyBonus: 0,
      borrowDailyBonus: 0,
      repayDailyBonus: 0,
      withdrawDailyBonus: 0,
      cooldownSeconds: 0,
      minDailyBonusUsd: 0,
      lastUpdate: 0,
    });

    // Pure-VP user: a permanent veDUST lock, NO reserves. Permanent => flat VP.
    const tokenId = 7000n;
    mockDb = mockDb.entities.DustLockToken.set({
      id: tokenId.toString(),
      owner: ADDRESSES.user,
      lockedAmount: 1_000_000_000_000_000_000_000n, // 1000 * 1e18
      end: 0,
      isPermanent: true,
      createdAt: 1767000000,
      updatedAt: 1767000000,
      lastDepositType: undefined,
      selfRepayEnabled: false,
      rewardReceiver: undefined,
    });
    mockDb = mockDb.entities.UserTokenList.set({
      id: ADDRESSES.user,
      user_id: ADDRESSES.user,
      tokenIds: [tokenId],
      lastUpdate: 1767000000,
    });

    // first settle establishes the VP accrual cursor at this timestamp
    const block = Number(LEADERBOARD_START_BLOCK) + 1000;
    const t0 = 1767000000;
    const settle0 = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: ADDRESSES.user,
      timestamp: BigInt(t0),
      ...eventData(block, t0, ADDRESSES.keeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: settle0,
      mockDb,
    });

    // a later settle in the same closed epoch: the pure-VP user FALLS THROUGH the
    // gate and accrues VP points over [t0, t1] (flat permanent VP * vpRate * dt).
    const t1 = t0 + 86_400; // one day later
    const settle1 = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: ADDRESSES.user,
      timestamp: BigInt(t1),
      ...eventData(block + 1, t1, ADDRESSES.keeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: settle1,
      mockDb,
    });

    const stats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:2`);
    assert.ok(stats, 'pure-VP user got an epoch stats row despite the gate');
    assert.ok(
      stats.vpPointsWithMultiplier > 0n,
      'pure-VP user accrued VP points for the closed epoch (gate did not drop them)'
    );
  } finally {
    if (prev === undefined) delete process.env.ENVIO_LEADERBOARD_LIVE_EPOCH;
    else process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = prev;
  }
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
    specialEditionCount: 0n,
    specialEditionMultiplier: 10000n,
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
