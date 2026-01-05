import assert from 'node:assert/strict';
import { test } from 'node:test';

// Disable bootstrap in tests to use default config values
process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

import {
  AUSD_ADDRESS,
  DUST_LOCK_START_BLOCK,
  EARNAUSD_ADDRESS,
  MAX_LOCK_TIME,
  SHMON_ADDRESS,
  USDC_ADDRESS,
  USDT0_ADDRESS,
  WBTC_ADDRESS,
  WETH_ADDRESS,
  WMON_ADDRESS,
} from '../helpers/constants';
import {
  addReserveToUserList,
  applyScheduledEpochTransitions,
  applyCombinedMultiplierScaled,
  applyMultipliersForUser,
  calculateAverageCombinedMultiplierBps,
  calculateAverageTokenVotingPower,
  calculateCurrentVPFromStorage,
  calculateNFTMultiplierFromCount,
  calculateVPMultiplier,
  createMultiplierSnapshot,
  computeTotalPointsWithMultiplier,
  ensureAssetPrice,
  findVPTierIndex,
  getAssetPriceUSD,
  recalculateUserTotalVP,
  recordProtocolTransaction,
  updateLifetimePoints,
  updateUserVotingPower,
  updateUserTokenList,
} from '../handlers/shared';
import type { handlerContext } from '../../generated';
import type {
  DustLockToken_t,
  LeaderboardEpoch_t,
  LeaderboardState_t,
  NFTMultiplierConfig_t,
  PriceOracleAsset_t,
  ProtocolStatsSnapshot_t,
  ProtocolStats_t,
  UserEpochStats_t,
  UserLeaderboardState_t,
  UserMultiplierSnapshot_t,
  UserPoints_t,
  UserReserveList_t,
  UserTokenList_t,
  UserVotingPowerHistory_t,
  User_t,
  VotingPowerTier_t,
} from '../../generated/src/db/Entities.gen';

type UserPointsMaybeEpochs = Omit<UserPoints_t, 'epochsParticipated' | 'lifetimeEpochsIncluded'> & {
  epochsParticipated?: bigint[];
  lifetimeEpochsIncluded?: bigint[];
};

type StoreEntity<T extends { readonly id: string }> = {
  get: (id: string) => Promise<T | undefined>;
  set: (entity: T) => void;
};

function createStore<T extends { readonly id: string }>(): StoreEntity<T> {
  const map = new Map<string, T>();
  return {
    get: async (id: string) => map.get(id),
    set: (entity: T & { id: string }) => {
      map.set(entity.id, entity);
    },
  };
}

type StoreEntityWithSize<T extends { readonly id: string }> = StoreEntity<T> & {
  size: () => number;
};

function createStoreWithSize<T extends { readonly id: string }>(): StoreEntityWithSize<T> {
  const map = new Map<string, T>();
  return {
    get: async (id: string) => map.get(id),
    set: (entity: T & { id: string }) => {
      map.set(entity.id, entity);
    },
    size: () => map.size,
  };
}

test('token list helpers handle duplicates and removals', async () => {
  const userStore = createStore<UserTokenList_t>();
  const context = { UserTokenList: userStore } as unknown as handlerContext;

  await updateUserTokenList(context, '0xuser', 1n, 1, 'add');
  await updateUserTokenList(context, '0xuser', 1n, 2, 'add');
  await updateUserTokenList(context, '0xuser', 1n, 3, 'remove');

  const list = await userStore.get('0xuser');
  assert.ok(list);
  assert.equal(list?.tokenIds.length, 0);
});

test('protocol stats snapshots overwrite within the same timestamp', async () => {
  const statsStore = createStore<ProtocolStats_t>();
  const snapshotStore = createStoreWithSize<ProtocolStatsSnapshot_t>();
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    ProtocolStats: statsStore,
    ProtocolStatsSnapshot: snapshotStore,
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  const timestamp = 1000;
  await recordProtocolTransaction(context, '0xtx1', timestamp, 10n);
  const first = await snapshotStore.get(`${timestamp}`);
  assert.ok(first);
  assert.equal(first?.txHash, '0xtx1');
  assert.equal(first?.totalTransactions, 1n);

  await recordProtocolTransaction(context, '0xtx2', timestamp, 10n);
  const second = await snapshotStore.get(`${timestamp}`);
  assert.ok(second);
  assert.equal(second?.txHash, '0xtx2');
  assert.equal(second?.totalTransactions, 2n);
  assert.equal(snapshotStore.size(), 1);
});

test('reserve list helpers update timestamps for existing entries', async () => {
  const reserveStore = createStore<UserReserveList_t>();
  const context = { UserReserveList: reserveStore } as unknown as handlerContext;

  await addReserveToUserList(context, '0xuser', 'reserve-1', 1);
  await addReserveToUserList(context, '0xuser', 'reserve-1', 2);

  const list = await reserveStore.get('0xuser');
  assert.ok(list);
  assert.equal(list?.reserveIds.length, 1);
  assert.equal(list?.lastUpdate, 2);
});

test('asset price helpers set defaults and fallback to priceInEth', async () => {
  const priceStore = createStore<PriceOracleAsset_t>();
  const context = { PriceOracleAsset: priceStore } as unknown as handlerContext;

  await ensureAssetPrice(context, USDC_ADDRESS, 1);
  await ensureAssetPrice(context, USDT0_ADDRESS, 2);
  await ensureAssetPrice(context, AUSD_ADDRESS, 3);
  await ensureAssetPrice(context, EARNAUSD_ADDRESS, 4);
  await ensureAssetPrice(context, WETH_ADDRESS, 5);
  await ensureAssetPrice(context, WBTC_ADDRESS, 6);
  await ensureAssetPrice(context, SHMON_ADDRESS, 7);
  await ensureAssetPrice(context, WMON_ADDRESS, 8);

  priceStore.set({
    id: '0xprice',
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 200000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: 0,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 0,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });

  const price = await getAssetPriceUSD(context, '0xprice', 10);
  assert.equal(price, 2);
});

test('getAssetPriceUSD returns zero when price is still missing', async () => {
  const priceStore = {
    get: async () => undefined,
    set: () => {},
  };
  const context = { PriceOracleAsset: priceStore } as unknown as handlerContext;

  const price = await getAssetPriceUSD(context, '0xmissing');
  assert.equal(price, 0);
});

test('ensureAssetPrice preserves existing metadata fields', async () => {
  const priceStore = createStore<PriceOracleAsset_t>();
  const context = { PriceOracleAsset: priceStore } as unknown as handlerContext;

  priceStore.set({
    id: '0xasset',
    oracle_id: 'oracle',
    priceSource: 'source',
    dependentAssets: ['0xdep'],
    priceType: 'type',
    platform: 'platform',
    priceInEth: 0n,
    isFallbackRequired: true,
    lastUpdateTimestamp: 0,
    priceCacheExpiry: 123,
    fromChainlinkSourcesRegistry: true,
    lastPriceUsd: 0,
    cumulativeUsdPriceHours: 7,
    resetTimestamp: 8,
    resetCumulativeUsdPriceHours: 9,
  });

  await ensureAssetPrice(context, '0xasset', 10);

  const updated = await priceStore.get('0xasset');
  assert.equal(updated?.oracle_id, 'oracle');
  assert.equal(updated?.priceSource, 'source');
  assert.deepEqual(updated?.dependentAssets, ['0xdep']);
  assert.equal(updated?.priceType, 'type');
  assert.equal(updated?.platform, 'platform');
  assert.equal(updated?.priceCacheExpiry, 123);
  assert.equal(updated?.fromChainlinkSourcesRegistry, true);
  assert.equal(updated?.cumulativeUsdPriceHours, 7);
  assert.equal(updated?.resetTimestamp, 8);
  assert.equal(updated?.resetCumulativeUsdPriceHours, 9);
});

test('calculateCurrentVPFromStorage skips missing tokens', async () => {
  const user = '0xuser';
  const userTokenList = createStore<UserTokenList_t>();
  const dustLockToken = createStore<DustLockToken_t>();

  userTokenList.set({
    id: user,
    user_id: user,
    tokenIds: [1n],
    lastUpdate: 0,
  });

  const context = {
    UserTokenList: userTokenList,
    DustLockToken: dustLockToken,
  } as unknown as handlerContext;

  const vp = await calculateCurrentVPFromStorage(context, user, 1000);
  assert.equal(vp, 0n);
});

test('calculateCurrentVPFromStorage uses stored token balances', async () => {
  const user = '0xuser';
  const userTokenList = createStore<UserTokenList_t>();
  const dustLockToken = createStore<DustLockToken_t>();

  userTokenList.set({
    id: user,
    user_id: user,
    tokenIds: [1n, 2n],
    lastUpdate: 0,
  });

  dustLockToken.set({
    id: '1',
    owner: user,
    lockedAmount: 100n,
    end: 1000 + Number(MAX_LOCK_TIME),
    isPermanent: false,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  dustLockToken.set({
    id: '2',
    owner: user,
    lockedAmount: 50n,
    end: 0,
    isPermanent: true,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });

  const context = {
    UserTokenList: userTokenList,
    DustLockToken: dustLockToken,
  } as unknown as handlerContext;

  const vp = await calculateCurrentVPFromStorage(context, user, 1000);
  assert.equal(vp, 150n);
});

test('NFT multiplier returns base when config missing', async () => {
  const configStore = createStore<NFTMultiplierConfig_t>();
  const context = { NFTMultiplierConfig: configStore } as unknown as handlerContext;

  const multiplier = await calculateNFTMultiplierFromCount(context, 2n);
  // Bootstrap config fallback: 10000 + 1000 + 900 = 11900
  assert.equal(multiplier, 11900n);
});

test('recalculateUserTotalVP returns early before dust lock start block', async () => {
  const stateStore = createStore<UserLeaderboardState_t>();
  const context = { UserLeaderboardState: stateStore } as unknown as handlerContext;

  await recalculateUserTotalVP(
    context,
    '0xuser',
    1000,
    '0xhash',
    'test',
    0,
    BigInt(DUST_LOCK_START_BLOCK - 1)
  );

  assert.equal(await stateStore.get('0xuser'), undefined);
});

test('recalculateUserTotalVP caps combined multiplier', async () => {
  const user = '0xuser';
  const tokenId = 1n;
  const userTokenList = createStore<UserTokenList_t>();
  const dustLockToken = createStore<DustLockToken_t>();
  const leaderboardState = createStore<UserLeaderboardState_t>();
  const vpTiers = createStore<VotingPowerTier_t>();
  const multiplierSnapshots = createStore<UserMultiplierSnapshot_t>();
  const vpHistory = createStore<UserVotingPowerHistory_t>();

  userTokenList.set({
    id: user,
    user_id: user,
    tokenIds: [tokenId],
    lastUpdate: 0,
  });
  dustLockToken.set({
    id: tokenId.toString(),
    owner: user,
    lockedAmount: 1000n,
    end: 0,
    isPermanent: true,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  leaderboardState.set({
    id: user,
    user_id: user,
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
  vpTiers.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 50000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });

  const context = {
    UserTokenList: userTokenList,
    DustLockToken: dustLockToken,
    UserLeaderboardState: leaderboardState,
    VotingPowerTier: vpTiers,
    UserMultiplierSnapshot: multiplierSnapshots,
    UserVotingPowerHistory: vpHistory,
  } as unknown as handlerContext;

  await recalculateUserTotalVP(
    context,
    user,
    1000,
    '0xhash',
    'test',
    0,
    BigInt(DUST_LOCK_START_BLOCK + 1)
  );

  const updated = await leaderboardState.get(user);
  assert.equal(updated?.combinedMultiplier, 100000n);
});

test('recalculateUserTotalVP skips missing tokens', async () => {
  const user = '0xuser';
  const userTokenList = createStore<UserTokenList_t>();
  const dustLockToken = createStore<DustLockToken_t>();
  const leaderboardState = createStore<UserLeaderboardState_t>();
  const vpTiers = createStore<VotingPowerTier_t>();
  const multiplierSnapshots = createStore<UserMultiplierSnapshot_t>();
  const vpHistory = createStore<UserVotingPowerHistory_t>();

  userTokenList.set({
    id: user,
    user_id: user,
    tokenIds: [1n],
    lastUpdate: 0,
  });

  const context = {
    UserTokenList: userTokenList,
    DustLockToken: dustLockToken,
    UserLeaderboardState: leaderboardState,
    VotingPowerTier: vpTiers,
    UserMultiplierSnapshot: multiplierSnapshots,
    UserVotingPowerHistory: vpHistory,
  } as unknown as handlerContext;

  await recalculateUserTotalVP(
    context,
    user,
    1000,
    '0xhash',
    'test',
    0,
    BigInt(DUST_LOCK_START_BLOCK + 1)
  );

  const updated = await leaderboardState.get(user);
  assert.equal(updated?.votingPower, 0n);
});

test('updateUserVotingPower caps combined multiplier and snapshots', async () => {
  const user = '0xuser';
  const tokenId = 1n;
  const leaderboardState = createStore<UserLeaderboardState_t>();
  const vpTiers = createStore<VotingPowerTier_t>();
  const snapshots = createStore<UserMultiplierSnapshot_t>();
  const vpHistory = createStore<UserVotingPowerHistory_t>();

  leaderboardState.set({
    id: user,
    user_id: user,
    nftCount: 0n,
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
  vpTiers.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 50000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });

  const context = {
    UserLeaderboardState: leaderboardState,
    VotingPowerTier: vpTiers,
    UserMultiplierSnapshot: snapshots,
    UserVotingPowerHistory: vpHistory,
  } as unknown as handlerContext;

  await updateUserVotingPower(context, user, tokenId, 1000n, 2000, '0xtx', 'test', 0);

  const updated = await leaderboardState.get(user);
  assert.equal(updated?.combinedMultiplier, 100000n);
  assert.ok(await snapshots.get(`${user}:2000:0xtx:0`));
});

test('updateLifetimePoints keeps existing epoch list', async () => {
  const user = '0xuser';
  const epochNumber = 1n;
  const userPoints = createStore<UserPointsMaybeEpochs>();
  const userEpochStats = createStore<UserEpochStats_t>();
  const users = createStore<User_t>();
  const leaderboardState = createStore<UserLeaderboardState_t>();
  const globalLeaderboardState = createStore<{ id: string; currentEpochNumber: bigint }>();
  const userIndex = createStore<{
    id: string;
    user: string;
    epochNumber: bigint;
    bucketIndex: number;
    points: number;
    updatedAt: number;
  }>();

  // Leaderboard state not active so updateAllTimeLeaderboard returns early
  globalLeaderboardState.set({ id: 'current', currentEpochNumber: 0n });

  userPoints.set({
    id: user,
    user_id: user,
    lifetimeDepositPoints: 0n,
    lifetimeBorrowPoints: 0n,
    lifetimeDailySupplyPoints: 0n,
    lifetimeDailyBorrowPoints: 0n,
    lifetimeDailyRepayPoints: 0n,
    lifetimeDailyWithdrawPoints: 0n,
    lifetimeDailyVPPoints: 0n,
    lifetimeTotalPoints: 0n,
    epochsParticipated: [epochNumber],
    lifetimeEpochsIncluded: [epochNumber],
    lastUpdatedAt: 0,
  });
  userEpochStats.set({
    id: `${user}:${epochNumber}`,
    user_id: user,
    epochNumber,
    depositPoints: 1n,
    borrowPoints: 2n,
    lpPoints: 0n,
    dailySupplyPoints: 3n,
    dailyBorrowPoints: 4n,
    dailyRepayPoints: 5n,
    dailyWithdrawPoints: 6n,
    dailyVPPoints: 7n,
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
    totalPoints: 28n,
    lastSupplyPointsDay: 0,
    lastBorrowPointsDay: 0,
    lastRepayPointsDay: 0,
    lastWithdrawPointsDay: 0,
    lastVPPointsDay: 0,
    lastVPAccrualTimestamp: 0,
    totalPointsWithMultiplier: 0n,
    totalMultiplierBps: 10000n,
    lastAppliedMultiplierBps: 10000n,
    testnetBonusBps: 0n,
    rank: 0,
    firstSeenAt: 0,
    lastUpdatedAt: 0,
  });

  const context = {
    UserPoints: userPoints,
    UserEpochStats: userEpochStats,
    User: users,
    UserLeaderboardState: leaderboardState,
    LeaderboardState: globalLeaderboardState,
    UserIndex: userIndex,
  } as unknown as handlerContext;

  await updateLifetimePoints(context, user, {
    epochNumber,
    lastUpdatedAt: 50,
  });

  const updated = await userPoints.get(user);
  assert.deepEqual(updated?.epochsParticipated, [epochNumber]);
  assert.equal(updated?.lifetimeDepositPoints, 1n);
  assert.equal(updated?.lifetimeBorrowPoints, 2n);
  assert.equal(updated?.lifetimeDailySupplyPoints, 3n);
  assert.equal(updated?.lifetimeDailyBorrowPoints, 4n);
  assert.equal(updated?.lifetimeDailyRepayPoints, 5n);
  assert.equal(updated?.lifetimeDailyWithdrawPoints, 6n);
  assert.equal(updated?.lifetimeDailyVPPoints, 7n);
  assert.equal(updated?.lifetimeTotalPoints, 28n);
});

test('updateLifetimePoints initializes epoch list when missing', async () => {
  const user = '0xuser';
  const epochNumber = 2n;
  const userPoints = createStore<UserPointsMaybeEpochs>();
  const userEpochStats = createStore<UserEpochStats_t>();
  const users = createStore<User_t>();
  const leaderboardState = createStore<UserLeaderboardState_t>();
  const globalLeaderboardState = createStore<{ id: string; currentEpochNumber: bigint }>();
  const userIndex = createStore<{
    id: string;
    user: string;
    epochNumber: bigint;
    bucketIndex: number;
    points: number;
    updatedAt: number;
  }>();

  // Leaderboard state not active so updateAllTimeLeaderboard returns early
  globalLeaderboardState.set({ id: 'current', currentEpochNumber: 0n });

  userPoints.set({
    id: user,
    user_id: user,
    lifetimeDepositPoints: 0n,
    lifetimeBorrowPoints: 0n,
    lifetimeDailySupplyPoints: 0n,
    lifetimeDailyBorrowPoints: 0n,
    lifetimeDailyRepayPoints: 0n,
    lifetimeDailyWithdrawPoints: 0n,
    lifetimeDailyVPPoints: 0n,
    lifetimeTotalPoints: 0n,
    lastUpdatedAt: 0,
  });
  userEpochStats.set({
    id: `${user}:${epochNumber}`,
    user_id: user,
    epochNumber,
    depositPoints: 2n,
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
    totalPoints: 2n,
    lastSupplyPointsDay: 0,
    lastBorrowPointsDay: 0,
    lastRepayPointsDay: 0,
    lastWithdrawPointsDay: 0,
    lastVPPointsDay: 0,
    lastVPAccrualTimestamp: 0,
    totalPointsWithMultiplier: 0n,
    totalMultiplierBps: 10000n,
    lastAppliedMultiplierBps: 10000n,
    testnetBonusBps: 0n,
    rank: 0,
    firstSeenAt: 0,
    lastUpdatedAt: 0,
  });

  const context = {
    UserPoints: userPoints,
    UserEpochStats: userEpochStats,
    User: users,
    UserLeaderboardState: leaderboardState,
    LeaderboardState: globalLeaderboardState,
    UserIndex: userIndex,
  } as unknown as handlerContext;

  await updateLifetimePoints(context, user, {
    epochNumber,
    lastUpdatedAt: 60,
  });

  const updated = await userPoints.get(user);
  assert.deepEqual(updated?.epochsParticipated, [epochNumber]);
});

test('multiplier helpers cap and apply correctly', async () => {
  const userId = '0x000000000000000000000000000000000000babe';
  const userStateStore = createStore<UserLeaderboardState_t>();
  const userTokenListStore = createStore<UserTokenList_t>();
  const dustLockStore = createStore<DustLockToken_t>();
  const vpTierStore = createStore<VotingPowerTier_t>();

  userTokenListStore.set({
    id: userId,
    user_id: userId,
    tokenIds: [],
    lastUpdate: 0,
  });

  const context = {
    UserLeaderboardState: userStateStore,
    UserTokenList: userTokenListStore,
    DustLockToken: dustLockStore,
    VotingPowerTier: vpTierStore,
  } as unknown as handlerContext;

  const basePoints = await applyMultipliersForUser(context, userId, 10, 0);
  assert.equal(basePoints, 10);

  const cappedPoints = await applyMultipliersForUser(context, userId, 10, 0, {
    combinedMultiplierBps: 200000n,
  });
  assert.equal(cappedPoints, 100);

  const scaled = applyCombinedMultiplierScaled(100n, 200000n);
  assert.equal(scaled, 1000n);
});

test('computeTotalPointsWithMultiplier applies testnet bonus', () => {
  const basePoints = 100n;
  const stats = {
    depositPointsWithMultiplier: basePoints,
    borrowPointsWithMultiplier: 0n,
    vpPointsWithMultiplier: 0n,
    lpPointsWithMultiplier: 0n,
    dailySupplyPoints: 0n,
    dailyBorrowPoints: 0n,
    dailyRepayPoints: 0n,
    dailyWithdrawPoints: 0n,
    dailyLPPoints: 0n,
    manualAwardPoints: 0n,
  };
  const userId = '0x0054b4ac1ece531676b7e6df7b261132d600f6e5';
  const total = computeTotalPointsWithMultiplier(stats, userId, 1n);
  assert.equal(total, 120n);
});

test('applyScheduledEpochTransitions covers end/start block fallbacks', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  leaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 150,
  });
  leaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });

  await applyScheduledEpochTransitions(context, 200, 999n);
  const epoch1 = await leaderboardEpoch.get('1');
  assert.equal(epoch1?.endBlock, 999n);
});

test('applyScheduledEpochTransitions starts next epoch when scheduled', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  leaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  leaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 150,
    scheduledEndTime: 0,
  });

  await applyScheduledEpochTransitions(context, 200, 888n);
  const epoch2 = await leaderboardEpoch.get('2');
  assert.equal(epoch2?.startBlock, 888n);
});

test('applyScheduledEpochTransitions starts first epoch from inactive state', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 0n,
    isActive: false,
  });
  leaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 100,
    scheduledEndTime: 0,
  });

  await applyScheduledEpochTransitions(context, 200, 777n);
  const epoch1 = await leaderboardEpoch.get('1');
  assert.equal(epoch1?.startBlock, 777n);
});

test('applyScheduledEpochTransitions ends epoch with defined endTime and missing block number', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  leaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 200,
    endBlock: undefined,
    endTime: 180,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 150,
  });

  await applyScheduledEpochTransitions(context, 200);
  const epoch1 = await leaderboardEpoch.get('1');
  assert.equal(epoch1?.endTime, 150);
  assert.equal(epoch1?.duration, undefined);
  assert.equal(epoch1?.endBlock, undefined);
});

test('applyScheduledEpochTransitions starts next epoch using existing endBlock', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  leaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 100,
    endBlock: 444n,
    endTime: 180,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  leaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: 0n,
    startTime: 200,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 150,
    scheduledEndTime: 0,
  });

  await applyScheduledEpochTransitions(context, 200);
  const epoch1 = await leaderboardEpoch.get('1');
  const epoch2 = await leaderboardEpoch.get('2');
  assert.equal(epoch1?.endBlock, 444n);
  assert.equal(epoch2?.startTime, 150);
  assert.equal(epoch2?.startBlock, 0n);
});

test('applyScheduledEpochTransitions starts next epoch without endBlock when block number missing', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  leaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  leaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 150,
    scheduledEndTime: 0,
  });

  await applyScheduledEpochTransitions(context, 200);
  const epoch1 = await leaderboardEpoch.get('1');
  assert.equal(epoch1?.endBlock, undefined);
});

test('applyScheduledEpochTransitions skips when scheduled start is missing', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: false,
  });
  leaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: undefined as unknown as number,
    scheduledEndTime: 0,
  });

  await applyScheduledEpochTransitions(context, 200);
  const epoch2 = await leaderboardEpoch.get('2');
  assert.equal(epoch2?.isActive, false);
});

test('applyScheduledEpochTransitions uses startTime fallback without block number', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 0n,
    isActive: false,
  });
  leaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 300,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 150,
    scheduledEndTime: 0,
  });

  await applyScheduledEpochTransitions(context, 200);
  const epoch1 = await leaderboardEpoch.get('1');
  assert.equal(epoch1?.startTime, 150);
  assert.equal(epoch1?.startBlock, 0n);
});

test('applyScheduledEpochTransitions skips when scheduled end is missing', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  leaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: undefined as unknown as number,
  });

  await applyScheduledEpochTransitions(context, 200);
  const epoch1 = await leaderboardEpoch.get('1');
  assert.equal(epoch1?.isActive, true);
});

test('applyScheduledEpochTransitions uses fallback when next epoch startBlock is undefined', async () => {
  const leaderboardState = createStore<LeaderboardState_t>();
  const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
  const context = {
    LeaderboardState: leaderboardState,
    LeaderboardEpoch: leaderboardEpoch,
  } as unknown as handlerContext;

  leaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  leaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 100,
    endBlock: 0n,
    endTime: 200,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  leaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: undefined as unknown as bigint,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 150,
    scheduledEndTime: 0,
  });

  await applyScheduledEpochTransitions(context, 200);
  const epoch2 = await leaderboardEpoch.get('2');
  assert.equal(epoch2?.startBlock, 0n);
});
test('average token voting power covers edge cases', async () => {
  const token = { lockedAmount: 100n, end: 200, isPermanent: false };
  const zeroWindow = await calculateAverageTokenVotingPower(token, 200, 100);
  assert.ok(zeroWindow >= 0n);

  assert.equal(
    await calculateAverageTokenVotingPower(
      { lockedAmount: 0n, end: 200, isPermanent: false },
      0,
      100
    ),
    0n
  );
  assert.equal(
    await calculateAverageTokenVotingPower(
      { lockedAmount: 100n, end: 200, isPermanent: true },
      0,
      100
    ),
    100n
  );
  assert.equal(
    await calculateAverageTokenVotingPower(
      { lockedAmount: 100n, end: 50, isPermanent: false },
      100,
      200
    ),
    0n
  );

  const avgWithin = await calculateAverageTokenVotingPower(
    { lockedAmount: 100n, end: 300, isPermanent: false },
    100,
    200
  );
  const avgPartial = await calculateAverageTokenVotingPower(
    { lockedAmount: 100n, end: 150, isPermanent: false },
    100,
    300
  );
  assert.ok(avgWithin >= 0n);
  assert.ok(avgPartial >= 0n);
});

test('average combined multiplier caps at maximum', async () => {
  const userState = createStore<UserLeaderboardState_t>();
  const userTokenList = createStore<UserTokenList_t>();
  const dustLockToken = createStore<DustLockToken_t>();
  const votingPowerTier = createStore<VotingPowerTier_t>();
  const nftMultiplierConfig = createStore<NFTMultiplierConfig_t>();
  const context = {
    UserLeaderboardState: userState,
    UserTokenList: userTokenList,
    DustLockToken: dustLockToken,
    VotingPowerTier: votingPowerTier,
    NFTMultiplierConfig: nftMultiplierConfig,
  } as unknown as handlerContext;

  // Set NFT config with high firstBonus to create a 5x NFT multiplier (50000)
  nftMultiplierConfig.set({
    id: 'current',
    firstBonus: 40000n, // 4x bonus per NFT
    decayRatio: 10000n, // no decay
    lastUpdate: 0,
  });

  userState.set({
    id: '0xuser',
    user_id: '0xuser',
    nftCount: 1n,
    nftMultiplier: 50000n, // Will be recalculated to 50000 (10000 + 40000)
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
  userTokenList.set({
    id: '0xuser',
    user_id: '0xuser',
    tokenIds: [1n],
    lastUpdate: 0,
  });
  dustLockToken.set({
    id: '1',
    owner: '0xuser',
    lockedAmount: 100n,
    end: 1000,
    isPermanent: false,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  votingPowerTier.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 50000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });

  const combined = await calculateAverageCombinedMultiplierBps(context, '0xuser', 0, 500);
  assert.equal(combined, 100000n);
});

test('average combined multiplier uses current vp when end before start', async () => {
  const userState = createStore<UserLeaderboardState_t>();
  const userTokenList = createStore<UserTokenList_t>();
  const dustLockToken = createStore<DustLockToken_t>();
  const votingPowerTier = createStore<VotingPowerTier_t>();
  const context = {
    UserLeaderboardState: userState,
    UserTokenList: userTokenList,
    DustLockToken: dustLockToken,
    VotingPowerTier: votingPowerTier,
  } as unknown as handlerContext;

  userState.set({
    id: '0xuser',
    user_id: '0xuser',
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
  userTokenList.set({
    id: '0xuser',
    user_id: '0xuser',
    tokenIds: [1n],
    lastUpdate: 0,
  });
  dustLockToken.set({
    id: '1',
    owner: '0xuser',
    lockedAmount: 100n,
    end: 1000,
    isPermanent: false,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  votingPowerTier.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 10000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });

  const combined = await calculateAverageCombinedMultiplierBps(context, '0xuser', 500, 500);
  assert.equal(combined, 10000n);
});

test('average combined multiplier skips missing dust lock tokens', async () => {
  const userState = createStore<UserLeaderboardState_t>();
  const userTokenList = createStore<UserTokenList_t>();
  const dustLockToken = createStore<DustLockToken_t>();
  const votingPowerTier = createStore<VotingPowerTier_t>();
  const context = {
    UserLeaderboardState: userState,
    UserTokenList: userTokenList,
    DustLockToken: dustLockToken,
    VotingPowerTier: votingPowerTier,
  } as unknown as handlerContext;

  userState.set({
    id: '0xuser',
    user_id: '0xuser',
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
  userTokenList.set({
    id: '0xuser',
    user_id: '0xuser',
    tokenIds: [1n],
    lastUpdate: 0,
  });
  votingPowerTier.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 10000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });

  const combined = await calculateAverageCombinedMultiplierBps(context, '0xuser', 0, 100);
  assert.equal(combined, 10000n);
});

test('createMultiplierSnapshot defaults log index to zero', async () => {
  const userMultiplierSnapshot = createStore<UserMultiplierSnapshot_t>();
  const context = { UserMultiplierSnapshot: userMultiplierSnapshot } as unknown as handlerContext;

  createMultiplierSnapshot(
    context,
    {
      id: '0xuser',
      nftCount: 0n,
      nftMultiplier: 10000n,
      votingPower: 0n,
      vpMultiplier: 10000n,
      combinedMultiplier: 10000n,
    },
    1234,
    '0xtx',
    'snapshot'
  );

  const snapshot = await userMultiplierSnapshot.get('0xuser:1234:0xtx:0');
  assert.ok(snapshot);
});

test('vp tier helpers break on lower tiers', async () => {
  const votingPowerTier = createStore<VotingPowerTier_t>();
  const context = { VotingPowerTier: votingPowerTier } as unknown as handlerContext;

  votingPowerTier.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 15000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });
  votingPowerTier.set({
    id: '1',
    tierIndex: 1n,
    minVotingPower: 1000n,
    multiplierBps: 20000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });

  const multiplier = await calculateVPMultiplier(context, 500n);
  assert.equal(multiplier, 15000n);
  const tierIndex = await findVPTierIndex(context, 500n);
  assert.equal(tierIndex, 0n);
});
