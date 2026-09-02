import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  TestHelpers,
  getRegisteredEventHandler,
  processEvents,
  type EntityRow,
  type MockDb,
  entityStores,
} from './v3-test-helpers';
import {
  AUSD_ADDRESS,
  BALANCER_AUTORANGE_V3_POOL_ADDRESS,
  EPOCH_DATES_OVERRIDES,
  LEADERBOARD_START_BLOCK,
  LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  LP_V2_CUTOVER_TIMESTAMP,
  LP_V2_RESUME_CUTOVER_BLOCK,
  LP_V2_RESUME_CUTOVER_TIMESTAMP,
  USDC_ADDRESS,
} from '../helpers/constants';
import { createDefaultReserve } from '../helpers/entityHelpers';
import { LP_GROWTH_Q128 } from '../helpers/lpGrowthMath';
import * as leaderboardKeeperModule from '../handlers/leaderboardKeeper';
import { resolveKeeperEventTimestamp } from '../handlers/leaderboardKeeper';
import type {
  LeaderboardEpoch,
  LeaderboardKeeperUserSettled,
  UserEpochFinalization,
  handlerContext,
} from '../../generated';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';

const ADDRESSES = {
  keeper: '0x000000000000000000000000000000000000b001',
  owner: '0x000000000000000000000000000000000000b002',
  user: '0x000000000000000000000000000000000000b003',
  collection: '0x000000000000000000000000000000000000b004',
};

const RAY = 10n ** 27n;
const KEEPER_PHASE = {
  reserveUser: '0x000000000000000000000000000000000000b020',
  pureVpUser: '0x000000000000000000000000000000000000b021',
  unifiedUser: '0x000000000000000000000000000000000000b022',
  lendingPool: '0x000000000000000000000000000000000000b023',
  asset: '0x000000000000000000000000000000000000b024',
  lpPool: '0x000000000000000000000000000000000000b025',
  lpToken0: '0x000000000000000000000000000000000000b026',
  lpToken1: '0x000000000000000000000000000000000000b027',
};
const ONE_DAY = 86_400;
const ONE_POINT_GROWTH_X128 = LP_GROWTH_Q128 * 100_000_000n * 10_000n * BigInt(ONE_DAY);
const KEEPER_TEST_BLOCK = LEADERBOARD_START_BLOCK + 500_000;
const KEEPER_LEGACY_V3_POOL = '0xd15965968fe8bf2babbe39b2fc5de1ab6749141f';
const KEEPER_LEGACY_V3_MANAGER = '0x7197e214c0b767cfb76fb734ab638e2c192f4e53';
const KEEPER_BALANCER_POOL = BALANCER_AUTORANGE_V3_POOL_ADDRESS;
const KEEPER_DUST_TOKEN = '0xad96c3dffcd6374294e2573a7fbba96097cc8d7c';

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

function seedKeeperPhase(
  mockDb: MockDb,
  epochNumber: bigint,
  isActive: boolean,
  startTime: number,
  endTime?: number
): MockDb {
  let next = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: epochNumber,
    isActive,
  });
  next = next.entities.LeaderboardEpoch.set({
    id: epochNumber.toString(),
    epochNumber,
    startBlock: BigInt(LEADERBOARD_START_BLOCK),
    startTime,
    endBlock: isActive ? undefined : BigInt(LEADERBOARD_START_BLOCK + 1),
    endTime: isActive ? undefined : endTime,
    isActive,
    duration: isActive || endTime === undefined ? undefined : BigInt(endTime - startTime),
    scheduledStartTime: startTime,
    scheduledEndTime: isActive ? 0 : endTime,
  });
  return next;
}

function seedKeeperPointsConfig(mockDb: MockDb): MockDb {
  return mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 10_000n,
    borrowRateBps: 0n,
    vpRateBps: 10_000n,
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

function seedPermanentVpUser(
  mockDb: MockDb,
  userId: string,
  tokenId: bigint,
  timestamp: number
): MockDb {
  let next = mockDb.entities.DustLockToken.set({
    id: tokenId.toString(),
    owner: userId,
    lockedAmount: 10n ** 18n,
    end: 0,
    isPermanent: true,
    createdAt: timestamp,
    updatedAt: timestamp,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  next = next.entities.UserTokenList.set({
    id: userId,
    user_id: userId,
    tokenIds: [tokenId],
    lastUpdate: timestamp,
  });
  return next;
}

function seedUnifiedKeeperFixture(
  mockDb: MockDb,
  params: {
    epochNumber: bigint;
    isActive: boolean;
    startTime: number;
    settlementEnd: number;
  }
): { mockDb: MockDb; reserveId: string; positionId: string } {
  const { epochNumber, isActive, startTime, settlementEnd } = params;
  const userId = KEEPER_PHASE.unifiedUser;
  const reserveId = `${KEEPER_PHASE.asset}-${KEEPER_PHASE.lendingPool}`;
  const positionId = `v2:${KEEPER_PHASE.lpPool}:${userId}`;
  let next = seedKeeperPhase(
    mockDb,
    epochNumber,
    isActive,
    startTime,
    isActive ? undefined : settlementEnd
  );
  next = seedKeeperPointsConfig(next);
  next = seedPermanentVpUser(next, userId, 8_008n, startTime);

  next = next.entities.Reserve.set({
    ...createDefaultReserve(reserveId, KEEPER_PHASE.lendingPool, KEEPER_PHASE.asset),
    decimals: 6,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: startTime,
    isActive: true,
    borrowingEnabled: true,
  });
  next = next.entities.UserReserve.set({
    id: `${userId}-${reserveId}`,
    pool_id: KEEPER_PHASE.lendingPool,
    user_id: userId,
    reserve_id: reserveId,
    scaledATokenBalance: 1_000_000n,
    currentATokenBalance: 1_000_000n,
    scaledDebt: 0n,
    currentDebt: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: startTime,
  });
  next = next.entities.UserReserveList.set({
    id: userId,
    user_id: userId,
    reserveIds: [reserveId],
    lastUpdate: startTime,
  });
  next = next.entities.PriceOracleAsset.set({
    id: KEEPER_PHASE.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100_000_000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: isActive ? startTime : settlementEnd,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: isActive ? 0 : 24,
    resetTimestamp: startTime,
    resetCumulativeUsdPriceHours: 0,
  });

  next = next.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [KEEPER_PHASE.lpPool],
    lastUpdate: startTime,
  });
  next = next.entities.LPPoolConfig.set({
    id: KEEPER_PHASE.lpPool,
    pool: KEEPER_PHASE.lpPool,
    positionManager: KEEPER_PHASE.lpPool,
    token0: KEEPER_PHASE.lpToken0,
    token1: KEEPER_PHASE.lpToken1,
    fee: 3000,
    lpRateBps: 0n,
    isActive: false,
    enabledAtEpoch: epochNumber,
    enabledAtTimestamp: startTime,
    disabledAtEpoch: isActive ? undefined : epochNumber,
    disabledAtTimestamp: isActive ? undefined : settlementEnd,
    lastUpdate: settlementEnd,
  });
  next = next.entities.LPPoolState.set({
    id: KEEPER_PHASE.lpPool,
    pool: KEEPER_PHASE.lpPool,
    currentTick: 0,
    sqrtPriceX96: 1n << 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: settlementEnd,
  });
  next = next.entities.LPPoolV2State.set({
    id: KEEPER_PHASE.lpPool,
    pool: KEEPER_PHASE.lpPool,
    reserve0: 1n,
    reserve1: 1n,
    lpTotalSupply: 1n,
    lastUpdate: settlementEnd,
  });
  next = next.entities.LPPoolEpochGrowth.set({
    id: `${KEEPER_PHASE.lpPool}:${epochNumber}`,
    pool: KEEPER_PHASE.lpPool,
    epochNumber,
    startTimestamp: startTime,
    lastTimestamp: settlementEnd,
    scalarGrowthX128: ONE_POINT_GROWTH_X128,
    isFrozen: !isActive,
    frozenAt: isActive ? undefined : settlementEnd,
    lastUpdate: settlementEnd,
  });
  next = next.entities.TokenInfo.set({
    id: KEEPER_PHASE.lpToken0,
    address: KEEPER_PHASE.lpToken0,
    decimals: 0,
    symbol: 'LP0',
    name: 'LP token 0',
    lastUpdate: startTime,
  });
  next = next.entities.TokenInfo.set({
    id: KEEPER_PHASE.lpToken1,
    address: KEEPER_PHASE.lpToken1,
    decimals: 0,
    symbol: 'LP1',
    name: 'LP token 1',
    lastUpdate: startTime,
  });
  next = next.entities.UserLPPosition.set({
    id: positionId,
    tokenId: 0n,
    user_id: userId,
    pool: KEEPER_PHASE.lpPool,
    positionManager: KEEPER_PHASE.lpPool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1n,
    amount0: 1n,
    amount1: 1n,
    isInRange: true,
    valueUsd: 200_000_000n,
    lastInRangeTimestamp: startTime,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: startTime,
    settledLpPoints: 0n,
    createdAt: startTime - 1,
    lastUpdate: startTime,
  });
  next = next.entities.UserLPPositionIndex.set({
    id: userId,
    user_id: userId,
    positionIds: [positionId],
    lastUpdate: startTime,
  });

  return { mockDb: next, reserveId, positionId };
}

function keeperRawPhase(mockDb: MockDb, id: string) {
  return mockDb.entities.LeaderboardKeeperUserSettled.get(id) as
    | {
        id: string;
        user_id: string;
        epochNumber?: bigint;
        isGap?: boolean;
        timestamp: number;
        txHash: string;
      }
    | undefined;
}

function createInstrumentedKeeperContext(mockDb: MockDb, isPreload: boolean) {
  const stores = new Map(
    Array.from(entityStores(mockDb), ([entityName, rows]) => [entityName, new Map(rows)])
  );
  const reads = new Set<string>();
  const storeCache = new Map<string, object>();
  const storeFor = (entityName: string) => ({
    async get(id: string) {
      reads.add(`${entityName}:${id}`);
      return stores.get(entityName)?.get(id);
    },
    async getAll() {
      reads.add(`${entityName}:*`);
      return Array.from(stores.get(entityName)?.values() ?? []);
    },
    async getWhere() {
      reads.add(`${entityName}:*`);
      return Array.from(stores.get(entityName)?.values() ?? []);
    },
    async getOrCreate(row: { id: string }) {
      reads.add(`${entityName}:${row.id}`);
      return stores.get(entityName)?.get(row.id) ?? row;
    },
    set(row: { id: string }) {
      if (isPreload) return;
      let rows = stores.get(entityName);
      if (!rows) {
        rows = new Map();
        stores.set(entityName, rows);
      }
      rows.set(row.id, row);
    },
    deleteUnsafe(id: string) {
      if (!isPreload) stores.get(entityName)?.delete(id);
    },
  });
  const context = new Proxy(
    {
      isPreload,
      log: { debug() {}, info() {}, warn() {}, error() {} },
    } as Record<string, unknown>,
    {
      get(target, property: string) {
        if (property in target) return target[property];
        let store = storeCache.get(property);
        if (!store) {
          store = storeFor(property);
          storeCache.set(property, store);
        }
        return store;
      },
    }
  ) as unknown as handlerContext;
  return { context, reads, stores };
}

function seedKeeperStaticChronologyFixture(
  mockDb: MockDb,
  tide7Start: number,
  tide7End: number
): MockDb {
  const tide6Start = LP_V2_CUTOVER_TIMESTAMP - 1_000;
  let next = seedKeeperPointsConfig(mockDb);
  next = next.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 6n,
    isActive: true,
  });
  next = next.entities.LeaderboardEpoch.set({
    id: '6',
    epochNumber: 6n,
    startBlock: BigInt(LEADERBOARD_START_BLOCK),
    startTime: tide6Start,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: tide6Start,
    scheduledEndTime: tide7Start,
  });
  next = next.entities.LeaderboardEpoch.set({
    id: '7',
    epochNumber: 7n,
    startBlock: 0n,
    startTime: tide7Start,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: tide7Start,
    scheduledEndTime: tide7End,
  });
  next = next.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [KEEPER_LEGACY_V3_POOL],
    lastUpdate: tide6Start,
  });
  next = next.entities.LPPoolConfig.set({
    id: KEEPER_LEGACY_V3_POOL,
    pool: KEEPER_LEGACY_V3_POOL,
    positionManager: KEEPER_LEGACY_V3_MANAGER,
    token0: AUSD_ADDRESS,
    token1: KEEPER_DUST_TOKEN,
    fee: 10_000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 6n,
    enabledAtTimestamp: tide6Start,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: tide6Start,
  });
  next = next.entities.LPPoolState.set({
    id: KEEPER_LEGACY_V3_POOL,
    pool: KEEPER_LEGACY_V3_POOL,
    currentTick: 0,
    sqrtPriceX96: 1n << 96n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: tide6Start,
  });
  return next;
}

const KEEPER_FINALIZATION_PROOF = {
  epochNumber: 8n,
  startTime: 100,
  epochEndTime: 900,
  finalizedAt: 1_000,
  blockNumber: BigInt(KEEPER_TEST_BLOCK + 340),
  txHash: `0x${'f1'.padStart(64, '0')}`,
  logIndex: 17,
};

function seedKeeperFinalizationProof(mockDb: MockDb): {
  mockDb: MockDb;
  certificate: UserEpochFinalization;
  epoch: LeaderboardEpoch;
  raw: LeaderboardKeeperUserSettled;
} {
  const proof = KEEPER_FINALIZATION_PROOF;
  const settlementEventId = `${proof.txHash}-${proof.logIndex}`;
  const epoch = {
    id: proof.epochNumber.toString(),
    epochNumber: proof.epochNumber,
    startBlock: BigInt(LEADERBOARD_START_BLOCK),
    startTime: proof.startTime,
    endBlock: proof.blockNumber,
    endTime: proof.epochEndTime,
    isActive: false,
    duration: BigInt(proof.epochEndTime - proof.startTime),
    scheduledStartTime: proof.startTime,
    scheduledEndTime: proof.epochEndTime,
  } satisfies LeaderboardEpoch;
  const raw = {
    id: settlementEventId,
    user_id: KEEPER_PHASE.unifiedUser,
    epochNumber: proof.epochNumber,
    isGap: true,
    timestamp: proof.finalizedAt,
    txHash: proof.txHash,
  } satisfies LeaderboardKeeperUserSettled;
  const certificate = {
    id: `${KEEPER_PHASE.unifiedUser}:${proof.epochNumber.toString()}`,
    user_id: KEEPER_PHASE.unifiedUser,
    epochNumber: proof.epochNumber,
    epochEndTime: proof.epochEndTime,
    settledThrough: proof.epochEndTime,
    finalizedAt: proof.finalizedAt,
    blockNumber: proof.blockNumber,
    txHash: proof.txHash,
    settlementEventId,
  } satisfies UserEpochFinalization;
  let next = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: proof.epochNumber,
    isActive: false,
  });
  next = next.entities.LeaderboardEpoch.set(epoch);
  next = next.entities.LeaderboardKeeperUserSettled.set(raw);
  next = next.entities.UserEpochFinalization.set(certificate);
  return { mockDb: next, certificate, epoch, raw };
}

// v2's `set` clones the store and returns a new MockDb rather than mutating in place, so
// this returns the updated db and callers reassign.
function overwriteKeeperFixtureRow(
  mockDb: MockDb,
  entityName: string,
  key: string,
  row: { id: string }
): MockDb {
  const ops = (
    mockDb as unknown as { entities: Record<string, { set: (r: unknown) => MockDb } | undefined> }
  ).entities[entityName];
  assert.ok(ops, `missing ${entityName} fixture store`);
  assert.equal(row.id, key, 'fixture row id must match the key it overwrites');
  return ops.set(row);
}

// Files `row` under `key` in an instrumented probe's stores even when `row.id !== key`. A
// keyed MockDb cannot represent that, but the handler's proof validation compares each row's
// `id` to the key it was fetched by, and those defensive branches are only reachable by
// serving a row from under a foreign key. Probe-only: `processEvent` paths use the real
// `overwriteKeeperFixtureRow` above.
function overrideProbeRow(
  stores: Map<string, Map<string, EntityRow>>,
  entityName: string,
  key: string,
  row: { id: string }
): void {
  let rows = stores.get(entityName);
  if (!rows) {
    rows = new Map();
    stores.set(entityName, rows);
  }
  rows.set(key, row as EntityRow);
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
    multiplierBps: 50000n, // clamped to MAX_VP_MULTIPLIER (5x)
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
  // Additive join of the two capped categories: nft 5x + vp 5x => 9x (90000).
  assert.equal(state?.combinedMultiplier, 90000n);

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
  // additive join: se +50% and vp +100% => +150% => 25000 (not 1.5*2.0 = 30000).
  assert.equal(state?.combinedMultiplier, 25000n);

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
  // additive join: nft +10% se +50% vp +100% => +160% => 26000 (not 1.1*1.5*2.0 = 33000).
  assert.equal(state?.combinedMultiplier, 26000n);
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

test('user settled records its event timestamp', async () => {
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
    timestamp: 777n,
    ...eventData(120, 777, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: settle,
    mockDb,
  });

  const recordId = `${settle.transaction.hash}-${settle.logIndex}`;
  const record = mockDb.entities.LeaderboardKeeperUserSettled.get(recordId);
  assert.equal(record?.timestamp, 777);
});

test('keeper user settled records the exact event-time Tide phase before accounting', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 8n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '8',
    epochNumber: 8n,
    startBlock: 0n,
    startTime: 700,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 700,
    scheduledEndTime: 0,
  });

  const settled = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: 777n,
    ...eventData(120, 777, ADDRESSES.keeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: settled,
    mockDb,
  });

  const raw = mockDb.entities.LeaderboardKeeperUserSettled.get(
    `${settled.transaction.hash}-${settled.logIndex}`
  ) as { epochNumber?: bigint; isGap?: boolean } | undefined;
  assert.ok(raw, 'raw keeper settlement is written');
  assert.equal(raw.epochNumber, 8n, 'raw settlement records its event-time Tide');
  assert.equal(raw.isGap, false, 'active event is not recorded as a gap');
});

test('keeper user settled records an unverified missing state as epoch zero active-phase work', async () => {
  const previousBootstrap = process.env.ENVIO_DISABLE_BOOTSTRAP;
  try {
    process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';
    const eventData = createEventDataFactory();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const settled = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: ADDRESSES.user,
      timestamp: 777n,
      ...eventData(KEEPER_TEST_BLOCK - 10, 777, ADDRESSES.keeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: settled,
      mockDb,
    });

    const raw = keeperRawPhase(mockDb, `${settled.transaction.hash}-${settled.logIndex}`);
    assert.deepEqual([raw?.epochNumber, raw?.isGap], [0n, false]);
    assert.equal(mockDb.entities.UserEpochFinalization.get(`${ADDRESSES.user}:0`), undefined);
  } finally {
    if (previousBootstrap === undefined) delete process.env.ENVIO_DISABLE_BOOTSTRAP;
    else process.env.ENVIO_DISABLE_BOOTSTRAP = previousBootstrap;
  }
});

test('keeper user settled classifies the scheduled event-time Tide transition before its raw write', async () => {
  try {
    const endTime = 900;
    let mockDb = seedKeeperPhase(TestHelpers.MockDb.createMockDb(), 8n, true, 100);
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      ...mockDb.entities.LeaderboardEpoch.get('8')!,
      scheduledEndTime: endTime,
    });
    const settled = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: ADDRESSES.user,
      timestamp: 1_000n,
      ...createEventDataFactory()(KEEPER_TEST_BLOCK - 5, 1_000, ADDRESSES.keeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: settled,
      mockDb,
    });

    const raw = keeperRawPhase(mockDb, `${settled.transaction.hash}-${settled.logIndex}`);
    assert.deepEqual([raw?.epochNumber, raw?.isGap], [8n, true]);
    assert.equal(mockDb.entities.LeaderboardState.get('current')?.isActive, false);
    assert.equal(mockDb.entities.LeaderboardEpoch.get('8')?.endTime, endTime);
    assert.equal(
      mockDb.entities.UserEpochFinalization.get(`${ADDRESSES.user}:8`)?.settledThrough,
      endTime
    );
  } finally {
  }
});

test('keeper scheduled phase projection makes every ordered read preloadable', async t => {
  const previousBootstrap = process.env.ENVIO_DISABLE_BOOTSTRAP;
  try {
    process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';
    const timestamp = 1_000;
    const scheduledEndTime = 900;
    const handler = await getRegisteredEventHandler('LeaderboardKeeper', 'UserSettled');
    const scenarios = [
      { label: 'active-to-gap', startsNextEpoch: false, transitions: true },
      { label: 'active-to-next-active', startsNextEpoch: true, transitions: true },
      { label: 'ordinary historical active', startsNextEpoch: false, transitions: false },
    ] as const;

    for (const [scenarioIndex, scenario] of scenarios.entries()) {
      await t.test(scenario.label, async () => {
        let mockDb = seedKeeperPhase(TestHelpers.MockDb.createMockDb(), 8n, true, 100);
        if (scenario.transitions) {
          mockDb = mockDb.entities.LeaderboardEpoch.set({
            ...mockDb.entities.LeaderboardEpoch.get('8')!,
            scheduledEndTime,
          });
        }
        if (scenario.startsNextEpoch) {
          mockDb = mockDb.entities.LeaderboardEpoch.set({
            id: '9',
            epochNumber: 9n,
            startBlock: 0n,
            startTime: scheduledEndTime,
            endBlock: undefined,
            endTime: undefined,
            isActive: false,
            duration: undefined,
            scheduledStartTime: scheduledEndTime,
            scheduledEndTime: 0,
          });
        }
        const event = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
          user: KEEPER_PHASE.unifiedUser,
          timestamp: BigInt(timestamp),
          ...createEventDataFactory()(
            KEEPER_TEST_BLOCK + 600 + scenarioIndex,
            timestamp,
            ADDRESSES.keeper
          ),
        });
        const preload = createInstrumentedKeeperContext(mockDb, true);
        await handler({ event, context: preload.context });
        const ordered = createInstrumentedKeeperContext(mockDb, false);
        await handler({ event, context: ordered.context });
        // The Tide-close LP holder sweep (FINDING 003) reads LPPoolPositionIndex to credit
        // holders that no event ever touched. That read is intentionally ordered-only: the
        // position set is not a bounded deterministic envelope, and Envio's LoadManager loads
        // an unanticipated id from storage during ordered processing. It is paid once per Tide
        // boundary (~29.5 days). Every OTHER entity must still be preloaded, so this guard
        // stays live for the rest of the handler.
        const orderedOnlyReads = [...ordered.reads]
          .filter(read => !preload.reads.has(read))
          .filter(read => !read.startsWith('LPPoolPositionIndex:'))
          .sort();
        assert.deepEqual(orderedOnlyReads, [], scenario.label);

        const settlementId = `${event.transaction.hash}-${event.logIndex}`;
        const raw = ordered.stores.get('LeaderboardKeeperUserSettled')?.get(settlementId);
        const expectedEpoch = scenario.startsNextEpoch ? 9n : 8n;
        const expectedGap = scenario.transitions && !scenario.startsNextEpoch;
        assert.deepEqual(
          [raw?.epochNumber, raw?.isGap, raw?.timestamp],
          [expectedEpoch, expectedGap, timestamp],
          scenario.label
        );

        if (expectedGap) {
          assert.ok(
            preload.reads.has(`UserEpochFinalization:${KEEPER_PHASE.unifiedUser}:8`),
            'preload classifies the projected gap'
          );
          assert.deepEqual(
            ordered.stores.get('UserEpochFinalization')?.get(`${KEEPER_PHASE.unifiedUser}:8`),
            {
              id: `${KEEPER_PHASE.unifiedUser}:8`,
              user_id: KEEPER_PHASE.unifiedUser,
              epochNumber: 8n,
              epochEndTime: scheduledEndTime,
              settledThrough: scheduledEndTime,
              finalizedAt: timestamp,
              blockNumber: BigInt(event.block.number),
              txHash: event.transaction.hash,
              settlementEventId: settlementId,
            }
          );
        } else {
          assert.equal(
            ordered.stores
              .get('UserEpochFinalization')
              ?.get(`${KEEPER_PHASE.unifiedUser}:${expectedEpoch.toString()}`),
            undefined,
            scenario.label
          );
        }

        // Every active-phase keeper settlement now performs a full settlement. The previous
        // final-only/raw-only path was gated by ENVIO_LEADERBOARD_LIVE_EPOCH, which was removed
        // after it was measured to corrupt closed-Tide reserve and VP-multiplier points.
        assert.ok(
          preload.reads.has(`UserLPPositionIndex:${KEEPER_PHASE.unifiedUser}`),
          `${scenario.label}: preload includes full settlement`
        );
      });
    }
  } finally {
    if (previousBootstrap === undefined) delete process.env.ENVIO_DISABLE_BOOTSTRAP;
    else process.env.ENVIO_DISABLE_BOOTSTRAP = previousBootstrap;
  }
});

test('keeper multi-cutover Tide closures preload every boundary-era static LP dependency', async t => {
  const previousBootstrap = process.env.ENVIO_DISABLE_BOOTSTRAP;
  const priorTide7Override = EPOCH_DATES_OVERRIDES['7'];
  try {
    process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';
    const strictTide7Start = Math.floor(
      (LP_V2_CUTOVER_TIMESTAMP + LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP) / 2
    );
    const strictTide7End = Math.floor(
      (LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + LP_V2_RESUME_CUTOVER_TIMESTAMP) / 2
    );
    const scenarios = [
      {
        label: 'strictly intervening boundaries',
        tide7Start: strictTide7Start,
        tide7End: strictTide7End,
        usesOverride: false,
      },
      {
        label: 'equal Tide and static boundaries',
        tide7Start: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
        tide7End: LP_V2_RESUME_CUTOVER_TIMESTAMP,
        usesOverride: false,
      },
      {
        label: 'overridden Tide 7 dates',
        tide7Start: strictTide7Start + 1,
        tide7End: strictTide7End + 1,
        usesOverride: true,
      },
    ] as const;
    const handler = await getRegisteredEventHandler('LeaderboardKeeper', 'UserSettled');
    const eventData = createEventDataFactory();

    for (const [scenarioIndex, scenario] of scenarios.entries()) {
      await t.test(scenario.label, async () => {
        if (scenario.usesOverride) {
          EPOCH_DATES_OVERRIDES['7'] = {
            startTime: scenario.tide7Start,
            endTime: scenario.tide7End,
          };
        } else {
          delete EPOCH_DATES_OVERRIDES['7'];
        }
        const mockDb = seedKeeperStaticChronologyFixture(
          TestHelpers.MockDb.createMockDb(),
          scenario.tide7Start,
          scenario.tide7End
        );
        const eventTimestamp = LP_V2_RESUME_CUTOVER_TIMESTAMP + 1_000;
        const event = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
          user: KEEPER_PHASE.unifiedUser,
          timestamp: BigInt(eventTimestamp),
          ...eventData(
            LP_V2_RESUME_CUTOVER_BLOCK + 1 + scenarioIndex,
            eventTimestamp,
            ADDRESSES.keeper
          ),
        });
        const preload = createInstrumentedKeeperContext(mockDb, true);
        await handler({ event, context: preload.context });
        const ordered = createInstrumentedKeeperContext(mockDb, false);
        await handler({ event, context: ordered.context });

        // The Tide-close LP holder sweep (FINDING 003) reads LPPoolPositionIndex to credit
        // holders that no event ever touched. That read is intentionally ordered-only: the
        // position set is not a bounded deterministic envelope, and Envio's LoadManager loads
        // an unanticipated id from storage during ordered processing. It is paid once per Tide
        // boundary (~29.5 days). Every OTHER entity must still be preloaded, so this guard
        // stays live for the rest of the handler.
        const orderedOnlyReads = [...ordered.reads]
          .filter(read => !preload.reads.has(read))
          .filter(read => !read.startsWith('LPPoolPositionIndex:'))
          .sort();
        assert.deepEqual(orderedOnlyReads, [], scenario.label);

        const settlementId = `${event.transaction.hash}-${event.logIndex}`;
        assert.deepEqual(
          ordered.stores.get('LeaderboardKeeperUserSettled')?.get(settlementId),
          {
            id: settlementId,
            user_id: KEEPER_PHASE.unifiedUser,
            epochNumber: 7n,
            isGap: true,
            timestamp: eventTimestamp,
            txHash: event.transaction.hash,
          },
          scenario.label
        );
        assert.deepEqual(
          ordered.stores.get('UserEpochFinalization')?.get(`${KEEPER_PHASE.unifiedUser}:7`),
          {
            id: `${KEEPER_PHASE.unifiedUser}:7`,
            user_id: KEEPER_PHASE.unifiedUser,
            epochNumber: 7n,
            epochEndTime: scenario.tide7End,
            settledThrough: scenario.tide7End,
            finalizedAt: eventTimestamp,
            blockNumber: BigInt(event.block.number),
            txHash: event.transaction.hash,
            settlementEventId: settlementId,
          },
          scenario.label
        );
        for (const dependency of [
          `LPPoolEpochGrowth:${KEEPER_BALANCER_POOL}:7`,
          `TokenInfo:${USDC_ADDRESS}`,
          `TokenInfo:${KEEPER_DUST_TOKEN}`,
        ]) {
          assert.ok(preload.reads.has(dependency), `${scenario.label}: ${dependency}`);
        }
        for (const read of [...preload.reads, ...ordered.reads]) {
          assert.equal(
            /^(LPPoolPositionIndex|UserLPPositionIndex|UserLPPosition):\*$/.test(read),
            false,
            `${scenario.label}: collection scan ${read}`
          );
        }
      });
    }
  } finally {
    if (priorTide7Override === undefined) delete EPOCH_DATES_OVERRIDES['7'];
    else EPOCH_DATES_OVERRIDES['7'] = priorTide7Override;
    if (previousBootstrap === undefined) delete process.env.ENVIO_DISABLE_BOOTSTRAP;
    else process.env.ENVIO_DISABLE_BOOTSTRAP = previousBootstrap;
  }
});

test('keeper scheduled gap duplicate validates its proof against the projected closed Tide in preload', async () => {
  const previousBootstrap = process.env.ENVIO_DISABLE_BOOTSTRAP;
  try {
    process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';
    const proof = seedKeeperFinalizationProof(TestHelpers.MockDb.createMockDb());
    let mockDb = seedKeeperPhase(TestHelpers.MockDb.createMockDb(), 8n, true, 100);
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      ...mockDb.entities.LeaderboardEpoch.get('8')!,
      scheduledEndTime: KEEPER_FINALIZATION_PROOF.epochEndTime,
    });
    mockDb = mockDb.entities.LeaderboardKeeperUserSettled.set(proof.raw);
    mockDb = mockDb.entities.UserEpochFinalization.set(proof.certificate);
    const event = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: KEEPER_PHASE.unifiedUser,
      timestamp: BigInt(KEEPER_FINALIZATION_PROOF.finalizedAt),
      ...createEventDataFactory()(
        KEEPER_TEST_BLOCK + 610,
        KEEPER_FINALIZATION_PROOF.finalizedAt,
        ADDRESSES.keeper
      ),
    });
    const handler = await getRegisteredEventHandler('LeaderboardKeeper', 'UserSettled');
    const preload = createInstrumentedKeeperContext(mockDb, true);
    await handler({ event, context: preload.context });
    const ordered = createInstrumentedKeeperContext(mockDb, false);
    await handler({ event, context: ordered.context });

    // LPPoolPositionIndex is the Tide-close holder sweep's intentionally ordered-only read
    // (see FINDING 003). Every other entity must still be preloaded.
    assert.deepEqual(
      [...ordered.reads]
        .filter(read => !preload.reads.has(read))
        .filter(read => !read.startsWith('LPPoolPositionIndex:'))
        .sort(),
      []
    );
    assert.deepEqual(
      ordered.stores.get('UserEpochFinalization')?.get(proof.certificate.id),
      proof.certificate
    );
    assert.deepEqual(
      [
        ordered.stores
          .get('LeaderboardKeeperUserSettled')
          ?.get(`${event.transaction.hash}-${event.logIndex}`)?.epochNumber,
        ordered.stores
          .get('LeaderboardKeeperUserSettled')
          ?.get(`${event.transaction.hash}-${event.logIndex}`)?.isGap,
      ],
      [8n, true]
    );
    assert.equal(
      ordered.stores.get('UserEpochStats')?.get(`${KEEPER_PHASE.unifiedUser}:8`),
      undefined
    );
  } finally {
    if (previousBootstrap === undefined) delete process.env.ENVIO_DISABLE_BOOTSTRAP;
    else process.env.ENVIO_DISABLE_BOOTSTRAP = previousBootstrap;
  }
});

test('keeper event timestamp falls back to the block timestamp when missing', () => {
  assert.equal(resolveKeeperEventTimestamp(undefined, 777), 777);
});

type Task7KeeperExports = {
  classifyKeeperSettlement?: (
    context: handlerContext,
    userId: string
  ) => Promise<{
    mode: 'LIVE_OR_UNVERIFIED_ACTIVE' | 'GAP_FINALIZE' | 'GAP_DUPLICATE';
    epochNumber: bigint;
    finalizationId: string;
  }>;
};

function getTask7KeeperExports() {
  const task7 = leaderboardKeeperModule as Task7KeeperExports;
  assert.equal(
    typeof task7.classifyKeeperSettlement,
    'function',
    'exports the keeper settlement classifier'
  );
  return {
    classifyKeeperSettlement: task7.classifyKeeperSettlement,
  } as {
    classifyKeeperSettlement: NonNullable<Task7KeeperExports['classifyKeeperSettlement']>;
  };
}

test('keeper settlement classifier distinguishes live active and gap replay modes', async () => {
  try {
    const { classifyKeeperSettlement } = getTask7KeeperExports();
    const userId = '0x000000000000000000000000000000000000B00A';
    const normalizedUserId = userId.toLowerCase();

    const classify = async (
      currentEpochNumber: bigint,
      isActive: boolean,
      hasCertificate = false
    ) => {
      const finalizationId = `${normalizedUserId}:${currentEpochNumber}`;
      const context = {
        LeaderboardState: {
          get: async (id: string) =>
            id === 'current' ? { id, currentEpochNumber, isActive } : undefined,
        },
        UserEpochFinalization: {
          get: async (id: string) =>
            hasCertificate && id === finalizationId
              ? {
                  id,
                  user_id: normalizedUserId,
                  epochNumber: currentEpochNumber,
                  epochEndTime: 800,
                  settledThrough: 800,
                  finalizedAt: 900,
                  blockNumber: 1n,
                  txHash: '0x01',
                  settlementEventId: '0x01-0',
                }
              : undefined,
        },
        LeaderboardEpoch: {
          get: async (id: string) =>
            hasCertificate && id === currentEpochNumber.toString()
              ? {
                  id,
                  epochNumber: currentEpochNumber,
                  startBlock: 1n,
                  startTime: 100,
                  endBlock: 2n,
                  endTime: 800,
                  isActive: false,
                  duration: 700n,
                  scheduledStartTime: 100,
                  scheduledEndTime: 800,
                }
              : undefined,
        },
        LeaderboardKeeperUserSettled: {
          get: async (id: string) =>
            hasCertificate && id === '0x01-0'
              ? {
                  id,
                  user_id: normalizedUserId,
                  epochNumber: currentEpochNumber,
                  isGap: true,
                  timestamp: 900,
                  txHash: '0x01',
                }
              : undefined,
        },
      } as unknown as handlerContext;
      return classifyKeeperSettlement(context, userId);
    };

    assert.deepEqual(await classify(9n, true), {
      mode: 'LIVE_OR_UNVERIFIED_ACTIVE',
      epochNumber: 9n,
      finalizationId: `${normalizedUserId}:9`,
    });
    // Historical active settlements are no longer final-only: the gate that produced that
    // mode was removed after it was measured to corrupt closed-Tide points.
    assert.equal((await classify(6n, true)).mode, 'LIVE_OR_UNVERIFIED_ACTIVE');
    assert.deepEqual(await classify(6n, true), {
      mode: 'LIVE_OR_UNVERIFIED_ACTIVE',
      epochNumber: 6n,
      finalizationId: `${normalizedUserId}:6`,
    });
    assert.deepEqual(await classify(8n, false), {
      mode: 'GAP_FINALIZE',
      epochNumber: 8n,
      finalizationId: `${normalizedUserId}:8`,
    });
    assert.deepEqual(await classify(8n, false, true), {
      mode: 'GAP_DUPLICATE',
      epochNumber: 8n,
      finalizationId: `${normalizedUserId}:8`,
    });

    assert.equal((await classify(8n, false)).mode, 'GAP_FINALIZE');
    const missingStateContext = {
      LeaderboardState: { get: async () => undefined },
      UserEpochFinalization: { get: async () => undefined },
    } as unknown as handlerContext;
    assert.deepEqual(await classifyKeeperSettlement(missingStateContext, userId), {
      mode: 'LIVE_OR_UNVERIFIED_ACTIVE',
      epochNumber: 0n,
      finalizationId: `${normalizedUserId}:0`,
    });
  } finally {
  }
});

test('keeper live active settlement records phase and settles LP, VP, and reserve accounting', async () => {
  try {
    const startTime = 100;
    const settlementEnd = startTime + ONE_DAY;
    const blockNumber = KEEPER_TEST_BLOCK;
    const eventData = createEventDataFactory();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const seeded = seedUnifiedKeeperFixture(mockDb, {
      epochNumber: 9n,
      isActive: true,
      startTime,
      settlementEnd,
    });
    mockDb = seeded.mockDb;

    const settled = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: KEEPER_PHASE.unifiedUser,
      timestamp: BigInt(settlementEnd),
      ...eventData(blockNumber, settlementEnd, ADDRESSES.keeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: settled,
      mockDb,
    });

    const settlementId = `${settled.transaction.hash}-${settled.logIndex}`;
    const raw = keeperRawPhase(mockDb, settlementId);
    assert.deepEqual(
      [raw?.epochNumber, raw?.isGap, raw?.timestamp, raw?.txHash],
      [9n, false, settlementEnd, settled.transaction.hash]
    );
    const stats = mockDb.entities.UserEpochStats.get(`${KEEPER_PHASE.unifiedUser}:9`);
    assert.equal(stats?.lpPoints, 10n ** 18n, 'one independently seeded LP point is consumed');
    assert.equal(
      stats?.depositPoints,
      10n ** 18n,
      'one $1 reserve held for one day earns exactly one point'
    );
    assert.equal(
      stats?.dailyVPPoints,
      10n ** 18n,
      'one permanent VP unit held for one day earns exactly one point'
    );
    assert.equal(stats?.totalPoints, 3n * 10n ** 18n);
    assert.equal(
      mockDb.entities.UserLPEpochCursor.get(`${seeded.positionId}:9`)?.lastSettledAt,
      settlementEnd
    );
    assert.equal(
      mockDb.entities.UserReservePoints.get(`${KEEPER_PHASE.unifiedUser}:${seeded.reserveId}`)
        ?.depositPoints,
      10n ** 18n
    );
    assert.equal(
      mockDb.entities.UserEpochFinalization.get(`${KEEPER_PHASE.unifiedUser}:9`),
      undefined,
      'active settlement never creates a gap certificate'
    );
  } finally {
  }
});

test('keeper first gap settles LP, VP, and reserve once through Tide end, then certificates replay', async () => {
  try {
    const startTime = 100;
    const epochEndTime = startTime + ONE_DAY;
    const firstEventTime = epochEndTime + ONE_DAY;
    const firstBlockNumber = KEEPER_TEST_BLOCK + 300;
    const eventData = createEventDataFactory();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const seeded = seedUnifiedKeeperFixture(mockDb, {
      epochNumber: 8n,
      isActive: false,
      startTime,
      settlementEnd: epochEndTime,
    });
    mockDb = seeded.mockDb;

    const first = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: KEEPER_PHASE.unifiedUser,
      timestamp: BigInt(firstEventTime),
      ...eventData(firstBlockNumber, firstEventTime, ADDRESSES.keeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: first,
      mockDb,
    });

    const firstSettlementId = `${first.transaction.hash}-${first.logIndex}`;
    const firstRaw = keeperRawPhase(mockDb, firstSettlementId);
    assert.deepEqual(firstRaw, {
      id: firstSettlementId,
      user_id: KEEPER_PHASE.unifiedUser,
      epochNumber: 8n,
      isGap: true,
      timestamp: firstEventTime,
      txHash: first.transaction.hash,
    });

    const statsId = `${KEEPER_PHASE.unifiedUser}:8`;
    const lpCursorId = `${seeded.positionId}:8`;
    const reserveCursorId = `${KEEPER_PHASE.unifiedUser}:${seeded.reserveId}`;
    const stats = mockDb.entities.UserEpochStats.get(statsId);
    assert.equal(stats?.lpPoints, 10n ** 18n);
    assert.equal(stats?.depositPoints, 10n ** 18n);
    assert.equal(stats?.dailyVPPoints, 10n ** 18n);
    assert.equal(stats?.totalPoints, 3n * 10n ** 18n);
    assert.equal(mockDb.entities.UserLPEpochCursor.get(lpCursorId)?.lastSettledAt, epochEndTime);
    assert.equal(
      mockDb.entities.UserLPEpochCursor.get(lpCursorId)?.growthBaselineX128,
      ONE_POINT_GROWTH_X128
    );
    assert.equal(mockDb.entities.UserReservePoints.get(reserveCursorId)?.depositPoints, 10n ** 18n);
    assert.equal(stats?.lastVPAccrualTimestamp, epochEndTime);

    const finalizationId = `${KEEPER_PHASE.unifiedUser}:8`;
    const firstCertificate = mockDb.entities.UserEpochFinalization.get(finalizationId);
    assert.deepEqual(firstCertificate, {
      id: finalizationId,
      user_id: KEEPER_PHASE.unifiedUser,
      epochNumber: 8n,
      epochEndTime,
      settledThrough: epochEndTime,
      finalizedAt: firstEventTime,
      blockNumber: BigInt(first.block.number),
      txHash: first.transaction.hash,
      settlementEventId: firstSettlementId,
    });

    const accountingAfterFirst = {
      stats: mockDb.entities.UserEpochStats.get(statsId),
      lpCursor: mockDb.entities.UserLPEpochCursor.get(lpCursorId),
      reserveCursor: mockDb.entities.UserReservePoints.get(reserveCursorId),
      lpPosition: mockDb.entities.UserLPPosition.get(seeded.positionId),
      lpStats: mockDb.entities.UserLPStats.get(KEEPER_PHASE.unifiedUser),
      userPoints: mockDb.entities.UserPoints.get(KEEPER_PHASE.unifiedUser),
      userState: mockDb.entities.UserLeaderboardState.get(KEEPER_PHASE.unifiedUser),
      price: mockDb.entities.PriceOracleAsset.get(KEEPER_PHASE.asset),
    };

    const secondEventTime = firstEventTime + 123;
    const second = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: KEEPER_PHASE.unifiedUser,
      timestamp: BigInt(secondEventTime),
      ...eventData(firstBlockNumber + 1, secondEventTime, ADDRESSES.keeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: second,
      mockDb,
    });

    const secondSettlementId = `${second.transaction.hash}-${second.logIndex}`;
    assert.deepEqual(keeperRawPhase(mockDb, secondSettlementId), {
      id: secondSettlementId,
      user_id: KEEPER_PHASE.unifiedUser,
      epochNumber: 8n,
      isGap: true,
      timestamp: secondEventTime,
      txHash: second.transaction.hash,
    });
    assert.deepEqual(
      {
        stats: mockDb.entities.UserEpochStats.get(statsId),
        lpCursor: mockDb.entities.UserLPEpochCursor.get(lpCursorId),
        reserveCursor: mockDb.entities.UserReservePoints.get(reserveCursorId),
        lpPosition: mockDb.entities.UserLPPosition.get(seeded.positionId),
        lpStats: mockDb.entities.UserLPStats.get(KEEPER_PHASE.unifiedUser),
        userPoints: mockDb.entities.UserPoints.get(KEEPER_PHASE.unifiedUser),
        userState: mockDb.entities.UserLeaderboardState.get(KEEPER_PHASE.unifiedUser),
        price: mockDb.entities.PriceOracleAsset.get(KEEPER_PHASE.asset),
      },
      accountingAfterFirst,
      'duplicate gap does not readvance any user accounting cursor or total'
    );
    assert.deepEqual(
      mockDb.entities.UserEpochFinalization.get(finalizationId),
      firstCertificate,
      'duplicate gap preserves the first certificate byte-for-byte'
    );
  } finally {
  }
});

test('keeper duplicate gap rejects every malformed certificate or referenced raw proof in preload and ordered processing', async t => {
  const proof = KEEPER_FINALIZATION_PROOF;
  const cases: Array<{
    label: string;
    mutateCertificate?: (row: UserEpochFinalization) => UserEpochFinalization;
    mutateEpoch?: (row: LeaderboardEpoch) => LeaderboardEpoch;
    mutateRaw?: (row: LeaderboardKeeperUserSettled) => LeaderboardKeeperUserSettled;
  }> = [
    {
      label: 'certificate id',
      mutateCertificate: row => ({ ...row, id: `${KEEPER_PHASE.unifiedUser}:7` }),
    },
    {
      label: 'certificate user',
      mutateCertificate: row => ({ ...row, user_id: ADDRESSES.owner }),
    },
    {
      label: 'certificate epoch',
      mutateCertificate: row => ({ ...row, epochNumber: 7n }),
    },
    {
      label: 'certificate epoch end',
      mutateCertificate: row => ({ ...row, epochEndTime: proof.epochEndTime - 1 }),
    },
    {
      label: 'certificate settled through',
      mutateCertificate: row => ({ ...row, settledThrough: proof.epochEndTime - 1 }),
    },
    {
      label: 'certificate finalized before end',
      mutateCertificate: row => ({ ...row, finalizedAt: proof.epochEndTime - 1 }),
    },
    {
      label: 'certificate transaction hash',
      mutateCertificate: row => ({ ...row, txHash: `0x${'f2'.padStart(64, '0')}` }),
    },
    {
      label: 'certificate settlement event reference',
      mutateCertificate: row => ({ ...row, settlementEventId: 'missing-proof-row' }),
    },
    {
      label: 'epoch id',
      mutateEpoch: row => ({ ...row, id: '7' }),
    },
    {
      label: 'epoch number',
      mutateEpoch: row => ({ ...row, epochNumber: 7n }),
    },
    {
      label: 'epoch still active',
      mutateEpoch: row => ({ ...row, isActive: true }),
    },
    {
      label: 'epoch missing end',
      mutateEpoch: row => ({ ...row, endTime: undefined }),
    },
    {
      label: 'epoch zero end',
      mutateEpoch: row => ({ ...row, endTime: 0 }),
    },
    {
      label: 'epoch end before start',
      mutateEpoch: row => ({ ...row, endTime: proof.startTime - 1 }),
    },
    {
      label: 'raw id',
      mutateRaw: row => ({ ...row, id: 'wrong-proof-row' }),
    },
    {
      label: 'raw user',
      mutateRaw: row => ({ ...row, user_id: ADDRESSES.owner }),
    },
    {
      label: 'raw epoch',
      mutateRaw: row => ({ ...row, epochNumber: 7n }),
    },
    {
      label: 'raw active phase',
      mutateRaw: row => ({ ...row, isGap: false }),
    },
    {
      label: 'raw timestamp',
      mutateRaw: row => ({ ...row, timestamp: proof.finalizedAt + 1 }),
    },
    {
      label: 'raw transaction hash',
      mutateRaw: row => ({ ...row, txHash: `0x${'f3'.padStart(64, '0')}` }),
    },
  ];
  const handler = await getRegisteredEventHandler('LeaderboardKeeper', 'UserSettled');

  for (const [caseIndex, entry] of cases.entries()) {
    await t.test(entry.label, async t => {
      const seeded = seedKeeperFinalizationProof(TestHelpers.MockDb.createMockDb());
      const certificate = entry.mutateCertificate
        ? entry.mutateCertificate(seeded.certificate)
        : seeded.certificate;
      const epoch = entry.mutateEpoch ? entry.mutateEpoch(seeded.epoch) : seeded.epoch;
      const raw = entry.mutateRaw ? entry.mutateRaw(seeded.raw) : seeded.raw;
      const event = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
        user: KEEPER_PHASE.unifiedUser,
        timestamp: BigInt(proof.finalizedAt),
        ...createEventDataFactory()(
          KEEPER_TEST_BLOCK + 450 + caseIndex,
          proof.finalizedAt,
          ADDRESSES.keeper
        ),
      });

      for (const isPreload of [true, false]) {
        await t.test(isPreload ? 'preload' : 'ordered', async () => {
          // The mutated rows go into the probe under their ORIGINAL keys. Three cases mutate
          // the id itself, which no keyed store can hold, and that mismatch is precisely what
          // the handler's proof validation must reject.
          const probe = createInstrumentedKeeperContext(seeded.mockDb, isPreload);
          overrideProbeRow(
            probe.stores,
            'UserEpochFinalization',
            seeded.certificate.id,
            certificate
          );
          overrideProbeRow(probe.stores, 'LeaderboardEpoch', seeded.epoch.id, epoch);
          overrideProbeRow(probe.stores, 'LeaderboardKeeperUserSettled', seeded.raw.id, raw);
          await assert.rejects(
            () => handler({ event, context: probe.context }),
            /invalid keeper gap finalization proof/
          );
        });
      }
    });
  }
});

test('keeper duplicate gap accepts address-equivalent proof and preserves its certificate byte-for-byte', async () => {
  const proof = KEEPER_FINALIZATION_PROOF;
  const seeded = seedKeeperFinalizationProof(TestHelpers.MockDb.createMockDb());
  const certificate = {
    ...seeded.certificate,
    user_id: seeded.certificate.user_id.toUpperCase(),
  };
  const raw = { ...seeded.raw, user_id: seeded.raw.user_id.toUpperCase() };
  let mutated = overwriteKeeperFixtureRow(
    seeded.mockDb,
    'UserEpochFinalization',
    seeded.certificate.id,
    certificate
  );
  mutated = overwriteKeeperFixtureRow(mutated, 'LeaderboardKeeperUserSettled', seeded.raw.id, raw);
  const event = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: KEEPER_PHASE.unifiedUser,
    timestamp: BigInt(proof.finalizedAt),
    ...createEventDataFactory()(KEEPER_TEST_BLOCK + 480, proof.finalizedAt, ADDRESSES.keeper),
  });

  const mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event,
    mockDb: mutated,
  });

  assert.deepEqual(mockDb.entities.UserEpochFinalization.get(seeded.certificate.id), certificate);
  assert.ok(keeperRawPhase(mockDb, `${event.transaction.hash}-${event.logIndex}`));
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${KEEPER_PHASE.unifiedUser}:${proof.epochNumber}`),
    undefined
  );
});

test('keeper malformed duplicate proof rejects atomically without accounting or proof mutation', async () => {
  const proof = KEEPER_FINALIZATION_PROOF;
  const seeded = seedKeeperFinalizationProof(TestHelpers.MockDb.createMockDb());
  const malformed = {
    ...seeded.certificate,
    settledThrough: proof.epochEndTime - 1,
  };
  const mutated = overwriteKeeperFixtureRow(
    seeded.mockDb,
    'UserEpochFinalization',
    seeded.certificate.id,
    malformed
  );
  const event = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: KEEPER_PHASE.unifiedUser,
    timestamp: BigInt(proof.finalizedAt),
    ...createEventDataFactory()(KEEPER_TEST_BLOCK + 490, proof.finalizedAt, ADDRESSES.keeper),
  });

  await assert.rejects(
    () =>
      TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
        event,
        mockDb: mutated,
      }),
    /invalid keeper gap finalization proof/
  );
  assert.deepEqual(mutated.entities.UserEpochFinalization.get(seeded.certificate.id), malformed);
  assert.equal(keeperRawPhase(mutated, `${event.transaction.hash}-${event.logIndex}`), undefined);
  assert.equal(
    mutated.entities.UserEpochStats.get(
      `${KEEPER_PHASE.unifiedUser}:${proof.epochNumber.toString()}`
    ),
    undefined
  );
});

test('keeper same-batch first gap and duplicate write two raw rows and one immutable first proof', async () => {
  try {
    const startTime = 100;
    const epochEndTime = startTime + ONE_DAY;
    const eventTimestamp = epochEndTime + ONE_DAY;
    const seeded = seedUnifiedKeeperFixture(TestHelpers.MockDb.createMockDb(), {
      epochNumber: 8n,
      isActive: false,
      startTime,
      settlementEnd: epochEndTime,
    });
    const eventData = createEventDataFactory();
    const first = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: KEEPER_PHASE.unifiedUser,
      timestamp: BigInt(eventTimestamp),
      ...eventData(KEEPER_TEST_BLOCK + 500, eventTimestamp, ADDRESSES.keeper),
    });
    const second = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: KEEPER_PHASE.unifiedUser,
      timestamp: BigInt(eventTimestamp),
      ...eventData(KEEPER_TEST_BLOCK + 500, eventTimestamp, ADDRESSES.keeper),
    });

    const batch = await processEvents({ events: [first, second], mockDb: seeded.mockDb });
    const firstId = `${first.transaction.hash}-${first.logIndex}`;
    const secondId = `${second.transaction.hash}-${second.logIndex}`;
    const certificate = batch.mockDb.entities.UserEpochFinalization.get(
      `${KEEPER_PHASE.unifiedUser}:8`
    );
    assert.ok(keeperRawPhase(batch.mockDb, firstId));
    assert.ok(keeperRawPhase(batch.mockDb, secondId));
    assert.deepEqual(
      [certificate?.settlementEventId, certificate?.txHash, certificate?.finalizedAt],
      [firstId, first.transaction.hash, eventTimestamp]
    );
    assert.equal(
      batch.mockDb.entities.UserEpochStats.get(`${KEEPER_PHASE.unifiedUser}:8`)?.totalPoints,
      3n * 10n ** 18n
    );
  } finally {
  }
});

test('keeper gap finalization fails closed without an exact usable closed Tide', async () => {
  try {
    const invalidEpochs = [
      { label: 'missing epoch', epoch: undefined },
      {
        label: 'open epoch row',
        epoch: {
          id: '8',
          epochNumber: 8n,
          startBlock: BigInt(LEADERBOARD_START_BLOCK),
          startTime: 100,
          endBlock: undefined,
          endTime: undefined,
          isActive: true,
          duration: undefined,
          scheduledStartTime: 100,
          scheduledEndTime: 0,
        },
      },
      {
        label: 'wrong epoch payload',
        epoch: {
          id: '8',
          epochNumber: 7n,
          startBlock: BigInt(LEADERBOARD_START_BLOCK),
          startTime: 100,
          endBlock: BigInt(LEADERBOARD_START_BLOCK + 1),
          endTime: 200,
          isActive: false,
          duration: 100n,
          scheduledStartTime: 100,
          scheduledEndTime: 200,
        },
      },
      {
        label: 'inactive row without an end',
        epoch: {
          id: '8',
          epochNumber: 8n,
          startBlock: BigInt(LEADERBOARD_START_BLOCK),
          startTime: 100,
          endBlock: undefined,
          endTime: undefined,
          isActive: false,
          duration: undefined,
          scheduledStartTime: 100,
          scheduledEndTime: 0,
        },
      },
      {
        label: 'zero end time',
        epoch: {
          id: '8',
          epochNumber: 8n,
          startBlock: BigInt(LEADERBOARD_START_BLOCK),
          startTime: 0,
          endBlock: BigInt(LEADERBOARD_START_BLOCK + 1),
          endTime: 0,
          isActive: false,
          duration: 0n,
          scheduledStartTime: 0,
          scheduledEndTime: 0,
        },
      },
      {
        label: 'end before start',
        epoch: {
          id: '8',
          epochNumber: 8n,
          startBlock: BigInt(LEADERBOARD_START_BLOCK),
          startTime: 200,
          endBlock: BigInt(LEADERBOARD_START_BLOCK + 1),
          endTime: 100,
          isActive: false,
          duration: undefined,
          scheduledStartTime: 200,
          scheduledEndTime: 100,
        },
      },
    ];

    for (const [index, entry] of invalidEpochs.entries()) {
      let mockDb = TestHelpers.MockDb.createMockDb().entities.LeaderboardState.set({
        id: 'current',
        currentEpochNumber: 8n,
        isActive: false,
      });
      if (entry.epoch) mockDb = mockDb.entities.LeaderboardEpoch.set(entry.epoch);
      const timestamp = 300 + index;
      const event = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
        user: KEEPER_PHASE.unifiedUser,
        timestamp: BigInt(timestamp),
        ...createEventDataFactory()(KEEPER_TEST_BLOCK + 400 + index, timestamp, ADDRESSES.keeper),
      });
      await assert.rejects(
        () => TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event, mockDb }),
        /closed LeaderboardEpoch/,
        entry.label
      );
      assert.equal(
        mockDb.entities.UserEpochFinalization.get(`${KEEPER_PHASE.unifiedUser}:8`),
        undefined,
        `${entry.label}: no certificate exists before successful settlement`
      );
    }
  } finally {
  }
});

test('keeper user settled: pure-VP user still settles for a closed past epoch (gate does not drop VP decay tail)', async () => {
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
  }
});

test('voting power synced joins capped nft and vp multipliers additively', async () => {
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
    multiplierBps: 50000n, // clamped to MAX_VP_MULTIPLIER (5x)
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
  // Additive join of the two capped categories: nft 5x + vp 5x => 9x (90000).
  assert.equal(updated?.combinedMultiplier, 90000n);
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

  const record = mockDb.entities.LeaderboardKeeperLPBalanceSynced.get(
    `${syncedMeta.mockEventData.transaction.hash}-${syncedMeta.mockEventData.logIndex}`
  );
  assert.ok(record);

  const clearedMeta = eventData(22, 110, ADDRESSES.keeper);
  const cleared = TestHelpers.LeaderboardKeeper.LPBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    pool,
    liquidity: 0n,
    timestamp: 110n,
    ...clearedMeta,
  });
  mockDb = await TestHelpers.LeaderboardKeeper.LPBalanceSynced.processEvent({
    event: cleared,
    mockDb,
  });

  assert.ok(
    mockDb.entities.LeaderboardKeeperLPBalanceSynced.get(
      `${clearedMeta.mockEventData.transaction.hash}-${clearedMeta.mockEventData.logIndex}`
    )
  );

  const unknownPool = '0x000000000000000000000000000000000000b099';
  const unknownMeta = eventData(23, 120, ADDRESSES.keeper);
  const unknown = TestHelpers.LeaderboardKeeper.LPBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    pool: unknownPool,
    liquidity: 1n,
    timestamp: 120n,
    ...unknownMeta,
  });
  mockDb = await TestHelpers.LeaderboardKeeper.LPBalanceSynced.processEvent({
    event: unknown,
    mockDb,
  });
  assert.ok(
    mockDb.entities.LeaderboardKeeperLPBalanceSynced.get(
      `${unknownMeta.mockEventData.transaction.hash}-${unknownMeta.mockEventData.logIndex}`
    )
  );
});
