import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TestHelpers, getRegisteredEventHandler, entityStores } from './v3-test-helpers';

import {
  BALANCER_AUTORANGE_V3_POOL_ADDRESS,
  BOOTSTRAP_CONFIG,
  BOOTSTRAP_LP_POOL_CONFIGS,
  LEADERBOARD_START_BLOCK,
  LP_BALANCER_AUTORANGE_CUTOVER_BLOCK,
  LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  LP_V2_CUTOVER_BLOCK,
  LP_V2_CUTOVER_TIMESTAMP,
  LP_V2_RESUME_CUTOVER_BLOCK,
  LP_V2_RESUME_CUTOVER_TIMESTAMP,
  USDC_ADDRESS,
} from '../helpers/constants';
import { installViemMock } from './viem-mock';

import type { MockDb } from './v3-test-helpers';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
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

const TASK6_TOKEN0 = '0x000000000000000000000000000000000000e001';
const TASK6_TOKEN1 = '0x000000000000000000000000000000000000e002';
const TASK6_V2_POOL = '0x86dbf00485871c901c5129bd525348db96c2eb2d';
const TASK6_LEGACY_POOL = BOOTSTRAP_LP_POOL_CONFIGS[0].pool.toLowerCase();
const TASK6_BALANCER_POOL = BALANCER_AUTORANGE_V3_POOL_ADDRESS.toLowerCase();
const TASK6_Q128 = 1n << 128n;
const TASK6_PRICE_E8 = 100_000_000n;
const TASK6_POINTS_SCALE = 10n ** 18n;

const TASK6_STATIC_TRANSITIONS = [
  {
    id: 'legacy-v3-to-v2',
    outgoingPool: TASK6_LEGACY_POOL,
    incomingPool: TASK6_V2_POOL,
    blockNumber: BigInt(LP_V2_CUTOVER_BLOCK),
    timestamp: LP_V2_CUTOVER_TIMESTAMP,
  },
  {
    id: 'v2-to-balancer-autorange',
    outgoingPool: TASK6_V2_POOL,
    incomingPool: TASK6_BALANCER_POOL,
    blockNumber: BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK),
    timestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  },
  {
    id: 'balancer-to-v2-resume',
    outgoingPool: TASK6_BALANCER_POOL,
    incomingPool: TASK6_V2_POOL,
    blockNumber: BigInt(LP_V2_RESUME_CUTOVER_BLOCK),
    timestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
  },
] as const;

function referenceTask6FungibleGrowth(rateBps: bigint, seconds: number): bigint {
  const poolValueE8 = 2_000n * TASK6_PRICE_E8;
  const totalSupply = 1_000n * 10n ** 6n;
  return ((poolValueE8 * TASK6_Q128) / totalSupply) * rateBps * BigInt(seconds);
}

function referenceTask6FungiblePoints(liquidity: bigint, growthX128: bigint): bigint {
  return (
    (liquidity * growthX128 * TASK6_POINTS_SCALE) /
    (TASK6_Q128 * TASK6_PRICE_E8 * 10_000n * 86_400n)
  );
}

function seedTask6FungiblePool(
  mockDb: MockDb,
  input: {
    pool: string;
    user: string;
    rateBps: bigint;
    startTimestamp?: number;
    active?: boolean;
    fakePositionCount?: number;
  }
): MockDb {
  const startTimestamp = input.startTimestamp ?? 100;
  const active = input.active ?? true;
  const positionId = `v2:${input.pool}:${input.user}`;
  let next = mockDb;
  next = next.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  next = next.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: startTimestamp,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: startTimestamp,
    scheduledEndTime: 0,
  });
  const registry = next.entities.LPPoolRegistry.get('global');
  const poolIds = registry?.poolIds.includes(input.pool)
    ? registry.poolIds
    : [...(registry?.poolIds ?? []), input.pool];
  next = next.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds,
    lastUpdate: startTimestamp,
  });
  next = next.entities.LPPoolConfig.set({
    id: input.pool,
    pool: input.pool,
    positionManager: input.pool,
    token0: TASK6_TOKEN0,
    token1: TASK6_TOKEN1,
    fee: 3000,
    lpRateBps: input.rateBps,
    isActive: active,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: startTimestamp,
    disabledAtEpoch: active ? undefined : 1n,
    disabledAtTimestamp: active ? undefined : startTimestamp,
    lastUpdate: startTimestamp,
  });
  for (const token of [TASK6_TOKEN0, TASK6_TOKEN1]) {
    next = next.entities.TokenInfo.set({
      id: token,
      address: token,
      decimals: 6,
      symbol: 'LP',
      name: 'LP token',
      lastUpdate: startTimestamp,
    });
  }
  next = next.entities.LPPoolState.set({
    id: input.pool,
    pool: input.pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: TASK6_PRICE_E8,
    token1Price: TASK6_PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: startTimestamp,
  });
  next = next.entities.LPPoolV2State.set({
    id: input.pool,
    pool: input.pool,
    reserve0: 1_000n * 10n ** 6n,
    reserve1: 1_000n * 10n ** 6n,
    lpTotalSupply: 1_000n * 10n ** 6n,
    lastUpdate: startTimestamp,
  });
  next = next.entities.LPPoolEpochGrowth.set({
    id: `${input.pool}:1`,
    pool: input.pool,
    epochNumber: 1n,
    startTimestamp,
    lastTimestamp: startTimestamp,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: startTimestamp,
  });
  next = next.entities.UserLPPosition.set({
    id: positionId,
    tokenId: BigInt(input.user),
    user_id: input.user,
    pool: input.pool,
    positionManager: input.pool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 100n * 10n ** 6n,
    amount0: 100n * 10n ** 6n,
    amount1: 100n * 10n ** 6n,
    isInRange: true,
    valueUsd: 200n * TASK6_PRICE_E8,
    lastInRangeTimestamp: startTimestamp,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: startTimestamp,
    settledLpPoints: 0n,
    createdAt: startTimestamp,
    lastUpdate: startTimestamp,
  });
  next = next.entities.UserLPPositionIndex.set({
    id: input.user,
    user_id: input.user,
    positionIds: [positionId],
    lastUpdate: startTimestamp,
  });
  const fakeIds = Array.from(
    { length: input.fakePositionCount ?? 0 },
    (_, index) => `fake:${input.pool}:${index}`
  );
  next = next.entities.LPPoolPositionIndex.set({
    id: input.pool,
    pool: input.pool,
    positionIds: [positionId, ...fakeIds],
    lastUpdate: startTimestamp,
  });
  return next;
}

function seedTask6RetiredLegacyConfig(mockDb: MockDb, timestamp: number): MockDb {
  const legacy = BOOTSTRAP_LP_POOL_CONFIGS[0];
  const registry = mockDb.entities.LPPoolRegistry.get('global');
  let next = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: registry?.poolIds.includes(TASK6_LEGACY_POOL)
      ? registry.poolIds
      : [TASK6_LEGACY_POOL, ...(registry?.poolIds ?? [])],
    lastUpdate: timestamp,
  });
  next = next.entities.LPPoolConfig.set({
    id: TASK6_LEGACY_POOL,
    pool: TASK6_LEGACY_POOL,
    positionManager: legacy.positionManager.toLowerCase(),
    token0: legacy.token0.toLowerCase(),
    token1: legacy.token1.toLowerCase(),
    fee: legacy.fee,
    lpRateBps: legacy.lpRateBps,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP - 1_000,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    lastUpdate: timestamp,
  });
  return next;
}

function seedTask6RetiredBalancerConfig(mockDb: MockDb, timestamp: number): MockDb {
  const pool = BALANCER_AUTORANGE_V3_POOL_ADDRESS.toLowerCase();
  const registry = mockDb.entities.LPPoolRegistry.get('global');
  let next = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: registry?.poolIds.includes(pool)
      ? registry.poolIds
      : [...(registry?.poolIds ?? []), pool],
    lastUpdate: timestamp,
  });
  next = next.entities.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager: pool,
    token0: TASK6_TOKEN0,
    token1: TASK6_TOKEN1,
    fee: 10_000,
    lpRateBps: 2_000n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
    lastUpdate: timestamp,
  });
  return next;
}

function seedTask6PausedV2Config(mockDb: MockDb, timestamp: number): MockDb {
  const registry = mockDb.entities.LPPoolRegistry.get('global');
  let next = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: registry?.poolIds.includes(TASK6_V2_POOL)
      ? registry.poolIds
      : [...(registry?.poolIds ?? []), TASK6_V2_POOL],
    lastUpdate: timestamp,
  });
  next = next.entities.LPPoolConfig.set({
    id: TASK6_V2_POOL,
    pool: TASK6_V2_POOL,
    positionManager: TASK6_V2_POOL,
    token0: TASK6_TOKEN0,
    token1: TASK6_TOKEN1,
    fee: 3_000,
    lpRateBps: 2_000n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    lastUpdate: timestamp,
  });
  return next;
}

function createTask6InstrumentedContext(mockDb: MockDb, isPreload: boolean) {
  const rows = new Map(
    Array.from(entityStores(mockDb), ([entityName, storeRows]) => [entityName, new Map(storeRows)])
  );
  const getCounts = new Map<string, number>();
  const setCounts = new Map<string, number>();
  const operations: string[] = [];
  const stores = new Map<
    string,
    {
      get: (id: string) => Promise<unknown>;
      getWhere: () => Promise<unknown[]>;
      set: (row: { id: string }) => void;
      deleteUnsafe: (id: string) => void;
    }
  >();
  const storeFor = (entityName: string) => ({
    async get(id: string) {
      getCounts.set(entityName, (getCounts.get(entityName) ?? 0) + 1);
      return rows.get(entityName)?.get(id);
    },
    async getWhere() {
      getCounts.set(entityName, (getCounts.get(entityName) ?? 0) + 1);
      return Array.from(rows.get(entityName)?.values() ?? []);
    },
    set(row: { id: string }) {
      setCounts.set(entityName, (setCounts.get(entityName) ?? 0) + 1);
      operations.push(`set:${entityName}:${row.id}`);
      if (isPreload) return;
      let entityRows = rows.get(entityName);
      if (!entityRows) {
        entityRows = new Map();
        rows.set(entityName, entityRows);
      }
      entityRows.set(row.id, row);
    },
    deleteUnsafe(id: string) {
      if (!isPreload) rows.get(entityName)?.delete(id);
    },
  });
  const context = new Proxy({ isPreload, log: { debug() {} } } as Record<string, unknown>, {
    get(target, property: string) {
      if (property in target) return target[property];
      let store = stores.get(property);
      if (!store) {
        store = storeFor(property);
        stores.set(property, store);
      }
      return store;
    },
  });
  return { context, rows, getCounts, setCounts, operations };
}

function countRecord(counts: Map<string, number>): Record<string, number> {
  return Object.fromEntries(
    [...counts.entries()].sort(([left], [right]) => left.localeCompare(right))
  );
}

async function runTask6RegisteredHandler(
  contractName: string,
  eventName: string,
  event: Parameters<Awaited<ReturnType<typeof getRegisteredEventHandler>>>[0]['event'],
  mockDb: MockDb
) {
  const handler = await getRegisteredEventHandler(contractName, eventName);
  const preload = createTask6InstrumentedContext(mockDb, true);
  await handler({ event, context: preload.context });
  const ordered = createTask6InstrumentedContext(mockDb, false);
  await handler({ event, context: ordered.context });
  return { preload, ordered };
}

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

test('a first config event initializes from the bootstrap rates when bootstrap is enabled', async () => {
  // This file pins bootstrap OFF at module load; the initializer's bootstrap arm reads the
  // switch at call time, so flip it for exactly this case.
  const previous = process.env.ENVIO_DISABLE_BOOTSTRAP;
  process.env.ENVIO_DISABLE_BOOTSTRAP = 'false';
  try {
    const TestHelpers = loadTestHelpers();
    const eventData = createEventDataFactory();
    const depositRate = TestHelpers.LeaderboardConfig.DepositRateUpdated.createMockEvent({
      oldRate: 0n,
      newRate: 500n,
      timestamp: 210n,
      ...eventData(3, 210, ADDRESSES.config),
    });
    const mockDb = await TestHelpers.LeaderboardConfig.DepositRateUpdated.processEvent({
      event: depositRate,
      mockDb: TestHelpers.MockDb.createMockDb(),
    });

    const config = mockDb.entities.LeaderboardConfig.get('global');
    assert.ok(config);
    // The event's own field is applied over the bootstrap image; every other field is the
    // bootstrap value rather than the zero a disabled bootstrap would leave.
    assert.equal(config?.depositRateBps, 500n);
    assert.equal(config?.borrowRateBps, BOOTSTRAP_CONFIG.borrowRateBps);
    assert.equal(config?.vpRateBps, BOOTSTRAP_CONFIG.vpRateBps);
    assert.equal(config?.lpRateBps, BOOTSTRAP_CONFIG.lpRateBps);
    assert.equal(config?.supplyDailyBonus, BOOTSTRAP_CONFIG.supplyDailyBonus);
    assert.equal(config?.borrowDailyBonus, BOOTSTRAP_CONFIG.borrowDailyBonus);
    assert.equal(config?.repayDailyBonus, BOOTSTRAP_CONFIG.repayDailyBonus);
    assert.equal(config?.withdrawDailyBonus, BOOTSTRAP_CONFIG.withdrawDailyBonus);
    assert.equal(config?.cooldownSeconds, BOOTSTRAP_CONFIG.cooldownSeconds);
    assert.equal(config?.minDailyBonusUsd, BOOTSTRAP_CONFIG.minDailyBonusUsd);
    assert.equal(config?.lastUpdate, 210);
  } finally {
    if (previous === undefined) delete process.env.ENVIO_DISABLE_BOOTSTRAP;
    else process.env.ENVIO_DISABLE_BOOTSTRAP = previous;
  }
});

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
  mockDb = mockDb.entities.VotingPowerTier.set({
    id: '0',
    tierIndex: 0n,
    minVotingPower: 0n,
    multiplierBps: 50000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
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

test('Tide end freezes growth with position-count-invariant growth work, sweeping holders once', async () => {
  const poolA = '0x000000000000000000000000000000000000e020';
  const poolB = '0x000000000000000000000000000000000000e021';

  const run = async (positionCount: number) => {
    let mockDb = TestHelpers.MockDb.createMockDb();
    mockDb = seedTask6FungiblePool(mockDb, {
      pool: poolA,
      user: ADDRESSES.user,
      rateBps: 2000n,
      fakePositionCount: positionCount - 1,
    });
    mockDb = seedTask6FungiblePool(mockDb, {
      pool: poolB,
      user: ADDRESSES.userTwo,
      rateBps: 2000n,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      ...mockDb.entities.LeaderboardEpoch.get('1')!,
      scheduledEndTime: 200,
    });
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
      lastUpdate: 100,
    });
    mockDb = mockDb.entities.LPPoolConfig.set({
      ...mockDb.entities.LPPoolConfig.get(poolB)!,
      token0: USDC_ADDRESS,
      token1: TASK6_TOKEN1,
    });
    mockDb = mockDb.entities.TokenInfo.set({
      id: USDC_ADDRESS,
      address: USDC_ADDRESS,
      decimals: 6,
      symbol: 'USDC',
      name: 'USDC',
      lastUpdate: 100,
    });
    mockDb = mockDb.entities.LPPoolState.set({
      ...mockDb.entities.LPPoolState.get(poolB)!,
      currentTick: 0,
      sqrtPriceX96: 1n << 96n,
    });
    const eventData = createEventDataFactory();
    const event = TestHelpers.LeaderboardConfig.DepositRateUpdated.createMockEvent({
      oldRate: 0n,
      newRate: 1n,
      timestamp: 200n,
      ...eventData(30, 200, ADDRESSES.config),
    });
    return await runTask6RegisteredHandler(
      'LeaderboardConfig',
      'DepositRateUpdated',
      event,
      mockDb
    );
  };

  const one = await run(1);
  const tenThousand = await run(10_000);

  // The Tide boundary now sweeps LP holders once so a holder no event ever touched still
  // scores (FINDING 003). That sweep resolves each indexed position id, so UserLPPosition
  // reads scale with position count BY DESIGN - once per Tide (~29.5 days), never per
  // market event. Everything else must stay position-count invariant; market-event
  // handlers remain strictly invariant and are pinned separately.
  const withoutSweptPositions = (counts: Map<string, number>) => {
    const filtered = new Map(counts);
    filtered.delete('UserLPPosition');
    return countRecord(filtered);
  };
  assert.deepEqual(
    withoutSweptPositions(tenThousand.preload.getCounts),
    withoutSweptPositions(one.preload.getCounts)
  );
  assert.deepEqual(
    withoutSweptPositions(tenThousand.ordered.getCounts),
    withoutSweptPositions(one.ordered.getCounts)
  );
  assert.deepEqual(
    withoutSweptPositions(tenThousand.preload.setCounts),
    withoutSweptPositions(one.preload.setCounts)
  );
  assert.deepEqual(
    withoutSweptPositions(tenThousand.ordered.setCounts),
    withoutSweptPositions(one.ordered.setCounts)
  );
  assert.ok(
    (tenThousand.ordered.getCounts.get('UserLPPosition') ?? 0) >
      (one.ordered.getCounts.get('UserLPPosition') ?? 0),
    'the Tide-close sweep must resolve every indexed position'
  );

  const orderedRows = tenThousand.ordered.rows;
  for (const pool of [poolA, poolB]) {
    const growth = orderedRows.get('LPPoolEpochGrowth')?.get(`${pool}:1`) as
      | { frozenAt?: number; lastTimestamp: number; isFrozen: boolean }
      | undefined;
    assert.equal(growth?.lastTimestamp, 200);
    assert.equal(growth?.frozenAt, 200);
    assert.equal(growth?.isFrozen, true);
  }
  assert.equal(
    (orderedRows.get('LeaderboardState')?.get('current') as { isActive: boolean } | undefined)
      ?.isActive,
    false
  );
  // Tide close now settles every LP holder (FINDING 003), so both seeded holders must have
  // epoch stats written. Previously this asserted zero, which is exactly the behavior that
  // left never-touched LP-only holders unscored in closed Tides.
  assert.equal(orderedRows.get('UserEpochStats')?.size ?? 0, 2);
});

test('lp pool config handlers register pools and rates', async () => {
  const previousExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEnableEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
  process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
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
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: 1n,
      startTime: 100,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 100,
      scheduledEndTime: 0,
    });

    const pool = '0x000000000000000000000000000000000000c010';
    const unrelatedPool = '0x000000000000000000000000000000000000c014';
    const manager = '0x000000000000000000000000000000000000c011';
    const token0 = '0x000000000000000000000000000000000000c012';
    const token1 = '0x000000000000000000000000000000000000c013';
    const globalConfigBefore = {
      id: 'global',
      depositRateBps: 1n,
      borrowRateBps: 2n,
      vpRateBps: 3n,
      lpRateBps: 777n,
      supplyDailyBonus: 4,
      borrowDailyBonus: 5,
      repayDailyBonus: 6,
      withdrawDailyBonus: 7,
      cooldownSeconds: 8,
      minDailyBonusUsd: 9,
      lastUpdate: 99,
    } as const;
    mockDb = mockDb.entities.LeaderboardConfig.set(globalConfigBefore);

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
    assert.equal(poolConfig?.fee, undefined);
    assert.equal(poolConfig?.lpRateBps, 2000n);

    const registry = mockDb.entities.LPPoolRegistry.get('global');
    assert.ok(registry?.poolIds.includes(pool.toLowerCase()));

    const state = mockDb.entities.LPPoolState.get(pool.toLowerCase());
    assert.ok(state);

    mockDb = mockDb.entities.LPPoolConfig.set({
      ...poolConfig!,
      id: unrelatedPool,
      pool: unrelatedPool,
      lpRateBps: 900n,
    });
    mockDb = mockDb.entities.LPPoolRegistry.set({
      id: 'global',
      poolIds: [pool.toLowerCase(), unrelatedPool],
      lastUpdate: 100,
    });
    mockDb = mockDb.entities.UserLPPosition.set({
      id: '1',
      tokenId: 1n,
      user_id: ADDRESSES.user,
      pool: pool.toLowerCase(),
      positionManager: manager,
      tickLower: -100,
      tickUpper: 100,
      liquidity: 100n,
      amount0: 1n,
      amount1: 1n,
      isInRange: true,
      valueUsd: 100_000_000n,
      lastInRangeTimestamp: 100,
      accumulatedInRangeSeconds: 0n,
      lastSettledAt: 100,
      settledLpPoints: 0n,
      createdAt: 100,
      lastUpdate: 100,
    });
    mockDb = mockDb.entities.UserLPPosition.set({
      id: '2',
      tokenId: 2n,
      user_id: ADDRESSES.userTwo,
      pool: unrelatedPool,
      positionManager: manager,
      tickLower: -100,
      tickUpper: 100,
      liquidity: 100n,
      amount0: 1n,
      amount1: 1n,
      isInRange: true,
      valueUsd: 100_000_000n,
      lastInRangeTimestamp: 100,
      accumulatedInRangeSeconds: 7n,
      lastSettledAt: 101,
      settledLpPoints: 11n,
      createdAt: 100,
      lastUpdate: 101,
    });
    mockDb = mockDb.entities.UserLPPositionIndex.set({
      id: ADDRESSES.user,
      user_id: ADDRESSES.user,
      positionIds: ['1'],
      lastUpdate: 100,
    });
    mockDb = mockDb.entities.UserLPPositionIndex.set({
      id: ADDRESSES.userTwo,
      user_id: ADDRESSES.userTwo,
      positionIds: ['2'],
      lastUpdate: 101,
    });
    mockDb = mockDb.entities.LPPoolPositionIndex.set({
      id: pool.toLowerCase(),
      pool: pool.toLowerCase(),
      positionIds: ['1'],
      lastUpdate: 100,
    });
    mockDb = mockDb.entities.LPPoolPositionIndex.set({
      id: unrelatedPool,
      pool: unrelatedPool,
      positionIds: ['2'],
      lastUpdate: 101,
    });

    const rateUpdated = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
      pool,
      oldRate: 2000n,
      newRate: 1500n,
      timestamp: 210n,
      ...eventData(31, 210, ADDRESSES.config),
    });
    mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
      event: rateUpdated,
      mockDb,
    });

    const settledPosition = mockDb.entities.UserLPPosition.get('1');
    assert.equal(settledPosition?.lastSettledAt, 100);
    assert.equal(settledPosition?.settledLpPoints, 0n);
    assert.equal(mockDb.entities.LPPoolConfig.get(pool.toLowerCase())?.lpRateBps, 1500n);
    assert.equal(mockDb.entities.LPPoolConfig.get(unrelatedPool)?.lpRateBps, 900n);
    assert.deepEqual(mockDb.entities.LeaderboardConfig.get('global'), globalConfigBefore);
    const unrelatedPosition = mockDb.entities.UserLPPosition.get('2');
    assert.equal(unrelatedPosition?.lastSettledAt, 101);
    assert.equal(unrelatedPosition?.accumulatedInRangeSeconds, 7n);
    assert.equal(unrelatedPosition?.settledLpPoints, 11n);
    assert.equal(mockDb.entities.UserEpochStats.get(`${ADDRESSES.userTwo}:1`), undefined);

    const disabled = TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
      pool,
      timestamp: 220n,
      ...eventData(32, 220, ADDRESSES.config),
    });
    mockDb = await TestHelpers.LeaderboardConfig.LPPoolDisabled.processEvent({
      event: disabled,
      mockDb,
    });

    const disabledConfig = mockDb.entities.LPPoolConfig.get(pool.toLowerCase());
    assert.equal(disabledConfig?.isActive, false);
  } finally {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = previousExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEnableEth;
  }
});

test('lp pool config contract register supports v2 pools', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });

  const v2Pool = '0x000000000000000000000000000000000000c020';
  const configured = TestHelpers.LeaderboardConfig.LPPoolConfigured.createMockEvent({
    pool: v2Pool,
    positionManager: v2Pool,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    lpRateBps: 900n,
    timestamp: 260n,
    ...eventData(33, 260, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolConfigured.processEvent({
    event: configured,
    mockDb,
  });

  const poolConfig = mockDb.entities.LPPoolConfig.get(v2Pool);
  assert.ok(poolConfig);
  assert.equal(poolConfig?.pool, v2Pool);
  assert.equal(poolConfig?.positionManager, v2Pool);
  assert.equal(poolConfig?.isActive, true);
});

test('registered LPPoolConfigured keeps the known V2 pool inactive before its era', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const timestamp = LP_V2_CUTOVER_TIMESTAMP - 100;

  const configured = TestHelpers.LeaderboardConfig.LPPoolConfigured.createMockEvent({
    pool: TASK6_V2_POOL,
    positionManager: TASK6_V2_POOL,
    token0: TASK6_TOKEN0,
    token1: TASK6_TOKEN1,
    lpRateBps: 2000n,
    timestamp: BigInt(timestamp),
    ...eventData(LP_V2_CUTOVER_BLOCK - 1, timestamp, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolConfigured.processEvent({
    event: configured,
    mockDb,
  });

  const poolConfig = mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL);
  assert.equal(poolConfig?.isActive, false);
  assert.equal(poolConfig?.enabledAtTimestamp, LP_V2_CUTOVER_TIMESTAMP);
  assert.equal(poolConfig?.disabledAtTimestamp, undefined);
});

test('registered LPPoolConfigured keeps the known Balancer pool inactive before its era', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const timestamp = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 100;

  const configured = TestHelpers.LeaderboardConfig.LPPoolConfigured.createMockEvent({
    pool: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
    positionManager: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
    token0: TASK6_TOKEN0,
    token1: TASK6_TOKEN1,
    lpRateBps: 2000n,
    timestamp: BigInt(timestamp),
    ...eventData(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK - 1, timestamp, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolConfigured.processEvent({
    event: configured,
    mockDb,
  });

  const poolConfig = mockDb.entities.LPPoolConfig.get(BALANCER_AUTORANGE_V3_POOL_ADDRESS);
  assert.equal(poolConfig?.isActive, false);
  assert.equal(poolConfig?.enabledAtTimestamp, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
  assert.equal(poolConfig?.disabledAtTimestamp, undefined);
});

test('registered LPPoolConfigured follows every static era and preserves arbitrary pools', async () => {
  const cases = [
    {
      name: 'legacy active era',
      pool: TASK6_LEGACY_POOL,
      timestamp: LP_V2_CUTOVER_TIMESTAMP - 1,
      block: LP_V2_CUTOVER_BLOCK - 1,
      active: true,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP - 1,
      disabledAt: undefined,
    },
    {
      name: 'legacy retired era',
      pool: TASK6_LEGACY_POOL,
      timestamp: LP_V2_CUTOVER_TIMESTAMP,
      block: LP_V2_CUTOVER_BLOCK,
      active: false,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
      disabledAt: LP_V2_CUTOVER_TIMESTAMP,
    },
    {
      name: 'V2 original era',
      pool: TASK6_V2_POOL,
      timestamp: LP_V2_CUTOVER_TIMESTAMP,
      block: LP_V2_CUTOVER_BLOCK,
      active: true,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
      disabledAt: undefined,
    },
    {
      name: 'V2 pause',
      pool: TASK6_V2_POOL,
      timestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 1,
      block: LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1,
      active: false,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
      disabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    },
    {
      name: 'Balancer era',
      pool: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
      timestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      block: LP_BALANCER_AUTORANGE_CUTOVER_BLOCK,
      active: true,
      enabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      disabledAt: undefined,
    },
    {
      name: 'Balancer after resume',
      pool: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
      timestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP + 1,
      block: LP_V2_RESUME_CUTOVER_BLOCK + 1,
      active: false,
      enabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      disabledAt: LP_V2_RESUME_CUTOVER_TIMESTAMP,
    },
    {
      name: 'V2 resumed era',
      pool: TASK6_V2_POOL,
      timestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
      block: LP_V2_RESUME_CUTOVER_BLOCK,
      active: true,
      enabledAt: LP_V2_RESUME_CUTOVER_TIMESTAMP,
      disabledAt: undefined,
    },
    {
      name: 'arbitrary dynamic pool',
      pool: '0x000000000000000000000000000000000000e099',
      timestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP + 1,
      block: LP_V2_RESUME_CUTOVER_BLOCK + 1,
      active: true,
      enabledAt: LP_V2_RESUME_CUTOVER_TIMESTAMP + 1,
      disabledAt: undefined,
    },
  ] as const;

  for (const entry of cases) {
    const TestHelpers = loadTestHelpers();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();
    const configured = TestHelpers.LeaderboardConfig.LPPoolConfigured.createMockEvent({
      pool: entry.pool,
      positionManager: entry.pool,
      token0: TASK6_TOKEN0,
      token1: TASK6_TOKEN1,
      lpRateBps: 2000n,
      timestamp: BigInt(entry.timestamp),
      ...eventData(entry.block, entry.timestamp, ADDRESSES.config),
    });
    mockDb = await TestHelpers.LeaderboardConfig.LPPoolConfigured.processEvent({
      event: configured,
      mockDb,
    });

    const poolConfig = mockDb.entities.LPPoolConfig.get(entry.pool);
    assert.equal(poolConfig?.isActive, entry.active, entry.name);
    assert.equal(poolConfig?.enabledAtTimestamp, entry.enabledAt, entry.name);
    assert.equal(poolConfig?.disabledAtTimestamp, entry.disabledAt, entry.name);
  }
});

test('registered inactive static-era rate updates are activity-neutral across the authority matrix', async () => {
  const balancerPool = BALANCER_AUTORANGE_V3_POOL_ADDRESS.toLowerCase();
  const cases = [
    {
      name: 'V2 before first entry',
      pool: TASK6_V2_POOL,
      block: LP_V2_CUTOVER_BLOCK - 1,
      timestamp: LP_V2_CUTOVER_TIMESTAMP - 100,
      seed(TestHelpers: ReturnType<typeof loadTestHelpers>) {
        let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
          pool: TASK6_V2_POOL,
          user: ADDRESSES.user,
          rateBps: 2_000n,
          startTimestamp: LP_V2_CUTOVER_TIMESTAMP - 1_000,
          active: false,
        });
        return mockDb.entities.LPPoolConfig.set({
          ...mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL)!,
          enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
          disabledAtEpoch: undefined,
          disabledAtTimestamp: undefined,
        });
      },
    },
    {
      name: 'V2 Balancer pause',
      pool: TASK6_V2_POOL,
      block: LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1,
      timestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 100,
      seed(TestHelpers: ReturnType<typeof loadTestHelpers>) {
        let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
          pool: TASK6_V2_POOL,
          user: ADDRESSES.user,
          rateBps: 2_000n,
          startTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
          active: false,
        });
        mockDb = seedTask6FungiblePool(mockDb, {
          pool: balancerPool,
          user: ADDRESSES.userTwo,
          rateBps: 2_000n,
          startTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
        });
        mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
        return seedTask6PausedV2Config(mockDb, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
      },
    },
    {
      name: 'Balancer before entry',
      pool: balancerPool,
      block: LP_V2_CUTOVER_BLOCK + 20,
      timestamp: LP_V2_CUTOVER_TIMESTAMP + 300,
      seed(TestHelpers: ReturnType<typeof loadTestHelpers>) {
        let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
          pool: TASK6_V2_POOL,
          user: ADDRESSES.user,
          rateBps: 2_000n,
          startTimestamp: LP_V2_CUTOVER_TIMESTAMP,
        });
        mockDb = seedTask6FungiblePool(mockDb, {
          pool: balancerPool,
          user: ADDRESSES.userTwo,
          rateBps: 2_000n,
          startTimestamp: LP_V2_CUTOVER_TIMESTAMP,
          active: false,
        });
        mockDb = mockDb.entities.LPPoolConfig.set({
          ...mockDb.entities.LPPoolConfig.get(balancerPool)!,
          enabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
          disabledAtEpoch: undefined,
          disabledAtTimestamp: undefined,
        });
        return seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
      },
    },
    {
      name: 'Balancer retired',
      pool: balancerPool,
      block: LP_V2_RESUME_CUTOVER_BLOCK + 1,
      timestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP + 100,
      seed(TestHelpers: ReturnType<typeof loadTestHelpers>) {
        let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
          pool: balancerPool,
          user: ADDRESSES.userTwo,
          rateBps: 2_000n,
          startTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
          active: false,
        });
        mockDb = seedTask6FungiblePool(mockDb, {
          pool: TASK6_V2_POOL,
          user: ADDRESSES.user,
          rateBps: 2_000n,
          startTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
        });
        mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
        return seedTask6RetiredBalancerConfig(mockDb, LP_V2_RESUME_CUTOVER_TIMESTAMP);
      },
    },
    {
      name: 'legacy retired',
      pool: TASK6_LEGACY_POOL,
      block: LP_V2_CUTOVER_BLOCK + 20,
      timestamp: LP_V2_CUTOVER_TIMESTAMP + 300,
      seed(TestHelpers: ReturnType<typeof loadTestHelpers>) {
        let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
          pool: TASK6_V2_POOL,
          user: ADDRESSES.user,
          rateBps: 2_000n,
          startTimestamp: LP_V2_CUTOVER_TIMESTAMP,
        });
        return seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
      },
    },
  ] as const;

  for (const entry of cases) {
    const TestHelpers = loadTestHelpers();
    const eventData = createEventDataFactory();
    let mockDb = entry.seed(TestHelpers);
    const configBefore = mockDb.entities.LPPoolConfig.get(entry.pool);
    const growthBefore = mockDb.entities.LPPoolEpochGrowth.get(`${entry.pool}:1`);
    const positionsBefore = mockDb.entities.UserLPPosition.getAll();
    assert.equal(configBefore?.isActive, false, entry.name);

    const rateUpdated = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
      pool: entry.pool,
      oldRate: 2_000n,
      newRate: 1_500n,
      timestamp: BigInt(entry.timestamp),
      ...eventData(entry.block, entry.timestamp, ADDRESSES.config),
    });
    mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
      event: rateUpdated,
      mockDb,
    });

    const configAfter = mockDb.entities.LPPoolConfig.get(entry.pool);
    assert.equal(configAfter?.isActive, false, entry.name);
    assert.equal(configAfter?.disabledAtTimestamp, configBefore?.disabledAtTimestamp, entry.name);
    assert.equal(configAfter?.lpRateBps, 1_500n, entry.name);
    assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${entry.pool}:1`), growthBefore);
    assert.deepEqual(mockDb.entities.UserLPPosition.getAll(), positionsBefore);

    mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
      event: rateUpdated,
      mockDb,
    });
    assert.deepEqual(mockDb.entities.LPPoolConfig.get(entry.pool), configAfter, entry.name);
    assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${entry.pool}:1`), growthBefore);
  }
});

test('LPRateUpdated splits lazy fungible growth at the old pool-local rate', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const pool = '0x000000000000000000000000000000000000e010';
  const unrelatedPool = '0x000000000000000000000000000000000000e011';
  const liquidity = 100n * 10n ** 6n;
  mockDb = seedTask6FungiblePool(mockDb, {
    pool,
    user: ADDRESSES.user,
    rateBps: 2000n,
  });
  mockDb = seedTask6FungiblePool(mockDb, {
    pool: unrelatedPool,
    user: ADDRESSES.userTwo,
    rateBps: 900n,
  });
  const globalConfigBefore = {
    id: 'global',
    depositRateBps: 1n,
    borrowRateBps: 2n,
    vpRateBps: 0n,
    lpRateBps: 777n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 100,
  } as const;
  mockDb = mockDb.entities.LeaderboardConfig.set(globalConfigBefore);
  const unrelatedConfigBefore = mockDb.entities.LPPoolConfig.get(unrelatedPool);
  const unrelatedGrowthBefore = mockDb.entities.LPPoolEpochGrowth.get(`${unrelatedPool}:1`);
  const targetPositionId = `v2:${pool}:${ADDRESSES.user}`;
  const targetPositionBefore = mockDb.entities.UserLPPosition.get(targetPositionId);

  const rateUpdated = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool,
    oldRate: 2000n,
    newRate: 1500n,
    timestamp: 200n,
    ...eventData(30, 200, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: rateUpdated,
    mockDb,
  });

  const firstSegmentGrowth = referenceTask6FungibleGrowth(2000n, 100);
  assert.equal(
    mockDb.entities.LPPoolEpochGrowth.get(`${pool}:1`)?.scalarGrowthX128,
    firstSegmentGrowth
  );
  assert.deepEqual(mockDb.entities.UserLPPosition.get(targetPositionId), targetPositionBefore);
  assert.equal(mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`), undefined);
  assert.deepEqual(mockDb.entities.LPPoolConfig.get(unrelatedPool), unrelatedConfigBefore);
  assert.deepEqual(
    mockDb.entities.LPPoolEpochGrowth.get(`${unrelatedPool}:1`),
    unrelatedGrowthBefore
  );
  assert.deepEqual(mockDb.entities.LeaderboardConfig.get('global'), globalConfigBefore);

  const sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 1_000n * 10n ** 6n,
    reserve1: 1_000n * 10n ** 6n,
    ...eventData(31, 300, pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({ event: sync, mockDb });

  const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: 300n,
    ...eventData(LEADERBOARD_START_BLOCK + 1, 300, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: settle,
    mockDb,
  });

  const totalGrowth = firstSegmentGrowth + referenceTask6FungibleGrowth(1500n, 100);
  assert.equal(mockDb.entities.LPPoolEpochGrowth.get(`${pool}:1`)?.scalarGrowthX128, totalGrowth);
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`)?.lpPoints,
    referenceTask6FungiblePoints(liquidity, totalGrowth)
  );
  assert.deepEqual(mockDb.entities.LPPoolConfig.get(unrelatedPool), unrelatedConfigBefore);
  assert.deepEqual(
    mockDb.entities.LPPoolEpochGrowth.get(`${unrelatedPool}:1`),
    unrelatedGrowthBefore
  );
  assert.deepEqual(mockDb.entities.LeaderboardConfig.get('global'), globalConfigBefore);
});

test('rate and disable handlers have position-cardinality-invariant store work', async () => {
  const pool = '0x000000000000000000000000000000000000e012';
  const run = async (eventName: 'LPRateUpdated' | 'LPPoolDisabled', positionCount: number) => {
    let mockDb = TestHelpers.MockDb.createMockDb();
    mockDb = seedTask6FungiblePool(mockDb, {
      pool,
      user: ADDRESSES.user,
      rateBps: 2000n,
      fakePositionCount: positionCount - 1,
    });
    const eventData = createEventDataFactory();
    const event =
      eventName === 'LPRateUpdated'
        ? TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
            pool,
            oldRate: 2000n,
            newRate: 1500n,
            timestamp: 200n,
            ...eventData(30, 200, ADDRESSES.config),
          })
        : TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
            pool,
            timestamp: 200n,
            ...eventData(30, 200, ADDRESSES.config),
          });
    return await runTask6RegisteredHandler('LeaderboardConfig', eventName, event, mockDb);
  };

  for (const eventName of ['LPRateUpdated', 'LPPoolDisabled'] as const) {
    const one = await run(eventName, 1);
    const tenThousand = await run(eventName, 10_000);
    assert.deepEqual(
      countRecord(tenThousand.preload.getCounts),
      countRecord(one.preload.getCounts),
      `${eventName} preload gets`
    );
    assert.deepEqual(
      countRecord(tenThousand.ordered.getCounts),
      countRecord(one.ordered.getCounts),
      `${eventName} ordered gets`
    );
    assert.deepEqual(
      countRecord(tenThousand.preload.setCounts),
      countRecord(one.preload.setCounts),
      `${eventName} preload sets`
    );
    assert.deepEqual(
      countRecord(tenThousand.ordered.setCounts),
      countRecord(one.ordered.setCounts),
      `${eventName} ordered sets`
    );
    for (const phase of [tenThousand.preload, tenThousand.ordered]) {
      for (const entity of [
        'LPPoolPositionIndex',
        'UserLPPositionIndex',
        'UserLPPosition',
        'UserLPStats',
        'UserEpochStats',
      ]) {
        assert.equal(phase.getCounts.get(entity) ?? 0, 0, `${eventName} ${entity} reads`);
        assert.equal(phase.setCounts.get(entity) ?? 0, 0, `${eventName} ${entity} writes`);
      }
    }
  }
});

test('lp pool config keeps previous fee when eth calls are disabled', async () => {
  const previousExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEnableEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
  process.env.ENVIO_ENABLE_ETH_CALLS = 'false';

  try {
    const TestHelpers = loadTestHelpers();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();
    const pool = '0x000000000000000000000000000000000000c021';

    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 3n,
      isActive: true,
    });
    mockDb = mockDb.entities.LPPoolConfig.set({
      id: pool,
      pool,
      positionManager: ADDRESSES.positionManager,
      token0: ADDRESSES.token0,
      token1: ADDRESSES.token1,
      fee: 1234,
      lpRateBps: 100n,
      isActive: true,
      enabledAtEpoch: 1n,
      enabledAtTimestamp: 0,
      disabledAtEpoch: undefined,
      disabledAtTimestamp: undefined,
      lastUpdate: 0,
    });

    const configured = TestHelpers.LeaderboardConfig.LPPoolConfigured.createMockEvent({
      pool,
      positionManager: ADDRESSES.positionManager,
      token0: ADDRESSES.token0,
      token1: ADDRESSES.token1,
      lpRateBps: 500n,
      timestamp: 270n,
      ...eventData(34, 270, ADDRESSES.config),
    });
    mockDb = await TestHelpers.LeaderboardConfig.LPPoolConfigured.processEvent({
      event: configured,
      mockDb,
    });

    const updated = mockDb.entities.LPPoolConfig.get(pool);
    assert.ok(updated);
    assert.equal(updated?.fee, 1234);
    assert.equal(updated?.enabledAtEpoch, 3n);
  } finally {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = previousExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEnableEth;
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
  assert.equal(config?.borrowRateBps, 0n);
  assert.equal(config?.vpRateBps, 0n);
  assert.equal(config?.lpRateBps, 0n);
  assert.equal(config?.supplyDailyBonus, 0);
  assert.equal(config?.borrowDailyBonus, 0);
  assert.equal(config?.repayDailyBonus, 0);
  assert.equal(config?.withdrawDailyBonus, 0);
  assert.equal(config?.cooldownSeconds, 0);
  assert.equal(config?.minDailyBonusUsd, 0);
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
  mockDb = mockDb.entities.UserIndex.set({
    id: ADDRESSES.user,
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
  mockDb = mockDb.entities.ScoreBucket.set({
    id: 'b:1',
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
  mockDb = mockDb.entities.LeaderboardTotals.set({
    id: 'global',
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
    lastVPAccrualTimestamp: 0,
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
  assert.equal(mockDb.entities.UserIndex.get(ADDRESSES.user), undefined);
  assert.equal(mockDb.entities.ScoreBucket.get('b:1')?.count, 0);
  assert.equal(mockDb.entities.LeaderboardTotals.get('global')?.totalUsers, 0);
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

  const duplicateTierAdded = TestHelpers.VotingPowerMultiplier.TierAdded.createMockEvent({
    tierIndex: 1n,
    minVotingPower: 100n,
    multiplierBps: 15000n,
    ...eventData(20, 405, ADDRESSES.vpMultiplier),
  });
  mockDb = await TestHelpers.VotingPowerMultiplier.TierAdded.processEvent({
    event: duplicateTierAdded,
    mockDb,
  });
  assert.deepEqual(mockDb.entities.VotingPowerTierIndex.get('current')?.activeTierIds, ['1']);
  assert.equal(mockDb.entities.VotingPowerTierIndex.get('current')?.lastUpdate, 405);

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
  assert.equal(epoch?.scheduledStartTime, 200);
});

test('epoch start replaces a stale schedule on an inactive ordinary epoch', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '3',
    epochNumber: 3n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 50,
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

  assert.equal(mockDb.entities.LeaderboardEpoch.get('3')?.scheduledStartTime, 200);
});

test('epoch start preserves the schedule of an active ordinary epoch', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '3',
    epochNumber: 3n,
    startBlock: 10n,
    startTime: 50,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 50,
    scheduledEndTime: 0,
  });

  const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 3n,
    startTime: 200n,
    ...eventData(20, 200, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStart,
    mockDb,
  });

  assert.equal(mockDb.entities.LeaderboardEpoch.get('3')?.scheduledStartTime, 50);
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
    // LeaderboardConfig starts after the legacy LP pool. Reflect the registry
    // state that exists at this real chain height so the assertion isolates
    // the already-registered `pool` branch.
    poolIds: [BOOTSTRAP_LP_POOL_CONFIGS[0].pool.toLowerCase(), pool],
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

test('LPPoolDisabled advances only pool growth and leaves indexed holders untouched', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const pool = '0x000000000000000000000000000000000000d011';
  const token0 = '0x000000000000000000000000000000000000d012';
  const token1 = '0x000000000000000000000000000000000000d013';
  const positionId = `v2:${pool}:${ADDRESSES.user}`;

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
    scheduledStartTime: 100,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [pool],
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager: pool,
    token0,
    token1,
    fee: 3000,
    lpRateBps: 2000n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 100,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 100,
  });
  for (const token of [token0, token1]) {
    mockDb = mockDb.entities.TokenInfo.set({
      id: token,
      address: token,
      decimals: 6,
      symbol: 'LP',
      name: 'LP token',
      lastUpdate: 100,
    });
  }
  mockDb = mockDb.entities.LPPoolState.set({
    id: pool,
    pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: pool,
    pool,
    reserve0: 1_000n * 10n ** 6n,
    reserve1: 1_000n * 10n ** 6n,
    lpTotalSupply: 1_000n * 10n ** 6n,
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.LPPoolEpochGrowth.set({
    id: `${pool}:1`,
    pool,
    epochNumber: 1n,
    startTimestamp: 100,
    lastTimestamp: 100,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: positionId,
    tokenId: 1n,
    user_id: ADDRESSES.user,
    pool,
    positionManager: pool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 100n * 10n ** 6n,
    amount0: 100n * 10n ** 6n,
    amount1: 100n * 10n ** 6n,
    isInRange: true,
    valueUsd: 200n * 100_000_000n,
    lastInRangeTimestamp: 100,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 100,
    settledLpPoints: 0n,
    createdAt: 100,
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: pool,
    pool,
    positionIds: [positionId],
    lastUpdate: 100,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    positionIds: [positionId],
    lastUpdate: 100,
  });
  const positionBefore = mockDb.entities.UserLPPosition.get(positionId);

  const disabled = TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
    pool,
    timestamp: 200n,
    ...eventData(26, 200, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolDisabled.processEvent({
    event: disabled,
    mockDb,
  });

  const growth = mockDb.entities.LPPoolEpochGrowth.get(`${pool}:1`);
  assert.equal(growth?.lastTimestamp, 200);
  assert.ok((growth?.scalarGrowthX128 ?? 0n) > 0n);
  assert.deepEqual(mockDb.entities.UserLPPosition.get(positionId), positionBefore);
  assert.equal(mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`), undefined);

  const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: 300n,
    ...eventData(LEADERBOARD_START_BLOCK + 2, 300, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: settle, mockDb });
  const expectedGrowth = referenceTask6FungibleGrowth(2000n, 100);
  const expectedPoints = referenceTask6FungiblePoints(100n * 10n ** 6n, expectedGrowth);
  assert.equal(mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`)?.lpPoints, expectedPoints);
  assert.equal(mockDb.entities.LPPoolEpochGrowth.get(`${pool}:1`)?.lastTimestamp, 200);

  const repeat = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: 400n,
    ...eventData(LEADERBOARD_START_BLOCK + 3, 400, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: repeat, mockDb });
  assert.equal(mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`)?.lpPoints, expectedPoints);
  assert.equal(mockDb.entities.LPPoolEpochGrowth.get(`${pool}:1`)?.lastTimestamp, 200);
});

test('registered V2 disable in its original era survives the next ordinary event and remains claimable', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  const disableTimestamp = LP_V2_CUTOVER_TIMESTAMP + 100;
  const nextTimestamp = LP_V2_CUTOVER_TIMESTAMP + 200;
  const positionId = `v2:${TASK6_V2_POOL}:${ADDRESSES.user}`;
  let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
    pool: TASK6_V2_POOL,
    user: ADDRESSES.user,
    rateBps: 2_000n,
    startTimestamp: LP_V2_CUTOVER_TIMESTAMP,
  });
  mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
  const holderBefore = {
    position: mockDb.entities.UserLPPosition.get(positionId),
    poolIndex: mockDb.entities.LPPoolPositionIndex.get(TASK6_V2_POOL),
    userIndex: mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user),
  };

  const disableData = eventData(LP_V2_CUTOVER_BLOCK + 10, disableTimestamp, ADDRESSES.config);
  const disabled = TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
    pool: TASK6_V2_POOL,
    timestamp: BigInt(disableTimestamp),
    ...disableData,
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolDisabled.processEvent({
    event: disabled,
    mockDb,
  });

  const disabledConfig = mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL);
  const disabledGrowth = mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`);
  const protocolAfterDisable = mockDb.entities.ProtocolStats.get('global');
  const markerAfterDisable = mockDb.entities.LPStaticTransition.get('legacy-v3-to-v2');
  assert.ok(markerAfterDisable);
  assert.equal(disabledConfig?.isActive, false);
  assert.equal(disabledConfig?.disabledAtTimestamp, disableTimestamp);
  assert.equal(disabledGrowth?.lastTimestamp, disableTimestamp);
  assert.equal(disabledGrowth?.scalarGrowthX128, referenceTask6FungibleGrowth(2_000n, 100));

  const nextData = eventData(LP_V2_CUTOVER_BLOCK + 11, nextTimestamp, ADDRESSES.config);
  nextData.mockEventData.transaction.hash = disableData.mockEventData.transaction.hash;
  const rateUpdated = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool: TASK6_V2_POOL,
    oldRate: 2_000n,
    newRate: 1_500n,
    timestamp: BigInt(nextTimestamp),
    ...nextData,
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: rateUpdated,
    mockDb,
  });

  const afterNextEvent = mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL);
  assert.equal(afterNextEvent?.isActive, false);
  assert.equal(afterNextEvent?.disabledAtTimestamp, disableTimestamp);
  assert.equal(afterNextEvent?.lpRateBps, 1_500n);
  assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`), disabledGrowth);
  assert.deepEqual(mockDb.entities.ProtocolStats.get('global'), protocolAfterDisable);
  assert.deepEqual(mockDb.entities.LPStaticTransition.get('legacy-v3-to-v2'), markerAfterDisable);
  assert.deepEqual(mockDb.entities.UserLPPosition.get(positionId), holderBefore.position);
  assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(TASK6_V2_POOL), holderBefore.poolIndex);
  assert.deepEqual(mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user), holderBefore.userIndex);

  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: rateUpdated,
    mockDb,
  });
  assert.deepEqual(mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL), afterNextEvent);
  assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`), disabledGrowth);

  const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(nextTimestamp + 100),
    ...eventData(LP_V2_CUTOVER_BLOCK + 12, nextTimestamp + 100, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: settle, mockDb });
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`)?.lpPoints,
    referenceTask6FungiblePoints(100n * 10n ** 6n, referenceTask6FungibleGrowth(2_000n, 100))
  );
  assert.equal(
    mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`)?.lastTimestamp,
    disableTimestamp
  );
});

test('registered V2 disable after resume survives the next ordinary event and remains claimable', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  const disableTimestamp = LP_V2_RESUME_CUTOVER_TIMESTAMP + 100;
  const nextTimestamp = LP_V2_RESUME_CUTOVER_TIMESTAMP + 200;
  const positionId = `v2:${TASK6_V2_POOL}:${ADDRESSES.user}`;
  let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
    pool: TASK6_V2_POOL,
    user: ADDRESSES.user,
    rateBps: 2_000n,
    startTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
  });
  mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
  mockDb = seedTask6RetiredBalancerConfig(mockDb, LP_V2_RESUME_CUTOVER_TIMESTAMP);
  const holderBefore = {
    position: mockDb.entities.UserLPPosition.get(positionId),
    poolIndex: mockDb.entities.LPPoolPositionIndex.get(TASK6_V2_POOL),
    userIndex: mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user),
  };

  const disableData = eventData(
    LP_V2_RESUME_CUTOVER_BLOCK + 10,
    disableTimestamp,
    ADDRESSES.config
  );
  const disabled = TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
    pool: TASK6_V2_POOL,
    timestamp: BigInt(disableTimestamp),
    ...disableData,
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolDisabled.processEvent({
    event: disabled,
    mockDb,
  });

  const disabledConfig = mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL);
  const disabledGrowth = mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`);
  const protocolAfterDisable = mockDb.entities.ProtocolStats.get('global');
  const markersAfterDisable = [
    mockDb.entities.LPStaticTransition.get('legacy-v3-to-v2'),
    mockDb.entities.LPStaticTransition.get('v2-to-balancer-autorange'),
    mockDb.entities.LPStaticTransition.get('balancer-to-v2-resume'),
  ];
  assert.ok(markersAfterDisable.every(Boolean));
  assert.equal(disabledConfig?.isActive, false);
  assert.equal(disabledConfig?.disabledAtTimestamp, disableTimestamp);
  assert.equal(disabledGrowth?.lastTimestamp, disableTimestamp);
  assert.equal(disabledGrowth?.scalarGrowthX128, referenceTask6FungibleGrowth(2_000n, 100));

  const nextData = eventData(LP_V2_RESUME_CUTOVER_BLOCK + 11, nextTimestamp, ADDRESSES.config);
  nextData.mockEventData.transaction.hash = disableData.mockEventData.transaction.hash;
  const rateUpdated = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool: TASK6_V2_POOL,
    oldRate: 2_000n,
    newRate: 1_500n,
    timestamp: BigInt(nextTimestamp),
    ...nextData,
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: rateUpdated,
    mockDb,
  });

  const afterNextEvent = mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL);
  assert.equal(afterNextEvent?.isActive, false);
  assert.equal(afterNextEvent?.disabledAtTimestamp, disableTimestamp);
  assert.equal(afterNextEvent?.lpRateBps, 1_500n);
  assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`), disabledGrowth);
  assert.deepEqual(mockDb.entities.ProtocolStats.get('global'), protocolAfterDisable);
  assert.deepEqual(
    [
      mockDb.entities.LPStaticTransition.get('legacy-v3-to-v2'),
      mockDb.entities.LPStaticTransition.get('v2-to-balancer-autorange'),
      mockDb.entities.LPStaticTransition.get('balancer-to-v2-resume'),
    ],
    markersAfterDisable
  );
  assert.deepEqual(mockDb.entities.UserLPPosition.get(positionId), holderBefore.position);
  assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(TASK6_V2_POOL), holderBefore.poolIndex);
  assert.deepEqual(mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user), holderBefore.userIndex);

  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: rateUpdated,
    mockDb,
  });
  assert.deepEqual(mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL), afterNextEvent);
  assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`), disabledGrowth);

  const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(nextTimestamp + 100),
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 12, nextTimestamp + 100, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: settle, mockDb });
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`)?.lpPoints,
    referenceTask6FungiblePoints(100n * 10n ** 6n, referenceTask6FungibleGrowth(2_000n, 100))
  );
  assert.equal(
    mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`)?.lastTimestamp,
    disableTimestamp
  );
});

test('registered Balancer disable in its active era survives the next ordinary event and remains claimable', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  const pool = BALANCER_AUTORANGE_V3_POOL_ADDRESS.toLowerCase();
  const disableTimestamp = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 100;
  const nextTimestamp = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 200;
  const positionId = `v2:${pool}:${ADDRESSES.user}`;
  let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
    pool,
    user: ADDRESSES.user,
    rateBps: 2_000n,
    startTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  });
  mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
  mockDb = seedTask6PausedV2Config(mockDb, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
  const holderBefore = {
    position: mockDb.entities.UserLPPosition.get(positionId),
    poolIndex: mockDb.entities.LPPoolPositionIndex.get(pool),
    userIndex: mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user),
  };

  const disableData = eventData(
    LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 10,
    disableTimestamp,
    ADDRESSES.config
  );
  const disabled = TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
    pool,
    timestamp: BigInt(disableTimestamp),
    ...disableData,
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolDisabled.processEvent({
    event: disabled,
    mockDb,
  });

  const disabledConfig = mockDb.entities.LPPoolConfig.get(pool);
  const disabledGrowth = mockDb.entities.LPPoolEpochGrowth.get(`${pool}:1`);
  const protocolAfterDisable = mockDb.entities.ProtocolStats.get('global');
  const markersAfterDisable = [
    mockDb.entities.LPStaticTransition.get('legacy-v3-to-v2'),
    mockDb.entities.LPStaticTransition.get('v2-to-balancer-autorange'),
  ];
  assert.ok(markersAfterDisable.every(Boolean));
  assert.equal(disabledConfig?.isActive, false);
  assert.equal(disabledConfig?.disabledAtTimestamp, disableTimestamp);
  assert.equal(disabledGrowth?.lastTimestamp, disableTimestamp);
  assert.equal(disabledGrowth?.scalarGrowthX128, referenceTask6FungibleGrowth(2_000n, 100));

  const nextData = eventData(
    LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 11,
    nextTimestamp,
    ADDRESSES.config
  );
  nextData.mockEventData.transaction.hash = disableData.mockEventData.transaction.hash;
  const rateUpdated = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool,
    oldRate: 2_000n,
    newRate: 1_500n,
    timestamp: BigInt(nextTimestamp),
    ...nextData,
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: rateUpdated,
    mockDb,
  });

  const afterNextEvent = mockDb.entities.LPPoolConfig.get(pool);
  assert.equal(afterNextEvent?.isActive, false);
  assert.equal(afterNextEvent?.disabledAtTimestamp, disableTimestamp);
  assert.equal(afterNextEvent?.lpRateBps, 1_500n);
  assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${pool}:1`), disabledGrowth);
  assert.deepEqual(mockDb.entities.ProtocolStats.get('global'), protocolAfterDisable);
  assert.deepEqual(
    [
      mockDb.entities.LPStaticTransition.get('legacy-v3-to-v2'),
      mockDb.entities.LPStaticTransition.get('v2-to-balancer-autorange'),
    ],
    markersAfterDisable
  );
  assert.deepEqual(mockDb.entities.UserLPPosition.get(positionId), holderBefore.position);
  assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(pool), holderBefore.poolIndex);
  assert.deepEqual(mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user), holderBefore.userIndex);

  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: rateUpdated,
    mockDb,
  });
  assert.deepEqual(mockDb.entities.LPPoolConfig.get(pool), afterNextEvent);
  assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${pool}:1`), disabledGrowth);

  const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(nextTimestamp + 100),
    ...eventData(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 12, nextTimestamp + 100, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: settle, mockDb });
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`)?.lpPoints,
    referenceTask6FungiblePoints(100n * 10n ** 6n, referenceTask6FungibleGrowth(2_000n, 100))
  );
  assert.equal(mockDb.entities.LPPoolEpochGrowth.get(`${pool}:1`)?.lastTimestamp, disableTimestamp);
});

test('registered retired legacy configuration survives the next ordinary event without reactivation', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  const legacy = BOOTSTRAP_LP_POOL_CONFIGS[0];
  const configTimestamp = LP_V2_CUTOVER_TIMESTAMP + 100;
  const nextTimestamp = LP_V2_CUTOVER_TIMESTAMP + 200;
  const positionId = `v2:${TASK6_V2_POOL}:${ADDRESSES.user}`;
  let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
    pool: TASK6_V2_POOL,
    user: ADDRESSES.user,
    rateBps: 2_000n,
    startTimestamp: LP_V2_CUTOVER_TIMESTAMP,
  });
  mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
  const seededGrowth = mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`);
  assert.ok(seededGrowth);
  mockDb = mockDb.entities.LPPoolEpochGrowth.set({
    ...seededGrowth,
    lastTimestamp: nextTimestamp,
    scalarGrowthX128: referenceTask6FungibleGrowth(2_000n, 200),
    lastUpdate: nextTimestamp,
  });
  const growthBefore = mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`);
  const holderBefore = {
    position: mockDb.entities.UserLPPosition.get(positionId),
    poolIndex: mockDb.entities.LPPoolPositionIndex.get(TASK6_V2_POOL),
    userIndex: mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user),
  };

  const configuredData = eventData(LP_V2_CUTOVER_BLOCK + 10, configTimestamp, ADDRESSES.config);
  const configured = TestHelpers.LeaderboardConfig.LPPoolConfigured.createMockEvent({
    pool: TASK6_LEGACY_POOL,
    positionManager: legacy.positionManager,
    token0: legacy.token0,
    token1: legacy.token1,
    lpRateBps: legacy.lpRateBps,
    timestamp: BigInt(configTimestamp),
    ...configuredData,
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolConfigured.processEvent({
    event: configured,
    mockDb,
  });
  const protocolAfterConfigured = mockDb.entities.ProtocolStats.get('global');
  const markerAfterConfigured = mockDb.entities.LPStaticTransition.get('legacy-v3-to-v2');
  assert.ok(markerAfterConfigured);

  const unknownPool = '0x000000000000000000000000000000000000e098';
  const nextData = eventData(LP_V2_CUTOVER_BLOCK + 11, nextTimestamp, ADDRESSES.config);
  nextData.mockEventData.transaction.hash = configuredData.mockEventData.transaction.hash;
  const ordinaryNextEvent = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool: unknownPool,
    oldRate: 0n,
    newRate: 1n,
    timestamp: BigInt(nextTimestamp),
    ...nextData,
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: ordinaryNextEvent,
    mockDb,
  });

  const retiredConfig = mockDb.entities.LPPoolConfig.get(TASK6_LEGACY_POOL);
  assert.equal(retiredConfig?.isActive, false);
  assert.equal(retiredConfig?.disabledAtTimestamp, LP_V2_CUTOVER_TIMESTAMP);
  assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`), growthBefore);
  assert.deepEqual(mockDb.entities.ProtocolStats.get('global'), protocolAfterConfigured);
  assert.deepEqual(
    mockDb.entities.LPStaticTransition.get('legacy-v3-to-v2'),
    markerAfterConfigured
  );
  assert.deepEqual(mockDb.entities.UserLPPosition.get(positionId), holderBefore.position);
  assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(TASK6_V2_POOL), holderBefore.poolIndex);
  assert.deepEqual(mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user), holderBefore.userIndex);

  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: ordinaryNextEvent,
    mockDb,
  });
  assert.deepEqual(mockDb.entities.LPPoolConfig.get(TASK6_LEGACY_POOL), retiredConfig);
  assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`), growthBefore);

  const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(nextTimestamp),
    ...eventData(LP_V2_CUTOVER_BLOCK + 12, nextTimestamp, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: settle, mockDb });
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`)?.lpPoints,
    referenceTask6FungiblePoints(100n * 10n ** 6n, referenceTask6FungibleGrowth(2_000n, 200))
  );
  assert.deepEqual(mockDb.entities.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`), growthBefore);
});

test('registered repeated static-pool disables preserve the original inactive provenance', async () => {
  const cases = [
    {
      name: 'retired legacy',
      pool: TASK6_LEGACY_POOL,
      block: LP_V2_CUTOVER_BLOCK + 20,
      timestamp: LP_V2_CUTOVER_TIMESTAMP + 300,
      expectedMarker: 'legacy-v3-to-v2',
      seed(TestHelpers: ReturnType<typeof loadTestHelpers>) {
        let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
          pool: TASK6_V2_POOL,
          user: ADDRESSES.user,
          rateBps: 2_000n,
          startTimestamp: LP_V2_CUTOVER_TIMESTAMP,
        });
        return seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
      },
    },
    {
      name: 'paused V2',
      pool: TASK6_V2_POOL,
      block: LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 20,
      timestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 300,
      expectedMarker: 'v2-to-balancer-autorange',
      seed(TestHelpers: ReturnType<typeof loadTestHelpers>) {
        const balancerPool = BALANCER_AUTORANGE_V3_POOL_ADDRESS.toLowerCase();
        let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
          pool: balancerPool,
          user: ADDRESSES.user,
          rateBps: 2_000n,
          startTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
        });
        mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
        return seedTask6PausedV2Config(mockDb, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
      },
    },
    {
      name: 'retired Balancer',
      pool: BALANCER_AUTORANGE_V3_POOL_ADDRESS.toLowerCase(),
      block: LP_V2_RESUME_CUTOVER_BLOCK + 20,
      timestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP + 300,
      expectedMarker: 'balancer-to-v2-resume',
      seed(TestHelpers: ReturnType<typeof loadTestHelpers>) {
        let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
          pool: TASK6_V2_POOL,
          user: ADDRESSES.user,
          rateBps: 2_000n,
          startTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
        });
        mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
        return seedTask6RetiredBalancerConfig(mockDb, LP_V2_RESUME_CUTOVER_TIMESTAMP);
      },
    },
  ] as const;

  for (const entry of cases) {
    const TestHelpers = loadTestHelpers();
    const eventData = createEventDataFactory();
    let mockDb = entry.seed(TestHelpers);
    const configBefore = mockDb.entities.LPPoolConfig.get(entry.pool);
    assert.ok(configBefore, entry.name);
    const disabled = TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
      pool: entry.pool,
      timestamp: BigInt(entry.timestamp),
      ...eventData(entry.block, entry.timestamp, ADDRESSES.config),
    });
    mockDb = await TestHelpers.LeaderboardConfig.LPPoolDisabled.processEvent({
      event: disabled,
      mockDb,
    });

    assert.deepEqual(mockDb.entities.LPPoolConfig.get(entry.pool), configBefore, entry.name);
    assert.ok(mockDb.entities.LPStaticTransition.get(entry.expectedMarker), entry.name);
  }
});

test('registered static transition markers are preload-safe, ordered after boundary state, and replay-idempotent', async () => {
  const TestHelpers = loadTestHelpers();
  const legacy = BOOTSTRAP_LP_POOL_CONFIGS[0];
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL],
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP - 100,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: TASK6_LEGACY_POOL,
    pool: TASK6_LEGACY_POOL,
    positionManager: legacy.positionManager.toLowerCase(),
    token0: legacy.token0.toLowerCase(),
    token1: legacy.token1.toLowerCase(),
    fee: legacy.fee,
    lpRateBps: legacy.lpRateBps,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP - 100,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP - 100,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: TASK6_LEGACY_POOL,
    pool: TASK6_LEGACY_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP - 100,
  });

  const eventData = createEventDataFactory();
  const event = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool: '0x000000000000000000000000000000000000e097',
    oldRate: 0n,
    newRate: 1n,
    timestamp: BigInt(LP_V2_CUTOVER_TIMESTAMP),
    ...eventData(LP_V2_CUTOVER_BLOCK, LP_V2_CUTOVER_TIMESTAMP, ADDRESSES.config),
  });
  const handler = await getRegisteredEventHandler('LeaderboardConfig', 'LPRateUpdated');
  const preload = createTask6InstrumentedContext(mockDb, true);
  await handler({ event, context: preload.context });
  assert.equal(preload.rows.get('LPStaticTransition')?.get('legacy-v3-to-v2'), undefined);

  const ordered = createTask6InstrumentedContext(mockDb, false);
  await handler({ event, context: ordered.context });
  assert.deepEqual(ordered.rows.get('LPStaticTransition')?.get('legacy-v3-to-v2'), {
    id: 'legacy-v3-to-v2',
    outgoingPool: TASK6_LEGACY_POOL,
    incomingPool: TASK6_V2_POOL,
    blockNumber: BigInt(LP_V2_CUTOVER_BLOCK),
    timestamp: LP_V2_CUTOVER_TIMESTAMP,
  });
  const outgoingWrite = ordered.operations.findIndex(
    operation => operation === `set:LPPoolConfig:${TASK6_LEGACY_POOL}`
  );
  const incomingWrite = ordered.operations.findIndex(
    operation => operation === `set:LPPoolConfig:${TASK6_V2_POOL}`
  );
  const markerWrite = ordered.operations.findIndex(
    operation => operation === 'set:LPStaticTransition:legacy-v3-to-v2'
  );
  assert.ok(outgoingWrite >= 0);
  assert.ok(incomingWrite > outgoingWrite);
  assert.ok(markerWrite > incomingWrite);

  const boundaryStateAfterFirst = {
    legacy: ordered.rows.get('LPPoolConfig')?.get(TASK6_LEGACY_POOL),
    v2: ordered.rows.get('LPPoolConfig')?.get(TASK6_V2_POOL),
    marker: ordered.rows.get('LPStaticTransition')?.get('legacy-v3-to-v2'),
  };
  const markerWriteCount = ordered.operations.filter(
    operation => operation === 'set:LPStaticTransition:legacy-v3-to-v2'
  ).length;
  await handler({ event, context: ordered.context });
  assert.deepEqual(
    {
      legacy: ordered.rows.get('LPPoolConfig')?.get(TASK6_LEGACY_POOL),
      v2: ordered.rows.get('LPPoolConfig')?.get(TASK6_V2_POOL),
      marker: ordered.rows.get('LPStaticTransition')?.get('legacy-v3-to-v2'),
    },
    boundaryStateAfterFirst
  );
  assert.equal(
    ordered.operations.filter(operation => operation === 'set:LPStaticTransition:legacy-v3-to-v2')
      .length,
    markerWriteCount
  );
});

test('registered chronology rejects every malformed static transition marker before preload or ordered writes', async t => {
  const corruptions = [
    {
      field: 'outgoingPool',
      mutate: (row: (typeof TASK6_STATIC_TRANSITIONS)[number]) => ({
        ...row,
        outgoingPool: '0x000000000000000000000000000000000000dead',
      }),
    },
    {
      field: 'incomingPool',
      mutate: (row: (typeof TASK6_STATIC_TRANSITIONS)[number]) => ({
        ...row,
        incomingPool: '0x000000000000000000000000000000000000beef',
      }),
    },
    {
      field: 'blockNumber',
      mutate: (row: (typeof TASK6_STATIC_TRANSITIONS)[number]) => ({
        ...row,
        blockNumber: row.blockNumber + 1n,
      }),
    },
    {
      field: 'timestamp',
      mutate: (row: (typeof TASK6_STATIC_TRANSITIONS)[number]) => ({
        ...row,
        timestamp: row.timestamp + 1,
      }),
    },
  ] as const;
  const handler = await getRegisteredEventHandler('LeaderboardConfig', 'LPRateUpdated');

  for (const transition of TASK6_STATIC_TRANSITIONS) {
    for (const corruption of corruptions) {
      for (const isPreload of [true, false]) {
        await t.test(
          `${transition.id} ${corruption.field} ${isPreload ? 'preload' : 'ordered'}`,
          async () => {
            const TestHelpers = loadTestHelpers();
            let mockDb = TestHelpers.MockDb.createMockDb();
            mockDb = mockDb.entities.LPStaticTransition.set(corruption.mutate(transition));
            const eventData = createEventDataFactory();
            const event = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
              pool: '0x000000000000000000000000000000000000e099',
              oldRate: 0n,
              newRate: 1n,
              timestamp: BigInt(LP_V2_RESUME_CUTOVER_TIMESTAMP + 100),
              ...eventData(
                LP_V2_RESUME_CUTOVER_BLOCK + 1,
                LP_V2_RESUME_CUTOVER_TIMESTAMP + 100,
                ADDRESSES.config
              ),
            });
            const run = createTask6InstrumentedContext(mockDb, isPreload);
            const rowsBefore = new Map(
              Array.from(run.rows, ([entityName, rows]) => [entityName, new Map(rows)])
            );

            await assert.rejects(
              handler({ event, context: run.context }),
              new RegExp(`invalid LP static transition marker.*${transition.id}`)
            );
            assert.deepEqual(run.rows, rowsBefore);
            assert.deepEqual(countRecord(run.setCounts), {});
            assert.deepEqual(run.operations, []);
          }
        );
      }
    }
  }
});

test('registered chronology backfills canonical transition rows once and replays them idempotently', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
    pool: TASK6_V2_POOL,
    user: ADDRESSES.user,
    rateBps: 2_000n,
    startTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
  });
  mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
  mockDb = seedTask6RetiredBalancerConfig(mockDb, LP_V2_RESUME_CUTOVER_TIMESTAMP);
  const handler = await getRegisteredEventHandler('LeaderboardConfig', 'LPRateUpdated');
  const eventData = createEventDataFactory();
  const event = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool: '0x000000000000000000000000000000000000e099',
    oldRate: 0n,
    newRate: 1n,
    timestamp: BigInt(LP_V2_RESUME_CUTOVER_TIMESTAMP + 100),
    ...eventData(
      LP_V2_RESUME_CUTOVER_BLOCK + 1,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 100,
      ADDRESSES.config
    ),
  });
  const ordered = createTask6InstrumentedContext(mockDb, false);
  const configsBefore = new Map(ordered.rows.get('LPPoolConfig'));

  await handler({ event, context: ordered.context });

  assert.deepEqual(ordered.rows.get('LPPoolConfig'), configsBefore);
  for (const transition of TASK6_STATIC_TRANSITIONS) {
    assert.deepEqual(ordered.rows.get('LPStaticTransition')?.get(transition.id), transition);
  }
  const markerOperations = () =>
    ordered.operations.filter(operation => operation.startsWith('set:LPStaticTransition:'));
  assert.deepEqual(
    markerOperations(),
    TASK6_STATIC_TRANSITIONS.map(transition => `set:LPStaticTransition:${transition.id}`)
  );

  const rowsAfterBackfill = new Map(ordered.rows.get('LPStaticTransition'));
  await handler({ event, context: ordered.context });
  assert.deepEqual(ordered.rows.get('LPStaticTransition'), rowsAfterBackfill);
  assert.equal(markerOperations().length, TASK6_STATIC_TRANSITIONS.length);
  assert.deepEqual(ordered.rows.get('LPPoolConfig'), configsBefore);
});

test('registered chronology treats marker address casing as normalized identity without rewriting rows', async t => {
  const mixedCaseAddress = (address: string) => `0x${address.slice(2).toUpperCase()}`;

  for (const isPreload of [true, false]) {
    await t.test(isPreload ? 'preload' : 'ordered', async () => {
      const TestHelpers = loadTestHelpers();
      let mockDb = seedTask6FungiblePool(TestHelpers.MockDb.createMockDb(), {
        pool: TASK6_V2_POOL,
        user: ADDRESSES.user,
        rateBps: 2_000n,
        startTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
      });
      mockDb = seedTask6RetiredLegacyConfig(mockDb, LP_V2_CUTOVER_TIMESTAMP);
      mockDb = seedTask6RetiredBalancerConfig(mockDb, LP_V2_RESUME_CUTOVER_TIMESTAMP);
      for (const transition of TASK6_STATIC_TRANSITIONS) {
        mockDb = mockDb.entities.LPStaticTransition.set({
          ...transition,
          outgoingPool: mixedCaseAddress(transition.outgoingPool),
          incomingPool: mixedCaseAddress(transition.incomingPool),
        });
      }
      const eventData = createEventDataFactory();
      const event = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
        pool: '0x000000000000000000000000000000000000e099',
        oldRate: 0n,
        newRate: 1n,
        timestamp: BigInt(LP_V2_RESUME_CUTOVER_TIMESTAMP + 100),
        ...eventData(
          LP_V2_RESUME_CUTOVER_BLOCK + 1,
          LP_V2_RESUME_CUTOVER_TIMESTAMP + 100,
          ADDRESSES.config
        ),
      });
      const handler = await getRegisteredEventHandler('LeaderboardConfig', 'LPRateUpdated');
      const run = createTask6InstrumentedContext(mockDb, isPreload);
      const markersBefore = new Map(run.rows.get('LPStaticTransition'));
      const configsBefore = new Map(run.rows.get('LPPoolConfig'));

      await handler({ event, context: run.context });

      assert.deepEqual(run.rows.get('LPStaticTransition'), markersBefore);
      assert.deepEqual(run.rows.get('LPPoolConfig'), configsBefore);
      assert.equal(run.setCounts.get('LPStaticTransition') ?? 0, 0);
    });
  }
});

test('a canonical later transition row cannot authorize an absent prerequisite boundary', async () => {
  const TestHelpers = loadTestHelpers();
  const legacy = BOOTSTRAP_LP_POOL_CONFIGS[0];
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL],
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP - 100,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: TASK6_LEGACY_POOL,
    pool: TASK6_LEGACY_POOL,
    positionManager: legacy.positionManager.toLowerCase(),
    token0: legacy.token0.toLowerCase(),
    token1: legacy.token1.toLowerCase(),
    fee: legacy.fee,
    lpRateBps: legacy.lpRateBps,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP - 100,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP - 100,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: TASK6_LEGACY_POOL,
    pool: TASK6_LEGACY_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP - 100,
  });
  mockDb = mockDb.entities.LPStaticTransition.set(TASK6_STATIC_TRANSITIONS[1]);
  const eventData = createEventDataFactory();
  const event = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool: '0x000000000000000000000000000000000000e099',
    oldRate: 0n,
    newRate: 1n,
    timestamp: BigInt(LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 100),
    ...eventData(
      LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 100,
      ADDRESSES.config
    ),
  });

  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({ event, mockDb });

  assert.deepEqual(
    mockDb.entities.LPStaticTransition.get(TASK6_STATIC_TRANSITIONS[0].id),
    TASK6_STATIC_TRANSITIONS[0]
  );
  assert.deepEqual(
    mockDb.entities.LPStaticTransition.get(TASK6_STATIC_TRANSITIONS[1].id),
    TASK6_STATIC_TRANSITIONS[1]
  );
  assert.equal(mockDb.entities.LPPoolConfig.get(TASK6_LEGACY_POOL)?.isActive, false);
  assert.equal(mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL)?.isActive, false);
  assert.equal(mockDb.entities.LPPoolConfig.get(TASK6_BALANCER_POOL)?.isActive, true);

  const boundaryState = {
    legacy: mockDb.entities.LPPoolConfig.get(TASK6_LEGACY_POOL),
    v2: mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL),
    balancer: mockDb.entities.LPPoolConfig.get(TASK6_BALANCER_POOL),
    markers: TASK6_STATIC_TRANSITIONS.map(transition =>
      mockDb.entities.LPStaticTransition.get(transition.id)
    ),
  };
  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({ event, mockDb });
  assert.deepEqual(
    {
      legacy: mockDb.entities.LPPoolConfig.get(TASK6_LEGACY_POOL),
      v2: mockDb.entities.LPPoolConfig.get(TASK6_V2_POOL),
      balancer: mockDb.entities.LPPoolConfig.get(TASK6_BALANCER_POOL),
      markers: TASK6_STATIC_TRANSITIONS.map(transition =>
        mockDb.entities.LPStaticTransition.get(transition.id)
      ),
    },
    boundaryState
  );
});

test('unknown LP pool events are ignored and inactive rate updates do not settle', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const unknownPool = '0x000000000000000000000000000000000000d020';

  const disabled = TestHelpers.LeaderboardConfig.LPPoolDisabled.createMockEvent({
    pool: unknownPool,
    timestamp: 700n,
    ...eventData(27, 700, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPPoolDisabled.processEvent({
    event: disabled,
    mockDb,
  });
  assert.equal(mockDb.entities.LPPoolConfig.get(unknownPool), undefined);

  const missingRate = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool: unknownPool,
    oldRate: 100n,
    newRate: 200n,
    timestamp: 710n,
    ...eventData(28, 710, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: missingRate,
    mockDb,
  });
  assert.equal(mockDb.entities.LPPoolConfig.get(unknownPool), undefined);

  const inactivePool = '0x000000000000000000000000000000000000d021';
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: inactivePool,
    pool: inactivePool,
    positionManager: ADDRESSES.positionManager,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 100n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 600,
    lastUpdate: 600,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: 'inactive-position',
    tokenId: 99n,
    user_id: ADDRESSES.user,
    pool: inactivePool,
    positionManager: ADDRESSES.positionManager,
    tickLower: -100,
    tickUpper: 100,
    liquidity: 100n,
    amount0: 1n,
    amount1: 1n,
    isInRange: true,
    valueUsd: 100_000_000n,
    lastInRangeTimestamp: 600,
    accumulatedInRangeSeconds: 7n,
    lastSettledAt: 600,
    settledLpPoints: 11n,
    createdAt: 600,
    lastUpdate: 600,
  });

  const inactiveRate = TestHelpers.LeaderboardConfig.LPRateUpdated.createMockEvent({
    pool: inactivePool,
    oldRate: 100n,
    newRate: 300n,
    timestamp: 720n,
    ...eventData(29, 720, ADDRESSES.config),
  });
  mockDb = await TestHelpers.LeaderboardConfig.LPRateUpdated.processEvent({
    event: inactiveRate,
    mockDb,
  });

  assert.equal(mockDb.entities.LPPoolConfig.get(inactivePool)?.lpRateBps, 300n);
  assert.equal(mockDb.entities.UserLPPosition.get('inactive-position')?.lastSettledAt, 600);
  assert.equal(mockDb.entities.UserLPPosition.get('inactive-position')?.settledLpPoints, 11n);
});

test('epoch dates override replaces the on-chain start and seeds the end', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  // Epoch 9 went on-chain with the intended end (1790442000) in the start field.
  const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 9n,
    startTime: 1790442000n,
    ...eventData(99800000, 1787890000, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStart,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('9');
  assert.equal(epoch?.scheduledStartTime, 1787893200);
  assert.equal(epoch?.scheduledEndTime, 1790442000);
  // Override start is still in the future at this block, so no start block yet.
  assert.equal(epoch?.startBlock, 0n);
});

test('epoch dates override activates the tide once the override start passes', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 8n,
    isActive: false,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '8',
    epochNumber: 8n,
    startBlock: 91385944n,
    startTime: 1785340800,
    endBlock: 99797722n,
    endTime: 1787889600,
    isActive: false,
    duration: 2548800n,
    scheduledStartTime: 1785340800,
    scheduledEndTime: 1787889600,
  });

  const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 9n,
    startTime: 1790442000n,
    ...eventData(99810000, 1787893300, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStart,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('9');
  assert.equal(epoch?.isActive, true);
  assert.equal(epoch?.startTime, 1787893200);
  assert.equal(epoch?.scheduledEndTime, 1790442000);

  const state = mockDb.entities.LeaderboardState.get('current');
  assert.equal(state?.currentEpochNumber, 9n);
  assert.equal(state?.isActive, true);
});

test('epoch dates override ignores a later on-chain end', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '9',
    epochNumber: 9n,
    startBlock: 99810000n,
    startTime: 1787893200,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 1787893200,
    scheduledEndTime: 1790442000,
  });

  // On-chain, epoch 9 can only be ended after its (wrong) chain start passes.
  // That end must not stretch the tide the indexer already scheduled.
  const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 9n,
    endTime: 1790442300n,
    ...eventData(99900000, 1790442100, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEnd,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('9');
  assert.equal(epoch?.scheduledEndTime, 1790442000);
  assert.equal(epoch?.scheduledStartTime, 1787893200);
});

test('epoch dates override keeps its start when a later start event arrives on an active tide', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '9',
    epochNumber: 9n,
    startBlock: 99810000n,
    startTime: 1787893200,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 1790442000,
    scheduledEndTime: 0,
  });

  const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 9n,
    startTime: 1790442000n,
    ...eventData(99820000, 1787894000, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStart,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('9');
  assert.equal(epoch?.scheduledStartTime, 1787893200);
  assert.equal(epoch?.scheduledEndTime, 1790442000);
});

test('epoch end seeds the override start when the epoch entity is missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 9n,
    endTime: 1790442300n,
    ...eventData(99900001, 1790442100, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEnd,
    mockDb,
  });

  const epoch = mockDb.entities.LeaderboardEpoch.get('9');
  assert.equal(epoch?.scheduledStartTime, 1787893200);
  assert.equal(epoch?.scheduledEndTime, 1790442000);
  // The effective start is seeded too, so the epoch this path creates carries
  // a duration rather than an end with no beginning.
  assert.equal(epoch?.startTime, 1787893200);
});

test('ending an overridden epoch on-chain later leaves it alone and lets the next tide start', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  // Tide 9 is live on the indexer with its overridden dates.
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 9n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '9',
    epochNumber: 9n,
    startBlock: 99810000n,
    startTime: 1787893200,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 1787893200,
    scheduledEndTime: 1790442000,
  });

  // On-chain, epoch 9 only becomes endable once its (wrong) chain start passes,
  // so the EpochEnd payload is necessarily later than the overridden end.
  const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 9n,
    endTime: 1790442300n,
    ...eventData(100000000, 1790442100, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEnd,
    mockDb,
  });

  const ended = mockDb.entities.LeaderboardEpoch.get('9');
  assert.equal(ended?.isActive, false);
  assert.equal(ended?.endTime, 1790442000);
  assert.equal(ended?.scheduledEndTime, 1790442000);
  assert.equal(ended?.duration, 2548800n);

  const gap = mockDb.entities.LeaderboardState.get('current');
  assert.equal(gap?.currentEpochNumber, 9n);
  assert.equal(gap?.isActive, false);

  // Tide 10 then starts normally, straight from the chain.
  const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 10n,
    startTime: 1790445600n,
    ...eventData(100000100, 1790445700, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStart,
    mockDb,
  });

  const tideTen = mockDb.entities.LeaderboardEpoch.get('10');
  assert.equal(tideTen?.scheduledStartTime, 1790445600);
  assert.equal(tideTen?.isActive, true);
  assert.equal(tideTen?.startTime, 1790445600);

  const state = mockDb.entities.LeaderboardState.get('current');
  assert.equal(state?.currentEpochNumber, 10n);
  assert.equal(state?.isActive, true);

  // Tide 9 is untouched by tide 10 starting.
  const stillEnded = mockDb.entities.LeaderboardEpoch.get('9');
  assert.equal(stillEnded?.endTime, 1790442000);
  assert.equal(stillEnded?.duration, 2548800n);
});

test('an overridden epoch is not cut short when the next tide is given an earlier start', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 9n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '9',
    epochNumber: 9n,
    startBlock: 99810000n,
    startTime: 1787893200,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 1787893200,
    scheduledEndTime: 1790442000,
  });

  // startNewEpoch accepts a retrospective timestamp, so tide 10 can be given a
  // start that falls inside tide 9. That must not end tide 9 early.
  const earlyStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 10n,
    startTime: 1789000000n,
    ...eventData(99900000, 1789100000, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: earlyStart,
    mockDb,
  });

  const stillRunning = mockDb.entities.LeaderboardEpoch.get('9');
  assert.equal(stillRunning?.isActive, true);
  assert.equal(stillRunning?.endTime, undefined);

  const held = mockDb.entities.LeaderboardState.get('current');
  assert.equal(held?.currentEpochNumber, 9n);
  assert.equal(held?.isActive, true);
  assert.equal(mockDb.entities.LeaderboardEpoch.get('10')?.isActive, false);

  // Once the overridden end passes, tide 9 closes on that end - not on tide 10's
  // start - and tide 10 takes over.
  const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 9n,
    endTime: 1790442300n,
    ...eventData(100000000, 1790442100, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEnd,
    mockDb,
  });

  const closed = mockDb.entities.LeaderboardEpoch.get('9');
  assert.equal(closed?.isActive, false);
  assert.equal(closed?.endTime, 1790442000);
  assert.equal(closed?.duration, 2548800n);

  // Tide 10 takes over from tide 9's end, not from its own retrospective start,
  // so the two never overlap and accrual cannot be credited twice.
  const tideTen = mockDb.entities.LeaderboardEpoch.get('10');
  assert.equal(tideTen?.isActive, true);
  assert.equal(tideTen?.startTime, 1790442000);
  assert.equal(tideTen?.scheduledStartTime, 1789000000, 'what was scheduled is still recorded');
  assert.ok(
    Number(tideTen?.startTime) >= Number(closed?.endTime),
    'tide 10 must not start before tide 9 ended'
  );
});
