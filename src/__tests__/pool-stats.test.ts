import assert from 'node:assert/strict';
import { test } from 'node:test';

import { createDefaultReserve } from '../helpers/entityHelpers';
import { ZERO_ADDRESS } from '../helpers/constants';
import {
  adjustPoolReserveCount,
  computeRevenueDeltasUsd,
  computeUtilizationRate,
  updatePoolStatsIncremental,
  updateProtocolStatsIncremental,
  updateReserveUsdValues,
} from '../helpers/protocolAggregation';
import { getOrCreateProtocolStats } from '../handlers/shared';
import { TestHelpers, type MockDb } from './v3-test-helpers';

import type { EvmOnEventContext as handlerContext } from 'envio';
process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';

const RAY = 10n ** 27n;
const DECIMALS = 6;
const UNIT = 10n ** 6n;
const PRICE_E8 = 200000000n; // $2.00

const ADDRESSES = {
  poolA: '0x0000000000000000000000000000000000003001',
  poolB: '0x0000000000000000000000000000000000003002',
  assetA: '0x0000000000000000000000000000000000003003',
  assetB: '0x0000000000000000000000000000000000003004',
  aTokenA: '0x0000000000000000000000000000000000003005',
  vTokenA: '0x0000000000000000000000000000000000003006',
  aTokenB: '0x0000000000000000000000000000000000003007',
  vTokenB: '0x0000000000000000000000000000000000003008',
  user: '0x0000000000000000000000000000000000003009',
};

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

function seedPool(mockDb: MockDb, pool: string, timestamp: number): MockDb {
  let nextDb = mockDb;
  nextDb = nextDb.entities.Protocol.set({ id: '1' });
  nextDb = nextDb.entities.Pool.set({
    id: pool,
    addressProviderId: 0n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: undefined,
    poolDataProviderImpl: undefined,
    poolImpl: undefined,
    proxyPriceProvider: undefined,
    bridgeProtocolFee: undefined,
    flashloanPremiumToProtocol: undefined,
    flashloanPremiumTotal: undefined,
    active: true,
    paused: false,
    lastUpdateTimestamp: timestamp,
  });
  return nextDb;
}

function seedReserve(
  mockDb: MockDb,
  params: {
    asset: string;
    pool: string;
    aToken: string;
    vToken: string;
    timestamp: number;
    totalATokenSupply?: bigint;
    totalLiquidity?: bigint;
    availableLiquidity?: bigint;
    liquidityRate?: bigint;
  }
) {
  const reserveId = `${params.asset}-${params.pool}`;
  const reserve = {
    ...createDefaultReserve(reserveId, params.pool, params.asset),
    decimals: DECIMALS,
    totalATokenSupply: params.totalATokenSupply ?? 0n,
    totalLiquidity: params.totalLiquidity ?? 0n,
    availableLiquidity: params.availableLiquidity ?? 0n,
    liquidityRate: params.liquidityRate ?? 0n,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    lastUpdateTimestamp: params.timestamp,
    isActive: true,
    borrowingEnabled: true,
  };

  let nextDb = mockDb;
  nextDb = nextDb.entities.Reserve.set(reserve);
  for (const token of [params.aToken, params.vToken]) {
    nextDb = nextDb.entities.SubToken.set({
      id: token,
      pool_id: params.pool,
      tokenContractImpl: undefined,
      underlyingAssetAddress: params.asset,
      underlyingAssetDecimals: DECIMALS,
    });
  }
  nextDb = nextDb.entities.PriceOracleAsset.set({
    id: params.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: PRICE_E8,
    isFallbackRequired: false,
    lastUpdateTimestamp: params.timestamp,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: Number(PRICE_E8) / 1e8,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });

  return { mockDb: nextDb, reserveId };
}

function toE8(amount: bigint): bigint {
  return (amount * PRICE_E8) / 10n ** BigInt(DECIMALS);
}

function assertApprox(actual: number, expected: number, epsilon = 1e-6) {
  assert.ok(Math.abs(actual - expected) < epsilon, `expected ${expected} got ${actual}`);
}

/* -------------------------------------------------------------------------- */
/* computeRevenueDeltasUsd                                                     */
/* -------------------------------------------------------------------------- */

test('utilization is borrows over supplies, and zero when nothing is supplied', () => {
  assertApprox(computeUtilizationRate(50n, 100n), 0.5);
  assertApprox(computeUtilizationRate(0n, 100n), 0);
  assertApprox(computeUtilizationRate(100n, 100n), 1);

  // An empty market is 0% utilized rather than a divide-by-zero, and the
  // incremental accounting can momentarily leave a negative total when the last
  // position closes — that must not produce a nonsense rate either, on either
  // side of the division.
  assert.equal(computeUtilizationRate(0n, 0n), 0);
  assert.equal(computeUtilizationRate(10n, -5n), 0);
  assert.equal(computeUtilizationRate(-5n, 100n), 0);
  assert.equal(computeUtilizationRate(-5n, -5n), 0);
});

test('a market rate is not a reserve rate', () => {
  // Reserve.utilizationRate divides (totalLiquidity - availableLiquidity) by
  // totalLiquidity; this divides debt by aToken supply. A flash loan credits
  // availableLiquidity and totalATokenSupply but not totalLiquidity, so the two
  // bases diverge and the numbers must never be reconciled against each other.
  // Same inputs, same scaling, different answer:
  const totalLiquidity = 1000n;
  const availableLiquidity = 900n; // 100 borrowed, then a 100 flash-loan credit
  const reserveRate =
    Number(((totalLiquidity - availableLiquidity) * 100000000n) / totalLiquidity) / 1e8;

  const debt = 100n;
  const aTokenSupply = 1100n;
  assertApprox(reserveRate, 0.1);
  assertApprox(computeUtilizationRate(debt, aTokenSupply), 100 / 1100);
  assert.notEqual(reserveRate, computeUtilizationRate(debt, aTokenSupply));
});

test('revenue deltas convert lifetime token growth into USD', () => {
  const { deltaProtocolUsd, deltaSupplyUsd } = computeRevenueDeltasUsd(
    0n,
    0n,
    10n * UNIT,
    5n * UNIT,
    PRICE_E8,
    DECIMALS
  );

  assertApprox(deltaSupplyUsd, 20); // 10 tokens @ $2
  assertApprox(deltaProtocolUsd, 10); // 5 tokens @ $2
});

test('revenue deltas ignore non-positive lifetime movement', () => {
  // Lifetime counters only grow; an unchanged or replayed reserve must never
  // book negative revenue.
  const unchanged = computeRevenueDeltasUsd(
    7n * UNIT,
    3n * UNIT,
    7n * UNIT,
    3n * UNIT,
    PRICE_E8,
    DECIMALS
  );
  assert.equal(unchanged.deltaSupplyUsd, 0);
  assert.equal(unchanged.deltaProtocolUsd, 0);

  const wentBackwards = computeRevenueDeltasUsd(
    7n * UNIT,
    3n * UNIT,
    1n * UNIT,
    1n * UNIT,
    PRICE_E8,
    DECIMALS
  );
  assert.equal(wentBackwards.deltaSupplyUsd, 0);
  assert.equal(wentBackwards.deltaProtocolUsd, 0);
});

test('incremental protocol and pool updaters derive omitted revenue deltas identically', async () => {
  const protocolRows = new Map<string, Record<string, unknown>>();
  const poolRows = new Map<string, Record<string, unknown>>();
  const snapshots = new Map<string, Record<string, unknown>>();
  const context = {
    ProtocolStats: {
      get: async (id: string) => protocolRows.get(id),
      set: (row: Record<string, unknown>) => protocolRows.set(row.id as string, row),
    },
    PoolStats: {
      get: async (id: string) => poolRows.get(id),
      set: (row: Record<string, unknown>) => poolRows.set(row.id as string, row),
    },
    PoolStatsSnapshot: {
      get: async (id: string) => snapshots.get(id),
      set: (row: Record<string, unknown>) => snapshots.set(row.id as string, row),
    },
  } as unknown as handlerContext;
  await getOrCreateProtocolStats(context, 1);

  const delta = {
    oldSuppliesUsd: 0,
    oldBorrowsUsd: 0,
    oldAvailableUsd: 0,
    newSuppliesUsd: 0,
    newBorrowsUsd: 0,
    newAvailableUsd: 0,
    oldSuppliesE8: 0n,
    oldBorrowsE8: 0n,
    oldAvailableE8: 0n,
    newSuppliesE8: 0n,
    newBorrowsE8: 0n,
    newAvailableE8: 0n,
    oldSuppliersInterestEarned: 0n,
    oldProtocolAccrued: 0n,
    newSuppliersInterestEarned: 10n * UNIT,
    newProtocolAccrued: 5n * UNIT,
    priceE8: PRICE_E8,
    decimals: DECIMALS,
  };

  assert.equal(await updateProtocolStatsIncremental(context, delta, 42), true);
  await updatePoolStatsIncremental(context, ADDRESSES.poolA.toUpperCase(), delta, 42);

  const protocol = protocolRows.get('1');
  assert.equal(protocol?.supplyRevenueUsd, 20);
  assert.equal(protocol?.protocolRevenueUsd, 10);
  assert.equal(protocol?.totalRevenueUsd, 30);

  const pool = poolRows.get(ADDRESSES.poolA);
  assert.equal(pool?.supplyRevenueUsd, 20);
  assert.equal(pool?.protocolRevenueUsd, 10);
  assert.equal(pool?.totalRevenueUsd, 30);
  const snapshot = snapshots.get(`${ADDRESSES.poolA}-42`);
  assert.equal(snapshot?.supplyRevenueUsd, 20);
  assert.equal(snapshot?.protocolRevenueUsd, 10);
  assert.equal(snapshot?.totalRevenueUsd, 30);
});

/* -------------------------------------------------------------------------- */
/* adjustPoolReserveCount                                                      */
/* -------------------------------------------------------------------------- */

test('reserve count never goes negative', async () => {
  const store = new Map<string, Record<string, unknown>>();
  const snapshots = new Map<string, Record<string, unknown>>();
  const warnings: string[] = [];
  const context = {
    PoolStats: {
      get: async (id: string) => store.get(id),
      set: (row: Record<string, unknown>) => store.set(row.id as string, row),
    },
    PoolStatsSnapshot: {
      get: async (id: string) => snapshots.get(id),
      set: (row: Record<string, unknown>) => snapshots.set(row.id as string, row),
    },
    log: { warn: (message: string) => warnings.push(message) },
  } as unknown as handlerContext;

  // Decrement with no prior row at all: the row is created, clamped at zero
  // rather than going to -1.
  await adjustPoolReserveCount(context, ADDRESSES.poolA, -1, 100);
  assert.equal(store.get(ADDRESSES.poolA)?.reserveCount, 0);

  await adjustPoolReserveCount(context, ADDRESSES.poolA, 1, 200);
  assert.equal(store.get(ADDRESSES.poolA)?.reserveCount, 1);

  await adjustPoolReserveCount(context, ADDRESSES.poolA, -1, 300);
  assert.equal(store.get(ADDRESSES.poolA)?.reserveCount, 0);

  // Every adjustment lands in the time series too.
  assert.equal(snapshots.size, 3);
  assert.equal(snapshots.get(`${ADDRESSES.poolA}-200`)?.reserveCount, 1);

  // The clamp is a symptom, not a feature: reaching it means the count has
  // desynced from the reserve rows, so it must be audible rather than silent.
  assert.equal(warnings.length, 1, 'only the decrement below zero warns');
  assert.match(warnings[0], /went negative \(-1\)/);
});

test('reserve count normalizes the pool id to lower case', async () => {
  const store = new Map<string, Record<string, unknown>>();
  const context = {
    PoolStats: {
      get: async (id: string) => store.get(id),
      set: (row: Record<string, unknown>) => store.set(row.id as string, row),
    },
    PoolStatsSnapshot: { get: async () => undefined, set: () => {} },
  } as unknown as handlerContext;

  await adjustPoolReserveCount(context, ADDRESSES.poolA.toUpperCase(), 1, 100);

  assert.ok(store.has(ADDRESSES.poolA.toLowerCase()));
});

/* -------------------------------------------------------------------------- */
/* Per-pool revenue + snapshots through the real event path                    */
/* -------------------------------------------------------------------------- */

test('per-pool revenue accumulates and sums back to the protocol total', async () => {
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = seedPool(mockDb, ADDRESSES.poolA, 7000);
  mockDb = seedPool(mockDb, ADDRESSES.poolB, 7000);

  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetA,
    pool: ADDRESSES.poolA,
    aToken: ADDRESSES.aTokenA,
    vToken: ADDRESSES.vTokenA,
    timestamp: 7000,
  }));
  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetB,
    pool: ADDRESSES.poolB,
    aToken: ADDRESSES.aTokenB,
    vToken: ADDRESSES.vTokenB,
    timestamp: 7000,
  }));

  // A treasury mint books protocol revenue on pool A only. The real shape is
  // Mint(caller = the Pool contract) followed by MintedToTreasury, which is the
  // event that actually recognizes the revenue - and the reason PoolStats sees
  // it at all, since the old inline path wrote to ProtocolStats only.
  const treasury = '0x00000000000000000000000000000000000000aa';
  mockDb = mockDb.entities.ATokenTreasury.set({
    id: ADDRESSES.aTokenA,
    treasury,
    poolContract: ADDRESSES.poolA,
    updatedAt: 7000,
  });

  const treasuryMint = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.poolA,
    onBehalfOf: treasury,
    value: 100n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(1, 7001, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: treasuryMint, mockDb });

  const mintedToTreasury = TestHelpers.Pool.MintedToTreasury.createMockEvent({
    reserve: ADDRESSES.assetA,
    amountMinted: 100n * UNIT,
    ...eventData(2, 7001, ADDRESSES.poolA),
  });
  mockDb = await TestHelpers.Pool.MintedToTreasury.processEvent({
    event: mintedToTreasury,
    mockDb,
  });

  // A plain supply on pool B moves totals but books no revenue.
  const supplyB = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 500n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(2, 7002, ADDRESSES.aTokenB),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyB, mockDb });

  const poolA = mockDb.entities.PoolStats.get(ADDRESSES.poolA);
  const poolB = mockDb.entities.PoolStats.get(ADDRESSES.poolB);
  const protocol = mockDb.entities.ProtocolStats.get('1');

  assert.ok(poolA, 'PoolStats for pool A should exist');
  assert.ok(poolB, 'PoolStats for pool B should exist');
  assert.ok(protocol);

  // Revenue landed on the market that earned it, not the other one.
  assert.ok((poolA?.protocolRevenueUsd ?? 0) > 0, 'pool A should have booked protocol revenue');
  assert.equal(poolB?.protocolRevenueUsd, 0);

  // The invariant that makes the per-pool numbers trustworthy: the markets sum
  // back to the grand total.
  assertApprox(
    (poolA?.totalRevenueUsd ?? 0) + (poolB?.totalRevenueUsd ?? 0),
    protocol?.totalRevenueUsd ?? 0
  );
  assertApprox(
    (poolA?.protocolRevenueUsd ?? 0) + (poolB?.protocolRevenueUsd ?? 0),
    protocol?.protocolRevenueUsd ?? 0
  );
  assertApprox(
    (poolA?.supplyRevenueUsd ?? 0) + (poolB?.supplyRevenueUsd ?? 0),
    protocol?.supplyRevenueUsd ?? 0
  );
  assert.equal(
    (poolA?.tvlE8 ?? 0n) + (poolB?.tvlE8 ?? 0n),
    protocol?.tvlE8,
    'per-pool TVL must sum to the protocol total'
  );

  // totalRevenue is the sum of its two parts, per market.
  assertApprox(
    poolA?.totalRevenueUsd ?? 0,
    (poolA?.protocolRevenueUsd ?? 0) + (poolA?.supplyRevenueUsd ?? 0)
  );
});

test('borrowing raises the stored utilization on both the pool and the protocol', async () => {
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = seedPool(mockDb, ADDRESSES.poolA, 9000);
  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetA,
    pool: ADDRESSES.poolA,
    aToken: ADDRESSES.aTokenA,
    vToken: ADDRESSES.vTokenA,
    timestamp: 9000,
  }));

  // Supply 1000, borrow nothing: an idle market sits at 0%.
  const supply = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(1, 9001, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supply, mockDb });

  assert.equal(mockDb.entities.PoolStats.get(ADDRESSES.poolA)?.utilizationRate, 0);
  assert.equal(mockDb.entities.ProtocolStats.get('1')?.utilizationRate, 0);

  // Borrow 250 of the 1000 supplied -> 25%.
  const borrow = TestHelpers.VariableDebtToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 250n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(2, 9002, ADDRESSES.vTokenA),
  });
  mockDb = await TestHelpers.VariableDebtToken.Mint.processEvent({ event: borrow, mockDb });

  const pool = mockDb.entities.PoolStats.get(ADDRESSES.poolA);
  const protocol = mockDb.entities.ProtocolStats.get('1');

  assertApprox(pool?.utilizationRate ?? 0, 0.25);

  // With a single market, the protocol rate must equal that market's rate.
  assertApprox(protocol?.utilizationRate ?? 0, 0.25);

  // And the stored value must agree with recomputing it from the stored totals,
  // so a consumer that derives it by hand gets the same answer.
  assertApprox(
    pool?.utilizationRate ?? 0,
    computeUtilizationRate(pool?.borrowsE8 ?? 0n, pool?.suppliesE8 ?? 0n)
  );
  assertApprox(
    protocol?.utilizationRate ?? 0,
    computeUtilizationRate(protocol?.borrowsE8 ?? 0n, protocol?.suppliesE8 ?? 0n)
  );

  // The rate is carried into the time series, not just the live row.
  const snapshot = mockDb.entities.PoolStatsSnapshot.get(`${ADDRESSES.poolA}-9002`);
  assertApprox(snapshot?.utilizationRate ?? 0, 0.25);
});

test('pool snapshots track the series and collapse within one second', async () => {
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = seedPool(mockDb, ADDRESSES.poolA, 8000);
  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetA,
    pool: ADDRESSES.poolA,
    aToken: ADDRESSES.aTokenA,
    vToken: ADDRESSES.vTokenA,
    timestamp: 8000,
  }));

  const first = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 100n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(1, 8001, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: first, mockDb });

  const snapshotOne = mockDb.entities.PoolStatsSnapshot.get(`${ADDRESSES.poolA}-8001`);
  assert.ok(snapshotOne, 'a snapshot should exist for the first update');
  assert.equal(snapshotOne?.pool_id, ADDRESSES.poolA);
  assert.equal(snapshotOne?.timestamp, 8001);
  assert.equal(snapshotOne?.suppliesE8, toE8(100n * UNIT));
  assert.equal(snapshotOne?.tvlE8, snapshotOne?.suppliesE8);

  // A second event in the SAME second overwrites rather than appending, so the
  // series carries one row per second exactly like ProtocolStatsSnapshot.
  const sameSecond = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 50n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(2, 8001, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: sameSecond, mockDb });

  const collapsed = mockDb.entities.PoolStatsSnapshot.get(`${ADDRESSES.poolA}-8001`);
  assert.equal(
    collapsed?.suppliesE8,
    toE8(150n * UNIT),
    'same-second row must hold the last value'
  );

  // A later second appends a new row, leaving the earlier one intact.
  const later = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 25n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(3, 8009, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: later, mockDb });

  const earlier = mockDb.entities.PoolStatsSnapshot.get(`${ADDRESSES.poolA}-8001`);
  const newest = mockDb.entities.PoolStatsSnapshot.get(`${ADDRESSES.poolA}-8009`);
  assert.equal(earlier?.suppliesE8, toE8(150n * UNIT), 'history must not be rewritten');
  assert.equal(newest?.suppliesE8, toE8(175n * UNIT));

  // The snapshot mirrors the live row at the moment it was taken.
  const live = mockDb.entities.PoolStats.get(ADDRESSES.poolA);
  assert.equal(newest?.suppliesE8, live?.suppliesE8);
  assert.equal(newest?.borrowsE8, live?.borrowsE8);
  assert.equal(newest?.availableE8, live?.availableE8);
  assert.equal(newest?.totalRevenueUsd, live?.totalRevenueUsd);
  assert.equal(newest?.reserveCount, live?.reserveCount);
});

test('a pool row never books a delta the protocol row refused', async () => {
  // updateProtocolStatsIncremental refuses a delta when ProtocolStats is
  // missing. The pool row has to refuse the same delta: one row moving without
  // the other is exactly the drift that makes the per-pool numbers untrustworthy.
  // Unreachable through the event path — every handler creates ProtocolStats
  // first — so it is pinned here directly.
  const poolWrites: Record<string, unknown>[] = [];
  const snapshotWrites: Record<string, unknown>[] = [];

  const context = {
    Reserve: {
      get: async () => ({
        id: 'reserve',
        pool_id: ADDRESSES.poolA,
        decimals: DECIMALS,
        totalLiquidity: 100n * UNIT,
        availableLiquidity: 100n * UNIT,
        totalATokenSupply: 100n * UNIT,
        totalCurrentDebt: 0n,
        lifetimeSuppliersInterestEarned: 5n * UNIT,
        lifetimeReserveFactorAccrued: 1n * UNIT,
      }),
      set: () => {},
    },
    PriceOracleAsset: {
      get: async () => ({ priceInEth: PRICE_E8, lastUpdateTimestamp: 1 }),
      set: () => {},
    },
    ReserveAggregate: { get: async () => undefined, set: () => {} },
    ProtocolStats: { get: async () => undefined, set: () => {} },
    PoolStats: {
      get: async () => undefined,
      set: (row: Record<string, unknown>) => poolWrites.push(row),
    },
    PoolStatsSnapshot: {
      get: async () => undefined,
      set: (row: Record<string, unknown>) => snapshotWrites.push(row),
    },
  } as unknown as handlerContext;

  await updateReserveUsdValues(context, 'reserve', ADDRESSES.assetA, 5000);

  assert.equal(poolWrites.length, 0, 'the pool row must not move');
  assert.equal(snapshotWrites.length, 0, 'and no snapshot should be taken for it');
});

/* -------------------------------------------------------------------------- */
/* reserveCount through the reserve lifecycle                                  */
/* -------------------------------------------------------------------------- */

const LIFECYCLE = {
  registry: '0x0000000000000000000000000000000000004001',
  provider: '0x0000000000000000000000000000000000004002',
  pool: '0x0000000000000000000000000000000000004003',
  configurator: '0x0000000000000000000000000000000000004004',
  asset: '0x0000000000000000000000000000000000004005',
  aToken: '0x0000000000000000000000000000000000004006',
  vToken: '0x0000000000000000000000000000000000004007',
  interestStrategy: '0x0000000000000000000000000000000000004008',
};

function seedMarket(): MockDb {
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();

  // The configurator -> provider mapping is what resolvePoolId reads to decide
  // which market a PoolConfigurator event belongs to; without it the handler
  // would fall back to keying by the configurator's own address.
  mockDb = mockDb.entities.ContractToPoolMapping.set({
    id: LIFECYCLE.configurator,
    pool_id: LIFECYCLE.provider,
  });

  // AToken.Initialized carries the Pool address rather than the configurator's,
  // and resolves it through the same mapping table.
  mockDb = mockDb.entities.ContractToPoolMapping.set({
    id: LIFECYCLE.pool,
    pool_id: LIFECYCLE.provider,
  });

  return mockDb;
}

function aTokenInitializedEvent(block: number, timestamp: number) {
  return TestHelpers.AToken.Initialized.createMockEvent({
    underlyingAsset: LIFECYCLE.asset,
    pool: LIFECYCLE.pool,
    treasury: ZERO_ADDRESS,
    incentivesController: ZERO_ADDRESS,
    aTokenDecimals: 6n,
    aTokenName: 'Neverland USDC',
    aTokenSymbol: 'nUSDC',
    params: '0x',
    mockEventData: {
      block: { number: block, timestamp },
      logIndex: block,
      srcAddress: LIFECYCLE.aToken,
      transaction: { hash: `0x${block.toString(16).padStart(64, '0')}` },
    },
  });
}

function reserveActiveEvent(block: number, timestamp: number, active: boolean) {
  return TestHelpers.PoolConfigurator.ReserveActive.createMockEvent({
    asset: LIFECYCLE.asset,
    active,
    mockEventData: {
      block: { number: block, timestamp },
      logIndex: block,
      srcAddress: LIFECYCLE.configurator,
      transaction: { hash: `0x${block.toString(16).padStart(64, '0')}` },
    },
  });
}

function initReserveEvent(
  block: number,
  timestamp: number,
  asset: string = LIFECYCLE.asset,
  aToken: string = LIFECYCLE.aToken,
  vToken: string = LIFECYCLE.vToken
) {
  return TestHelpers.PoolConfigurator.ReserveInitialized.createMockEvent({
    asset,
    aToken,
    stableDebtToken: ZERO_ADDRESS,
    variableDebtToken: vToken,
    interestRateStrategyAddress: LIFECYCLE.interestStrategy,
    mockEventData: {
      block: { number: block, timestamp },
      logIndex: block,
      srcAddress: LIFECYCLE.configurator,
      transaction: { hash: `0x${block.toString(16).padStart(64, '0')}` },
    },
  });
}

function dropReserveEvent(block: number, timestamp: number) {
  return TestHelpers.PoolConfigurator.ReserveDropped.createMockEvent({
    asset: LIFECYCLE.asset,
    mockEventData: {
      block: { number: block, timestamp },
      logIndex: block,
      srcAddress: LIFECYCLE.configurator,
      transaction: { hash: `0x${block.toString(16).padStart(64, '0')}` },
    },
  });
}

test('listing a reserve raises the pool reserve count exactly once', async () => {
  let mockDb = seedMarket();

  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(10, 1100),
    mockDb,
  });

  const afterFirst = mockDb.entities.PoolStats.get(LIFECYCLE.provider);
  assert.equal(afterFirst?.reserveCount, 1);

  // Replaying the same listing must not inflate the count — ReserveInitialized
  // overwrites the Reserve row unconditionally, so the guard is the only thing
  // standing between a re-org replay and a wrong count.
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(11, 1110),
    mockDb,
  });

  const afterReplay = mockDb.entities.PoolStats.get(LIFECYCLE.provider);
  assert.equal(afterReplay?.reserveCount, 1, 're-listing must not double count');
});

test('each listed reserve adds to the same market count', async () => {
  let mockDb = seedMarket();

  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(10, 1100),
    mockDb,
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(
      11,
      1110,
      '0x0000000000000000000000000000000000004105',
      '0x0000000000000000000000000000000000004106',
      '0x0000000000000000000000000000000000004107'
    ),
    mockDb,
  });

  const stats = mockDb.entities.PoolStats.get(LIFECYCLE.provider);
  assert.equal(stats?.reserveCount, 2, 'a second distinct reserve must raise the count');

  // The running count, not just the delta, is what the series carries.
  const snapshot = mockDb.entities.PoolStatsSnapshot.get(`${LIFECYCLE.provider}-1110`);
  assert.equal(snapshot?.reserveCount, 2);
});

test('dropping a reserve lowers the count exactly once', async () => {
  let mockDb = seedMarket();

  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(10, 1100),
    mockDb,
  });
  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount, 1);

  mockDb = await TestHelpers.PoolConfigurator.ReserveDropped.processEvent({
    event: dropReserveEvent(12, 1120),
    mockDb,
  });
  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount, 0);

  // Dropping an already-dropped reserve is a no-op.
  mockDb = await TestHelpers.PoolConfigurator.ReserveDropped.processEvent({
    event: dropReserveEvent(13, 1130),
    mockDb,
  });
  assert.equal(
    mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount,
    0,
    'dropping twice must not double decrement'
  );

  // The count change is visible in the time series, not just the live row.
  const snapshot = mockDb.entities.PoolStatsSnapshot.get(`${LIFECYCLE.provider}-1120`);
  assert.equal(snapshot?.reserveCount, 0);
});

test('dropping a reserve that was never listed leaves the count alone', async () => {
  const mockDb = seedMarket();

  const after = await TestHelpers.PoolConfigurator.ReserveDropped.processEvent({
    event: dropReserveEvent(14, 1140),
    mockDb,
  });

  // No Reserve row exists, so the handler must not create PoolStats either.
  assert.equal(after.entities.PoolStats.get(LIFECYCLE.provider), undefined);
});

test('the aToken stub written before the listing does not swallow the count', async () => {
  let mockDb = seedMarket();

  // This is the real on-chain order: ConfiguratorLogic.executeInitReserve
  // initializes the aToken proxy — emitting AToken.Initialized — before it
  // emits ReserveInitialized, so the Reserve row already exists by the time the
  // listing is handled. Counting on row existence would miss every listing.
  mockDb = await TestHelpers.AToken.Initialized.processEvent({
    event: aTokenInitializedEvent(10, 1100),
    mockDb,
  });

  const stub = mockDb.entities.Reserve.get(`${LIFECYCLE.asset}-${LIFECYCLE.provider}`);
  assert.ok(stub, 'the aToken handler should have created the reserve row');
  assert.equal(stub?.isListed, false, 'a stub is not a listing');
  assert.equal(
    mockDb.entities.PoolStats.get(LIFECYCLE.provider),
    undefined,
    'an unlisted stub must not create or move PoolStats'
  );

  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(11, 1101),
    mockDb,
  });

  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount, 1);
  assert.equal(
    mockDb.entities.Reserve.get(`${LIFECYCLE.asset}-${LIFECYCLE.provider}`)?.isListed,
    true
  );
});

test('dropping an unlisted stub is a no-op', async () => {
  let mockDb = seedMarket();

  mockDb = await TestHelpers.AToken.Initialized.processEvent({
    event: aTokenInitializedEvent(10, 1100),
    mockDb,
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveDropped.processEvent({
    event: dropReserveEvent(11, 1110),
    mockDb,
  });

  // The stub was never counted, so dropping it must not push the count down.
  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider), undefined);
});

test('re-listing a dropped reserve restores the count', async () => {
  let mockDb = seedMarket();

  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(10, 1100),
    mockDb,
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveDropped.processEvent({
    event: dropReserveEvent(11, 1110),
    mockDb,
  });
  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount, 0);

  // The dropped row survives, so the count has to key off the dropped state
  // rather than row existence or it would never recover.
  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(12, 1120),
    mockDb,
  });

  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount, 1);
});

test('reactivating a dropped reserve restores the count', async () => {
  let mockDb = seedMarket();

  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(20, 2100),
    mockDb,
  });
  mockDb = await TestHelpers.PoolConfigurator.ReserveDropped.processEvent({
    event: dropReserveEvent(21, 2110),
    mockDb,
  });

  // ReserveActive(true) clears isDropped, which puts the reserve back into the
  // counted state — the count has to follow.
  mockDb = await TestHelpers.PoolConfigurator.ReserveActive.processEvent({
    event: reserveActiveEvent(22, 2120, true),
    mockDb,
  });
  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount, 1);

  // And it is genuinely counted again, not merely clamped: the next drop takes
  // it back down rather than bottoming out at an already-zero count.
  mockDb = await TestHelpers.PoolConfigurator.ReserveDropped.processEvent({
    event: dropReserveEvent(23, 2130),
    mockDb,
  });
  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount, 0);
});

test('deactivating a live reserve leaves the count alone', async () => {
  let mockDb = seedMarket();

  mockDb = await TestHelpers.PoolConfigurator.ReserveInitialized.processEvent({
    event: initReserveEvent(30, 3100),
    mockDb,
  });

  // Freezing or pausing a market is not delisting it: isActive moves, isDropped
  // does not, so the reserve stays counted.
  mockDb = await TestHelpers.PoolConfigurator.ReserveActive.processEvent({
    event: reserveActiveEvent(31, 3110, false),
    mockDb,
  });
  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount, 1);

  mockDb = await TestHelpers.PoolConfigurator.ReserveActive.processEvent({
    event: reserveActiveEvent(32, 3120, true),
    mockDb,
  });
  assert.equal(mockDb.entities.PoolStats.get(LIFECYCLE.provider)?.reserveCount, 1);
});
