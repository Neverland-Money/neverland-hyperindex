import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { createDefaultReserve } from '../helpers/entityHelpers';
import { TREASURY_ADDRESSES } from '../helpers/constants';
import { calculateGrowth } from '../helpers/math';
import {
  updateProtocolStatsIncremental,
  updateReserveUsdValues,
} from '../helpers/protocolAggregation';
import type { handlerContext } from '../../generated';
import type { t as MockDb } from '../../generated/src/TestHelpers_MockDb.gen';

process.env.DISABLE_EXTERNAL_CALLS = 'true';
process.env.DISABLE_ETH_CALLS = 'true';

const RAY = 10n ** 27n;
const DECIMALS = 6;
const UNIT = 10n ** 6n;

const ADDRESSES = {
  pool: '0x0000000000000000000000000000000000001001',
  assetA: '0x0000000000000000000000000000000000001002',
  assetB: '0x0000000000000000000000000000000000001003',
  aTokenA: '0x0000000000000000000000000000000000001004',
  vTokenA: '0x0000000000000000000000000000000000001005',
  aTokenB: '0x0000000000000000000000000000000000001006',
  vTokenB: '0x0000000000000000000000000000000000001007',
  user: '0x0000000000000000000000000000000000001008',
};

type TestHelpersApi = typeof import('../../generated').TestHelpers;

function loadTestHelpers(): TestHelpersApi {
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
    priceE8: bigint;
    timestamp: number;
    totalATokenSupply?: bigint;
    totalLiquidity?: bigint;
    availableLiquidity?: bigint;
    liquidityRate?: bigint;
    lastUpdateTimestamp?: number;
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
    lastUpdateTimestamp: params.lastUpdateTimestamp ?? params.timestamp,
    isActive: true,
    borrowingEnabled: true,
  };

  let nextDb = mockDb;
  nextDb = nextDb.entities.Reserve.set(reserve);
  nextDb = nextDb.entities.SubToken.set({
    id: params.aToken,
    pool_id: params.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: params.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  nextDb = nextDb.entities.SubToken.set({
    id: params.vToken,
    pool_id: params.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: params.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  nextDb = nextDb.entities.PriceOracleAsset.set({
    id: params.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: params.priceE8,
    isFallbackRequired: false,
    lastUpdateTimestamp: params.timestamp,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: Number(params.priceE8) / 1e8,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });

  return { mockDb: nextDb, reserveId };
}

function toE8(amount: bigint, priceE8: bigint): bigint {
  const decimalsBI = 10n ** BigInt(DECIMALS);
  return (amount * priceE8) / decimalsBI;
}

function toUsd(e8: bigint): number {
  return Number(e8) / 1e8;
}

function assertApprox(actual: number, expected: number, epsilon = 1e-6) {
  assert.ok(Math.abs(actual - expected) < epsilon, `expected ${expected} got ${actual}`);
}

test('protocol stats aggregate supplies across reserves', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = seedPool(mockDb, ADDRESSES.pool, 1000);

  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetA,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aTokenA,
    vToken: ADDRESSES.vTokenA,
    priceE8: 200000000n,
    timestamp: 1000,
  }));

  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetB,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aTokenB,
    vToken: ADDRESSES.vTokenB,
    priceE8: 150000000n,
    timestamp: 1000,
  }));

  const supplyA = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(1, 1001, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyA, mockDb });

  const supplyB = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 500n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(2, 1002, ADDRESSES.aTokenB),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyB, mockDb });

  const stats = mockDb.entities.ProtocolStats.get('1');
  assert.ok(stats);

  const suppliesA = toE8(1000n * UNIT, 200000000n);
  const suppliesB = toE8(500n * UNIT, 150000000n);
  const expectedSuppliesE8 = suppliesA + suppliesB;
  const expectedSuppliesUsd = toUsd(expectedSuppliesE8);

  assert.equal(stats?.suppliesE8, expectedSuppliesE8);
  assert.equal(stats?.tvlE8, expectedSuppliesE8);
  assertApprox(stats?.suppliesUsd ?? 0, expectedSuppliesUsd);
  assertApprox(stats?.tvlUsd ?? 0, expectedSuppliesUsd);
  assertApprox(stats?.availableUsd ?? 0, expectedSuppliesUsd);
});

test('aggregation helpers return early when data is missing', async () => {
  const noopStore = {
    get: async () => undefined,
    set: () => {},
  };
  const contextMissingReserve = {
    Reserve: noopStore,
    PriceOracleAsset: noopStore,
  } as unknown as handlerContext;

  await updateReserveUsdValues(contextMissingReserve, 'missing', 'asset', 0);

  const contextMissingOracle = {
    Reserve: {
      get: async () => ({
        id: 'reserve',
        decimals: 6,
        totalLiquidity: 0n,
        availableLiquidity: 0n,
        totalATokenSupply: 0n,
        totalCurrentVariableDebt: 0n,
        totalPrincipalStableDebt: 0n,
        lifetimeSuppliersInterestEarned: 0n,
        lifetimeReserveFactorAccrued: 0n,
      }),
      set: () => {},
    },
    PriceOracleAsset: noopStore,
  } as unknown as handlerContext;

  await updateReserveUsdValues(contextMissingOracle, 'reserve', 'asset', 0);

  await updateProtocolStatsIncremental(
    { ProtocolStats: noopStore } as unknown as handlerContext,
    0,
    0,
    0,
    0,
    0,
    0,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0n,
    0,
    0
  );
});

test('borrows and available update after variable debt mint', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = seedPool(mockDb, ADDRESSES.pool, 2000);
  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetA,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aTokenA,
    vToken: ADDRESSES.vTokenA,
    priceE8: 200000000n,
    timestamp: 2000,
  }));

  const supply = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(1, 2001, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supply, mockDb });

  const borrow = TestHelpers.VariableDebtToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 300n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(2, 2002, ADDRESSES.vTokenA),
  });
  mockDb = await TestHelpers.VariableDebtToken.Mint.processEvent({ event: borrow, mockDb });

  const stats = mockDb.entities.ProtocolStats.get('1');
  assert.ok(stats);

  const suppliesE8 = toE8(1000n * UNIT, 200000000n);
  const borrowsE8 = toE8(300n * UNIT, 200000000n);
  const availableE8 = toE8(700n * UNIT, 200000000n);

  assert.equal(stats?.suppliesE8, suppliesE8);
  assert.equal(stats?.borrowsE8, borrowsE8);
  assert.equal(stats?.availableE8, availableE8);
  assertApprox(stats?.suppliesUsd ?? 0, toUsd(suppliesE8));
  assertApprox(stats?.borrowsUsd ?? 0, toUsd(borrowsE8));
  assertApprox(stats?.availableUsd ?? 0, toUsd(availableE8));
});

test('treasury mints increase protocol revenue', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = seedPool(mockDb, ADDRESSES.pool, 3000);
  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetA,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aTokenA,
    vToken: ADDRESSES.vTokenA,
    priceE8: 200000000n,
    timestamp: 3000,
  }));

  const treasury = TREASURY_ADDRESSES[0];
  const mint = TestHelpers.AToken.Mint.createMockEvent({
    caller: treasury,
    onBehalfOf: treasury,
    value: 50n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(1, 3001, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: mint, mockDb });

  const stats = mockDb.entities.ProtocolStats.get('1');
  assert.ok(stats);

  const protocolRevenueE8 = toE8(50n * UNIT, 200000000n);
  const protocolRevenueUsd = toUsd(protocolRevenueE8);

  assertApprox(stats?.protocolRevenueUsd ?? 0, protocolRevenueUsd);
  assertApprox(stats?.supplyRevenueUsd ?? 0, 0);
  assertApprox(stats?.totalRevenueUsd ?? 0, protocolRevenueUsd);
});

test('reserve interest accrual updates supply revenue', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = seedPool(mockDb, ADDRESSES.pool, 4000);
  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetA,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aTokenA,
    vToken: ADDRESSES.vTokenA,
    priceE8: 200000000n,
    timestamp: 4000,
    totalATokenSupply: 1000n * UNIT,
    totalLiquidity: 1000n * UNIT,
    availableLiquidity: 1000n * UNIT,
    liquidityRate: RAY / 10n,
    lastUpdateTimestamp: 4000,
  }));

  const nextTimestamp = 4000 + 86400;
  const expectedGrowth = calculateGrowth(
    1000n * UNIT,
    RAY / 10n,
    BigInt(4000),
    BigInt(nextTimestamp)
  );
  const expectedRevenueUsd = toUsd(toE8(expectedGrowth, 200000000n));

  const update = TestHelpers.Pool.ReserveDataUpdated.createMockEvent({
    reserve: ADDRESSES.assetA,
    liquidityRate: RAY / 10n,
    stableBorrowRate: 0n,
    variableBorrowRate: 0n,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    ...eventData(1, nextTimestamp, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.ReserveDataUpdated.processEvent({ event: update, mockDb });

  const stats = mockDb.entities.ProtocolStats.get('1');
  assert.ok(stats);
  assertApprox(stats?.supplyRevenueUsd ?? 0, expectedRevenueUsd);
  assertApprox(stats?.protocolRevenueUsd ?? 0, 0);
  assertApprox(stats?.totalRevenueUsd ?? 0, expectedRevenueUsd);
});

test('withdrawals reduce supplies and available', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = seedPool(mockDb, ADDRESSES.pool, 5000);
  ({ mockDb } = seedReserve(mockDb, {
    asset: ADDRESSES.assetA,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aTokenA,
    vToken: ADDRESSES.vTokenA,
    priceE8: 200000000n,
    timestamp: 5000,
  }));

  const supply = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(1, 5001, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supply, mockDb });

  const burn = TestHelpers.AToken.Burn.createMockEvent({
    from: ADDRESSES.user,
    target: ADDRESSES.user,
    value: 200n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(2, 5002, ADDRESSES.aTokenA),
  });
  mockDb = await TestHelpers.AToken.Burn.processEvent({ event: burn, mockDb });

  const stats = mockDb.entities.ProtocolStats.get('1');
  assert.ok(stats);

  const suppliesE8 = toE8(800n * UNIT, 200000000n);
  assert.equal(stats?.suppliesE8, suppliesE8);
  assertApprox(stats?.suppliesUsd ?? 0, toUsd(suppliesE8));
  assertApprox(stats?.availableUsd ?? 0, toUsd(suppliesE8));
});
