import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { createDefaultReserve } from '../helpers/entityHelpers';
import type { t as MockDb } from '../../generated/src/TestHelpers_MockDb.gen';

process.env.DISABLE_EXTERNAL_CALLS = 'true';
process.env.DISABLE_ETH_CALLS = 'true';

const RAY = 10n ** 27n;
const DECIMALS = 6;
const UNIT = 10n ** 6n;

const ADDRESSES = {
  pool: '0x0000000000000000000000000000000000003001',
  collateral: '0x0000000000000000000000000000000000003002',
  debt: '0x0000000000000000000000000000000000003003',
  user: '0x0000000000000000000000000000000000003004',
  liquidator: '0x0000000000000000000000000000000000003005',
  flashTarget: '0x0000000000000000000000000000000000003006',
  collateralAlt: '0x0000000000000000000000000000000000003007',
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

function seedPool(
  mockDb: MockDb,
  pool: string,
  timestamp: number,
  flashloanPremiumToProtocol: bigint | undefined,
  bridgeProtocolFee?: bigint
) {
  return mockDb.entities.Pool.set({
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
    bridgeProtocolFee,
    flashloanPremiumToProtocol,
    flashloanPremiumTotal: undefined,
    active: true,
    paused: false,
    lastUpdateTimestamp: timestamp,
  });
}

function seedReserve(
  mockDb: MockDb,
  asset: string,
  pool: string,
  timestamp: number,
  totals: {
    totalATokenSupply?: bigint;
    totalLiquidity?: bigint;
    availableLiquidity?: bigint;
    lifetimeLiquidated?: bigint;
  } = {}
) {
  const reserveId = `${asset}-${pool}`;
  const reserve = {
    ...createDefaultReserve(reserveId, pool, asset),
    decimals: DECIMALS,
    totalATokenSupply: totals.totalATokenSupply ?? 0n,
    totalLiquidity: totals.totalLiquidity ?? 0n,
    availableLiquidity: totals.availableLiquidity ?? 0n,
    lifetimeLiquidated: totals.lifetimeLiquidated ?? 0n,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    lastUpdateTimestamp: timestamp,
    isActive: true,
    borrowingEnabled: true,
  };

  let nextDb = mockDb.entities.Reserve.set(reserve);
  nextDb = nextDb.entities.PriceOracleAsset.set({
    id: asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: timestamp,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });
  return nextDb;
}

function setAssetPrice(mockDb: MockDb, asset: string, priceUsd: number, timestamp: number) {
  const priceInEth = BigInt(Math.round(priceUsd * 1e8));
  return mockDb.entities.PriceOracleAsset.set({
    id: asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth,
    isFallbackRequired: false,
    lastUpdateTimestamp: timestamp,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: priceUsd,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });
}

test('flashloan splits premium between protocol and LP', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 1000, 5000n);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 1000, {
    totalATokenSupply: 1000n * UNIT,
    totalLiquidity: 1000n * UNIT,
    availableLiquidity: 1000n * UNIT,
  });

  const flash = TestHelpers.Pool.FlashLoan.createMockEvent({
    target: ADDRESSES.flashTarget,
    initiator: ADDRESSES.user,
    asset: ADDRESSES.collateral,
    amount: 500n * UNIT,
    interestRateMode: 0n,
    premium: 100n * UNIT,
    referralCode: 0n,
    ...eventData(1, 1001, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.FlashLoan.processEvent({ event: flash, mockDb });

  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.ok(reserve);
  assert.equal(reserve?.availableLiquidity, 1100n * UNIT);
  assert.equal(reserve?.totalATokenSupply, 1100n * UNIT);
  assert.equal(reserve?.lifetimeFlashLoans, 500n * UNIT);
  assert.equal(reserve?.lifetimeFlashLoanPremium, 100n * UNIT);
  assert.equal(reserve?.lifetimeFlashLoanProtocolPremium, 50n * UNIT);
  assert.equal(reserve?.lifetimeFlashLoanLPPremium, 50n * UNIT);

  const flashId = `${flash.transaction.hash}-${flash.logIndex}`;
  const flashEntity = mockDb.entities.FlashLoan.get(flashId);
  assert.ok(flashEntity);
  assert.equal(flashEntity?.protocolFee, 50n * UNIT);
  assert.equal(flashEntity?.lpFee, 50n * UNIT);
});

test('liquidations update reserve totals and create event record', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 2000, undefined);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 2000);
  mockDb = seedReserve(mockDb, ADDRESSES.debt, ADDRESSES.pool, 2000);

  const liquidation = TestHelpers.Pool.LiquidationCall.createMockEvent({
    collateralAsset: ADDRESSES.collateral,
    debtAsset: ADDRESSES.debt,
    user: ADDRESSES.user,
    debtToCover: 200n * UNIT,
    liquidatedCollateralAmount: 150n * UNIT,
    liquidator: ADDRESSES.liquidator,
    receiveAToken: false,
    ...eventData(2, 2010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.LiquidationCall.processEvent({
    event: liquidation,
    mockDb,
  });

  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.lifetimeLiquidated, 150n * UNIT);

  const liquidationId = `${liquidation.transaction.hash}-${liquidation.logIndex}`;
  const record = mockDb.entities.LiquidationCall.get(liquidationId);
  assert.ok(record);
  assert.equal(record?.collateralAmount, 150n * UNIT);
  assert.equal(record?.principalAmount, 200n * UNIT);

  assert.ok(mockDb.entities.User.get(ADDRESSES.user));
  assert.ok(mockDb.entities.User.get(ADDRESSES.liquidator));
});

test('liquidations scope reserves and record asset prices', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 2000, undefined);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 2000);
  mockDb = seedReserve(mockDb, ADDRESSES.debt, ADDRESSES.pool, 2000);
  mockDb = seedReserve(mockDb, ADDRESSES.collateralAlt, ADDRESSES.pool, 2000);

  mockDb = setAssetPrice(mockDb, ADDRESSES.collateral, 2.5, 2000);
  mockDb = setAssetPrice(mockDb, ADDRESSES.debt, 0.75, 2000);
  mockDb = setAssetPrice(mockDb, ADDRESSES.collateralAlt, 4, 2000);

  const liquidationOne = TestHelpers.Pool.LiquidationCall.createMockEvent({
    collateralAsset: ADDRESSES.collateral,
    debtAsset: ADDRESSES.debt,
    user: ADDRESSES.user,
    debtToCover: 200n * UNIT,
    liquidatedCollateralAmount: 150n * UNIT,
    liquidator: ADDRESSES.liquidator,
    receiveAToken: false,
    ...eventData(12, 2010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.LiquidationCall.processEvent({
    event: liquidationOne,
    mockDb,
  });

  const collateralId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
  const debtId = `${ADDRESSES.debt}-${ADDRESSES.pool}`;
  const collateralReserve = mockDb.entities.Reserve.get(collateralId);
  const debtReserve = mockDb.entities.Reserve.get(debtId);
  assert.equal(collateralReserve?.lifetimeLiquidated, 150n * UNIT);
  assert.equal(debtReserve?.lifetimeLiquidated, 0n);

  const liquidationOneId = `${liquidationOne.transaction.hash}-${liquidationOne.logIndex}`;
  const recordOne = mockDb.entities.LiquidationCall.get(liquidationOneId);
  assert.ok(recordOne);
  assert.equal(recordOne?.collateralAssetPriceUSD, 2.5);
  assert.equal(recordOne?.borrowAssetPriceUSD, 0.75);

  const liquidationTwo = TestHelpers.Pool.LiquidationCall.createMockEvent({
    collateralAsset: ADDRESSES.collateralAlt,
    debtAsset: ADDRESSES.debt,
    user: ADDRESSES.user,
    debtToCover: 100n * UNIT,
    liquidatedCollateralAmount: 90n * UNIT,
    liquidator: ADDRESSES.liquidator,
    receiveAToken: false,
    ...eventData(13, 2020, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.LiquidationCall.processEvent({
    event: liquidationTwo,
    mockDb,
  });

  const collateralAltId = `${ADDRESSES.collateralAlt}-${ADDRESSES.pool}`;
  const collateralAlt = mockDb.entities.Reserve.get(collateralAltId);
  const collateralAfter = mockDb.entities.Reserve.get(collateralId);
  assert.equal(collateralAlt?.lifetimeLiquidated, 90n * UNIT);
  assert.equal(collateralAfter?.lifetimeLiquidated, 150n * UNIT);

  const liquidationTwoId = `${liquidationTwo.transaction.hash}-${liquidationTwo.logIndex}`;
  const recordTwo = mockDb.entities.LiquidationCall.get(liquidationTwoId);
  assert.ok(recordTwo);
  assert.equal(recordTwo?.collateralAssetPriceUSD, 4);
  assert.equal(recordTwo?.borrowAssetPriceUSD, 0.75);
});

test('supply, borrow, and repay record prices and referrers', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 3000, undefined);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 3000);
  mockDb = setAssetPrice(mockDb, ADDRESSES.collateral, 2.5, 3000);

  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
  const userReserveId = `${ADDRESSES.user}-${reserveId}`;
  mockDb = mockDb.entities.UserReserve.set({
    id: userReserveId,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.user,
    reserve_id: reserveId,
    scaledATokenBalance: 0n,
    currentATokenBalance: 0n,
    scaledVariableDebt: 22n,
    currentVariableDebt: 0n,
    principalStableDebt: 11n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 3000,
    stableBorrowLastUpdateTimestamp: 0,
  });

  const supplyEvent = TestHelpers.Pool.Supply.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.liquidator,
    onBehalfOf: ADDRESSES.user,
    amount: 100n * UNIT,
    referralCode: 123n,
    ...eventData(14, 3010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.Supply.processEvent({ event: supplyEvent, mockDb });

  const supplyId = `${supplyEvent.transaction.hash}-${supplyEvent.logIndex}`;
  const supply = mockDb.entities.Supply.get(supplyId);
  assert.ok(supply);
  assert.equal(supply?.user_id, ADDRESSES.user);
  assert.equal(supply?.caller_id, ADDRESSES.liquidator);
  assert.equal(supply?.referrer_id, '123');
  assert.equal(supply?.assetPriceUSD, 2.5);

  const borrowEvent = TestHelpers.Pool.Borrow.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.liquidator,
    onBehalfOf: ADDRESSES.user,
    amount: 80n * UNIT,
    interestRateMode: 2n,
    borrowRate: 9n,
    referralCode: 123n,
    ...eventData(15, 3020, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.Borrow.processEvent({ event: borrowEvent, mockDb });

  const borrowId = `${borrowEvent.transaction.hash}-${borrowEvent.logIndex}`;
  const borrow = mockDb.entities.Borrow.get(borrowId);
  assert.ok(borrow);
  assert.equal(borrow?.stableTokenDebt, 11n);
  assert.equal(borrow?.variableTokenDebt, 22n);
  assert.equal(borrow?.borrowRate, 9n);
  assert.equal(borrow?.borrowRateMode, 2);
  assert.equal(borrow?.assetPriceUSD, 2.5);

  const repayEvent = TestHelpers.Pool.Repay.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.user,
    repayer: ADDRESSES.liquidator,
    amount: 20n * UNIT,
    useATokens: true,
    ...eventData(16, 3030, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.Repay.processEvent({ event: repayEvent, mockDb });

  const repayId = `${repayEvent.transaction.hash}-${repayEvent.logIndex}`;
  const repay = mockDb.entities.Repay.get(repayId);
  assert.ok(repay);
  assert.equal(repay?.repayer_id, ADDRESSES.liquidator);
  assert.equal(repay?.useATokens, true);
  assert.equal(repay?.assetPriceUSD, 2.5);

  assert.ok(mockDb.entities.Referrer.get('123'));
});

test('supply without referral code leaves referrer empty', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 3100, undefined);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 3100);
  mockDb = setAssetPrice(mockDb, ADDRESSES.collateral, 1.1, 3100);

  const supplyEvent = TestHelpers.Pool.Supply.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    amount: 10n * UNIT,
    referralCode: 0n,
    ...eventData(20, 3110, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.Supply.processEvent({ event: supplyEvent, mockDb });

  const supplyId = `${supplyEvent.transaction.hash}-${supplyEvent.logIndex}`;
  const supply = mockDb.entities.Supply.get(supplyId);
  assert.ok(supply);
  assert.equal(supply?.referrer_id, undefined);
  assert.equal(supply?.pool_id, ADDRESSES.pool);
});

test('resolvePoolId uses mapping when present', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const mappedPool = '0x0000000000000000000000000000000000003999';

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = mockDb.entities.ContractToPoolMapping.set({
    id: ADDRESSES.pool,
    pool_id: mappedPool,
  });
  mockDb = setAssetPrice(mockDb, ADDRESSES.collateral, 1.2, 3200);

  const supplyEvent = TestHelpers.Pool.Supply.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    amount: 5n * UNIT,
    referralCode: 0n,
    ...eventData(25, 3200, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.Supply.processEvent({ event: supplyEvent, mockDb });

  const supplyId = `${supplyEvent.transaction.hash}-${supplyEvent.logIndex}`;
  const supply = mockDb.entities.Supply.get(supplyId);
  assert.equal(supply?.pool_id, mappedPool);
});

test('reserve data updates skip epoch-end snapshots when gated', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;

  const buildDb = (lastUpdateTimestamp: number) => {
    let db = TestHelpers.MockDb.createMockDb();
    db = db.entities.Protocol.set({ id: '1' });
    db = seedPool(db, ADDRESSES.pool, lastUpdateTimestamp, undefined);
    db = seedReserve(db, ADDRESSES.collateral, ADDRESSES.pool, lastUpdateTimestamp, {
      totalATokenSupply: 100n,
      totalLiquidity: 100n,
      availableLiquidity: 100n,
    });
    return db;
  };

  const makeEvent = (blockNumber: number, timestamp: number) =>
    TestHelpers.Pool.ReserveDataUpdated.createMockEvent({
      reserve: ADDRESSES.collateral,
      liquidityRate: 0n,
      stableBorrowRate: 0n,
      variableBorrowRate: 0n,
      liquidityIndex: RAY,
      variableBorrowIndex: RAY,
      ...eventData(blockNumber, timestamp, ADDRESSES.pool),
    });

  let mockDb = buildDb(900);
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: false,
  });
  mockDb = await TestHelpers.Pool.ReserveDataUpdated.processEvent({
    event: makeEvent(21, 1100),
    mockDb,
  });
  assert.equal(mockDb.entities.ReserveParamsHistoryItem.get(`epochEnd:1:${reserveId}`), undefined);

  mockDb = buildDb(900);
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: false,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: 10n,
    endTime: 1200,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = await TestHelpers.Pool.ReserveDataUpdated.processEvent({
    event: makeEvent(22, 1200),
    mockDb,
  });
  assert.equal(mockDb.entities.ReserveParamsHistoryItem.get(`epochEnd:1:${reserveId}`), undefined);

  mockDb = buildDb(1300);
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: false,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: 10n,
    endTime: 1200,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = await TestHelpers.Pool.ReserveDataUpdated.processEvent({
    event: makeEvent(23, 1301),
    mockDb,
  });
  assert.equal(mockDb.entities.ReserveParamsHistoryItem.get(`epochEnd:1:${reserveId}`), undefined);

  mockDb = buildDb(900);
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: false,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: 10n,
    endTime: 1200,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  const reserve = mockDb.entities.Reserve.get(reserveId);
  mockDb = mockDb.entities.ReserveParamsHistoryItem.set({
    id: `epochEnd:1:${reserveId}`,
    reserve_id: reserveId,
    variableBorrowRate: reserve?.variableBorrowRate || 0n,
    variableBorrowIndex: reserve?.variableBorrowIndex || 0n,
    utilizationRate: reserve?.utilizationRate || 0,
    stableBorrowRate: reserve?.stableBorrowRate || 0n,
    averageStableBorrowRate: reserve?.averageStableRate || 0n,
    liquidityIndex: reserve?.liquidityIndex || 0n,
    liquidityRate: reserve?.liquidityRate || 0n,
    totalLiquidity: reserve?.totalLiquidity || 0n,
    totalATokenSupply: reserve?.totalATokenSupply || 0n,
    totalLiquidityAsCollateral: reserve?.totalLiquidityAsCollateral || 0n,
    availableLiquidity: reserve?.availableLiquidity || 0n,
    priceInEth: reserve?.priceInUsdE8 || 0n,
    priceInUsd: reserve?.priceInUsd || 0,
    timestamp: 1200,
    accruedToTreasury: reserve?.accruedToTreasury || 0n,
    totalScaledVariableDebt: reserve?.totalScaledVariableDebt || 0n,
    totalCurrentVariableDebt: reserve?.totalCurrentVariableDebt || 0n,
    totalPrincipalStableDebt: reserve?.totalPrincipalStableDebt || 0n,
    lifetimePrincipalStableDebt: reserve?.lifetimePrincipalStableDebt || 0n,
    lifetimeScaledVariableDebt: reserve?.lifetimeScaledVariableDebt || 0n,
    lifetimeCurrentVariableDebt: reserve?.lifetimeCurrentVariableDebt || 0n,
    lifetimeLiquidity: reserve?.lifetimeLiquidity || 0n,
    lifetimeRepayments: reserve?.lifetimeRepayments || 0n,
    lifetimeWithdrawals: reserve?.lifetimeWithdrawals || 0n,
    lifetimeBorrows: reserve?.lifetimeBorrows || 0n,
    lifetimeLiquidated: reserve?.lifetimeLiquidated || 0n,
    lifetimeFlashLoans: reserve?.lifetimeFlashLoans || 0n,
    lifetimeFlashLoanPremium: reserve?.lifetimeFlashLoanPremium || 0n,
    lifetimeFlashLoanLPPremium: reserve?.lifetimeFlashLoanLPPremium || 0n,
    lifetimeFlashLoanProtocolPremium: reserve?.lifetimeFlashLoanProtocolPremium || 0n,
    lifetimeReserveFactorAccrued: reserve?.lifetimeReserveFactorAccrued || 0n,
    lifetimePortalLPFee: reserve?.lifetimePortalLPFee || 0n,
    lifetimePortalProtocolFee: reserve?.lifetimePortalProtocolFee || 0n,
    lifetimeSuppliersInterestEarned: reserve?.lifetimeSuppliersInterestEarned || 0n,
  });
  mockDb = await TestHelpers.Pool.ReserveDataUpdated.processEvent({
    event: makeEvent(24, 1300),
    mockDb,
  });

  const snapshot = mockDb.entities.ReserveParamsHistoryItem.get(`epochEnd:1:${reserveId}`);
  assert.equal(snapshot?.timestamp, 1200);
});

test('usage as collateral toggles and records history', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 3000, undefined);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 3000);

  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
  const userReserveId = `${ADDRESSES.user}-${reserveId}`;
  mockDb = mockDb.entities.UserReserve.set({
    id: userReserveId,
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
    lastUpdateTimestamp: 3000,
    stableBorrowLastUpdateTimestamp: 0,
  });

  const enable = TestHelpers.Pool.ReserveUsedAsCollateralEnabled.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.user,
    ...eventData(3, 3010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.ReserveUsedAsCollateralEnabled.processEvent({
    event: enable,
    mockDb,
  });

  const updated = mockDb.entities.UserReserve.get(userReserveId);
  assert.equal(updated?.usageAsCollateralEnabledOnUser, true);

  const historyId = `${enable.transaction.hash}-${enable.logIndex}`;
  const enableRecord = mockDb.entities.UsageAsCollateral.get(historyId);
  assert.ok(enableRecord);
  assert.equal(enableRecord?.fromState, false);
  assert.equal(enableRecord?.toState, true);

  const disable = TestHelpers.Pool.ReserveUsedAsCollateralDisabled.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.user,
    ...eventData(4, 3020, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.ReserveUsedAsCollateralDisabled.processEvent({
    event: disable,
    mockDb,
  });

  const updatedDisabled = mockDb.entities.UserReserve.get(userReserveId);
  assert.equal(updatedDisabled?.usageAsCollateralEnabledOnUser, false);

  const disableId = `${disable.transaction.hash}-${disable.logIndex}`;
  const disableRecord = mockDb.entities.UsageAsCollateral.get(disableId);
  assert.ok(disableRecord);
  assert.equal(disableRecord?.fromState, true);
  assert.equal(disableRecord?.toState, false);
});

test('minted to treasury records history and updates reserve', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 4000, undefined);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 4000);
  mockDb = mockDb.entities.ReserveAggregate.set({
    id: `${ADDRESSES.collateral}-${ADDRESSES.pool}`,
    suppliesUsd: 0,
    borrowsUsd: 0,
    availableUsd: 0,
    suppliesE8: 0n,
    borrowsE8: 0n,
    availableE8: 0n,
    priceE8: 0n,
    lastSuppliersInterestEarnedToken: 0n,
    lastProtocolAccruedToken: 0n,
    updatedAt: 0,
  });

  const mintEvent = TestHelpers.Pool.MintedToTreasury.createMockEvent({
    reserve: ADDRESSES.collateral,
    amountMinted: 25n * UNIT,
    ...eventData(5, 4010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.MintedToTreasury.processEvent({
    event: mintEvent,
    mockDb,
  });

  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
  const reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.lifetimeReserveFactorAccrued, 25n * UNIT);
  const aggregate = mockDb.entities.ReserveAggregate.get(reserveId);
  assert.equal(aggregate?.lastProtocolAccruedToken, 25n * UNIT);

  const historyId = `${mintEvent.transaction.hash}-${mintEvent.logIndex}`;
  assert.ok(mockDb.entities.MintedToTreasury.get(historyId));
});

test('minted to treasury creates reserve aggregate when missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 4500, undefined);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 4500);

  const mintEvent = TestHelpers.Pool.MintedToTreasury.createMockEvent({
    reserve: ADDRESSES.collateral,
    amountMinted: 10n * UNIT,
    ...eventData(6, 4060, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.MintedToTreasury.processEvent({
    event: mintEvent,
    mockDb,
  });

  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
  const aggregate = mockDb.entities.ReserveAggregate.get(reserveId);
  assert.ok(aggregate);
  assert.equal(aggregate?.lastProtocolAccruedToken, 10n * UNIT);
});

test('mint/back unbacked adjusts reserve and portal fees', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 5000, undefined, 3333n);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 5000, {
    totalATokenSupply: 0n,
    totalLiquidity: 0n,
    availableLiquidity: 0n,
  });

  const mintEvent = TestHelpers.Pool.MintUnbacked.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    amount: 100n,
    referralCode: 0n,
    ...eventData(6, 5010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.MintUnbacked.processEvent({
    event: mintEvent,
    mockDb,
  });

  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
  let reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.unbacked, 100n);

  const mintId = `${mintEvent.transaction.hash}-${mintEvent.logIndex}`;
  assert.ok(mockDb.entities.MintUnbacked.get(mintId));

  const backEvent = TestHelpers.Pool.BackUnbacked.createMockEvent({
    reserve: ADDRESSES.collateral,
    backer: ADDRESSES.user,
    amount: 60n,
    fee: 101n,
    ...eventData(7, 5020, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.BackUnbacked.processEvent({
    event: backEvent,
    mockDb,
  });

  reserve = mockDb.entities.Reserve.get(reserveId);
  assert.equal(reserve?.unbacked, 40n);
  assert.equal(reserve?.lifetimePortalProtocolFee, 34n);
  assert.equal(reserve?.lifetimePortalLPFee, 67n);

  const backId = `${backEvent.transaction.hash}-${backEvent.logIndex}`;
  const back = mockDb.entities.BackUnbacked.get(backId);
  assert.ok(back);
  assert.equal(back?.protocolFee, 34n);
  assert.equal(back?.lpFee, 67n);
});

test('user eMode set updates user and creates history', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 6000, undefined);

  const event = TestHelpers.Pool.UserEModeSet.createMockEvent({
    user: ADDRESSES.user,
    categoryId: 2n,
    ...eventData(8, 6010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.UserEModeSet.processEvent({ event, mockDb });

  const user = mockDb.entities.User.get(ADDRESSES.user);
  assert.equal(user?.eModeCategoryId_id, '2');

  const historyId = `${event.transaction.hash}-${event.logIndex}`;
  const record = mockDb.entities.UserEModeSet.get(historyId);
  assert.ok(record);
  assert.equal(record?.categoryId, 2);
});

test('isolation mode total debt updates record', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 7000, undefined);
  mockDb = seedReserve(mockDb, ADDRESSES.collateral, ADDRESSES.pool, 7000);

  const event = TestHelpers.Pool.IsolationModeTotalDebtUpdated.createMockEvent({
    asset: ADDRESSES.collateral,
    totalDebt: 500n,
    ...eventData(9, 7010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.IsolationModeTotalDebtUpdated.processEvent({
    event,
    mockDb,
  });

  const historyId = `${event.transaction.hash}-${event.logIndex}`;
  const record = mockDb.entities.IsolationModeTotalDebtUpdated.get(historyId);
  assert.ok(record);
  assert.equal(record?.isolatedDebt, 500n);
});

test('swap borrow rate mode records direction and rates', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 8000, undefined);
  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.collateral),
    decimals: DECIMALS,
    isActive: true,
    borrowingEnabled: true,
    stableBorrowRate: 7n,
    variableBorrowRate: 9n,
  });

  const event = TestHelpers.Pool.SwapBorrowRateMode.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.user,
    interestRateMode: 1n,
    ...eventData(10, 8010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.SwapBorrowRateMode.processEvent({ event, mockDb });

  const historyId = `${event.transaction.hash}-${event.logIndex}`;
  const record = mockDb.entities.SwapBorrowRate.get(historyId);
  assert.ok(record);
  assert.equal(record?.borrowRateModeFrom, 1);
  assert.equal(record?.borrowRateModeTo, 2);
  assert.equal(record?.stableBorrowRate, 7n);
  assert.equal(record?.variableBorrowRate, 9n);
});

test('rebalance stable borrow rate uses user reserve rates', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = seedPool(mockDb, ADDRESSES.pool, 9000, undefined);
  const reserveId = `${ADDRESSES.collateral}-${ADDRESSES.pool}`;
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
    stableBorrowRate: 11n,
    oldStableBorrowRate: 9n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: 0,
    stableBorrowLastUpdateTimestamp: 0,
  });

  const event = TestHelpers.Pool.RebalanceStableBorrowRate.createMockEvent({
    reserve: ADDRESSES.collateral,
    user: ADDRESSES.user,
    ...eventData(11, 9010, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.RebalanceStableBorrowRate.processEvent({
    event,
    mockDb,
  });

  const historyId = `${event.transaction.hash}-${event.logIndex}`;
  const record = mockDb.entities.RebalanceStableBorrowRate.get(historyId);
  assert.ok(record);
  assert.equal(record?.borrowRateFrom, 9n);
  assert.equal(record?.borrowRateTo, 11n);
});
