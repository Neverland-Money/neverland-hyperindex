import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { createDefaultReserve } from '../helpers/entityHelpers';
import { LEADERBOARD_START_BLOCK, ZERO_ADDRESS } from '../helpers/constants';
import { calculateLinearInterest, rayMul, toDecimal } from '../helpers/math';
import type { t as MockDb } from '../../generated/src/TestHelpers_MockDb.gen';

const DAY = 86400;
const RAY = 10n ** 27n;
const DECIMALS = 6;
const UNIT = 10n ** 6n;

process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'true';
process.env.ENVIO_DISABLE_ETH_CALLS = 'true';

const ADDRESSES = {
  user: '0x0000000000000000000000000000000000000001',
  asset: '0x00000000000000000000000000000000000000a1',
  pool: '0x00000000000000000000000000000000000000b1',
  aToken: '0x00000000000000000000000000000000000000c1',
  vToken: '0x00000000000000000000000000000000000000d1',
  assetTwo: '0x00000000000000000000000000000000000000a2',
  aTokenTwo: '0x00000000000000000000000000000000000000c2',
  vTokenTwo: '0x00000000000000000000000000000000000000d2',
  epochManager: '0x00000000000000000000000000000000000000e1',
  leaderboardConfig: '0x00000000000000000000000000000000000000f1',
  leaderboardKeeper: '0x00000000000000000000000000000000000000f2',
  nftRegistry: '0x00000000000000000000000000000000000000f3',
  nftCollection: '0x00000000000000000000000000000000000000f4',
  vpMultiplier: '0x00000000000000000000000000000000000000f5',
  dustLock: '0x00000000000000000000000000000000000000f6',
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

function seedBaseState(
  mockDb: MockDb,
  params: {
    asset: string;
    pool: string;
    aToken: string;
    vToken: string;
    priceTimestamp: number;
    liquidityRate?: bigint;
  }
) {
  const reserveId = `${params.asset}-${params.pool}`;
  const reserve = {
    ...createDefaultReserve(reserveId, params.pool, params.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    liquidityRate: params.liquidityRate ?? 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: params.priceTimestamp,
    isActive: true,
    borrowingEnabled: true,
  };

  let nextDb = mockDb;
  nextDb = nextDb.entities.Protocol.set({ id: '1' });
  nextDb = nextDb.entities.Pool.set({
    id: params.pool,
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
    lastUpdateTimestamp: params.priceTimestamp,
  });
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
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: params.priceTimestamp,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });

  return { mockDb: nextDb, reserveId };
}

function assertApprox(actual: number | bigint, expected: number, epsilon = 1e-6) {
  const actualNum = typeof actual === 'bigint' ? Number(actual) / 1e18 : actual;
  assert.ok(Math.abs(actualNum - expected) < epsilon, `expected ${expected} got ${actualNum}`);
}

test('accrues across epochs and caps gap settlements', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const preEpochTs = epochStartTs - DAY;
  const midEpochTs = epochStartTs + DAY * 2;
  const epochEndTs = epochStartTs + DAY * 10;
  const gapSettleTs = epochStartTs + DAY * 15;
  const gapSettleTsTwo = epochStartTs + DAY * 18;
  const epoch2StartTs = epochStartTs + DAY * 20;
  const epoch2SettleTs = epoch2StartTs + DAY * 5;

  const baseBlock = LEADERBOARD_START_BLOCK + 100;
  const preEpochBlock = baseBlock - 1;
  const epochStartBlock = baseBlock;
  const midEpochBlock = baseBlock + 2;
  const epochEndBlock = baseBlock + 10;
  const gapBlock = baseBlock + 15;
  const gapBlockTwo = baseBlock + 18;
  const epoch2StartBlock = baseBlock + 20;
  const epoch2SettleBlock = baseBlock + 25;

  ({ mockDb } = seedBaseState(mockDb, {
    asset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aToken,
    vToken: ADDRESSES.vToken,
    priceTimestamp: preEpochTs,
  }));

  const configEvent = TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
    depositRateBps: 10000n,
    borrowRateBps: 20000n,
    vpRateBps: 0n,
    supplyDailyBonus: 0n,
    borrowDailyBonus: 0n,
    repayDailyBonus: 0n,
    withdrawDailyBonus: 0n,
    cooldownSeconds: 0n,
    minDailyBonusUsd: 0n,
    timestamp: BigInt(preEpochTs),
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.leaderboardConfig),
  });
  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: configEvent,
    mockDb,
  });

  const supplyEvent = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyEvent, mockDb });

  const borrowEvent = TestHelpers.VariableDebtToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 500n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(preEpochBlock, preEpochTs + 1, ADDRESSES.vToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Mint.processEvent({ event: borrowEvent, mockDb });

  const epochStartEvent = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 1n,
    startTime: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStartEvent,
    mockDb,
  });

  const supplyMidEvent = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 100n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(midEpochBlock, midEpochTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyMidEvent, mockDb });

  const midStats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(midStats);
  assertApprox(midStats.depositPoints, 2000);
  assertApprox(midStats.borrowPoints, 2000);

  const epochEndEvent = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 1n,
    endTime: BigInt(epochEndTs),
    ...eventData(epochEndBlock, epochEndTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEndEvent,
    mockDb,
  });

  const gapSettleEvent = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(gapSettleTs),
    ...eventData(gapBlock, gapSettleTs, ADDRESSES.leaderboardKeeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: gapSettleEvent,
    mockDb,
  });

  const gapStats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(gapStats);
  assertApprox(gapStats.depositPoints, 10800);
  assertApprox(gapStats.borrowPoints, 10000);

  const gapSettleEventTwo = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(gapSettleTsTwo),
    ...eventData(gapBlockTwo, gapSettleTsTwo, ADDRESSES.leaderboardKeeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: gapSettleEventTwo,
    mockDb,
  });

  const gapStatsTwo = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(gapStatsTwo);
  assertApprox(gapStatsTwo.depositPoints, 10800);
  assertApprox(gapStatsTwo.borrowPoints, 10000);

  const epoch2StartEvent = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 2n,
    startTime: BigInt(epoch2StartTs),
    ...eventData(epoch2StartBlock, epoch2StartTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epoch2StartEvent,
    mockDb,
  });

  const epoch2SettleEvent = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(epoch2SettleTs),
    ...eventData(epoch2SettleBlock, epoch2SettleTs, ADDRESSES.leaderboardKeeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: epoch2SettleEvent,
    mockDb,
  });

  const epoch2Stats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:2`);
  assert.ok(epoch2Stats);
  assertApprox(epoch2Stats.depositPoints, 5500);
  assertApprox(epoch2Stats.borrowPoints, 5000);

  const lifetime = mockDb.entities.UserPoints.get(ADDRESSES.user);
  assert.ok(lifetime);
  assertApprox(lifetime.lifetimeTotalPoints, 31300);

  const userState = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.ok(userState);
  assert.equal(userState.lifetimePoints, lifetime.lifetimeTotalPoints);
});

test('keeper state and NFT multipliers update leaderboard', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const preEpochTs = epochStartTs - DAY;
  const settleTs = epochStartTs + DAY;

  const baseBlock = LEADERBOARD_START_BLOCK + 200;
  const preEpochBlock = baseBlock - 1;
  const epochStartBlock = baseBlock;
  const settleBlock = baseBlock + 1;

  ({ mockDb } = seedBaseState(mockDb, {
    asset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aToken,
    vToken: ADDRESSES.vToken,
    priceTimestamp: preEpochTs,
  }));

  const configEvent = TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
    depositRateBps: 10000n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    supplyDailyBonus: 0n,
    borrowDailyBonus: 0n,
    repayDailyBonus: 0n,
    withdrawDailyBonus: 0n,
    cooldownSeconds: 0n,
    minDailyBonusUsd: 0n,
    timestamp: BigInt(preEpochTs),
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.leaderboardConfig),
  });
  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: configEvent,
    mockDb,
  });

  const supplyEvent = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyEvent, mockDb });

  const epochStartEvent = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 1n,
    startTime: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStartEvent,
    mockDb,
  });

  const nftParamsEvent = TestHelpers.NFTPartnershipRegistry.MultiplierParamsUpdated.createMockEvent(
    {
      oldFirstBonus: 0n,
      newFirstBonus: 10000n,
      oldDecayRatio: 0n,
      newDecayRatio: 0n,
      timestamp: BigInt(epochStartTs),
      totalActivePartnerships: 1n,
      ...eventData(epochStartBlock, epochStartTs, ADDRESSES.nftRegistry),
    }
  );
  mockDb = await TestHelpers.NFTPartnershipRegistry.MultiplierParamsUpdated.processEvent({
    event: nftParamsEvent,
    mockDb,
  });

  const vpSynced = TestHelpers.LeaderboardKeeper.VotingPowerSynced.createMockEvent({
    user: ADDRESSES.user,
    votingPower: 0n,
    timestamp: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.leaderboardKeeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.VotingPowerSynced.processEvent({
    event: vpSynced,
    mockDb,
  });

  const collectionVerified = TestHelpers.LeaderboardKeeper.NFTBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    collection: ADDRESSES.nftCollection,
    balance: 1n,
    timestamp: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.leaderboardKeeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.NFTBalanceSynced.processEvent({
    event: collectionVerified,
    mockDb,
  });

  const settleEvent = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(settleTs),
    ...eventData(settleBlock, settleTs, ADDRESSES.leaderboardKeeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: settleEvent,
    mockDb,
  });

  const epochStats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(epochStats);
  assertApprox(epochStats.depositPoints, 1000);

  const userIndex = mockDb.entities.UserIndex.get(`${ADDRESSES.user}:1`);
  assert.ok(userIndex);
  assertApprox(userIndex.points, 2000);

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.ok(state);
  assert.equal(state.nftMultiplier, 20000n);
  assert.equal(state.combinedMultiplier, 20000n);

  const ownership = mockDb.entities.UserNFTOwnership.get(
    `${ADDRESSES.user}:${ADDRESSES.nftCollection}`
  );
  assert.ok(ownership);

  const collectionCleared = TestHelpers.LeaderboardKeeper.NFTBalanceSynced.createMockEvent({
    user: ADDRESSES.user,
    collection: ADDRESSES.nftCollection,
    balance: 0n,
    timestamp: BigInt(settleTs + 1),
    ...eventData(settleBlock + 1, settleTs + 1, ADDRESSES.leaderboardKeeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.NFTBalanceSynced.processEvent({
    event: collectionCleared,
    mockDb,
  });

  const ownershipCleared = mockDb.entities.UserNFTOwnership.get(
    `${ADDRESSES.user}:${ADDRESSES.nftCollection}`
  );
  assert.equal(ownershipCleared, undefined);
});

test('dust lock voting power applies VP multiplier', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const preEpochTs = epochStartTs - DAY;
  const settleTs = epochStartTs + DAY;

  const baseBlock = LEADERBOARD_START_BLOCK + 300;
  const preEpochBlock = baseBlock - 1;
  const epochStartBlock = baseBlock;
  const settleBlock = baseBlock + 1;

  ({ mockDb } = seedBaseState(mockDb, {
    asset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aToken,
    vToken: ADDRESSES.vToken,
    priceTimestamp: preEpochTs,
  }));

  const configEvent = TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
    depositRateBps: 10000n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    supplyDailyBonus: 0n,
    borrowDailyBonus: 0n,
    repayDailyBonus: 0n,
    withdrawDailyBonus: 0n,
    cooldownSeconds: 0n,
    minDailyBonusUsd: 0n,
    timestamp: BigInt(preEpochTs),
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.leaderboardConfig),
  });
  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: configEvent,
    mockDb,
  });

  const supplyEvent = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyEvent, mockDb });

  const epochStartEvent = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 1n,
    startTime: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStartEvent,
    mockDb,
  });

  const tierAdded = TestHelpers.VotingPowerMultiplier.TierAdded.createMockEvent({
    tierIndex: 1n,
    minVotingPower: 1000n * 10n ** 18n,
    multiplierBps: 15000n,
    totalTiers: 1n,
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.vpMultiplier),
  });
  mockDb = await TestHelpers.VotingPowerMultiplier.TierAdded.processEvent({
    event: tierAdded,
    mockDb,
  });

  const tokenTransfer = TestHelpers.DustLock.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: 1n,
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Transfer.processEvent({ event: tokenTransfer, mockDb });

  const lockPermanent = TestHelpers.DustLock.LockPermanent.createMockEvent({
    sender: ADDRESSES.user,
    tokenId: 1n,
    amount: 2000n * 10n ** 18n,
    ts: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.LockPermanent.processEvent({ event: lockPermanent, mockDb });

  const settleEvent = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(settleTs),
    ...eventData(settleBlock, settleTs, ADDRESSES.leaderboardKeeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: settleEvent,
    mockDb,
  });

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.ok(state);
  assert.equal(state.votingPower, 2000n * 10n ** 18n);
  assert.equal(state.vpMultiplier, 15000n);

  const userIndex = mockDb.entities.UserIndex.get(`${ADDRESSES.user}:1`);
  assert.ok(userIndex);
  assertApprox(userIndex.points, 1500);
});

test('repay and withdraw bonuses only apply during active epochs', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const preEpochTs = epochStartTs - DAY;
  const repayTs = epochStartTs + DAY;
  const withdrawTs = epochStartTs + DAY;
  const epochEndTs = epochStartTs + DAY * 2;
  const gapTs = epochStartTs + DAY * 3;

  const baseBlock = LEADERBOARD_START_BLOCK + 400;
  const preEpochBlock = baseBlock - 1;
  const epochStartBlock = baseBlock;
  const repayBlock = baseBlock + 1;
  const withdrawBlock = baseBlock + 2;
  const epochEndBlock = baseBlock + 3;
  const gapBlock = baseBlock + 4;

  ({ mockDb } = seedBaseState(mockDb, {
    asset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aToken,
    vToken: ADDRESSES.vToken,
    priceTimestamp: preEpochTs,
  }));

  const configEvent = TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    supplyDailyBonus: 0n,
    borrowDailyBonus: 0n,
    repayDailyBonus: 10n * 10n ** 18n,
    withdrawDailyBonus: 5n * 10n ** 18n,
    cooldownSeconds: 0n,
    minDailyBonusUsd: 0n,
    timestamp: BigInt(preEpochTs),
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.leaderboardConfig),
  });
  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: configEvent,
    mockDb,
  });

  const supplyEvent = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyEvent, mockDb });

  const borrowEvent = TestHelpers.VariableDebtToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 500n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(preEpochBlock, preEpochTs + 1, ADDRESSES.vToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Mint.processEvent({ event: borrowEvent, mockDb });

  const epochStartEvent = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 1n,
    startTime: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStartEvent,
    mockDb,
  });

  const repayEvent = TestHelpers.VariableDebtToken.Burn.createMockEvent({
    from: ADDRESSES.user,
    target: ADDRESSES.user,
    value: 100n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(repayBlock, repayTs, ADDRESSES.vToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Burn.processEvent({ event: repayEvent, mockDb });

  const withdrawEvent = TestHelpers.AToken.Burn.createMockEvent({
    from: ADDRESSES.user,
    target: ADDRESSES.user,
    value: 50n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(withdrawBlock, withdrawTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Burn.processEvent({ event: withdrawEvent, mockDb });

  const activeStats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(activeStats);
  assertApprox(activeStats.dailyRepayPoints, 10);
  assertApprox(activeStats.dailyWithdrawPoints, 5);

  const userReserve = mockDb.entities.UserReserve.get(
    `${ADDRESSES.user}-${ADDRESSES.asset}-${ADDRESSES.pool}`
  );
  assert.ok(userReserve);
  assert.equal(userReserve.currentVariableDebt, 400n * UNIT);
  assert.equal(userReserve.currentATokenBalance, 950n * UNIT);

  const epochEndEvent = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 1n,
    endTime: BigInt(epochEndTs),
    ...eventData(epochEndBlock, epochEndTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEndEvent,
    mockDb,
  });

  const gapRepay = TestHelpers.VariableDebtToken.Burn.createMockEvent({
    from: ADDRESSES.user,
    target: ADDRESSES.user,
    value: 50n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(gapBlock, gapTs, ADDRESSES.vToken),
  });
  mockDb = await TestHelpers.VariableDebtToken.Burn.processEvent({ event: gapRepay, mockDb });

  const gapWithdraw = TestHelpers.AToken.Burn.createMockEvent({
    from: ADDRESSES.user,
    target: ADDRESSES.user,
    value: 20n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(gapBlock + 1, gapTs + 1, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Burn.processEvent({ event: gapWithdraw, mockDb });

  const gapStats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(gapStats);
  assertApprox(gapStats.dailyRepayPoints, 10);
  assertApprox(gapStats.dailyWithdrawPoints, 5);
});

test('gap settlements use epoch-end indices snapshots', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const preEpochTs = epochStartTs - DAY;
  const epochEndTs = epochStartTs + DAY * 2;
  const reserveUpdateTs = epochStartTs + DAY * 3;
  const gapSettleTs = epochStartTs + DAY * 4;

  const baseBlock = LEADERBOARD_START_BLOCK + 500;
  const preEpochBlock = baseBlock - 1;
  const epochStartBlock = baseBlock;
  const epochEndBlock = baseBlock + 2;
  const reserveUpdateBlock = baseBlock + 3;
  const gapSettleBlock = baseBlock + 4;

  const seeded = seedBaseState(mockDb, {
    asset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aToken,
    vToken: ADDRESSES.vToken,
    priceTimestamp: preEpochTs,
    liquidityRate: RAY,
  });
  mockDb = seeded.mockDb;

  const configEvent = TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    supplyDailyBonus: 0n,
    borrowDailyBonus: 0n,
    repayDailyBonus: 0n,
    withdrawDailyBonus: 0n,
    cooldownSeconds: 0n,
    minDailyBonusUsd: 0n,
    timestamp: BigInt(preEpochTs),
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.leaderboardConfig),
  });
  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: configEvent,
    mockDb,
  });

  const supplyEvent = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyEvent, mockDb });

  const epochStartEvent = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 1n,
    startTime: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStartEvent,
    mockDb,
  });

  const epochEndEvent = TestHelpers.EpochManager.EpochEnd.createMockEvent({
    epochNumber: 1n,
    endTime: BigInt(epochEndTs),
    ...eventData(epochEndBlock, epochEndTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({
    event: epochEndEvent,
    mockDb,
  });

  const reserveUpdate = TestHelpers.Pool.ReserveDataUpdated.createMockEvent({
    reserve: ADDRESSES.asset,
    liquidityRate: RAY,
    stableBorrowRate: 0n,
    variableBorrowRate: 0n,
    liquidityIndex: 2n * RAY,
    variableBorrowIndex: 2n * RAY,
    ...eventData(reserveUpdateBlock, reserveUpdateTs, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.ReserveDataUpdated.processEvent({
    event: reserveUpdate,
    mockDb,
  });

  const gapSettle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user: ADDRESSES.user,
    timestamp: BigInt(gapSettleTs),
    ...eventData(gapSettleBlock, gapSettleTs, ADDRESSES.leaderboardKeeper),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: gapSettle,
    mockDb,
  });

  const interest = calculateLinearInterest(RAY, BigInt(preEpochTs), BigInt(epochEndTs));
  const indexAtEnd = rayMul(RAY + interest, RAY);
  const expectedSupply = rayMul(1000n * UNIT, indexAtEnd);

  const points = mockDb.entities.UserReservePoints.get(
    `${ADDRESSES.user}:${ADDRESSES.asset}-${ADDRESSES.pool}`
  );
  assert.ok(points);
  assert.equal(points.lastDepositTokens, toDecimal(expectedSupply, DECIMALS));
});

test('cooldown only settles the interacted reserve', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const preEpochTs = epochStartTs - DAY;
  const firstSettleTs = epochStartTs + DAY;
  const cooldownSettleTs = firstSettleTs + 3600;

  const baseBlock = LEADERBOARD_START_BLOCK + 700;
  const preEpochBlock = baseBlock - 1;
  const epochStartBlock = baseBlock;
  const firstSettleBlock = baseBlock + 1;
  const cooldownSettleBlock = baseBlock + 2;

  ({ mockDb } = seedBaseState(mockDb, {
    asset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aToken,
    vToken: ADDRESSES.vToken,
    priceTimestamp: preEpochTs,
  }));

  const reserveOneId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  const reserveTwoId = `${ADDRESSES.assetTwo}-${ADDRESSES.pool}`;
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveTwoId, ADDRESSES.pool, ADDRESSES.assetTwo),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: preEpochTs,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aTokenTwo,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.assetTwo,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.vTokenTwo,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.assetTwo,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.PriceOracleAsset.set({
    id: ADDRESSES.assetTwo,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: preEpochTs,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });

  const configEvent = TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
    depositRateBps: 10000n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    supplyDailyBonus: 0n,
    borrowDailyBonus: 0n,
    repayDailyBonus: 0n,
    withdrawDailyBonus: 0n,
    cooldownSeconds: BigInt(DAY * 2),
    minDailyBonusUsd: 0n,
    timestamp: BigInt(preEpochTs),
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.leaderboardConfig),
  });
  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: configEvent,
    mockDb,
  });

  const supplyReserveOne = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 1000n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supplyReserveOne, mockDb });

  const userReserveTwoId = `${ADDRESSES.user}-${reserveTwoId}`;
  mockDb = mockDb.entities.UserReserve.set({
    id: userReserveTwoId,
    pool_id: ADDRESSES.pool,
    user_id: ADDRESSES.user,
    reserve_id: reserveTwoId,
    scaledATokenBalance: 500n * UNIT,
    currentATokenBalance: 500n * UNIT,
    scaledVariableDebt: 0n,
    currentVariableDebt: 0n,
    principalStableDebt: 0n,
    currentStableDebt: 0n,
    currentTotalDebt: 0n,
    stableBorrowRate: 0n,
    oldStableBorrowRate: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: preEpochTs,
    stableBorrowLastUpdateTimestamp: 0,
  });
  mockDb = mockDb.entities.UserReserveList.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    reserveIds: [reserveOneId, reserveTwoId],
    lastUpdate: preEpochTs,
  });

  const epochStartEvent = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 1n,
    startTime: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStartEvent,
    mockDb,
  });

  const firstSettle = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 0n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(firstSettleBlock, firstSettleTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: firstSettle, mockDb });

  const statsAfterFirst = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(statsAfterFirst);
  assertApprox(statsAfterFirst.depositPoints, 1500);

  const cooldownSettle = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 0n,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(cooldownSettleBlock, cooldownSettleTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: cooldownSettle, mockDb });

  const statsAfterCooldown = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(statsAfterCooldown);
  assertApprox(statsAfterCooldown.depositPoints, 1500 + 1000 / 24);
});

test('daily supply bonus respects min usd threshold', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const preEpochTs = epochStartTs - DAY;
  const supplyTs = epochStartTs + 3600;

  const baseBlock = LEADERBOARD_START_BLOCK + 800;
  const preEpochBlock = baseBlock - 1;
  const epochStartBlock = baseBlock;
  const supplyBlock = baseBlock + 1;

  ({ mockDb } = seedBaseState(mockDb, {
    asset: ADDRESSES.asset,
    pool: ADDRESSES.pool,
    aToken: ADDRESSES.aToken,
    vToken: ADDRESSES.vToken,
    priceTimestamp: preEpochTs,
  }));

  const configEvent = TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    supplyDailyBonus: 10n * 10n ** 18n,
    borrowDailyBonus: 0n,
    repayDailyBonus: 0n,
    withdrawDailyBonus: 0n,
    cooldownSeconds: 0n,
    minDailyBonusUsd: 100n,
    timestamp: BigInt(preEpochTs),
    ...eventData(preEpochBlock, preEpochTs, ADDRESSES.leaderboardConfig),
  });
  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: configEvent,
    mockDb,
  });

  const epochStartEvent = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 1n,
    startTime: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStartEvent,
    mockDb,
  });

  const smallSupply = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 50n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(supplyBlock, supplyTs, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: smallSupply, mockDb });

  const statsAfterSmall = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(statsAfterSmall);
  assertApprox(statsAfterSmall.dailySupplyPoints, 0);

  const largerSupply = TestHelpers.AToken.Mint.createMockEvent({
    caller: ADDRESSES.user,
    onBehalfOf: ADDRESSES.user,
    value: 60n * UNIT,
    balanceIncrease: 0n,
    index: RAY,
    ...eventData(supplyBlock + 1, supplyTs + 10, ADDRESSES.aToken),
  });
  mockDb = await TestHelpers.AToken.Mint.processEvent({ event: largerSupply, mockDb });

  const statsAfterLarge = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(statsAfterLarge);
  assertApprox(statsAfterLarge.dailySupplyPoints, 10);
});

test('manual points updates epoch and lifetime totals', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const baseBlock = LEADERBOARD_START_BLOCK + 900;
  const epochStartBlock = baseBlock;

  const epochStartEvent = TestHelpers.EpochManager.EpochStart.createMockEvent({
    epochNumber: 1n,
    startTime: BigInt(epochStartTs),
    ...eventData(epochStartBlock, epochStartTs, ADDRESSES.epochManager),
  });
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: epochStartEvent,
    mockDb,
  });

  const awarded = TestHelpers.LeaderboardConfig.PointsAwarded.createMockEvent({
    user: ADDRESSES.user,
    points: 100n * 10n ** 18n,
    reason: 'bonus',
    timestamp: BigInt(epochStartTs + 1),
    ...eventData(epochStartBlock + 1, epochStartTs + 1, ADDRESSES.leaderboardConfig),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsAwarded.processEvent({
    event: awarded,
    mockDb,
  });

  const removed = TestHelpers.LeaderboardConfig.PointsRemoved.createMockEvent({
    user: ADDRESSES.user,
    points: 40n * 10n ** 18n,
    reason: 'correction',
    timestamp: BigInt(epochStartTs + 2),
    ...eventData(epochStartBlock + 2, epochStartTs + 2, ADDRESSES.leaderboardConfig),
  });
  mockDb = await TestHelpers.LeaderboardConfig.PointsRemoved.processEvent({
    event: removed,
    mockDb,
  });

  const manualAwardId = `${awarded.transaction.hash}-${awarded.logIndex}`;
  const manualAward = mockDb.entities.ManualPointsAward.get(manualAwardId);
  assert.ok(manualAward);
  assertApprox(manualAward.points, 100);

  const manualRemoveId = `${removed.transaction.hash}-${removed.logIndex}`;
  const manualRemove = mockDb.entities.ManualPointsAward.get(manualRemoveId);
  assert.ok(manualRemove);
  assertApprox(manualRemove.points, -40);

  const stats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  assert.ok(stats);
  assertApprox(stats.manualAwardPoints, 60);
  assertApprox(stats.totalPoints, 60);

  const lifetime = mockDb.entities.UserPoints.get(ADDRESSES.user);
  assert.ok(lifetime);
  assertApprox(lifetime.lifetimeTotalPoints, 60);

  const epochIndex = mockDb.entities.UserIndex.get(`${ADDRESSES.user}:1`);
  assert.ok(epochIndex);
  assertApprox(epochIndex.points, 60);

  const allTimeIndex = mockDb.entities.UserIndex.get(`${ADDRESSES.user}:0`);
  assert.ok(allTimeIndex);
  assertApprox(allTimeIndex.points, 60);
});
