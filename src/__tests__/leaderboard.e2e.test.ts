import assert from 'node:assert/strict';
import { test } from 'node:test';

import { createDefaultReserve } from '../helpers/entityHelpers';
import {
  BALANCER_AUTORANGE_V3_POOL_ADDRESS,
  BALANCER_VAULT_ADDRESS,
  LEADERBOARD_START_BLOCK,
  LP_BALANCER_AUTORANGE_CUTOVER_BLOCK,
  LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  LP_V2_CUTOVER_BLOCK,
  LP_V2_CUTOVER_TIMESTAMP,
  USDC_ADDRESS,
  ZERO_ADDRESS,
} from '../helpers/constants';
import { LP_GROWTH_Q128 } from '../helpers/lpGrowthMath';
import { calculateLinearInterest, rayMul, toDecimal } from '../helpers/math';
import { getSqrtRatioAtTick } from '../helpers/uniswapV3';
import { TestHelpers, type MockDb, type EntityRow } from './v3-test-helpers';

const DAY = 86400;
const RAY = 10n ** 27n;
const DECIMALS = 6;
const UNIT = 10n ** 6n;
// Above the statically configured leaderboard-contract start blocks, but still
// before the first historical LP cutover. Lower values are interpreted by the
// V3 compatibility seam as relative block offsets.
const E2E_BASE_BLOCK = LEADERBOARD_START_BLOCK + 300_000;

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

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
  userTwo: '0x0000000000000000000000000000000000000102',
  ceremonyV3Pool: '0x0000000000000000000000000000000000000103',
  ceremonyV3Manager: '0x0000000000000000000000000000000000000104',
  ceremonyV3Token1: '0x0000000000000000000000000000000000000105',
  ceremonyV2Pool: '0x0000000000000000000000000000000000000106',
  ceremonyV2Token1: '0x0000000000000000000000000000000000000107',
};

const CEREMONY_DUST_TOKEN = '0xad96c3dffcd6374294e2573a7fbba96097cc8d7c';
const CEREMONY_LEGACY_V3_POOL = '0xd15965968fe8bf2babbe39b2fc5de1ab6749141f';
const CEREMONY_CANONICAL_V2_POOL = '0x86dbf00485871c901c5129bd525348db96c2eb2d';
const CEREMONY_RATE_BPS = 10_000n;
const CEREMONY_PRICE_E8 = 100_000_000n;
const CEREMONY_POINTS_SCALE = 10n ** 18n;
const CEREMONY_Q96 = 1n << 96n;
const CEREMONY_TICK_LOWER = -120;
const CEREMONY_TICK_UPPER = 120;
const CEREMONY_V3_LIQUIDITY = 1_000n;
const CEREMONY_V2_LIQUIDITY = 100n;

type TestHelpersApi = typeof TestHelpers;

function loadTestHelpers(): TestHelpersApi {
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

function ceremonyFungibleGrowthX128(
  intervals: readonly {
    reserve0: bigint;
    reserve1: bigint;
    token0PriceE8: bigint;
    token1PriceE8: bigint;
    seconds: number;
  }[]
): bigint {
  return intervals.reduce((growth, interval) => {
    const poolValueUsdE8 =
      interval.reserve0 * interval.token0PriceE8 + interval.reserve1 * interval.token1PriceE8;
    const unitValueUsdE8X128 = (poolValueUsdE8 * LP_GROWTH_Q128) / 1_000n;
    return growth + unitValueUsdE8X128 * CEREMONY_RATE_BPS * BigInt(interval.seconds);
  }, 0n);
}

function ceremonyGrowthToPoints(liquidity: bigint, growthX128: bigint): bigint {
  return (
    (liquidity * growthX128 * CEREMONY_POINTS_SCALE) /
    (LP_GROWTH_Q128 * CEREMONY_PRICE_E8 * 10_000n * BigInt(DAY))
  );
}

function applyCeremonyMultiplier(points: bigint, multiplierBps: bigint): bigint {
  return (points * multiplierBps) / 10_000n;
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

  const baseBlock = E2E_BASE_BLOCK + 100;
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

  const baseBlock = E2E_BASE_BLOCK + 200;
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

  const baseBlock = E2E_BASE_BLOCK + 300;
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

  const baseBlock = E2E_BASE_BLOCK + 400;
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
  assert.equal(userReserve.currentDebt, 400n * UNIT);
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

  const baseBlock = E2E_BASE_BLOCK + 500;
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

test('lending actions settle supply and borrow reserves during cooldown', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const preEpochTs = epochStartTs - DAY;
  const firstSettleTs = epochStartTs + DAY;
  const cooldownSettleTs = firstSettleTs + 3600;

  const baseBlock = E2E_BASE_BLOCK + 700;
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
    borrowRateBps: 10000n,
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
    scaledATokenBalance: 0n,
    currentATokenBalance: 0n,
    scaledDebt: 500n * UNIT,
    currentDebt: 500n * UNIT,
    liquidityRate: 0n,
    variableBorrowIndex: RAY,
    usageAsCollateralEnabledOnUser: false,
    lastUpdateTimestamp: preEpochTs,
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
  assertApprox(statsAfterFirst.depositPoints, 1000);
  assertApprox(statsAfterFirst.borrowPoints, 500);

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
  assertApprox(statsAfterCooldown.depositPoints, 1000 + 1000 / 24);
  assertApprox(statsAfterCooldown.borrowPoints, 500 + 500 / 24);
});

test('daily supply bonus respects min usd threshold', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const epochStartTs = DAY * 10;
  const preEpochTs = epochStartTs - DAY;
  const supplyTs = epochStartTs + 3600;

  const baseBlock = E2E_BASE_BLOCK + 800;
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
  const baseBlock = E2E_BASE_BLOCK + 900;
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

test('Task 8 Tide ceremony preserves lazy LP parity, multiplier splits, and immutable gap proofs', async () => {
  const previousFinalOnlyFloor = process.env.ENVIO_KEEPER_FINAL_ONLY_FROM_EPOCH;
  delete process.env.ENVIO_KEEPER_FINAL_ONLY_FROM_EPOCH;

  async function runCeremony(includeRedundantV3Swap: boolean) {
    const TestHelpers = loadTestHelpers();
    const eventData = createEventDataFactory();
    const hour = 3_600;
    const epochNumber = 42n;
    const nextEpochNumber = 43n;
    const startTime = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + hour;
    const v3FirstSwapTime = startTime + 6 * hour;
    const redundantSwapTime = startTime + 8 * hour;
    const v2SyncTime = startTime + 9 * hour;
    const balancerSwapTime = startTime + 10 * hour;
    const multiplierChangeTime = startTime + 12 * hour;
    const v3SecondSwapTime = startTime + 18 * hour;
    const endTime = startTime + DAY;
    const firstGapTime = endTime + hour;
    const duplicateGapTime = endTime + 2 * hour;
    const secondUserGapTime = endTime + 3 * hour;
    const nextStartTime = endTime + 4 * hour;
    const baseBlock = LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1_000;
    const sqrtAtOne = getSqrtRatioAtTick(1);
    const primaryV3Position = `ceremony-v3:${ADDRESSES.user}`;
    const primaryV2Position = `v2:${ADDRESSES.ceremonyV2Pool}:${ADDRESSES.user}`;
    const primaryBalancerPosition = `v2:${BALANCER_AUTORANGE_V3_POOL_ADDRESS}:${ADDRESSES.user}`;
    const secondaryV3Position = `ceremony-v3:${ADDRESSES.userTwo}`;
    const secondaryV2Position = `v2:${ADDRESSES.ceremonyV2Pool}:${ADDRESSES.userTwo}`;
    const secondaryBalancerPosition = `v2:${BALANCER_AUTORANGE_V3_POOL_ADDRESS}:${ADDRESSES.userTwo}`;

    let mockDb = TestHelpers.MockDb.createMockDb();
    ({ mockDb } = seedBaseState(mockDb, {
      asset: ADDRESSES.asset,
      pool: ADDRESSES.pool,
      aToken: ADDRESSES.aToken,
      vToken: ADDRESSES.vToken,
      priceTimestamp: startTime,
    }));
    mockDb = mockDb.entities.LeaderboardConfig.set({
      id: 'global',
      depositRateBps: CEREMONY_RATE_BPS,
      borrowRateBps: 0n,
      vpRateBps: CEREMONY_RATE_BPS,
      lpRateBps: 0n,
      supplyDailyBonus: 0,
      borrowDailyBonus: 0,
      repayDailyBonus: 0,
      withdrawDailyBonus: 0,
      cooldownSeconds: 0,
      minDailyBonusUsd: 0,
      lastUpdate: startTime,
    });
    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: epochNumber - 1n,
      isActive: false,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: (epochNumber - 1n).toString(),
      epochNumber: epochNumber - 1n,
      startBlock: BigInt(baseBlock - 2),
      startTime: startTime - 2 * hour,
      endBlock: BigInt(baseBlock - 1),
      endTime: startTime - hour,
      isActive: false,
      duration: BigInt(hour),
      scheduledStartTime: startTime - 2 * hour,
      scheduledEndTime: startTime - hour,
    });
    mockDb = mockDb.entities.NFTMultiplierConfig.set({
      id: 'current',
      firstBonus: 5_000n,
      decayRatio: 10_000n,
      lastUpdate: startTime,
    });
    mockDb = mockDb.entities.NFTPartnershipRegistryState.set({
      id: 'current',
      activeCollections: [ADDRESSES.nftCollection],
      lastUpdate: startTime,
    });
    mockDb = mockDb.entities.NFTPartnership.set({
      id: ADDRESSES.nftCollection,
      collection: ADDRESSES.nftCollection,
      name: 'Ceremony collection',
      active: true,
      staticBoostBps: undefined,
      startTimestamp: startTime,
      endTimestamp: undefined,
      addedAt: startTime,
      lastUpdate: startTime,
    });
    mockDb = mockDb.entities.LPStaticTransition.set({
      id: 'legacy-v3-to-v2',
      outgoingPool: CEREMONY_LEGACY_V3_POOL,
      incomingPool: CEREMONY_CANONICAL_V2_POOL,
      blockNumber: BigInt(LP_V2_CUTOVER_BLOCK),
      timestamp: LP_V2_CUTOVER_TIMESTAMP,
    });
    mockDb = mockDb.entities.LPStaticTransition.set({
      id: 'v2-to-balancer-autorange',
      outgoingPool: CEREMONY_CANONICAL_V2_POOL,
      incomingPool: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
      blockNumber: BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK),
      timestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    });
    mockDb = mockDb.entities.LPPoolRegistry.set({
      id: 'global',
      poolIds: [
        ADDRESSES.ceremonyV3Pool,
        ADDRESSES.ceremonyV2Pool,
        BALANCER_AUTORANGE_V3_POOL_ADDRESS,
      ],
      lastUpdate: startTime,
    });

    const poolConfigs = [
      {
        pool: ADDRESSES.ceremonyV3Pool,
        positionManager: ADDRESSES.ceremonyV3Manager,
        token0: USDC_ADDRESS,
        token1: ADDRESSES.ceremonyV3Token1,
        fee: 3_000,
      },
      {
        pool: ADDRESSES.ceremonyV2Pool,
        positionManager: ADDRESSES.ceremonyV2Pool,
        token0: USDC_ADDRESS,
        token1: ADDRESSES.ceremonyV2Token1,
        fee: 3_000,
      },
      {
        pool: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
        positionManager: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
        token0: USDC_ADDRESS,
        token1: CEREMONY_DUST_TOKEN,
        fee: 10_000,
      },
    ];
    for (const config of poolConfigs) {
      mockDb = mockDb.entities.LPPoolConfig.set({
        id: config.pool,
        ...config,
        // The concentrated pool runs at a zero LP rate here. Its swaps, freezes and gap
        // certificates still flow through every handler, but it contributes no points, so
        // every LP expectation below stays closed-form on the two fungible legs. The V3
        // per-swap point arithmetic is covered directly in lp-events.test.ts.
        lpRateBps: config.pool === ADDRESSES.ceremonyV3Pool ? 0n : CEREMONY_RATE_BPS,
        isActive: true,
        enabledAtEpoch: epochNumber,
        enabledAtTimestamp: startTime,
        disabledAtEpoch: undefined,
        disabledAtTimestamp: undefined,
        lastUpdate: startTime,
      });
      mockDb = mockDb.entities.LPPoolState.set({
        id: config.pool,
        pool: config.pool,
        currentTick: 0,
        sqrtPriceX96: config.pool === ADDRESSES.ceremonyV3Pool ? CEREMONY_Q96 : 0n,
        token0Price: CEREMONY_PRICE_E8,
        token1Price: CEREMONY_PRICE_E8,
        feeProtocol0: 0,
        feeProtocol1: 0,
        lastUpdate: startTime,
      });
      if (config.pool !== ADDRESSES.ceremonyV3Pool) {
        mockDb = mockDb.entities.LPPoolV2State.set({
          id: config.pool,
          pool: config.pool,
          reserve0: 500n,
          reserve1: 500n,
          lpTotalSupply: 1_000n,
          lastUpdate: startTime,
        });
      }
    }
    for (const [address, symbol] of [
      [USDC_ADDRESS, 'USDC'],
      [ADDRESSES.ceremonyV3Token1, 'V3X'],
      [ADDRESSES.ceremonyV2Token1, 'V2X'],
      [CEREMONY_DUST_TOKEN, 'DUST'],
    ] as const) {
      mockDb = mockDb.entities.TokenInfo.set({
        id: address,
        address,
        decimals: 0,
        symbol,
        name: `${symbol} ceremony token`,
        lastUpdate: startTime,
      });
    }

    const positions = [
      {
        id: primaryV3Position,
        tokenId: 8_001n,
        user: ADDRESSES.user,
        pool: ADDRESSES.ceremonyV3Pool,
        manager: ADDRESSES.ceremonyV3Manager,
        tickLower: CEREMONY_TICK_LOWER,
        tickUpper: CEREMONY_TICK_UPPER,
        liquidity: CEREMONY_V3_LIQUIDITY,
      },
      {
        id: primaryV2Position,
        tokenId: 8_002n,
        user: ADDRESSES.user,
        pool: ADDRESSES.ceremonyV2Pool,
        manager: ADDRESSES.ceremonyV2Pool,
        tickLower: -887_272,
        tickUpper: 887_272,
        liquidity: CEREMONY_V2_LIQUIDITY,
      },
      {
        id: primaryBalancerPosition,
        tokenId: 8_003n,
        user: ADDRESSES.user,
        pool: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
        manager: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
        tickLower: -887_272,
        tickUpper: 887_272,
        liquidity: CEREMONY_V2_LIQUIDITY,
      },
      {
        id: secondaryV3Position,
        tokenId: 8_004n,
        user: ADDRESSES.userTwo,
        pool: ADDRESSES.ceremonyV3Pool,
        manager: ADDRESSES.ceremonyV3Manager,
        tickLower: CEREMONY_TICK_LOWER,
        tickUpper: CEREMONY_TICK_UPPER,
        liquidity: CEREMONY_V3_LIQUIDITY / 2n,
      },
      {
        id: secondaryV2Position,
        tokenId: 8_005n,
        user: ADDRESSES.userTwo,
        pool: ADDRESSES.ceremonyV2Pool,
        manager: ADDRESSES.ceremonyV2Pool,
        tickLower: -887_272,
        tickUpper: 887_272,
        liquidity: CEREMONY_V2_LIQUIDITY / 2n,
      },
      {
        id: secondaryBalancerPosition,
        tokenId: 8_006n,
        user: ADDRESSES.userTwo,
        pool: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
        manager: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
        tickLower: -887_272,
        tickUpper: 887_272,
        liquidity: CEREMONY_V2_LIQUIDITY / 2n,
      },
    ];
    for (const position of positions) {
      mockDb = mockDb.entities.UserLPPosition.set({
        id: position.id,
        tokenId: position.tokenId,
        user_id: position.user,
        pool: position.pool,
        positionManager: position.manager,
        tickLower: position.tickLower,
        tickUpper: position.tickUpper,
        liquidity: position.liquidity,
        amount0: 0n,
        amount1: 0n,
        isInRange: true,
        valueUsd: 0n,
        lastInRangeTimestamp: startTime,
        accumulatedInRangeSeconds: 0n,
        lastSettledAt: startTime,
        settledLpPoints: 0n,
        createdAt: startTime - 1,
        lastUpdate: startTime,
      });
    }
    mockDb = mockDb.entities.UserLPPositionIndex.set({
      id: ADDRESSES.user,
      user_id: ADDRESSES.user,
      positionIds: [primaryV3Position, primaryV2Position, primaryBalancerPosition],
      lastUpdate: startTime,
    });
    mockDb = mockDb.entities.UserLPPositionIndex.set({
      id: ADDRESSES.userTwo,
      user_id: ADDRESSES.userTwo,
      positionIds: [secondaryV3Position, secondaryV2Position, secondaryBalancerPosition],
      lastUpdate: startTime,
    });
    mockDb = mockDb.entities.UserTokenList.set({
      id: ADDRESSES.user,
      user_id: ADDRESSES.user,
      tokenIds: [8_100n],
      lastUpdate: startTime,
    });
    mockDb = mockDb.entities.UserTokenList.set({
      id: ADDRESSES.userTwo,
      user_id: ADDRESSES.userTwo,
      tokenIds: [],
      lastUpdate: startTime,
    });
    mockDb = mockDb.entities.DustLockToken.set({
      id: '8100',
      owner: ADDRESSES.user,
      lockedAmount: CEREMONY_POINTS_SCALE,
      end: 0,
      isPermanent: true,
      createdAt: startTime,
      updatedAt: startTime,
      lastDepositType: undefined,
      selfRepayEnabled: false,
      rewardReceiver: undefined,
    });

    const epochStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
      epochNumber,
      startTime: BigInt(startTime),
      ...eventData(baseBlock, startTime, ADDRESSES.epochManager),
    });
    mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
      event: epochStart,
      mockDb,
    });
    const supply = TestHelpers.AToken.Mint.createMockEvent({
      caller: ADDRESSES.user,
      onBehalfOf: ADDRESSES.user,
      value: 1_000n * UNIT,
      balanceIncrease: 0n,
      index: RAY,
      ...eventData(baseBlock + 1, startTime, ADDRESSES.aToken),
    });
    mockDb = await TestHelpers.AToken.Mint.processEvent({ event: supply, mockDb });

    const v3FirstSwap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
      sender: ADDRESSES.user,
      recipient: ADDRESSES.user,
      amount0: 0n,
      amount1: 0n,
      sqrtPriceX96: sqrtAtOne,
      liquidity: 0n,
      tick: 1n,
      ...eventData(baseBlock + 2, v3FirstSwapTime, ADDRESSES.ceremonyV3Pool),
    });
    mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
      event: v3FirstSwap,
      mockDb,
    });
    if (includeRedundantV3Swap) {
      const redundantV3Swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
        sender: ADDRESSES.user,
        recipient: ADDRESSES.user,
        amount0: 0n,
        amount1: 0n,
        sqrtPriceX96: sqrtAtOne,
        liquidity: 0n,
        tick: 1n,
        ...eventData(baseBlock + 3, redundantSwapTime, ADDRESSES.ceremonyV3Pool),
      });
      mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
        event: redundantV3Swap,
        mockDb,
      });
    }
    const v2Sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
      reserve0: 600n,
      reserve1: 400n,
      ...eventData(baseBlock + 4, v2SyncTime, ADDRESSES.ceremonyV2Pool),
    });
    mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({ event: v2Sync, mockDb });
    const balancerSwap = TestHelpers.BalancerVault.Swap.createMockEvent({
      pool: BALANCER_AUTORANGE_V3_POOL_ADDRESS,
      tokenIn: USDC_ADDRESS,
      tokenOut: CEREMONY_DUST_TOKEN,
      amountIn: 50n,
      amountOut: 50n,
      swapFeePercentage: 10n ** 16n,
      swapFeeAmount: 0n,
      ...eventData(baseBlock + 5, balancerSwapTime, BALANCER_VAULT_ADDRESS),
    });
    mockDb = await TestHelpers.BalancerVault.Swap.processEvent({
      event: balancerSwap,
      mockDb,
    });
    const multiplierChange = TestHelpers.PartnerNFT.Transfer.createMockEvent({
      from: ZERO_ADDRESS,
      to: ADDRESSES.user,
      id: 42n,
      ...eventData(baseBlock + 6, multiplierChangeTime, ADDRESSES.nftCollection),
    });
    mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
      event: multiplierChange,
      mockDb,
    });
    const statsAtMultiplierBoundary = mockDb.entities.UserEpochStats.get(
      `${ADDRESSES.user}:${epochNumber.toString()}`
    );
    assert.ok(
      statsAtMultiplierBoundary,
      `missing boundary stats state=${JSON.stringify(
        mockDb.entities.LeaderboardState.get('current'),
        (_, value) => (typeof value === 'bigint' ? value.toString() : value)
      )} ownership=${JSON.stringify(
        mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${ADDRESSES.nftCollection}`),
        (_, value) => (typeof value === 'bigint' ? value.toString() : value)
      )}`
    );
    assert.equal(
      mockDb.entities.UserLeaderboardState.get(ADDRESSES.user)?.combinedMultiplier,
      15_000n
    );

    const v3SecondSwap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
      sender: ADDRESSES.user,
      recipient: ADDRESSES.user,
      amount0: 0n,
      amount1: 0n,
      sqrtPriceX96: CEREMONY_Q96,
      liquidity: 0n,
      tick: 0n,
      ...eventData(baseBlock + 7, v3SecondSwapTime, ADDRESSES.ceremonyV3Pool),
    });
    mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
      event: v3SecondSwap,
      mockDb,
    });
    const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
      epochNumber,
      endTime: BigInt(endTime),
      ...eventData(baseBlock + 8, endTime, ADDRESSES.epochManager),
    });
    mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({ event: epochEnd, mockDb });

    const oldGrowthIds = [
      `${ADDRESSES.ceremonyV2Pool}:${epochNumber.toString()}`,
      `${BALANCER_AUTORANGE_V3_POOL_ADDRESS}:${epochNumber.toString()}`,
    ];
    const frozenGrowthBeforeGap = oldGrowthIds.map(id => mockDb.entities.LPPoolEpochGrowth.get(id));
    for (const growth of frozenGrowthBeforeGap) {
      assert.equal(growth?.isFrozen, true);
      assert.equal(growth?.lastTimestamp, endTime);
      assert.equal(growth?.frozenAt, endTime);
    }
    const firstGap = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: ADDRESSES.user,
      timestamp: BigInt(firstGapTime),
      ...eventData(baseBlock + 9, firstGapTime, ADDRESSES.leaderboardKeeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: firstGap,
      mockDb,
    });
    const primaryFinalizationId = `${ADDRESSES.user}:${epochNumber.toString()}`;
    const firstCertificate = mockDb.entities.UserEpochFinalization.get(primaryFinalizationId);
    assert.ok(firstCertificate);
    const duplicateGap = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: ADDRESSES.user,
      timestamp: BigInt(duplicateGapTime),
      ...eventData(baseBlock + 10, duplicateGapTime, ADDRESSES.leaderboardKeeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: duplicateGap,
      mockDb,
    });
    assert.deepEqual(
      mockDb.entities.UserEpochFinalization.get(primaryFinalizationId),
      firstCertificate,
      'duplicate gap preserves the first certificate byte-for-byte'
    );
    const secondUserGap = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
      user: ADDRESSES.userTwo,
      timestamp: BigInt(secondUserGapTime),
      ...eventData(baseBlock + 11, secondUserGapTime, ADDRESSES.leaderboardKeeper),
    });
    mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
      event: secondUserGap,
      mockDb,
    });

    assert.deepEqual(
      oldGrowthIds.map(id => mockDb.entities.LPPoolEpochGrowth.get(id)),
      frozenGrowthBeforeGap,
      'gap settlements add zero pool growth'
    );

    const v2Token1PriceAfterSync = (CEREMONY_PRICE_E8 * 600n) / 400n;
    const balancerToken1PriceAfterSwap = (CEREMONY_PRICE_E8 * 550n) / 450n;
    const preV2Growth = ceremonyFungibleGrowthX128([
      {
        reserve0: 500n,
        reserve1: 500n,
        token0PriceE8: CEREMONY_PRICE_E8,
        token1PriceE8: CEREMONY_PRICE_E8,
        seconds: 9 * hour,
      },
      {
        reserve0: 600n,
        reserve1: 400n,
        token0PriceE8: CEREMONY_PRICE_E8,
        token1PriceE8: v2Token1PriceAfterSync,
        seconds: 3 * hour,
      },
    ]);
    const postV2Growth = ceremonyFungibleGrowthX128([
      {
        reserve0: 600n,
        reserve1: 400n,
        token0PriceE8: CEREMONY_PRICE_E8,
        token1PriceE8: v2Token1PriceAfterSync,
        seconds: 12 * hour,
      },
    ]);
    const preBalancerGrowth = ceremonyFungibleGrowthX128([
      {
        reserve0: 500n,
        reserve1: 500n,
        token0PriceE8: CEREMONY_PRICE_E8,
        token1PriceE8: CEREMONY_PRICE_E8,
        seconds: 10 * hour,
      },
      {
        reserve0: 550n,
        reserve1: 450n,
        token0PriceE8: CEREMONY_PRICE_E8,
        token1PriceE8: balancerToken1PriceAfterSwap,
        seconds: 2 * hour,
      },
    ]);
    const postBalancerGrowth = ceremonyFungibleGrowthX128([
      {
        reserve0: 550n,
        reserve1: 450n,
        token0PriceE8: CEREMONY_PRICE_E8,
        token1PriceE8: balancerToken1PriceAfterSwap,
        seconds: 12 * hour,
      },
    ]);
    const primaryPreLP =
      ceremonyGrowthToPoints(CEREMONY_V2_LIQUIDITY, preV2Growth) +
      ceremonyGrowthToPoints(CEREMONY_V2_LIQUIDITY, preBalancerGrowth);
    const primaryPostLP =
      ceremonyGrowthToPoints(CEREMONY_V2_LIQUIDITY, postV2Growth) +
      ceremonyGrowthToPoints(CEREMONY_V2_LIQUIDITY, postBalancerGrowth);
    const expectedPrimaryLP = primaryPreLP + primaryPostLP;
    const expectedPrimaryLPWithMultiplier =
      primaryPreLP + applyCeremonyMultiplier(primaryPostLP, 15_000n);
    const expectedSecondaryLP =
      ceremonyGrowthToPoints(CEREMONY_V2_LIQUIDITY / 2n, preV2Growth + postV2Growth) +
      ceremonyGrowthToPoints(CEREMONY_V2_LIQUIDITY / 2n, preBalancerGrowth + postBalancerGrowth);
    const halfDayDeposit = 500n * CEREMONY_POINTS_SCALE;
    const halfDayVP = CEREMONY_POINTS_SCALE / 2n;
    const expectedDeposit = 2n * halfDayDeposit;
    const expectedDepositWithMultiplier =
      halfDayDeposit + applyCeremonyMultiplier(halfDayDeposit, 15_000n);
    const expectedVP = 2n * halfDayVP;
    const expectedVPWithMultiplier = halfDayVP + applyCeremonyMultiplier(halfDayVP, 15_000n);

    assert.equal(statsAtMultiplierBoundary.lpPoints, primaryPreLP);
    assert.equal(statsAtMultiplierBoundary.lpPointsWithMultiplier, primaryPreLP);
    assert.equal(statsAtMultiplierBoundary.depositPoints, halfDayDeposit);
    assert.equal(statsAtMultiplierBoundary.depositPointsWithMultiplier, halfDayDeposit);
    assert.equal(statsAtMultiplierBoundary.dailyVPPoints, halfDayVP);
    assert.equal(statsAtMultiplierBoundary.vpPointsWithMultiplier, halfDayVP);

    const primaryStats = mockDb.entities.UserEpochStats.get(
      `${ADDRESSES.user}:${epochNumber.toString()}`
    );
    const secondaryStats = mockDb.entities.UserEpochStats.get(
      `${ADDRESSES.userTwo}:${epochNumber.toString()}`
    );
    assert.ok(primaryStats);
    assert.ok(secondaryStats);
    assert.equal(primaryStats.lpPoints, expectedPrimaryLP);
    assert.equal(primaryStats.lpPointsWithMultiplier, expectedPrimaryLPWithMultiplier);
    assert.equal(primaryStats.depositPoints, expectedDeposit);
    assert.equal(primaryStats.depositPointsWithMultiplier, expectedDepositWithMultiplier);
    assert.equal(primaryStats.dailyVPPoints, expectedVP);
    assert.equal(primaryStats.vpPointsWithMultiplier, expectedVPWithMultiplier);
    assert.equal(primaryStats.totalPoints, expectedPrimaryLP + expectedDeposit + expectedVP);
    assert.equal(
      primaryStats.totalPointsWithMultiplier,
      expectedPrimaryLPWithMultiplier + expectedDepositWithMultiplier + expectedVPWithMultiplier
    );
    assert.equal(primaryStats.lpMultiplierBps, 15_000n);
    assert.equal(primaryStats.depositMultiplierBps, 15_000n);
    assert.equal(primaryStats.vpMultiplierBps, 15_000n);
    assert.equal(secondaryStats.lpPoints, expectedSecondaryLP);
    assert.equal(secondaryStats.lpPointsWithMultiplier, expectedSecondaryLP);
    assert.equal(secondaryStats.totalPoints, expectedSecondaryLP);
    assert.equal(secondaryStats.totalPointsWithMultiplier, expectedSecondaryLP);

    assert.equal(
      mockDb.entities.LPPoolEpochGrowth.get(`${ADDRESSES.ceremonyV2Pool}:${epochNumber.toString()}`)
        ?.scalarGrowthX128,
      preV2Growth + postV2Growth
    );
    assert.equal(
      mockDb.entities.LPPoolEpochGrowth.get(
        `${BALANCER_AUTORANGE_V3_POOL_ADDRESS}:${epochNumber.toString()}`
      )?.scalarGrowthX128,
      preBalancerGrowth + postBalancerGrowth
    );

    const rawGapRows = mockDb.entities.LeaderboardKeeperUserSettled.getAll().filter(
      (row: EntityRow) => row.epochNumber === epochNumber && row.isGap
    );
    const primaryRawRows = rawGapRows.filter((row: EntityRow) => row.user_id === ADDRESSES.user);
    const secondaryRawRows = rawGapRows.filter(
      (row: EntityRow) => row.user_id === ADDRESSES.userTwo
    );
    assert.equal(primaryRawRows.length, 2);
    assert.equal(secondaryRawRows.length, 1);
    assert.deepEqual(
      primaryRawRows.map((row: EntityRow) => [row.timestamp, row.txHash]),
      [
        [firstGapTime, firstGap.transaction.hash],
        [duplicateGapTime, duplicateGap.transaction.hash],
      ]
    );
    assert.deepEqual(
      secondaryRawRows.map((row: EntityRow) => [row.timestamp, row.txHash]),
      [[secondUserGapTime, secondUserGap.transaction.hash]]
    );
    const finalizations = mockDb.entities.UserEpochFinalization.getAll().filter(
      (row: EntityRow) => row.epochNumber === epochNumber
    );
    assert.equal(finalizations.length, 2);
    assert.deepEqual(firstCertificate, {
      id: primaryFinalizationId,
      user_id: ADDRESSES.user,
      epochNumber,
      epochEndTime: endTime,
      settledThrough: endTime,
      finalizedAt: firstGapTime,
      blockNumber: BigInt(firstGap.block.number),
      txHash: firstGap.transaction.hash,
      settlementEventId: `${firstGap.transaction.hash}-${firstGap.logIndex}`,
    });

    const nextStart = TestHelpers.EpochManager.EpochStart.createMockEvent({
      epochNumber: nextEpochNumber,
      startTime: BigInt(nextStartTime),
      ...eventData(baseBlock + 12, nextStartTime, ADDRESSES.epochManager),
    });
    mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
      event: nextStart,
      mockDb,
    });
    for (const [offset, user] of [ADDRESSES.user, ADDRESSES.userTwo].entries()) {
      const nextTideSettle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
        user,
        timestamp: BigInt(nextStartTime),
        ...eventData(baseBlock + 13 + offset, nextStartTime, ADDRESSES.leaderboardKeeper),
      });
      mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
        event: nextTideSettle,
        mockDb,
      });
      const nextStats = mockDb.entities.UserEpochStats.get(`${user}:${nextEpochNumber.toString()}`);
      assert.ok(nextStats);
      assert.deepEqual(
        [
          nextStats.depositPoints,
          nextStats.borrowPoints,
          nextStats.lpPoints,
          nextStats.dailyVPPoints,
          nextStats.totalPoints,
          nextStats.totalPointsWithMultiplier,
        ],
        [0n, 0n, 0n, 0n, 0n, 0n],
        `${user} next-Tide stats have zero contamination`
      );
    }

    return {
      primary: {
        lpPoints: primaryStats.lpPoints,
        lpPointsWithMultiplier: primaryStats.lpPointsWithMultiplier,
        depositPoints: primaryStats.depositPoints,
        depositPointsWithMultiplier: primaryStats.depositPointsWithMultiplier,
        vpPoints: primaryStats.dailyVPPoints,
        vpPointsWithMultiplier: primaryStats.vpPointsWithMultiplier,
        totalPoints: primaryStats.totalPoints,
        totalPointsWithMultiplier: primaryStats.totalPointsWithMultiplier,
      },
      secondary: {
        lpPoints: secondaryStats.lpPoints,
        lpPointsWithMultiplier: secondaryStats.lpPointsWithMultiplier,
        totalPoints: secondaryStats.totalPoints,
        totalPointsWithMultiplier: secondaryStats.totalPointsWithMultiplier,
      },
      finalizationCount: finalizations.length,
      primaryRawCount: primaryRawRows.length,
      secondaryRawCount: secondaryRawRows.length,
    };
  }

  try {
    const withoutRedundantSwap = await runCeremony(false);
    const withRedundantSwap = await runCeremony(true);
    assert.deepEqual(
      withRedundantSwap,
      withoutRedundantSwap,
      'redundant same-state V3 swap cannot repartition ceremony points or proofs'
    );
  } finally {
    if (previousFinalOnlyFloor === undefined) {
      delete process.env.ENVIO_KEEPER_FINAL_ONLY_FROM_EPOCH;
    } else {
      process.env.ENVIO_KEEPER_FINAL_ONLY_FROM_EPOCH = previousFinalOnlyFloor;
    }
  }
});
