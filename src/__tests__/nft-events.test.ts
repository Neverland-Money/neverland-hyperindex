import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TestHelpers, getRegisteredContractRegister, type EntityRow } from './v3-test-helpers';

import {
  AUSD_ADDRESS,
  LEADERBOARD_START_BLOCK,
  STATIC_NFT_COLLECTION_ADDRESSES,
  ZERO_ADDRESS,
} from '../helpers/constants';
import { LP_GROWTH_Q128 } from '../helpers/lpGrowthMath';
import { VIEM_ERROR_ADDRESS, installViemMock } from './viem-mock';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
installViemMock();

const ADDRESSES = {
  registry: '0x000000000000000000000000000000000000d001',
  collection: '0x000000000000000000000000000000000000d002',
  collectionTwo: '0x000000000000000000000000000000000000d003',
  user: '0x000000000000000000000000000000000000d004',
  userTwo: '0x000000000000000000000000000000000000d005',
  lpPool: '0x000000000000000000000000000000000000d006',
  lpToken: '0x000000000000000000000000000000000000d007',
};

const LP_POSITION_ID = `v2:${ADDRESSES.lpPool}:${ADDRESSES.user}`;
const ONE_POINT_GROWTH_X128 = LP_GROWTH_Q128 * 100_000_000n * 10_000n * 86_400n;

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

function seedStaticCollectionContext(
  mockDb: ReturnType<typeof TestHelpers.MockDb.createMockDb>,
  balance: bigint,
  nftCount: bigint,
  nftMultiplier: bigint
) {
  let next = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 1000n,
    decayRatio: 9000n,
    lastUpdate: 0,
  });
  next = next.entities.NFTPartnershipRegistryState.set({
    id: 'current',
    activeCollections: [ADDRESSES.collection],
    lastUpdate: 0,
  });
  next = next.entities.NFTPartnership.set({
    id: ADDRESSES.collection,
    collection: ADDRESSES.collection,
    name: 'Static',
    active: true,
    staticBoostBps: undefined,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
    lastUpdate: 0,
  });
  next = next.entities.UserLeaderboardState.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    nftCount,
    nftMultiplier,
    specialEditionCount: 0n,
    specialEditionMultiplier: 10000n,
    votingPower: 0n,
    vpTierIndex: 0n,
    vpMultiplier: 10000n,
    combinedMultiplier: nftMultiplier,
    totalEpochsParticipated: 0n,
    lifetimePoints: 0n,
    currentEpochId: undefined,
    currentEpochRank: undefined,
    lastUpdate: 0,
  });
  if (balance > 0n) {
    next = next.entities.UserNFTOwnership.set({
      id: `${ADDRESSES.user}:${ADDRESSES.collection}`,
      user_id: ADDRESSES.user,
      partnership_id: ADDRESSES.collection,
      balance,
      hasNFT: true,
      lastCheckedAt: 0,
      lastCheckedBlock: 0n,
    });
  }
  return next;
}

test('partnership lifecycle and transfer updates ownership', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const added = TestHelpers.NFTPartnershipRegistry.PartnershipAdded.createMockEvent({
    collection: ADDRESSES.collection,
    name: 'Partner',
    active: true,
    startTimestamp: 100n,
    endTimestamp: 0n,
    staticBoostBps: 0n,
    currentFirstBonus: 1000n,
    currentDecayRatio: 9000n,
    ...eventData(1, 100, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipAdded.processEvent({
    event: added,
    mockDb,
  });

  const updated = TestHelpers.NFTPartnershipRegistry.PartnershipUpdated.createMockEvent({
    collection: ADDRESSES.collection,
    name: 'Partner v2',
    active: false,
    startTimestamp: 200n,
    endTimestamp: 300n,
    ...eventData(2, 200, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipUpdated.processEvent({
    event: updated,
    mockDb,
  });

  const removed = TestHelpers.NFTPartnershipRegistry.PartnershipRemoved.createMockEvent({
    collection: ADDRESSES.collection,
    ...eventData(3, 210, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipRemoved.processEvent({
    event: removed,
    mockDb,
  });

  const multiplier = TestHelpers.NFTPartnershipRegistry.MultiplierParamsUpdated.createMockEvent({
    newFirstBonus: 2000n,
    newDecayRatio: 8000n,
    totalActivePartnerships: 2n,
    timestamp: 220n,
    ...eventData(4, 220, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.MultiplierParamsUpdated.processEvent({
    event: multiplier,
    mockDb,
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

  const selfTransfer = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ADDRESSES.user,
    tokenId: 1n,
    ...eventData(5, 230, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: selfTransfer,
    mockDb,
  });

  const received = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: 2n,
    ...eventData(6, 240, ADDRESSES.collectionTwo),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: received,
    mockDb,
  });

  mockDb = mockDb.entities.UserNFTOwnership.set({
    id: `${ADDRESSES.user}:${VIEM_ERROR_ADDRESS}`,
    user_id: ADDRESSES.user,
    partnership_id: VIEM_ERROR_ADDRESS,
    balance: 1n,
    hasNFT: true,
    lastCheckedAt: 0,
    lastCheckedBlock: 0n,
  });

  const sent = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ZERO_ADDRESS,
    tokenId: 3n,
    ...eventData(7, 250, VIEM_ERROR_ADDRESS),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: sent,
    mockDb,
  });

  const ownership = mockDb.entities.UserNFTOwnership.get(
    `${ADDRESSES.user}:${ADDRESSES.collection}`
  );
  assert.ok(ownership);
  assert.ok(mockDb.entities.NFTMultiplierSnapshot.get('220'));
  assert.equal(
    mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${VIEM_ERROR_ADDRESS}`),
    undefined
  );
});

test('generated NFT mutation while disabled splits resumed pool growth at the new multiplier', async () => {
  let mockDb = seedStaticCollectionContext(TestHelpers.MockDb.createMockDb(), 0n, 0n, 10_000n);
  const timestamp = 1000;
  const blockNumber = LEADERBOARD_START_BLOCK + 100;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: BigInt(LEADERBOARD_START_BLOCK),
    startTime: 900,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 900,
    scheduledEndTime: 2000,
  });
  mockDb = mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    lpRateBps: 0n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.UserTokenList.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    tokenIds: [],
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [ADDRESSES.lpPool],
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    positionManager: ADDRESSES.lpPool,
    token0: AUSD_ADDRESS,
    token1: ADDRESSES.lpToken,
    fee: 3000,
    lpRateBps: 8_640_000n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 900,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 950,
    lastUpdate: 950,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    currentTick: 0,
    sqrtPriceX96: 1n << 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    reserve0: 1n,
    reserve1: 1n,
    lpTotalSupply: 2n,
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: 0,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.lpToken,
    address: ADDRESSES.lpToken,
    decimals: 0,
    symbol: 'LP1',
    name: 'LP token',
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.LPPoolEpochGrowth.set({
    id: `${ADDRESSES.lpPool}:1`,
    pool: ADDRESSES.lpPool,
    epochNumber: 1n,
    startTimestamp: 900,
    lastTimestamp: 950,
    scalarGrowthX128: ONE_POINT_GROWTH_X128,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: 950,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: LP_POSITION_ID,
    tokenId: 0n,
    user_id: ADDRESSES.user,
    pool: ADDRESSES.lpPool,
    positionManager: ADDRESSES.lpPool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1n,
    amount0: 1n,
    amount1: 1n,
    isInRange: true,
    valueUsd: 200_000_000n,
    lastInRangeTimestamp: 900,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 900,
    settledLpPoints: 0n,
    createdAt: 899,
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    positionIds: [LP_POSITION_ID],
    lastUpdate: 900,
  });

  const transfer = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: 99n,
    mockEventData: {
      block: { number: blockNumber, timestamp },
      logIndex: 88,
      srcAddress: ADDRESSES.collection,
      transaction: { hash: `0x${'ef'.repeat(32)}` },
    },
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({ event: transfer, mockDb });

  const cursor = mockDb.entities.UserLPEpochCursor.get(`${LP_POSITION_ID}:1`);
  const stats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  const ownership = mockDb.entities.UserNFTOwnership.get(
    `${ADDRESSES.user}:${ADDRESSES.collection}`
  );
  assert.equal(cursor?.lastSettledAt, timestamp);
  assert.equal(cursor?.growthBaselineX128, ONE_POINT_GROWTH_X128);
  assert.equal(stats?.lpPoints, 1_000_000_000_000_000_000n);
  assert.equal(ownership?.balance, 1n);

  mockDb = mockDb.entities.LPPoolConfig.set({
    ...mockDb.entities.LPPoolConfig.get(ADDRESSES.lpPool),
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    positionManager: ADDRESSES.lpPool,
    token0: AUSD_ADDRESS,
    token1: ADDRESSES.lpToken,
    fee: 3000,
    lpRateBps: 8_640_000n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: timestamp,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: timestamp,
  });
  const resumedTimestamp = timestamp + 100;
  const sent = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ZERO_ADDRESS,
    tokenId: 99n,
    mockEventData: {
      block: { number: blockNumber + 1, timestamp: resumedTimestamp },
      logIndex: 89,
      srcAddress: ADDRESSES.collection,
      transaction: { hash: `0x${'fe'.repeat(32)}` },
    },
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({ event: sent, mockDb });

  const splitCursor = mockDb.entities.UserLPEpochCursor.get(`${LP_POSITION_ID}:1`);
  const splitStats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  const resumedGrowth = mockDb.entities.LPPoolEpochGrowth.get(`${ADDRESSES.lpPool}:1`);
  assert.equal(splitCursor?.lastSettledAt, resumedTimestamp);
  assert.equal(resumedGrowth?.lastTimestamp, resumedTimestamp);
  assert.equal(resumedGrowth?.scalarGrowthX128, 2n * ONE_POINT_GROWTH_X128);
  assert.equal(splitStats?.lpPoints, 2_000_000_000_000_000_000n);
  assert.equal(splitStats?.lpPointsWithMultiplier, 2_100_000_000_000_000_000n);
  assert.equal(
    mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${ADDRESSES.collection}`),
    undefined
  );
});

test('partnership end timestamps set and clear as expected', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const added = TestHelpers.NFTPartnershipRegistry.PartnershipAdded.createMockEvent({
    collection: ADDRESSES.collection,
    name: 'Partner',
    active: true,
    startTimestamp: 100n,
    endTimestamp: 500n,
    staticBoostBps: 0n,
    currentFirstBonus: 1000n,
    currentDecayRatio: 9000n,
    ...eventData(50, 500, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipAdded.processEvent({
    event: added,
    mockDb,
  });

  const addedRecord = mockDb.entities.NFTPartnership.get(ADDRESSES.collection);
  assert.equal(addedRecord?.endTimestamp, 500);

  const updated = TestHelpers.NFTPartnershipRegistry.PartnershipUpdated.createMockEvent({
    collection: ADDRESSES.collection,
    name: 'Partner',
    active: true,
    startTimestamp: 200n,
    endTimestamp: 0n,
    ...eventData(51, 510, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipUpdated.processEvent({
    event: updated,
    mockDb,
  });

  const updatedRecord = mockDb.entities.NFTPartnership.get(ADDRESSES.collection);
  assert.equal(updatedRecord?.endTimestamp, undefined);
});

test('partnership-added events apply explicit boosts and updates preserve them', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.NFTPartnership.set({
    id: ADDRESSES.collection,
    collection: ADDRESSES.collection,
    name: 'Existing',
    active: true,
    staticBoostBps: 123n,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
    lastUpdate: 0,
  });

  const positiveBoostAdded = TestHelpers.NFTPartnershipRegistry.PartnershipAdded.createMockEvent({
    collection: ADDRESSES.collection,
    name: 'Positive boost',
    active: true,
    startTimestamp: 100n,
    endTimestamp: 0n,
    staticBoostBps: 456n,
    currentFirstBonus: 1000n,
    currentDecayRatio: 9000n,
    ...eventData(52, 520, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipAdded.processEvent({
    event: positiveBoostAdded,
    mockDb,
  });
  assert.equal(mockDb.entities.NFTPartnership.get(ADDRESSES.collection)?.staticBoostBps, 456n);

  const zeroBoostAdded = TestHelpers.NFTPartnershipRegistry.PartnershipAdded.createMockEvent({
    collection: ADDRESSES.collection,
    name: 'Decay boost',
    active: true,
    startTimestamp: 200n,
    endTimestamp: 0n,
    staticBoostBps: 0n,
    currentFirstBonus: 1000n,
    currentDecayRatio: 9000n,
    ...eventData(53, 530, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipAdded.processEvent({
    event: zeroBoostAdded,
    mockDb,
  });
  assert.equal(mockDb.entities.NFTPartnership.get(ADDRESSES.collection)?.staticBoostBps, 0n);

  const updated = TestHelpers.NFTPartnershipRegistry.PartnershipUpdated.createMockEvent({
    collection: ADDRESSES.collection,
    name: 'Update preserves boost',
    active: true,
    startTimestamp: 300n,
    endTimestamp: 0n,
    ...eventData(54, 540, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipUpdated.processEvent({
    event: updated,
    mockDb,
  });
  assert.equal(mockDb.entities.NFTPartnership.get(ADDRESSES.collection)?.staticBoostBps, 0n);
});

test('legacy partnership-added events preserve existing boosts and default to decay', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.NFTPartnership.set({
    id: ADDRESSES.collection,
    collection: ADDRESSES.collection,
    name: 'Existing',
    active: true,
    staticBoostBps: 123n,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
    lastUpdate: 0,
  });

  const existingLegacyAdded =
    TestHelpers.NFTPartnershipRegistry.PartnershipAddedLegacy.createMockEvent({
      collection: ADDRESSES.collection,
      name: 'Existing legacy partnership',
      active: true,
      startTimestamp: 100n,
      endTimestamp: 0n,
      currentFirstBonus: 1000n,
      currentDecayRatio: 9000n,
      totalPartnerships: 1n,
      ...eventData(55, 550, ADDRESSES.registry),
    });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipAddedLegacy.processEvent({
    event: existingLegacyAdded,
    mockDb,
  });
  assert.equal(mockDb.entities.NFTPartnership.get(ADDRESSES.collection)?.staticBoostBps, 123n);

  const newLegacyAdded = TestHelpers.NFTPartnershipRegistry.PartnershipAddedLegacy.createMockEvent({
    collection: ADDRESSES.collectionTwo,
    name: 'New legacy partnership',
    active: true,
    startTimestamp: 200n,
    endTimestamp: 0n,
    currentFirstBonus: 1000n,
    currentDecayRatio: 9000n,
    totalPartnerships: 2n,
    ...eventData(56, 560, ADDRESSES.registry),
  });
  mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipAddedLegacy.processEvent({
    event: newLegacyAdded,
    mockDb,
  });
  assert.equal(
    mockDb.entities.NFTPartnership.get(ADDRESSES.collectionTwo)?.staticBoostBps,
    undefined
  );
});

test('transfer uses default multiplier when config missing', async () => {
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
  mockDb = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 1000n,
    decayRatio: 9000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnershipRegistryState.set({
    id: 'current',
    activeCollections: [ADDRESSES.collection],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnership.set({
    id: ADDRESSES.collection,
    collection: ADDRESSES.collection,
    name: 'Test Collection',
    active: true,
    staticBoostBps: undefined,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
    lastUpdate: 0,
  });

  const received = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: 1n,
    ...eventData(10, 300, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: received,
    mockDb,
  });

  // After transfer, user should have ownership record
  const ownership = mockDb.entities.UserNFTOwnership.get(
    `${ADDRESSES.user}:${ADDRESSES.collection}`
  );
  assert.equal(ownership?.hasNFT, true);

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(state?.nftMultiplier, 11000n); // Bootstrap config: 10000 + 1000 = 11000
  assert.equal(state?.nftCount, 1n);
});

test('transfer composes collection multiplier with special edition multiplier', async () => {
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
  mockDb = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 1000n,
    decayRatio: 9000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnershipRegistryState.set({
    id: 'current',
    activeCollections: [ADDRESSES.collection],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnership.set({
    id: ADDRESSES.collection,
    collection: ADDRESSES.collection,
    name: 'Test Collection',
    active: true,
    staticBoostBps: undefined,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.SpecialEditionRegistryState.set({
    id: 'current',
    editionIds: [1n],
    totalEditions: 1n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.SpecialEditionConfig.set({
    id: '1',
    editionId: 1n,
    key: 'SHINY',
    name: 'Shiny',
    perTokenBoostBps: 2000n,
    enabled: true,
    exists: true,
    createdAt: 0,
    updatedAt: 0,
    changeTimestamps: [0],
    boostBpsHistory: [2000n],
    enabledHistory: [1n],
  });
  mockDb = mockDb.entities.UserSpecialEditionState.set({
    id: `${ADDRESSES.user}:1`,
    user_id: ADDRESSES.user,
    editionId: 1n,
    tokenCount: 1n,
    countTimestamps: [0],
    tokenCountHistory: [1n],
    updatedAt: 0,
  });
  mockDb = mockDb.entities.UserLeaderboardState.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    nftCount: 0n,
    nftMultiplier: 10000n,
    specialEditionCount: 1n,
    specialEditionMultiplier: 12000n,
    votingPower: 0n,
    vpTierIndex: 0n,
    vpMultiplier: 10000n,
    combinedMultiplier: 12000n,
    totalEpochsParticipated: 0n,
    lifetimePoints: 0n,
    currentEpochId: undefined,
    currentEpochRank: undefined,
    lastUpdate: 0,
  });

  const received = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: 7n,
    ...eventData(11, 310, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: received,
    mockDb,
  });

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(state?.nftMultiplier, 11000n);
  assert.equal(state?.specialEditionMultiplier, 12000n);
  // additive join: nft +10% and se +20% => +30% => 13000 (not 11000*1.2 = 13200).
  assert.equal(state?.combinedMultiplier, 13000n);
});

test('transfer caps multiplier at max', async () => {
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
  mockDb = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 40000n,
    decayRatio: 10000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnershipRegistryState.set({
    id: 'current',
    activeCollections: [ADDRESSES.collection, ADDRESSES.collectionTwo],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnership.set({
    id: ADDRESSES.collection,
    collection: ADDRESSES.collection,
    name: 'Collection 1',
    active: true,
    staticBoostBps: undefined,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnership.set({
    id: ADDRESSES.collectionTwo,
    collection: ADDRESSES.collectionTwo,
    name: 'Collection 2',
    active: true,
    staticBoostBps: undefined,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
    lastUpdate: 0,
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
  // User already owns first collection
  mockDb = mockDb.entities.UserNFTOwnership.set({
    id: `${ADDRESSES.user}:${ADDRESSES.collection}`,
    user_id: ADDRESSES.user,
    partnership_id: ADDRESSES.collection,
    balance: 1n,
    hasNFT: true,
    lastCheckedAt: 0,
    lastCheckedBlock: 0n,
  });

  const received = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: 2n,
    ...eventData(11, 320, ADDRESSES.collectionTwo),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: received,
    mockDb,
  });

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(state?.nftMultiplier, 50000n);
  assert.equal(state?.nftCount, 2n);
});

test('transfer fallback clamps negative balances', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const sent = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ZERO_ADDRESS,
    tokenId: 4n,
    ...eventData(12, 330, VIEM_ERROR_ADDRESS),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: sent,
    mockDb,
  });

  const ownership = mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${VIEM_ERROR_ADDRESS}`);
  assert.equal(ownership, undefined);
});

test('transfer removal handles empty state count', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.UserNFTOwnership.set({
    id: `${ADDRESSES.user}:${VIEM_ERROR_ADDRESS}`,
    user_id: ADDRESSES.user,
    partnership_id: VIEM_ERROR_ADDRESS,
    balance: 1n,
    hasNFT: true,
    lastCheckedAt: 0,
    lastCheckedBlock: 0n,
  });
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

  const sent = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ZERO_ADDRESS,
    tokenId: 5n,
    ...eventData(13, 340, VIEM_ERROR_ADDRESS),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: sent,
    mockDb,
  });

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(state?.nftCount, 0n);
});

test('combined multiplier caps at maximum', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 40000n,
    decayRatio: 10000n,
    lastUpdate: 0,
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
    vpMultiplier: 50000n,
    combinedMultiplier: 10000n,
    totalEpochsParticipated: 0n,
    lifetimePoints: 0n,
    currentEpochId: undefined,
    currentEpochRank: undefined,
    lastUpdate: 0,
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
    lockedAmount: 1n,
    end: 0,
    isPermanent: true,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
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

  const received = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: 6n,
    ...eventData(14, 350, ADDRESSES.collectionTwo),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: received,
    mockDb,
  });

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(state?.combinedMultiplier, 50000n);
});

test('partnership added creates registry state when none exists', async () => {
  const previousBootstrap = process.env.ENVIO_DISABLE_BOOTSTRAP;
  process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

  try {
    const TestHelpers = loadTestHelpers();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();

    const added = TestHelpers.NFTPartnershipRegistry.PartnershipAdded.createMockEvent({
      collection: ADDRESSES.collection,
      name: 'Partner',
      active: true,
      startTimestamp: 100n,
      endTimestamp: 0n,
      staticBoostBps: 0n,
      currentFirstBonus: 1000n,
      currentDecayRatio: 9000n,
      ...eventData(20, 400, ADDRESSES.registry),
    });
    mockDb = await TestHelpers.NFTPartnershipRegistry.PartnershipAdded.processEvent({
      event: added,
      mockDb,
    });

    const registry = mockDb.entities.NFTPartnershipRegistryState.get('current');
    assert.ok(registry);
    assert.deepEqual(registry?.activeCollections, [ADDRESSES.collection]);
  } finally {
    process.env.ENVIO_DISABLE_BOOTSTRAP = previousBootstrap;
  }
});

test('active transfer snapshots sender and recipient multiplier reasons', async () => {
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
  mockDb = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 1000n,
    decayRatio: 9000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnershipRegistryState.set({
    id: 'current',
    activeCollections: [ADDRESSES.collection],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnership.set({
    id: ADDRESSES.collection,
    collection: ADDRESSES.collection,
    name: 'Active',
    active: true,
    staticBoostBps: undefined,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
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
  mockDb = mockDb.entities.UserLeaderboardState.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    nftCount: 1n,
    nftMultiplier: 11000n,
    specialEditionCount: 0n,
    specialEditionMultiplier: 10000n,
    votingPower: 0n,
    vpTierIndex: 0n,
    vpMultiplier: 10000n,
    combinedMultiplier: 11000n,
    totalEpochsParticipated: 0n,
    lifetimePoints: 0n,
    currentEpochId: undefined,
    currentEpochRank: undefined,
    lastUpdate: 0,
  });

  const transfer = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ADDRESSES.userTwo,
    tokenId: 1n,
    ...eventData(30, 600, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({ event: transfer, mockDb });

  const reasons = mockDb.entities.UserMultiplierSnapshot.getAll().map(
    (row: EntityRow) => row.changeReason
  );
  assert.equal(mockDb.entities.UserLeaderboardState.get(ADDRESSES.user)?.nftMultiplier, 10000n);
  assert.equal(mockDb.entities.UserLeaderboardState.get(ADDRESSES.userTwo)?.nftMultiplier, 11000n);
  assert.ok(reasons.includes(`NFT_TRANSFERRED:${ADDRESSES.collection}`));
  assert.ok(reasons.includes(`NFT_RECEIVED:${ADDRESSES.collection}`));
});

test('static nft collection handlers reuse shared transfer logic', async () => {
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
  mockDb = mockDb.entities.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: 1000n,
    decayRatio: 9000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnershipRegistryState.set({
    id: 'current',
    activeCollections: [ADDRESSES.collection, ADDRESSES.collectionTwo],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnership.set({
    id: ADDRESSES.collection,
    collection: ADDRESSES.collection,
    name: 'Collection 1',
    active: true,
    staticBoostBps: undefined,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.NFTPartnership.set({
    id: ADDRESSES.collectionTwo,
    collection: ADDRESSES.collectionTwo,
    name: 'Collection 2',
    active: true,
    staticBoostBps: undefined,
    startTimestamp: 0,
    endTimestamp: undefined,
    addedAt: 0,
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
  mockDb = mockDb.entities.UserLeaderboardState.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    nftCount: 1n,
    nftMultiplier: 11000n,
    specialEditionCount: 0n,
    specialEditionMultiplier: 10000n,
    votingPower: 0n,
    vpTierIndex: 0n,
    vpMultiplier: 10000n,
    combinedMultiplier: 11000n,
    totalEpochsParticipated: 0n,
    lifetimePoints: 0n,
    currentEpochId: undefined,
    currentEpochRank: undefined,
    lastUpdate: 0,
  });

  const selfTransfer = TestHelpers.The10kSquad.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ADDRESSES.user,
    tokenId: 1n,
    ...eventData(21, 410, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.The10kSquad.Transfer.processEvent({
    event: selfTransfer,
    mockDb,
  });

  const minted = TestHelpers.Overnads.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: 2n,
    ...eventData(22, 420, ADDRESSES.collectionTwo),
  });
  mockDb = await TestHelpers.Overnads.Transfer.processEvent({
    event: minted,
    mockDb,
  });

  const burned = TestHelpers.LilStars.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ZERO_ADDRESS,
    tokenId: 3n,
    ...eventData(23, 430, ADDRESSES.collectionTwo),
  });
  mockDb = await TestHelpers.LilStars.Transfer.processEvent({
    event: burned,
    mockDb,
  });

  const realNads = TestHelpers.RealNads.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ADDRESSES.user,
    tokenId: 4n,
    ...eventData(24, 440, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.RealNads.Transfer.processEvent({
    event: realNads,
    mockDb,
  });

  const keptOwnership = mockDb.entities.UserNFTOwnership.get(
    `${ADDRESSES.user}:${ADDRESSES.collection}`
  );
  assert.equal(keptOwnership?.lastCheckedAt, 440);
  assert.equal(
    mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${ADDRESSES.collectionTwo}`),
    undefined
  );
});

test('static transfer clamps underflow, stale counts, and positive balances', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();

  let mockDb = seedStaticCollectionContext(TestHelpers.MockDb.createMockDb(), 0n, 0n, 10000n);
  const underflow = TestHelpers.RealNads.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ZERO_ADDRESS,
    tokenId: 10n,
    ...eventData(40, 700, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.RealNads.Transfer.processEvent({ event: underflow, mockDb });
  assert.equal(
    mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${ADDRESSES.collection}`),
    undefined
  );
  assert.deepEqual(mockDb.entities.UserMultiplierSnapshot.getAll(), []);

  mockDb = seedStaticCollectionContext(TestHelpers.MockDb.createMockDb(), 1n, 0n, 10000n);
  const staleCount = TestHelpers.RealNads.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ZERO_ADDRESS,
    tokenId: 11n,
    ...eventData(41, 710, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.RealNads.Transfer.processEvent({ event: staleCount, mockDb });
  assert.equal(mockDb.entities.UserLeaderboardState.get(ADDRESSES.user)?.nftCount, 0n);

  mockDb = seedStaticCollectionContext(TestHelpers.MockDb.createMockDb(), 2n, 1n, 11000n);
  const retained = TestHelpers.RealNads.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ZERO_ADDRESS,
    tokenId: 12n,
    ...eventData(42, 720, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.RealNads.Transfer.processEvent({ event: retained, mockDb });
  assert.equal(
    mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${ADDRESSES.collection}`)?.balance,
    1n
  );
  assert.equal(mockDb.entities.UserLeaderboardState.get(ADDRESSES.user)?.nftCount, 1n);
  assert.equal(mockDb.entities.UserLeaderboardState.get(ADDRESSES.user)?.nftMultiplier, 11000n);
  assert.deepEqual(mockDb.entities.UserMultiplierSnapshot.getAll(), []);
});

test('a partnership for a statically configured collection is not registered dynamically', async () => {
  // Envio does not dedupe across contract names: registering a config.yaml collection as a
  // dynamic PartnerNFT too would dispatch every Transfer twice and double-count balances.
  // The guard lives in the contractRegister callback, so it is driven directly here.
  const added: string[] = [];
  const context = { addPartnerNFT: (address: string) => added.push(address) };
  for (const eventName of ['PartnershipAdded', 'PartnershipAddedLegacy']) {
    const register = await getRegisteredContractRegister('NFTPartnershipRegistry', eventName);
    // Upper-cased on purpose: the guard must match on the normalized address.
    const staticCollection = STATIC_NFT_COLLECTION_ADDRESSES[0].toUpperCase();
    await register({ event: { params: { collection: staticCollection } }, context });
    assert.deepEqual(added, [], `${eventName}: static collection must not be added`);

    const dynamicCollection = '0x000000000000000000000000000000000000d00d';
    await register({ event: { params: { collection: dynamicCollection } }, context });
    assert.deepEqual(added, [dynamicCollection], `${eventName}: a new collection is added`);
    added.length = 0;
  }
});
