import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { ZERO_ADDRESS } from '../helpers/constants';
import { VIEM_ERROR_ADDRESS, installViemMock } from './viem-mock';

process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'true';
process.env.ENVIO_DISABLE_ETH_CALLS = 'true';
installViemMock();

const ADDRESSES = {
  registry: '0x000000000000000000000000000000000000d001',
  collection: '0x000000000000000000000000000000000000d002',
  collectionTwo: '0x000000000000000000000000000000000000d003',
  user: '0x000000000000000000000000000000000000d004',
};

function loadTestHelpers() {
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
    id: 1n,
    ...eventData(5, 230, ADDRESSES.collection),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: selfTransfer,
    mockDb,
  });

  const received = TestHelpers.PartnerNFT.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    id: 2n,
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
    id: 3n,
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
    id: 1n,
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
    id: 2n,
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
    id: 4n,
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
    id: 5n,
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
    id: 6n,
    ...eventData(14, 350, ADDRESSES.collectionTwo),
  });
  mockDb = await TestHelpers.PartnerNFT.Transfer.processEvent({
    event: received,
    mockDb,
  });

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.equal(state?.combinedMultiplier, 50000n);
});
