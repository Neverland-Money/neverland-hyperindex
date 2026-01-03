import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { EMERGENCY_ADMIN_ID, POOL_ADMIN_ID } from '../helpers/constants';

process.env.DISABLE_EXTERNAL_CALLS = 'true';
process.env.DISABLE_ETH_CALLS = 'true';

const ADDRESSES = {
  registry: '0x0000000000000000000000000000000000000101',
  provider: '0x0000000000000000000000000000000000000202',
  rewards: '0x0000000000000000000000000000000000000303',
  pool: '0x0000000000000000000000000000000000000404',
  user: '0x0000000000000000000000000000000000000505',
  reserve: '0x0000000000000000000000000000000000000606',
  to: '0x0000000000000000000000000000000000000707',
  oldAdmin: '0x0000000000000000000000000000000000000a01',
  newAdmin: '0x0000000000000000000000000000000000000a02',
  oldManager: '0x0000000000000000000000000000000000000b01',
  newManager: '0x0000000000000000000000000000000000000b02',
  oldSentinel: '0x0000000000000000000000000000000000000c01',
  newSentinel: '0x0000000000000000000000000000000000000c02',
  oldOwner: '0x0000000000000000000000000000000000000d01',
  newOwner: '0x0000000000000000000000000000000000000d02',
  oldDistributor: '0x0000000000000000000000000000000000000e01',
  newDistributor: '0x0000000000000000000000000000000000000e02',
  poolAdmin: '0x0000000000000000000000000000000000000f01',
  emergencyAdmin: '0x0000000000000000000000000000000000000f02',
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

test('pool addresses provider admin events update state and history', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const registerEvent =
    TestHelpers.PoolAddressesProviderRegistry.AddressesProviderRegistered.createMockEvent({
      addressesProvider: ADDRESSES.provider,
      id: 1n,
      ...eventData(100, 1000, ADDRESSES.registry),
    });
  mockDb = await TestHelpers.PoolAddressesProviderRegistry.AddressesProviderRegistered.processEvent(
    {
      event: registerEvent,
      mockDb,
    }
  );

  let state = mockDb.entities.PoolAddressesProviderState.get(ADDRESSES.provider);
  assert.ok(state);

  const aclEvent = TestHelpers.PoolAddressesProvider.ACLAdminUpdated.createMockEvent({
    oldAddress: ADDRESSES.oldAdmin,
    newAddress: ADDRESSES.newAdmin,
    ...eventData(101, 1001, ADDRESSES.provider),
  });
  const aclId = `${aclEvent.transaction.hash}-${aclEvent.logIndex}`;
  mockDb = await TestHelpers.PoolAddressesProvider.ACLAdminUpdated.processEvent({
    event: aclEvent,
    mockDb,
  });
  assert.ok(mockDb.entities.PoolAddressesProviderACLAdminUpdated.get(aclId));

  const managerEvent = TestHelpers.PoolAddressesProvider.ACLManagerUpdated.createMockEvent({
    oldAddress: ADDRESSES.oldManager,
    newAddress: ADDRESSES.newManager,
    ...eventData(102, 1002, ADDRESSES.provider),
  });
  const managerId = `${managerEvent.transaction.hash}-${managerEvent.logIndex}`;
  mockDb = await TestHelpers.PoolAddressesProvider.ACLManagerUpdated.processEvent({
    event: managerEvent,
    mockDb,
  });
  assert.ok(mockDb.entities.PoolAddressesProviderACLManagerUpdated.get(managerId));

  const marketEvent = TestHelpers.PoolAddressesProvider.MarketIdSet.createMockEvent({
    oldMarketId: 'old-market',
    newMarketId: 'new-market',
    ...eventData(103, 1003, ADDRESSES.provider),
  });
  const marketId = `${marketEvent.transaction.hash}-${marketEvent.logIndex}`;
  mockDb = await TestHelpers.PoolAddressesProvider.MarketIdSet.processEvent({
    event: marketEvent,
    mockDb,
  });
  assert.ok(mockDb.entities.PoolAddressesProviderMarketIdSet.get(marketId));

  const ownershipEvent = TestHelpers.PoolAddressesProvider.OwnershipTransferred.createMockEvent({
    previousOwner: ADDRESSES.oldOwner,
    newOwner: ADDRESSES.newOwner,
    ...eventData(104, 1004, ADDRESSES.provider),
  });
  const ownershipId = `${ownershipEvent.transaction.hash}-${ownershipEvent.logIndex}`;
  mockDb = await TestHelpers.PoolAddressesProvider.OwnershipTransferred.processEvent({
    event: ownershipEvent,
    mockDb,
  });
  assert.ok(mockDb.entities.PoolAddressesProviderOwnershipTransferred.get(ownershipId));

  const sentinelEvent =
    TestHelpers.PoolAddressesProvider.PriceOracleSentinelUpdated.createMockEvent({
      oldAddress: ADDRESSES.oldSentinel,
      newAddress: ADDRESSES.newSentinel,
      ...eventData(105, 1005, ADDRESSES.provider),
    });
  const sentinelId = `${sentinelEvent.transaction.hash}-${sentinelEvent.logIndex}`;
  mockDb = await TestHelpers.PoolAddressesProvider.PriceOracleSentinelUpdated.processEvent({
    event: sentinelEvent,
    mockDb,
  });
  assert.ok(mockDb.entities.PoolAddressesProviderPriceOracleSentinelUpdated.get(sentinelId));

  state = mockDb.entities.PoolAddressesProviderState.get(ADDRESSES.provider);
  assert.equal(state?.aclAdmin, ADDRESSES.newAdmin);
  assert.equal(state?.aclManager, ADDRESSES.newManager);
  assert.equal(state?.marketId, 'new-market');
  assert.equal(state?.owner, ADDRESSES.newOwner);
  assert.equal(state?.priceOracleSentinel, ADDRESSES.newSentinel);
});

test('address set and proxy updates are recorded and admin fields update', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const addressSetEvent = TestHelpers.PoolAddressesProvider.AddressSet.createMockEvent({
    id: POOL_ADMIN_ID,
    oldAddress: ADDRESSES.oldAdmin,
    newAddress: ADDRESSES.poolAdmin,
    ...eventData(110, 1010, ADDRESSES.provider),
  });
  const addressSetId = `${addressSetEvent.transaction.hash}-${addressSetEvent.logIndex}`;
  mockDb = await TestHelpers.PoolAddressesProvider.AddressSet.processEvent({
    event: addressSetEvent,
    mockDb,
  });
  assert.ok(mockDb.entities.PoolAddressesProviderAddressSet.get(addressSetId));

  const emergencyEvent = TestHelpers.PoolAddressesProvider.AddressSet.createMockEvent({
    id: EMERGENCY_ADMIN_ID,
    oldAddress: ADDRESSES.oldAdmin,
    newAddress: ADDRESSES.emergencyAdmin,
    ...eventData(111, 1011, ADDRESSES.provider),
  });
  const emergencyId = `${emergencyEvent.transaction.hash}-${emergencyEvent.logIndex}`;
  mockDb = await TestHelpers.PoolAddressesProvider.AddressSet.processEvent({
    event: emergencyEvent,
    mockDb,
  });
  assert.ok(mockDb.entities.PoolAddressesProviderAddressSet.get(emergencyId));

  const proxyEvent = TestHelpers.PoolAddressesProvider.AddressSetAsProxy.createMockEvent({
    id: POOL_ADMIN_ID,
    proxyAddress: ADDRESSES.provider,
    oldImplementationAddress: ADDRESSES.oldAdmin,
    newImplementationAddress: ADDRESSES.newAdmin,
    ...eventData(112, 1012, ADDRESSES.provider),
  });
  const proxyId = `${proxyEvent.transaction.hash}-${proxyEvent.logIndex}`;
  mockDb = await TestHelpers.PoolAddressesProvider.AddressSetAsProxy.processEvent({
    event: proxyEvent,
    mockDb,
  });
  assert.ok(mockDb.entities.PoolAddressesProviderAddressSetAsProxy.get(proxyId));

  const state = mockDb.entities.PoolAddressesProviderState.get(ADDRESSES.provider);
  assert.equal(state?.poolAdmin, ADDRESSES.poolAdmin);
  assert.equal(state?.emergencyAdmin, ADDRESSES.emergencyAdmin);
});

test('reward distributor updates are recorded', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const updateEvent = TestHelpers.RevenueReward.RewardDistributorUpdated.createMockEvent({
    oldDistributor: ADDRESSES.oldDistributor,
    newDistributor: ADDRESSES.newDistributor,
    ...eventData(120, 1020, ADDRESSES.rewards),
  });
  const updateId = `${updateEvent.transaction.hash}-${updateEvent.logIndex}`;
  mockDb = await TestHelpers.RevenueReward.RewardDistributorUpdated.processEvent({
    event: updateEvent,
    mockDb,
  });

  const record = mockDb.entities.RevenueRewardDistributorUpdate.get(updateId);
  assert.ok(record);
  assert.equal(record?.newDistributor, ADDRESSES.newDistributor);
});

test('pool withdraw does not create RedeemUnderlying', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const withdrawEvent = TestHelpers.Pool.Withdraw.createMockEvent({
    reserve: ADDRESSES.reserve,
    user: ADDRESSES.user,
    to: ADDRESSES.to,
    amount: 100n,
    ...eventData(130, 1030, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.Pool.Withdraw.processEvent({ event: withdrawEvent, mockDb });

  const stats = mockDb.entities.ProtocolStats.get('1');
  assert.equal(stats?.totalTransactions, 1n);
  assert.equal(mockDb.entities.RedeemUnderlying.getAll().length, 0);
});
