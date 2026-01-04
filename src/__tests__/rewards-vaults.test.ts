import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { ZERO_ADDRESS } from '../helpers/constants';

process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'true';
process.env.ENVIO_DISABLE_ETH_CALLS = 'true';

const ADDRESSES = {
  controller: '0x0000000000000000000000000000000000002001',
  revenueReward: '0x0000000000000000000000000000000000002002',
  dustToken: '0x0000000000000000000000000000000000002003',
  asset: '0x0000000000000000000000000000000000002004',
  reward: '0x0000000000000000000000000000000000002005',
  user: '0x0000000000000000000000000000000000002006',
  to: '0x0000000000000000000000000000000000002007',
  claimer: '0x0000000000000000000000000000000000002008',
  distributorOld: '0x0000000000000000000000000000000000002009',
  distributorNew: '0x0000000000000000000000000000000000002010',
  vault: '0x0000000000000000000000000000000000002011',
  provider: '0x0000000000000000000000000000000000002012',
  debtAsset: '0x0000000000000000000000000000000000002013',
  collateralAsset: '0x0000000000000000000000000000000000002014',
  rewardReceiver: '0x0000000000000000000000000000000000002015',
  dustTokenOwner: '0x0000000000000000000000000000000000002016',
  debtUnderlying: '0x0000000000000000000000000000000000002017',
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

test('rewards controller config, accrual, and claim flows', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.asset,
    pool_id: 'pool',
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: 6,
  });

  const configEvent = TestHelpers.RewardsController.AssetConfigUpdated.createMockEvent({
    asset: ADDRESSES.asset,
    reward: ADDRESSES.reward,
    oldEmission: 0n,
    newEmission: 5n,
    oldDistributionEnd: 0n,
    newDistributionEnd: 1000n,
    assetIndex: 123n,
    ...eventData(1, 100, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.AssetConfigUpdated.processEvent({
    event: configEvent,
    mockDb,
  });

  const rewardId = `${ADDRESSES.controller}:${ADDRESSES.asset}:${ADDRESSES.reward}`;
  const configId = `${ADDRESSES.controller}-${ADDRESSES.asset}-${ADDRESSES.reward}`;
  const reward = mockDb.entities.Reward.get(rewardId);
  const config = mockDb.entities.RewardAssetConfig.get(configId);
  const oracle = mockDb.entities.RewardFeedOracle.get(ADDRESSES.reward);

  assert.ok(reward);
  assert.ok(config);
  assert.ok(oracle);
  assert.equal(reward?.precision, 6n);
  assert.equal(config?.assetIndex, 123n);
  const historyId = `${configEvent.transaction.hash}-${configEvent.logIndex}`;
  const history = mockDb.entities.RewardAssetConfigHistory.get(historyId);
  assert.ok(history);
  assert.equal(history?.emittedAmount, 0n);

  const configUpdate = TestHelpers.RewardsController.AssetConfigUpdated.createMockEvent({
    asset: ADDRESSES.asset,
    reward: ADDRESSES.reward,
    oldEmission: 5n,
    newEmission: 10n,
    oldDistributionEnd: 1000n,
    newDistributionEnd: 2000n,
    assetIndex: 200n,
    ...eventData(2, 110, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.AssetConfigUpdated.processEvent({
    event: configUpdate,
    mockDb,
  });
  const historyUpdateId = `${configUpdate.transaction.hash}-${configUpdate.logIndex}`;
  const historyUpdate = mockDb.entities.RewardAssetConfigHistory.get(historyUpdateId);
  assert.ok(historyUpdate);
  assert.equal(historyUpdate?.emittedAmount, 50n);

  const configEndUpdate = TestHelpers.RewardsController.AssetConfigUpdated.createMockEvent({
    asset: ADDRESSES.asset,
    reward: ADDRESSES.reward,
    oldEmission: 10n,
    newEmission: 20n,
    oldDistributionEnd: 2000n,
    newDistributionEnd: 3000n,
    assetIndex: 250n,
    ...eventData(5, 2500, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.AssetConfigUpdated.processEvent({
    event: configEndUpdate,
    mockDb,
  });
  const historyEndId = `${configEndUpdate.transaction.hash}-${configEndUpdate.logIndex}`;
  const historyEnd = mockDb.entities.RewardAssetConfigHistory.get(historyEndId);
  assert.ok(historyEnd);
  assert.equal(historyEnd?.periodEnd, 2000);
  assert.equal(historyEnd?.emittedAmount, 18900n);

  const accrueEvent = TestHelpers.RewardsController.Accrued.createMockEvent({
    asset: ADDRESSES.asset,
    reward: ADDRESSES.reward,
    user: ADDRESSES.user,
    assetIndex: 200n,
    userIndex: 55n,
    rewardsAccrued: 100n,
    ...eventData(2, 110, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.Accrued.processEvent({
    event: accrueEvent,
    mockDb,
  });

  const user = mockDb.entities.User.get(ADDRESSES.user);
  assert.ok(user);
  assert.equal(user?.unclaimedRewards, 100n);
  assert.equal(user?.lifetimeRewards, 100n);

  const userRewardId = `${rewardId}:${ADDRESSES.user}`;
  const userReward = mockDb.entities.UserReward.get(userRewardId);
  assert.ok(userReward);
  assert.equal(userReward?.index, 55n);

  const rewardedId = `${accrueEvent.transaction.hash}-${accrueEvent.logIndex}`;
  assert.ok(mockDb.entities.RewardedAction.get(rewardedId));

  const accrueUpdate = TestHelpers.RewardsController.Accrued.createMockEvent({
    asset: ADDRESSES.asset,
    reward: ADDRESSES.reward,
    user: ADDRESSES.user,
    assetIndex: 250n,
    userIndex: 75n,
    rewardsAccrued: 50n,
    ...eventData(4, 120, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.Accrued.processEvent({
    event: accrueUpdate,
    mockDb,
  });

  const claimEvent = TestHelpers.RewardsController.RewardsClaimed.createMockEvent({
    user: ADDRESSES.user,
    reward: ADDRESSES.reward,
    to: ADDRESSES.to,
    claimer: ADDRESSES.claimer,
    amount: 150n,
    ...eventData(3, 120, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.RewardsClaimed.processEvent({
    event: claimEvent,
    mockDb,
  });

  const updatedUser = mockDb.entities.User.get(ADDRESSES.user);
  assert.ok(updatedUser);
  assert.equal(updatedUser?.unclaimedRewards, 0n);
  assert.equal(updatedUser?.lifetimeRewards, 150n);

  const claimId = `${claimEvent.transaction.hash}-${claimEvent.logIndex}`;
  const claim = mockDb.entities.ClaimRewardsCall.get(claimId);
  assert.ok(claim);
  assert.equal(claim?.action, 'ClaimRewardsCall');
  assert.ok(mockDb.entities.User.get(ADDRESSES.to));
  assert.ok(mockDb.entities.User.get(ADDRESSES.claimer));
});

test('rewards config falls back to default precision without subtoken', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const configEvent = TestHelpers.RewardsController.AssetConfigUpdated.createMockEvent({
    asset: ADDRESSES.debtAsset,
    reward: ADDRESSES.reward,
    oldEmission: 0n,
    newEmission: 5n,
    oldDistributionEnd: 0n,
    newDistributionEnd: 1000n,
    assetIndex: 123n,
    ...eventData(20, 300, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.AssetConfigUpdated.processEvent({
    event: configEvent,
    mockDb,
  });

  const rewardId = `${ADDRESSES.controller}:${ADDRESSES.debtAsset}:${ADDRESSES.reward}`;
  const reward = mockDb.entities.Reward.get(rewardId);
  assert.ok(reward);
  assert.equal(reward?.precision, 18n);
});

test('rewards claimed decrements remaining unclaimed balance', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.User.set({
    id: ADDRESSES.user,
    borrowedReservesCount: 0,
    eModeCategoryId_id: undefined,
    points_id: undefined,
    totalSelfRepaymentsReceived: 0n,
    unclaimedRewards: 200n,
    lifetimeRewards: 200n,
    rewardsLastUpdated: 0,
    userVault_id: undefined,
  });

  const claimEvent = TestHelpers.RewardsController.RewardsClaimed.createMockEvent({
    user: ADDRESSES.user,
    reward: ADDRESSES.reward,
    to: ADDRESSES.to,
    claimer: ADDRESSES.claimer,
    amount: 150n,
    ...eventData(21, 310, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.RewardsClaimed.processEvent({
    event: claimEvent,
    mockDb,
  });

  const updatedUser = mockDb.entities.User.get(ADDRESSES.user);
  assert.equal(updatedUser?.unclaimedRewards, 50n);
});

test('rewards controller claimer and transfer strategy updates', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const claimerEvent = TestHelpers.RewardsController.ClaimerSet.createMockEvent({
    user: ADDRESSES.user,
    claimer: ADDRESSES.claimer,
    ...eventData(4, 130, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.ClaimerSet.processEvent({
    event: claimerEvent,
    mockDb,
  });

  const claimerId = `${ADDRESSES.controller}-${ADDRESSES.user}`;
  const claimer = mockDb.entities.RewardClaimer.get(claimerId);
  assert.ok(claimer);
  assert.equal(claimer?.claimer, ADDRESSES.claimer);

  const strategyEvent = TestHelpers.RewardsController.TransferStrategyInstalled.createMockEvent({
    reward: ADDRESSES.reward,
    transferStrategy: ADDRESSES.distributorNew,
    ...eventData(5, 140, ADDRESSES.controller),
  });
  mockDb = await TestHelpers.RewardsController.TransferStrategyInstalled.processEvent({
    event: strategyEvent,
    mockDb,
  });

  const strategyId = `${ADDRESSES.controller}-${ADDRESSES.reward}`;
  const strategy = mockDb.entities.RewardTransferStrategy.get(strategyId);
  assert.ok(strategy);
  assert.equal(strategy?.strategy, ADDRESSES.distributorNew);
});

test('revenue reward notifications and claims accumulate totals', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const notifyEvent = TestHelpers.RevenueReward.NotifyReward.createMockEvent({
    from: ADDRESSES.user,
    token: ADDRESSES.reward,
    epoch: 1n,
    amount: 500n,
    ...eventData(6, 150, ADDRESSES.revenueReward),
  });
  mockDb = await TestHelpers.RevenueReward.NotifyReward.processEvent({
    event: notifyEvent,
    mockDb,
  });

  const notifyEventTwo = TestHelpers.RevenueReward.NotifyReward.createMockEvent({
    from: ADDRESSES.user,
    token: ADDRESSES.reward,
    epoch: 1n,
    amount: 250n,
    ...eventData(7, 160, ADDRESSES.revenueReward),
  });
  mockDb = await TestHelpers.RevenueReward.NotifyReward.processEvent({
    event: notifyEventTwo,
    mockDb,
  });

  const rewardToken = mockDb.entities.RevenueRewardToken.get(ADDRESSES.reward);
  assert.ok(rewardToken);
  assert.equal(rewardToken?.totalNotified, 750n);

  const epochId = `${ADDRESSES.reward}:1`;
  const epoch = mockDb.entities.RevenueRewardEpoch.get(epochId);
  assert.ok(epoch);
  assert.equal(epoch?.amount, 750n);

  const claimEvent = TestHelpers.RevenueReward.ClaimRewards.createMockEvent({
    tokenId: 1n,
    user: ADDRESSES.user,
    rewardToken: ADDRESSES.reward,
    amount: 200n,
    ...eventData(8, 170, ADDRESSES.revenueReward),
  });
  mockDb = await TestHelpers.RevenueReward.ClaimRewards.processEvent({
    event: claimEvent,
    mockDb,
  });

  const claimId = `${claimEvent.transaction.hash}-${claimEvent.logIndex}`;
  const claim = mockDb.entities.RevenueRewardClaim.get(claimId);
  assert.ok(claim);

  const updatedToken = mockDb.entities.RevenueRewardToken.get(ADDRESSES.reward);
  assert.equal(updatedToken?.totalClaimed, 200n);
});

test('revenue reward recover and distributor update records are created', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const recoverEvent = TestHelpers.RevenueReward.RecoverTokens.createMockEvent({
    token: ADDRESSES.reward,
    amount: 42n,
    ...eventData(9, 180, ADDRESSES.revenueReward),
  });
  mockDb = await TestHelpers.RevenueReward.RecoverTokens.processEvent({
    event: recoverEvent,
    mockDb,
  });

  const recoverId = `${recoverEvent.transaction.hash}-${recoverEvent.logIndex}`;
  assert.ok(mockDb.entities.RevenueRewardRecovery.get(recoverId));

  const distributorEvent = TestHelpers.RevenueReward.RewardDistributorUpdated.createMockEvent({
    oldDistributor: ADDRESSES.distributorOld,
    newDistributor: ADDRESSES.distributorNew,
    ...eventData(10, 190, ADDRESSES.revenueReward),
  });
  mockDb = await TestHelpers.RevenueReward.RewardDistributorUpdated.processEvent({
    event: distributorEvent,
    mockDb,
  });

  const distributorId = `${distributorEvent.transaction.hash}-${distributorEvent.logIndex}`;
  assert.ok(mockDb.entities.RevenueRewardDistributorUpdate.get(distributorId));
});

test('self-repay updates vault, user, and protocol totals', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const createEvent = TestHelpers.UserVaultFactory.UserVaultCreated.createMockEvent({
    user: ADDRESSES.user,
    vault: ADDRESSES.vault,
    ...eventData(11, 200, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.UserVaultFactory.UserVaultCreated.processEvent({
    event: createEvent,
    mockDb,
  });

  const vaultEntity = mockDb.entities.UserVaultEntity.get(ADDRESSES.vault.toLowerCase());
  const vault = mockDb.entities.UserVault.get(ADDRESSES.vault.toLowerCase());
  assert.ok(vaultEntity);
  assert.ok(vault);

  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.collateralAsset,
    pool_id: 'pool',
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.debtUnderlying,
    underlyingAssetDecimals: 18,
  });

  const repayEvent = TestHelpers.UserVault.LoanSelfRepaid.createMockEvent({
    user: ADDRESSES.user,
    vault: ADDRESSES.vault,
    debtAsset: ADDRESSES.debtAsset,
    collateralAsset: ADDRESSES.collateralAsset,
    amount: 300n,
    ...eventData(12, 210, ADDRESSES.vault),
  });
  mockDb = await TestHelpers.UserVault.LoanSelfRepaid.processEvent({
    event: repayEvent,
    mockDb,
  });

  const repaymentId = `${repayEvent.transaction.hash}-${repayEvent.logIndex}`;
  const repayment = mockDb.entities.LoanSelfRepayment.get(repaymentId);
  assert.ok(repayment);
  assert.equal(repayment?.poolAddressesProvider, ADDRESSES.debtAsset.toLowerCase());
  assert.equal(repayment?.debtAsset, ADDRESSES.debtUnderlying.toLowerCase());

  const updatedVault = mockDb.entities.UserVault.get(ADDRESSES.vault.toLowerCase());
  assert.equal(updatedVault?.totalRepayVolume, 300n);
  assert.equal(updatedVault?.repayCount, 1n);

  const updatedEntity = mockDb.entities.UserVaultEntity.get(ADDRESSES.vault.toLowerCase());
  assert.equal(updatedEntity?.totalSelfRepayVolume, 300n);
  assert.equal(updatedEntity?.totalSelfRepayCount, 1n);

  const user = mockDb.entities.User.get(ADDRESSES.user.toLowerCase());
  assert.equal(user?.totalSelfRepaymentsReceived, 300n);

  const stats = mockDb.entities.ProtocolStats.get('1');
  assert.equal(stats?.totalSelfRepayVolume, 300n);
  assert.equal(stats?.totalSelfRepayCount, 1n);
});

test('self-repay toggle updates DustLockToken and history', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const transferEvent = TestHelpers.DustLock.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: 1n,
    ...eventData(13, 220, ADDRESSES.provider),
  });
  mockDb = await TestHelpers.DustLock.Transfer.processEvent({
    event: transferEvent,
    mockDb,
  });

  const updateEvent = TestHelpers.RevenueReward.SelfRepayingLoanUpdate.createMockEvent({
    token: 1n,
    rewardReceiver: ADDRESSES.rewardReceiver,
    isEnabled: true,
    ...eventData(14, 230, ADDRESSES.revenueReward),
  });
  mockDb = await TestHelpers.RevenueReward.SelfRepayingLoanUpdate.processEvent({
    event: updateEvent,
    mockDb,
  });

  const token = mockDb.entities.DustLockToken.get('1');
  assert.ok(token);
  assert.equal(token?.selfRepayEnabled, true);
  assert.equal(token?.rewardReceiver, ADDRESSES.rewardReceiver);

  const selfRepay = mockDb.entities.SelfRepayingLoan.get('1');
  assert.ok(selfRepay);
  assert.equal(selfRepay?.receiver, ADDRESSES.rewardReceiver);

  const updateId = `${updateEvent.transaction.hash}-${updateEvent.logIndex}`;
  const update = mockDb.entities.SelfRepayLoanUpdate.get(updateId);
  assert.ok(update);
  assert.equal(update?.user, ADDRESSES.user);
});

test('self-repay update records empty user when token missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const updateEvent = TestHelpers.RevenueReward.SelfRepayingLoanUpdate.createMockEvent({
    token: 2n,
    rewardReceiver: ADDRESSES.rewardReceiver,
    isEnabled: false,
    ...eventData(15, 240, ADDRESSES.revenueReward),
  });
  mockDb = await TestHelpers.RevenueReward.SelfRepayingLoanUpdate.processEvent({
    event: updateEvent,
    mockDb,
  });

  const updateId = `${updateEvent.transaction.hash}-${updateEvent.logIndex}`;
  const update = mockDb.entities.SelfRepayLoanUpdate.get(updateId);
  assert.ok(update);
  assert.equal(update?.user, '');
});

test('dust token events increment stats and create history', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const transfer = TestHelpers.DustToken.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.dustTokenOwner,
    value: 10n,
    ...eventData(15, 240, ADDRESSES.dustToken),
  });
  mockDb = await TestHelpers.DustToken.Transfer.processEvent({ event: transfer, mockDb });

  const transferOut = TestHelpers.DustToken.Transfer.createMockEvent({
    from: ADDRESSES.dustTokenOwner,
    to: ADDRESSES.user,
    value: 5n,
    ...eventData(16, 245, ADDRESSES.dustToken),
  });
  mockDb = await TestHelpers.DustToken.Transfer.processEvent({ event: transferOut, mockDb });

  const approval = TestHelpers.DustToken.Approval.createMockEvent({
    owner: ADDRESSES.dustTokenOwner,
    spender: ADDRESSES.user,
    value: 5n,
    ...eventData(17, 250, ADDRESSES.dustToken),
  });
  mockDb = await TestHelpers.DustToken.Approval.processEvent({ event: approval, mockDb });

  const ownershipStart = TestHelpers.DustToken.OwnershipTransferStarted.createMockEvent({
    previousOwner: ADDRESSES.dustTokenOwner,
    newOwner: ADDRESSES.user,
    ...eventData(17, 260, ADDRESSES.dustToken),
  });
  mockDb = await TestHelpers.DustToken.OwnershipTransferStarted.processEvent({
    event: ownershipStart,
    mockDb,
  });

  const ownershipDone = TestHelpers.DustToken.OwnershipTransferred.createMockEvent({
    previousOwner: ADDRESSES.dustTokenOwner,
    newOwner: ADDRESSES.user,
    ...eventData(18, 270, ADDRESSES.dustToken),
  });
  mockDb = await TestHelpers.DustToken.OwnershipTransferred.processEvent({
    event: ownershipDone,
    mockDb,
  });

  const pause = TestHelpers.DustToken.Paused.createMockEvent({
    account: ADDRESSES.user,
    ...eventData(19, 280, ADDRESSES.dustToken),
  });
  mockDb = await TestHelpers.DustToken.Paused.processEvent({ event: pause, mockDb });

  const unpause = TestHelpers.DustToken.Unpaused.createMockEvent({
    account: ADDRESSES.user,
    ...eventData(20, 290, ADDRESSES.dustToken),
  });
  mockDb = await TestHelpers.DustToken.Unpaused.processEvent({ event: unpause, mockDb });

  const stats = mockDb.entities.DustTokenStat.get('dust-token-stats');
  assert.ok(stats);
  assert.equal(stats?.transferCount, 2n);
  assert.equal(stats?.approvalCount, 1n);
  assert.equal(stats?.ownershipChangeCount, 2n);
  assert.equal(stats?.pauseEventCount, 2n);
  assert.equal(stats?.paused, false);

  const protocolStats = mockDb.entities.ProtocolStats.get('1');
  assert.equal(protocolStats?.totalDustTransfers, 2n);
});
