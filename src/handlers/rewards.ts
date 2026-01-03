/**
 * Rewards Event Handlers
 * RewardsController, RevenueReward, DustToken
 */

import type { handlerContext } from '../../generated';
import { RewardsController, RevenueReward, DustToken } from '../../generated';
import { ZERO_ADDRESS, normalizeAddress } from '../helpers/constants';
import { recordProtocolTransaction, getOrCreateUser, getOrCreateProtocolStats } from './shared';

async function getOrCreateDustTokenStat(context: handlerContext) {
  const id = 'dust-token-stats';
  let stat = await context.DustTokenStat.get(id);
  if (!stat) {
    stat = {
      id,
      transferCount: 0n,
      approvalCount: 0n,
      ownershipChangeCount: 0n,
      pauseEventCount: 0n,
      paused: false,
      lastUpdate: 0,
    };
    context.DustTokenStat.set(stat);
  }
  return stat;
}

// ============================================
// RewardsController Handlers
// ============================================

RewardsController.AssetConfigUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const controllerId = normalizeAddress(event.srcAddress);
  const asset = normalizeAddress(event.params.asset);
  const rewardAddress = normalizeAddress(event.params.reward);
  const id = `${controllerId}-${asset}-${rewardAddress}`;
  const previousConfig = await context.RewardAssetConfig.get(id);

  const oldDistributionEnd =
    previousConfig?.distributionEnd ?? Number(event.params.oldDistributionEnd);
  const periodStart = previousConfig?.updatedAt ?? timestamp;
  const periodEnd =
    oldDistributionEnd > 0 && timestamp > oldDistributionEnd ? oldDistributionEnd : timestamp;
  const emittedSeconds = periodEnd > periodStart ? periodEnd - periodStart : 0;
  const emittedAmount = BigInt(emittedSeconds) * event.params.oldEmission;

  context.RewardAssetConfigHistory.set({
    id: `${event.transaction.hash}-${event.logIndex}`,
    rewardsController: controllerId,
    asset,
    reward: rewardAddress,
    oldEmission: event.params.oldEmission,
    newEmission: event.params.newEmission,
    oldDistributionEnd: Number(event.params.oldDistributionEnd),
    newDistributionEnd: Number(event.params.newDistributionEnd),
    assetIndex: event.params.assetIndex,
    periodStart,
    periodEnd,
    emittedAmount,
    timestamp,
  });

  const controller = await context.RewardsController.get(controllerId);
  if (!controller) {
    context.RewardsController.set({ id: controllerId });
  }

  const rewardId = `${controllerId}:${asset}:${rewardAddress}`;
  let reward = await context.Reward.get(rewardId);
  if (!reward) {
    const subToken = await context.SubToken.get(asset);
    const precision = BigInt(subToken?.underlyingAssetDecimals ?? 18);

    reward = {
      id: rewardId,
      rewardToken: rewardAddress,
      asset,
      rewardsController: controllerId,
      index: event.params.assetIndex,
      distributionEnd: Number(event.params.newDistributionEnd),
      emissionsPerSecond: event.params.newEmission,
      precision,
      createdAt: timestamp,
      updatedAt: timestamp,
      rewardTokenDecimals: 18,
      rewardTokenSymbol: 'RWD',
      rewardFeedOracle: rewardAddress,
    };
  } else {
    reward = {
      ...reward,
      index: event.params.assetIndex,
      distributionEnd: Number(event.params.newDistributionEnd),
      emissionsPerSecond: event.params.newEmission,
      updatedAt: timestamp,
    };
  }

  let oracle = await context.RewardFeedOracle.get(rewardAddress);
  if (!oracle) {
    oracle = {
      id: rewardAddress,
      rewardFeedAddress: ZERO_ADDRESS,
      createdAt: timestamp,
      updatedAt: timestamp,
    };
    context.RewardFeedOracle.set(oracle);
  }

  context.Reward.set(reward);

  context.RewardAssetConfig.set({
    id,
    rewardsController: controllerId,
    asset,
    reward: rewardAddress,
    emission: event.params.newEmission,
    distributionEnd: Number(event.params.newDistributionEnd),
    assetIndex: event.params.assetIndex,
    updatedAt: timestamp,
  });
});

RewardsController.Accrued.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const userAddress = normalizeAddress(event.params.user);
  await getOrCreateUser(context, userAddress);
  const timestamp = Number(event.block.timestamp);
  const amount = event.params.rewardsAccrued;

  const user = await context.User.get(userAddress);
  if (user) {
    context.User.set({
      ...user,
      unclaimedRewards: user.unclaimedRewards + amount,
      lifetimeRewards: user.lifetimeRewards + amount,
      rewardsLastUpdated: timestamp,
    });
  }

  const rewardId = `${normalizeAddress(event.srcAddress)}:${normalizeAddress(
    event.params.asset
  )}:${normalizeAddress(event.params.reward)}`;
  const reward = await context.Reward.get(rewardId);
  if (reward) {
    context.Reward.set({
      ...reward,
      index: event.params.assetIndex,
      updatedAt: timestamp,
    });
  }

  const userRewardId = `${rewardId}:${userAddress}`;
  let userReward = await context.UserReward.get(userRewardId);

  if (!userReward) {
    userReward = {
      id: userRewardId,
      user: userAddress,
      reward: rewardId,
      index: event.params.userIndex,
      createdAt: timestamp,
      updatedAt: timestamp,
    };
  } else {
    userReward = {
      ...userReward,
      index: event.params.userIndex,
      updatedAt: timestamp,
    };
  }

  context.UserReward.set(userReward);

  const rewardedId = `${event.transaction.hash}-${event.logIndex}`;
  context.RewardedAction.set({
    id: rewardedId,
    rewardsController: normalizeAddress(event.srcAddress),
    user: userAddress,
    amount,
    txHash: event.transaction.hash,
    timestamp,
  });
});

RewardsController.RewardsClaimed.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const userAddress = normalizeAddress(event.params.user);
  await getOrCreateUser(context, userAddress);
  await getOrCreateUser(context, normalizeAddress(event.params.to));
  await getOrCreateUser(context, normalizeAddress(event.params.claimer));

  const amount = event.params.amount;

  const user = await context.User.get(userAddress);
  if (user) {
    context.User.set({
      ...user,
      unclaimedRewards: user.unclaimedRewards > amount ? user.unclaimedRewards - amount : 0n,
      rewardsLastUpdated: Number(event.block.timestamp),
    });
  }

  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.ClaimRewardsCall.set({
    id,
    rewardsController: normalizeAddress(event.srcAddress),
    user: userAddress,
    to: normalizeAddress(event.params.to),
    caller: normalizeAddress(event.params.claimer),
    amount,
    txHash: event.transaction.hash,
    action: 'ClaimRewardsCall',
    timestamp: Number(event.block.timestamp),
  });
});

RewardsController.ClaimerSet.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = `${normalizeAddress(event.srcAddress)}-${normalizeAddress(event.params.user)}`;

  context.RewardClaimer.set({
    id,
    rewardsController: normalizeAddress(event.srcAddress),
    user: normalizeAddress(event.params.user),
    claimer: normalizeAddress(event.params.claimer),
    updatedAt: Number(event.block.timestamp),
  });
});

RewardsController.TransferStrategyInstalled.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = `${normalizeAddress(event.srcAddress)}-${normalizeAddress(event.params.reward)}`;

  context.RewardTransferStrategy.set({
    id,
    rewardsController: normalizeAddress(event.srcAddress),
    reward: normalizeAddress(event.params.reward),
    strategy: normalizeAddress(event.params.transferStrategy),
    updatedAt: Number(event.block.timestamp),
  });
});

// ============================================
// RevenueReward Handlers
// ============================================

RevenueReward.NotifyReward.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = `${event.transaction.hash}-${event.logIndex}`;
  const from = normalizeAddress(event.params.from);
  const rewardToken = normalizeAddress(event.params.token);

  context.RevenueRewardNotification.set({
    id,
    from,
    rewardToken,
    amount: event.params.amount,
    epochId: event.params.epoch,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });

  const tokenAddress = rewardToken;
  let rewardTokenEntity = await context.RevenueRewardToken.get(tokenAddress);
  if (!rewardTokenEntity) {
    rewardTokenEntity = {
      id: tokenAddress,
      totalNotified: event.params.amount,
      totalClaimed: 0n,
      firstSeen: Number(event.block.timestamp),
      lastUpdated: Number(event.block.timestamp),
    };
  } else {
    rewardTokenEntity = {
      ...rewardTokenEntity,
      totalNotified: rewardTokenEntity.totalNotified + event.params.amount,
      lastUpdated: Number(event.block.timestamp),
    };
  }
  context.RevenueRewardToken.set(rewardTokenEntity);

  const epochId = `${tokenAddress}:${event.params.epoch}`;
  let rewardEpoch = await context.RevenueRewardEpoch.get(epochId);
  if (!rewardEpoch) {
    rewardEpoch = {
      id: epochId,
      token: tokenAddress,
      epoch: Number(event.params.epoch),
      amount: event.params.amount,
    };
  } else {
    rewardEpoch = {
      ...rewardEpoch,
      amount: rewardEpoch.amount + event.params.amount,
    };
  }
  context.RevenueRewardEpoch.set(rewardEpoch);
});

RevenueReward.ClaimRewards.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = `${event.transaction.hash}-${event.logIndex}`;
  const userAddress = normalizeAddress(event.params.user);
  const rewardToken = normalizeAddress(event.params.rewardToken);

  context.RevenueRewardClaim.set({
    id,
    tokenId: event.params.tokenId,
    user: userAddress,
    token: rewardToken,
    amount: event.params.amount,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });

  const tokenAddress = rewardToken;
  const rewardTokenEntity = await context.RevenueRewardToken.get(tokenAddress);
  if (rewardTokenEntity) {
    context.RevenueRewardToken.set({
      ...rewardTokenEntity,
      totalClaimed: rewardTokenEntity.totalClaimed + event.params.amount,
      lastUpdated: Number(event.block.timestamp),
    });
  }

  await getOrCreateUser(context, userAddress);
});

RevenueReward.RecoverTokens.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.RevenueRewardRecovery.set({
    id,
    token: normalizeAddress(event.params.token),
    amount: event.params.amount,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

RevenueReward.RewardDistributorUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.RevenueRewardDistributorUpdate.set({
    id,
    oldDistributor: normalizeAddress(event.params.oldDistributor),
    newDistributor: normalizeAddress(event.params.newDistributor),
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

RevenueReward.SelfRepayingLoanUpdate.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const tokenId = event.params.token.toString();
  const timestamp = Number(event.block.timestamp);

  context.SelfRepayingLoan.set({
    id: tokenId,
    tokenId: event.params.token,
    receiver: normalizeAddress(event.params.rewardReceiver),
    enabled: event.params.isEnabled,
    updatedAt: timestamp,
  });

  const token = await context.DustLockToken.get(tokenId);
  if (token) {
    context.DustLockToken.set({
      ...token,
      selfRepayEnabled: event.params.isEnabled,
      rewardReceiver: normalizeAddress(event.params.rewardReceiver),
      updatedAt: timestamp,
    });
  }

  const updateId = `${event.transaction.hash}-${event.logIndex}`;
  context.SelfRepayLoanUpdate.set({
    id: updateId,
    tokenId: event.params.token,
    user: normalizeAddress(token?.owner ?? ''),
    receiver: normalizeAddress(event.params.rewardReceiver),
    isEnabled: event.params.isEnabled,
    txHash: event.transaction.hash,
    timestamp,
  });
});

// ============================================
// DustToken Handlers
// ============================================

DustToken.Transfer.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const stat = await getOrCreateDustTokenStat(context);
  context.DustTokenStat.set({
    ...stat,
    transferCount: stat.transferCount + 1n,
    lastUpdate: timestamp,
  });
  const id = `${event.transaction.hash}-${event.logIndex}`;

  const from = normalizeAddress(event.params.from);
  const to = normalizeAddress(event.params.to);
  context.DustTransfer.set({
    id,
    from,
    to,
    value: event.params.value,
    timestamp,
    txHash: event.transaction.hash,
  });

  const ps = await getOrCreateProtocolStats(context, Number(event.block.timestamp));
  context.ProtocolStats.set({
    ...ps,
    totalDustTransfers: ps.totalDustTransfers + 1n,
  });

  if (from !== ZERO_ADDRESS) {
    await getOrCreateUser(context, from);
  }

  if (to !== ZERO_ADDRESS) {
    await getOrCreateUser(context, to);
  }
});

DustToken.Approval.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const stat = await getOrCreateDustTokenStat(context);
  context.DustTokenStat.set({
    ...stat,
    approvalCount: stat.approvalCount + 1n,
    lastUpdate: timestamp,
  });
  const id = `${event.transaction.hash}-${event.logIndex}`;

  const owner = normalizeAddress(event.params.owner);
  const spender = normalizeAddress(event.params.spender);
  context.DustApproval.set({
    id,
    owner,
    spender,
    value: event.params.value,
    timestamp,
    txHash: event.transaction.hash,
  });

  if (owner !== ZERO_ADDRESS) {
    await getOrCreateUser(context, owner);
  }

  if (spender !== ZERO_ADDRESS) {
    await getOrCreateUser(context, spender);
  }
});

DustToken.OwnershipTransferStarted.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const stat = await getOrCreateDustTokenStat(context);
  context.DustTokenStat.set({
    ...stat,
    ownershipChangeCount: stat.ownershipChangeCount + 1n,
    lastUpdate: timestamp,
  });
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.DustOwnershipChange.set({
    id,
    previousOwner: normalizeAddress(event.params.previousOwner),
    newOwner: normalizeAddress(event.params.newOwner),
    timestamp,
    txHash: event.transaction.hash,
  });
});

DustToken.OwnershipTransferred.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const stat = await getOrCreateDustTokenStat(context);
  context.DustTokenStat.set({
    ...stat,
    ownershipChangeCount: stat.ownershipChangeCount + 1n,
    lastUpdate: timestamp,
  });
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.DustOwnershipChange.set({
    id,
    previousOwner: normalizeAddress(event.params.previousOwner),
    newOwner: normalizeAddress(event.params.newOwner),
    timestamp,
    txHash: event.transaction.hash,
  });
});

DustToken.Paused.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const stat = await getOrCreateDustTokenStat(context);
  context.DustTokenStat.set({
    ...stat,
    pauseEventCount: stat.pauseEventCount + 1n,
    paused: true,
    lastUpdate: timestamp,
  });
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.DustPauseEvent.set({
    id,
    account: normalizeAddress(event.params.account),
    paused: true,
    timestamp,
    txHash: event.transaction.hash,
  });
});

DustToken.Unpaused.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const stat = await getOrCreateDustTokenStat(context);
  context.DustTokenStat.set({
    ...stat,
    pauseEventCount: stat.pauseEventCount + 1n,
    paused: false,
    lastUpdate: timestamp,
  });
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.DustPauseEvent.set({
    id,
    account: normalizeAddress(event.params.account),
    paused: false,
    timestamp,
    txHash: event.transaction.hash,
  });
});
