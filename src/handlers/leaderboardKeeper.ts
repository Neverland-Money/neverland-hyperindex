/**
 * LeaderboardKeeper Event Handlers
 * VotingPowerSynced, NFTBalanceSynced, LPBalanceSynced, UserSettled
 */

import { LeaderboardKeeper } from '../../generated';
import type { handlerContext } from '../../generated';
import {
  calculateNFTMultiplierFromCount,
  calculateVPMultiplier,
  findVPTierIndex,
  getOrCreateUserLeaderboardState,
  recordProtocolTransaction,
  settlePointsForAllReserves,
} from './shared';
import { normalizeAddress } from '../helpers/constants';

const BASIS_POINTS = 10000n;
const MAX_COMBINED_MULTIPLIER = 100000n;

async function getOrCreateKeeperState(context: handlerContext, timestamp: number) {
  let state = await context.LeaderboardKeeperState.get('current');
  if (!state) {
    state = {
      id: 'current',
      keeper: undefined,
      owner: undefined,
      minSettlementInterval: undefined,
      selfSyncCooldown: undefined,
      lastUpdate: timestamp,
    };
    context.LeaderboardKeeperState.set(state);
  }
  return state;
}

LeaderboardKeeper.VotingPowerSynced.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const userId = normalizeAddress(event.params.user);
  const timestamp = Number(event.params.timestamp);

  const state = await getOrCreateUserLeaderboardState(context, userId, timestamp);
  const votingPower = event.params.votingPower;
  const vpMultiplier = await calculateVPMultiplier(context, votingPower);
  const vpTierIndex = await findVPTierIndex(context, votingPower);
  const nftMultiplier = await calculateNFTMultiplierFromCount(context, state.nftCount);

  let combinedMultiplier = (nftMultiplier * vpMultiplier) / BASIS_POINTS;
  if (combinedMultiplier > MAX_COMBINED_MULTIPLIER) {
    combinedMultiplier = MAX_COMBINED_MULTIPLIER;
  }

  context.UserLeaderboardState.set({
    ...state,
    votingPower,
    vpMultiplier,
    vpTierIndex,
    nftMultiplier,
    combinedMultiplier,
    lastUpdate: timestamp,
  });

  const syncId = `${event.transaction.hash}-${event.logIndex}`;
  context.LeaderboardKeeperVotingPowerSynced.set({
    id: syncId,
    user_id: userId,
    votingPower,
    timestamp,
    txHash: event.transaction.hash,
  });
});

LeaderboardKeeper.NFTBalanceSynced.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const userId = normalizeAddress(event.params.user);
  const collection = normalizeAddress(event.params.collection);
  const balance = event.params.balance;
  const timestamp = Number(event.params.timestamp);
  const blockNumber = BigInt(event.block.number);

  const ownershipId = `${userId}:${collection}`;
  const hadBalance = (await context.UserNFTOwnership.get(ownershipId))?.balance ?? 0n;
  const hadNFT = hadBalance > 0n;
  const hasNFT = balance > 0n;

  if (hasNFT) {
    context.UserNFTOwnership.set({
      id: ownershipId,
      user_id: userId,
      partnership_id: collection,
      balance,
      hasNFT: true,
      lastCheckedAt: timestamp,
      lastCheckedBlock: blockNumber,
    });
  } else if (hadNFT) {
    context.UserNFTOwnership.deleteUnsafe(ownershipId);
  }

  context.UserNFTBaseline.set({
    id: ownershipId,
    user_id: userId,
    partnership_id: collection,
    checkedAt: timestamp,
    checkedBlock: blockNumber,
  });

  if (hadNFT !== hasNFT) {
    const state = await getOrCreateUserLeaderboardState(context, userId, timestamp);
    let newNftCount = state.nftCount;
    if (hasNFT && !hadNFT) {
      newNftCount = state.nftCount + 1n;
    } else if (!hasNFT && hadNFT) {
      newNftCount = state.nftCount > 0n ? state.nftCount - 1n : 0n;
    }

    const nftMultiplier = await calculateNFTMultiplierFromCount(context, newNftCount);
    let combinedMultiplier = (nftMultiplier * state.vpMultiplier) / BASIS_POINTS;
    if (combinedMultiplier > MAX_COMBINED_MULTIPLIER) {
      combinedMultiplier = MAX_COMBINED_MULTIPLIER;
    }

    context.UserLeaderboardState.set({
      ...state,
      nftCount: newNftCount,
      nftMultiplier,
      combinedMultiplier,
      lastUpdate: timestamp,
    });
  }

  const syncId = `${event.transaction.hash}-${event.logIndex}`;
  context.LeaderboardKeeperNFTBalanceSynced.set({
    id: syncId,
    user_id: userId,
    collection,
    balance,
    timestamp,
    txHash: event.transaction.hash,
  });
});

LeaderboardKeeper.LPBalanceSynced.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const userId = normalizeAddress(event.params.user);
  const pool = normalizeAddress(event.params.pool);
  const liquidity = event.params.liquidity;
  const timestamp = Number(event.params.timestamp);
  const blockNumber = BigInt(event.block.number);

  const poolConfig = await context.LPPoolConfig.get(pool);
  if (poolConfig && liquidity > 0n) {
    const { syncUserLPPositionsFromChain } = await import('./lp');
    await syncUserLPPositionsFromChain(context, userId, timestamp, blockNumber, {
      forceRescan: true,
      managers: [poolConfig.positionManager],
    });
  }

  const syncId = `${event.transaction.hash}-${event.logIndex}`;
  context.LeaderboardKeeperLPBalanceSynced.set({
    id: syncId,
    user_id: userId,
    pool,
    liquidity,
    timestamp,
    txHash: event.transaction.hash,
  });
});

LeaderboardKeeper.UserSettled.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const userId = normalizeAddress(event.params.user);
  const timestamp = Number(event.params.timestamp ?? event.block.timestamp);
  const blockNumber = BigInt(event.block.number);

  const settlementId = `${event.transaction.hash}-${event.logIndex}`;
  context.LeaderboardKeeperUserSettled.set({
    id: settlementId,
    user_id: userId,
    timestamp,
    txHash: event.transaction.hash,
  });

  await settlePointsForAllReserves(context, userId, timestamp, blockNumber, {
    ignoreCooldown: true,
  });
});

LeaderboardKeeper.BatchComplete.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = `${event.transaction.hash}-${event.logIndex}`;
  context.LeaderboardKeeperBatchComplete.set({
    id,
    operation: event.params.operation,
    count: event.params.count,
    timestamp: Number(event.params.timestamp),
    txHash: event.transaction.hash,
  });
});

LeaderboardKeeper.KeeperUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.LeaderboardKeeperKeeperUpdate.set({
    id,
    oldKeeper: normalizeAddress(event.params.oldKeeper),
    newKeeper: normalizeAddress(event.params.newKeeper),
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateKeeperState(context, timestamp);
  context.LeaderboardKeeperState.set({
    ...state,
    keeper: normalizeAddress(event.params.newKeeper),
    lastUpdate: timestamp,
  });
});

LeaderboardKeeper.MinSettlementIntervalUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.LeaderboardKeeperMinSettlementIntervalUpdate.set({
    id,
    oldInterval: event.params.oldInterval,
    newInterval: event.params.newInterval,
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateKeeperState(context, timestamp);
  context.LeaderboardKeeperState.set({
    ...state,
    minSettlementInterval: event.params.newInterval,
    lastUpdate: timestamp,
  });
});

LeaderboardKeeper.SelfSyncCooldownUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.LeaderboardKeeperSelfSyncCooldownUpdate.set({
    id,
    oldCooldown: event.params.oldCooldown,
    newCooldown: event.params.newCooldown,
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateKeeperState(context, timestamp);
  context.LeaderboardKeeperState.set({
    ...state,
    selfSyncCooldown: event.params.newCooldown,
    lastUpdate: timestamp,
  });
});

LeaderboardKeeper.OwnershipTransferred.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.LeaderboardKeeperOwnershipTransferred.set({
    id,
    previousOwner: normalizeAddress(event.params.previousOwner),
    newOwner: normalizeAddress(event.params.newOwner),
    timestamp,
    txHash: event.transaction.hash,
  });

  const state = await getOrCreateKeeperState(context, timestamp);
  context.LeaderboardKeeperState.set({
    ...state,
    owner: normalizeAddress(event.params.newOwner),
    lastUpdate: timestamp,
  });
});

LeaderboardKeeper.Initialized.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.block.timestamp);
  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.LeaderboardKeeperInitialized.set({
    id,
    version: Number(event.params.version),
    timestamp,
    txHash: event.transaction.hash,
  });
});
