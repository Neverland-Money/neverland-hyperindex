/**
 * LeaderboardKeeper Event Handlers
 * VotingPowerSynced, NFTBalanceSynced, LPBalanceSynced, UserSettled
 */

import { indexer } from 'envio';
import type { handlerContext } from '../types/envio';
import {
  calculateNFTMultiplierFromCount,
  calculateVPMultiplier,
  composeCombinedMultiplierBps,
  findVPTierIndex,
  getOrCreateUserLeaderboardState,
  recordProtocolTransaction,
  settlePointsForAllReserves,
} from './shared';
import { normalizeAddress } from '../helpers/constants';

function shouldSyncChainStateOnKeeperSettlement(): boolean {
  return process.env.ENVIO_KEEPER_USER_SETTLED_SYNC_CHAIN === 'true';
}

/**
 * Backfill optimization gate for keeper `UserSettled`.
 *
 * The indexer can't know the live Tide in advance while replaying history, so the operator sets
 * `ENVIO_LEADERBOARD_LIVE_EPOCH` to the current live epoch number before a backfill. With it set,
 * a keeper settlement that lands MID-EPOCH (`isActive`) inside a PAST/closed epoch (below the live
 * one) only re-forces points accrual that each balance-change settlement plus the epoch's gap
 * settlement already capture — a no-op for the final points — so the heavy reserve sweep is skipped.
 *
 * Always kept: GAP-period settlements (`isActive === false`, which finalize the epoch), the live
 * epoch, and any future epoch. Unset/0 disables the gate (original behavior). The raw
 * `LeaderboardKeeperUserSettled` event is still recorded regardless.
 *
 * CAVEAT: reserve points use the point-in-time combined multiplier sampled at each settlement
 * (VP points are averaged, so they're unaffected). Because `VotingPowerSynced` does not settle
 * reserves, skipping keeper settlements can shift reserve points for users whose VP-driven
 * multiplier changes between their own activity events. Validate against the prod parity check
 * (scripts/compare-leaderboard-parity.ts) before trusting it on a production index.
 */
export function getConfiguredLiveEpoch(): bigint {
  const raw = process.env.ENVIO_LEADERBOARD_LIVE_EPOCH;
  if (!raw) return 0n;
  try {
    const value = BigInt(raw);
    return value > 0n ? value : 0n;
  } catch {
    return 0n;
  }
}

export async function shouldSkipMidEpochKeeperSettle(context: handlerContext): Promise<boolean> {
  const liveEpoch = getConfiguredLiveEpoch();
  if (liveEpoch === 0n) return false;
  const state = await context.LeaderboardState.get('current');
  if (!state) return false;
  return state.currentEpochNumber < liveEpoch && state.isActive === true;
}

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

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'VotingPowerSynced' },
  async ({ event, context }) => {
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
    const combinedMultiplier = composeCombinedMultiplierBps(
      nftMultiplier,
      state.specialEditionMultiplier,
      vpMultiplier
    );

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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'NFTBalanceSynced' },
  async ({ event, context }) => {
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
      const combinedMultiplier = composeCombinedMultiplierBps(
        nftMultiplier,
        state.specialEditionMultiplier,
        state.vpMultiplier
      );

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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'LPBalanceSynced' },
  async ({ event, context }) => {
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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'UserSettled' },
  async ({ event, context }) => {
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

    // Backfill gate: a mid-epoch keeper settlement in a closed past epoch is a no-op for the
    // final points (accrual is captured by balance changes + the gap settlement), so skip the
    // heavy reserve sweep. The raw event above is still recorded for parity/observability.
    if (await shouldSkipMidEpochKeeperSettle(context)) return;

    const syncChainState = shouldSyncChainStateOnKeeperSettlement();
    await settlePointsForAllReserves(context, userId, timestamp, blockNumber, {
      ignoreCooldown: true,
      skipNftSync: !syncChainState,
      skipLPChainSync: !syncChainState,
    });
  }
);

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'BatchComplete' },
  async ({ event, context }) => {
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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'KeeperUpdated' },
  async ({ event, context }) => {
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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'MinSettlementIntervalUpdated' },
  async ({ event, context }) => {
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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'SelfSyncCooldownUpdated' },
  async ({ event, context }) => {
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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'OwnershipTransferred' },
  async ({ event, context }) => {
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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardKeeper', event: 'Initialized' },
  async ({ event, context }) => {
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
  }
);
