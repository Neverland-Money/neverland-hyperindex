/**
 * LeaderboardKeeper Event Handlers
 * VotingPowerSynced, NFTBalanceSynced, LPBalanceSynced, UserSettled
 */

import {
  calculateNFTMultiplierFromCount,
  calculateVPMultiplier,
  composeCombinedMultiplierBps,
  findVPTierIndex,
  getOrCreateUserLeaderboardState,
  recordProtocolTransaction,
  settleAllLPHolders,
  settlePointsForAllReserves,
  type ScheduledEpochTransitionProjection,
} from './shared';
import { normalizeAddress } from '../helpers/constants';
import { syncUserLPPositionsFromChain } from './lp';

import type {
  LeaderboardEpoch,
  UserEpochFinalization,
  EvmOnEventContext as handlerContext,
} from 'envio';
import { indexer } from './registry';
/**
 * Keeper `UserSettled` settlement modes.
 *
 * Every keeper settlement is processed in full. A previous backfill optimization
 * (`ENVIO_LEADERBOARD_LIVE_EPOCH`) skipped the reserve sweep for mid-epoch settlements in
 * closed past epochs, on the assumption it was a no-op for final points. It was not: reserve
 * points use the point-in-time combined multiplier sampled at each settlement, so skipping
 * settlements shifts them. Measured against production and the published Tide draws, that gate
 * altered deposit, borrow and VP-multiplier points for ~1,000 / ~600 / ~800 users per gated
 * Tide, and moved 92-97 of every 100 published winners in Tides 1-5. It has been removed; the
 * raw `LeaderboardKeeperUserSettled` row is recorded and the settlement is always applied.
 */

export type KeeperSettlementMode = 'LIVE_OR_UNVERIFIED_ACTIVE' | 'GAP_FINALIZE' | 'GAP_DUPLICATE';

function isExactUsableClosedKeeperEpoch(
  epoch: LeaderboardEpoch | undefined,
  epochNumber: bigint
): epoch is LeaderboardEpoch & { endTime: number } {
  return (
    epoch !== undefined &&
    epoch.id === epochNumber.toString() &&
    epoch.epochNumber === epochNumber &&
    !epoch.isActive &&
    epoch.endTime !== undefined &&
    epoch.endTime > 0 &&
    epoch.endTime >= epoch.startTime
  );
}

async function validateKeeperGapFinalizationProof(
  context: handlerContext,
  normalizedUserId: string,
  epochNumber: bigint,
  finalizationId: string,
  finalization: UserEpochFinalization
): Promise<void> {
  const [epoch, settlementEvent] = await Promise.all([
    context.LeaderboardEpoch.get(epochNumber.toString()),
    context.LeaderboardKeeperUserSettled.get(finalization.settlementEventId),
  ]);
  if (
    finalization.id !== finalizationId ||
    normalizeAddress(finalization.user_id) !== normalizedUserId ||
    finalization.epochNumber !== epochNumber ||
    !isExactUsableClosedKeeperEpoch(epoch, epochNumber) ||
    finalization.epochEndTime !== epoch.endTime ||
    finalization.settledThrough !== epoch.endTime ||
    finalization.finalizedAt < epoch.endTime ||
    !settlementEvent ||
    settlementEvent.id !== finalization.settlementEventId ||
    normalizeAddress(settlementEvent.user_id) !== normalizedUserId ||
    settlementEvent.epochNumber !== epochNumber ||
    !settlementEvent.isGap ||
    settlementEvent.timestamp !== finalization.finalizedAt ||
    settlementEvent.txHash !== finalization.txHash
  ) {
    throw new Error(`invalid keeper gap finalization proof: ${finalizationId}`);
  }
}

export async function classifyKeeperSettlement(
  context: handlerContext,
  userId: string
): Promise<{
  mode: KeeperSettlementMode;
  epochNumber: bigint;
  finalizationId: string;
}> {
  const state = await context.LeaderboardState.get('current');
  const epochNumber = state?.currentEpochNumber ?? 0n;
  const finalizationId = `${normalizeAddress(userId)}:${epochNumber}`;

  if (!state || state.isActive) {
    return {
      mode: 'LIVE_OR_UNVERIFIED_ACTIVE',
      epochNumber,
      finalizationId,
    };
  }

  const finalization = await context.UserEpochFinalization.get(finalizationId);
  if (finalization) {
    await validateKeeperGapFinalizationProof(
      context,
      normalizeAddress(userId),
      epochNumber,
      finalizationId,
      finalization
    );
  }
  return {
    mode: finalization ? 'GAP_DUPLICATE' : 'GAP_FINALIZE',
    epochNumber,
    finalizationId,
  };
}

export function resolveKeeperEventTimestamp(
  eventTimestamp: bigint | undefined,
  blockTimestamp: number
): number {
  return Number(eventTimestamp ?? blockTimestamp);
}

function withScheduledEpochProjection(
  context: handlerContext,
  projection: ScheduledEpochTransitionProjection
): handlerContext {
  if (!projection.transitioned || context.isPreload !== true || !projection.state) {
    return context;
  }
  const stateStore = Object.create(context.LeaderboardState) as handlerContext['LeaderboardState'];
  Object.defineProperty(stateStore, 'get', { value: async () => projection.state });
  const projectedEpochId = projection.state.currentEpochNumber.toString();
  const epochStore = Object.create(context.LeaderboardEpoch) as handlerContext['LeaderboardEpoch'];
  Object.defineProperty(epochStore, 'get', {
    value: async (id: string) => {
      const storedEpoch = await context.LeaderboardEpoch.get(id);
      return new Map<string, LeaderboardEpoch | undefined>([
        [id, storedEpoch],
        [projectedEpochId, projection.epoch],
      ]).get(id);
    },
  });
  const projectedContext = Object.create(context) as handlerContext;
  Object.defineProperties(projectedContext, {
    LeaderboardState: { value: stateStore },
    LeaderboardEpoch: { value: epochStore },
  });
  return projectedContext;
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
    const transitionProjection = await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );
    const phaseContext = withScheduledEpochProjection(context, transitionProjection);
    const userId = normalizeAddress(event.params.user);
    const timestamp = resolveKeeperEventTimestamp(event.params.timestamp, event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    const eventTimeState = await phaseContext.LeaderboardState.get('current');
    const epochNumber = eventTimeState?.currentEpochNumber ?? 0n;
    const isGap = eventTimeState ? !eventTimeState.isActive : false;

    const settlementId = `${event.transaction.hash}-${event.logIndex}`;
    context.LeaderboardKeeperUserSettled.set({
      id: settlementId,
      user_id: userId,
      epochNumber,
      isGap,
      timestamp,
      txHash: event.transaction.hash,
    });

    const classification = await classifyKeeperSettlement(phaseContext, userId);
    if (classification.mode === 'GAP_DUPLICATE') {
      return;
    }

    if (classification.mode === 'GAP_FINALIZE') {
      const epoch = await phaseContext.LeaderboardEpoch.get(classification.epochNumber.toString());
      if (!isExactUsableClosedKeeperEpoch(epoch, classification.epochNumber)) {
        throw new Error(
          `keeper gap finalization requires an exact closed LeaderboardEpoch: ${classification.epochNumber.toString()}`
        );
      }

      await settlePointsForAllReserves(phaseContext, userId, timestamp, blockNumber, {
        ignoreCooldown: true,
      });
      context.UserEpochFinalization.set({
        id: classification.finalizationId,
        user_id: userId,
        epochNumber: classification.epochNumber,
        epochEndTime: epoch.endTime,
        settledThrough: epoch.endTime,
        finalizedAt: timestamp,
        blockNumber,
        txHash: event.transaction.hash,
        settlementEventId: settlementId,
      });
      return;
    }

    // Backfill gate: skip the HEAVY reserve sweep for a mid-epoch keeper settlement
    // in a closed past epoch — for reserve users that accrual is recaptured by later
    // balance changes + the gap settlement, so the mid-epoch pass is redundant.
    //
    // BUT a pure-VP user (no lending reserves) must still settle here: veDUST events
    // do not accrue points, so for a closed past epoch the keeper UserSettled is the
    // ONLY ungated touch that can credit their VP decay tail (e.g. a lock that
    // decayed in epoch N whose owner had no other event in N — there is no later
    // balance change to recapture it). The pure-VP path is cheap (empty reserve
    // loop, no eth_call) and idempotent via the per-(user,epoch) accrual cursor, so
    // running it even mid-epoch is safe. The raw event above is recorded regardless.

    await settlePointsForAllReserves(phaseContext, userId, timestamp, blockNumber, {
      ignoreCooldown: true,
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

    // The keeper's ceremony is threshold-filtered, so it never settles a sub-threshold LP-only
    // holder, and no market event touches one either. Under the lazy fungible-growth model that
    // holder accrues into a cursor and stays absent from the OPEN Tide's leaderboard until the
    // Tide closes, while the pre-rewrite indexer credits it live. Sweeping here restores
    // agreement at the operator's settle-users moment. Bounded to holders with liquidity > 0,
    // and points inside a prefilled Tide stay suppressed by updateUserEpochLPPoints.
    await settleAllLPHolders(context, Number(event.block.timestamp));
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
