/**
 * Leaderboard Event Handlers
 * EpochManager, LeaderboardConfig, VotingPowerMultiplier
 */

import type { handlerContext } from '../../generated';
import {
  EpochManager,
  LeaderboardConfig as LeaderboardConfigContract,
  VotingPowerMultiplier,
} from '../../generated';
import {
  computeTotalPointsWithMultiplier,
  getOrCreateUser,
  getOrCreateUserEpochStats,
  applyScheduledEpochTransitions,
  recordProtocolTransaction,
  refreshUserVotingPowerState,
  shouldUseEthCalls,
  updateLifetimePoints,
} from './shared';
import { getTestnetBonusBps } from '../helpers/testnetTiers';
import { normalizeAddress } from '../helpers/constants';
import { readPoolFee } from '../helpers/viem';
import './lp';

async function getOrInitLeaderboardConfig(context: handlerContext, timestamp: number) {
  let config = await context.LeaderboardConfig.get('global');
  if (!config) {
    config = {
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
      lastUpdate: timestamp,
    };
    context.LeaderboardConfig.set(config);
  }
  return config;
}

// ============================================
// EpochManager Handlers
// ============================================

EpochManager.EpochStart.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const epochNumber = event.params.epochNumber;
  const epochId = epochNumber.toString();
  const scheduledStartTime = Number(event.params.startTime);
  const currentTimestamp = Number(event.block.timestamp);

  const existingEpoch = await context.LeaderboardEpoch.get(epochId);
  const shouldSetStartBlock = scheduledStartTime > 0 && scheduledStartTime <= currentTimestamp;
  const startBlock =
    existingEpoch?.startBlock && existingEpoch.startBlock > 0n
      ? existingEpoch.startBlock
      : shouldSetStartBlock
        ? BigInt(event.block.number)
        : 0n;

  context.LeaderboardEpoch.set({
    ...(existingEpoch ?? {
      id: epochId,
      epochNumber,
      startBlock,
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: false,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    }),
    scheduledStartTime,
    startBlock,
  });

  await applyScheduledEpochTransitions(context, currentTimestamp, BigInt(event.block.number));
});

EpochManager.EpochEnd.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const epochNumber = event.params.epochNumber;
  const epochId = epochNumber.toString();
  const scheduledEndTime = Number(event.params.endTime);
  const currentTimestamp = Number(event.block.timestamp);

  const existingEpoch = await context.LeaderboardEpoch.get(epochId);
  const shouldSetEndBlock = scheduledEndTime > 0 && scheduledEndTime <= currentTimestamp;
  const endBlock =
    existingEpoch?.endBlock && existingEpoch.endBlock > 0n
      ? existingEpoch.endBlock
      : shouldSetEndBlock
        ? BigInt(event.block.number)
        : undefined;

  context.LeaderboardEpoch.set({
    ...(existingEpoch ?? {
      id: epochId,
      epochNumber,
      startBlock: 0n,
      startTime: 0,
      endBlock,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    }),
    scheduledEndTime,
    endBlock,
  });

  await applyScheduledEpochTransitions(context, currentTimestamp, BigInt(event.block.number));
});

// ============================================
// LeaderboardConfig Handlers
// ============================================

LeaderboardConfigContract.ConfigSnapshot.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = event.params.timestamp.toString();

  // CRITICAL: Normalize daily bonuses by dividing by 1e18
  // Contract sends 10e18 to represent "10 points"
  const EIGHTEEN_DECIMALS = 1e18;

  context.LeaderboardConfigSnapshot.set({
    id,
    depositRateBps: event.params.depositRateBps,
    borrowRateBps: event.params.borrowRateBps,
    vpRateBps: event.params.vpRateBps,
    supplyDailyBonus: Number(event.params.supplyDailyBonus) / EIGHTEEN_DECIMALS,
    borrowDailyBonus: Number(event.params.borrowDailyBonus) / EIGHTEEN_DECIMALS,
    repayDailyBonus: Number(event.params.repayDailyBonus) / EIGHTEEN_DECIMALS,
    withdrawDailyBonus: Number(event.params.withdrawDailyBonus) / EIGHTEEN_DECIMALS,
    cooldownSeconds: Number(event.params.cooldownSeconds),
    minDailyBonusUsd: Number(event.params.minDailyBonusUsd),
    timestamp: Number(event.params.timestamp),
    txHash: event.transaction.hash,
  });

  const existingConfig = await context.LeaderboardConfig.get('global');
  context.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: event.params.depositRateBps,
    borrowRateBps: event.params.borrowRateBps,
    vpRateBps: event.params.vpRateBps,
    lpRateBps: existingConfig?.lpRateBps ?? 0n,
    supplyDailyBonus: Number(event.params.supplyDailyBonus) / EIGHTEEN_DECIMALS,
    borrowDailyBonus: Number(event.params.borrowDailyBonus) / EIGHTEEN_DECIMALS,
    repayDailyBonus: Number(event.params.repayDailyBonus) / EIGHTEEN_DECIMALS,
    withdrawDailyBonus: Number(event.params.withdrawDailyBonus) / EIGHTEEN_DECIMALS,
    cooldownSeconds: Number(event.params.cooldownSeconds),
    minDailyBonusUsd: Number(event.params.minDailyBonusUsd),
    lastUpdate: Number(event.params.timestamp),
  });
});

LeaderboardConfigContract.DepositRateUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.params.timestamp);
  const config = await getOrInitLeaderboardConfig(context, timestamp);

  context.LeaderboardConfig.set({
    ...config,
    depositRateBps: event.params.newRate,
    lastUpdate: timestamp,
  });
});

LeaderboardConfigContract.BorrowRateUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.params.timestamp);
  const config = await getOrInitLeaderboardConfig(context, timestamp);

  context.LeaderboardConfig.set({
    ...config,
    borrowRateBps: event.params.newRate,
    lastUpdate: timestamp,
  });
});

LeaderboardConfigContract.VpRateUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.params.timestamp);
  const config = await getOrInitLeaderboardConfig(context, timestamp);

  context.LeaderboardConfig.set({
    ...config,
    vpRateBps: event.params.newRate,
    lastUpdate: timestamp,
  });
});

LeaderboardConfigContract.DailyBonusUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.params.timestamp);
  const config = await getOrInitLeaderboardConfig(context, timestamp);
  const EIGHTEEN_DECIMALS = 1e18;

  context.LeaderboardConfig.set({
    ...config,
    supplyDailyBonus: Number(event.params.newSupplyBonus) / EIGHTEEN_DECIMALS,
    borrowDailyBonus: Number(event.params.newBorrowBonus) / EIGHTEEN_DECIMALS,
    repayDailyBonus: Number(event.params.newRepayBonus) / EIGHTEEN_DECIMALS,
    withdrawDailyBonus: Number(event.params.newWithdrawBonus) / EIGHTEEN_DECIMALS,
    lastUpdate: timestamp,
  });
});

LeaderboardConfigContract.CooldownUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.params.timestamp);
  const config = await getOrInitLeaderboardConfig(context, timestamp);

  context.LeaderboardConfig.set({
    ...config,
    cooldownSeconds: Number(event.params.newSeconds),
    lastUpdate: timestamp,
  });
});

LeaderboardConfigContract.MinDailyBonusUsdUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const timestamp = Number(event.params.timestamp);
  const config = await getOrInitLeaderboardConfig(context, timestamp);

  context.LeaderboardConfig.set({
    ...config,
    minDailyBonusUsd: Number(event.params.newMin),
    lastUpdate: timestamp,
  });
});

LeaderboardConfigContract.AddressBlacklisted.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const userId = normalizeAddress(event.params.account);
  const timestamp = Number(event.params.timestamp);

  context.LeaderboardBlacklist.set({
    id: userId,
    user_id: userId,
    isBlacklisted: true,
    lastUpdate: timestamp,
  });

  const { removeUserFromLeaderboards } = await import('../helpers/leaderboard');
  await removeUserFromLeaderboards(context, userId, timestamp);
});

LeaderboardConfigContract.AddressUnblacklisted.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const userId = normalizeAddress(event.params.account);
  const timestamp = Number(event.params.timestamp);

  context.LeaderboardBlacklist.set({
    id: userId,
    user_id: userId,
    isBlacklisted: false,
    lastUpdate: timestamp,
  });
});

LeaderboardConfigContract.PointsAwarded.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = `${event.transaction.hash}-${event.logIndex}`;
  const userId = normalizeAddress(event.params.user);

  await getOrCreateUser(context, userId);

  const state = await context.LeaderboardState.get('current');
  if (!state || state.currentEpochNumber === 0n) return;

  const epochNumber = state.currentEpochNumber;
  const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
  if (!epoch) return;
  // Use BigInt directly to avoid float precision loss
  const scaledPoints = event.params.points;
  const displayPoints = Number(scaledPoints) / 1e18;

  context.ManualPointsAward.set({
    id,
    user_id: userId,
    epochNumber,
    points: displayPoints,
    reason: event.params.reason,
    timestamp: Number(event.params.timestamp),
    txHash: event.transaction.hash,
  });

  // Update UserEpochStats
  const stats = await getOrCreateUserEpochStats(
    context,
    userId,
    epochNumber,
    Number(event.params.timestamp)
  );
  const newManualPoints = stats.manualAwardPoints + scaledPoints;
  const updatedStats = {
    ...stats,
    manualAwardPoints: newManualPoints,
    lastUpdatedAt: Number(event.params.timestamp),
  };
  const totalPoints =
    updatedStats.depositPoints +
    updatedStats.borrowPoints +
    updatedStats.lpPoints +
    updatedStats.dailySupplyPoints +
    updatedStats.dailyBorrowPoints +
    updatedStats.dailyRepayPoints +
    updatedStats.dailyWithdrawPoints +
    updatedStats.dailyVPPoints +
    updatedStats.dailyLPPoints +
    updatedStats.manualAwardPoints;
  const vpState = await refreshUserVotingPowerState(
    context,
    userId,
    Number(event.params.timestamp)
  );
  const totalPointsWithMultiplier = computeTotalPointsWithMultiplier(
    updatedStats,
    userId,
    epochNumber
  );

  const testnetBonusBps = epochNumber === 1n ? getTestnetBonusBps(userId) : 0n;
  context.UserEpochStats.set({
    ...updatedStats,
    totalPoints,
    totalPointsWithMultiplier,
    totalMultiplierBps: vpState.combinedMultiplierBps,
    lastAppliedMultiplierBps: vpState.combinedMultiplierBps,
    testnetBonusBps,
  });

  await updateLifetimePoints(context, userId, {
    epochNumber: stats.epochNumber,
    lastUpdatedAt: Number(event.params.timestamp),
  });

  const finalPoints = Number(totalPointsWithMultiplier) / 1e18;

  // Update leaderboard
  const { updateLeaderboard } = await import('../helpers/leaderboard');
  await updateLeaderboard(context, userId, finalPoints, Number(event.params.timestamp));
});

LeaderboardConfigContract.PointsRemoved.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = `${event.transaction.hash}-${event.logIndex}`;
  const userId = normalizeAddress(event.params.user);

  await getOrCreateUser(context, userId);

  const state = await context.LeaderboardState.get('current');
  if (!state || state.currentEpochNumber === 0n) return;

  const epochNumber = state.currentEpochNumber;
  const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
  if (!epoch) return;
  // Use BigInt directly to avoid float precision loss
  const scaledPoints = event.params.points;
  const displayPoints = Number(scaledPoints) / 1e18;

  context.ManualPointsAward.set({
    id,
    user_id: userId,
    epochNumber,
    points: -displayPoints,
    reason: event.params.reason,
    timestamp: Number(event.params.timestamp),
    txHash: event.transaction.hash,
  });

  // Update UserEpochStats
  const stats = await getOrCreateUserEpochStats(
    context,
    userId,
    epochNumber,
    Number(event.params.timestamp)
  );
  const newManualPoints = stats.manualAwardPoints - scaledPoints;
  const updatedStats = {
    ...stats,
    manualAwardPoints: newManualPoints,
    lastUpdatedAt: Number(event.params.timestamp),
  };
  const totalPoints =
    updatedStats.depositPoints +
    updatedStats.borrowPoints +
    updatedStats.lpPoints +
    updatedStats.dailySupplyPoints +
    updatedStats.dailyBorrowPoints +
    updatedStats.dailyRepayPoints +
    updatedStats.dailyWithdrawPoints +
    updatedStats.dailyVPPoints +
    updatedStats.dailyLPPoints +
    updatedStats.manualAwardPoints;
  const vpState = await refreshUserVotingPowerState(
    context,
    userId,
    Number(event.params.timestamp)
  );
  const totalPointsWithMultiplier = computeTotalPointsWithMultiplier(
    updatedStats,
    userId,
    epochNumber
  );

  const testnetBonusBps = epochNumber === 1n ? getTestnetBonusBps(userId) : 0n;
  context.UserEpochStats.set({
    ...updatedStats,
    totalPoints,
    totalPointsWithMultiplier,
    totalMultiplierBps: vpState.combinedMultiplierBps,
    lastAppliedMultiplierBps: vpState.combinedMultiplierBps,
    testnetBonusBps,
  });

  await updateLifetimePoints(context, userId, {
    epochNumber: stats.epochNumber,
    lastUpdatedAt: Number(event.params.timestamp),
  });

  const finalPoints = Number(totalPointsWithMultiplier) / 1e18;

  // Update leaderboard
  const { updateLeaderboard } = await import('../helpers/leaderboard');
  await updateLeaderboard(context, userId, finalPoints, Number(event.params.timestamp));
});

// ============================================
// VotingPowerMultiplier Handlers
// ============================================

VotingPowerMultiplier.TierAdded.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const tierId = event.params.tierIndex.toString();
  const timestamp = Number(event.block.timestamp);

  context.VotingPowerTier.set({
    id: tierId,
    tierIndex: event.params.tierIndex,
    minVotingPower: event.params.minVotingPower,
    multiplierBps: event.params.multiplierBps,
    createdAt: timestamp,
    lastUpdate: timestamp,
    isActive: true,
  });
});

VotingPowerMultiplier.TierUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const tierId = event.params.tierIndex.toString();
  const timestamp = Number(event.block.timestamp);

  const tier = await context.VotingPowerTier.get(tierId);
  if (tier) {
    context.VotingPowerTier.set({
      ...tier,
      minVotingPower: event.params.newMinVotingPower,
      multiplierBps: event.params.newMultiplierBps,
      lastUpdate: timestamp,
    });
  }
});

VotingPowerMultiplier.TierRemoved.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const tierId = event.params.tierIndex.toString();
  const timestamp = Number(event.block.timestamp);

  const tier = await context.VotingPowerTier.get(tierId);
  if (tier) {
    context.VotingPowerTier.set({
      ...tier,
      isActive: false,
      lastUpdate: timestamp,
    });
  }
});

// ============================================
// LP Pool Config Handlers
// ============================================

LeaderboardConfigContract.LPPoolConfigured.contractRegister(({ event, context }) => {
  context.addNonfungiblePositionManager(normalizeAddress(event.params.positionManager));
  context.addUniswapV3Pool(normalizeAddress(event.params.pool));
});

LeaderboardConfigContract.LPPoolConfigured.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const pool = normalizeAddress(event.params.pool);
  const positionManager = normalizeAddress(event.params.positionManager);
  const token0 = normalizeAddress(event.params.token0);
  const token1 = normalizeAddress(event.params.token1);
  const lpRateBps = event.params.lpRateBps;
  const timestamp = Number(event.params.timestamp);
  let fee: number | undefined;
  if (shouldUseEthCalls()) {
    const fetchedFee = await readPoolFee(pool, BigInt(event.block.number), context.log);
    if (fetchedFee !== null) {
      fee = fetchedFee;
    }
  }
  if (fee === undefined) {
    const existingConfig = await context.LPPoolConfig.get(pool);
    if (existingConfig?.fee !== undefined) {
      fee = existingConfig.fee;
    }
  }

  // Get current epoch
  const leaderboardState = await context.LeaderboardState.get('current');
  const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;

  // Create or update LP pool config
  context.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager,
    token0,
    token1,
    fee,
    lpRateBps,
    isActive: true,
    enabledAtEpoch: currentEpoch,
    enabledAtTimestamp: timestamp,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: timestamp,
  });

  const registry = await context.LPPoolRegistry.get('global');
  const existingPoolIds = registry?.poolIds ?? [];
  const poolIds = existingPoolIds.includes(pool) ? existingPoolIds : [...existingPoolIds, pool];
  if (!registry || poolIds.length !== existingPoolIds.length) {
    context.LPPoolRegistry.set({
      id: 'global',
      poolIds,
      lastUpdate: timestamp,
    });
  }

  // Initialize pool state
  context.LPPoolState.set({
    id: pool,
    pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    lastUpdate: timestamp,
  });

  // Update global LP rate in config
  const config = await getOrInitLeaderboardConfig(context, timestamp);
  context.LeaderboardConfig.set({
    ...config,
    lpRateBps,
    lastUpdate: timestamp,
  });
});

LeaderboardConfigContract.LPPoolDisabled.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const pool = normalizeAddress(event.params.pool);
  const timestamp = Number(event.params.timestamp);

  const poolConfig = await context.LPPoolConfig.get(pool);
  if (poolConfig) {
    const { settleLPPoolPositions } = await import('./lp');
    await settleLPPoolPositions(context, pool, timestamp);

    const leaderboardState = await context.LeaderboardState.get('current');
    const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;

    context.LPPoolConfig.set({
      ...poolConfig,
      isActive: false,
      disabledAtEpoch: currentEpoch,
      disabledAtTimestamp: timestamp,
      lastUpdate: timestamp,
    });
  }
});

LeaderboardConfigContract.LPRateUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const timestamp = Number(event.params.timestamp);
  const config = await getOrInitLeaderboardConfig(context, timestamp);

  context.LeaderboardConfig.set({
    ...config,
    lpRateBps: event.params.newRate,
    lastUpdate: timestamp,
  });
});
