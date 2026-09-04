/**
 * Leaderboard Event Handlers
 * EpochManager, LeaderboardConfig, VotingPowerMultiplier
 */

import {
  addTierToIndex,
  computeTotalPointsWithMultiplier,
  getOrCreateUser,
  getOrCreateUserEpochStats,
  applyScheduledEpochTransitions,
  recordProtocolTransaction,
  refreshUserVotingPowerState,
  removeTierFromIndex,
  touchTierIndex,
  updateLifetimePoints,
  writeLeaderboardConfig,
  writeLeaderboardEpoch,
  loadLPGrowthModule,
  loadLPModule,
} from './shared';
import { getTestnetBonusBps } from '../helpers/testnetTiers';
import { setUserEpochStats } from '../helpers/prefill';
import {
  normalizeAddress,
  EPOCH_1_START_TIME_OVERRIDE,
  BOOTSTRAP_CONFIG,
  BALANCER_AUTORANGE_V3_POOL_ADDRESS,
  getEpochDatesOverride,
} from '../helpers/constants';
import { removeUserFromLeaderboards, updateLeaderboard } from '../helpers/leaderboard';

import type { EvmOnEventContext as handlerContext } from 'envio';
import { indexer } from './registry';
async function getOrInitLeaderboardConfig(context: handlerContext, timestamp: number) {
  let config = await context.LeaderboardConfig.get('global');
  if (!config) {
    // Use bootstrap config if epoch 1 override is set and not disabled via env var
    const useBootstrap =
      EPOCH_1_START_TIME_OVERRIDE > 0 && process.env.ENVIO_DISABLE_BOOTSTRAP !== 'true';
    config = {
      id: 'global',
      depositRateBps: useBootstrap ? BOOTSTRAP_CONFIG.depositRateBps : 0n,
      borrowRateBps: useBootstrap ? BOOTSTRAP_CONFIG.borrowRateBps : 0n,
      vpRateBps: useBootstrap ? BOOTSTRAP_CONFIG.vpRateBps : 0n,
      lpRateBps: useBootstrap ? BOOTSTRAP_CONFIG.lpRateBps : 0n,
      supplyDailyBonus: useBootstrap ? BOOTSTRAP_CONFIG.supplyDailyBonus : 0,
      borrowDailyBonus: useBootstrap ? BOOTSTRAP_CONFIG.borrowDailyBonus : 0,
      repayDailyBonus: useBootstrap ? BOOTSTRAP_CONFIG.repayDailyBonus : 0,
      withdrawDailyBonus: useBootstrap ? BOOTSTRAP_CONFIG.withdrawDailyBonus : 0,
      cooldownSeconds: useBootstrap ? BOOTSTRAP_CONFIG.cooldownSeconds : 0,
      minDailyBonusUsd: useBootstrap ? BOOTSTRAP_CONFIG.minDailyBonusUsd : 0,
      lastUpdate: timestamp,
    };
    writeLeaderboardConfig(context, config);
  }
  return config;
}

// ============================================
// EpochManager Handlers
// ============================================

indexer.onEvent({ contract: 'EpochManager', event: 'EpochStart' }, async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const epochNumber = event.params.epochNumber;
  const epochId = epochNumber.toString();
  const currentTimestamp = Number(event.block.timestamp);

  // An epoch listed in EPOCH_DATES_OVERRIDES takes its dates from that table:
  // the on-chain payload is ignored, which is how epoch 1 (no events at all) and
  // epoch 9 (scheduled with the wrong start) both get the right dates.
  const override = getEpochDatesOverride(epochNumber);
  const scheduledStartTime = override ? override.startTime : Number(event.params.startTime);

  const existingEpoch = await context.LeaderboardEpoch.get(epochId);
  const shouldSetStartBlock = scheduledStartTime > 0 && scheduledStartTime <= currentTimestamp;
  const startBlock =
    existingEpoch?.startBlock && existingEpoch.startBlock > 0n
      ? existingEpoch.startBlock
      : shouldSetStartBlock
        ? BigInt(event.block.number)
        : 0n;

  // Never overwrite scheduledStartTime for an active epoch
  // This is a defensive measure - altering start time of an active epoch would corrupt point calculations
  // Note: this also protects bootstrap's start time since bootstrap sets isActive: true
  // An overridden epoch is exempt: its dates are owned by the table, active or
  // not. For epoch 1 that is a no-op - bootstrap seeds the same timestamp.
  const finalScheduledStartTime =
    !override && existingEpoch?.isActive ? existingEpoch.scheduledStartTime : scheduledStartTime;

  const base = existingEpoch ?? {
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
  };

  writeLeaderboardEpoch(context, {
    ...base,
    scheduledStartTime: finalScheduledStartTime,
    // Seed the overridden end here too - the on-chain EpochEnd for an overridden
    // epoch can arrive long after the tide is meant to be over, or not at all.
    scheduledEndTime: override ? override.endTime : base.scheduledEndTime,
    startBlock,
  });

  await applyScheduledEpochTransitions(context, currentTimestamp, BigInt(event.block.number));
});

indexer.onEvent({ contract: 'EpochManager', event: 'EpochEnd' }, async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const epochNumber = event.params.epochNumber;
  const epochId = epochNumber.toString();

  // An overridden epoch keeps the end from the table. Ending it on-chain is the
  // only way to start the next one, and that end must not move this tide.
  const override = getEpochDatesOverride(epochNumber);
  const scheduledEndTime = override ? override.endTime : Number(event.params.endTime);
  const currentTimestamp = Number(event.block.timestamp);

  const existingEpoch = await context.LeaderboardEpoch.get(epochId);
  const shouldSetEndBlock = scheduledEndTime > 0 && scheduledEndTime <= currentTimestamp;
  const endBlock =
    existingEpoch?.endBlock && existingEpoch.endBlock > 0n
      ? existingEpoch.endBlock
      : shouldSetEndBlock
        ? BigInt(event.block.number)
        : undefined;

  const base = existingEpoch ?? {
    id: epochId,
    epochNumber,
    startBlock: 0n,
    // Seeded from the override so a fallback-created epoch closes coherently:
    // applyScheduledEpochTransitions only computes duration when startTime > 0,
    // so leaving it at 0 here would store an epoch with an end and no duration.
    startTime: override ? override.startTime : 0,
    endBlock,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  };

  writeLeaderboardEpoch(context, {
    ...base,
    scheduledStartTime: override ? override.startTime : base.scheduledStartTime,
    scheduledEndTime,
    endBlock,
  });

  await applyScheduledEpochTransitions(context, currentTimestamp, BigInt(event.block.number));
});

// ============================================
// LeaderboardConfig Handlers
// ============================================

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'ConfigSnapshot' },
  async ({ event, context }) => {
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
    writeLeaderboardConfig(context, {
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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'DepositRateUpdated' },
  async ({ event, context }) => {
    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );
    const timestamp = Number(event.params.timestamp);
    const config = await getOrInitLeaderboardConfig(context, timestamp);

    writeLeaderboardConfig(context, {
      ...config,
      depositRateBps: event.params.newRate,
      lastUpdate: timestamp,
    });
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'BorrowRateUpdated' },
  async ({ event, context }) => {
    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );
    const timestamp = Number(event.params.timestamp);
    const config = await getOrInitLeaderboardConfig(context, timestamp);

    writeLeaderboardConfig(context, {
      ...config,
      borrowRateBps: event.params.newRate,
      lastUpdate: timestamp,
    });
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'VpRateUpdated' },
  async ({ event, context }) => {
    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );
    const timestamp = Number(event.params.timestamp);
    const config = await getOrInitLeaderboardConfig(context, timestamp);

    writeLeaderboardConfig(context, {
      ...config,
      vpRateBps: event.params.newRate,
      lastUpdate: timestamp,
    });
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'DailyBonusUpdated' },
  async ({ event, context }) => {
    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );
    const timestamp = Number(event.params.timestamp);
    const config = await getOrInitLeaderboardConfig(context, timestamp);
    const EIGHTEEN_DECIMALS = 1e18;

    writeLeaderboardConfig(context, {
      ...config,
      supplyDailyBonus: Number(event.params.newSupplyBonus) / EIGHTEEN_DECIMALS,
      borrowDailyBonus: Number(event.params.newBorrowBonus) / EIGHTEEN_DECIMALS,
      repayDailyBonus: Number(event.params.newRepayBonus) / EIGHTEEN_DECIMALS,
      withdrawDailyBonus: Number(event.params.newWithdrawBonus) / EIGHTEEN_DECIMALS,
      lastUpdate: timestamp,
    });
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'CooldownUpdated' },
  async ({ event, context }) => {
    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );
    const timestamp = Number(event.params.timestamp);
    const config = await getOrInitLeaderboardConfig(context, timestamp);

    writeLeaderboardConfig(context, {
      ...config,
      cooldownSeconds: Number(event.params.newSeconds),
      lastUpdate: timestamp,
    });
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'MinDailyBonusUsdUpdated' },
  async ({ event, context }) => {
    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );
    const timestamp = Number(event.params.timestamp);
    const config = await getOrInitLeaderboardConfig(context, timestamp);

    writeLeaderboardConfig(context, {
      ...config,
      minDailyBonusUsd: Number(event.params.newMin),
      lastUpdate: timestamp,
    });
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'AddressBlacklisted' },
  async ({ event, context }) => {
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

    await removeUserFromLeaderboards(context, userId, timestamp);
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'AddressUnblacklisted' },
  async ({ event, context }) => {
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
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'PointsAwarded' },
  async ({ event, context }) => {
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
    setUserEpochStats(context, {
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
    await updateLeaderboard(context, userId, finalPoints, Number(event.params.timestamp));
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'PointsRemoved' },
  async ({ event, context }) => {
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
    setUserEpochStats(context, {
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
    await updateLeaderboard(context, userId, finalPoints, Number(event.params.timestamp));
  }
);

// ============================================
// VotingPowerMultiplier Handlers
// ============================================

indexer.onEvent(
  { contract: 'VotingPowerMultiplier', event: 'TierAdded' },
  async ({ event, context }) => {
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
    await addTierToIndex(context, tierId, timestamp);
  }
);

indexer.onEvent(
  { contract: 'VotingPowerMultiplier', event: 'TierUpdated' },
  async ({ event, context }) => {
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
      await touchTierIndex(context, timestamp);
    }
  }
);

indexer.onEvent(
  { contract: 'VotingPowerMultiplier', event: 'TierRemoved' },
  async ({ event, context }) => {
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
      await removeTierFromIndex(context, tierId, timestamp);
    }
  }
);

// ============================================
// LP Pool Config Handlers
// ============================================

indexer.contractRegister(
  { contract: 'LeaderboardConfig', event: 'LPPoolConfigured' },
  async ({ event, context }) => {
    const pool = normalizeAddress(event.params.pool);
    const positionManager = normalizeAddress(event.params.positionManager);
    if (pool === normalizeAddress(BALANCER_AUTORANGE_V3_POOL_ADDRESS)) {
      context.chain.BalancerAutoRangePool.add(pool);
      return;
    }
    if (pool === positionManager) {
      context.chain.UniswapV2Pair.add(pool);
      return;
    }
    context.chain.NonfungiblePositionManager.add(positionManager);
    context.chain.UniswapV3Pool.add(pool);
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'LPPoolConfigured' },
  async ({ event, context }) => {
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
    const existingConfig = await context.LPPoolConfig.get(pool);
    if (existingConfig?.fee !== undefined) {
      fee = existingConfig.fee;
    }

    // Get current epoch
    const leaderboardState = await context.LeaderboardState.get('current');
    const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;
    // Lazy load breaks the leaderboard <-> lp cycle; the loader is memoized in shared.ts.
    const { getStaticLPPoolEraState } = await loadLPModule();
    const staticEra = getStaticLPPoolEraState(
      pool,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );

    // Create or update LP pool config
    context.LPPoolConfig.set({
      id: pool,
      pool,
      positionManager,
      token0,
      token1,
      fee,
      lpRateBps,
      isActive: staticEra?.isActive ?? true,
      enabledAtEpoch: currentEpoch,
      enabledAtTimestamp: staticEra?.enabledAtTimestamp ?? timestamp,
      disabledAtEpoch: staticEra?.disabledAtTimestamp === undefined ? undefined : currentEpoch,
      disabledAtTimestamp: staticEra?.disabledAtTimestamp,
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
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: timestamp,
    });

    // The global LP rate is only a bootstrap/default for newly discovered
    // pools. Once a pool exists, its rate is owned by LPPoolConfig.
    await getOrInitLeaderboardConfig(context, timestamp);
  }
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'LPPoolDisabled' },
  async ({ event, context }) => {
    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );

    const pool = normalizeAddress(event.params.pool);
    const timestamp = Number(event.params.timestamp);

    const poolConfig = await context.LPPoolConfig.get(pool);
    if (!poolConfig?.isActive) return;

    const { advanceLPPoolGrowth } = await loadLPGrowthModule();
    await advanceLPPoolGrowth(context, pool, timestamp);

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
);

indexer.onEvent(
  { contract: 'LeaderboardConfig', event: 'LPRateUpdated' },
  async ({ event, context }) => {
    await recordProtocolTransaction(
      context,
      event.transaction.hash,
      Number(event.block.timestamp),
      BigInt(event.block.number)
    );

    const pool = normalizeAddress(event.params.pool);
    const timestamp = Number(event.params.timestamp);
    const poolConfig = await context.LPPoolConfig.get(pool);
    if (!poolConfig) return;

    // Preserve the old rate for the entire interval preceding this event. The
    // contract only emits LPRateUpdated for a configured active pool, while the
    // indexer's local isActive flag can also represent an off-chain points era.
    // Inactive local pools keep their next rate without accruing paused points.
    if (poolConfig.isActive) {
      const { advanceLPPoolGrowth } = await loadLPGrowthModule();
      await advanceLPPoolGrowth(context, pool, timestamp);
    }

    context.LPPoolConfig.set({
      ...poolConfig,
      lpRateBps: event.params.newRate,
      lastUpdate: timestamp,
    });
  }
);
