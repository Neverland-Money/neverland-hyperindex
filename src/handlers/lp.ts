/**
 * LP (Liquidity Provider) Event Handlers
 * Uniswap V3 style concentrated liquidity tracking
 *
 * Points accrue per USD of in-range liquidity per hour, same pattern as supply/borrow.
 * Key difference: LP positions only earn points when the current tick is within their range.
 *
 * Settlement flow:
 * 1. Track position state (liquidity, amounts, tick range)
 * 2. On each update, settle accumulated in-range time
 * 3. Calculate points: (valueUsd / 1e8) * lpRatePerHour * inRangeHours
 */

import type { handlerContext } from '../../generated';
import type { LPPoolConfig_t } from '../../generated/src/db/Entities.gen';
import { NonfungiblePositionManager, UniswapV3Pool } from '../../generated';
import {
  applyCombinedMultiplierScaled,
  calculateAverageCombinedMultiplierBps,
  computeTotalPointsWithMultiplier,
  getOrCreateUser,
  getOrCreateUserEpochStats,
  recordProtocolTransaction,
  refreshUserVotingPowerState,
  shouldUseEthCalls,
  updateLifetimePoints,
} from './shared';
import {
  AUSD_ADDRESS,
  BASIS_POINTS,
  normalizeAddress,
  POINTS_SCALE,
  SECONDS_PER_DAY,
} from '../helpers/constants';
import { getTestnetBonusBps } from '../helpers/testnetTiers';
import {
  readLPBalance,
  readLPPosition,
  readLPTokenOfOwnerByIndex,
  readTokenDecimals,
  readPoolFee,
  readPoolSlot0,
} from '../helpers/viem';
import { getAmountsForLiquidity } from '../helpers/uniswapV3';

const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
const DUST_DECIMALS = 18;
const AUSD_DECIMALS_FALLBACK = 6;
const FEE_UNITS_DENOMINATOR = 1_000_000n;
const VOLUME_BUCKET_SECONDS = 3600;
const VOLUME_WINDOW_HOURS = 24;
const DAYS_PER_YEAR = 365n;
const HARDCODED_LP_POOL = normalizeAddress('0xd15965968fe8bf2babbe39b2fc5de1ab6749141f');
const HARDCODED_LP_POSITION_MANAGER = normalizeAddress(
  '0x7197e214c0b767cfb76fb734ab638e2c192f4e53'
);
const HARDCODED_LP_TOKEN0 = AUSD_ADDRESS;
const HARDCODED_LP_TOKEN1 = normalizeAddress('0xad96c3dffcd6374294e2573a7fbba96097cc8d7c');
const HARDCODED_LP_FEE = 10000;

function logLpDebug(context: handlerContext, message: string) {
  if (process.env.DEBUG_LP_POINTS === 'true') {
    context.log?.debug?.(message);
  }
}

function absBigInt(value: bigint): bigint {
  return value < 0n ? -value : value;
}

function getVolumeBucketStart(timestamp: number): number {
  return Math.floor(timestamp / VOLUME_BUCKET_SECONDS) * VOLUME_BUCKET_SECONDS;
}

// ============================================
//     Helper Functions
// ============================================

async function getActiveLPPoolConfig(context: handlerContext, pool: string) {
  const config = await context.LPPoolConfig.get(normalizeAddress(pool));
  if (!config || !config.isActive) return null;
  return config;
}

type LPPoolRegistryRecord = {
  poolIds?: string[];
};

type LPPoolConfigRecord = LPPoolConfig_t;

type LPPoolStatsRecord = {
  id: string;
  pool: string;
  totalPositions: number;
  inRangePositions: number;
  totalValueUsd: bigint;
  inRangeValueUsd: bigint;
  lastUpdate: number;
};

type LPPoolVolumeBucketRecord = {
  id: string;
  pool: string;
  bucketStart: number;
  volumeUsd: bigint;
  lastUpdate: number;
};

type LPPoolFeeStatsRecord = {
  id: string;
  pool: string;
  volumeUsd24h: bigint;
  feesUsd24h: bigint;
  feeAprBps: bigint;
  lastUpdate: number;
};

// For multi-pool scenarios, we track pools via LPPoolRegistry entity.
async function listActiveLPPoolConfigs(context: handlerContext) {
  // Get the registry of tracked pools
  const registryStore = (
    context as unknown as {
      LPPoolRegistry?: { get: (id: string) => Promise<LPPoolRegistryRecord | undefined> };
    }
  ).LPPoolRegistry;
  const poolConfigStore = (
    context as unknown as {
      LPPoolConfig?: { get: (id: string) => Promise<LPPoolConfigRecord | undefined> };
    }
  ).LPPoolConfig;
  if (!registryStore || !poolConfigStore) {
    return [];
  }

  const registry = await registryStore.get('global');
  if (!registry || !registry.poolIds || registry.poolIds.length === 0) {
    return [];
  }

  const configs = await Promise.all(registry.poolIds.map((id: string) => poolConfigStore.get(id)));
  return configs.filter((c): c is Exclude<typeof c, undefined> => c !== undefined && c.isActive);
}

async function getSingleActiveLPPoolConfig(context: handlerContext) {
  const configs = await listActiveLPPoolConfigs(context);
  return configs.length === 1 ? configs[0] : null;
}

async function ensureHardcodedPoolConfig(context: handlerContext, timestamp: number) {
  let config = await context.LPPoolConfig.get(HARDCODED_LP_POOL);
  if (config) return config;

  const leaderboardState = await context.LeaderboardState.get('current');
  const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;
  const globalConfig = await context.LeaderboardConfig.get('global');
  const lpRateBps = globalConfig?.lpRateBps ?? 0n;

  config = {
    id: HARDCODED_LP_POOL,
    pool: HARDCODED_LP_POOL,
    positionManager: HARDCODED_LP_POSITION_MANAGER,
    token0: HARDCODED_LP_TOKEN0,
    token1: HARDCODED_LP_TOKEN1,
    fee: HARDCODED_LP_FEE,
    lpRateBps,
    isActive: true,
    enabledAtEpoch: currentEpoch,
    enabledAtTimestamp: timestamp,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: timestamp,
  };
  context.LPPoolConfig.set(config);

  const registry = await context.LPPoolRegistry.get('global');
  const existingPoolIds = registry?.poolIds ?? [];
  if (!existingPoolIds.includes(HARDCODED_LP_POOL)) {
    context.LPPoolRegistry.set({
      id: 'global',
      poolIds: [...existingPoolIds, HARDCODED_LP_POOL],
      lastUpdate: timestamp,
    });
  }

  const existingState = await context.LPPoolState.get(HARDCODED_LP_POOL);
  if (!existingState) {
    context.LPPoolState.set({
      id: HARDCODED_LP_POOL,
      pool: HARDCODED_LP_POOL,
      currentTick: 0,
      sqrtPriceX96: 0n,
      token0Price: 0n,
      token1Price: 0n,
      lastUpdate: timestamp,
    });
  }

  return config;
}

async function getEffectiveLPPoolConfig(context: handlerContext, pool: string) {
  const poolId = normalizeAddress(pool);
  const config = await context.LPPoolConfig.get(poolId);
  if (config) {
    return config.isActive ? config : null;
  }
  return await getSingleActiveLPPoolConfig(context);
}

async function ensurePoolFee(
  context: handlerContext,
  poolConfig: LPPoolConfig_t,
  timestamp: number,
  blockNumber?: bigint
): Promise<number | null> {
  if (poolConfig.fee !== undefined) {
    return poolConfig.fee;
  }
  if (!shouldUseEthCalls()) return null;

  const fetchedFee = await readPoolFee(poolConfig.pool, blockNumber, context.log);
  if (fetchedFee === null) return null;

  context.LPPoolConfig.set({
    ...poolConfig,
    fee: fetchedFee,
    lastUpdate: timestamp,
  });
  return fetchedFee;
}

async function resolvePoolConfigForPosition(
  context: handlerContext,
  positionManager: string,
  token0: string,
  token1: string,
  positionFee: number,
  timestamp: number,
  blockNumber?: bigint
) {
  const manager = normalizeAddress(positionManager);
  const token0Lower = normalizeAddress(token0);
  const token1Lower = normalizeAddress(token1);
  const configs = await listActiveLPPoolConfigs(context);
  if (configs.length === 0) return null;

  const matching = configs.filter(
    config =>
      config.positionManager === manager &&
      config.token0 === token0Lower &&
      config.token1 === token1Lower
  );

  if (matching.length === 1) {
    const configFee = await ensurePoolFee(context, matching[0], timestamp, blockNumber);
    if (configFee === null || configFee === positionFee) {
      return matching[0];
    }
    return null;
  }

  if (matching.length > 1) {
    for (const config of matching) {
      const configFee = await ensurePoolFee(context, config, timestamp, blockNumber);
      if (configFee !== null && configFee === positionFee) {
        return config;
      }
    }
  }

  return null;
}

export async function syncUserLPPositionsFromChain(
  context: handlerContext,
  userId: string,
  timestamp: number,
  blockNumber?: bigint,
  options?: { forceRescan?: boolean; managers?: string[] }
): Promise<void> {
  if (!shouldUseEthCalls() || process.env.ENVIO_ENABLE_LP_CHAIN_SYNC !== 'true') {
    return;
  }

  const normalizedUserId = normalizeAddress(userId);
  const configs = await listActiveLPPoolConfigs(context);
  if (configs.length === 0) return;

  let uniqueManagers = Array.from(
    new Set(configs.map(config => normalizeAddress(config.positionManager)))
  );
  if (options?.managers && options.managers.length > 0) {
    const allowedManagers = new Set(options.managers.map(manager => normalizeAddress(manager)));
    uniqueManagers = uniqueManagers.filter(manager => allowedManagers.has(manager));
    if (uniqueManagers.length === 0) return;
  }
  let createdAny = false;
  const touchedPools = new Set<string>();

  for (const manager of uniqueManagers) {
    const baselineId = `${normalizedUserId}:${manager}`;
    const baseline = await context.UserLPBaseline.get(baselineId);
    if (baseline && !options?.forceRescan) continue;
    if (baseline && options?.forceRescan) {
      logLpDebug(
        context,
        `[lp] chain sync force rescan user=${normalizedUserId} manager=${manager}`
      );
    }

    const balance = await readLPBalance(manager, normalizedUserId, blockNumber, context.log);
    if (balance === null) {
      logLpDebug(
        context,
        `[lp] chain sync skipped manager=${manager} user=${normalizedUserId} reason=balance_unavailable`
      );
      continue;
    }

    const tokenIds: bigint[] = [];
    let readFailed = false;

    for (let index = 0n; index < balance; index += 1n) {
      const tokenId = await readLPTokenOfOwnerByIndex(
        manager,
        normalizedUserId,
        index,
        blockNumber,
        context.log
      );
      if (tokenId === null) {
        readFailed = true;
        logLpDebug(
          context,
          `[lp] chain sync skipped manager=${manager} user=${normalizedUserId} reason=token_unavailable`
        );
        break;
      }
      tokenIds.push(tokenId);
    }

    if (readFailed) continue;

    for (const tokenId of tokenIds) {
      const positionId = tokenId.toString();
      const existing = await context.UserLPPosition.get(positionId);
      if (existing) {
        await addPositionToUserIndex(context, normalizedUserId, positionId, timestamp);
        await addPositionToPoolIndex(context, existing.pool, positionId, timestamp);
        continue;
      }

      const positionData = await readLPPosition(manager, tokenId, blockNumber, context.log);
      if (!positionData) {
        readFailed = true;
        break;
      }

      const poolConfig = await resolvePoolConfigForPosition(
        context,
        manager,
        positionData.token0,
        positionData.token1,
        positionData.fee,
        timestamp,
        blockNumber
      );
      if (!poolConfig) continue;

      const poolState = await seedPoolStateFromChain(
        context,
        poolConfig.pool,
        timestamp,
        blockNumber
      );
      if (poolState.sqrtPriceX96 === 0n) {
        readFailed = true;
        logLpDebug(
          context,
          `[lp] chain sync skipped tokenId=${positionId} pool=${poolConfig.pool} reason=slot0_unavailable`
        );
        break;
      }

      const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
        context,
        poolConfig,
        timestamp
      );
      let token0Price = poolState.token0Price;
      let token1Price = poolState.token1Price;
      const isAusdToken0 = poolConfig.token0 === AUSD_ADDRESS;
      const isAusdToken1 = poolConfig.token1 === AUSD_ADDRESS;
      if (isAusdToken0 || isAusdToken1) {
        const ausdPrice = getAusdPrice();
        const pairedTokenPrice = calculateDustPriceFromPool(
          poolState.sqrtPriceX96,
          ausdPrice,
          isAusdToken0,
          token0Decimals,
          token1Decimals
        );
        token0Price = isAusdToken0 ? ausdPrice : pairedTokenPrice;
        token1Price = isAusdToken0 ? pairedTokenPrice : ausdPrice;
      }

      const { amount0, amount1 } = getAmountsForLiquidity(
        poolState.sqrtPriceX96,
        positionData.tickLower,
        positionData.tickUpper,
        positionData.liquidity
      );
      const valueUsd = calculatePositionValueUsd(
        amount0,
        amount1,
        token0Price,
        token1Price,
        token0Decimals,
        token1Decimals
      );
      const isInRange = isPositionInRange(
        positionData.tickLower,
        positionData.tickUpper,
        poolState.currentTick
      );

      context.UserLPPosition.set({
        id: positionId,
        tokenId,
        user_id: normalizedUserId,
        pool: poolConfig.pool,
        positionManager: manager,
        tickLower: positionData.tickLower,
        tickUpper: positionData.tickUpper,
        liquidity: positionData.liquidity,
        amount0,
        amount1,
        isInRange,
        valueUsd,
        lastInRangeTimestamp: isInRange ? timestamp : 0,
        accumulatedInRangeSeconds: 0n,
        lastSettledAt: timestamp,
        settledLpPoints: 0n,
        createdAt: timestamp,
        lastUpdate: timestamp,
      });

      await addPositionToPoolIndex(context, poolConfig.pool, positionId, timestamp);
      await addPositionToUserIndex(context, normalizedUserId, positionId, timestamp);
      createdAny = true;
      touchedPools.add(poolConfig.pool);

      if (poolState.token0Price !== token0Price || poolState.token1Price !== token1Price) {
        context.LPPoolState.set({
          ...poolState,
          token0Price,
          token1Price,
          lastUpdate: timestamp,
        });
      }
    }

    if (readFailed) continue;

    context.UserLPBaseline.set({
      id: baselineId,
      user_id: normalizedUserId,
      positionManager: manager,
      checkedAt: timestamp,
      checkedBlock: blockNumber ?? 0n,
    });
  }

  if (createdAny) {
    await updateUserLPStats(context, normalizedUserId, timestamp);
  }
  if (touchedPools.size > 0) {
    for (const poolId of touchedPools) {
      await updatePoolLPStats(context, poolId, timestamp);
    }
  }
}

function buildTxMintKey(txHash: string, amount0: bigint, amount1: bigint, liquidity: bigint) {
  return `tx:${txHash}:${amount0.toString()}:${amount1.toString()}:${liquidity.toString()}`;
}

async function getOrCreateLPPoolState(context: handlerContext, pool: string, timestamp: number) {
  const id = normalizeAddress(pool);
  let state = await context.LPPoolState.get(id);
  if (!state) {
    state = {
      id,
      pool: normalizeAddress(pool),
      currentTick: 0,
      sqrtPriceX96: 0n,
      token0Price: 0n,
      token1Price: 0n,
      lastUpdate: timestamp,
    };
    context.LPPoolState.set(state);
  }
  return state;
}

function setPoolStats(
  context: handlerContext,
  poolId: string,
  totalPositions: number,
  inRangePositions: number,
  totalValueUsd: bigint,
  inRangeValueUsd: bigint,
  timestamp: number
) {
  const poolStatsStore = (
    context as unknown as {
      LPPoolStats?: { set: (value: LPPoolStatsRecord) => void };
    }
  ).LPPoolStats;
  if (!poolStatsStore) {
    return;
  }
  poolStatsStore.set({
    id: poolId,
    pool: poolId,
    totalPositions,
    inRangePositions,
    totalValueUsd,
    inRangeValueUsd,
    lastUpdate: timestamp,
  });
}

async function getOrCreateLPPoolStats(context: handlerContext, pool: string, timestamp: number) {
  const poolStatsStore = (
    context as unknown as {
      LPPoolStats?: {
        get: (id: string) => Promise<LPPoolStatsRecord | undefined>;
        set: (value: LPPoolStatsRecord) => void;
      };
    }
  ).LPPoolStats;
  if (!poolStatsStore) {
    return null;
  }
  const poolId = normalizeAddress(pool);
  let stats = await poolStatsStore.get(poolId);
  if (!stats) {
    stats = {
      id: poolId,
      pool: poolId,
      totalPositions: 0,
      inRangePositions: 0,
      totalValueUsd: 0n,
      inRangeValueUsd: 0n,
      lastUpdate: timestamp,
    };
    poolStatsStore.set(stats);
  }
  return stats;
}

async function updatePoolLPStats(context: handlerContext, pool: string, timestamp: number) {
  const poolStatsStore = (
    context as unknown as {
      LPPoolStats?: { set: (value: LPPoolStatsRecord) => void };
    }
  ).LPPoolStats;
  if (!poolStatsStore) {
    return;
  }
  const poolId = normalizeAddress(pool);
  const positions = await listPoolLPPositions(context, poolId);
  if (positions.length === 0) {
    const stats = await getOrCreateLPPoolStats(context, poolId, timestamp);
    if (stats) {
      setPoolStats(context, poolId, 0, 0, 0n, 0n, Math.max(stats.lastUpdate, timestamp));
    }
    return;
  }

  let totalPositions = 0;
  let inRangePositions = 0;
  let totalValueUsd = 0n;
  let inRangeValueUsd = 0n;

  for (const position of positions) {
    if (position.liquidity === 0n && position.amount0 === 0n && position.amount1 === 0n) {
      continue;
    }
    totalPositions += 1;
    totalValueUsd += position.valueUsd;
    if (position.isInRange) {
      inRangePositions += 1;
      inRangeValueUsd += position.valueUsd;
    }
  }

  setPoolStats(
    context,
    poolId,
    totalPositions,
    inRangePositions,
    totalValueUsd,
    inRangeValueUsd,
    timestamp
  );
}

function calculateSwapVolumeUsd(
  amount0: bigint,
  amount1: bigint,
  token0PriceUsd: bigint,
  token1PriceUsd: bigint,
  token0Decimals: number,
  token1Decimals: number
): bigint {
  const scale0 = 10n ** BigInt(token0Decimals);
  const scale1 = 10n ** BigInt(token1Decimals);
  const value0 = (absBigInt(amount0) * token0PriceUsd) / scale0;
  const value1 = (absBigInt(amount1) * token1PriceUsd) / scale1;
  return value0 > value1 ? value0 : value1;
}

async function updatePoolFeeStats(
  context: handlerContext,
  poolConfig: LPPoolConfig_t,
  volumeUsd: bigint,
  timestamp: number,
  blockNumber?: bigint
) {
  if (volumeUsd === 0n) return;

  const bucketStore = (
    context as unknown as {
      LPPoolVolumeBucket?: {
        get: (id: string) => Promise<LPPoolVolumeBucketRecord | undefined>;
        set: (value: LPPoolVolumeBucketRecord) => void;
      };
    }
  ).LPPoolVolumeBucket;
  const poolStatsStore = (
    context as unknown as {
      LPPoolStats?: { get: (id: string) => Promise<LPPoolStatsRecord | undefined> };
    }
  ).LPPoolStats;
  const feeStatsStore = (
    context as unknown as {
      LPPoolFeeStats?: { set: (value: LPPoolFeeStatsRecord) => void };
    }
  ).LPPoolFeeStats;
  if (!bucketStore || !feeStatsStore) {
    return;
  }

  const poolId = normalizeAddress(poolConfig.pool);
  const bucketStart = getVolumeBucketStart(timestamp);
  const bucketId = `${poolId}:${bucketStart}`;
  const bucket = await bucketStore.get(bucketId);
  const nextBucketVolume = (bucket?.volumeUsd ?? 0n) + volumeUsd;

  bucketStore.set({
    id: bucketId,
    pool: poolId,
    bucketStart,
    volumeUsd: nextBucketVolume,
    lastUpdate: timestamp,
  });

  let volumeUsd24h = 0n;
  for (let i = 0; i < VOLUME_WINDOW_HOURS; i += 1) {
    const start = bucketStart - i * VOLUME_BUCKET_SECONDS;
    if (start < 0) break;
    if (start === bucketStart) {
      volumeUsd24h += nextBucketVolume;
      continue;
    }
    const windowBucket = await bucketStore.get(`${poolId}:${start}`);
    if (windowBucket) {
      volumeUsd24h += windowBucket.volumeUsd;
    }
  }

  const poolFee =
    (await ensurePoolFee(context, poolConfig, timestamp, blockNumber)) ?? poolConfig.fee ?? 0;
  const feesUsd24h = poolFee > 0 ? (volumeUsd24h * BigInt(poolFee)) / FEE_UNITS_DENOMINATOR : 0n;
  const poolStats = await poolStatsStore?.get?.(poolId);
  const tvlUsd = poolStats?.inRangeValueUsd ?? 0n;
  const feeAprBps =
    feesUsd24h > 0n && tvlUsd > 0n ? (feesUsd24h * DAYS_PER_YEAR * BASIS_POINTS) / tvlUsd : 0n;

  feeStatsStore.set({
    id: poolId,
    pool: poolId,
    volumeUsd24h,
    feesUsd24h,
    feeAprBps,
    lastUpdate: timestamp,
  });
}

async function getOrCreateUserLPStats(context: handlerContext, userId: string, timestamp: number) {
  const normalizedUserId = normalizeAddress(userId);
  let stats = await context.UserLPStats.get(normalizedUserId);
  if (!stats) {
    stats = {
      id: normalizedUserId,
      user_id: normalizedUserId,
      totalPositions: 0,
      inRangePositions: 0,
      totalValueUsd: 0n,
      inRangeValueUsd: 0n,
      lastUpdate: timestamp,
    };
    context.UserLPStats.set(stats);
  }
  return stats;
}

function isPositionInRange(tickLower: number, tickUpper: number, currentTick: number): boolean {
  return tickLower <= currentTick && currentTick < tickUpper;
}

async function getTokenDecimals(
  context: handlerContext,
  tokenAddress: string,
  fallbackDecimals: number,
  timestamp?: number
): Promise<number> {
  const tokenId = normalizeAddress(tokenAddress);
  const tokenInfo = await context.TokenInfo.get(tokenId);
  if (tokenInfo?.decimals !== undefined && tokenInfo.decimals > 0) {
    return tokenInfo.decimals;
  }

  if (shouldUseEthCalls()) {
    try {
      const decimals = await readTokenDecimals(tokenId);
      if (decimals > 0) {
        context.TokenInfo.set({
          id: tokenId,
          address: tokenId,
          decimals,
          symbol: tokenInfo?.symbol,
          name: tokenInfo?.name,
          lastUpdate: timestamp ?? 0,
        });
        return decimals;
      }
    } catch {
      // fall back to default
    }
  }

  return tokenInfo?.decimals ?? fallbackDecimals;
}

async function getPoolTokenDecimals(
  context: handlerContext,
  poolConfig: { token0: string; token1: string },
  timestamp?: number
): Promise<{ token0Decimals: number; token1Decimals: number }> {
  const token0Fallback =
    poolConfig.token0 === AUSD_ADDRESS ? AUSD_DECIMALS_FALLBACK : DUST_DECIMALS;
  const token1Fallback =
    poolConfig.token1 === AUSD_ADDRESS ? AUSD_DECIMALS_FALLBACK : DUST_DECIMALS;
  const token0Decimals = await getTokenDecimals(
    context,
    poolConfig.token0,
    token0Fallback,
    timestamp
  );
  const token1Decimals = await getTokenDecimals(
    context,
    poolConfig.token1,
    token1Fallback,
    timestamp
  );

  return { token0Decimals, token1Decimals };
}

async function getOrCreateUserLPPositionIndex(
  context: handlerContext,
  userId: string,
  timestamp: number
) {
  const normalizedUserId = normalizeAddress(userId);
  let index = await context.UserLPPositionIndex.get(normalizedUserId);
  if (!index) {
    index = {
      id: normalizedUserId,
      user_id: normalizedUserId,
      positionIds: [],
      lastUpdate: timestamp,
    };
    context.UserLPPositionIndex.set(index);
  }
  return index;
}

async function getOrCreatePoolLPPositionIndex(
  context: handlerContext,
  pool: string,
  timestamp: number
) {
  const poolId = normalizeAddress(pool);
  let index = await context.LPPoolPositionIndex.get(poolId);
  if (!index) {
    index = {
      id: poolId,
      pool: poolId,
      positionIds: [],
      lastUpdate: timestamp,
    };
    context.LPPoolPositionIndex.set(index);
  }
  return index;
}

async function seedPoolStateFromChain(
  context: handlerContext,
  pool: string,
  timestamp: number,
  blockNumber?: bigint
): Promise<Awaited<ReturnType<typeof getOrCreateLPPoolState>>> {
  const poolState = await getOrCreateLPPoolState(context, pool, timestamp);
  if (!shouldUseEthCalls()) {
    logLpDebug(context, `[lp] seedPoolStateFromChain skipped (eth calls disabled) pool=${pool}`);
    return poolState;
  }
  if (
    poolState.sqrtPriceX96 !== 0n &&
    poolState.token0Price !== 0n &&
    poolState.token1Price !== 0n
  ) {
    return poolState;
  }

  const slot0 = await readPoolSlot0(pool, blockNumber, context.log);
  if (!slot0) {
    logLpDebug(context, `[lp] seedPoolStateFromChain slot0 unavailable pool=${pool}`);
    return poolState;
  }

  let token0Price = poolState.token0Price;
  let token1Price = poolState.token1Price;
  const poolConfig = await getEffectiveLPPoolConfig(context, pool);
  if (poolConfig) {
    const isAusdToken0 = poolConfig.token0 === AUSD_ADDRESS;
    const isAusdToken1 = poolConfig.token1 === AUSD_ADDRESS;
    if (isAusdToken0 || isAusdToken1) {
      const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
        context,
        poolConfig,
        timestamp
      );
      const ausdPrice = getAusdPrice();
      const pairedTokenPrice = calculateDustPriceFromPool(
        slot0.sqrtPriceX96,
        ausdPrice,
        isAusdToken0,
        token0Decimals,
        token1Decimals
      );
      token0Price = isAusdToken0 ? ausdPrice : pairedTokenPrice;
      token1Price = isAusdToken0 ? pairedTokenPrice : ausdPrice;
    }
  }

  const updatedState = {
    ...poolState,
    currentTick: slot0.tick,
    sqrtPriceX96: slot0.sqrtPriceX96,
    token0Price,
    token1Price,
    lastUpdate: timestamp,
  };
  context.LPPoolState.set(updatedState);
  if (token0Price === 0n || token1Price === 0n) {
    logLpDebug(
      context,
      `[lp] seedPoolStateFromChain zero price pool=${pool} token0Price=${token0Price.toString()} token1Price=${token1Price.toString()}`
    );
  }
  return updatedState;
}

async function addPositionToUserIndex(
  context: handlerContext,
  userId: string,
  positionId: string,
  timestamp: number
) {
  const normalizedUserId = normalizeAddress(userId);
  const index = await getOrCreateUserLPPositionIndex(context, normalizedUserId, timestamp);
  if (index.positionIds.includes(positionId)) {
    if (index.lastUpdate !== timestamp) {
      context.UserLPPositionIndex.set({ ...index, lastUpdate: timestamp });
    }
    return;
  }
  context.UserLPPositionIndex.set({
    ...index,
    positionIds: [...index.positionIds, positionId],
    lastUpdate: timestamp,
  });
}

async function removePositionFromUserIndex(
  context: handlerContext,
  userId: string,
  positionId: string,
  timestamp: number
) {
  const normalizedUserId = normalizeAddress(userId);
  const index = await context.UserLPPositionIndex.get(normalizedUserId);
  if (!index || index.positionIds.length === 0) return;
  const nextIds = index.positionIds.filter(id => id !== positionId);
  if (nextIds.length === index.positionIds.length) return;
  context.UserLPPositionIndex.set({
    ...index,
    positionIds: nextIds,
    lastUpdate: timestamp,
  });
}

async function addPositionToPoolIndex(
  context: handlerContext,
  pool: string,
  positionId: string,
  timestamp: number
) {
  const index = await getOrCreatePoolLPPositionIndex(context, pool, timestamp);
  if (index.positionIds.includes(positionId)) {
    if (index.lastUpdate !== timestamp) {
      context.LPPoolPositionIndex.set({ ...index, lastUpdate: timestamp });
    }
    return;
  }
  context.LPPoolPositionIndex.set({
    ...index,
    positionIds: [...index.positionIds, positionId],
    lastUpdate: timestamp,
  });
}

async function removePositionFromPoolIndex(
  context: handlerContext,
  pool: string,
  positionId: string,
  timestamp: number
) {
  const poolId = normalizeAddress(pool);
  const index = await context.LPPoolPositionIndex.get(poolId);
  if (!index || index.positionIds.length === 0) return;
  const nextIds = index.positionIds.filter(id => id !== positionId);
  if (nextIds.length === index.positionIds.length) return;
  context.LPPoolPositionIndex.set({
    ...index,
    positionIds: nextIds,
    lastUpdate: timestamp,
  });
}

async function listUserLPPositions(context: handlerContext, userId: string) {
  const normalizedUserId = normalizeAddress(userId);
  const indexStore = (
    context as unknown as {
      UserLPPositionIndex?: handlerContext['UserLPPositionIndex'];
    }
  ).UserLPPositionIndex;
  const positionStore = (
    context as unknown as {
      UserLPPosition?: handlerContext['UserLPPosition'];
    }
  ).UserLPPosition;
  if (!indexStore || !positionStore) return [];

  const index = await indexStore.get(normalizedUserId);
  if (!index || index.positionIds.length === 0) return [];
  const positions = await Promise.all(index.positionIds.map(id => positionStore.get(id)));
  return positions.filter(
    (position): position is Exclude<typeof position, undefined> => position !== undefined
  );
}

async function listPoolLPPositions(context: handlerContext, pool: string) {
  const poolId = normalizeAddress(pool);
  const indexStore = (
    context as unknown as {
      LPPoolPositionIndex?: handlerContext['LPPoolPositionIndex'];
    }
  ).LPPoolPositionIndex;
  const positionStore = (
    context as unknown as {
      UserLPPosition?: handlerContext['UserLPPosition'];
    }
  ).UserLPPosition;
  if (!indexStore || !positionStore) return [];

  const index = await indexStore.get(poolId);
  if (!index || index.positionIds.length === 0) return [];
  const positions = await Promise.all(index.positionIds.map(id => positionStore.get(id)));
  return positions.filter(
    (position): position is Exclude<typeof position, undefined> => position !== undefined
  );
}

async function getConfiguredLPPoolCount(context: handlerContext): Promise<number> {
  const registry = await context.LPPoolRegistry.get('global');
  return registry?.poolIds?.length ?? 0;
}

async function getEffectiveLpRateBps(
  context: handlerContext,
  poolConfig: { lpRateBps: bigint }
): Promise<bigint> {
  const configuredPools = await getConfiguredLPPoolCount(context);
  if (configuredPools <= 1) {
    const config = await context.LeaderboardConfig.get('global');
    if (config?.lpRateBps !== undefined) {
      return config.lpRateBps;
    }
  }
  return poolConfig.lpRateBps;
}

/**
 * Calculate paired token USD price from pool sqrtPriceX96 and AUSD price.
 *
 * sqrtPriceX96 = sqrt(token1/token0) * 2^96 (raw units)
 * Human price ratio requires decimal adjustment: 10^(dec0-dec1).
 */
function calculateDustPriceFromPool(
  sqrtPriceX96: bigint,
  ausdPriceUsd: bigint,
  isAusdToken0: boolean,
  token0Decimals: number,
  token1Decimals: number
): bigint {
  if (sqrtPriceX96 === 0n || ausdPriceUsd === 0n) return 0n;

  // price = (sqrtPriceX96 / 2^96)^2
  // To avoid precision loss, we calculate: price = sqrtPriceX96^2 / 2^192
  const Q192 = 2n ** 192n;

  // sqrtPriceX96^2 gives us token1/token0 * 2^192
  const priceX192 = sqrtPriceX96 * sqrtPriceX96;

  if (priceX192 === 0n) return 0n;

  if (isAusdToken0) {
    // token0 = AUSD, token1 = non-AUSD
    // token1USD = AUSD_USD / (token1/token0)
    let numerator = ausdPriceUsd * Q192;
    let denominator = priceX192;
    const decDiff = token1Decimals - token0Decimals;
    if (decDiff >= 0) {
      numerator *= 10n ** BigInt(decDiff);
    } else {
      denominator *= 10n ** BigInt(-decDiff);
    }
    return numerator / denominator;
  }

  // token1 = AUSD, token0 = non-AUSD
  // token0USD = AUSD_USD * (token1/token0)
  let numerator = ausdPriceUsd * priceX192;
  let denominator = Q192;
  const decDiff = token0Decimals - token1Decimals;
  if (decDiff >= 0) {
    numerator *= 10n ** BigInt(decDiff);
  } else {
    denominator *= 10n ** BigInt(-decDiff);
  }
  return numerator / denominator;
}

/**
 * Get AUSD price - stablecoin pegged to $1
 * Returns price in 8 decimals
 */
function getAusdPrice(): bigint {
  // AUSD is a stablecoin pegged to $1
  return BigInt(1e8); // $1 in 8 decimals
}

/**
 * Calculate USD value of LP position based on token amounts and prices
 * Prices are expected in 8 decimals (Chainlink format)
 * Returns value in 8 decimals (same as prices)
 */
function calculatePositionValueUsd(
  amount0: bigint,
  amount1: bigint,
  token0PriceUsd: bigint,
  token1PriceUsd: bigint,
  token0Decimals: number = 18,
  token1Decimals: number = 18
): bigint {
  // Value = (amount0 * price0 / 10^decimals0) + (amount1 * price1 / 10^decimals1)
  // Result is in 8 decimals (price decimals)
  const scale0 = 10n ** BigInt(token0Decimals);
  const scale1 = 10n ** BigInt(token1Decimals);
  const value0 = (amount0 * token0PriceUsd) / scale0;
  const value1 = (amount1 * token1PriceUsd) / scale1;
  return value0 + value1;
}

function derivePositionAmounts(
  liquidity: bigint,
  tickLower: number,
  tickUpper: number,
  sqrtPriceX96: bigint,
  fallbackAmount0: bigint,
  fallbackAmount1: bigint
): { amount0: bigint; amount1: bigint; usedLiquidity: boolean } {
  if (liquidity === 0n || sqrtPriceX96 === 0n) {
    return { amount0: fallbackAmount0, amount1: fallbackAmount1, usedLiquidity: false };
  }

  const { amount0, amount1 } = getAmountsForLiquidity(
    sqrtPriceX96,
    tickLower,
    tickUpper,
    liquidity
  );
  return { amount0, amount1, usedLiquidity: true };
}

/**
 * Settle LP points for a position based on accumulated in-range time
 * Called before any position state change to capture earned points
 */
async function settleLPPosition(
  context: handlerContext,
  position: {
    id: string;
    user_id: string;
    pool: string;
    isInRange: boolean;
    valueUsd: bigint;
    lastInRangeTimestamp: number;
    accumulatedInRangeSeconds: bigint;
    lastSettledAt: number;
    settledLpPoints: bigint;
  },
  currentTimestamp: number
): Promise<{
  newAccumulatedSeconds: bigint;
  newSettledPoints: bigint;
  pointsEarned: bigint;
  settledAt: number;
  pointsStartTimestamp: number;
  pointsEndTimestamp: number;
}> {
  const leaderboardState = await context.LeaderboardState.get('current');
  const epochNumber = leaderboardState?.currentEpochNumber ?? 0n;
  let epochStart = 0;
  let effectiveTimestamp = currentTimestamp;

  if (epochNumber > 0n) {
    const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
    if (epoch) {
      epochStart = epoch.startTime;
      if (!leaderboardState?.isActive && epoch.endTime && currentTimestamp > epoch.endTime) {
        effectiveTimestamp = epoch.endTime;
      }
    }
  } else {
    epochStart = currentTimestamp;
  }

  let additionalInRangeSeconds = 0n;

  // If position was in range, accumulate the time since last update
  if (position.isInRange && position.lastInRangeTimestamp > 0) {
    const secondsElapsed = currentTimestamp - position.lastInRangeTimestamp;
    if (secondsElapsed > 0) {
      additionalInRangeSeconds = BigInt(secondsElapsed);
    }
  }

  const newAccumulatedSeconds = position.accumulatedInRangeSeconds + additionalInRangeSeconds;

  const poolConfig = await getEffectiveLPPoolConfig(context, position.pool);
  if (!poolConfig || epochNumber === 0n) {
    const reasons = [];
    if (!poolConfig) reasons.push('missing_pool_config');
    if (epochNumber === 0n) reasons.push('epoch_number_zero');
    if (!position.isInRange) reasons.push('out_of_range');
    if (position.valueUsd === 0n) reasons.push('value_usd_zero');
    if (reasons.length > 0) {
      logLpDebug(context, `[lp] settle skip position=${position.id} reasons=${reasons.join(',')}`);
    }
    return {
      newAccumulatedSeconds,
      newSettledPoints: position.settledLpPoints,
      pointsEarned: 0n,
      settledAt: effectiveTimestamp,
      pointsStartTimestamp: 0,
      pointsEndTimestamp: effectiveTimestamp,
    };
  }

  // Calculate points for the period since last settlement
  // Points = (valueUsd / 1e8) * lpRatePerHour * (inRangeHours)
  const accrualStart = Math.max(position.lastInRangeTimestamp, position.lastSettledAt, epochStart);
  let pointsStartTimestamp = 0;
  let pointsSeconds = 0n;
  if (
    position.isInRange &&
    effectiveTimestamp > accrualStart &&
    position.lastInRangeTimestamp > 0
  ) {
    pointsStartTimestamp = accrualStart;
    pointsSeconds = BigInt(effectiveTimestamp - accrualStart);
  }

  const effectiveLpRateBps = await getEffectiveLpRateBps(context, poolConfig);
  let pointsEarned = 0n;
  if (pointsSeconds > 0n && position.valueUsd > 0n && effectiveLpRateBps > 0n) {
    const numerator = position.valueUsd * effectiveLpRateBps * pointsSeconds * POINTS_SCALE;
    const denominator = 10n ** 8n * BASIS_POINTS * BigInt(SECONDS_PER_DAY);
    pointsEarned = numerator / denominator;
  }

  if (pointsEarned === 0n) {
    const reasons = [];
    if (!position.isInRange) reasons.push('out_of_range');
    if (position.lastInRangeTimestamp === 0) reasons.push('last_in_range_zero');
    if (pointsSeconds === 0n) reasons.push('no_accrual_seconds');
    if (position.valueUsd === 0n) reasons.push('value_usd_zero');
    if (effectiveLpRateBps === 0n) reasons.push('lp_rate_zero');
    logLpDebug(
      context,
      `[lp] settle zero points position=${position.id} reasons=${reasons.join(',')} valueUsd=${position.valueUsd.toString()} pointsSeconds=${pointsSeconds.toString()} lpRateBps=${effectiveLpRateBps.toString()}`
    );
  }

  const newSettledPoints = position.settledLpPoints + pointsEarned;

  return {
    newAccumulatedSeconds,
    newSettledPoints,
    pointsEarned,
    settledAt: effectiveTimestamp,
    pointsStartTimestamp,
    pointsEndTimestamp: effectiveTimestamp,
  };
}

// ============================================
//     NonfungiblePositionManager Handlers
// ============================================

NonfungiblePositionManager.IncreaseLiquidity.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenId = event.params.tokenId;
  const positionId = tokenId.toString();
  const timestamp = Number(event.block.timestamp);
  const positionManager = normalizeAddress(event.srcAddress);
  const liquidityDelta = BigInt(event.params.liquidity);
  const isHardcodedManager = positionManager === HARDCODED_LP_POSITION_MANAGER;
  // For hardcoded manager, ensure config exists upfront
  const hardcodedConfig = isHardcodedManager
    ? await ensureHardcodedPoolConfig(context, timestamp)
    : null;
  logLpDebug(
    context,
    `[lp] IncreaseLiquidity tokenId=${positionId} manager=${positionManager} amount0=${event.params.amount0.toString()} amount1=${event.params.amount1.toString()} liquidity=${liquidityDelta.toString()}`
  );

  let position = await context.UserLPPosition.get(positionId);

  // If position doesn't exist, it will be created when we process the Transfer event
  // The Transfer event sets the owner, and we update tick ranges here
  if (!position) {
    // Position not yet created - cache liquidity amounts for the Transfer handler.
    const pendingKey = `pending:${tokenId.toString()}`;
    const existingMint = await context.LPMintData.get(pendingKey);
    if (existingMint) {
      return;
    }

    let positionData = shouldUseEthCalls()
      ? await readLPPosition(positionManager, tokenId, BigInt(event.block.number), context.log)
      : null;
    let poolConfig: Awaited<ReturnType<typeof getActiveLPPoolConfig>> = positionData
      ? await resolvePoolConfigForPosition(
          context,
          positionManager,
          positionData.token0,
          positionData.token1,
          positionData.fee,
          timestamp,
          BigInt(event.block.number)
        )
      : hardcodedConfig;
    const txMintKey = buildTxMintKey(
      event.transaction.hash,
      event.params.amount0,
      event.params.amount1,
      liquidityDelta
    );
    let mintData: Awaited<ReturnType<typeof context.LPMintData.get>>;

    if (!positionData || !poolConfig) {
      mintData = await context.LPMintData.get(txMintKey);
      if (mintData) {
        const mintPool = normalizeAddress(mintData.pool);
        poolConfig =
          mintPool === HARDCODED_LP_POOL
            ? await ensureHardcodedPoolConfig(context, timestamp)
            : await getActiveLPPoolConfig(context, mintPool);
        if (poolConfig) {
          positionData = {
            token0: poolConfig.token0,
            token1: poolConfig.token1,
            fee: 0,
            tickLower: mintData.tickLower,
            tickUpper: mintData.tickUpper,
            liquidity: mintData.liquidity,
          };
        }
      }
    }

    if (!positionData || !poolConfig) return;

    // Create position directly - don't rely on Transfer event ordering
    const owner = event.transaction.from ? normalizeAddress(event.transaction.from) : ZERO_ADDRESS;
    await getOrCreateUser(context, owner);

    const poolState = await seedPoolStateFromChain(
      context,
      poolConfig.pool,
      timestamp,
      BigInt(event.block.number)
    );
    const isInRange = isPositionInRange(
      positionData.tickLower,
      positionData.tickUpper,
      poolState.currentTick
    );

    const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
      context,
      poolConfig,
      timestamp
    );
    const ausdPrice = getAusdPrice();
    const isAusdToken0 = poolConfig.token0 === AUSD_ADDRESS;
    const isAusdToken1 = poolConfig.token1 === AUSD_ADDRESS;
    let token0Price = poolState.token0Price;
    let token1Price = poolState.token1Price;
    if (isAusdToken0 || isAusdToken1) {
      const pairedTokenPrice = calculateDustPriceFromPool(
        poolState.sqrtPriceX96,
        ausdPrice,
        isAusdToken0,
        token0Decimals,
        token1Decimals
      );
      token0Price = isAusdToken0 ? ausdPrice : pairedTokenPrice;
      token1Price = isAusdToken0 ? pairedTokenPrice : ausdPrice;
    }

    const derivedAmounts = derivePositionAmounts(
      liquidityDelta,
      positionData.tickLower,
      positionData.tickUpper,
      poolState.sqrtPriceX96,
      event.params.amount0,
      event.params.amount1
    );
    const valueUsd = calculatePositionValueUsd(
      derivedAmounts.amount0,
      derivedAmounts.amount1,
      token0Price,
      token1Price,
      token0Decimals,
      token1Decimals
    );

    context.UserLPPosition.set({
      id: positionId,
      tokenId,
      user_id: owner,
      pool: poolConfig.pool,
      positionManager,
      tickLower: positionData.tickLower,
      tickUpper: positionData.tickUpper,
      liquidity: liquidityDelta,
      amount0: derivedAmounts.amount0,
      amount1: derivedAmounts.amount1,
      isInRange,
      valueUsd,
      lastInRangeTimestamp: isInRange ? timestamp : 0,
      accumulatedInRangeSeconds: 0n,
      lastSettledAt: timestamp,
      settledLpPoints: 0n,
      createdAt: timestamp,
      lastUpdate: timestamp,
    });

    await addPositionToPoolIndex(context, poolConfig.pool, positionId, timestamp);
    await addPositionToUserIndex(context, owner, positionId, timestamp);
    await updateUserLPStats(context, owner, timestamp);
    await updatePoolLPStats(context, poolConfig.pool, timestamp);

    // Clean up mint data
    if (mintData) {
      const poolMintKey = `${mintData.pool}:${mintData.tickLower}:${mintData.tickUpper}:${mintData.txHash}`;
      context.LPMintData.deleteUnsafe(poolMintKey);
      context.LPMintData.deleteUnsafe(txMintKey);
    } else {
      context.LPMintData.deleteUnsafe(txMintKey);
    }
    return;
  }

  const existingPool = normalizeAddress(position.pool);
  const poolConfig =
    existingPool === HARDCODED_LP_POOL
      ? await ensureHardcodedPoolConfig(context, timestamp)
      : await getActiveLPPoolConfig(context, existingPool);
  if (!poolConfig) return;

  const poolState = await seedPoolStateFromChain(
    context,
    position.pool,
    timestamp,
    BigInt(event.block.number)
  );
  const wasInRange = position.isInRange;
  const isNowInRange = isPositionInRange(
    position.tickLower,
    position.tickUpper,
    poolState.currentTick
  );

  // Settle any accumulated points before changing state
  const settlement = await settleLPPosition(context, position, timestamp);

  // Update position amounts
  const newLiquidity = position.liquidity + BigInt(event.params.liquidity);
  const fallbackAmount0 = position.amount0 + event.params.amount0;
  const fallbackAmount1 = position.amount1 + event.params.amount1;
  const derivedAmounts = derivePositionAmounts(
    newLiquidity,
    position.tickLower,
    position.tickUpper,
    poolState.sqrtPriceX96,
    fallbackAmount0,
    fallbackAmount1
  );
  const newAmount0 = derivedAmounts.amount0;
  const newAmount1 = derivedAmounts.amount1;

  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const valueUsd = calculatePositionValueUsd(
    newAmount0,
    newAmount1,
    poolState.token0Price,
    poolState.token1Price,
    token0Decimals,
    token1Decimals
  );

  // Update lastInRangeTimestamp based on range transition
  let newLastInRangeTimestamp = position.lastInRangeTimestamp;
  if (isNowInRange && !wasInRange) {
    // Entering range - start tracking time
    newLastInRangeTimestamp = timestamp;
  } else if (!isNowInRange && wasInRange) {
    // Exiting range - time already accumulated in settlement
    newLastInRangeTimestamp = 0;
  } else if (isNowInRange) {
    // Still in range - update timestamp for next settlement
    newLastInRangeTimestamp = timestamp;
  }

  context.UserLPPosition.set({
    ...position,
    liquidity: newLiquidity,
    amount0: newAmount0,
    amount1: newAmount1,
    isInRange: isNowInRange,
    valueUsd,
    lastInRangeTimestamp: newLastInRangeTimestamp,
    accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
    lastSettledAt: settlement.settledAt,
    settledLpPoints: settlement.newSettledPoints,
    lastUpdate: timestamp,
  });

  await updateUserEpochLPPoints(
    context,
    position.user_id,
    settlement.pointsEarned,
    timestamp,
    settlement.pointsStartTimestamp,
    settlement.pointsEndTimestamp
  );
  await updateUserLPStats(context, position.user_id, timestamp);
  await updatePoolLPStats(context, position.pool, timestamp);
});

NonfungiblePositionManager.DecreaseLiquidity.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenId = event.params.tokenId;
  const positionId = tokenId.toString();
  const timestamp = Number(event.block.timestamp);

  let position = await context.UserLPPosition.get(positionId);
  if (!position) return;

  const decreasePool = normalizeAddress(position.pool);
  const poolConfig =
    decreasePool === HARDCODED_LP_POOL
      ? await ensureHardcodedPoolConfig(context, timestamp)
      : await getActiveLPPoolConfig(context, decreasePool);
  if (!poolConfig) return;

  const poolState = await seedPoolStateFromChain(
    context,
    position.pool,
    timestamp,
    BigInt(event.block.number)
  );
  const wasInRange = position.isInRange;
  const isNowInRange = isPositionInRange(
    position.tickLower,
    position.tickUpper,
    poolState.currentTick
  );

  // Settle any accumulated points before changing state
  const settlement = await settleLPPosition(context, position, timestamp);

  // Update position amounts
  const newLiquidity = position.liquidity - BigInt(event.params.liquidity);
  const fallbackAmount0 =
    position.amount0 > event.params.amount0 ? position.amount0 - event.params.amount0 : 0n;
  const fallbackAmount1 =
    position.amount1 > event.params.amount1 ? position.amount1 - event.params.amount1 : 0n;
  const derivedAmounts = derivePositionAmounts(
    newLiquidity,
    position.tickLower,
    position.tickUpper,
    poolState.sqrtPriceX96,
    fallbackAmount0,
    fallbackAmount1
  );
  const newAmount0 = derivedAmounts.amount0;
  const newAmount1 = derivedAmounts.amount1;

  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const valueUsd = calculatePositionValueUsd(
    newAmount0,
    newAmount1,
    poolState.token0Price,
    poolState.token1Price,
    token0Decimals,
    token1Decimals
  );

  // Update lastInRangeTimestamp based on range transition
  let newLastInRangeTimestamp = position.lastInRangeTimestamp;
  if (isNowInRange && !wasInRange) {
    newLastInRangeTimestamp = timestamp;
  } else if (!isNowInRange && wasInRange) {
    newLastInRangeTimestamp = 0;
  } else if (isNowInRange) {
    newLastInRangeTimestamp = timestamp;
  }

  context.UserLPPosition.set({
    ...position,
    liquidity: newLiquidity,
    amount0: newAmount0,
    amount1: newAmount1,
    isInRange: isNowInRange,
    valueUsd,
    lastInRangeTimestamp: newLastInRangeTimestamp,
    accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
    lastSettledAt: settlement.settledAt,
    settledLpPoints: settlement.newSettledPoints,
    lastUpdate: timestamp,
  });

  await updateUserEpochLPPoints(
    context,
    position.user_id,
    settlement.pointsEarned,
    timestamp,
    settlement.pointsStartTimestamp,
    settlement.pointsEndTimestamp
  );
  await updateUserLPStats(context, position.user_id, timestamp);
  await updatePoolLPStats(context, position.pool, timestamp);
});

NonfungiblePositionManager.Transfer.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenId = event.params.tokenId;
  const positionId = tokenId.toString();
  const from = normalizeAddress(event.params.from);
  const to = normalizeAddress(event.params.to);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const positionManager = normalizeAddress(event.srcAddress);
  const isHardcodedManager = positionManager === HARDCODED_LP_POSITION_MANAGER;
  // For hardcoded manager, ensure config exists upfront
  const hardcodedConfig = isHardcodedManager
    ? await ensureHardcodedPoolConfig(context, timestamp)
    : null;
  logLpDebug(
    context,
    `[lp] Transfer tokenId=${positionId} from=${from} to=${to} manager=${positionManager} tx=${event.transaction.hash}`
  );

  // Handle mint (from zero address)
  if (from === ZERO_ADDRESS) {
    await getOrCreateUser(context, to);

    // Check if position was already created by IncreaseLiquidity (event ordering may vary)
    const existingPosition = await context.UserLPPosition.get(positionId);
    if (existingPosition) {
      // Position exists - just update owner if different (Transfer has correct owner)
      if (existingPosition.user_id !== to) {
        const oldOwner = existingPosition.user_id;
        context.UserLPPosition.set({
          ...existingPosition,
          user_id: to,
          lastUpdate: timestamp,
        });
        await removePositionFromUserIndex(context, oldOwner, positionId, timestamp);
        await addPositionToUserIndex(context, to, positionId, timestamp);
        await updateUserLPStats(context, oldOwner, timestamp);
        await updateUserLPStats(context, to, timestamp);
      }
      return;
    }

    // Try eth_call first to get position data
    let positionData = shouldUseEthCalls()
      ? await readLPPosition(positionManager, tokenId, blockNumber, context.log)
      : null;
    let poolConfig: Awaited<ReturnType<typeof getActiveLPPoolConfig>> = positionData
      ? await resolvePoolConfigForPosition(
          context,
          positionManager,
          positionData.token0,
          positionData.token1,
          positionData.fee,
          timestamp,
          blockNumber
        )
      : hardcodedConfig;

    // Fallback: If eth_call failed or no matching pool config, try to find LPMintData
    // The IncreaseLiquidity handler caches this data keyed by tokenId
    const pendingMintKey = `pending:${tokenId.toString()}`;
    let mintData = await context.LPMintData.get(pendingMintKey);

    if (!positionData || !poolConfig) {
      // Use LPMintData to reconstruct position data
      if (mintData) {
        const mintPool = normalizeAddress(mintData.pool);
        poolConfig =
          mintPool === HARDCODED_LP_POOL
            ? await ensureHardcodedPoolConfig(context, timestamp)
            : await getActiveLPPoolConfig(context, mintPool);
        if (poolConfig) {
          positionData = {
            token0: poolConfig.token0,
            token1: poolConfig.token1,
            fee: 0,
            tickLower: mintData.tickLower,
            tickUpper: mintData.tickUpper,
            liquidity: mintData.liquidity,
          };
        }
      }
    }

    // If we still don't have pool config, we can't create the position
    if (!poolConfig || !positionData) {
      logLpDebug(
        context,
        `[lp] Transfer mint skip tokenId=${positionId} missing=${!positionData ? 'position' : ''}${!positionData && !poolConfig ? ',' : ''}${!poolConfig ? 'poolConfig' : ''} tx=${event.transaction.hash}`
      );
      return;
    }

    const pool = poolConfig.pool;
    const poolState = await seedPoolStateFromChain(
      context,
      pool,
      timestamp,
      BigInt(event.block.number)
    );
    const isInRange = isPositionInRange(
      positionData.tickLower,
      positionData.tickUpper,
      poolState.currentTick
    );

    // Calculate TVL: AUSD = $1, DUST price from pool ratio
    const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
      context,
      poolConfig,
      timestamp
    );
    const ausdPrice = getAusdPrice();
    const isAusdToken0 = poolConfig.token0 === AUSD_ADDRESS;
    const isAusdToken1 = poolConfig.token1 === AUSD_ADDRESS;
    let token0Price = poolState.token0Price;
    let token1Price = poolState.token1Price;
    if (isAusdToken0 || isAusdToken1) {
      const pairedTokenPrice = calculateDustPriceFromPool(
        poolState.sqrtPriceX96,
        ausdPrice,
        isAusdToken0,
        token0Decimals,
        token1Decimals
      );
      token0Price = isAusdToken0 ? ausdPrice : pairedTokenPrice;
      token1Price = isAusdToken0 ? pairedTokenPrice : ausdPrice;
    }

    // Use amounts from LPMintData (IncreaseLiquidity fires before Transfer and caches this)
    const fallbackAmount0 = mintData?.amount0 ?? 0n;
    const fallbackAmount1 = mintData?.amount1 ?? 0n;
    const derivedAmounts = derivePositionAmounts(
      positionData.liquidity,
      positionData.tickLower,
      positionData.tickUpper,
      poolState.sqrtPriceX96,
      fallbackAmount0,
      fallbackAmount1
    );
    const amount0 = derivedAmounts.amount0;
    const amount1 = derivedAmounts.amount1;

    // Calculate valueUsd using the amounts from mint data
    const valueUsd = calculatePositionValueUsd(
      amount0,
      amount1,
      token0Price,
      token1Price,
      token0Decimals,
      token1Decimals
    );
    logLpDebug(
      context,
      `[lp] mint position=${positionId} pool=${pool} tickLower=${positionData.tickLower} tickUpper=${positionData.tickUpper} currentTick=${poolState.currentTick} isInRange=${isInRange} valueUsd=${valueUsd.toString()}`
    );

    context.UserLPPosition.set({
      id: positionId,
      tokenId,
      user_id: to,
      pool,
      positionManager,
      tickLower: positionData.tickLower,
      tickUpper: positionData.tickUpper,
      liquidity: positionData.liquidity,
      amount0,
      amount1,
      isInRange,
      valueUsd,
      lastInRangeTimestamp: isInRange ? timestamp : 0,
      accumulatedInRangeSeconds: 0n,
      lastSettledAt: timestamp,
      settledLpPoints: 0n,
      createdAt: timestamp,
      lastUpdate: timestamp,
    });

    // Clean up mint data after use
    if (mintData) {
      context.LPMintData.deleteUnsafe(pendingMintKey);
    }

    await addPositionToPoolIndex(context, pool, positionId, timestamp);
    await addPositionToUserIndex(context, to, positionId, timestamp);

    // Update pool state with current prices
    context.LPPoolState.set({
      ...poolState,
      token0Price,
      token1Price,
      lastUpdate: timestamp,
    });

    await updateUserLPStats(context, to, timestamp);
    await updatePoolLPStats(context, pool, timestamp);
    return;
  }

  // Handle burn (to zero address)
  if (to === ZERO_ADDRESS) {
    const position = await context.UserLPPosition.get(positionId);
    if (position) {
      // Settle any remaining points before removing
      const settlement = await settleLPPosition(context, position, timestamp);

      // Update user's epoch stats with settled LP points
      await updateUserEpochLPPoints(
        context,
        position.user_id,
        settlement.pointsEarned,
        timestamp,
        settlement.pointsStartTimestamp,
        settlement.pointsEndTimestamp
      );

      // Mark position as removed
      context.UserLPPosition.set({
        ...position,
        liquidity: 0n,
        amount0: 0n,
        amount1: 0n,
        isInRange: false,
        valueUsd: 0n,
        accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
        settledLpPoints: settlement.newSettledPoints,
        lastSettledAt: settlement.settledAt,
        lastUpdate: timestamp,
      });

      await removePositionFromUserIndex(context, position.user_id, positionId, timestamp);
      await removePositionFromPoolIndex(context, position.pool, positionId, timestamp);

      await updateUserLPStats(context, position.user_id, timestamp);
      await updatePoolLPStats(context, position.pool, timestamp);
    }
    return;
  }

  // Handle transfer between users
  const position = await context.UserLPPosition.get(positionId);
  if (position) {
    const oldOwner = position.user_id;

    // Settle points for old owner before transfer
    const settlement = await settleLPPosition(context, position, timestamp);
    await updateUserEpochLPPoints(
      context,
      oldOwner,
      settlement.pointsEarned,
      timestamp,
      settlement.pointsStartTimestamp,
      settlement.pointsEndTimestamp
    );

    // Update position owner
    await getOrCreateUser(context, to);
    context.UserLPPosition.set({
      ...position,
      user_id: to,
      accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
      settledLpPoints: settlement.newSettledPoints,
      lastInRangeTimestamp: position.isInRange ? timestamp : 0, // Reset for new owner
      lastSettledAt: settlement.settledAt,
      lastUpdate: timestamp,
    });

    await removePositionFromUserIndex(context, oldOwner, positionId, timestamp);
    await addPositionToUserIndex(context, to, positionId, timestamp);

    await updateUserLPStats(context, oldOwner, timestamp);
    await updateUserLPStats(context, to, timestamp);
    await updatePoolLPStats(context, position.pool, timestamp);
  }
});

// ============================================
//     UniswapV3Pool Handlers
// ============================================

UniswapV3Pool.Initialize.handler(async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);
  const poolConfig =
    pool === HARDCODED_LP_POOL
      ? await ensureHardcodedPoolConfig(context, timestamp)
      : await getActiveLPPoolConfig(context, pool);
  if (!poolConfig) return;

  const currentTick = Number(event.params.tick);
  const sqrtPriceX96 = event.params.sqrtPriceX96;

  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  let token0Price = 0n;
  let token1Price = 0n;
  const isAusdToken0 = poolConfig.token0 === AUSD_ADDRESS;
  const isAusdToken1 = poolConfig.token1 === AUSD_ADDRESS;
  if (isAusdToken0 || isAusdToken1) {
    const ausdPrice = getAusdPrice();
    const pairedTokenPrice = calculateDustPriceFromPool(
      sqrtPriceX96,
      ausdPrice,
      isAusdToken0,
      token0Decimals,
      token1Decimals
    );
    token0Price = isAusdToken0 ? ausdPrice : pairedTokenPrice;
    token1Price = isAusdToken0 ? pairedTokenPrice : ausdPrice;
  }

  context.LPPoolState.set({
    id: pool,
    pool,
    currentTick,
    sqrtPriceX96,
    token0Price,
    token1Price,
    lastUpdate: timestamp,
  });
});

UniswapV3Pool.Swap.handler(async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);

  // Check if this pool is tracked
  const poolConfig =
    pool === HARDCODED_LP_POOL
      ? await ensureHardcodedPoolConfig(context, timestamp)
      : await getActiveLPPoolConfig(context, pool);
  if (!poolConfig) return;
  const currentTick = Number(event.params.tick);
  const sqrtPriceX96 = event.params.sqrtPriceX96;

  // Update pool state
  let poolState = await context.LPPoolState.get(pool);
  const oldTick = poolState?.currentTick ?? 0;

  // Update token prices for AUSD-paired pools using sqrtPriceX96
  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  let token0Price = poolState?.token0Price ?? 0n;
  let token1Price = poolState?.token1Price ?? 0n;
  const isAusdToken0 = poolConfig.token0 === AUSD_ADDRESS;
  const isAusdToken1 = poolConfig.token1 === AUSD_ADDRESS;
  if (isAusdToken0 || isAusdToken1) {
    const ausdPrice = getAusdPrice();
    const pairedTokenPrice = calculateDustPriceFromPool(
      sqrtPriceX96,
      ausdPrice,
      isAusdToken0,
      token0Decimals,
      token1Decimals
    );
    token0Price = isAusdToken0 ? ausdPrice : pairedTokenPrice;
    token1Price = isAusdToken0 ? pairedTokenPrice : ausdPrice;
  }

  context.LPPoolState.set({
    id: pool,
    pool,
    currentTick,
    sqrtPriceX96,
    token0Price,
    token1Price,
    lastUpdate: timestamp,
  });

  // If tick changed significantly, update in-range status for affected positions
  // This is expensive, so we only do it on significant tick changes
  if (Math.abs(currentTick - oldTick) > 0) {
    await updatePositionsInRangeStatus(
      context,
      pool,
      currentTick,
      timestamp,
      token0Price,
      token1Price,
      sqrtPriceX96
    );
  }

  const volumeUsd = calculateSwapVolumeUsd(
    event.params.amount0,
    event.params.amount1,
    token0Price,
    token1Price,
    token0Decimals,
    token1Decimals
  );
  await updatePoolFeeStats(context, poolConfig, volumeUsd, timestamp, BigInt(event.block.number));
});

UniswapV3Pool.Mint.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const pool = normalizeAddress(event.srcAddress);
  let poolConfig: Awaited<ReturnType<typeof getActiveLPPoolConfig>> = null;
  if (pool === HARDCODED_LP_POOL) {
    poolConfig = await ensureHardcodedPoolConfig(context, Number(event.block.timestamp));
  } else {
    poolConfig = await getActiveLPPoolConfig(context, pool);
  }
  if (!poolConfig) return;

  const timestamp = Number(event.block.timestamp);
  const tickLower = Number(event.params.tickLower);
  const tickUpper = Number(event.params.tickUpper);
  const owner = normalizeAddress(event.params.owner);

  // Pool Mint event gives us tick ranges but not tokenId
  // Store this data for correlation with IncreaseLiquidity
  // Key: pool:tickLower:tickUpper:txHash to correlate with IncreaseLiquidity in same tx
  const mintKey = `${pool}:${tickLower}:${tickUpper}:${event.transaction.hash}`;
  const txMintKey = buildTxMintKey(
    event.transaction.hash,
    event.params.amount0,
    event.params.amount1,
    BigInt(event.params.amount)
  );

  // Store mint data for position creation correlation
  // This will be used by IncreaseLiquidity to get tick ranges
  context.LPMintData.set({
    id: mintKey,
    pool,
    positionManager: poolConfig.positionManager,
    owner,
    tickLower,
    tickUpper,
    liquidity: BigInt(event.params.amount),
    amount0: event.params.amount0,
    amount1: event.params.amount1,
    txHash: event.transaction.hash,
    timestamp,
  });

  const existingTxMint = await context.LPMintData.get(txMintKey);
  if (!existingTxMint) {
    context.LPMintData.set({
      id: txMintKey,
      pool,
      positionManager: poolConfig.positionManager,
      owner,
      tickLower,
      tickUpper,
      liquidity: BigInt(event.params.amount),
      amount0: event.params.amount0,
      amount1: event.params.amount1,
      txHash: event.transaction.hash,
      timestamp,
    });
  }
});

UniswapV3Pool.Burn.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  // Burn is handled via DecreaseLiquidity on PositionManager
});

// ============================================
//     Helper: Update User LP Stats
// ============================================

async function updateUserLPStats(context: handlerContext, userId: string, timestamp: number) {
  const stats = await getOrCreateUserLPStats(context, userId, timestamp);
  const positions = await listUserLPPositions(context, userId);
  let totalPositions = 0;
  let inRangePositions = 0;
  let totalValueUsd = 0n;
  let inRangeValueUsd = 0n;

  for (const position of positions) {
    const isActive =
      position.liquidity !== 0n || position.amount0 !== 0n || position.amount1 !== 0n;
    if (!isActive) continue;

    totalPositions += 1;
    totalValueUsd += position.valueUsd;

    if (position.isInRange) {
      inRangePositions += 1;
      inRangeValueUsd += position.valueUsd;
    }
  }

  context.UserLPStats.set({
    ...stats,
    totalPositions,
    inRangePositions,
    totalValueUsd,
    inRangeValueUsd,
    lastUpdate: timestamp,
  });
}

/**
 * Update UserEpochStats with earned LP points
 * This flows LP points into the leaderboard system
 */
async function updateUserEpochLPPoints(
  context: handlerContext,
  userId: string,
  pointsEarned: bigint,
  timestamp: number,
  accrualStartTimestamp: number,
  accrualEndTimestamp: number
) {
  if (pointsEarned === 0n) return;

  const leaderboardState = await context.LeaderboardState.get('current');
  if (!leaderboardState || leaderboardState.currentEpochNumber === 0n) return;

  const epochNumber = leaderboardState.currentEpochNumber;
  const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
  if (!epoch) return;

  const vpState = await refreshUserVotingPowerState(context, userId, timestamp);
  const combinedMultiplierBps =
    accrualEndTimestamp > accrualStartTimestamp
      ? await calculateAverageCombinedMultiplierBps(
          context,
          userId,
          accrualStartTimestamp,
          accrualEndTimestamp
        )
      : vpState.combinedMultiplierBps;

  const epochStats = await getOrCreateUserEpochStats(context, userId, epochNumber, timestamp);
  const newLpPoints = epochStats.lpPoints + pointsEarned;
  const lpPointsWithMultiplier =
    epochStats.lpPointsWithMultiplier +
    applyCombinedMultiplierScaled(pointsEarned, combinedMultiplierBps);

  const totalPoints =
    epochStats.depositPoints +
    epochStats.borrowPoints +
    newLpPoints +
    epochStats.dailySupplyPoints +
    epochStats.dailyBorrowPoints +
    epochStats.dailyRepayPoints +
    epochStats.dailyWithdrawPoints +
    epochStats.dailyVPPoints +
    epochStats.dailyLPPoints +
    epochStats.manualAwardPoints;

  const totalPointsWithMultiplier = computeTotalPointsWithMultiplier(
    {
      ...epochStats,
      lpPointsWithMultiplier,
    },
    userId,
    epochNumber
  );

  const testnetBonusBps = epochNumber === 1n ? getTestnetBonusBps(userId) : 0n;
  const updatedStats = {
    ...epochStats,
    lpPoints: newLpPoints,
    lpPointsWithMultiplier,
    lpMultiplierBps: combinedMultiplierBps,
    totalPoints,
    totalPointsWithMultiplier,
    totalMultiplierBps: combinedMultiplierBps,
    lastAppliedMultiplierBps: combinedMultiplierBps,
    testnetBonusBps,
    lastUpdatedAt: timestamp,
  };

  context.UserEpochStats.set(updatedStats);
  await updateLifetimePoints(context, userId, updatedStats);

  const finalPoints = Number(updatedStats.totalPointsWithMultiplier) / 1e18;
  const { updateLeaderboard } = await import('../helpers/leaderboard');
  await updateLeaderboard(context, userId, finalPoints, timestamp);
}

export async function settleUserLPPositions(
  context: handlerContext,
  userId: string,
  timestamp: number,
  blockNumber?: bigint
): Promise<void> {
  const normalizedUserId = normalizeAddress(userId);
  const positions = await listUserLPPositions(context, normalizedUserId);
  if (positions.length === 0) return;

  const positionsByPool = new Map<string, typeof positions>();
  for (const position of positions) {
    const poolId = normalizeAddress(position.pool);
    const bucket = positionsByPool.get(poolId);
    if (bucket) {
      bucket.push(position);
    } else {
      positionsByPool.set(poolId, [position]);
    }
  }

  for (const [poolId, poolPositions] of positionsByPool.entries()) {
    const poolConfig = await getEffectiveLPPoolConfig(context, poolId);
    if (!poolConfig) continue;

    const poolState = await seedPoolStateFromChain(context, poolId, timestamp, blockNumber);
    const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
      context,
      poolConfig,
      timestamp
    );

    for (const position of poolPositions) {
      if (position.liquidity === 0n && position.amount0 === 0n && position.amount1 === 0n) {
        continue;
      }

      const wasInRange = position.isInRange;
      const isNowInRange = isPositionInRange(
        position.tickLower,
        position.tickUpper,
        poolState.currentTick
      );

      const settlement = await settleLPPosition(context, position, timestamp);
      const derivedAmounts = derivePositionAmounts(
        position.liquidity,
        position.tickLower,
        position.tickUpper,
        poolState.sqrtPriceX96,
        position.amount0,
        position.amount1
      );
      const valueUsd = calculatePositionValueUsd(
        derivedAmounts.amount0,
        derivedAmounts.amount1,
        poolState.token0Price,
        poolState.token1Price,
        token0Decimals,
        token1Decimals
      );

      context.UserLPPosition.set({
        ...position,
        isInRange: isNowInRange,
        amount0: derivedAmounts.amount0,
        amount1: derivedAmounts.amount1,
        valueUsd,
        lastInRangeTimestamp: isNowInRange ? timestamp : 0,
        accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
        lastSettledAt: settlement.settledAt,
        settledLpPoints: settlement.newSettledPoints,
        lastUpdate: timestamp,
      });

      if (settlement.pointsEarned > 0n || wasInRange !== isNowInRange) {
        await updateUserEpochLPPoints(
          context,
          position.user_id,
          settlement.pointsEarned,
          timestamp,
          settlement.pointsStartTimestamp,
          settlement.pointsEndTimestamp
        );
      }
    }
  }

  await updateUserLPStats(context, normalizedUserId, timestamp);
}

async function settleLPPoolPositions(
  context: handlerContext,
  pool: string,
  timestamp: number
): Promise<void> {
  const poolConfig = await getEffectiveLPPoolConfig(context, pool);
  if (!poolConfig) return;

  const positions = await listPoolLPPositions(context, pool);
  if (positions.length === 0) {
    setPoolStats(context, normalizeAddress(pool), 0, 0, 0n, 0n, timestamp);
    return;
  }

  const poolState = await getOrCreateLPPoolState(context, pool, timestamp);
  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const touchedUsers = new Set<string>();
  let totalPositions = 0;
  let inRangePositions = 0;
  let totalValueUsd = 0n;
  let inRangeValueUsd = 0n;

  for (const position of positions) {
    if (position.liquidity === 0n && position.amount0 === 0n && position.amount1 === 0n) continue;

    const settlement = await settleLPPosition(context, position, timestamp);
    const derivedAmounts = derivePositionAmounts(
      position.liquidity,
      position.tickLower,
      position.tickUpper,
      poolState.sqrtPriceX96,
      position.amount0,
      position.amount1
    );
    const valueUsd = calculatePositionValueUsd(
      derivedAmounts.amount0,
      derivedAmounts.amount1,
      poolState.token0Price,
      poolState.token1Price,
      token0Decimals,
      token1Decimals
    );

    // Preserve current in-range status - don't reset it!
    // Update lastInRangeTimestamp to current time if still in-range for next settlement
    context.UserLPPosition.set({
      ...position,
      amount0: derivedAmounts.amount0,
      amount1: derivedAmounts.amount1,
      valueUsd,
      lastInRangeTimestamp: position.isInRange ? timestamp : 0,
      accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
      lastSettledAt: settlement.settledAt,
      settledLpPoints: settlement.newSettledPoints,
      lastUpdate: timestamp,
    });

    await updateUserEpochLPPoints(
      context,
      position.user_id,
      settlement.pointsEarned,
      timestamp,
      settlement.pointsStartTimestamp,
      settlement.pointsEndTimestamp
    );
    touchedUsers.add(position.user_id);

    totalPositions += 1;
    totalValueUsd += valueUsd;
    if (position.isInRange) {
      inRangePositions += 1;
      inRangeValueUsd += valueUsd;
    }
  }

  setPoolStats(
    context,
    normalizeAddress(pool),
    totalPositions,
    inRangePositions,
    totalValueUsd,
    inRangeValueUsd,
    timestamp
  );

  for (const userId of touchedUsers) {
    await updateUserLPStats(context, userId, timestamp);
  }
}

export async function settleAllLPPoolPositions(
  context: handlerContext,
  timestamp: number
): Promise<void> {
  const configs = await listActiveLPPoolConfigs(context);
  if (configs.length === 0) return;

  for (const config of configs) {
    await settleLPPoolPositions(context, config.pool, timestamp);
  }
}

async function updatePositionsInRangeStatus(
  context: handlerContext,
  pool: string,
  currentTick: number,
  timestamp: number,
  token0Price: bigint,
  token1Price: bigint,
  sqrtPriceX96: bigint
) {
  const poolConfig = await getEffectiveLPPoolConfig(context, pool);
  if (!poolConfig) return;

  const positions = await listPoolLPPositions(context, pool);
  if (positions.length === 0) {
    setPoolStats(context, normalizeAddress(pool), 0, 0, 0n, 0n, timestamp);
    return;
  }

  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const touchedUsers = new Set<string>();
  let totalPositions = 0;
  let inRangePositions = 0;
  let totalValueUsd = 0n;
  let inRangeValueUsd = 0n;

  for (const position of positions) {
    if (position.liquidity === 0n && position.amount0 === 0n && position.amount1 === 0n) continue;

    const wasInRange = position.isInRange;
    const isNowInRange = isPositionInRange(position.tickLower, position.tickUpper, currentTick);
    if (!wasInRange && !isNowInRange) {
      totalPositions += 1;
      totalValueUsd += position.valueUsd;
      continue;
    }

    const settlement = await settleLPPosition(context, position, timestamp);
    const derivedAmounts = derivePositionAmounts(
      position.liquidity,
      position.tickLower,
      position.tickUpper,
      sqrtPriceX96,
      position.amount0,
      position.amount1
    );
    const valueUsd = calculatePositionValueUsd(
      derivedAmounts.amount0,
      derivedAmounts.amount1,
      token0Price,
      token1Price,
      token0Decimals,
      token1Decimals
    );

    context.UserLPPosition.set({
      ...position,
      isInRange: isNowInRange,
      amount0: derivedAmounts.amount0,
      amount1: derivedAmounts.amount1,
      valueUsd,
      lastInRangeTimestamp: isNowInRange ? timestamp : 0,
      accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
      lastSettledAt: settlement.settledAt,
      settledLpPoints: settlement.newSettledPoints,
      lastUpdate: timestamp,
    });

    if (settlement.pointsEarned > 0n || wasInRange !== isNowInRange) {
      await updateUserEpochLPPoints(
        context,
        position.user_id,
        settlement.pointsEarned,
        timestamp,
        settlement.pointsStartTimestamp,
        settlement.pointsEndTimestamp
      );
    }
    touchedUsers.add(position.user_id);

    totalPositions += 1;
    totalValueUsd += valueUsd;
    if (isNowInRange) {
      inRangePositions += 1;
      inRangeValueUsd += valueUsd;
    }
  }

  setPoolStats(
    context,
    normalizeAddress(pool),
    totalPositions,
    inRangePositions,
    totalValueUsd,
    inRangeValueUsd,
    timestamp
  );

  for (const userId of touchedUsers) {
    await updateUserLPStats(context, userId, timestamp);
  }
}

// ============================================
//     Exports for use in settlement
// ============================================

export {
  getActiveLPPoolConfig,
  getOrCreateLPPoolState,
  getOrCreateLPPoolStats,
  getOrCreateUserLPStats,
  isPositionInRange,
  calculatePositionValueUsd,
  settleLPPoolPositions,
  updatePoolFeeStats,
  updatePoolLPStats,
  updateUserLPStats,
};
