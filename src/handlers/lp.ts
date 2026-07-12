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

import {
  applyCombinedMultiplierScaled,
  calculateAverageCombinedMultiplierBps,
  computeTotalPointsWithMultiplier,
  getOrCreateUser,
  getOrCreateUserEpochStats,
  recordProtocolTransaction,
  refreshUserVotingPowerState,
  updateLifetimePoints,
} from './shared';
import {
  AUSD_ADDRESS,
  BALANCER_AUTORANGE_V3_POOL_ADDRESS,
  BALANCER_VAULT_ADDRESS,
  BASIS_POINTS,
  LP_BALANCER_MAX_SETTLEMENTS_PER_SWAP,
  LP_BALANCER_AUTORANGE_CUTOVER_BLOCK,
  LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  LP_BALANCER_STALE_SETTLEMENT_SECONDS,
  LP_V2_CUTOVER_BLOCK,
  LP_V2_CUTOVER_TIMESTAMP,
  LP_V2_RESUME_CUTOVER_BLOCK,
  LP_V2_RESUME_CUTOVER_TIMESTAMP,
  normalizeAddress,
  POINTS_SCALE,
  SECONDS_PER_DAY,
  USDC_ADDRESS,
  USDT0_ADDRESS,
} from '../helpers/constants';
import { getTestnetBonusBps } from '../helpers/testnetTiers';
import { getAmountsForLiquidity } from '../helpers/uniswapV3';

import {
  BalancerAutoRangePool,
  BalancerVault,
  NonfungiblePositionManager,
  UniswapV2Pair,
  UniswapV3Pool,
} from '../../generated';
import type { LPPoolConfig, handlerContext } from '../../generated';

const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
const DUST_DECIMALS = 18;
const AUSD_DECIMALS_FALLBACK = 6;
const FEE_UNITS_DENOMINATOR = 1_000_000n;
const VOLUME_BUCKET_SECONDS = 3600;
const VOLUME_WINDOW_HOURS = 24;
const DAYS_PER_YEAR = 365n;
const LEGACY_V3_LP_POOL = normalizeAddress('0xd15965968fe8bf2babbe39b2fc5de1ab6749141f');
const LEGACY_V3_LP_POSITION_MANAGER = normalizeAddress(
  '0x7197e214c0b767cfb76fb734ab638e2c192f4e53'
);
const DUST_TOKEN_ADDRESS = normalizeAddress('0xad96c3dffcd6374294e2573a7fbba96097cc8d7c');
const LEGACY_V3_LP_TOKEN0 = AUSD_ADDRESS;
const LEGACY_V3_LP_TOKEN1 = DUST_TOKEN_ADDRESS;
const LEGACY_V3_LP_FEE = 10000;
const V2_LP_POOL = normalizeAddress('0x86dbf00485871c901c5129bd525348db96c2eb2d');
const V2_LP_POSITION_MANAGER = V2_LP_POOL;
const V2_LP_TOKEN0 = USDC_ADDRESS;
const V2_LP_TOKEN1 = DUST_TOKEN_ADDRESS;
const V2_LP_FEE = 3000;
const V2_TICK_LOWER = -887272;
const V2_TICK_UPPER = 887272;
const LEGACY_V3_LP_START_BLOCK = 41231451n;
const BALANCER_AUTORANGE_V3_POOL = normalizeAddress(BALANCER_AUTORANGE_V3_POOL_ADDRESS);
const BALANCER_AUTORANGE_V3_TOKEN0 = USDC_ADDRESS;
const BALANCER_AUTORANGE_V3_TOKEN1 = DUST_TOKEN_ADDRESS;
const BALANCER_AUTORANGE_V3_FEE = 10000;
const WAD = 10n ** 18n;

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

type LPPoolConfigRecord = LPPoolConfig;

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

type LPPoolSettlementCursorRecord = {
  id: string;
  pool: string;
  cursorIndex: number;
  lastSweepTimestamp: number;
  lastUpdate: number;
};

type CutoverStores = {
  LPPoolConfig?: {
    get: (id: string) => Promise<LPPoolConfigRecord | undefined>;
    set: (value: LPPoolConfigRecord) => void;
  };
  LPPoolRegistry?: {
    get: (id: string) => Promise<LPPoolRegistryRecord | undefined>;
    set: (value: { id: string; poolIds: string[]; lastUpdate: number }) => void;
  };
  LPPoolState?: {
    get: (id: string) => Promise<unknown>;
    set: (value: unknown) => void;
  };
  LPPoolV2State?: {
    get: (id: string) => Promise<unknown>;
    set: (value: unknown) => void;
  };
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

function isV2PoolConfig(poolConfig: { pool: string; positionManager: string }): boolean {
  return normalizeAddress(poolConfig.positionManager) === normalizeAddress(poolConfig.pool);
}

function isBalancerAutoRangePool(pool: string): boolean {
  return normalizeAddress(pool) === BALANCER_AUTORANGE_V3_POOL;
}

function isStableUsdToken(token: string): boolean {
  const normalizedToken = normalizeAddress(token);
  return (
    normalizedToken === AUSD_ADDRESS ||
    normalizedToken === USDC_ADDRESS ||
    normalizedToken === USDT0_ADDRESS
  );
}

function isPastLpV2Cutover(timestamp: number, blockNumber?: bigint): boolean {
  if (timestamp >= LP_V2_CUTOVER_TIMESTAMP) return true;
  if (blockNumber === undefined) return false;
  return blockNumber >= BigInt(LP_V2_CUTOVER_BLOCK);
}

function isPastBalancerAutoRangeCutover(timestamp: number, blockNumber?: bigint): boolean {
  if (timestamp >= LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP) return true;
  if (blockNumber === undefined) return false;
  return blockNumber >= BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK);
}

// LP points bounce back from Balancer AutoRange to UniswapV2 at this cutover:
// Balancer stops accruing, V2 resumes as the active pool.
function isPastLpV2ResumeCutover(timestamp: number, blockNumber?: bigint): boolean {
  if (blockNumber !== undefined) {
    return blockNumber >= BigInt(LP_V2_RESUME_CUTOVER_BLOCK);
  }
  return timestamp >= LP_V2_RESUME_CUTOVER_TIMESTAMP;
}

// The accrual window for a pool is derived from its OWN persisted LPPoolConfig
// (isActive / disabledAtTimestamp) -- the exact same single source of truth
// applyStaticLPPoolCutover just wrote using both timestamp AND blockNumber --
// rather than re-derived independently from timestamp alone. Two independent
// boundary checks that can each see a different signal (block height vs.
// wall-clock time -- live under fast/sub-second block production) would
// otherwise be able to disagree at the exact instant a pool's era flips,
// permanently zeroing or double-counting points for that window. This also
// generalizes correctly to any pool with an LPPoolConfig row, not just the
// three statically-cutover ones.
function getPoolAccrualEndTimestamp(poolConfig: LPPoolConfig | null): number | undefined {
  if (!poolConfig || poolConfig.isActive) return undefined;
  return poolConfig.disabledAtTimestamp;
}

function isLegacyV3PoolHardStopped(pool: string, timestamp: number, blockNumber?: bigint): boolean {
  return normalizeAddress(pool) === LEGACY_V3_LP_POOL && isPastLpV2Cutover(timestamp, blockNumber);
}

// During the Balancer window Transfer and Sync keep replay bookkeeping current,
// but V2 remains inactive for points, fees, and protocol transaction accounting.
// Swap has no required replay state and stays fully paused in this interval.
function isV2PoolTrackingOnly(pool: string, timestamp: number, blockNumber?: bigint): boolean {
  return (
    normalizeAddress(pool) === V2_LP_POOL &&
    isPastBalancerAutoRangeCutover(timestamp, blockNumber) &&
    !isPastLpV2ResumeCutover(timestamp, blockNumber)
  );
}

// Balancer AutoRange is hard-stopped for good once the resume cutover passes and
// LP points bounce back to V2.
function isBalancerPoolHardStopped(timestamp: number, blockNumber?: bigint): boolean {
  return isPastLpV2ResumeCutover(timestamp, blockNumber);
}

async function isBalancerResumeTransitionComplete(
  context: handlerContext,
  timestamp: number,
  blockNumber?: bigint
): Promise<boolean> {
  if (!isBalancerPoolHardStopped(timestamp, blockNumber)) return false;
  const config = await context.LPPoolConfig.get(BALANCER_AUTORANGE_V3_POOL);
  return config?.isActive === false;
}

// Balancer AutoRange is the active LP points pool only between the Balancer
// cutover and the resume cutover.
function isBalancerAutoRangeActiveEra(timestamp: number, blockNumber?: bigint): boolean {
  return (
    isPastBalancerAutoRangeCutover(timestamp, blockNumber) &&
    !isPastLpV2ResumeCutover(timestamp, blockNumber)
  );
}

function isLegacyV3ManagerHardStopped(
  positionManager: string,
  timestamp: number,
  blockNumber?: bigint
): boolean {
  return (
    normalizeAddress(positionManager) === LEGACY_V3_LP_POSITION_MANAGER &&
    isPastLpV2Cutover(timestamp, blockNumber)
  );
}

function getV2PositionId(pool: string, userId: string): string {
  return `v2:${normalizeAddress(pool)}:${normalizeAddress(userId)}`;
}

function getSyntheticTokenIdFromAddress(address: string): bigint {
  return BigInt(normalizeAddress(address));
}

async function ensurePoolInRegistry(context: handlerContext, pool: string, timestamp: number) {
  const poolId = normalizeAddress(pool);
  const registry = await context.LPPoolRegistry.get('global');
  const existingPoolIds = registry?.poolIds ?? [];
  if (!existingPoolIds.includes(poolId)) {
    context.LPPoolRegistry.set({
      id: 'global',
      poolIds: [...existingPoolIds, poolId],
      lastUpdate: timestamp,
    });
  }
}

async function ensurePoolState(
  context: handlerContext,
  pool: string,
  timestamp: number,
  overrides?: Partial<{
    currentTick: number;
    sqrtPriceX96: bigint;
    token0Price: bigint;
    token1Price: bigint;
    feeProtocol0: number;
    feeProtocol1: number;
    lastUpdate: number;
  }>
) {
  const poolId = normalizeAddress(pool);
  const existing = await context.LPPoolState.get(poolId);
  if (existing) {
    return existing;
  }

  const state = {
    id: poolId,
    pool: poolId,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: timestamp,
    ...(overrides ?? {}),
  };
  context.LPPoolState.set(state);
  return state;
}

async function getOrCreateLPPoolV2State(context: handlerContext, pool: string, timestamp: number) {
  const poolId = normalizeAddress(pool);
  let state = await context.LPPoolV2State.get(poolId);
  if (!state) {
    state = {
      id: poolId,
      pool: poolId,
      reserve0: 0n,
      reserve1: 0n,
      lpTotalSupply: 0n,
      lastUpdate: timestamp,
    };
    context.LPPoolV2State.set(state);
  }
  return state;
}

async function ensureLegacyV3PoolConfigEntity(context: handlerContext, timestamp: number) {
  let config = await context.LPPoolConfig.get(LEGACY_V3_LP_POOL);
  if (config) return config;

  const leaderboardState = await context.LeaderboardState.get('current');
  const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;
  const globalConfig = await context.LeaderboardConfig.get('global');
  const lpRateBps = globalConfig?.lpRateBps ?? 0n;

  config = {
    id: LEGACY_V3_LP_POOL,
    pool: LEGACY_V3_LP_POOL,
    positionManager: LEGACY_V3_LP_POSITION_MANAGER,
    token0: LEGACY_V3_LP_TOKEN0,
    token1: LEGACY_V3_LP_TOKEN1,
    fee: LEGACY_V3_LP_FEE,
    lpRateBps,
    isActive: true,
    enabledAtEpoch: currentEpoch,
    enabledAtTimestamp: timestamp,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: timestamp,
  };
  context.LPPoolConfig.set(config);

  await ensurePoolInRegistry(context, LEGACY_V3_LP_POOL, timestamp);
  await ensurePoolState(context, LEGACY_V3_LP_POOL, timestamp);

  return config;
}

async function ensureV2PoolConfigEntity(
  context: handlerContext,
  timestamp: number,
  isActive: boolean
) {
  const leaderboardState = await context.LeaderboardState.get('current');
  const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;
  const globalConfig = await context.LeaderboardConfig.get('global');
  const lpRateBps = globalConfig?.lpRateBps ?? 0n;

  const existing = await context.LPPoolConfig.get(V2_LP_POOL);
  const config = {
    id: V2_LP_POOL,
    pool: V2_LP_POOL,
    positionManager: V2_LP_POSITION_MANAGER,
    token0: V2_LP_TOKEN0,
    token1: V2_LP_TOKEN1,
    fee: V2_LP_FEE,
    lpRateBps: existing?.lpRateBps ?? lpRateBps,
    isActive,
    enabledAtEpoch: existing?.enabledAtEpoch ?? currentEpoch,
    enabledAtTimestamp: existing?.enabledAtTimestamp ?? LP_V2_CUTOVER_TIMESTAMP,
    disabledAtEpoch: isActive ? undefined : (existing?.disabledAtEpoch ?? currentEpoch),
    disabledAtTimestamp: isActive
      ? undefined
      : (existing?.disabledAtTimestamp ?? LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP),
    lastUpdate: timestamp,
  };
  context.LPPoolConfig.set(config);
  await ensurePoolInRegistry(context, V2_LP_POOL, timestamp);
  await ensurePoolState(context, V2_LP_POOL, timestamp, {
    currentTick: 0,
    sqrtPriceX96: 0n,
  });
  await getOrCreateLPPoolV2State(context, V2_LP_POOL, timestamp);
  return config;
}

async function ensureBalancerAutoRangePoolConfigEntity(
  context: handlerContext,
  timestamp: number,
  isActive: boolean
) {
  const leaderboardState = await context.LeaderboardState.get('current');
  const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;
  const globalConfig = await context.LeaderboardConfig.get('global');
  const lpRateBps = globalConfig?.lpRateBps ?? 0n;

  const existing = await context.LPPoolConfig.get(BALANCER_AUTORANGE_V3_POOL);
  const config = {
    id: BALANCER_AUTORANGE_V3_POOL,
    pool: BALANCER_AUTORANGE_V3_POOL,
    positionManager: BALANCER_AUTORANGE_V3_POOL,
    token0: BALANCER_AUTORANGE_V3_TOKEN0,
    token1: BALANCER_AUTORANGE_V3_TOKEN1,
    fee: BALANCER_AUTORANGE_V3_FEE,
    lpRateBps: existing?.lpRateBps ?? lpRateBps,
    isActive,
    enabledAtEpoch: existing?.enabledAtEpoch ?? currentEpoch,
    enabledAtTimestamp: existing?.enabledAtTimestamp ?? LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    disabledAtEpoch: isActive ? undefined : existing?.disabledAtEpoch,
    disabledAtTimestamp: isActive ? undefined : existing?.disabledAtTimestamp,
    lastUpdate: timestamp,
  };
  context.LPPoolConfig.set(config);
  await ensurePoolInRegistry(context, BALANCER_AUTORANGE_V3_POOL, timestamp);
  await ensurePoolState(context, BALANCER_AUTORANGE_V3_POOL, timestamp, {
    currentTick: 0,
    sqrtPriceX96: 0n,
  });
  await getOrCreateLPPoolV2State(context, BALANCER_AUTORANGE_V3_POOL, timestamp);
  return config;
}

export async function applyStaticLPPoolCutover(
  context: handlerContext,
  timestamp: number,
  blockNumber?: bigint
) {
  const stores = context as unknown as CutoverStores;
  if (
    !stores.LPPoolConfig ||
    !stores.LPPoolRegistry ||
    !stores.LPPoolState ||
    !stores.LPPoolV2State
  ) {
    return;
  }

  if (blockNumber !== undefined && blockNumber < LEGACY_V3_LP_START_BLOCK) {
    return;
  }

  const registry = await stores.LPPoolRegistry.get('global');
  const trackedPoolIds = registry?.poolIds ?? [];
  const hasTrackedPools = trackedPoolIds.length > 0;
  const hasStaticPoolInRegistry =
    trackedPoolIds.includes(LEGACY_V3_LP_POOL) ||
    trackedPoolIds.includes(V2_LP_POOL) ||
    trackedPoolIds.includes(BALANCER_AUTORANGE_V3_POOL);
  const legacyConfig = await stores.LPPoolConfig.get(LEGACY_V3_LP_POOL);
  const v2Config = await stores.LPPoolConfig.get(V2_LP_POOL);
  const balancerConfig = await stores.LPPoolConfig.get(BALANCER_AUTORANGE_V3_POOL);
  const shouldBootstrapStaticPools =
    blockNumber !== undefined ||
    !hasTrackedPools ||
    hasStaticPoolInRegistry ||
    legacyConfig !== undefined ||
    v2Config !== undefined ||
    balancerConfig !== undefined;

  if (!shouldBootstrapStaticPools) {
    return;
  }
  const ensuredLegacyConfig = await ensureLegacyV3PoolConfigEntity(context, timestamp);
  const hasPassedV2Cutover = isPastLpV2Cutover(timestamp, blockNumber);
  const hasPassedBalancerCutover = isPastBalancerAutoRangeCutover(timestamp, blockNumber);
  const hasPassedV2ResumeCutover = isPastLpV2ResumeCutover(timestamp, blockNumber);
  if (!hasPassedV2Cutover) {
    if (!ensuredLegacyConfig.isActive) {
      context.LPPoolConfig.set({
        ...ensuredLegacyConfig,
        isActive: true,
        disabledAtEpoch: undefined,
        disabledAtTimestamp: undefined,
        lastUpdate: timestamp,
      });
    }
    return;
  }

  // V2 is active in its original era (pre-Balancer) and again once resumed;
  // it's only inactive while Balancer AutoRange is the active pool.
  const v2ShouldBeActive =
    !hasPassedBalancerCutover || hasPassedV2ResumeCutover || v2Config?.isActive !== false;
  const ensuredV2Config = await ensureV2PoolConfigEntity(context, timestamp, v2ShouldBeActive);

  // On reactivation, bump enabledAtTimestamp to the resume boundary. Positions
  // left over from the pre-Balancer era keep whatever stale lastInRangeTimestamp
  // they were frozen at when V2 was disabled; settleLPPosition floors accrual at
  // Math.max(..., poolConfig.enabledAtTimestamp), so without this bump those
  // positions would silently accrue phantom points for the entire paused window.
  if (hasPassedV2ResumeCutover && v2Config?.isActive === false && ensuredV2Config.isActive) {
    const leaderboardState = await context.LeaderboardState.get('current');
    const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;
    context.LPPoolConfig.set({
      ...ensuredV2Config,
      enabledAtEpoch: currentEpoch,
      enabledAtTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
      disabledAtEpoch: undefined,
      disabledAtTimestamp: undefined,
      lastUpdate: timestamp,
    });
  }

  if (ensuredLegacyConfig.isActive) {
    await settleLPPoolPositions(context, LEGACY_V3_LP_POOL, LP_V2_CUTOVER_TIMESTAMP);
    const leaderboardState = await context.LeaderboardState.get('current');
    const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;
    context.LPPoolConfig.set({
      ...ensuredLegacyConfig,
      isActive: false,
      disabledAtEpoch: currentEpoch,
      disabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
      lastUpdate: timestamp,
    });
  }

  if (!hasPassedBalancerCutover) {
    return;
  }

  // V2 -> Balancer transition: settle and disable V2 exactly once. `v2ShouldBeActive`
  // stays true on the first post-cutover call (bridged from the pre-transition state
  // via the OR above), so this fires precisely then; it must NOT fire again once V2
  // has resumed (era D), where `ensuredV2Config.isActive` is also true but for good.
  if (ensuredV2Config.isActive && !hasPassedV2ResumeCutover) {
    await settleLPPoolPositions(context, V2_LP_POOL, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
    const leaderboardState = await context.LeaderboardState.get('current');
    const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;
    context.LPPoolConfig.set({
      ...ensuredV2Config,
      isActive: false,
      disabledAtEpoch: currentEpoch,
      disabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      lastUpdate: timestamp,
    });
  }

  // Balancer is active only between the Balancer cutover and the resume cutover.
  // On the transition call this stays bridged to true (old persisted state) so
  // the settle-on-disable below still sees an active pool; subsequent era-D
  // calls see the persisted false and stay stable, same pattern as V2 above.
  const balancerShouldBeActive = !hasPassedV2ResumeCutover || balancerConfig?.isActive !== false;
  const ensuredBalancerConfig = await ensureBalancerAutoRangePoolConfigEntity(
    context,
    timestamp,
    balancerShouldBeActive
  );

  if (!hasPassedV2ResumeCutover) {
    return;
  }

  // Balancer -> V2 (resume) transition: settle and disable Balancer exactly once,
  // the moment it flips from active to inactive.
  if (ensuredBalancerConfig.isActive) {
    await settleLPPoolPositions(
      context,
      BALANCER_AUTORANGE_V3_POOL,
      LP_V2_RESUME_CUTOVER_TIMESTAMP
    );
    const leaderboardState = await context.LeaderboardState.get('current');
    const currentEpoch = leaderboardState?.currentEpochNumber ?? 1n;
    context.LPPoolConfig.set({
      ...ensuredBalancerConfig,
      isActive: false,
      disabledAtEpoch: currentEpoch,
      disabledAtTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
      lastUpdate: timestamp,
    });
  }
}

async function ensureHardcodedPoolConfig(context: handlerContext, timestamp: number) {
  const config = await ensureLegacyV3PoolConfigEntity(context, timestamp);
  return config.isActive ? config : null;
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
  poolConfig: LPPoolConfig,
  timestamp: number,
  blockNumber?: bigint
): Promise<number | null> {
  void context;
  void timestamp;
  void blockNumber;
  if (poolConfig.fee !== undefined) {
    return poolConfig.fee;
  }
  return null;
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
  void context;
  void userId;
  void timestamp;
  void blockNumber;
  void options;
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
      feeProtocol0: 0,
      feeProtocol1: 0,
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

  // Get current prices and decimals to recalculate position values
  const poolConfig = await getActiveLPPoolConfig(context, poolId);
  const poolState = await context.LPPoolState.get(poolId);
  if (!poolConfig || !poolState) {
    // Fallback to using stored values if we can't get current prices
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
    return;
  }

  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );

  let totalPositions = 0;
  let inRangePositions = 0;
  let totalValueUsd = 0n;
  let inRangeValueUsd = 0n;

  for (const position of positions) {
    if (position.liquidity === 0n && position.amount0 === 0n && position.amount1 === 0n) {
      continue;
    }

    // Recalculate valueUsd with current prices instead of using stale stored value
    const valueUsd = calculatePositionValueUsd(
      position.amount0,
      position.amount1,
      poolState.token0Price,
      poolState.token1Price,
      token0Decimals,
      token1Decimals
    );

    totalPositions += 1;
    totalValueUsd += valueUsd;
    if (position.isInRange) {
      inRangePositions += 1;
      inRangeValueUsd += valueUsd;
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
  // Use average of both sides to match Uniswap's volume calculation
  return (value0 + value1) / 2n;
}

async function updatePoolFeeStats(
  context: handlerContext,
  poolConfig: LPPoolConfig,
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

  // The current bucket contributes the just-written value; the prior in-window buckets
  // are independent reads summed commutatively, so fetch them in one batched round
  // instead of VOLUME_WINDOW_HOURS-1 serial awaits. The set of bucket ids read and the
  // resulting sum are identical to the serial loop, so volumeUsd24h is byte-identical.
  let volumeUsd24h = nextBucketVolume;
  const windowBucketIds: string[] = [];
  for (let i = 1; i < VOLUME_WINDOW_HOURS; i += 1) {
    const start = bucketStart - i * VOLUME_BUCKET_SECONDS;
    if (start < 0) break;
    windowBucketIds.push(`${poolId}:${start}`);
  }
  const windowBuckets = await Promise.all(windowBucketIds.map(id => bucketStore.get(id)));
  for (const windowBucket of windowBuckets) {
    if (windowBucket) {
      volumeUsd24h += windowBucket.volumeUsd;
    }
  }

  const poolFee =
    (await ensurePoolFee(context, poolConfig, timestamp, blockNumber)) ?? poolConfig.fee ?? 0;
  let feesUsd24h = poolFee > 0 ? (volumeUsd24h * BigInt(poolFee)) / FEE_UNITS_DENOMINATOR : 0n;

  // Adjust for protocol fees - if feeProtocol is set, protocol takes 1/feeProtocol of the fees
  // LPs only receive the remainder
  const poolState = await context.LPPoolState.get(poolId);
  const feeProtocol0 = poolState?.feeProtocol0 ?? 0;
  const feeProtocol1 = poolState?.feeProtocol1 ?? 0;

  // Use the higher protocol fee (more conservative for LP APR)
  const maxFeeProtocol = Math.max(feeProtocol0, feeProtocol1);
  if (maxFeeProtocol > 0 && feesUsd24h > 0n) {
    // Protocol takes 1/feeProtocol, LPs get (feeProtocol-1)/feeProtocol
    const lpFeeFraction = BigInt(maxFeeProtocol - 1);
    const totalFraction = BigInt(maxFeeProtocol);
    feesUsd24h = (feesUsd24h * lpFeeFraction) / totalFraction;
  }

  const poolStats = await poolStatsStore?.get?.(poolId);
  const tvlUsd = poolStats?.totalValueUsd ?? 0n;
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
  void timestamp;
  const tokenId = normalizeAddress(tokenAddress);
  const tokenInfo = await context.TokenInfo.get(tokenId);
  if (tokenInfo?.decimals !== undefined && tokenInfo.decimals > 0) {
    return tokenInfo.decimals;
  }

  return tokenInfo?.decimals ?? fallbackDecimals;
}

async function getPoolTokenDecimals(
  context: handlerContext,
  poolConfig: { token0: string; token1: string },
  timestamp?: number
): Promise<{ token0Decimals: number; token1Decimals: number }> {
  const token0Fallback = isStableUsdToken(poolConfig.token0)
    ? AUSD_DECIMALS_FALLBACK
    : DUST_DECIMALS;
  const token1Fallback = isStableUsdToken(poolConfig.token1)
    ? AUSD_DECIMALS_FALLBACK
    : DUST_DECIMALS;
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
  void blockNumber;
  const poolState = await getOrCreateLPPoolState(context, pool, timestamp);
  logLpDebug(context, `[lp] seedPoolStateFromChain skipped (eth calls disabled) pool=${pool}`);
  return poolState;
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

function calculateTokenPriceFromStableReserves(
  stableReserve: bigint,
  tokenReserve: bigint,
  stableDecimals: number,
  tokenDecimals: number
): bigint {
  if (stableReserve <= 0n || tokenReserve <= 0n) return 0n;
  // tokenPriceUsd = stablePriceUsd * (stableReserve / tokenReserve) * 10^(tokenDecimals-stableDecimals)
  const numerator = BigInt(1e8) * stableReserve * 10n ** BigInt(tokenDecimals);
  const denominator = tokenReserve * 10n ** BigInt(stableDecimals);
  if (denominator === 0n) return 0n;
  return numerator / denominator;
}

function calculateV2TokenPricesFromReserves(
  token0: string,
  token1: string,
  reserve0: bigint,
  reserve1: bigint,
  token0Decimals: number,
  token1Decimals: number,
  fallbackToken0Price: bigint,
  fallbackToken1Price: bigint
): { token0Price: bigint; token1Price: bigint } {
  const normalizedToken0 = normalizeAddress(token0);
  const normalizedToken1 = normalizeAddress(token1);
  const token0Stable = isStableUsdToken(normalizedToken0);
  const token1Stable = isStableUsdToken(normalizedToken1);

  if (token0Stable && reserve0 > 0n && reserve1 > 0n) {
    return {
      token0Price: BigInt(1e8),
      token1Price: calculateTokenPriceFromStableReserves(
        reserve0,
        reserve1,
        token0Decimals,
        token1Decimals
      ),
    };
  }

  if (token1Stable && reserve0 > 0n && reserve1 > 0n) {
    return {
      token0Price: calculateTokenPriceFromStableReserves(
        reserve1,
        reserve0,
        token1Decimals,
        token0Decimals
      ),
      token1Price: BigInt(1e8),
    };
  }

  return {
    token0Price: fallbackToken0Price,
    token1Price: fallbackToken1Price,
  };
}

function calculateV2PositionAmounts(
  liquidity: bigint,
  reserve0: bigint,
  reserve1: bigint,
  totalSupply: bigint
): { amount0: bigint; amount1: bigint } {
  if (liquidity <= 0n || reserve0 < 0n || reserve1 < 0n || totalSupply <= 0n) {
    return { amount0: 0n, amount1: 0n };
  }

  return {
    amount0: (reserve0 * liquidity) / totalSupply,
    amount1: (reserve1 * liquidity) / totalSupply,
  };
}

function balancerFeeToPpm(staticSwapFeePercentage: bigint): number {
  if (staticSwapFeePercentage <= 0n) return 0;
  return Number((staticSwapFeePercentage * FEE_UNITS_DENOMINATOR) / WAD);
}

function safeSubtract(value: bigint, delta: bigint): bigint {
  if (delta <= 0n) return value;
  return value > delta ? value - delta : 0n;
}

function isBalancerVault(address: string): boolean {
  return normalizeAddress(address) === normalizeAddress(BALANCER_VAULT_ADDRESS);
}

function getBalancerTokenIndex(poolConfig: { token0: string; token1: string }, token: string) {
  const tokenId = normalizeAddress(token);
  if (tokenId === normalizeAddress(poolConfig.token0)) return 0;
  if (tokenId === normalizeAddress(poolConfig.token1)) return 1;
  return null;
}

async function updateBalancerAutoRangePoolStateFromReserves(
  context: handlerContext,
  poolConfig: LPPoolConfig,
  timestamp: number,
  reserve0: bigint,
  reserve1: bigint,
  lpTotalSupply: bigint,
  fee?: number
): Promise<{
  poolState: Awaited<ReturnType<typeof getOrCreateLPPoolState>>;
  poolV2State: Awaited<ReturnType<typeof getOrCreateLPPoolV2State>>;
}> {
  const pool = normalizeAddress(poolConfig.pool);
  const poolState = await getOrCreateLPPoolState(context, pool, timestamp);
  const poolV2State = await getOrCreateLPPoolV2State(context, pool, timestamp);
  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const nextPrices = calculateV2TokenPricesFromReserves(
    poolConfig.token0,
    poolConfig.token1,
    reserve0,
    reserve1,
    token0Decimals,
    token1Decimals,
    poolState.token0Price,
    poolState.token1Price
  );

  if (fee !== undefined && poolConfig.fee !== fee) {
    context.LPPoolConfig.set({
      ...poolConfig,
      fee,
      lastUpdate: timestamp,
    });
  }

  const nextPoolState = {
    ...poolState,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: nextPrices.token0Price,
    token1Price: nextPrices.token1Price,
    lastUpdate: timestamp,
  };
  const nextPoolV2State = {
    ...poolV2State,
    reserve0,
    reserve1,
    lpTotalSupply,
    lastUpdate: timestamp,
  };
  context.LPPoolState.set(nextPoolState);
  context.LPPoolV2State.set(nextPoolV2State);

  return { poolState: nextPoolState, poolV2State: nextPoolV2State };
}

async function applyBalancerAutoRangeLiquidityDelta(
  context: handlerContext,
  poolConfig: LPPoolConfig,
  timestamp: number,
  amountsRaw: readonly bigint[],
  totalSupply: bigint,
  isAdd: boolean
): Promise<{
  poolState: Awaited<ReturnType<typeof getOrCreateLPPoolState>>;
  poolV2State: Awaited<ReturnType<typeof getOrCreateLPPoolV2State>>;
}> {
  const pool = normalizeAddress(poolConfig.pool);
  const poolV2State = await getOrCreateLPPoolV2State(context, pool, timestamp);
  const amount0 = amountsRaw[0] ?? 0n;
  const amount1 = amountsRaw[1] ?? 0n;
  const reserve0 = isAdd
    ? poolV2State.reserve0 + amount0
    : safeSubtract(poolV2State.reserve0, amount0);
  const reserve1 = isAdd
    ? poolV2State.reserve1 + amount1
    : safeSubtract(poolV2State.reserve1, amount1);

  return await updateBalancerAutoRangePoolStateFromReserves(
    context,
    poolConfig,
    timestamp,
    reserve0,
    reserve1,
    totalSupply
  );
}

async function applyBalancerAutoRangeSwapDelta(
  context: handlerContext,
  poolConfig: LPPoolConfig,
  timestamp: number,
  tokenIn: string,
  tokenOut: string,
  amountIn: bigint,
  amountOut: bigint,
  swapFeePercentage: bigint
) {
  const pool = normalizeAddress(poolConfig.pool);
  const tokenInIndex = getBalancerTokenIndex(poolConfig, tokenIn);
  const tokenOutIndex = getBalancerTokenIndex(poolConfig, tokenOut);
  if (tokenInIndex === null || tokenOutIndex === null || tokenInIndex === tokenOutIndex) {
    return null;
  }

  const poolV2State = await getOrCreateLPPoolV2State(context, pool, timestamp);
  let reserve0 = poolV2State.reserve0;
  let reserve1 = poolV2State.reserve1;
  if (tokenInIndex === 0) {
    reserve0 += amountIn;
  } else {
    reserve1 += amountIn;
  }
  if (tokenOutIndex === 0) {
    reserve0 = safeSubtract(reserve0, amountOut);
  } else {
    reserve1 = safeSubtract(reserve1, amountOut);
  }

  const fee = balancerFeeToPpm(swapFeePercentage);
  return await updateBalancerAutoRangePoolStateFromReserves(
    context,
    poolConfig,
    timestamp,
    reserve0,
    reserve1,
    poolV2State.lpTotalSupply,
    fee
  );
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
  currentTimestamp: number,
  precomputedLpRateBps?: bigint
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

  const poolConfig = await getEffectiveLPPoolConfig(context, position.pool);

  const accrualEndTimestamp = getPoolAccrualEndTimestamp(poolConfig);
  if (accrualEndTimestamp !== undefined) {
    effectiveTimestamp = Math.min(effectiveTimestamp, accrualEndTimestamp);
  }

  let additionalInRangeSeconds = 0n;

  // If position was in range, accumulate the time since last update. Floored
  // by poolConfig.enabledAtTimestamp so a position that sat idle through a
  // paused era (e.g. V2 during the Balancer window) doesn't have that paused
  // span counted as in-range time once its pool reactivates -- mirrors the
  // same floor accrualStart applies to the points calculation below.
  if (position.isInRange && position.lastInRangeTimestamp > 0) {
    const inRangeStart = poolConfig
      ? Math.max(position.lastInRangeTimestamp, poolConfig.enabledAtTimestamp)
      : position.lastInRangeTimestamp;
    const secondsElapsed = effectiveTimestamp - inRangeStart;
    if (secondsElapsed > 0) {
      additionalInRangeSeconds = BigInt(secondsElapsed);
    }
  }

  const newAccumulatedSeconds = position.accumulatedInRangeSeconds + additionalInRangeSeconds;

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
  const accrualStart = Math.max(
    position.lastInRangeTimestamp,
    position.lastSettledAt,
    epochStart,
    poolConfig.enabledAtTimestamp
  );
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

  const effectiveLpRateBps = precomputedLpRateBps ?? poolConfig.lpRateBps;
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

async function getOrCreateLPPoolSettlementCursor(
  context: handlerContext,
  pool: string,
  timestamp: number
) {
  const cursorStore = (
    context as unknown as {
      LPPoolSettlementCursor?: {
        get: (id: string) => Promise<LPPoolSettlementCursorRecord | undefined>;
        set: (value: LPPoolSettlementCursorRecord) => void;
      };
    }
  ).LPPoolSettlementCursor;
  if (!cursorStore) return null;

  const poolId = normalizeAddress(pool);
  let cursor = await cursorStore.get(poolId);
  if (!cursor) {
    cursor = {
      id: poolId,
      pool: poolId,
      cursorIndex: 0,
      lastSweepTimestamp: timestamp,
      lastUpdate: timestamp,
    };
    cursorStore.set(cursor);
  }
  return cursor;
}

async function settleBalancerAutoRangePoolPositionsClockwise(
  context: handlerContext,
  pool: string,
  timestamp: number
): Promise<void> {
  const poolId = normalizeAddress(pool);
  if (!isBalancerAutoRangePool(poolId)) return;

  const poolConfig = await getActiveLPPoolConfig(context, poolId);
  if (!poolConfig) return;

  const cursorStore = (
    context as unknown as {
      LPPoolSettlementCursor?: {
        get: (id: string) => Promise<LPPoolSettlementCursorRecord | undefined>;
        set: (value: LPPoolSettlementCursorRecord) => void;
      };
    }
  ).LPPoolSettlementCursor;
  if (!cursorStore) return;

  const positions = await listPoolLPPositions(context, poolId);
  if (positions.length === 0) {
    cursorStore.set({
      id: poolId,
      pool: poolId,
      cursorIndex: 0,
      lastSweepTimestamp: timestamp,
      lastUpdate: timestamp,
    });
    setPoolStats(context, poolId, 0, 0, 0n, 0n, timestamp);
    return;
  }

  const cursor = await getOrCreateLPPoolSettlementCursor(context, poolId, timestamp);
  if (!cursor) return;

  const poolState = await getOrCreateLPPoolState(context, poolId, timestamp);
  const poolV2State = await getOrCreateLPPoolV2State(context, poolId, timestamp);
  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  // Precompute the pool-local LP rate once for the whole sweep. Every swept
  // position belongs to poolId, so the same canonical rate applies to each.
  const effectiveLpRateBps = poolConfig.lpRateBps;
  const startIndex = cursor.cursorIndex % positions.length;
  const maxScans = Math.min(positions.length, LP_BALANCER_MAX_SETTLEMENTS_PER_SWAP);
  let nextCursorIndex = startIndex;
  const touchedUsers = new Set<string>();

  for (let offset = 0; offset < maxScans; offset += 1) {
    const index = (startIndex + offset) % positions.length;
    nextCursorIndex = (index + 1) % positions.length;
    const position = positions[index];
    if (
      !position ||
      (position.liquidity === 0n && position.amount0 === 0n && position.amount1 === 0n)
    ) {
      continue;
    }
    if (timestamp - position.lastSettledAt < LP_BALANCER_STALE_SETTLEMENT_SECONDS) {
      continue;
    }

    const settlement = await settleLPPosition(context, position, timestamp, effectiveLpRateBps);
    const derivedAmounts = calculateV2PositionAmounts(
      position.liquidity,
      poolV2State.reserve0,
      poolV2State.reserve1,
      poolV2State.lpTotalSupply
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
  }

  cursorStore.set({
    ...cursor,
    cursorIndex: nextCursorIndex,
    lastSweepTimestamp: timestamp,
    lastUpdate: timestamp,
  });

  for (const userId of touchedUsers) {
    await updateUserLPStats(context, userId, timestamp);
  }
  await updatePoolLPStats(context, poolId, timestamp);
}

// ============================================
//     NonfungiblePositionManager Handlers
// ============================================

NonfungiblePositionManager.IncreaseLiquidity.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const positionManager = normalizeAddress(event.srcAddress);
  if (isLegacyV3ManagerHardStopped(positionManager, timestamp, blockNumber)) return;

  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const tokenId = event.params.tokenId;
  const positionId = tokenId.toString();
  const liquidityDelta = BigInt(event.params.liquidity);
  const isHardcodedManager = positionManager === LEGACY_V3_LP_POSITION_MANAGER;
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

    let positionData = null as null | {
      token0: string;
      token1: string;
      fee: number;
      tickLower: number;
      tickUpper: number;
      liquidity: bigint;
    };
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
          mintPool === LEGACY_V3_LP_POOL
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
    existingPool === LEGACY_V3_LP_POOL
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
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const positionManager = normalizeAddress(event.srcAddress);
  if (isLegacyV3ManagerHardStopped(positionManager, timestamp, blockNumber)) return;

  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const tokenId = event.params.tokenId;
  const positionId = tokenId.toString();

  let position = await context.UserLPPosition.get(positionId);
  if (!position) return;

  const decreasePool = normalizeAddress(position.pool);
  const poolConfig =
    decreasePool === LEGACY_V3_LP_POOL
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
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const positionManager = normalizeAddress(event.srcAddress);
  if (isLegacyV3ManagerHardStopped(positionManager, timestamp, blockNumber)) return;

  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const tokenId = event.params.tokenId;
  const positionId = tokenId.toString();
  const from = normalizeAddress(event.params.from);
  const to = normalizeAddress(event.params.to);
  const isHardcodedManager = positionManager === LEGACY_V3_LP_POSITION_MANAGER;
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

    let positionData = null as null | {
      token0: string;
      token1: string;
      fee: number;
      tickLower: number;
      tickUpper: number;
      liquidity: bigint;
    };
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

    // The IncreaseLiquidity handler caches this data keyed by tokenId.
    const pendingMintKey = `pending:${tokenId.toString()}`;
    let mintData = await context.LPMintData.get(pendingMintKey);

    if (!positionData || !poolConfig) {
      // Use LPMintData to reconstruct position data
      if (mintData) {
        const mintPool = normalizeAddress(mintData.pool);
        poolConfig =
          mintPool === LEGACY_V3_LP_POOL
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
      feeProtocol0: poolState.feeProtocol0 ?? 0,
      feeProtocol1: poolState.feeProtocol1 ?? 0,
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
  const blockNumber = BigInt(event.block.number);
  if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) return;
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
  const poolConfig =
    pool === LEGACY_V3_LP_POOL
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
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: timestamp,
  });
});

UniswapV3Pool.Swap.handler(async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) return;
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);

  // Check if this pool is tracked
  const poolConfig =
    pool === LEGACY_V3_LP_POOL
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

  const prevFeeProtocol0 = poolState?.feeProtocol0 ?? 0;
  const prevFeeProtocol1 = poolState?.feeProtocol1 ?? 0;
  context.LPPoolState.set({
    id: pool,
    pool,
    currentTick,
    sqrtPriceX96,
    token0Price,
    token1Price,
    feeProtocol0: prevFeeProtocol0,
    feeProtocol1: prevFeeProtocol1,
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
  await updatePoolFeeStats(context, poolConfig, volumeUsd, timestamp, blockNumber);
});

UniswapV3Pool.SetFeeProtocol.handler(async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) return;
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);

  const poolConfig =
    pool === LEGACY_V3_LP_POOL
      ? await ensureHardcodedPoolConfig(context, timestamp)
      : await getActiveLPPoolConfig(context, pool);
  if (!poolConfig) return;

  const poolState = await context.LPPoolState.get(pool);
  if (!poolState) return;

  const feeProtocol0 = Number(event.params.feeProtocol0New);
  const feeProtocol1 = Number(event.params.feeProtocol1New);

  context.LPPoolState.set({
    ...poolState,
    feeProtocol0,
    feeProtocol1,
    lastUpdate: timestamp,
  });
});

UniswapV3Pool.Mint.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const pool = normalizeAddress(event.srcAddress);
  if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) return;

  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  let poolConfig: Awaited<ReturnType<typeof getActiveLPPoolConfig>> = null;
  if (pool === LEGACY_V3_LP_POOL) {
    poolConfig = await ensureHardcodedPoolConfig(context, timestamp);
  } else {
    poolConfig = await getActiveLPPoolConfig(context, pool);
  }
  if (!poolConfig) return;

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
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const pool = normalizeAddress(event.srcAddress);
  if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) return;

  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  // Burn is handled via DecreaseLiquidity on PositionManager
});

// ============================================
//     UniswapV2Pair Handlers
// ============================================

function getV2TrackingSettlement(
  position: { accumulatedInRangeSeconds: bigint; settledLpPoints: bigint } | undefined,
  timestamp: number
) {
  return {
    newAccumulatedSeconds: position?.accumulatedInRangeSeconds ?? 0n,
    newSettledPoints: position?.settledLpPoints ?? 0n,
    pointsEarned: 0n,
    settledAt: timestamp,
    pointsStartTimestamp: 0,
    pointsEndTimestamp: timestamp,
  };
}

async function applyV2LiquidityDelta(
  context: handlerContext,
  poolConfig: LPPoolConfig,
  userId: string,
  delta: bigint,
  timestamp: number,
  poolState: Awaited<ReturnType<typeof getOrCreateLPPoolState>>,
  poolV2State: Awaited<ReturnType<typeof getOrCreateLPPoolV2State>>,
  token0Decimals: number,
  token1Decimals: number
): Promise<boolean> {
  const normalizedUserId = normalizeAddress(userId);
  if (normalizedUserId === ZERO_ADDRESS || delta === 0n) return false;

  const pool = normalizeAddress(poolConfig.pool);
  const positionId = getV2PositionId(pool, normalizedUserId);
  const existing = await context.UserLPPosition.get(positionId);
  const isInactiveV2Tracking = pool === V2_LP_POOL && !poolConfig.isActive;
  if (!isInactiveV2Tracking) {
    await getOrCreateUser(context, normalizedUserId);
  }

  const settlement =
    existing && poolConfig.isActive
      ? await settleLPPosition(context, existing, timestamp)
      : getV2TrackingSettlement(existing, timestamp);

  if (poolConfig.isActive && existing && settlement.pointsEarned > 0n) {
    await updateUserEpochLPPoints(
      context,
      normalizedUserId,
      settlement.pointsEarned,
      timestamp,
      settlement.pointsStartTimestamp,
      settlement.pointsEndTimestamp
    );
  }

  const previousLiquidity = existing?.liquidity ?? 0n;
  let liquidity = previousLiquidity + delta;
  if (liquidity < 0n) liquidity = 0n;

  const reserve0 = poolV2State.reserve0;
  const reserve1 = poolV2State.reserve1;
  const totalSupply = poolV2State.lpTotalSupply;
  const { amount0, amount1 } = calculateV2PositionAmounts(
    liquidity,
    reserve0,
    reserve1,
    totalSupply
  );
  const valueUsd = calculatePositionValueUsd(
    amount0,
    amount1,
    poolState.token0Price,
    poolState.token1Price,
    token0Decimals,
    token1Decimals
  );
  const isActive = liquidity > 0n;

  context.UserLPPosition.set({
    id: positionId,
    tokenId: getSyntheticTokenIdFromAddress(normalizedUserId),
    user_id: normalizedUserId,
    pool,
    positionManager: pool,
    tickLower: V2_TICK_LOWER,
    tickUpper: V2_TICK_UPPER,
    liquidity,
    amount0,
    amount1,
    isInRange: isActive,
    valueUsd,
    lastInRangeTimestamp: isActive ? timestamp : 0,
    accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
    lastSettledAt: settlement.settledAt,
    settledLpPoints: settlement.newSettledPoints,
    createdAt: existing?.createdAt ?? timestamp,
    lastUpdate: timestamp,
  });

  if (isActive) {
    await addPositionToUserIndex(context, normalizedUserId, positionId, timestamp);
    await addPositionToPoolIndex(context, pool, positionId, timestamp);
  } else {
    await removePositionFromUserIndex(context, normalizedUserId, positionId, timestamp);
    await removePositionFromPoolIndex(context, pool, positionId, timestamp);
  }

  return true;
}

async function settleV2PoolPositions(
  context: handlerContext,
  pool: string,
  timestamp: number
): Promise<void> {
  const poolId = normalizeAddress(pool);
  const poolConfig = await context.LPPoolConfig.get(poolId);
  if (!poolConfig || !isV2PoolConfig(poolConfig)) return;

  const poolState = await getOrCreateLPPoolState(context, poolId, timestamp);
  const poolV2State = await getOrCreateLPPoolV2State(context, poolId, timestamp);
  const reserve0 = poolV2State.reserve0;
  const reserve1 = poolV2State.reserve1;
  const totalSupply = poolV2State.lpTotalSupply;
  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );

  const positions = await listPoolLPPositions(context, poolId);
  if (positions.length === 0) {
    await updatePoolLPStats(context, poolId, timestamp);
    return;
  }

  // Every V2 position in this sweep belongs to poolId, so the pool-local rate is
  // loop-invariant. Tracking-only eras explicitly use zero to suppress points.
  const effectiveLpRateBps = poolConfig.isActive ? poolConfig.lpRateBps : 0n;

  const touchedUsers = new Set<string>();
  for (const position of positions) {
    if (!position.id.startsWith('v2:')) continue;
    if (position.liquidity <= 0n) continue;

    if (poolConfig.isActive) {
      await getOrCreateUser(context, position.user_id);
    }

    const settlement = poolConfig.isActive
      ? await settleLPPosition(context, position, timestamp, effectiveLpRateBps)
      : getV2TrackingSettlement(position, timestamp);
    if (poolConfig.isActive && settlement.pointsEarned > 0n) {
      await updateUserEpochLPPoints(
        context,
        position.user_id,
        settlement.pointsEarned,
        timestamp,
        settlement.pointsStartTimestamp,
        settlement.pointsEndTimestamp
      );
    }

    const { amount0, amount1 } = calculateV2PositionAmounts(
      position.liquidity,
      reserve0,
      reserve1,
      totalSupply
    );
    const valueUsd = calculatePositionValueUsd(
      amount0,
      amount1,
      poolState.token0Price,
      poolState.token1Price,
      token0Decimals,
      token1Decimals
    );

    context.UserLPPosition.set({
      ...position,
      amount0,
      amount1,
      isInRange: true,
      valueUsd,
      lastInRangeTimestamp: timestamp,
      accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
      lastSettledAt: settlement.settledAt,
      settledLpPoints: settlement.newSettledPoints,
      lastUpdate: timestamp,
    });
    touchedUsers.add(position.user_id);
  }

  for (const userId of touchedUsers) {
    await updateUserLPStats(context, userId, timestamp);
  }
  await updatePoolLPStats(context, poolId, timestamp);
}

UniswapV2Pair.Transfer.handler(async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
  const isTrackingOnly = isV2PoolTrackingOnly(pool, timestamp, blockNumber);

  if (!isTrackingOnly) {
    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);
  }

  const poolConfig = isTrackingOnly
    ? await context.LPPoolConfig.get(pool)
    : await getActiveLPPoolConfig(context, pool);
  if (!poolConfig || !isV2PoolConfig(poolConfig)) return;

  const amount = event.params.value;
  if (amount <= 0n) return;

  const from = normalizeAddress(event.params.from);
  const to = normalizeAddress(event.params.to);
  const existingPoolState = await getOrCreateLPPoolState(context, pool, timestamp);
  const existingPoolV2State = await getOrCreateLPPoolV2State(context, pool, timestamp);

  let nextTotalSupply = existingPoolV2State.lpTotalSupply;
  if (from === ZERO_ADDRESS) {
    nextTotalSupply += amount;
  } else if (to === ZERO_ADDRESS) {
    nextTotalSupply = nextTotalSupply > amount ? nextTotalSupply - amount : 0n;
  }

  const nextPoolV2State = {
    ...existingPoolV2State,
    lpTotalSupply: nextTotalSupply,
    reserve0: existingPoolV2State.reserve0,
    reserve1: existingPoolV2State.reserve1,
    lastUpdate: timestamp,
  };
  context.LPPoolV2State.set(nextPoolV2State);
  context.LPPoolState.set({
    ...existingPoolState,
    lastUpdate: timestamp,
  });

  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const touchedUsers = new Set<string>();
  if (from !== ZERO_ADDRESS) {
    const changed = await applyV2LiquidityDelta(
      context,
      poolConfig,
      from,
      -amount,
      timestamp,
      existingPoolState,
      nextPoolV2State,
      token0Decimals,
      token1Decimals
    );
    if (changed) touchedUsers.add(from);
  }

  if (to !== ZERO_ADDRESS) {
    const changed = await applyV2LiquidityDelta(
      context,
      poolConfig,
      to,
      amount,
      timestamp,
      existingPoolState,
      nextPoolV2State,
      token0Decimals,
      token1Decimals
    );
    if (changed) touchedUsers.add(to);
  }

  for (const userId of touchedUsers) {
    await updateUserLPStats(context, userId, timestamp);
  }
  await updatePoolLPStats(context, pool, timestamp);
});

UniswapV2Pair.Swap.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const pool = normalizeAddress(event.srcAddress);
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
  if (isV2PoolTrackingOnly(pool, timestamp, blockNumber)) return;

  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const poolConfig = await getActiveLPPoolConfig(context, pool);
  if (!poolConfig || !isV2PoolConfig(poolConfig)) return;

  const poolState = await getOrCreateLPPoolState(context, pool, timestamp);
  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );

  let token0Price = poolState.token0Price;
  let token1Price = poolState.token1Price;
  if (token0Price === 0n && isStableUsdToken(poolConfig.token0)) {
    token0Price = getAusdPrice();
  }
  if (token1Price === 0n && isStableUsdToken(poolConfig.token1)) {
    token1Price = getAusdPrice();
  }

  const amount0 = event.params.amount0In - event.params.amount0Out;
  const amount1 = event.params.amount1In - event.params.amount1Out;
  const volumeUsd = calculateSwapVolumeUsd(
    amount0,
    amount1,
    token0Price,
    token1Price,
    token0Decimals,
    token1Decimals
  );
  await updatePoolFeeStats(context, poolConfig, volumeUsd, timestamp, blockNumber);
});

UniswapV2Pair.Sync.handler(async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
  const isTrackingOnly = isV2PoolTrackingOnly(pool, timestamp, blockNumber);

  if (!isTrackingOnly) {
    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);
  }

  const poolConfig = isTrackingOnly
    ? await context.LPPoolConfig.get(pool)
    : await getActiveLPPoolConfig(context, pool);
  if (!poolConfig || !isV2PoolConfig(poolConfig)) return;

  const reserve0 = BigInt(event.params.reserve0);
  const reserve1 = BigInt(event.params.reserve1);
  const poolState = await getOrCreateLPPoolState(context, pool, timestamp);
  const poolV2State = await getOrCreateLPPoolV2State(context, pool, timestamp);
  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const nextPrices = calculateV2TokenPricesFromReserves(
    poolConfig.token0,
    poolConfig.token1,
    reserve0,
    reserve1,
    token0Decimals,
    token1Decimals,
    poolState.token0Price,
    poolState.token1Price
  );

  context.LPPoolState.set({
    ...poolState,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: nextPrices.token0Price,
    token1Price: nextPrices.token1Price,
    lastUpdate: timestamp,
  });
  context.LPPoolV2State.set({
    ...poolV2State,
    reserve0,
    reserve1,
    lpTotalSupply: poolV2State.lpTotalSupply,
    lastUpdate: timestamp,
  });

  await settleV2PoolPositions(context, pool, timestamp);
});

// ============================================
//     Balancer AutoRange V3 Pool Handlers
// ============================================

BalancerAutoRangePool.Transfer.handler(async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  if (!isBalancerAutoRangePool(pool)) return;

  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  if (await isBalancerResumeTransitionComplete(context, timestamp, blockNumber)) return;
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
  if (isBalancerPoolHardStopped(timestamp, blockNumber)) return;
  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const poolConfig = await ensureBalancerAutoRangePoolConfigEntity(
    context,
    timestamp,
    isBalancerAutoRangeActiveEra(timestamp, blockNumber)
  );
  const amount = event.params.value;
  if (amount <= 0n) return;

  const from = normalizeAddress(event.params.from);
  const to = normalizeAddress(event.params.to);
  const poolState = await getOrCreateLPPoolState(context, pool, timestamp);
  let poolV2State = await getOrCreateLPPoolV2State(context, pool, timestamp);

  let nextTotalSupply = poolV2State.lpTotalSupply;
  if (from === ZERO_ADDRESS) {
    nextTotalSupply += amount;
  } else if (to === ZERO_ADDRESS) {
    nextTotalSupply = safeSubtract(nextTotalSupply, amount);
  }
  poolV2State = {
    ...poolV2State,
    lpTotalSupply: nextTotalSupply,
    lastUpdate: timestamp,
  };
  context.LPPoolV2State.set(poolV2State);
  context.LPPoolState.set({
    ...poolState,
    lastUpdate: timestamp,
  });

  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const touchedUsers = new Set<string>();
  if (from !== ZERO_ADDRESS) {
    const changed = await applyV2LiquidityDelta(
      context,
      poolConfig,
      from,
      -amount,
      timestamp,
      poolState,
      poolV2State,
      token0Decimals,
      token1Decimals
    );
    if (changed) touchedUsers.add(from);
  }

  if (to !== ZERO_ADDRESS) {
    const changed = await applyV2LiquidityDelta(
      context,
      poolConfig,
      to,
      amount,
      timestamp,
      poolState,
      poolV2State,
      token0Decimals,
      token1Decimals
    );
    if (changed) touchedUsers.add(to);
  }

  for (const userId of touchedUsers) {
    await updateUserLPStats(context, userId, timestamp);
  }
  await updatePoolLPStats(context, pool, timestamp);
});

// The Vault is a singleton emitting for EVERY Balancer V3 pool on Monad. Registering
// it wildcard (no address in config) makes the indexed `pool` topic filter part of the
// HyperSync query itself, so only the DUST AutoRange pool's events are FETCHED — not
// fetched-then-discarded. Envio only pushes topic filters server-side for wildcard
// events; an addressed contract filters client-side and still pays for every pool.
// The in-handler guards below stay as defense-in-depth.
BalancerVault.LiquidityAdded.handler(
  async ({ event, context }) => {
    if (!isBalancerVault(event.srcAddress)) return;

    const pool = normalizeAddress(event.params.pool);
    if (!isBalancerAutoRangePool(pool)) return;

    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    if (await isBalancerResumeTransitionComplete(context, timestamp, blockNumber)) return;
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isBalancerPoolHardStopped(timestamp, blockNumber)) return;
    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

    const isActive = isBalancerAutoRangeActiveEra(timestamp, blockNumber);
    const poolConfig = await ensureBalancerAutoRangePoolConfigEntity(context, timestamp, isActive);
    await applyBalancerAutoRangeLiquidityDelta(
      context,
      poolConfig,
      timestamp,
      event.params.amountsAddedRaw,
      event.params.totalSupply,
      true
    );
    await settleBalancerAutoRangePoolPositionsClockwise(context, pool, timestamp);
  },
  { eventFilters: { pool: BALANCER_AUTORANGE_V3_POOL } }
);

BalancerVault.LiquidityRemoved.handler(
  async ({ event, context }) => {
    if (!isBalancerVault(event.srcAddress)) return;

    const pool = normalizeAddress(event.params.pool);
    if (!isBalancerAutoRangePool(pool)) return;

    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    if (await isBalancerResumeTransitionComplete(context, timestamp, blockNumber)) return;
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isBalancerPoolHardStopped(timestamp, blockNumber)) return;
    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

    const isActive = isBalancerAutoRangeActiveEra(timestamp, blockNumber);
    const poolConfig = await ensureBalancerAutoRangePoolConfigEntity(context, timestamp, isActive);
    await applyBalancerAutoRangeLiquidityDelta(
      context,
      poolConfig,
      timestamp,
      event.params.amountsRemovedRaw,
      event.params.totalSupply,
      false
    );
    await settleBalancerAutoRangePoolPositionsClockwise(context, pool, timestamp);
  },
  { eventFilters: { pool: BALANCER_AUTORANGE_V3_POOL } }
);

BalancerVault.Swap.handler(
  async ({ event, context }) => {
    if (!isBalancerVault(event.srcAddress)) return;

    const pool = normalizeAddress(event.params.pool);
    if (!isBalancerAutoRangePool(pool)) return;

    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    if (await isBalancerResumeTransitionComplete(context, timestamp, blockNumber)) return;
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isBalancerPoolHardStopped(timestamp, blockNumber)) return;
    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

    const isActive = isBalancerAutoRangeActiveEra(timestamp, blockNumber);
    const poolConfig = await ensureBalancerAutoRangePoolConfigEntity(context, timestamp, isActive);
    const updatedState = await applyBalancerAutoRangeSwapDelta(
      context,
      poolConfig,
      timestamp,
      event.params.tokenIn,
      event.params.tokenOut,
      event.params.amountIn,
      event.params.amountOut,
      event.params.swapFeePercentage
    );
    if (!updatedState) return;

    if (isActive) {
      const tokenInIndex = getBalancerTokenIndex(poolConfig, event.params.tokenIn);
      const tokenOutIndex = getBalancerTokenIndex(poolConfig, event.params.tokenOut);
      if (tokenInIndex !== null && tokenOutIndex !== null) {
        const amount0 =
          (tokenInIndex === 0 ? event.params.amountIn : 0n) -
          (tokenOutIndex === 0 ? event.params.amountOut : 0n);
        const amount1 =
          (tokenInIndex === 1 ? event.params.amountIn : 0n) -
          (tokenOutIndex === 1 ? event.params.amountOut : 0n);
        const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
          context,
          poolConfig,
          timestamp
        );
        const currentConfig = await context.LPPoolConfig.get(poolConfig.id);
        await updatePoolFeeStats(
          context,
          currentConfig ?? poolConfig,
          calculateSwapVolumeUsd(
            amount0,
            amount1,
            updatedState.poolState.token0Price,
            updatedState.poolState.token1Price,
            token0Decimals,
            token1Decimals
          ),
          timestamp
        );
      }
    }

    await settleBalancerAutoRangePoolPositionsClockwise(context, pool, timestamp);
  },
  { eventFilters: { pool: BALANCER_AUTORANGE_V3_POOL } }
);

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
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
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

    const poolState = isBalancerAutoRangePool(poolId)
      ? await getOrCreateLPPoolState(context, poolId, timestamp)
      : await seedPoolStateFromChain(context, poolId, timestamp, blockNumber);
    const poolV2State = isV2PoolConfig(poolConfig)
      ? await getOrCreateLPPoolV2State(context, poolId, timestamp)
      : null;
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
      const isNowInRange = poolV2State
        ? position.liquidity > 0n
        : isPositionInRange(position.tickLower, position.tickUpper, poolState.currentTick);

      const settlement = await settleLPPosition(context, position, timestamp);
      const derivedAmounts = poolV2State
        ? {
            ...calculateV2PositionAmounts(
              position.liquidity,
              poolV2State.reserve0,
              poolV2State.reserve1,
              poolV2State.lpTotalSupply
            ),
            usedLiquidity: false,
          }
        : derivePositionAmounts(
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
  const poolV2State = isV2PoolConfig(poolConfig)
    ? await getOrCreateLPPoolV2State(context, pool, timestamp)
    : null;
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
    const derivedAmounts = poolV2State
      ? {
          ...calculateV2PositionAmounts(
            position.liquidity,
            poolV2State.reserve0,
            poolV2State.reserve1,
            poolV2State.lpTotalSupply
          ),
          usedLiquidity: false,
        }
      : derivePositionAmounts(
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
  await applyStaticLPPoolCutover(context, timestamp);
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
  isPastLpV2ResumeCutover,
  isPositionInRange,
  calculatePositionValueUsd,
  settleLPPosition,
  settleLPPoolPositions,
  settleV2PoolPositions,
  updatePoolFeeStats,
  updatePoolLPStats,
  updateUserLPStats,
};
