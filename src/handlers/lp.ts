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
  applyScheduledEpochTransitions,
  recordProtocolTransaction,
  refreshUserVotingPowerState,
  updateLifetimePoints,
} from './shared';
import {
  AUSD_ADDRESS,
  BALANCER_AUTORANGE_V3_FEE,
  BALANCER_AUTORANGE_V3_POOL,
  BALANCER_AUTORANGE_V3_TOKEN0,
  BALANCER_AUTORANGE_V3_TOKEN1,
  BALANCER_TO_V2_RESUME_TRANSITION,
  BALANCER_VAULT_ADDRESS,
  BASIS_POINTS,
  DAYS_PER_YEAR,
  DUST_TOKEN_ADDRESS,
  FEE_UNITS_DENOMINATOR,
  isPointAccrualBlacklisted,
  LEGACY_V3_LP_FEE,
  LEGACY_V3_LP_POOL,
  LEGACY_V3_LP_POSITION_MANAGER,
  LEGACY_V3_LP_START_BLOCK,
  LEGACY_V3_LP_TOKEN0,
  LEGACY_V3_LP_TOKEN1,
  LEGACY_V3_TO_V2_TRANSITION,
  LP_BALANCER_AUTORANGE_CUTOVER_BLOCK,
  LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  LP_V2_CUTOVER_BLOCK,
  LP_V2_CUTOVER_TIMESTAMP,
  LP_V2_RESUME_CUTOVER_BLOCK,
  LP_V2_RESUME_CUTOVER_TIMESTAMP,
  normalizeAddress,
  POINTS_SCALE,
  SECONDS_PER_DAY,
  USDC_ADDRESS,
  V2_LP_FEE,
  V2_LP_POOL,
  V2_LP_POSITION_MANAGER,
  V2_LP_TOKEN0,
  V2_LP_TOKEN1,
  V2_TICK_LOWER,
  V2_TICK_UPPER,
  V2_TO_BALANCER_TRANSITION,
  VOLUME_BUCKET_SECONDS,
  VOLUME_WINDOW_HOURS,
  ZERO_ADDRESS,
} from '../helpers/constants';
import { pow10 } from '../helpers/math';
import { getTestnetBonusBps } from '../helpers/testnetTiers';
import { getAmountsForLiquidity } from '../helpers/uniswapV3';
import { isPrefilledTimestamp, setUserEpochStats } from '../helpers/prefill';
import {
  getLPPoolTokenDecimals as getPoolTokenDecimals,
  isFungibleLPPoolConfig as isV2PoolConfig,
  isStableUsdToken,
} from './lpEntityHelpers';
import {
  advanceLPPoolGrowth,
  lpPoolEpochGrowthId,
  resetLPPositionGrowthBaseline,
  settleLPPositionGrowthAfterPoolAdvance,
} from './lpGrowth';

import type { LPStaticTransitionRecord } from '../helpers/constants';
import type { LPPoolConfig, UserLPPosition, EvmOnEventContext as handlerContext } from 'envio';
import { indexer } from './registry';
const WAD = 10n ** 18n;
// Loop-invariant: a 193-bit exponentiation that was rebuilt on every price update.
const Q192 = 2n ** 192n;

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

function assertCanonicalStaticTransitionMarker(
  marker: LPStaticTransitionRecord | undefined,
  expected: LPStaticTransitionRecord
): void {
  if (!marker) return;
  const isCanonical =
    marker.id === expected.id &&
    normalizeAddress(marker.outgoingPool) === expected.outgoingPool &&
    normalizeAddress(marker.incomingPool) === expected.incomingPool &&
    marker.blockNumber === expected.blockNumber &&
    marker.timestamp === expected.timestamp;
  if (isCanonical) return;

  throw new Error(
    `invalid LP static transition marker ${expected.id}: ` +
      `expected ${expected.outgoingPool}->${expected.incomingPool} ` +
      `at block ${expected.blockNumber} timestamp ${expected.timestamp}; ` +
      `received ${marker.outgoingPool}->${marker.incomingPool} ` +
      `at block ${marker.blockNumber} timestamp ${marker.timestamp}`
  );
}

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
  LPStaticTransition?: {
    get: (id: string) => Promise<LPStaticTransitionRecord | undefined>;
    set: (value: LPStaticTransitionRecord) => void;
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

function isBalancerAutoRangePool(pool: string): boolean {
  return normalizeAddress(pool) === BALANCER_AUTORANGE_V3_POOL;
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

export type StaticLPPoolEraState = {
  isActive: boolean;
  enabledAtTimestamp: number;
  disabledAtTimestamp?: number;
};

/** Canonical event-time ownership for pools whose points eras are static. */
export function getStaticLPPoolEraState(
  pool: string,
  timestamp: number,
  blockNumber?: bigint
): StaticLPPoolEraState | undefined {
  const poolId = normalizeAddress(pool);
  if (poolId === LEGACY_V3_LP_POOL) {
    if (blockNumber !== undefined && blockNumber < LEGACY_V3_LP_START_BLOCK) {
      return;
    }
    return isPastLpV2Cutover(timestamp, blockNumber)
      ? {
          isActive: false,
          enabledAtTimestamp: timestamp,
          disabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
        }
      : {
          isActive: true,
          enabledAtTimestamp: timestamp,
        };
  }
  if (poolId === BALANCER_AUTORANGE_V3_POOL) {
    if (!isPastBalancerAutoRangeCutover(timestamp, blockNumber)) {
      return {
        isActive: false,
        enabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      };
    }
    if (isPastLpV2ResumeCutover(timestamp, blockNumber)) {
      return {
        isActive: false,
        enabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
        disabledAtTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
      };
    }
    return {
      isActive: true,
      enabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    };
  }
  if (poolId !== V2_LP_POOL) return;
  if (!isPastLpV2Cutover(timestamp, blockNumber)) {
    return {
      isActive: false,
      enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    };
  }
  if (isPastLpV2ResumeCutover(timestamp, blockNumber)) {
    return {
      isActive: true,
      enabledAtTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
    };
  }
  if (isPastBalancerAutoRangeCutover(timestamp, blockNumber)) {
    return {
      isActive: false,
      enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
      disabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    };
  }
  return {
    isActive: true,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
  };
}

/** Preload reads needed when same-event static bootstrap writes are intentionally no-op. */
export async function preloadStaticLPPoolFreezeProjection(
  context: handlerContext,
  epochNumbers: readonly bigint[]
): Promise<void> {
  const staticPools = [LEGACY_V3_LP_POOL, V2_LP_POOL, BALANCER_AUTORANGE_V3_POOL];
  const staticTokens = [AUSD_ADDRESS, USDC_ADDRESS, DUST_TOKEN_ADDRESS];
  const uniqueEpochNumbers = [...new Set(epochNumbers)];
  // advanceLPPoolGrowthForEpoch reads LPPoolState for fungible pools too, so these
  // reads must stay in the preload set or the preload and ordered read-sets diverge.
  await Promise.all(staticPools.map(pool => context.LPPoolState.get(pool)));

  // A single event can apply several static boundaries before its final Tide
  // projection. Envio preload discards the config/registry writes between
  // those boundaries, so final-event era ownership cannot identify which
  // static pool ordered processing freezes for an earlier Tide. Preload the
  // complete constant-size static dependency set for each projected close:
  // three pools and at most five Tides.
  await Promise.all([
    ...uniqueEpochNumbers.map(epochNumber => context.LeaderboardEpoch.get(epochNumber.toString())),
    ...staticTokens.map(token => context.TokenInfo.get(token)),
    ...staticPools.flatMap(pool => [
      context.LPPoolConfig.get(pool),
      context.LPPoolV2State.get(pool),
      ...uniqueEpochNumbers.map(epochNumber =>
        context.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(pool, epochNumber))
      ),
    ]),
  ]);
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

async function ensureLegacyV3PoolConfigEntity(
  context: handlerContext,
  timestamp: number,
  isActive = true
) {
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
    isActive,
    enabledAtEpoch: currentEpoch,
    enabledAtTimestamp: timestamp,
    disabledAtEpoch: isActive ? undefined : currentEpoch,
    disabledAtTimestamp: isActive ? undefined : LP_V2_CUTOVER_TIMESTAMP,
    lastUpdate: timestamp,
  };
  context.LPPoolConfig.set(config);

  await ensurePoolInRegistry(context, LEGACY_V3_LP_POOL, timestamp);
  await ensurePoolState(context, LEGACY_V3_LP_POOL, timestamp);

  return config;
}

/**
 * Whether a rebuilt pool config differs from the stored row in any field that
 * carries meaning.
 *
 * `lastUpdate` is deliberately excluded. These two ensure* helpers run on every
 * event past their cutover, and rewriting an otherwise identical row purely to
 * advance `lastUpdate` costs a write plus an entity-history row on every event
 * across the whole replay. Nothing reads `LPPoolConfig.lastUpdate` - not the
 * indexer, not the scripts, and not the app, which selects only pool, token0,
 * token1, fee and lpRateBps - so it now marks the last time the config actually
 * changed rather than the last time any event happened to touch it.
 */
function lpPoolConfigChanged(
  existing: LPPoolConfigRecord | undefined,
  next: LPPoolConfigRecord
): boolean {
  if (!existing) return true;
  return (
    existing.pool !== next.pool ||
    existing.positionManager !== next.positionManager ||
    existing.token0 !== next.token0 ||
    existing.token1 !== next.token1 ||
    existing.fee !== next.fee ||
    existing.lpRateBps !== next.lpRateBps ||
    existing.isActive !== next.isActive ||
    existing.enabledAtEpoch !== next.enabledAtEpoch ||
    existing.enabledAtTimestamp !== next.enabledAtTimestamp ||
    existing.disabledAtEpoch !== next.disabledAtEpoch ||
    existing.disabledAtTimestamp !== next.disabledAtTimestamp
  );
}

async function ensureV2PoolConfigEntity(
  context: handlerContext,
  timestamp: number,
  isActive: boolean,
  enabledAtTimestamp?: number
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
    enabledAtEpoch:
      isActive && enabledAtTimestamp !== undefined
        ? currentEpoch
        : (existing?.enabledAtEpoch ?? currentEpoch),
    enabledAtTimestamp:
      isActive && enabledAtTimestamp !== undefined
        ? enabledAtTimestamp
        : (existing?.enabledAtTimestamp ?? LP_V2_CUTOVER_TIMESTAMP),
    disabledAtEpoch: isActive ? undefined : (existing?.disabledAtEpoch ?? currentEpoch),
    disabledAtTimestamp: isActive
      ? undefined
      : (existing?.disabledAtTimestamp ?? LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP),
    lastUpdate: timestamp,
  };
  // Unchanged rows are returned as stored rather than rewritten, so nothing
  // mutates the freshly built literal after the fact.
  let stored: LPPoolConfigRecord = config;
  if (lpPoolConfigChanged(existing, config)) {
    context.LPPoolConfig.set(config);
  } else if (existing) {
    stored = existing;
  }
  await ensurePoolInRegistry(context, V2_LP_POOL, timestamp);
  await ensurePoolState(context, V2_LP_POOL, timestamp, {
    currentTick: 0,
    sqrtPriceX96: 0n,
  });
  await getOrCreateLPPoolV2State(context, V2_LP_POOL, timestamp);
  return stored;
}

async function ensureBalancerAutoRangePoolConfigEntity(
  context: handlerContext,
  timestamp: number,
  isActive: boolean,
  enabledAtTimestamp?: number
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
    enabledAtEpoch:
      isActive && enabledAtTimestamp !== undefined
        ? currentEpoch
        : (existing?.enabledAtEpoch ?? currentEpoch),
    enabledAtTimestamp:
      isActive && enabledAtTimestamp !== undefined
        ? enabledAtTimestamp
        : (existing?.enabledAtTimestamp ?? LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP),
    disabledAtEpoch: isActive ? undefined : existing?.disabledAtEpoch,
    disabledAtTimestamp: isActive ? undefined : existing?.disabledAtTimestamp,
    lastUpdate: timestamp,
  };
  // Unchanged rows are returned as stored rather than rewritten, so nothing
  // mutates the freshly built literal after the fact.
  let stored: LPPoolConfigRecord = config;
  if (lpPoolConfigChanged(existing, config)) {
    context.LPPoolConfig.set(config);
  } else if (existing) {
    stored = existing;
  }
  await ensurePoolInRegistry(context, BALANCER_AUTORANGE_V3_POOL, timestamp);
  await ensurePoolState(context, BALANCER_AUTORANGE_V3_POOL, timestamp, {
    currentTick: 0,
    sqrtPriceX96: 0n,
  });
  await getOrCreateLPPoolV2State(context, BALANCER_AUTORANGE_V3_POOL, timestamp);
  return stored;
}

async function disableStaticLPPoolAt(
  context: handlerContext,
  config: LPPoolConfigRecord,
  disabledAtTimestamp: number,
  timestamp: number
): Promise<LPPoolConfigRecord> {
  if (!config.isActive) return config;
  const leaderboardState = await context.LeaderboardState.get('current');
  const next = {
    ...config,
    isActive: false,
    disabledAtEpoch: leaderboardState?.currentEpochNumber ?? 1n,
    disabledAtTimestamp,
    lastUpdate: timestamp,
  };
  if (!lpPoolConfigChanged(config, next)) return config;
  context.LPPoolConfig.set(next);
  return next;
}

function prepareExactBoundaryOutgoingPool(
  context: handlerContext,
  config: LPPoolConfigRecord,
  boundaryTimestamp: number,
  timestamp: number
): LPPoolConfigRecord {
  if (config.isActive || config.disabledAtTimestamp !== boundaryTimestamp) {
    return config;
  }
  const active = {
    ...config,
    isActive: true,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: timestamp,
  };
  context.LPPoolConfig.set(active);
  return active;
}

async function getCurrentLPPoolGrowth(context: handlerContext, pool: string) {
  const state = await context.LeaderboardState.get('current');
  if (!state?.isActive || state.currentEpochNumber === 0n) return;
  return await context.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(pool, state.currentEpochNumber));
}

async function assertFirstActivationGrowthIsEmpty(
  context: handlerContext,
  pool: string
): Promise<void> {
  const growth = await getCurrentLPPoolGrowth(context, pool);
  if (!growth) return;
  if (growth.scalarGrowthX128 !== 0n) {
    throw new Error(
      `cannot activate LP pool with nonzero pre-era LP growth: pool=${normalizeAddress(pool)} scalar=${growth.scalarGrowthX128.toString()}`
    );
  }
}

async function hideIncomingPoolBeforeBoundary(
  context: handlerContext,
  config: LPPoolConfigRecord | undefined,
  boundaryTimestamp: number
): Promise<LPPoolConfigRecord | undefined> {
  if (!config?.isActive) return config;
  const inactive = {
    ...config,
    isActive: false,
    enabledAtTimestamp: boundaryTimestamp,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: boundaryTimestamp,
  };
  context.LPPoolConfig.set(inactive);
  return inactive;
}

async function rebaseEmptyGrowthForFirstActivation(
  context: handlerContext,
  pool: string,
  boundaryTimestamp: number
): Promise<void> {
  const state = await context.LeaderboardState.get('current');
  if (!state?.isActive || state.currentEpochNumber === 0n) return;
  const growth = await context.LPPoolEpochGrowth.get(
    lpPoolEpochGrowthId(pool, state.currentEpochNumber)
  );
  if (!growth) return;
  await assertFirstActivationGrowthIsEmpty(context, pool);
  const epoch = await context.LeaderboardEpoch.get(state.currentEpochNumber.toString());
  const effectiveStart = Math.max(epoch?.startTime ?? boundaryTimestamp, boundaryTimestamp);
  context.LPPoolEpochGrowth.set({
    ...growth,
    startTimestamp: effectiveStart,
    lastTimestamp: effectiveStart,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: effectiveStart,
  });
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

  const legacyV3ToV2Marker = await stores.LPStaticTransition?.get(LEGACY_V3_TO_V2_TRANSITION.id);
  assertCanonicalStaticTransitionMarker(legacyV3ToV2Marker, LEGACY_V3_TO_V2_TRANSITION);
  const v2ToBalancerMarker = await stores.LPStaticTransition?.get(V2_TO_BALANCER_TRANSITION.id);
  assertCanonicalStaticTransitionMarker(v2ToBalancerMarker, V2_TO_BALANCER_TRANSITION);
  const balancerToV2ResumeMarker = await stores.LPStaticTransition?.get(
    BALANCER_TO_V2_RESUME_TRANSITION.id
  );
  assertCanonicalStaticTransitionMarker(balancerToV2ResumeMarker, BALANCER_TO_V2_RESUME_TRANSITION);

  const registry = await stores.LPPoolRegistry.get('global');
  const trackedPoolIds = registry?.poolIds ?? [];
  const hasTrackedPools = trackedPoolIds.length > 0;
  const hasStaticPoolInRegistry =
    trackedPoolIds.includes(LEGACY_V3_LP_POOL) ||
    trackedPoolIds.includes(V2_LP_POOL) ||
    trackedPoolIds.includes(BALANCER_AUTORANGE_V3_POOL);
  const legacyConfig = await stores.LPPoolConfig.get(LEGACY_V3_LP_POOL);
  let v2Config = await stores.LPPoolConfig.get(V2_LP_POOL);
  let balancerConfig = await stores.LPPoolConfig.get(BALANCER_AUTORANGE_V3_POOL);
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

  // Existing databases predate explicit transition rows. Backfill a missing
  // marker only from exact evidence on both sides of that boundary. Current
  // activity and later admin-disable timestamps are deliberately irrelevant:
  // they are mutable live policy, not historical completion proof.
  const v2ResumeTransitionConfigEvidence =
    balancerConfig?.enabledAtTimestamp === LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP &&
    balancerConfig.disabledAtTimestamp === LP_V2_RESUME_CUTOVER_TIMESTAMP &&
    v2Config?.enabledAtTimestamp === LP_V2_RESUME_CUTOVER_TIMESTAMP;
  const balancerTransitionConfigEvidence =
    balancerConfig?.enabledAtTimestamp === LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP &&
    ((v2Config?.enabledAtTimestamp === LP_V2_CUTOVER_TIMESTAMP &&
      v2Config.disabledAtTimestamp === LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP) ||
      v2ResumeTransitionConfigEvidence);
  const v2TransitionConfigEvidence =
    (legacyConfig?.disabledAtTimestamp === LP_V2_CUTOVER_TIMESTAMP &&
      (v2Config?.enabledAtTimestamp === LP_V2_CUTOVER_TIMESTAMP ||
        balancerTransitionConfigEvidence ||
        v2ResumeTransitionConfigEvidence)) ||
    // A missing legacy row plus a complete later two-sided boundary is safe
    // migration evidence: later chronology cannot exist before first entry.
    // A present active/unclosed legacy row remains contradictory and forces
    // the first transition to run rather than trusting the later state.
    (legacyConfig === undefined &&
      (balancerTransitionConfigEvidence || v2ResumeTransitionConfigEvidence));
  const v2TransitionComplete = legacyV3ToV2Marker !== undefined || v2TransitionConfigEvidence;
  const balancerTransitionComplete =
    v2TransitionComplete && (v2ToBalancerMarker !== undefined || balancerTransitionConfigEvidence);
  const v2ResumeTransitionComplete =
    balancerTransitionComplete &&
    (balancerToV2ResumeMarker !== undefined || v2ResumeTransitionConfigEvidence);
  if (!legacyV3ToV2Marker && v2TransitionConfigEvidence) {
    stores.LPStaticTransition?.set({ ...LEGACY_V3_TO_V2_TRANSITION });
  }
  if (!v2ToBalancerMarker && balancerTransitionComplete && balancerTransitionConfigEvidence) {
    stores.LPStaticTransition?.set({ ...V2_TO_BALANCER_TRANSITION });
  }
  if (!balancerToV2ResumeMarker && v2ResumeTransitionComplete && v2ResumeTransitionConfigEvidence) {
    stores.LPStaticTransition?.set({ ...BALANCER_TO_V2_RESUME_TRANSITION });
  }
  const hasPassedV2Cutover = isPastLpV2Cutover(timestamp, blockNumber);
  const hasPassedBalancerCutover = isPastBalancerAutoRangeCutover(timestamp, blockNumber);
  const hasPassedV2ResumeCutover = isPastLpV2ResumeCutover(timestamp, blockNumber);
  if (!hasPassedV2Cutover && v2Config?.isActive) {
    await assertFirstActivationGrowthIsEmpty(context, V2_LP_POOL);
    v2Config = await hideIncomingPoolBeforeBoundary(context, v2Config, LP_V2_CUTOVER_TIMESTAMP);
  }
  if (!hasPassedBalancerCutover && balancerConfig?.isActive) {
    await assertFirstActivationGrowthIsEmpty(context, BALANCER_AUTORANGE_V3_POOL);
    balancerConfig = await hideIncomingPoolBeforeBoundary(
      context,
      balancerConfig,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP
    );
  }
  if (hasPassedV2Cutover && !v2TransitionComplete) {
    await assertFirstActivationGrowthIsEmpty(context, V2_LP_POOL);
  }
  if (hasPassedBalancerCutover && !balancerTransitionComplete) {
    await assertFirstActivationGrowthIsEmpty(context, BALANCER_AUTORANGE_V3_POOL);
  }
  let ensuredLegacyConfig = await ensureLegacyV3PoolConfigEntity(
    context,
    timestamp,
    !v2TransitionComplete
  );
  if (!hasPassedV2Cutover) {
    return;
  }

  // Every historical transition is applied at its own boundary. Tide changes
  // run first so epoch freeze sees only the outgoing era; the incoming pool is
  // activated only after outgoing growth has been advanced and disabled.
  if (!v2TransitionComplete) {
    ensuredLegacyConfig = prepareExactBoundaryOutgoingPool(
      context,
      ensuredLegacyConfig,
      LP_V2_CUTOVER_TIMESTAMP,
      timestamp
    );
    v2Config = await hideIncomingPoolBeforeBoundary(context, v2Config, LP_V2_CUTOVER_TIMESTAMP);
    await applyScheduledEpochTransitions(
      context,
      LP_V2_CUTOVER_TIMESTAMP,
      BigInt(LP_V2_CUTOVER_BLOCK)
    );
    await settleLPPoolPositions(context, LEGACY_V3_LP_POOL, LP_V2_CUTOVER_TIMESTAMP);
    await disableStaticLPPoolAt(context, ensuredLegacyConfig, LP_V2_CUTOVER_TIMESTAMP, timestamp);
    await rebaseEmptyGrowthForFirstActivation(context, V2_LP_POOL, LP_V2_CUTOVER_TIMESTAMP);
    v2Config = await ensureV2PoolConfigEntity(context, timestamp, true, LP_V2_CUTOVER_TIMESTAMP);
    stores.LPStaticTransition?.set({ ...LEGACY_V3_TO_V2_TRANSITION });
  }

  if (!hasPassedBalancerCutover) {
    return;
  }

  if (!balancerTransitionComplete) {
    if (v2Config) {
      v2Config = prepareExactBoundaryOutgoingPool(
        context,
        v2Config,
        LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
        timestamp
      );
    }
    balancerConfig = await hideIncomingPoolBeforeBoundary(
      context,
      balancerConfig,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP
    );
    await applyScheduledEpochTransitions(
      context,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK)
    );
    await advanceLPPoolGrowth(context, V2_LP_POOL, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
    if (v2Config) {
      v2Config = await disableStaticLPPoolAt(
        context,
        v2Config,
        LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
        timestamp
      );
    }
    await rebaseEmptyGrowthForFirstActivation(
      context,
      BALANCER_AUTORANGE_V3_POOL,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP
    );
    balancerConfig = await ensureBalancerAutoRangePoolConfigEntity(
      context,
      timestamp,
      true,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP
    );
    stores.LPStaticTransition?.set({ ...V2_TO_BALANCER_TRANSITION });
  }

  if (!hasPassedV2ResumeCutover) {
    return;
  }

  if (!v2ResumeTransitionComplete) {
    if (balancerConfig) {
      balancerConfig = prepareExactBoundaryOutgoingPool(
        context,
        balancerConfig,
        LP_V2_RESUME_CUTOVER_TIMESTAMP,
        timestamp
      );
    }
    await applyScheduledEpochTransitions(
      context,
      LP_V2_RESUME_CUTOVER_TIMESTAMP,
      BigInt(LP_V2_RESUME_CUTOVER_BLOCK)
    );
    await advanceLPPoolGrowth(context, BALANCER_AUTORANGE_V3_POOL, LP_V2_RESUME_CUTOVER_TIMESTAMP);
    if (balancerConfig) {
      balancerConfig = await disableStaticLPPoolAt(
        context,
        balancerConfig,
        LP_V2_RESUME_CUTOVER_TIMESTAMP,
        timestamp
      );
    }
    await ensureV2PoolConfigEntity(context, timestamp, true, LP_V2_RESUME_CUTOVER_TIMESTAMP);
    stores.LPStaticTransition?.set({ ...BALANCER_TO_V2_RESUME_TRANSITION });
  }
}

async function ensureHardcodedPoolConfig(context: handlerContext, timestamp: number) {
  const config = await ensureLegacyV3PoolConfigEntity(context, timestamp);
  return config.isActive ? config : null;
}

async function getKnownLPPoolConfig(context: handlerContext, pool: string, timestamp: number) {
  const poolId = normalizeAddress(pool);
  const config = await context.LPPoolConfig.get(poolId);
  if (config) return config;
  return poolId === LEGACY_V3_LP_POOL ? await ensureHardcodedPoolConfig(context, timestamp) : null;
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

function buildPoolMintKey(input: {
  pool: string;
  tickLower: number;
  tickUpper: number;
  txHash: string;
}) {
  return `${normalizeAddress(input.pool)}:${input.tickLower}:${input.tickUpper}:${input.txHash}`;
}

function buildPendingMintOwnerId(positionManager: string, tokenId: bigint) {
  return `${normalizeAddress(positionManager)}:${tokenId.toString()}`;
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

async function hasObservedFungiblePoolState(
  context: handlerContext,
  pool: string
): Promise<boolean> {
  const poolId = normalizeAddress(pool);
  const [poolState, poolV2State] = await Promise.all([
    context.LPPoolState.get(poolId),
    context.LPPoolV2State.get(poolId),
  ]);
  return poolState !== undefined && poolV2State !== undefined;
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
  const scale0 = pow10(token0Decimals);
  const scale1 = pow10(token1Decimals);
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
  const positionIds = [...new Set(index.positionIds)];
  const positions = await Promise.all(positionIds.map(id => positionStore.get(id)));
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
      numerator *= pow10(decDiff);
    } else {
      denominator *= pow10(-decDiff);
    }
    return numerator / denominator;
  }

  // token1 = AUSD, token0 = non-AUSD
  // token0USD = AUSD_USD * (token1/token0)
  let numerator = ausdPriceUsd * priceX192;
  let denominator = Q192;
  const decDiff = token0Decimals - token1Decimals;
  if (decDiff >= 0) {
    numerator *= pow10(decDiff);
  } else {
    denominator *= pow10(-decDiff);
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
  const scale0 = pow10(token0Decimals);
  const scale1 = pow10(token1Decimals);
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
  const numerator = BigInt(1e8) * stableReserve * pow10(tokenDecimals);
  const denominator = tokenReserve * pow10(stableDecimals);
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
// Restored from the pre-lazy-growth implementation. Uniswap V3 is historical-only (the pool
// was disabled in Tide 2) and every Tide it touched is prefilled, so the lazy Fenwick path
// bought nothing in production while carrying the most complex code in the indexer - and it
// measurably drifted from production on Tides 1-2. Concentrated-liquidity pools therefore go
// back to per-swap position revaluation, which is what production itself does. Fungible pools
// (Uniswap V2, Balancer) keep the lazy scalar growth: proven exact and still the live path.
/**
 * Settles every position in a concentrated-range pool against the wall clock and
 * republishes the pool's aggregate stats.
 *
 * This is a clock advance, not a range re-evaluation: `isInRange` is deliberately
 * preserved, and `lastInRangeTimestamp` is only re-armed for positions already in
 * range. Range flips are the Swap handler's job (updatePositionsInRangeStatus).
 *
 * Fungible pools return immediately -- Uniswap V2 and Balancer keep the lazy scalar
 * growth clock, which advanceLPPoolGrowth settles in O(1) without walking the index.
 */
async function settleLPPoolPositions(
  context: handlerContext,
  pool: string,
  timestamp: number
): Promise<void> {
  const poolConfig = await getEffectiveLPPoolConfig(context, pool);
  if (!poolConfig) return;
  if (isV2PoolConfig(poolConfig)) return;

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

// ============================================
//     NonfungiblePositionManager Handlers
// ============================================

// Concentrated-liquidity positions settle on the pre-lazy-growth model: accumulated in-range
// seconds against the position's own value snapshot. Uniswap V3 is historical-only and its
// Tides are prefilled, so it does not use the lazy scalar growth that Uniswap V2 and Balancer
// keep. Returned field names mirror the growth settlement so mutation call sites are shared.
async function settleV3PositionBeforeMutation(
  context: handlerContext,
  position: UserLPPosition,
  timestamp: number
) {
  const settled = await settleLPPosition(context, position, timestamp);
  if (settled.pointsEarned > 0n) {
    await updateUserEpochLPPoints(
      context,
      position.user_id,
      settled.pointsEarned,
      timestamp,
      settled.pointsStartTimestamp,
      settled.pointsEndTimestamp
    );
  }
  return {
    pointsEarned: settled.pointsEarned,
    settledAt: settled.settledAt,
    accrualStartTimestamp: settled.pointsStartTimestamp,
    accrualEndTimestamp: settled.pointsEndTimestamp,
    newAccumulatedSeconds: settled.newAccumulatedSeconds,
    newSettledPoints: settled.newSettledPoints,
  };
}

async function getCurrentV3PositionSnapshot(
  context: handlerContext,
  position: UserLPPosition,
  timestamp: number
) {
  if (position.liquidity === 0n) {
    return {
      amount0: 0n,
      amount1: 0n,
      isInRange: false,
      valueUsd: 0n,
      lastInRangeTimestamp: 0,
    };
  }

  const poolId = normalizeAddress(position.pool);
  const [poolConfig, poolState] = await Promise.all([
    context.LPPoolConfig.get(poolId),
    context.LPPoolState.get(poolId),
  ]);
  if (!poolConfig || !poolState) {
    return {
      amount0: position.amount0,
      amount1: position.amount1,
      isInRange: position.isInRange,
      valueUsd: position.valueUsd,
      lastInRangeTimestamp: position.isInRange ? timestamp : 0,
    };
  }

  const { amount0, amount1 } = derivePositionAmounts(
    position.liquidity,
    position.tickLower,
    position.tickUpper,
    poolState.sqrtPriceX96,
    position.amount0,
    position.amount1
  );
  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const isInRange = isPositionInRange(
    position.tickLower,
    position.tickUpper,
    poolState.currentTick
  );
  return {
    amount0,
    amount1,
    isInRange,
    valueUsd: calculatePositionValueUsd(
      amount0,
      amount1,
      poolState.token0Price,
      poolState.token1Price,
      token0Decimals,
      token1Decimals
    ),
    lastInRangeTimestamp: isInRange ? timestamp : 0,
  };
}

indexer.onEvent(
  { contract: 'NonfungiblePositionManager', event: 'IncreaseLiquidity' },
  async ({ event, context }) => {
    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    const positionManager = normalizeAddress(event.srcAddress);
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isLegacyV3ManagerHardStopped(positionManager, timestamp, blockNumber)) {
      await applyScheduledEpochTransitions(context, timestamp, blockNumber);
      return;
    }

    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

    const tokenId = event.params.tokenId;
    const positionId = tokenId.toString();
    const liquidityDelta = BigInt(event.params.liquidity);
    const txMintKey = buildTxMintKey(
      event.transaction.hash,
      event.params.amount0,
      event.params.amount1,
      liquidityDelta
    );
    const pendingOwnerId = buildPendingMintOwnerId(positionManager, tokenId);
    const isHardcodedManager = positionManager === LEGACY_V3_LP_POSITION_MANAGER;
    // For hardcoded manager, ensure config exists upfront
    const hardcodedConfig = isHardcodedManager
      ? await ensureHardcodedPoolConfig(context, timestamp)
      : null;
    logLpDebug(
      context,
      `[lp] IncreaseLiquidity tokenId=${positionId} manager=${positionManager} amount0=${event.params.amount0.toString()} amount1=${event.params.amount1.toString()} liquidity=${liquidityDelta.toString()}`
    );

    const [position, txMintData, pendingOwner] = await Promise.all([
      context.UserLPPosition.get(positionId),
      context.LPMintData.get(txMintKey),
      context.LPPendingMintOwner.get(pendingOwnerId),
    ]);

    // A new NFT can be observed in canonical Transfer-before-Increase order or
    // through the legacy token-keyed compatibility row.
    if (!position) {
      // A token-keyed row is legacy compatibility state; canonical correlation
      // uses the exact transaction mint plus the manager-scoped pending owner.
      const pendingKey = `pending:${tokenId.toString()}`;
      const existingMint = await context.LPMintData.get(pendingKey);
      if (existingMint) {
        return;
      }
      if (
        pendingOwner &&
        (pendingOwner.positionManager !== positionManager ||
          pendingOwner.tokenId !== tokenId ||
          pendingOwner.txHash !== event.transaction.hash)
      ) {
        throw new Error(
          `pending LP mint owner mismatch: id=${pendingOwner.id} manager=${positionManager} tokenId=${tokenId.toString()} tx=${event.transaction.hash}`
        );
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
      let mintData = txMintData;

      if (!positionData || !poolConfig) {
        if (mintData) {
          const mintPool = normalizeAddress(mintData.pool);
          poolConfig = await getKnownLPPoolConfig(context, mintPool, timestamp);
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

      if (!positionData || !poolConfig) {
        if (pendingOwner) {
          context.LPPendingMintOwner.deleteUnsafe(pendingOwnerId);
        }
        return;
      }

      // Create position directly - don't rely on Transfer event ordering
      const owner = pendingOwner
        ? normalizeAddress(pendingOwner.owner)
        : event.transaction.from
          ? normalizeAddress(event.transaction.from)
          : ZERO_ADDRESS;
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

      const nextPosition: UserLPPosition = {
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
      };
      context.UserLPPosition.set(nextPosition);

      await addPositionToPoolIndex(context, poolConfig.pool, positionId, timestamp);
      await addPositionToUserIndex(context, owner, positionId, timestamp);
      await updateUserLPStats(context, owner, timestamp);
      await updatePoolLPStats(context, poolConfig.pool, timestamp);

      // Clean up mint data
      if (mintData) {
        context.LPMintData.deleteUnsafe(buildPoolMintKey(mintData));
      }
      context.LPMintData.deleteUnsafe(txMintKey);
      context.LPPendingMintOwner.deleteUnsafe(pendingOwnerId);
      return;
    }

    const existingPool = normalizeAddress(position.pool);
    const poolConfig = await getKnownLPPoolConfig(context, existingPool, timestamp);
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

    // Settle any accumulated points before changing state.
    const settlement = await settleV3PositionBeforeMutation(context, position, timestamp);

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

    const nextPosition: UserLPPosition = {
      ...position,
      liquidity: newLiquidity,
      amount0: newAmount0,
      amount1: newAmount1,
      isInRange: isNowInRange,
      valueUsd,
      lastInRangeTimestamp: newLastInRangeTimestamp,
      accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
      lastSettledAt: settlement.settledAt,
      settledLpPoints: position.settledLpPoints + settlement.pointsEarned,
      lastUpdate: timestamp,
    };
    context.UserLPPosition.set(nextPosition);
    await updateUserLPStats(context, position.user_id, timestamp);
    await updatePoolLPStats(context, position.pool, timestamp);
    if (txMintData) {
      context.LPMintData.deleteUnsafe(buildPoolMintKey(txMintData));
      context.LPMintData.deleteUnsafe(txMintKey);
    }
  }
);

indexer.onEvent(
  { contract: 'NonfungiblePositionManager', event: 'DecreaseLiquidity' },
  async ({ event, context }) => {
    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    const positionManager = normalizeAddress(event.srcAddress);
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isLegacyV3ManagerHardStopped(positionManager, timestamp, blockNumber)) {
      await applyScheduledEpochTransitions(context, timestamp, blockNumber);
      return;
    }

    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

    const tokenId = event.params.tokenId;
    const positionId = tokenId.toString();

    let position = await context.UserLPPosition.get(positionId);
    if (!position) return;

    const decreasePool = normalizeAddress(position.pool);
    const poolConfig = await getKnownLPPoolConfig(context, decreasePool, timestamp);
    if (!poolConfig) return;

    const poolState = await seedPoolStateFromChain(
      context,
      position.pool,
      timestamp,
      BigInt(event.block.number)
    );
    const wasInRange = position.isInRange;

    const liquidityDelta = BigInt(event.params.liquidity);
    if (liquidityDelta > position.liquidity) {
      throw new Error(
        `LP position liquidity underflow: position=${position.id} current=${position.liquidity.toString()} decrease=${liquidityDelta.toString()}`
      );
    }

    // Settle any accumulated points before changing state.
    const settlement = await settleV3PositionBeforeMutation(context, position, timestamp);

    // Update position amounts
    const newLiquidity = position.liquidity - liquidityDelta;
    const fallbackAmount0 =
      position.amount0 > event.params.amount0 ? position.amount0 - event.params.amount0 : 0n;
    const fallbackAmount1 =
      position.amount1 > event.params.amount1 ? position.amount1 - event.params.amount1 : 0n;
    const derivedAmounts =
      newLiquidity === 0n
        ? { amount0: 0n, amount1: 0n }
        : derivePositionAmounts(
            newLiquidity,
            position.tickLower,
            position.tickUpper,
            poolState.sqrtPriceX96,
            fallbackAmount0,
            fallbackAmount1
          );
    const newAmount0 = derivedAmounts.amount0;
    const newAmount1 = derivedAmounts.amount1;
    const isNowInRange =
      newLiquidity > 0n &&
      isPositionInRange(position.tickLower, position.tickUpper, poolState.currentTick);

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

    const nextPosition: UserLPPosition = {
      ...position,
      liquidity: newLiquidity,
      amount0: newAmount0,
      amount1: newAmount1,
      isInRange: isNowInRange,
      valueUsd,
      lastInRangeTimestamp: newLastInRangeTimestamp,
      accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
      lastSettledAt: settlement.settledAt,
      settledLpPoints: position.settledLpPoints + settlement.pointsEarned,
      lastUpdate: timestamp,
    };
    context.UserLPPosition.set(nextPosition);
    await updateUserLPStats(context, position.user_id, timestamp);
    await updatePoolLPStats(context, position.pool, timestamp);
  }
);

indexer.onEvent(
  { contract: 'NonfungiblePositionManager', event: 'Transfer' },
  async ({ event, context }) => {
    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    const positionManager = normalizeAddress(event.srcAddress);
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isLegacyV3ManagerHardStopped(positionManager, timestamp, blockNumber)) {
      await applyScheduledEpochTransitions(context, timestamp, blockNumber);
      return;
    }

    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

    const tokenId = event.params.tokenId;
    const positionId = tokenId.toString();
    const from = normalizeAddress(event.params.from);
    const to = normalizeAddress(event.params.to);
    const pendingOwnerId = buildPendingMintOwnerId(positionManager, tokenId);
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
          const settlement = await settleV3PositionBeforeMutation(
            context,
            existingPosition,
            timestamp
          );
          const snapshot = await getCurrentV3PositionSnapshot(context, existingPosition, timestamp);
          const nextPosition: UserLPPosition = {
            ...existingPosition,
            ...snapshot,
            user_id: to,
            accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
            lastSettledAt: settlement.settledAt,
            settledLpPoints: existingPosition.settledLpPoints + settlement.pointsEarned,
            lastUpdate: timestamp,
          };
          context.UserLPPosition.set(nextPosition);
          await removePositionFromUserIndex(context, oldOwner, positionId, timestamp);
          await addPositionToUserIndex(context, to, positionId, timestamp);
          await updateUserLPStats(context, oldOwner, timestamp);
          await updateUserLPStats(context, to, timestamp);
        }
        return;
      }

      const pendingMintKey = `pending:${tokenId.toString()}`;
      const mintData = await context.LPMintData.get(pendingMintKey);
      if (!mintData) {
        context.LPPendingMintOwner.set({
          id: pendingOwnerId,
          tokenId,
          positionManager,
          owner: to,
          txHash: event.transaction.hash,
          timestamp,
        });
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

      if (!positionData || !poolConfig) {
        // Use LPMintData to reconstruct position data
        if (mintData) {
          const mintPool = normalizeAddress(mintData.pool);
          poolConfig = await getKnownLPPoolConfig(context, mintPool, timestamp);
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

      // Legacy token-keyed mint data carries the fallback event amounts.
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

      const nextPosition: UserLPPosition = {
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
      };
      context.UserLPPosition.set(nextPosition);

      // Clean up mint data after use
      context.LPMintData.deleteUnsafe(pendingMintKey);
      context.LPMintData.deleteUnsafe(buildPoolMintKey(mintData));
      context.LPMintData.deleteUnsafe(
        buildTxMintKey(mintData.txHash, mintData.amount0, mintData.amount1, mintData.liquidity)
      );
      context.LPPendingMintOwner.deleteUnsafe(pendingOwnerId);

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
        const settlement = await settleV3PositionBeforeMutation(context, position, timestamp);

        // Mark position as removed
        const nextPosition: UserLPPosition = {
          ...position,
          liquidity: 0n,
          amount0: 0n,
          amount1: 0n,
          isInRange: false,
          valueUsd: 0n,
          lastInRangeTimestamp: 0,
          accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
          settledLpPoints: position.settledLpPoints + settlement.pointsEarned,
          lastSettledAt: settlement.settledAt,
          lastUpdate: timestamp,
        };
        context.UserLPPosition.set(nextPosition);

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
      const settlement = await settleV3PositionBeforeMutation(context, position, timestamp);
      const snapshot = await getCurrentV3PositionSnapshot(context, position, timestamp);

      // Update position owner
      await getOrCreateUser(context, to);
      const nextPosition: UserLPPosition = {
        ...position,
        ...snapshot,
        user_id: to,
        accumulatedInRangeSeconds: settlement.newAccumulatedSeconds,
        settledLpPoints: position.settledLpPoints + settlement.pointsEarned,
        lastSettledAt: settlement.settledAt,
        lastUpdate: timestamp,
      };
      context.UserLPPosition.set(nextPosition);

      await removePositionFromUserIndex(context, oldOwner, positionId, timestamp);
      await addPositionToUserIndex(context, to, positionId, timestamp);

      await updateUserLPStats(context, oldOwner, timestamp);
      await updateUserLPStats(context, to, timestamp);
      await updatePoolLPStats(context, position.pool, timestamp);
    }
  }
);

// ============================================
//     UniswapV3Pool Handlers
// ============================================

indexer.onEvent({ contract: 'UniswapV3Pool', event: 'Initialize' }, async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
  if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) {
    await applyScheduledEpochTransitions(context, timestamp, blockNumber);
    return;
  }
  const poolConfig =
    pool === LEGACY_V3_LP_POOL
      ? await ensureHardcodedPoolConfig(context, timestamp)
      : await getActiveLPPoolConfig(context, pool);
  if (!poolConfig) return;
  await applyScheduledEpochTransitions(context, timestamp, blockNumber);

  const currentTick = Number(event.params.tick);
  const sqrtPriceX96 = event.params.sqrtPriceX96;
  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  let token0Price = 0n;
  let token1Price = 0n;
  const isStableToken0 = isStableUsdToken(poolConfig.token0);
  const isStableToken1 = isStableUsdToken(poolConfig.token1);
  if (isStableToken0 !== isStableToken1) {
    const stablePriceE8 = getAusdPrice();
    const pairedTokenPrice = calculateDustPriceFromPool(
      sqrtPriceX96,
      stablePriceE8,
      isStableToken0,
      token0Decimals,
      token1Decimals
    );
    token0Price = isStableToken0 ? stablePriceE8 : pairedTokenPrice;
    token1Price = isStableToken0 ? pairedTokenPrice : stablePriceE8;
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

indexer.onEvent({ contract: 'UniswapV3Pool', event: 'Swap' }, async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
  if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) {
    await applyScheduledEpochTransitions(context, timestamp, blockNumber);
    return;
  }

  // Check if this pool is tracked
  const poolConfig =
    pool === LEGACY_V3_LP_POOL
      ? await ensureHardcodedPoolConfig(context, timestamp)
      : await getActiveLPPoolConfig(context, pool);
  if (!poolConfig) return;
  await applyScheduledEpochTransitions(context, timestamp, blockNumber);
  const currentTick = Number(event.params.tick);
  const sqrtPriceX96 = event.params.sqrtPriceX96;
  const oldState = await context.LPPoolState.get(pool);
  const oldTick = oldState?.currentTick ?? 0;

  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  let token0Price = oldState?.token0Price ?? 0n;
  let token1Price = oldState?.token1Price ?? 0n;
  const isStableToken0 = isStableUsdToken(poolConfig.token0);
  const isStableToken1 = isStableUsdToken(poolConfig.token1);
  if (isStableToken0 !== isStableToken1) {
    const stablePriceE8 = getAusdPrice();
    const pairedTokenPrice = calculateDustPriceFromPool(
      sqrtPriceX96,
      stablePriceE8,
      isStableToken0,
      token0Decimals,
      token1Decimals
    );
    token0Price = isStableToken0 ? stablePriceE8 : pairedTokenPrice;
    token1Price = isStableToken0 ? pairedTokenPrice : stablePriceE8;
  }

  context.LPPoolState.set({
    ...oldState,
    id: pool,
    pool,
    currentTick,
    sqrtPriceX96,
    token0Price,
    token1Price,
    feeProtocol0: oldState?.feeProtocol0 ?? 0,
    feeProtocol1: oldState?.feeProtocol1 ?? 0,
    lastUpdate: timestamp,
  });
  if (currentTick !== oldTick) {
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

indexer.onEvent(
  { contract: 'UniswapV3Pool', event: 'SetFeeProtocol' },
  async ({ event, context }) => {
    const pool = normalizeAddress(event.srcAddress);
    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) {
      await applyScheduledEpochTransitions(context, timestamp, blockNumber);
      return;
    }

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
  }
);

indexer.onEvent({ contract: 'UniswapV3Pool', event: 'Mint' }, async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const pool = normalizeAddress(event.srcAddress);
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
  if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) {
    await applyScheduledEpochTransitions(context, timestamp, blockNumber);
    return;
  }

  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const poolConfig = await getKnownLPPoolConfig(context, pool, timestamp);
  if (!poolConfig) return;

  const tickLower = Number(event.params.tickLower);
  const tickUpper = Number(event.params.tickUpper);
  const owner = normalizeAddress(event.params.owner);
  if (owner !== normalizeAddress(poolConfig.positionManager)) return;

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

indexer.onEvent({ contract: 'UniswapV3Pool', event: 'Burn' }, async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const pool = normalizeAddress(event.srcAddress);
  await applyStaticLPPoolCutover(context, timestamp, blockNumber);
  if (isLegacyV3PoolHardStopped(pool, timestamp, blockNumber)) {
    await applyScheduledEpochTransitions(context, timestamp, blockNumber);
    return;
  }

  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  // Burn is handled via DecreaseLiquidity on PositionManager
});

// ============================================
//     UniswapV2Pair Handlers
// ============================================

async function applyFungibleShareTransfer(
  context: handlerContext,
  poolConfig: LPPoolConfig,
  from: string,
  to: string,
  amount: bigint,
  timestamp: number,
  poolState: Awaited<ReturnType<typeof getOrCreateLPPoolState>>,
  poolV2State: Awaited<ReturnType<typeof getOrCreateLPPoolV2State>>
): Promise<void> {
  const pool = normalizeAddress(poolConfig.pool);
  const normalizedFrom = normalizeAddress(from);
  const normalizedTo = normalizeAddress(to);
  const touchedUsers = [normalizedFrom, normalizedTo].filter(
    (userId, index, users) => userId !== ZERO_ADDRESS && users.indexOf(userId) === index
  );
  const existingPositions = new Map<string, UserLPPosition | undefined>();
  for (const userId of touchedUsers) {
    const positionId = getV2PositionId(pool, userId);
    existingPositions.set(userId, await context.UserLPPosition.get(positionId));
  }

  const settlements = new Map<
    string,
    Awaited<ReturnType<typeof settleLPPositionGrowthAfterPoolAdvance>>
  >();
  for (const userId of touchedUsers) {
    const existing = existingPositions.get(userId);
    if (!existing) continue;
    const settlement = await settleLPPositionGrowthAfterPoolAdvance(context, existing, timestamp);
    settlements.set(userId, settlement);
    if (settlement.pointsEarned > 0n) {
      await updateUserEpochLPPoints(
        context,
        userId,
        settlement.pointsEarned,
        timestamp,
        settlement.accrualStartTimestamp,
        settlement.accrualEndTimestamp
      );
    }
  }

  let nextTotalSupply = poolV2State.lpTotalSupply;
  if (normalizedFrom === ZERO_ADDRESS) {
    nextTotalSupply += amount;
  } else if (normalizedTo === ZERO_ADDRESS) {
    // A pool bootstrapped mid-life - the UniswapV2 pair is deliberately indexed from
    // LP_V2_CUTOVER_BLOCK - never observed the mints that created its outstanding supply and
    // holder balances, so a legitimate burn/transfer can exceed what this indexer has
    // tracked. Production clamps at zero here (see the pre-rewrite lp.ts); failing closed
    // instead makes real mainnet history unindexable. Clamp to match production.
    nextTotalSupply = nextTotalSupply > amount ? nextTotalSupply - amount : 0n;
  }
  const nextPoolV2State = {
    ...poolV2State,
    lpTotalSupply: nextTotalSupply,
    lastUpdate: timestamp,
  };
  context.LPPoolV2State.set(nextPoolV2State);
  context.LPPoolState.set({
    ...poolState,
    lastUpdate: timestamp,
  });

  const { token0Decimals, token1Decimals } = await getPoolTokenDecimals(
    context,
    poolConfig,
    timestamp
  );
  const leaderboardState = await context.LeaderboardState.get('current');
  const epochNumber = leaderboardState?.currentEpochNumber ?? 0n;
  for (const userId of touchedUsers) {
    const existing = existingPositions.get(userId);
    const outgoing = userId === normalizedFrom ? amount : 0n;
    const incoming = userId === normalizedTo ? amount : 0n;
    // Same mid-life bootstrap case as the supply clamp above: a holder can burn or send
    // shares this indexer never saw minted. Production clamps the balance at zero rather
    // than failing, so replay stays consistent with the deployed indexer.
    const rawLiquidity = (existing?.liquidity ?? 0n) - outgoing + incoming;
    const liquidity = rawLiquidity < 0n ? 0n : rawLiquidity;
    if (poolConfig.isActive) {
      await getOrCreateUser(context, userId);
    }

    const { amount0, amount1 } = calculateV2PositionAmounts(
      liquidity,
      nextPoolV2State.reserve0,
      nextPoolV2State.reserve1,
      nextPoolV2State.lpTotalSupply
    );
    const valueUsd = calculatePositionValueUsd(
      amount0,
      amount1,
      poolState.token0Price,
      poolState.token1Price,
      token0Decimals,
      token1Decimals
    );
    const settlement = settlements.get(userId);
    const nextPosition: UserLPPosition = {
      id: getV2PositionId(pool, userId),
      tokenId: getSyntheticTokenIdFromAddress(userId),
      user_id: userId,
      pool,
      positionManager: pool,
      tickLower: V2_TICK_LOWER,
      tickUpper: V2_TICK_UPPER,
      liquidity,
      amount0,
      amount1,
      isInRange: liquidity > 0n,
      valueUsd,
      lastInRangeTimestamp: liquidity > 0n ? timestamp : 0,
      accumulatedInRangeSeconds: existing?.accumulatedInRangeSeconds ?? 0n,
      lastSettledAt: settlement?.settledAt ?? timestamp,
      settledLpPoints: (existing?.settledLpPoints ?? 0n) + (settlement?.pointsEarned ?? 0n),
      createdAt: existing?.createdAt ?? timestamp,
      lastUpdate: timestamp,
    };
    context.UserLPPosition.set(nextPosition);

    if (liquidity > 0n) {
      await addPositionToUserIndex(context, userId, nextPosition.id, timestamp);
      await addPositionToPoolIndex(context, pool, nextPosition.id, timestamp);
    } else {
      await removePositionFromUserIndex(context, userId, nextPosition.id, timestamp);
      await removePositionFromPoolIndex(context, pool, nextPosition.id, timestamp);
    }
    if (epochNumber > 0n) {
      await resetLPPositionGrowthBaseline(context, nextPosition, epochNumber, timestamp);
    }
  }

  for (const userId of touchedUsers) {
    await updateUserLPStats(context, userId, timestamp);
  }
}

indexer.onEvent({ contract: 'UniswapV2Pair', event: 'Transfer' }, async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const hadFungibleState = await hasObservedFungiblePoolState(context, pool);
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
  await advanceLPPoolGrowth(context, pool, timestamp, hadFungibleState ? undefined : timestamp);
  await applyFungibleShareTransfer(
    context,
    poolConfig,
    from,
    to,
    amount,
    timestamp,
    existingPoolState,
    existingPoolV2State
  );
});

indexer.onEvent({ contract: 'UniswapV2Pair', event: 'Swap' }, async ({ event, context }) => {
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

indexer.onEvent({ contract: 'UniswapV2Pair', event: 'Sync' }, async ({ event, context }) => {
  const pool = normalizeAddress(event.srcAddress);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  const hadFungibleState = await hasObservedFungiblePoolState(context, pool);
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
  // Attribute the elapsed interval to the pre-Sync reserves and prices. This
  // is deliberately pool-only: holders consume the scalar growth lazily on
  // their next explicit touch.
  await advanceLPPoolGrowth(context, pool, timestamp, hadFungibleState ? undefined : timestamp);
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
});

// ============================================
//     Balancer AutoRange V3 Pool Handlers
// ============================================

indexer.onEvent(
  { contract: 'BalancerAutoRangePool', event: 'Transfer' },
  async ({ event, context }) => {
    const pool = normalizeAddress(event.srcAddress);
    if (!isBalancerAutoRangePool(pool)) return;

    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    const hadFungibleState = await hasObservedFungiblePoolState(context, pool);
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isBalancerPoolHardStopped(timestamp, blockNumber)) {
      await applyScheduledEpochTransitions(context, timestamp, blockNumber);
      return;
    }
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
    const poolV2State = await getOrCreateLPPoolV2State(context, pool, timestamp);
    await advanceLPPoolGrowth(context, pool, timestamp, hadFungibleState ? undefined : timestamp);
    await applyFungibleShareTransfer(
      context,
      poolConfig,
      from,
      to,
      amount,
      timestamp,
      poolState,
      poolV2State
    );
  }
);

// The Vault is an addressed singleton so it shares the main address partition.
// The indexed `pool` topic filter keeps unrelated Balancer pools out of the query;
// the in-handler guards remain as defense-in-depth.
indexer.onEvent(
  {
    contract: 'BalancerVault',
    event: 'LiquidityAdded',
    where: () => ({ params: { pool: BALANCER_AUTORANGE_V3_POOL } }),
  },
  async ({ event, context }) => {
    if (!isBalancerVault(event.srcAddress)) return;

    const pool = normalizeAddress(event.params.pool);
    if (!isBalancerAutoRangePool(pool)) return;

    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    const hadFungibleState = await hasObservedFungiblePoolState(context, pool);
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isBalancerPoolHardStopped(timestamp, blockNumber)) {
      await applyScheduledEpochTransitions(context, timestamp, blockNumber);
      return;
    }
    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

    const isActive = isBalancerAutoRangeActiveEra(timestamp, blockNumber);
    const poolConfig = await ensureBalancerAutoRangePoolConfigEntity(context, timestamp, isActive);
    await advanceLPPoolGrowth(context, pool, timestamp, hadFungibleState ? undefined : timestamp);
    await applyBalancerAutoRangeLiquidityDelta(
      context,
      poolConfig,
      timestamp,
      event.params.amountsAddedRaw,
      event.params.totalSupply,
      true
    );
  }
);

indexer.onEvent(
  {
    contract: 'BalancerVault',
    event: 'LiquidityRemoved',
    where: () => ({ params: { pool: BALANCER_AUTORANGE_V3_POOL } }),
  },
  async ({ event, context }) => {
    if (!isBalancerVault(event.srcAddress)) return;

    const pool = normalizeAddress(event.params.pool);
    if (!isBalancerAutoRangePool(pool)) return;

    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    const hadFungibleState = await hasObservedFungiblePoolState(context, pool);
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isBalancerPoolHardStopped(timestamp, blockNumber)) {
      await applyScheduledEpochTransitions(context, timestamp, blockNumber);
      return;
    }
    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

    const isActive = isBalancerAutoRangeActiveEra(timestamp, blockNumber);
    const poolConfig = await ensureBalancerAutoRangePoolConfigEntity(context, timestamp, isActive);
    await advanceLPPoolGrowth(context, pool, timestamp, hadFungibleState ? undefined : timestamp);
    await applyBalancerAutoRangeLiquidityDelta(
      context,
      poolConfig,
      timestamp,
      event.params.amountsRemovedRaw,
      event.params.totalSupply,
      false
    );
  }
);

indexer.onEvent(
  {
    contract: 'BalancerVault',
    event: 'Swap',
    where: () => ({ params: { pool: BALANCER_AUTORANGE_V3_POOL } }),
  },
  async ({ event, context }) => {
    if (!isBalancerVault(event.srcAddress)) return;

    const pool = normalizeAddress(event.params.pool);
    if (!isBalancerAutoRangePool(pool)) return;

    const timestamp = Number(event.block.timestamp);
    const blockNumber = BigInt(event.block.number);
    const hadFungibleState = await hasObservedFungiblePoolState(context, pool);
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    if (isBalancerPoolHardStopped(timestamp, blockNumber)) {
      await applyScheduledEpochTransitions(context, timestamp, blockNumber);
      return;
    }
    await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

    const isActive = isBalancerAutoRangeActiveEra(timestamp, blockNumber);
    const poolConfig = await ensureBalancerAutoRangePoolConfigEntity(context, timestamp, isActive);
    await advanceLPPoolGrowth(context, pool, timestamp, hadFungibleState ? undefined : timestamp);
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
  }
);

// ============================================
//     Helper: Update User LP Stats
// ============================================

async function updateUserLPStats(context: handlerContext, userId: string, timestamp: number) {
  const positions = await listUserLPPositions(context, userId);
  updateUserLPStatsFromPositions(context, userId, positions, timestamp);
}

function updateUserLPStatsFromPositions(
  context: handlerContext,
  userId: string,
  positions: Awaited<ReturnType<typeof listUserLPPositions>>,
  timestamp: number
) {
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

  const normalizedUserId = normalizeAddress(userId);
  context.UserLPStats.set({
    id: normalizedUserId,
    user_id: normalizedUserId,
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
  if (isPointAccrualBlacklisted(normalizeAddress(userId), timestamp)) return;
  if (isPrefilledTimestamp(timestamp)) return;

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

  setUserEpochStats(context, updatedStats);
  await updateLifetimePoints(context, userId, updatedStats);

  const finalPoints = Number(updatedStats.totalPointsWithMultiplier) / 1e18;
  const { updateLeaderboard } = await import('../helpers/leaderboard');
  await updateLeaderboard(context, userId, finalPoints, timestamp);
}

/**
 * The two LP settlement engines return differently-named fields; settleUserLPPositions
 * consumes both, so each is normalized to this one shape. `newAccumulatedSeconds` is
 * absent for fungible pools, which track no per-position in-range clock.
 */
interface UserLPPositionSettlement {
  settledAt: number;
  pointsEarned: bigint;
  accrualStartTimestamp: number;
  accrualEndTimestamp: number;
  newAccumulatedSeconds?: bigint;
}

function emptyUserLPPositionSettlement(timestamp: number): UserLPPositionSettlement {
  return {
    settledAt: timestamp,
    pointsEarned: 0n,
    accrualStartTimestamp: timestamp,
    accrualEndTimestamp: timestamp,
  };
}

async function settleUserLPPositionScalar(
  context: handlerContext,
  position: UserLPPosition,
  timestamp: number
): Promise<UserLPPositionSettlement> {
  const settlement = await settleLPPositionGrowthAfterPoolAdvance(context, position, timestamp);
  return {
    settledAt: settlement.settledAt,
    pointsEarned: settlement.pointsEarned,
    accrualStartTimestamp: settlement.accrualStartTimestamp,
    accrualEndTimestamp: settlement.accrualEndTimestamp,
  };
}

async function settleUserLPPositionRanged(
  context: handlerContext,
  position: UserLPPosition,
  timestamp: number
): Promise<UserLPPositionSettlement> {
  const settlement = await settleLPPosition(context, position, timestamp);
  return {
    settledAt: settlement.settledAt,
    pointsEarned: settlement.pointsEarned,
    accrualStartTimestamp: settlement.pointsStartTimestamp,
    accrualEndTimestamp: settlement.pointsEndTimestamp,
    newAccumulatedSeconds: settlement.newAccumulatedSeconds,
  };
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
  const currentPositions = [...positions];
  const positionOffsets = new Map(positions.map((position, index) => [position.id, index]));

  const positionsByPool = new Map<string, typeof positions>();
  for (const position of positions) {
    if (position.liquidity === 0n && position.amount0 === 0n && position.amount1 === 0n) continue;

    const poolId = normalizeAddress(position.pool);
    const bucket = positionsByPool.get(poolId);
    if (bucket) {
      bucket.push(position);
    } else {
      positionsByPool.set(poolId, [position]);
    }
  }

  for (const [poolId, poolPositions] of positionsByPool.entries()) {
    const poolConfig = await context.LPPoolConfig.get(poolId);
    // Fungible pools advance one scalar growth clock for the whole pool, then read
    // each position's share off it. Concentrated-range pools have no pool-wide clock
    // to advance -- each position accrues only while the price sits inside its own
    // tick range, so they settle individually through settleLPPosition.
    const isFungiblePool = poolConfig ? isV2PoolConfig(poolConfig) : false;
    if (poolConfig?.isActive && isFungiblePool) {
      await advanceLPPoolGrowth(context, poolId, timestamp);
    }

    const poolState = poolConfig
      ? await getOrCreateLPPoolState(context, poolId, timestamp)
      : undefined;
    const poolV2State =
      poolConfig && isV2PoolConfig(poolConfig)
        ? await getOrCreateLPPoolV2State(context, poolId, timestamp)
        : null;
    const poolTokenDecimals = poolConfig
      ? await getPoolTokenDecimals(context, poolConfig, timestamp)
      : undefined;

    for (const position of poolPositions) {
      // A pool with no LPPoolConfig of its own must not settle at all. The ranged engine
      // resolves its rate through getEffectiveLPPoolConfig, which falls back to the single
      // active pool when a pool is unconfigured -- scoring the position against an
      // unrelated market. Advance the clock, credit nothing.
      const settlement = !poolConfig
        ? emptyUserLPPositionSettlement(timestamp)
        : isFungiblePool
          ? await settleUserLPPositionScalar(context, position, timestamp)
          : await settleUserLPPositionRanged(context, position, timestamp);
      let updatedPosition = {
        ...position,
        lastSettledAt: settlement.settledAt,
        settledLpPoints: position.settledLpPoints + settlement.pointsEarned,
        lastUpdate: timestamp,
        ...(settlement.newAccumulatedSeconds === undefined
          ? {}
          : { accumulatedInRangeSeconds: settlement.newAccumulatedSeconds }),
      };

      if (poolConfig && poolState && poolTokenDecimals) {
        const isNowInRange = poolV2State
          ? position.liquidity > 0n
          : isPositionInRange(position.tickLower, position.tickUpper, poolState.currentTick);
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
          poolTokenDecimals.token0Decimals,
          poolTokenDecimals.token1Decimals
        );
        updatedPosition = {
          ...updatedPosition,
          isInRange: isNowInRange,
          amount0: derivedAmounts.amount0,
          amount1: derivedAmounts.amount1,
          valueUsd,
          lastInRangeTimestamp: isNowInRange ? settlement.settledAt : 0,
        };
      }

      context.UserLPPosition.set(updatedPosition);
      const positionOffset = positionOffsets.get(position.id);
      if (positionOffset !== undefined) currentPositions[positionOffset] = updatedPosition;

      if (settlement.pointsEarned > 0n) {
        await updateUserEpochLPPoints(
          context,
          position.user_id,
          settlement.pointsEarned,
          timestamp,
          settlement.accrualStartTimestamp,
          settlement.accrualEndTimestamp
        );
      }
    }
  }

  updateUserLPStatsFromPositions(context, normalizedUserId, currentPositions, timestamp);
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
  updatePoolFeeStats,
  updatePoolLPStats,
  updateUserLPStats,
};
