import { normalizeAddress } from '../helpers/constants';
import { fungibleUnitValueGrowthX128, growthToPoints } from '../helpers/lpGrowthMath';
import { getLPPoolTokenDecimals, isFungibleLPPoolConfig } from './lpEntityHelpers';

import type {
  LPPoolEpochGrowth,
  UserLPEpochCursor,
  UserLPPosition,
  handlerContext,
} from '../../generated';

export type LPPositionGrowthSettlement = {
  epochNumber: bigint;
  growthBaselineX128: bigint;
  currentGrowthX128: bigint;
  pointsEarned: bigint;
  accrualStartTimestamp: number;
  accrualEndTimestamp: number;
  settledAt: number;
};

export function lpPoolEpochGrowthId(pool: string, epochNumber: bigint): string {
  return `${normalizeAddress(pool)}:${epochNumber.toString()}`;
}

export function userLPEpochCursorId(positionId: string, epochNumber: bigint): string {
  return `${normalizeAddress(positionId)}:${epochNumber.toString()}`;
}

function assertLPGrowthFreezeTimestamp(growth: LPPoolEpochGrowth, endTimestamp: number): void {
  if (endTimestamp < growth.lastTimestamp) {
    throw new Error(
      `LP growth freeze timestamp moved backward: pool=${growth.pool} epoch=${growth.epochNumber.toString()} end=${endTimestamp} last=${growth.lastTimestamp}`
    );
  }
  if (growth.isFrozen && growth.frozenAt !== endTimestamp) {
    throw new Error(
      `LP growth already frozen at a different timestamp: pool=${growth.pool} epoch=${growth.epochNumber.toString()} requested=${endTimestamp} frozenAt=${growth.frozenAt}`
    );
  }
}

async function advanceLPPoolGrowthForEpoch(
  context: handlerContext,
  pool: string,
  epochNumber: bigint,
  throughTimestamp: number,
  newHeaderStartTimestamp?: number
): Promise<LPPoolEpochGrowth | undefined> {
  const poolId = normalizeAddress(pool);
  const growthId = lpPoolEpochGrowthId(poolId, epochNumber);
  const [epoch, config, poolState, v2State, existing] = await Promise.all([
    context.LeaderboardEpoch.get(epochNumber.toString()),
    context.LPPoolConfig.get(poolId),
    context.LPPoolState.get(poolId),
    context.LPPoolV2State.get(poolId),
    context.LPPoolEpochGrowth.get(growthId),
  ]);
  if (!epoch || !config?.isActive || !poolState) return;
  // Concentrated-range pools no longer use this scalar clock -- they settle per
  // position through settleLPPosition on every swap and mutation. They still reach
  // this function through LeaderboardConfig.LPPoolDisabled / LPRateUpdated,
  // freezeLPGrowthForEpoch and settleUserLPPositions, so the guard belongs here
  // rather than at each call site.
  if (!isFungibleLPPoolConfig(config)) return;

  const startTimestamp = Math.max(
    epoch.startTime,
    config.enabledAtTimestamp,
    newHeaderStartTimestamp ?? epoch.startTime
  );
  if (!existing && throughTimestamp < startTimestamp) return;
  const growth: LPPoolEpochGrowth = existing ?? {
    id: growthId,
    pool: poolId,
    epochNumber,
    startTimestamp,
    lastTimestamp: startTimestamp,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: startTimestamp,
  };

  if (throughTimestamp < growth.lastTimestamp) {
    throw new Error(
      `LP growth timestamp moved backward: pool=${poolId} epoch=${epochNumber.toString()} through=${throughTimestamp} last=${growth.lastTimestamp}`
    );
  }
  if (growth.isFrozen) {
    if (throughTimestamp > (growth.frozenAt ?? growth.lastTimestamp)) {
      throw new Error(
        `cannot advance frozen LP growth: pool=${poolId} epoch=${epochNumber.toString()} through=${throughTimestamp}`
      );
    }
    return growth;
  }

  const caps = [throughTimestamp];
  if (epoch.endTime !== undefined) caps.push(epoch.endTime);
  if (config.disabledAtTimestamp !== undefined) caps.push(config.disabledAtTimestamp);
  const cappedTimestamp = Math.max(growth.lastTimestamp, Math.min(...caps));
  const accrualStartTimestamp = Math.max(growth.lastTimestamp, config.enabledAtTimestamp);
  const elapsedSeconds = Math.max(0, cappedTimestamp - accrualStartTimestamp);

  const { token0Decimals, token1Decimals } = await getLPPoolTokenDecimals(context, config);
  const scalarGrowthDeltaX128 = fungibleUnitValueGrowthX128({
    reserve0: v2State?.reserve0 ?? 0n,
    reserve1: v2State?.reserve1 ?? 0n,
    token0PriceE8: poolState.token0Price,
    token1PriceE8: poolState.token1Price,
    token0Decimals,
    token1Decimals,
    totalSupply: v2State?.lpTotalSupply ?? 0n,
    elapsedSeconds,
    lpRateBps: config.lpRateBps,
  });

  const nextGrowth = {
    ...growth,
    lastTimestamp: cappedTimestamp,
    scalarGrowthX128: growth.scalarGrowthX128 + scalarGrowthDeltaX128,
    lastUpdate: cappedTimestamp,
  };
  context.LPPoolEpochGrowth.set(nextGrowth);
  return nextGrowth;
}

export async function advanceLPPoolGrowth(
  context: handlerContext,
  pool: string,
  throughTimestamp: number,
  newHeaderStartTimestamp?: number
): Promise<void> {
  const state = await context.LeaderboardState.get('current');
  if (!state || state.currentEpochNumber === 0n || !state.isActive) return;

  await advanceLPPoolGrowthForEpoch(
    context,
    pool,
    state.currentEpochNumber,
    throughTimestamp,
    newHeaderStartTimestamp
  );
}

export async function freezeLPGrowthForEpoch(
  context: handlerContext,
  epochNumber: bigint,
  endTimestamp: number
): Promise<void> {
  const registry = await context.LPPoolRegistry.get('global');
  if (!registry || registry.poolIds.length === 0) return;

  const poolIds = [...new Set(registry.poolIds.map(normalizeAddress))];
  await Promise.all(
    poolIds.map(async poolId => {
      const config = await context.LPPoolConfig.get(poolId);
      const growth = config?.isActive
        ? await advanceLPPoolGrowthForEpoch(context, poolId, epochNumber, endTimestamp)
        : await context.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(poolId, epochNumber));
      if (!growth) return;
      assertLPGrowthFreezeTimestamp(growth, endTimestamp);
      if (growth.isFrozen) return;

      context.LPPoolEpochGrowth.set({
        ...growth,
        lastTimestamp: Math.max(growth.lastTimestamp, endTimestamp),
        isFrozen: true,
        frozenAt: endTimestamp,
        lastUpdate: endTimestamp,
      });
    })
  );
}

/**
 * Returns the position's cumulative epoch growth, or `undefined` when growth cannot be
 * determined (no epoch header, pool config, or pool state yet). `undefined` and `0n` are
 * deliberately distinct: `0n` means "this position has provably accrued nothing", while
 * `undefined` means "growth is unknown right now". Collapsing the two lets an unreadable
 * pool report negative growth against an already-persisted nonzero cursor baseline.
 */
export async function readLPPositionGrowthX128(
  context: handlerContext,
  position: UserLPPosition,
  epochNumber: bigint
): Promise<bigint | undefined> {
  const poolId = normalizeAddress(position.pool);
  const growth = await context.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(poolId, epochNumber));
  if (!growth) return undefined;
  const config = await context.LPPoolConfig.get(poolId);
  if (!config) return undefined;

  return growth.scalarGrowthX128;
}

function emptyLPPositionGrowthSettlement(
  epochNumber: bigint,
  throughTimestamp: number
): LPPositionGrowthSettlement {
  return {
    epochNumber,
    growthBaselineX128: 0n,
    currentGrowthX128: 0n,
    pointsEarned: 0n,
    accrualStartTimestamp: throughTimestamp,
    accrualEndTimestamp: throughTimestamp,
    settledAt: throughTimestamp,
  };
}

function assertUserLPEpochCursor(
  cursor: UserLPEpochCursor,
  position: UserLPPosition,
  epochNumber: bigint,
  settledAt: number,
  allowOwnerMismatch = false
): void {
  const positionId = normalizeAddress(position.id);
  const userId = normalizeAddress(position.user_id);
  const poolId = normalizeAddress(position.pool);
  if (cursor.position_id !== positionId) {
    throw new Error(
      `LP cursor position mismatch: cursor=${cursor.id} expected=${positionId} actual=${cursor.position_id}`
    );
  }
  if (!allowOwnerMismatch && cursor.user_id !== userId) {
    throw new Error(
      `LP cursor owner mismatch: cursor=${cursor.id} expected=${userId} actual=${cursor.user_id}`
    );
  }
  if (cursor.pool !== poolId) {
    throw new Error(
      `LP cursor pool mismatch: cursor=${cursor.id} expected=${poolId} actual=${cursor.pool}`
    );
  }
  if (cursor.epochNumber !== epochNumber) {
    throw new Error(
      `LP cursor epoch mismatch: cursor=${cursor.id} expected=${epochNumber.toString()} actual=${cursor.epochNumber.toString()}`
    );
  }
  if (cursor.growthBaselineX128 < 0n) {
    throw new Error(
      `LP cursor growth baseline cannot be negative: cursor=${cursor.id} baseline=${cursor.growthBaselineX128.toString()}`
    );
  }
  if (cursor.lastSettledAt < 0) {
    throw new Error(
      `LP cursor settled timestamp cannot be negative: cursor=${cursor.id} timestamp=${cursor.lastSettledAt}`
    );
  }
  if (cursor.lastUpdate < 0) {
    throw new Error(
      `LP cursor update timestamp cannot be negative: cursor=${cursor.id} timestamp=${cursor.lastUpdate}`
    );
  }
  if (cursor.lastSettledAt > settledAt) {
    throw new Error(
      `LP cursor timestamp moved backward: cursor=${cursor.id} through=${settledAt} last=${cursor.lastSettledAt}`
    );
  }
}

/** @internal Used by the grouped user path after one pool-level advancement. */
export async function settleLPPositionGrowthAfterPoolAdvance(
  context: handlerContext,
  position: UserLPPosition,
  throughTimestamp: number
): Promise<LPPositionGrowthSettlement> {
  if (position.liquidity < 0n) {
    throw new Error(
      `LP position liquidity cannot be negative: position=${position.id} liquidity=${position.liquidity.toString()}`
    );
  }
  const state = await context.LeaderboardState.get('current');
  if (!state || state.currentEpochNumber === 0n) {
    return emptyLPPositionGrowthSettlement(0n, throughTimestamp);
  }

  const epochNumber = state.currentEpochNumber;
  const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
  if (!epoch) return emptyLPPositionGrowthSettlement(epochNumber, throughTimestamp);

  const settledAt =
    !state.isActive && epoch.endTime !== undefined
      ? Math.min(throughTimestamp, epoch.endTime)
      : throughTimestamp;
  const cursorId = userLPEpochCursorId(position.id, epochNumber);
  const [currentGrowthX128, cursor] = await Promise.all([
    readLPPositionGrowthX128(context, position, epochNumber),
    context.UserLPEpochCursor.get(cursorId),
  ]);
  if (!cursor && position.createdAt > epoch.startTime) {
    throw new Error(`missing LP cursor for mid-Tide position ${position.id}`);
  }
  if (cursor) assertUserLPEpochCursor(cursor, position, epochNumber, settledAt);

  const growthBaselineX128 = cursor?.growthBaselineX128 ?? 0n;
  const accrualStartTimestamp = Math.max(cursor?.lastSettledAt ?? epoch.startTime, epoch.startTime);

  // Unknown growth may only be treated as zero when nothing has ever accrued for this
  // position in this epoch - e.g. a pool disabled in a prior Tide that never opened a
  // header for the current one. With a nonzero baseline, growth was demonstrably readable
  // when that baseline was written, so its absence now is transient: defer instead of
  // reporting negative growth and burning the cursor. The accrual window is returned
  // unchanged so callers keep the correct multiplier interval.
  if (currentGrowthX128 === undefined && growthBaselineX128 !== 0n) {
    return {
      epochNumber,
      growthBaselineX128,
      currentGrowthX128: growthBaselineX128,
      pointsEarned: 0n,
      accrualStartTimestamp,
      accrualEndTimestamp: settledAt,
      settledAt,
    };
  }

  const resolvedGrowthX128 = currentGrowthX128 ?? 0n;
  const growthDeltaX128 = resolvedGrowthX128 - growthBaselineX128;
  if (growthDeltaX128 < 0n && resolvedGrowthX128 === 0n) {
    // A zero reading against a nonzero baseline carries no information: this pool's
    // valuation inputs are unavailable at this instant (an uninitialized LPPoolState
    // reads back as tick 0 / price 0, and the value integral multiplies through price).
    // Defer settlement - leaving the cursor intact - instead of mistaking it for a
    // monotonicity violation. A positive-but-smaller reading is still a real inversion
    // and still throws below.
    return {
      epochNumber,
      growthBaselineX128,
      currentGrowthX128: growthBaselineX128,
      pointsEarned: 0n,
      accrualStartTimestamp,
      accrualEndTimestamp: settledAt,
      settledAt,
    };
  }
  if (growthDeltaX128 < 0n) {
    throw new Error(
      `LP growth delta cannot be negative: position=${position.id} epoch=${epochNumber.toString()} current=${resolvedGrowthX128.toString()} baseline=${growthBaselineX128.toString()}`
    );
  }

  context.UserLPEpochCursor.set({
    id: cursorId,
    position_id: normalizeAddress(position.id),
    user_id: normalizeAddress(position.user_id),
    pool: normalizeAddress(position.pool),
    epochNumber,
    growthBaselineX128: resolvedGrowthX128,
    lastSettledAt: settledAt,
    lastUpdate: throughTimestamp,
  });

  return {
    epochNumber,
    growthBaselineX128,
    currentGrowthX128: resolvedGrowthX128,
    pointsEarned: growthToPoints(position.liquidity, growthDeltaX128),
    accrualStartTimestamp,
    accrualEndTimestamp: settledAt,
    settledAt,
  };
}

export async function settleLPPositionGrowth(
  context: handlerContext,
  position: UserLPPosition,
  throughTimestamp: number
): Promise<LPPositionGrowthSettlement> {
  await advanceLPPoolGrowth(context, position.pool, throughTimestamp);
  return await settleLPPositionGrowthAfterPoolAdvance(context, position, throughTimestamp);
}

export async function resetLPPositionGrowthBaseline(
  context: handlerContext,
  position: UserLPPosition,
  epochNumber: bigint,
  timestamp: number
): Promise<void> {
  if (position.liquidity < 0n) {
    throw new Error(
      `LP position liquidity cannot be negative: position=${position.id} liquidity=${position.liquidity.toString()}`
    );
  }
  const cursorId = userLPEpochCursorId(position.id, epochNumber);
  const [currentGrowthX128, epoch, cursor] = await Promise.all([
    readLPPositionGrowthX128(context, position, epochNumber),
    context.LeaderboardEpoch.get(epochNumber.toString()),
    context.UserLPEpochCursor.get(cursorId),
  ]);
  const lastSettledAt =
    epoch?.endTime === undefined ? timestamp : Math.min(timestamp, epoch.endTime);
  // Unknown growth must never overwrite a NONZERO baseline; that would silently zero
  // accrued growth. With no cursor or a zero baseline there is nothing to lose, and the
  // cursor must still be written so a mid-Tide position stays settleable.
  if (currentGrowthX128 === undefined && (cursor?.growthBaselineX128 ?? 0n) !== 0n) return;
  const resolvedGrowthX128 = currentGrowthX128 ?? 0n;
  if (resolvedGrowthX128 < 0n) {
    throw new Error(
      `LP position growth cannot be negative: position=${position.id} epoch=${epochNumber.toString()} current=${resolvedGrowthX128.toString()}`
    );
  }
  if (cursor) {
    assertUserLPEpochCursor(cursor, position, epochNumber, lastSettledAt, true);
    if (context.isPreload !== true) {
      if (cursor.growthBaselineX128 !== resolvedGrowthX128) {
        throw new Error(
          `LP cursor growth is not settled before reset: cursor=${cursor.id} current=${resolvedGrowthX128.toString()} baseline=${cursor.growthBaselineX128.toString()}`
        );
      }
      if (cursor.lastSettledAt !== lastSettledAt) {
        throw new Error(
          `LP cursor timestamp is not settled before reset: cursor=${cursor.id} expected=${lastSettledAt} actual=${cursor.lastSettledAt}`
        );
      }
    }
  }
  context.UserLPEpochCursor.set({
    id: cursorId,
    position_id: normalizeAddress(position.id),
    user_id: normalizeAddress(position.user_id),
    pool: normalizeAddress(position.pool),
    epochNumber,
    growthBaselineX128: resolvedGrowthX128,
    lastSettledAt,
    lastUpdate: timestamp,
  });
}
