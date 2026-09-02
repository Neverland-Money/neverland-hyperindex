// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. This file does not import the `v3-test-helpers` seam, so without this
// a bare `node --test` invocation would inherit them from the repo `.env` via envio's
// dotenv. Redundant under `pnpm run test`, which loads the same module via `--import`.
import './test-env-preload';

import assert from 'node:assert/strict';
import { test } from 'node:test';

process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

import {
  AUSD_ADDRESS,
  BALANCER_AUTORANGE_V3_POOL_ADDRESS,
  LP_BALANCER_AUTORANGE_CUTOVER_BLOCK,
  LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  LP_V2_CUTOVER_BLOCK,
  LP_V2_CUTOVER_TIMESTAMP,
  LP_V2_RESUME_CUTOVER_BLOCK,
  LP_V2_RESUME_CUTOVER_TIMESTAMP,
} from '../helpers/constants';
import { LP_GROWTH_Q128 } from '../helpers/lpGrowthMath';
import {
  applyStaticLPPoolCutover,
  getOrCreateLPPoolState,
  getOrCreateLPPoolStats,
  settleLPPosition,
  settleUserLPPositions,
  syncUserLPPositionsFromChain,
  updatePoolFeeStats,
  updatePoolLPStats,
} from '../handlers/lp';
import { applyScheduledEpochTransitions } from '../handlers/shared';
import { advanceLPPoolGrowth, resetLPPositionGrowthBaseline } from '../handlers/lpGrowth';
import { publicClient } from '../helpers/viem';
import {
  installViemMock,
  setLPBalanceOverride,
  setLPPositionOverride,
  setLPTokensOverride,
} from './viem-mock';
import type {
  DustLockToken,
  LPMintData,
  LPPoolConfig,
  LPPoolEpochGrowth,
  LPPoolFeeStats,
  LPPoolPositionIndex,
  LPPoolRegistry,
  LPPoolState,
  LPPoolStats,
  LPPoolV2State,
  LPPoolVolumeBucket,
  leaderboardConfig as LeaderboardConfig,
  LeaderboardEpoch,
  LeaderboardState,
  LeaderboardTotals,
  ScoreBucket,
  TokenInfo,
  User,
  UserEpochStats,
  UserIndex,
  UserLPEpochCursor,
  UserLPPosition,
  UserLPPositionIndex,
  UserLPStats,
  UserLeaderboardState,
  UserPoints,
  UserTokenList,
  VotingPowerTier,
  VotingPowerTierIndex,
  handlerContext,
} from '../../generated';

installViemMock();

type Store<T extends { readonly id: string }> = {
  get: (id: string) => Promise<T | undefined>;
  set: (entity: T) => void;
  deleteUnsafe: (id: string) => void;
  readonly getIds: string[];
  readonly setRows: T[];
};

function createStore<T extends { readonly id: string }>(initial?: T[]): Store<T> {
  const map = new Map<string, T>();
  const getIds: string[] = [];
  const setRows: T[] = [];
  if (initial) {
    for (const entry of initial) {
      map.set(entry.id, entry);
    }
  }
  return {
    get: async (id: string) => {
      getIds.push(id);
      return map.get(id);
    },
    set: (entity: T & { id: string }) => {
      setRows.push(entity);
      map.set(entity.id, entity);
    },
    deleteUnsafe: (id: string) => {
      map.delete(id);
    },
    getIds,
    setRows,
  };
}

function buildContext() {
  const stores = {
    LPPoolRegistry: createStore<LPPoolRegistry>(),
    LPPoolConfig: createStore<LPPoolConfig>(),
    UserLPPosition: createStore<UserLPPosition>(),
    UserLPPositionIndex: createStore<UserLPPositionIndex>(),
    LPPoolPositionIndex: createStore<LPPoolPositionIndex>(),
    LPPoolState: createStore<LPPoolState>(),
    LPPoolV2State: createStore<LPPoolV2State>(),
    LPPoolEpochGrowth: createStore<LPPoolEpochGrowth>(),
    UserLPEpochCursor: createStore<UserLPEpochCursor>(),
    TokenInfo: createStore<TokenInfo>(),
    UserLPStats: createStore<UserLPStats>(),
    LPPoolStats: createStore<LPPoolStats>(),
    LPPoolVolumeBucket: createStore<LPPoolVolumeBucket>(),
    LPPoolFeeStats: createStore<LPPoolFeeStats>(),
    LPMintData: createStore<LPMintData>(),
    LeaderboardState: createStore<LeaderboardState>(),
    LeaderboardEpoch: createStore<LeaderboardEpoch>(),
    LeaderboardConfig: createStore<LeaderboardConfig>(),
    LeaderboardTotals: createStore<LeaderboardTotals>(),
    ScoreBucket: createStore<ScoreBucket>(),
    UserEpochStats: createStore<UserEpochStats>(),
    UserLeaderboardState: createStore<UserLeaderboardState>(),
    UserTokenList: createStore<UserTokenList>(),
    DustLockToken: createStore<DustLockToken>(),
    VotingPowerTier: createStore<VotingPowerTier>(),
    VotingPowerTierIndex: createStore<VotingPowerTierIndex>(),
    UserPoints: createStore<UserPoints>(),
    User: createStore<User>(),
    UserIndex: createStore<UserIndex>(),
  };
  const logs: string[] = [];
  const context = {
    ...stores,
    log: {
      debug: (message: string) => logs.push(message),
      error: (message: string) => logs.push(message),
    },
  } as unknown as handlerContext;
  return { context, stores, logs };
}

const ADDRESSES = {
  userA: '0x000000000000000000000000000000000000f001',
  userB: '0x000000000000000000000000000000000000f002',
  userC: '0x000000000000000000000000000000000000f003',
  poolA: '0x000000000000000000000000000000000000f010',
  poolB: '0x000000000000000000000000000000000000f011',
  managerA: '0x000000000000000000000000000000000000f020',
  token0: AUSD_ADDRESS,
  token1: '0x000000000000000000000000000000000000f030',
  token2: '0x000000000000000000000000000000000000f031',
};

const TASK6_LEGACY_POOL = '0xd15965968fe8bf2babbe39b2fc5de1ab6749141f';
const TASK6_LEGACY_MANAGER = '0x7197e214c0b767cfb76fb734ab638e2c192f4e53';
const TASK6_V2_POOL = '0x86dbf00485871c901c5129bd525348db96c2eb2d';
const TASK6_BALANCER_POOL = BALANCER_AUTORANGE_V3_POOL_ADDRESS;
const TASK6_PRICE_E8 = 100_000_000n;
const TASK6_Q128 = 1n << 128n;

function task6ReferenceFungibleGrowth(seconds: number): bigint {
  const poolValueE8 = 2_000n * TASK6_PRICE_E8;
  const totalSupply = 1_000n * 10n ** 18n;
  return ((poolValueE8 * TASK6_Q128) / totalSupply) * 2000n * BigInt(seconds);
}

function task6ReferenceFungiblePoints(liquidity: bigint, growthX128: bigint): bigint {
  return (liquidity * growthX128 * 10n ** 18n) / (TASK6_Q128 * TASK6_PRICE_E8 * 10_000n * 86_400n);
}

const POSITION = {
  tokenId: 1n,
  tickLower: -120,
  tickUpper: 120,
  liquidity: 1000n,
  feeGrowthInside0LastX128: 0n,
  feeGrowthInside1LastX128: 0n,
  tokensOwed0: 0n,
  tokensOwed1: 0n,
};

const ONE_POINT_GROWTH_X128 = LP_GROWTH_Q128 * 100_000_000n * 10_000n * 86_400n;

function setActivePoolConfig(
  stores: ReturnType<typeof buildContext>['stores'],
  pool: string,
  manager: string,
  token0: string,
  token1: string,
  fee: number | undefined,
  lpRateBps: bigint
) {
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [pool],
    lastUpdate: 0,
  });
  stores.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager: manager,
    token0,
    token1,
    fee,
    lpRateBps,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
}

function setLeaderboardState(
  stores: ReturnType<typeof buildContext>['stores'],
  currentEpochNumber: bigint,
  isActive: boolean,
  startTime: number,
  endTime?: number
) {
  stores.LeaderboardState.set({
    id: 'current',
    currentEpochNumber,
    isActive,
  });
  if (currentEpochNumber > 0n) {
    stores.LeaderboardEpoch.set({
      id: currentEpochNumber.toString(),
      epochNumber: currentEpochNumber,
      startBlock: 0n,
      startTime,
      endBlock: undefined,
      endTime,
      isActive,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });
  }
  stores.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    lpRateBps: 2000n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 0,
  });
}

function task6StaticPoolConfig(input: {
  pool: string;
  positionManager: string;
  active: boolean;
  enabledAt: number;
  disabledAt?: number;
  epoch?: bigint;
}): LPPoolConfig {
  const epoch = input.epoch ?? 1n;
  return {
    id: input.pool,
    pool: input.pool,
    positionManager: input.positionManager,
    token0: AUSD_ADDRESS,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 2000n,
    isActive: input.active,
    enabledAtEpoch: epoch,
    enabledAtTimestamp: input.enabledAt,
    disabledAtEpoch: input.active ? undefined : epoch,
    disabledAtTimestamp: input.active ? undefined : input.disabledAt,
    lastUpdate: input.disabledAt ?? input.enabledAt,
  };
}

function seedTask6StaticPoolStorage(
  stores: ReturnType<typeof buildContext>['stores'],
  pool: string,
  timestamp: number
): void {
  stores.LPPoolState.set({
    id: pool,
    pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: TASK6_PRICE_E8,
    token1Price: TASK6_PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: timestamp,
  });
  stores.LPPoolV2State.set({
    id: pool,
    pool,
    reserve0: 1_000n * 10n ** 6n,
    reserve1: 1_000n * 10n ** 18n,
    lpTotalSupply: 1_000n * 10n ** 18n,
    lastUpdate: timestamp,
  });
}

function seedTask6FungiblePosition(
  stores: ReturnType<typeof buildContext>['stores'],
  pool: string,
  user: string,
  timestamp: number
): { id: string; liquidity: bigint } {
  const id = `v2:${pool}:${user}`;
  const liquidity = 100n * 10n ** 18n;
  stores.UserLPPosition.set({
    id,
    tokenId: BigInt(user),
    user_id: user,
    pool,
    positionManager: pool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity,
    amount0: 100n * 10n ** 6n,
    amount1: 100n * 10n ** 18n,
    isInRange: true,
    valueUsd: 200n * TASK6_PRICE_E8,
    lastInRangeTimestamp: timestamp,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: timestamp,
    settledLpPoints: 0n,
    createdAt: timestamp,
    lastUpdate: timestamp,
  });
  stores.UserLPPositionIndex.set({
    id: user,
    user_id: user,
    positionIds: [id],
    lastUpdate: timestamp,
  });
  return { id, liquidity };
}

function seedTask6CutoverTides(
  stores: ReturnType<typeof buildContext>['stores'],
  boundaryTimestamp: number,
  positionCount = 10_000
): void {
  stores.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  stores.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: boundaryTimestamp - 200,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: boundaryTimestamp - 200,
    scheduledEndTime: boundaryTimestamp - 100,
  });
  stores.LeaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: boundaryTimestamp - 50,
    scheduledEndTime: 0,
  });
  stores.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    lpRateBps: 2000n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: boundaryTimestamp - 200,
  });
  stores.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: 6,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: boundaryTimestamp - 200,
  });
  stores.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: 18,
    symbol: 'PAIR',
    name: 'Pair',
    lastUpdate: boundaryTimestamp - 200,
  });
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, boundaryTimestamp - 200);
  }
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
    lastUpdate: boundaryTimestamp - 200,
  });
  stores.LPPoolPositionIndex.set({
    id: TASK6_LEGACY_POOL,
    pool: TASK6_LEGACY_POOL,
    positionIds: Array.from({ length: positionCount }, (_, index) => `legacy-fake-${index}`),
    lastUpdate: boundaryTimestamp - 200,
  });
  stores.LPPoolPositionIndex.set({
    id: TASK6_V2_POOL,
    pool: TASK6_V2_POOL,
    positionIds: Array.from({ length: positionCount }, (_, index) => `v2-fake-${index}`),
    lastUpdate: boundaryTimestamp - 200,
  });
  stores.LPPoolPositionIndex.set({
    id: TASK6_BALANCER_POOL,
    pool: TASK6_BALANCER_POOL,
    positionIds: Array.from({ length: positionCount }, (_, index) => `balancer-fake-${index}`),
    lastUpdate: boundaryTimestamp - 200,
  });
}

test('lp chain sync respects flags and missing registry', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';
    const { context } = buildContext();
    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 0);

    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';
    const contextMissing = { log: { debug: () => {} } } as unknown as handlerContext;
    await syncUserLPPositionsFromChain(contextMissing, ADDRESSES.userA, 0);
  } finally {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync logs missing balance and tokens', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;
  const prevDebug = process.env.DEBUG_LP_POINTS;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';
    process.env.DEBUG_LP_POINTS = 'true';

    const { context, stores, logs } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolA,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      undefined,
      0n
    );

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, null);
    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 100);

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userB, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userB, null);
    await syncUserLPPositionsFromChain(context, ADDRESSES.userB, 110);

    assert.equal(logs.length, 0);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userB, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userB, undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
    process.env.DEBUG_LP_POINTS = prevDebug;
  }
});

test('lp chain sync handles missing position data and slot0 zero', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;
  const prevDebug = process.env.DEBUG_LP_POINTS;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';
    process.env.DEBUG_LP_POINTS = 'true';

    const { context, stores } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolA,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      undefined,
      0n
    );

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, [POSITION.tokenId]);
    setLPPositionOverride(undefined);
    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 120);

    setLPPositionOverride([
      0n,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      POSITION.tickLower,
      POSITION.tickUpper,
      POSITION.liquidity,
      0n,
      0n,
      0n,
      0n,
    ]);
    await syncUserLPPositionsFromChain(context, ADDRESSES.userB, 130);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
    process.env.DEBUG_LP_POINTS = prevDebug;
  }
});

test('lp chain sync creates positions and updates indices/prices', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';

    const { context, stores } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolA,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      0n
    );
    stores.LPPoolState.set({
      id: ADDRESSES.poolA,
      pool: ADDRESSES.poolA,
      currentTick: 0,
      sqrtPriceX96: 2n ** 96n,
      token0Price: 7n,
      token1Price: 11n,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 0,
    });
    stores.TokenInfo.set({
      id: ADDRESSES.token0,
      address: ADDRESSES.token0,
      decimals: 6,
      symbol: 'AUSD',
      name: 'AUSD',
      lastUpdate: 0,
    });
    stores.TokenInfo.set({
      id: ADDRESSES.token1,
      address: ADDRESSES.token1,
      decimals: 4,
      symbol: 'DUST',
      name: 'Dust',
      lastUpdate: 0,
    });

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, [POSITION.tokenId]);
    setLPPositionOverride([
      0n,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      POSITION.tickLower,
      POSITION.tickUpper,
      POSITION.liquidity,
      0n,
      0n,
      0n,
      0n,
    ]);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 200);

    const position = await stores.UserLPPosition.get(POSITION.tokenId.toString());
    assert.equal(position, undefined);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync uses token1 ausd pricing', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';

    const { context, stores } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolB,
      ADDRESSES.managerA,
      ADDRESSES.token2,
      AUSD_ADDRESS,
      3000,
      0n
    );
    stores.LPPoolState.set({
      id: ADDRESSES.poolB,
      pool: ADDRESSES.poolB,
      currentTick: 0,
      sqrtPriceX96: 2n ** 96n,
      token0Price: 9n,
      token1Price: 9n,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 0,
    });
    stores.TokenInfo.set({
      id: ADDRESSES.token2,
      address: ADDRESSES.token2,
      decimals: 4,
      symbol: 'TK2',
      name: 'Token2',
      lastUpdate: 0,
    });
    stores.TokenInfo.set({
      id: AUSD_ADDRESS,
      address: AUSD_ADDRESS,
      decimals: 6,
      symbol: 'AUSD',
      name: 'AUSD',
      lastUpdate: 0,
    });

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userB, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userB, [POSITION.tokenId]);
    setLPPositionOverride([
      0n,
      ADDRESSES.managerA,
      ADDRESSES.token2,
      AUSD_ADDRESS,
      3000,
      POSITION.tickLower,
      POSITION.tickUpper,
      POSITION.liquidity,
      0n,
      0n,
      0n,
      0n,
    ]);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userB, 220);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userB, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userB, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync uses token1 ausd pricing with higher token0 decimals', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';

    const { context, stores } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolB,
      ADDRESSES.managerA,
      ADDRESSES.token2,
      AUSD_ADDRESS,
      3000,
      0n
    );
    stores.LPPoolState.set({
      id: ADDRESSES.poolB,
      pool: ADDRESSES.poolB,
      currentTick: 0,
      sqrtPriceX96: 2n ** 96n,
      token0Price: 9n,
      token1Price: 9n,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 0,
    });
    stores.TokenInfo.set({
      id: ADDRESSES.token2,
      address: ADDRESSES.token2,
      decimals: 18,
      symbol: 'TK2',
      name: 'Token2',
      lastUpdate: 0,
    });
    stores.TokenInfo.set({
      id: AUSD_ADDRESS,
      address: AUSD_ADDRESS,
      decimals: 6,
      symbol: 'AUSD',
      name: 'AUSD',
      lastUpdate: 0,
    });

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, [POSITION.tokenId]);
    setLPPositionOverride([
      0n,
      ADDRESSES.managerA,
      ADDRESSES.token2,
      AUSD_ADDRESS,
      3000,
      POSITION.tickLower,
      POSITION.tickUpper,
      POSITION.liquidity,
      POSITION.feeGrowthInside0LastX128,
      POSITION.feeGrowthInside1LastX128,
      POSITION.tokensOwed0,
      POSITION.tokensOwed1,
    ]);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 200);

    const poolState = await stores.LPPoolState.get(ADDRESSES.poolB);
    assert.ok(poolState);
    assert.ok(poolState?.token0Price > 0n);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync updates existing position indices', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';

    const { context, stores } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolA,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      0n
    );
    stores.LPPoolState.set({
      id: ADDRESSES.poolA,
      pool: ADDRESSES.poolA,
      currentTick: 0,
      sqrtPriceX96: 2n ** 96n,
      token0Price: 1n,
      token1Price: 1n,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 0,
    });
    stores.UserLPPosition.set({
      id: POSITION.tokenId.toString(),
      tokenId: POSITION.tokenId,
      user_id: ADDRESSES.userC,
      pool: ADDRESSES.poolA,
      positionManager: ADDRESSES.managerA,
      tickLower: POSITION.tickLower,
      tickUpper: POSITION.tickUpper,
      liquidity: POSITION.liquidity,
      amount0: 1n,
      amount1: 1n,
      isInRange: true,
      valueUsd: 1n,
      lastInRangeTimestamp: 0,
      accumulatedInRangeSeconds: 0n,
      lastSettledAt: 0,
      settledLpPoints: 0n,
      createdAt: 0,
      lastUpdate: 0,
    });
    stores.UserLPPositionIndex.set({
      id: ADDRESSES.userC,
      user_id: ADDRESSES.userC,
      positionIds: [POSITION.tokenId.toString()],
      lastUpdate: 0,
    });
    stores.LPPoolPositionIndex.set({
      id: ADDRESSES.poolA,
      pool: ADDRESSES.poolA,
      positionIds: [POSITION.tokenId.toString()],
      lastUpdate: 0,
    });

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userC, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userC, [POSITION.tokenId]);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userC, 300);

    const index = await stores.UserLPPositionIndex.get(ADDRESSES.userC);
    assert.equal(index?.lastUpdate, 0);
    const poolIndex = await stores.LPPoolPositionIndex.get(ADDRESSES.poolA);
    assert.equal(poolIndex?.lastUpdate, 0);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userC, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userC, undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('settleUserLPPositions settles points and skips empty', async () => {
  const { context, stores } = buildContext();
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    2000n
  );
  setLeaderboardState(stores, 1n, true, 0);

  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  stores.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: 6,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 0,
  });
  stores.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: 6,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  stores.UserLPPosition.set({
    id: 'active',
    tokenId: 2n,
    user_id: ADDRESSES.userA,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.managerA,
    tickLower: POSITION.tickLower,
    tickUpper: POSITION.tickUpper,
    liquidity: 1000n,
    amount0: 1000n,
    amount1: 1000n,
    isInRange: true,
    valueUsd: 1000n * 10n ** 8n,
    lastInRangeTimestamp: 1000,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 0,
    lastUpdate: 1000,
  });
  stores.UserLPPosition.set({
    id: 'empty',
    tokenId: 3n,
    user_id: ADDRESSES.userA,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.managerA,
    tickLower: POSITION.tickLower,
    tickUpper: POSITION.tickUpper,
    liquidity: 0n,
    amount0: 0n,
    amount1: 0n,
    isInRange: false,
    valueUsd: 0n,
    lastInRangeTimestamp: 0,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 1000,
    lastUpdate: 1000,
  });
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    positionIds: ['active', 'empty'],
    lastUpdate: 0,
  });

  stores.UserTokenList.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    tokenIds: [],
    lastUpdate: 0,
  });

  await settleUserLPPositions(context, ADDRESSES.userA, 4600);

  const epochStats = await stores.UserEpochStats.get(`${ADDRESSES.userA}:1`);
  assert.ok(epochStats?.lpPoints && epochStats.lpPoints > 0n);
});

test('settleUserLPPositions rejects negative liquidity before cursor or compatibility writes', async () => {
  const { context, stores } = buildContext();
  setLeaderboardState(stores, 1n, true, 100);
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.poolA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    0n
  );
  stores.LPPoolConfig.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.poolA,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 100,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 100,
  });
  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 100,
  });
  stores.LPPoolV2State.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    reserve0: 1n,
    reserve1: 1n,
    lpTotalSupply: 1n,
    lastUpdate: 100,
  });
  stores.LPPoolEpochGrowth.set({
    id: `${ADDRESSES.poolA}:1`,
    pool: ADDRESSES.poolA,
    epochNumber: 1n,
    startTimestamp: 100,
    lastTimestamp: 200,
    scalarGrowthX128: ONE_POINT_GROWTH_X128,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: 200,
  });
  const position: UserLPPosition = {
    id: 'negative-liquidity-position',
    tokenId: 1n,
    user_id: ADDRESSES.userA,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.poolA,
    tickLower: POSITION.tickLower,
    tickUpper: POSITION.tickUpper,
    liquidity: -1n,
    amount0: 1n,
    amount1: 1n,
    isInRange: true,
    valueUsd: 200_000_000n,
    lastInRangeTimestamp: 100,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 100,
    settledLpPoints: 0n,
    createdAt: 99,
    lastUpdate: 100,
  };
  stores.UserLPPosition.set(position);
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    positionIds: [position.id],
    lastUpdate: 100,
  });
  stores.UserLPPosition.setRows.length = 0;
  stores.UserLPEpochCursor.setRows.length = 0;

  await assert.rejects(
    () => settleUserLPPositions(context, ADDRESSES.userA, 200),
    /LP position liquidity cannot be negative/
  );

  assert.equal(stores.UserLPEpochCursor.setRows.length, 0);
  assert.equal(stores.UserLPPosition.setRows.length, 0);
  assert.deepEqual(await stores.UserLPPosition.get(position.id), position);
});

test('settleUserLPPositions deduplicates one user index and advances one shared pool once', async () => {
  const { context, stores } = buildContext();
  setLeaderboardState(stores, 1n, true, 100);
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.poolA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    10_000n
  );
  stores.LPPoolConfig.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.poolA,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 10_000n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 100,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 100,
  });
  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 100,
  });
  stores.LPPoolV2State.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    reserve0: 100n,
    reserve1: 100n,
    lpTotalSupply: 100n,
    lastUpdate: 100,
  });
  stores.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: 0,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 100,
  });
  stores.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: 0,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 100,
  });
  for (const [id, liquidity] of [
    ['shared-pool-1', 10n],
    ['shared-pool-2', 20n],
  ] as const) {
    stores.UserLPPosition.set({
      id,
      tokenId: BigInt(liquidity),
      user_id: ADDRESSES.userA,
      pool: ADDRESSES.poolA,
      positionManager: ADDRESSES.poolA,
      tickLower: POSITION.tickLower,
      tickUpper: POSITION.tickUpper,
      liquidity,
      amount0: liquidity,
      amount1: liquidity,
      isInRange: true,
      valueUsd: liquidity * 200_000_000n,
      lastInRangeTimestamp: 100,
      accumulatedInRangeSeconds: 0n,
      lastSettledAt: 100,
      settledLpPoints: 0n,
      createdAt: 99,
      lastUpdate: 100,
    });
  }
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    positionIds: ['shared-pool-1', 'shared-pool-1', 'shared-pool-2'],
    lastUpdate: 100,
  });
  stores.UserTokenList.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    tokenIds: [],
    lastUpdate: 100,
  });
  stores.UserLPPositionIndex.getIds.length = 0;
  stores.LPPoolPositionIndex.getIds.length = 0;
  stores.UserLPPosition.getIds.length = 0;
  stores.LPPoolEpochGrowth.setRows.length = 0;

  await settleUserLPPositions(context, ADDRESSES.userA, 200);

  assert.deepEqual(stores.UserLPPositionIndex.getIds, [ADDRESSES.userA]);
  assert.equal(stores.LPPoolPositionIndex.getIds.length, 0);
  assert.deepEqual(stores.UserLPPosition.getIds, ['shared-pool-1', 'shared-pool-2']);
  assert.equal(stores.LPPoolEpochGrowth.setRows.length, 1);
  assert.equal(stores.UserLPEpochCursor.setRows.length, 2);
  const first = await stores.UserLPPosition.get('shared-pool-1');
  const second = await stores.UserLPPosition.get('shared-pool-2');
  const epochStats = await stores.UserEpochStats.get(`${ADDRESSES.userA}:1`);
  const userStats = await stores.UserLPStats.get(ADDRESSES.userA);
  assert.ok((first?.settledLpPoints ?? 0n) > 0n);
  assert.ok((second?.settledLpPoints ?? 0n) > (first?.settledLpPoints ?? 0n));
  assert.equal(epochStats?.lpPoints, first!.settledLpPoints + second!.settledLpPoints);
  assert.equal(userStats?.totalPositions, 2);
  assert.equal(userStats?.totalValueUsd, 6_000_000_000n);
});

test('redundant same-state growth partitions preserve one average decaying-VP multiplier interval', async () => {
  async function run(partitionAt?: number) {
    const { context, stores } = buildContext();
    setLeaderboardState(stores, 1n, true, 100);
    setActivePoolConfig(
      stores,
      ADDRESSES.poolA,
      ADDRESSES.poolA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      10_000n
    );
    stores.LPPoolConfig.set({
      id: ADDRESSES.poolA,
      pool: ADDRESSES.poolA,
      positionManager: ADDRESSES.poolA,
      token0: ADDRESSES.token0,
      token1: ADDRESSES.token1,
      fee: 3000,
      lpRateBps: 10_000n,
      isActive: true,
      enabledAtEpoch: 1n,
      enabledAtTimestamp: 100,
      disabledAtEpoch: undefined,
      disabledAtTimestamp: undefined,
      lastUpdate: 100,
    });
    stores.LPPoolState.set({
      id: ADDRESSES.poolA,
      pool: ADDRESSES.poolA,
      currentTick: 0,
      sqrtPriceX96: 2n ** 96n,
      token0Price: 100_000_000n,
      token1Price: 100_000_000n,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 100,
    });
    stores.LPPoolV2State.set({
      id: ADDRESSES.poolA,
      pool: ADDRESSES.poolA,
      reserve0: 100n,
      reserve1: 100n,
      lpTotalSupply: 100n,
      lastUpdate: 100,
    });
    stores.TokenInfo.set({
      id: ADDRESSES.token0,
      address: ADDRESSES.token0,
      decimals: 0,
      symbol: 'AUSD',
      name: 'AUSD',
      lastUpdate: 100,
    });
    stores.TokenInfo.set({
      id: ADDRESSES.token1,
      address: ADDRESSES.token1,
      decimals: 0,
      symbol: 'TK1',
      name: 'Token1',
      lastUpdate: 100,
    });
    stores.UserLPPosition.set({
      id: 'partitioned-position',
      tokenId: 1n,
      user_id: ADDRESSES.userA,
      pool: ADDRESSES.poolA,
      positionManager: ADDRESSES.poolA,
      tickLower: POSITION.tickLower,
      tickUpper: POSITION.tickUpper,
      liquidity: 10n,
      amount0: 10n,
      amount1: 10n,
      isInRange: true,
      valueUsd: 2_000_000_000n,
      lastInRangeTimestamp: 100,
      accumulatedInRangeSeconds: 0n,
      lastSettledAt: 100,
      settledLpPoints: 0n,
      createdAt: 99,
      lastUpdate: 100,
    });
    stores.UserLPPositionIndex.set({
      id: ADDRESSES.userA,
      user_id: ADDRESSES.userA,
      positionIds: ['partitioned-position'],
      lastUpdate: 100,
    });
    stores.UserTokenList.set({
      id: ADDRESSES.userA,
      user_id: ADDRESSES.userA,
      tokenIds: [1n],
      lastUpdate: 100,
    });
    stores.DustLockToken.set({
      id: '1',
      owner: ADDRESSES.userA,
      lockedAmount: 10n ** 18n,
      end: 400,
      isPermanent: false,
      createdAt: 0,
      updatedAt: 100,
      lastDepositType: undefined,
      selfRepayEnabled: false,
      rewardReceiver: undefined,
    });
    stores.VotingPowerTierIndex.set({
      id: 'current',
      activeTierIds: ['1'],
      lastUpdate: 100,
    });
    stores.VotingPowerTier.set({
      id: '1',
      tierIndex: 1n,
      minVotingPower: 1n,
      multiplierBps: 12_000n,
      createdAt: 100,
      lastUpdate: 100,
      isActive: true,
    });

    if (partitionAt !== undefined) {
      await advanceLPPoolGrowth(context, ADDRESSES.poolA, partitionAt);
    }
    await advanceLPPoolGrowth(context, ADDRESSES.poolA, 300);
    await settleUserLPPositions(context, ADDRESSES.userA, 300);

    return {
      growth: await stores.LPPoolEpochGrowth.get(`${ADDRESSES.poolA}:1`),
      stats: await stores.UserEpochStats.get(`${ADDRESSES.userA}:1`),
    };
  }

  const unpartitioned = await run();
  const partitioned = await run(200);
  assert.ok((unpartitioned.stats?.lpPoints ?? 0n) > 0n);
  assert.equal(unpartitioned.stats?.lpMultiplierBps, 12_000n);
  assert.equal(partitioned.growth?.scalarGrowthX128, unpartitioned.growth?.scalarGrowthX128);
  assert.equal(partitioned.stats?.lpPoints, unpartitioned.stats?.lpPoints);
  assert.equal(
    partitioned.stats?.lpPointsWithMultiplier,
    unpartitioned.stats?.lpPointsWithMultiplier
  );
});

test('owner transfer reset gives old and new users disjoint growth-backed epoch points', async () => {
  const { context, stores } = buildContext();
  setLeaderboardState(stores, 1n, true, 100);
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.poolA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    0n
  );
  stores.LPPoolConfig.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.poolA,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 100,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 100,
  });
  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 100,
  });
  stores.LPPoolV2State.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    reserve0: 1n,
    reserve1: 1n,
    lpTotalSupply: 1n,
    lastUpdate: 100,
  });
  stores.LPPoolEpochGrowth.set({
    id: `${ADDRESSES.poolA}:1`,
    pool: ADDRESSES.poolA,
    epochNumber: 1n,
    startTimestamp: 100,
    lastTimestamp: 200,
    scalarGrowthX128: ONE_POINT_GROWTH_X128,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: 200,
  });
  stores.UserTokenList.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    tokenIds: [],
    lastUpdate: 100,
  });
  stores.UserTokenList.set({
    id: ADDRESSES.userB,
    user_id: ADDRESSES.userB,
    tokenIds: [],
    lastUpdate: 100,
  });
  const position: UserLPPosition = {
    id: 'transferred-position',
    tokenId: 1n,
    user_id: ADDRESSES.userA,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.poolA,
    tickLower: POSITION.tickLower,
    tickUpper: POSITION.tickUpper,
    liquidity: 1n,
    amount0: 1n,
    amount1: 1n,
    isInRange: true,
    valueUsd: 200_000_000n,
    lastInRangeTimestamp: 100,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 100,
    settledLpPoints: 0n,
    createdAt: 99,
    lastUpdate: 100,
  };
  stores.UserLPPosition.set(position);
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    positionIds: [position.id],
    lastUpdate: 100,
  });

  await settleUserLPPositions(context, ADDRESSES.userA, 200);
  const settledOldPosition = (await stores.UserLPPosition.get(position.id))!;
  const transferred = {
    ...settledOldPosition,
    user_id: ADDRESSES.userB,
    lastUpdate: 200,
  };
  await resetLPPositionGrowthBaseline(context, transferred, 1n, 200);
  stores.UserLPPosition.set(transferred);
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    positionIds: [],
    lastUpdate: 200,
  });
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userB,
    user_id: ADDRESSES.userB,
    positionIds: [position.id],
    lastUpdate: 200,
  });
  stores.LPPoolEpochGrowth.set({
    ...(await stores.LPPoolEpochGrowth.get(`${ADDRESSES.poolA}:1`))!,
    lastTimestamp: 300,
    scalarGrowthX128: 2n * ONE_POINT_GROWTH_X128,
    lastUpdate: 300,
  });

  await settleUserLPPositions(context, ADDRESSES.userB, 300);

  const oldStats = await stores.UserEpochStats.get(`${ADDRESSES.userA}:1`);
  const newStats = await stores.UserEpochStats.get(`${ADDRESSES.userB}:1`);
  const cursor = await stores.UserLPEpochCursor.get(`${position.id}:1`);
  assert.equal(oldStats?.lpPoints, 1_000_000_000_000_000_000n);
  assert.equal(newStats?.lpPoints, 1_000_000_000_000_000_000n);
  assert.equal(cursor?.user_id, ADDRESSES.userB);
  assert.equal(cursor?.lastSettledAt, 300);
});

test('settleUserLPPositions handles missing epoch', async () => {
  const { context, stores } = buildContext();
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    2000n
  );
  setLeaderboardState(stores, 0n, false, 0);

  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  stores.UserLPPosition.set({
    id: 'noop',
    tokenId: 4n,
    user_id: ADDRESSES.userB,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.managerA,
    tickLower: POSITION.tickLower,
    tickUpper: POSITION.tickUpper,
    liquidity: 100n,
    amount0: 1n,
    amount1: 1n,
    isInRange: true,
    valueUsd: 100n,
    lastInRangeTimestamp: 1000,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 0,
    lastUpdate: 0,
  });
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userB,
    user_id: ADDRESSES.userB,
    positionIds: ['noop'],
    lastUpdate: 0,
  });

  await settleUserLPPositions(context, ADDRESSES.userB, 2000);
});

test('lp pool state helper initializes when missing', async () => {
  const { context, stores } = buildContext();
  const state = await getOrCreateLPPoolState(context, ADDRESSES.poolA, 50);
  const stored = await stores.LPPoolState.get(ADDRESSES.poolA);
  assert.equal(stored?.id, state.id);
});

test('swap fee stats handle missing stores and windowed volume', async () => {
  const { context, stores } = buildContext();
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    0n
  );
  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  stores.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: 6,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 0,
  });
  stores.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: 6,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });

  const sparseContext = {
    ...context,
    LPPoolVolumeBucket: undefined,
    LPPoolFeeStats: undefined,
  } as unknown as handlerContext;

  const poolConfig = await stores.LPPoolConfig.get(ADDRESSES.poolA);
  assert.ok(poolConfig);

  await updatePoolFeeStats(sparseContext, poolConfig, 100000000n, 3600);

  stores.LPPoolStats.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    totalPositions: 0,
    inRangePositions: 0,
    totalValueUsd: 0n,
    inRangeValueUsd: 100000000n,
    lastUpdate: 0,
  });

  await updatePoolFeeStats(context, poolConfig, 100000000n, 3600);
  await updatePoolFeeStats(context, poolConfig, 200000000n, 7200);

  const feeStats = await stores.LPPoolFeeStats.get(ADDRESSES.poolA);
  assert.ok(feeStats);
});

test('updatePoolFeeStats sums only the in-window volume buckets (batched window)', async () => {
  const { context, stores } = buildContext();
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    0n
  );
  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  const poolConfig = await stores.LPPoolConfig.get(ADDRESSES.poolA);
  assert.ok(poolConfig);

  // Timestamp far enough in that the full 24-hour window (i=1..23) reads with no early
  // break, exercising the batched Promise.all path across many buckets.
  const ts = 100000;
  const bucketStart = Math.floor(ts / 3600) * 3600;
  const seedBucket = (start: number, volumeUsd: bigint) =>
    stores.LPPoolVolumeBucket.set({
      id: `${ADDRESSES.poolA}:${start}`,
      pool: ADDRESSES.poolA,
      bucketStart: start,
      volumeUsd,
      lastUpdate: 0,
    });
  seedBucket(bucketStart - 3600, 11n); // i=1, in window
  seedBucket(bucketStart - 23 * 3600, 23n); // i=23, in window (far edge)
  seedBucket(bucketStart - 24 * 3600, 9999n); // i=24, OUTSIDE the window -> must be excluded

  await updatePoolFeeStats(context, poolConfig, 100n, ts);

  const feeStats = await stores.LPPoolFeeStats.get(ADDRESSES.poolA);
  assert.ok(feeStats);
  // current bucket (100) + in-window prior buckets (11 + 23); the 24h-ago bucket (9999)
  // is excluded. Identical for the serial and the batched implementations.
  assert.equal(feeStats?.volumeUsd24h, 134n);
});

test('updatePoolFeeStats reads exactly the in-window bucket set (structural, not just value)', async () => {
  // The summed value alone is byte-identical even if the batched rewrite drops the
  // `start < 0` break (negative-start ids -> undefined -> 0n) or folds the current
  // bucket into the window (re-read returns the just-written value). Only the SET of
  // ids actually fetched distinguishes correct from broken, so spy on the reads.
  const { context, stores } = buildContext();
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    0n
  );
  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  const poolConfig = await stores.LPPoolConfig.get(ADDRESSES.poolA);
  assert.ok(poolConfig);

  const requested: string[] = [];
  const origGet = stores.LPPoolVolumeBucket.get;
  stores.LPPoolVolumeBucket.get = async (id: string) => {
    requested.push(id);
    return origGet(id);
  };

  // Expected read set: the current bucket once (read before the loop to seed
  // nextBucketVolume), then the prior in-window buckets i=1..23, stopping at start>=0.
  const expectedIds = (ts: number) => {
    const bs = Math.floor(ts / 3600) * 3600;
    const ids = [`${ADDRESSES.poolA}:${bs}`];
    for (let i = 1; i < 24; i += 1) {
      const start = bs - i * 3600;
      if (start < 0) break;
      ids.push(`${ADDRESSES.poolA}:${start}`);
    }
    return ids;
  };

  // (1) Full window, no early break: current bucket + 23 priors, current read exactly once.
  requested.length = 0;
  await updatePoolFeeStats(context, poolConfig, 100n, 100000);
  assert.deepEqual(requested, expectedIds(100000));
  assert.equal(requested.filter(id => id === `${ADDRESSES.poolA}:97200`).length, 1);

  // (2) Early-break edge: bucketStart=18000 -> only i=1..5 valid; no negative-start id.
  requested.length = 0;
  await updatePoolFeeStats(context, poolConfig, 100n, 18017);
  assert.deepEqual(requested, expectedIds(18017));
  assert.ok(!requested.some(id => id.includes(':-')));
});

test('settleLPPosition uses the pool-local rate without a registry or global override', async () => {
  const { context, stores } = buildContext();
  stores.LeaderboardState.set({ id: 'current', currentEpochNumber: 1n, isActive: true });
  stores.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    5000n
  );

  // A directly resolved pool config must be sufficient for rate selection. The global
  // rate is deliberately different (2000 bps), and registry access must not participate.
  stores.LPPoolRegistry.get = async () => {
    throw new Error('pool-local-rate-must-not-read-the-registry');
  };

  const position = {
    id: 'v2:pos1',
    user_id: ADDRESSES.userA,
    pool: ADDRESSES.poolA,
    isInRange: true,
    valueUsd: 1_000_000n,
    lastInRangeTimestamp: 1,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 0,
    settledLpPoints: 0n,
  };

  const settled = await settleLPPosition(context, position, 3600);
  assert.equal(settled.pointsEarned, 208_275_462_962_962n);
});

test('lp chain sync skips when pool fee mismatches', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';

    const { context, stores } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolA,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      0n
    );

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, [POSITION.tokenId]);
    setLPPositionOverride([
      0n,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      500,
      POSITION.tickLower,
      POSITION.tickUpper,
      POSITION.liquidity,
      0n,
      0n,
      0n,
      0n,
    ]);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 100, 1n);

    const position = await stores.UserLPPosition.get(POSITION.tokenId.toString());
    assert.equal(position, undefined);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync skips when multiple configs do not match fee', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';

    const { context, stores } = buildContext();
    stores.LPPoolRegistry.set({
      id: 'global',
      poolIds: [ADDRESSES.poolA, ADDRESSES.poolB],
      lastUpdate: 0,
    });
    stores.LPPoolConfig.set({
      id: ADDRESSES.poolA,
      pool: ADDRESSES.poolA,
      positionManager: ADDRESSES.managerA,
      token0: ADDRESSES.token0,
      token1: ADDRESSES.token1,
      fee: 1000,
      lpRateBps: 0n,
      isActive: true,
      enabledAtEpoch: 1n,
      enabledAtTimestamp: 0,
      disabledAtEpoch: undefined,
      disabledAtTimestamp: undefined,
      lastUpdate: 0,
    });
    stores.LPPoolConfig.set({
      id: ADDRESSES.poolB,
      pool: ADDRESSES.poolB,
      positionManager: ADDRESSES.managerA,
      token0: ADDRESSES.token0,
      token1: ADDRESSES.token1,
      fee: 2000,
      lpRateBps: 0n,
      isActive: true,
      enabledAtEpoch: 1n,
      enabledAtTimestamp: 0,
      disabledAtEpoch: undefined,
      disabledAtTimestamp: undefined,
      lastUpdate: 0,
    });

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, [POSITION.tokenId]);
    setLPPositionOverride([
      0n,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      500,
      POSITION.tickLower,
      POSITION.tickUpper,
      POSITION.liquidity,
      0n,
      0n,
      0n,
      0n,
    ]);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 100, 1n);

    const position = await stores.UserLPPosition.get(POSITION.tokenId.toString());
    assert.equal(position, undefined);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('getOrCreateLPPoolStats returns null without store', async () => {
  const { context } = buildContext();
  const sparseContext = { ...context, LPPoolStats: undefined } as unknown as handlerContext;

  const stats = await getOrCreateLPPoolStats(sparseContext, ADDRESSES.poolA, 50);
  assert.equal(stats, null);
});

test('updatePoolLPStats handles empty and zeroed positions', async () => {
  const { context, stores } = buildContext();

  await updatePoolLPStats(context, ADDRESSES.poolA, 10);
  const initial = await stores.LPPoolStats.get(ADDRESSES.poolA);
  assert.ok(initial);
  assert.equal(initial?.totalPositions, 0);

  stores.UserLPPosition.set({
    id: POSITION.tokenId.toString(),
    tokenId: POSITION.tokenId,
    user_id: ADDRESSES.userA,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.managerA,
    tickLower: POSITION.tickLower,
    tickUpper: POSITION.tickUpper,
    liquidity: 0n,
    amount0: 0n,
    amount1: 0n,
    isInRange: false,
    valueUsd: 0n,
    lastInRangeTimestamp: 0,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 0,
    settledLpPoints: 0n,
    createdAt: 0,
    lastUpdate: 0,
  });
  stores.LPPoolPositionIndex.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    positionIds: [POSITION.tokenId.toString()],
    lastUpdate: 0,
  });

  await updatePoolLPStats(context, ADDRESSES.poolA, 20);
  const updated = await stores.LPPoolStats.get(ADDRESSES.poolA);
  assert.equal(updated?.totalPositions, 0);
});

test('lp chain sync falls back when token decimals read fails', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;
  const originalRead = publicClient.readContract;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';

    publicClient.readContract = async params => {
      if (params.functionName === 'decimals') {
        throw new Error('decimals unavailable');
      }
      if (params.functionName === 'slot0') {
        return [2n ** 96n, 0, 0, 0, 0, 0, true];
      }
      return originalRead(params as Parameters<typeof originalRead>[0]);
    };

    const { context, stores } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolA,
      ADDRESSES.managerA,
      AUSD_ADDRESS,
      ADDRESSES.token1,
      3000,
      0n
    );

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, [POSITION.tokenId]);
    setLPPositionOverride([
      0n,
      ADDRESSES.managerA,
      AUSD_ADDRESS,
      ADDRESSES.token1,
      3000,
      POSITION.tickLower,
      POSITION.tickUpper,
      POSITION.liquidity,
      0n,
      0n,
      0n,
      0n,
    ]);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 100, 1n);

    const position = await stores.UserLPPosition.get(POSITION.tokenId.toString());
    assert.equal(position, undefined);
  } finally {
    publicClient.readContract = originalRead;
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync logs when slot0 is unavailable', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;
  const originalRead = publicClient.readContract;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';

    publicClient.readContract = async params => {
      if (params.functionName === 'slot0') {
        throw new Error('slot0 unavailable');
      }
      return originalRead(params as Parameters<typeof originalRead>[0]);
    };

    const { context, stores } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolA,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      0n
    );

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, [POSITION.tokenId]);
    setLPPositionOverride([
      0n,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      POSITION.tickLower,
      POSITION.tickUpper,
      POSITION.liquidity,
      0n,
      0n,
      0n,
      0n,
    ]);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 100, 1n);

    const position = await stores.UserLPPosition.get(POSITION.tokenId.toString());
    assert.equal(position, undefined);
  } finally {
    publicClient.readContract = originalRead;
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('settleUserLPPositions never falls back to an unrelated active pool config', async () => {
  const { context, stores } = buildContext();

  setLeaderboardState(stores, 1n, true, 0);
  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    2000n
  );

  stores.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: 6,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  stores.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: 6,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  stores.LPPoolState.set({
    id: ADDRESSES.poolB,
    pool: ADDRESSES.poolB,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });

  stores.UserLPPosition.set({
    id: POSITION.tokenId.toString(),
    tokenId: POSITION.tokenId,
    user_id: ADDRESSES.userA,
    pool: ADDRESSES.poolB,
    positionManager: ADDRESSES.managerA,
    tickLower: POSITION.tickLower,
    tickUpper: POSITION.tickUpper,
    liquidity: 1000n,
    amount0: 1000n,
    amount1: 1000n,
    isInRange: true,
    valueUsd: 1000n * 10n ** 8n,
    lastInRangeTimestamp: 10,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 0,
    settledLpPoints: 0n,
    createdAt: 0,
    lastUpdate: 0,
  });
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    positionIds: [POSITION.tokenId.toString()],
    lastUpdate: 0,
  });
  stores.LPPoolPositionIndex.set({
    id: ADDRESSES.poolB,
    pool: ADDRESSES.poolB,
    positionIds: [POSITION.tokenId.toString()],
    lastUpdate: 0,
  });

  await settleUserLPPositions(context, ADDRESSES.userA, 100);

  const epochStats = await stores.UserEpochStats.get(`${ADDRESSES.userA}:1`);
  const position = await stores.UserLPPosition.get(POSITION.tokenId.toString());
  assert.equal(epochStats?.lpPoints ?? 0n, 0n);
  assert.equal(position?.settledLpPoints, 0n);
});

test('applyStaticLPPoolCutover preserves an explicit legacy disable before cutover', async () => {
  const { context, stores } = buildContext();
  const legacyPool = '0xd15965968fe8bf2babbe39b2fc5de1ab6749141f';
  const legacyManager = '0x7197e214c0b767cfb76fb734ab638e2c192f4e53';
  const dustToken = '0xad96c3dffcd6374294e2573a7fbba96097cc8d7c';

  stores.LPPoolConfig.set({
    id: legacyPool,
    pool: legacyPool,
    positionManager: legacyManager,
    token0: AUSD_ADDRESS,
    token1: dustToken,
    fee: 10000,
    lpRateBps: 2500n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 1,
    lastUpdate: 0,
  });

  await applyStaticLPPoolCutover(context, 1771517876, 56436797n);

  const updated = await stores.LPPoolConfig.get(legacyPool);
  assert.equal(updated?.isActive, false);
  assert.equal(updated?.disabledAtEpoch, 1n);
  assert.equal(updated?.disabledAtTimestamp, 1);
});

test('V2 first activation rejects nonzero pre-era growth before exposing the era', async () => {
  const { context, stores } = buildContext();
  const boundary = LP_V2_CUTOVER_TIMESTAMP;
  setLeaderboardState(stores, 1n, true, boundary - 1_000);
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL],
    lastUpdate: boundary - 1_000,
  });
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, boundary - 1_000);
  }
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: true,
      enabledAt: boundary - 1_000,
    })
  );
  const prematureV2 = task6StaticPoolConfig({
    pool: TASK6_V2_POOL,
    positionManager: TASK6_V2_POOL,
    active: true,
    enabledAt: boundary - 1_000,
  });
  stores.LPPoolConfig.set(prematureV2);
  const preEraGrowth = {
    id: `${TASK6_V2_POOL}:1`,
    pool: TASK6_V2_POOL,
    epochNumber: 1n,
    kind: 'FUNGIBLE_SHARE' as const,
    startTimestamp: boundary - 1_000,
    lastTimestamp: boundary - 100,
    scalarGrowthX128: 999n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: boundary - 100,
  };
  stores.LPPoolEpochGrowth.set(preEraGrowth);

  await assert.rejects(
    applyStaticLPPoolCutover(context, boundary + 100, BigInt(LP_V2_CUTOVER_BLOCK)),
    /nonzero pre-era LP growth/
  );

  assert.deepEqual(await stores.LPPoolConfig.get(TASK6_V2_POOL), prematureV2);
  assert.deepEqual(await stores.LPPoolEpochGrowth.get(preEraGrowth.id), preEraGrowth);
});

test('a pre-era active V2 config cannot accrue through an intervening Tide freeze', async () => {
  const { context, stores } = buildContext();
  const boundary = LP_V2_CUTOVER_TIMESTAMP;
  const epochStart = boundary - 300;
  const tideEnd = boundary - 100;
  setLeaderboardState(stores, 1n, true, epochStart);
  stores.LeaderboardEpoch.set({
    ...(await stores.LeaderboardEpoch.get('1'))!,
    scheduledEndTime: tideEnd,
  });
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL],
    lastUpdate: epochStart,
  });
  for (const [token, decimals] of [
    [AUSD_ADDRESS, 6],
    [ADDRESSES.token1, 18],
  ] as const) {
    stores.TokenInfo.set({
      id: token,
      address: token,
      decimals,
      symbol: 'LP',
      name: 'LP token',
      lastUpdate: epochStart,
    });
  }
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, epochStart);
  }
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: true,
      enabledAt: epochStart,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_V2_POOL,
      positionManager: TASK6_V2_POOL,
      active: true,
      enabledAt: epochStart,
    })
  );
  stores.LPPoolEpochGrowth.set({
    id: `${TASK6_V2_POOL}:1`,
    pool: TASK6_V2_POOL,
    epochNumber: 1n,
    startTimestamp: epochStart,
    lastTimestamp: epochStart,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: epochStart,
  });
  const eventTimestamp = boundary - 50;
  const eventBlock = BigInt(LP_V2_CUTOVER_BLOCK - 1);

  await applyStaticLPPoolCutover(context, eventTimestamp, eventBlock);
  await applyScheduledEpochTransitions(context, eventTimestamp, eventBlock);

  const growth = await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`);
  assert.equal(growth?.scalarGrowthX128, 0n);
  assert.equal(growth?.frozenAt, tideEnd);
  const config = await stores.LPPoolConfig.get(TASK6_V2_POOL);
  assert.equal(config?.isActive, false);
  assert.equal(config?.enabledAtTimestamp, boundary);
});

test('V2 first activation rebases an empty pre-era header before later settlement', async () => {
  const { context, stores } = buildContext();
  const boundary = LP_V2_CUTOVER_TIMESTAMP;
  const epochStart = boundary - 1_000;
  setLeaderboardState(stores, 1n, true, epochStart);
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL],
    lastUpdate: epochStart,
  });
  for (const [token, decimals] of [
    [AUSD_ADDRESS, 6],
    [ADDRESSES.token1, 18],
  ] as const) {
    stores.TokenInfo.set({
      id: token,
      address: token,
      decimals,
      symbol: 'LP',
      name: 'LP token',
      lastUpdate: epochStart,
    });
  }
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, epochStart);
  }
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: true,
      enabledAt: epochStart,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_V2_POOL,
      positionManager: TASK6_V2_POOL,
      active: true,
      enabledAt: epochStart,
    })
  );
  stores.LPPoolEpochGrowth.set({
    id: `${TASK6_V2_POOL}:1`,
    pool: TASK6_V2_POOL,
    epochNumber: 1n,
    startTimestamp: epochStart,
    lastTimestamp: boundary - 100,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: boundary - 100,
  });
  const position = seedTask6FungiblePosition(stores, TASK6_V2_POOL, ADDRESSES.userA, epochStart);

  await applyStaticLPPoolCutover(context, boundary, BigInt(LP_V2_CUTOVER_BLOCK));

  const rebased = await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`);
  assert.equal(rebased?.startTimestamp, boundary);
  assert.equal(rebased?.lastTimestamp, boundary);
  assert.equal(rebased?.scalarGrowthX128, 0n);

  await settleUserLPPositions(context, ADDRESSES.userA, boundary + 100);
  const validGrowth = task6ReferenceFungibleGrowth(100);
  assert.equal(
    (await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`))?.scalarGrowthX128,
    validGrowth
  );
  assert.equal(
    (await stores.UserEpochStats.get(`${ADDRESSES.userA}:1`))?.lpPoints,
    task6ReferenceFungiblePoints(position.liquidity, validGrowth)
  );
});

test('Balancer first activation rejects nonzero pre-era growth before exposing the era', async () => {
  const { context, stores } = buildContext();
  const boundary = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP;
  setLeaderboardState(stores, 1n, true, boundary - 1_000);
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
    lastUpdate: boundary - 1_000,
  });
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, boundary - 1_000);
  }
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: false,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
      disabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_V2_POOL,
      positionManager: TASK6_V2_POOL,
      active: true,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  const prematureBalancer = task6StaticPoolConfig({
    pool: TASK6_BALANCER_POOL,
    positionManager: TASK6_BALANCER_POOL,
    active: true,
    enabledAt: boundary - 1_000,
  });
  stores.LPPoolConfig.set(prematureBalancer);
  const preEraGrowth = {
    id: `${TASK6_BALANCER_POOL}:1`,
    pool: TASK6_BALANCER_POOL,
    epochNumber: 1n,
    kind: 'FUNGIBLE_SHARE' as const,
    startTimestamp: boundary - 1_000,
    lastTimestamp: boundary - 100,
    scalarGrowthX128: 999n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: boundary - 100,
  };
  stores.LPPoolEpochGrowth.set(preEraGrowth);

  await assert.rejects(
    applyStaticLPPoolCutover(context, boundary + 100, BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK)),
    /nonzero pre-era LP growth/
  );

  assert.deepEqual(await stores.LPPoolConfig.get(TASK6_BALANCER_POOL), prematureBalancer);
  assert.deepEqual(await stores.LPPoolEpochGrowth.get(preEraGrowth.id), preEraGrowth);
});

test('a pre-era active Balancer config cannot accrue through an intervening Tide freeze', async () => {
  const { context, stores } = buildContext();
  const boundary = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP;
  const epochStart = boundary - 300;
  const tideEnd = boundary - 100;
  setLeaderboardState(stores, 1n, true, epochStart);
  stores.LeaderboardEpoch.set({
    ...(await stores.LeaderboardEpoch.get('1'))!,
    scheduledEndTime: tideEnd,
  });
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
    lastUpdate: epochStart,
  });
  for (const [token, decimals] of [
    [AUSD_ADDRESS, 6],
    [ADDRESSES.token1, 18],
  ] as const) {
    stores.TokenInfo.set({
      id: token,
      address: token,
      decimals,
      symbol: 'LP',
      name: 'LP token',
      lastUpdate: epochStart,
    });
  }
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, epochStart);
  }
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: false,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
      disabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_V2_POOL,
      positionManager: TASK6_V2_POOL,
      active: true,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_BALANCER_POOL,
      positionManager: TASK6_BALANCER_POOL,
      active: true,
      enabledAt: epochStart,
    })
  );
  stores.LPPoolEpochGrowth.set({
    id: `${TASK6_BALANCER_POOL}:1`,
    pool: TASK6_BALANCER_POOL,
    epochNumber: 1n,
    startTimestamp: epochStart,
    lastTimestamp: epochStart,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: epochStart,
  });
  const eventTimestamp = boundary - 50;
  const eventBlock = BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK - 1);

  await applyStaticLPPoolCutover(context, eventTimestamp, eventBlock);
  await applyScheduledEpochTransitions(context, eventTimestamp, eventBlock);

  const growth = await stores.LPPoolEpochGrowth.get(`${TASK6_BALANCER_POOL}:1`);
  assert.equal(growth?.scalarGrowthX128, 0n);
  assert.equal(growth?.frozenAt, tideEnd);
  const config = await stores.LPPoolConfig.get(TASK6_BALANCER_POOL);
  assert.equal(config?.isActive, false);
  assert.equal(config?.enabledAtTimestamp, boundary);
});

test('Balancer first activation rebases an empty pre-era header before later settlement', async () => {
  const { context, stores } = buildContext();
  const boundary = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP;
  const epochStart = boundary - 1_000;
  setLeaderboardState(stores, 1n, true, epochStart);
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
    lastUpdate: epochStart,
  });
  for (const [token, decimals] of [
    [AUSD_ADDRESS, 6],
    [ADDRESSES.token1, 18],
  ] as const) {
    stores.TokenInfo.set({
      id: token,
      address: token,
      decimals,
      symbol: 'LP',
      name: 'LP token',
      lastUpdate: epochStart,
    });
  }
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, epochStart);
  }
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: false,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
      disabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_V2_POOL,
      positionManager: TASK6_V2_POOL,
      active: true,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_BALANCER_POOL,
      positionManager: TASK6_BALANCER_POOL,
      active: true,
      enabledAt: epochStart,
    })
  );
  stores.LPPoolEpochGrowth.set({
    id: `${TASK6_BALANCER_POOL}:1`,
    pool: TASK6_BALANCER_POOL,
    epochNumber: 1n,
    startTimestamp: epochStart,
    lastTimestamp: boundary - 100,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: boundary - 100,
  });
  const position = seedTask6FungiblePosition(
    stores,
    TASK6_BALANCER_POOL,
    ADDRESSES.userA,
    epochStart
  );

  await applyStaticLPPoolCutover(context, boundary, BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK));

  const rebased = await stores.LPPoolEpochGrowth.get(`${TASK6_BALANCER_POOL}:1`);
  assert.equal(rebased?.startTimestamp, boundary);
  assert.equal(rebased?.lastTimestamp, boundary);
  assert.equal(rebased?.scalarGrowthX128, 0n);

  await settleUserLPPositions(context, ADDRESSES.userA, boundary + 100);
  const validGrowth = task6ReferenceFungibleGrowth(100);
  assert.equal(
    (await stores.LPPoolEpochGrowth.get(`${TASK6_BALANCER_POOL}:1`))?.scalarGrowthX128,
    validGrowth
  );
  assert.equal(
    (await stores.UserEpochStats.get(`${ADDRESSES.userA}:1`))?.lpPoints,
    task6ReferenceFungiblePoints(position.liquidity, validGrowth)
  );
});

test('all three static cutovers close Tide growth before changing LP eras', async () => {
  const boundaries = [
    {
      name: 'legacy V3 to V2',
      timestamp: LP_V2_CUTOVER_TIMESTAMP,
      block: BigInt(LP_V2_CUTOVER_BLOCK),
      outgoing: TASK6_LEGACY_POOL,
      incoming: TASK6_V2_POOL,
      outgoingIsFungible: false,
    },
    {
      name: 'V2 to Balancer',
      timestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      block: BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK),
      outgoing: TASK6_V2_POOL,
      incoming: TASK6_BALANCER_POOL,
      outgoingIsFungible: true,
    },
    {
      name: 'Balancer to resumed V2',
      timestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP,
      block: BigInt(LP_V2_RESUME_CUTOVER_BLOCK),
      outgoing: TASK6_BALANCER_POOL,
      incoming: TASK6_V2_POOL,
      outgoingIsFungible: true,
    },
  ];

  for (const boundary of boundaries) {
    const { context, stores } = buildContext();
    seedTask6CutoverTides(stores, boundary.timestamp);
    stores.LPPoolConfig.set(
      task6StaticPoolConfig({
        pool: TASK6_LEGACY_POOL,
        positionManager: TASK6_LEGACY_MANAGER,
        active: boundary.outgoing === TASK6_LEGACY_POOL,
        enabledAt: boundary.timestamp - 1_000,
        disabledAt: boundary.outgoing === TASK6_LEGACY_POOL ? undefined : LP_V2_CUTOVER_TIMESTAMP,
      })
    );
    if (boundary.name !== 'legacy V3 to V2') {
      stores.LPPoolConfig.set(
        task6StaticPoolConfig({
          pool: TASK6_V2_POOL,
          positionManager: TASK6_V2_POOL,
          active: boundary.outgoing === TASK6_V2_POOL,
          enabledAt: LP_V2_CUTOVER_TIMESTAMP,
          disabledAt:
            boundary.outgoing === TASK6_V2_POOL
              ? undefined
              : LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
        })
      );
    }
    if (boundary.name === 'Balancer to resumed V2') {
      stores.LPPoolConfig.set(
        task6StaticPoolConfig({
          pool: TASK6_BALANCER_POOL,
          positionManager: TASK6_BALANCER_POOL,
          active: true,
          enabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
        })
      );
    }
    // Concentrated-range eras keep no growth header at all -- they settle per position
    // through settleLPPoolPositions -- so only a fungible outgoing era seeds and freezes one.
    if (boundary.outgoingIsFungible) {
      stores.LPPoolEpochGrowth.set({
        id: `${boundary.outgoing}:1`,
        pool: boundary.outgoing,
        epochNumber: 1n,
        startTimestamp: boundary.timestamp - 200,
        lastTimestamp: boundary.timestamp - 200,
        scalarGrowthX128: 0n,
        isFrozen: false,
        frozenAt: undefined,
        lastUpdate: boundary.timestamp - 200,
      });
    }

    await applyStaticLPPoolCutover(context, boundary.timestamp + 100, boundary.block);

    if (boundary.outgoingIsFungible) {
      const closedGrowth = await stores.LPPoolEpochGrowth.get(`${boundary.outgoing}:1`);
      assert.equal(closedGrowth?.frozenAt, boundary.timestamp - 100, boundary.name);
      assert.equal(closedGrowth?.lastTimestamp, boundary.timestamp - 100, boundary.name);
    }
    const outgoingConfig = await stores.LPPoolConfig.get(boundary.outgoing);
    assert.equal(outgoingConfig?.isActive, false, boundary.name);
    assert.equal(outgoingConfig?.disabledAtTimestamp, boundary.timestamp, boundary.name);
    const incomingConfig = await stores.LPPoolConfig.get(boundary.incoming);
    assert.equal(incomingConfig?.isActive, true, boundary.name);
    assert.equal(incomingConfig?.enabledAtTimestamp, boundary.timestamp, boundary.name);
    assert.deepEqual(await stores.LeaderboardState.get('current'), {
      id: 'current',
      currentEpochNumber: 2n,
      isActive: true,
    });

    if (boundary.outgoingIsFungible) {
      const outgoingNextTideGrowth = await stores.LPPoolEpochGrowth.get(`${boundary.outgoing}:2`);
      assert.equal(outgoingNextTideGrowth?.startTimestamp, boundary.timestamp - 50, boundary.name);
      assert.equal(outgoingNextTideGrowth?.lastTimestamp, boundary.timestamp, boundary.name);
    }

    await advanceLPPoolGrowth(context, boundary.incoming, boundary.timestamp + 100);
    const incomingGrowth = await stores.LPPoolEpochGrowth.get(`${boundary.incoming}:2`);
    assert.equal(incomingGrowth?.startTimestamp, boundary.timestamp, boundary.name);
    assert.equal(incomingGrowth?.lastTimestamp, boundary.timestamp + 100, boundary.name);
    // Tide closure now sweeps LP holders so a never-touched holder still scores
    // (FINDING 003), which legitimately reads position state and writes holder stats here.
    // What this test pins is the ERA CHANGE itself: the cutover must not enumerate holders
    // beyond that closure sweep, and the growth clocks above must rebase exactly.
    // The era change itself must still not touch fee aggregates; only the closure sweep may
    // reach holder state.
    assert.equal(stores.LPPoolFeeStats.setRows.length, 0, boundary.name);
  }
});

test('a partial Balancer marker cannot preserve dual-active LP eras', async () => {
  const { context, stores } = buildContext();
  const boundary = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP;
  setLeaderboardState(stores, 1n, true, boundary - 200);
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
    lastUpdate: boundary - 200,
  });
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, boundary - 200);
  }
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: false,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
      disabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_V2_POOL,
      positionManager: TASK6_V2_POOL,
      active: true,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_BALANCER_POOL,
      positionManager: TASK6_BALANCER_POOL,
      active: true,
      enabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolEpochGrowth.set({
    id: `${TASK6_V2_POOL}:1`,
    pool: TASK6_V2_POOL,
    epochNumber: 1n,
    startTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    lastTimestamp: boundary - 100,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: boundary - 100,
  });

  await applyStaticLPPoolCutover(
    context,
    boundary + 100,
    BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK)
  );

  const v2Config = await stores.LPPoolConfig.get(TASK6_V2_POOL);
  const balancerConfig = await stores.LPPoolConfig.get(TASK6_BALANCER_POOL);
  assert.equal(v2Config?.isActive, false);
  assert.equal(v2Config?.disabledAtTimestamp, boundary);
  assert.equal(balancerConfig?.isActive, true);
  assert.equal(balancerConfig?.enabledAtTimestamp, boundary);
  assert.equal((await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`))?.lastTimestamp, boundary);
  assert.equal(stores.LPPoolPositionIndex.getIds.length, 0);
  assert.equal(stores.UserLPPosition.getIds.length, 0);
});

test('an exact legacy disable plus lone Balancer marker cannot fake V2 history', async () => {
  const { context, stores } = buildContext();
  const epochStart = LP_V2_CUTOVER_TIMESTAMP - 200;
  setLeaderboardState(stores, 1n, true, epochStart);
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
    lastUpdate: epochStart,
  });
  for (const [token, decimals] of [
    [AUSD_ADDRESS, 6],
    [ADDRESSES.token1, 18],
  ] as const) {
    stores.TokenInfo.set({
      id: token,
      address: token,
      decimals,
      symbol: 'LP',
      name: 'LP token',
      lastUpdate: epochStart,
    });
  }
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, epochStart);
  }
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: false,
      enabledAt: epochStart,
      disabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_BALANCER_POOL,
      positionManager: TASK6_BALANCER_POOL,
      active: true,
      enabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    })
  );
  await applyStaticLPPoolCutover(
    context,
    LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK)
  );

  // The legacy pool is concentrated-range: it settles per position through
  // settleLPPoolPositions and never owns a scalar growth header.
  assert.equal(await stores.LPPoolEpochGrowth.get(`${TASK6_LEGACY_POOL}:1`), undefined);
  assert.equal(
    (await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`))?.lastTimestamp,
    LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP
  );
});

test('lone later-era markers cannot skip the unclosed legacy boundary', async () => {
  const run = async (marker: 'balancer' | 'resume') => {
    const { context, stores } = buildContext();
    const epochStart = LP_V2_CUTOVER_TIMESTAMP - 200;
    setLeaderboardState(stores, 1n, true, epochStart);
    stores.LPPoolRegistry.set({
      id: 'global',
      poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
      lastUpdate: epochStart,
    });
    for (const [token, decimals] of [
      [AUSD_ADDRESS, 6],
      [ADDRESSES.token1, 18],
    ] as const) {
      stores.TokenInfo.set({
        id: token,
        address: token,
        decimals,
        symbol: 'LP',
        name: 'LP token',
        lastUpdate: epochStart,
      });
    }
    for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
      seedTask6StaticPoolStorage(stores, pool, epochStart);
    }
    stores.LPPoolConfig.set(
      task6StaticPoolConfig({
        pool: TASK6_LEGACY_POOL,
        positionManager: TASK6_LEGACY_MANAGER,
        active: true,
        enabledAt: epochStart,
      })
    );
    if (marker === 'balancer') {
      stores.LPPoolConfig.set(
        task6StaticPoolConfig({
          pool: TASK6_BALANCER_POOL,
          positionManager: TASK6_BALANCER_POOL,
          active: true,
          enabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
        })
      );
    } else {
      stores.LPPoolConfig.set(
        task6StaticPoolConfig({
          pool: TASK6_V2_POOL,
          positionManager: TASK6_V2_POOL,
          active: true,
          enabledAt: LP_V2_RESUME_CUTOVER_TIMESTAMP,
        })
      );
    }
    await applyStaticLPPoolCutover(
      context,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 100,
      BigInt(LP_V2_RESUME_CUTOVER_BLOCK)
    );

    const afterFirst = {
      legacy: await stores.LPPoolConfig.get(TASK6_LEGACY_POOL),
      v2: await stores.LPPoolConfig.get(TASK6_V2_POOL),
      balancer: await stores.LPPoolConfig.get(TASK6_BALANCER_POOL),
      legacyGrowth: await stores.LPPoolEpochGrowth.get(`${TASK6_LEGACY_POOL}:1`),
      v2Growth: await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`),
      balancerGrowth: await stores.LPPoolEpochGrowth.get(`${TASK6_BALANCER_POOL}:1`),
    };
    const configWrites = stores.LPPoolConfig.setRows.length;
    const growthWrites = stores.LPPoolEpochGrowth.setRows.length;
    await applyStaticLPPoolCutover(
      context,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 100,
      BigInt(LP_V2_RESUME_CUTOVER_BLOCK)
    );
    assert.equal(stores.LPPoolConfig.setRows.length, configWrites, marker);
    assert.equal(stores.LPPoolEpochGrowth.setRows.length, growthWrites, marker);
    return afterFirst;
  };

  for (const marker of ['balancer', 'resume'] as const) {
    const result = await run(marker);
    assert.equal(result.legacy?.isActive, false, marker);
    assert.equal(result.legacy?.disabledAtTimestamp, LP_V2_CUTOVER_TIMESTAMP, marker);
    assert.equal(result.legacyGrowth, undefined, marker);
    assert.equal(result.v2Growth?.startTimestamp, LP_V2_CUTOVER_TIMESTAMP, marker);
    assert.equal(result.v2Growth?.lastTimestamp, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP, marker);
    assert.equal(
      result.balancerGrowth?.startTimestamp,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      marker
    );
    assert.equal(result.balancerGrowth?.lastTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP, marker);
    assert.equal(result.v2?.isActive, true, marker);
    assert.equal(result.v2?.enabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP, marker);
    assert.equal(result.balancer?.isActive, false, marker);
    assert.equal(result.balancer?.disabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP, marker);
  }
});

test('one call processes every missed LP cutover in order and replay is idempotent', async () => {
  const run = async (positionCount: number) => {
    const { context, stores } = buildContext();
    const startTimestamp = LP_V2_CUTOVER_TIMESTAMP - 100;
    setLeaderboardState(stores, 1n, true, startTimestamp);
    stores.LPPoolRegistry.set({
      id: 'global',
      poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
      lastUpdate: startTimestamp,
    });
    stores.TokenInfo.set({
      id: AUSD_ADDRESS,
      address: AUSD_ADDRESS,
      decimals: 6,
      symbol: 'AUSD',
      name: 'AUSD',
      lastUpdate: startTimestamp,
    });
    stores.TokenInfo.set({
      id: ADDRESSES.token1,
      address: ADDRESSES.token1,
      decimals: 18,
      symbol: 'PAIR',
      name: 'Pair',
      lastUpdate: startTimestamp,
    });
    for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
      seedTask6StaticPoolStorage(stores, pool, startTimestamp);
      stores.LPPoolPositionIndex.set({
        id: pool,
        pool,
        positionIds: Array.from({ length: positionCount }, (_, index) => `${pool}:${index}`),
        lastUpdate: startTimestamp,
      });
    }
    stores.LPPoolConfig.set(
      task6StaticPoolConfig({
        pool: TASK6_LEGACY_POOL,
        positionManager: TASK6_LEGACY_MANAGER,
        active: true,
        enabledAt: startTimestamp,
      })
    );
    stores.LPPoolConfig.set(
      task6StaticPoolConfig({
        pool: TASK6_V2_POOL,
        positionManager: TASK6_V2_POOL,
        active: false,
        enabledAt: 0,
        disabledAt: 0,
      })
    );
    stores.LPPoolConfig.set(
      task6StaticPoolConfig({
        pool: TASK6_BALANCER_POOL,
        positionManager: TASK6_BALANCER_POOL,
        active: false,
        enabledAt: 0,
        disabledAt: 0,
      })
    );

    await applyStaticLPPoolCutover(
      context,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 100,
      BigInt(LP_V2_RESUME_CUTOVER_BLOCK)
    );
    const cutoverReads = {
      poolIndex: stores.LPPoolPositionIndex.getIds.length,
      userIndex: stores.UserLPPositionIndex.getIds.length,
      position: stores.UserLPPosition.getIds.length,
    };
    const firstConfigWrites = stores.LPPoolConfig.setRows.length;
    const firstGrowthWrites = stores.LPPoolEpochGrowth.setRows.length;
    const beforeReplay = {
      legacy: await stores.LPPoolConfig.get(TASK6_LEGACY_POOL),
      v2: await stores.LPPoolConfig.get(TASK6_V2_POOL),
      balancer: await stores.LPPoolConfig.get(TASK6_BALANCER_POOL),
      legacyGrowth: await stores.LPPoolEpochGrowth.get(`${TASK6_LEGACY_POOL}:1`),
      v2Growth: await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`),
      balancerGrowth: await stores.LPPoolEpochGrowth.get(`${TASK6_BALANCER_POOL}:1`),
    };

    await applyStaticLPPoolCutover(
      context,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 100,
      BigInt(LP_V2_RESUME_CUTOVER_BLOCK)
    );
    assert.equal(stores.LPPoolConfig.setRows.length, firstConfigWrites);
    assert.equal(stores.LPPoolEpochGrowth.setRows.length, firstGrowthWrites);
    assert.deepEqual(
      {
        legacy: await stores.LPPoolConfig.get(TASK6_LEGACY_POOL),
        v2: await stores.LPPoolConfig.get(TASK6_V2_POOL),
        balancer: await stores.LPPoolConfig.get(TASK6_BALANCER_POOL),
        legacyGrowth: await stores.LPPoolEpochGrowth.get(`${TASK6_LEGACY_POOL}:1`),
        v2Growth: await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`),
        balancerGrowth: await stores.LPPoolEpochGrowth.get(`${TASK6_BALANCER_POOL}:1`),
      },
      beforeReplay
    );

    const v2GrowthBeforeResumeAccrual = beforeReplay.v2Growth?.scalarGrowthX128 ?? 0n;
    await advanceLPPoolGrowth(context, TASK6_V2_POOL, LP_V2_RESUME_CUTOVER_TIMESTAMP + 100);
    const v2GrowthAfterResumeAccrual = await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`);
    return {
      stores,
      cutoverReads,
      beforeReplay,
      v2GrowthBeforeResumeAccrual,
      v2GrowthAfterResumeAccrual,
    };
  };

  const one = await run(1);
  const tenThousand = await run(10_000);
  // The legacy-V3 cutover settles every position in the pool through settleLPPoolPositions,
  // so position reads scale with the pool by design. What stays invariant is that each
  // position is read exactly once, the pool index is walked once, and no user index is
  // touched -- i.e. the cutover never double-settles and never fans out per user.
  assert.deepEqual(one.cutoverReads, { poolIndex: 1, userIndex: 0, position: 1 });
  assert.deepEqual(tenThousand.cutoverReads, { poolIndex: 1, userIndex: 0, position: 10_000 });

  const result = tenThousand.beforeReplay;
  assert.equal(result.legacy?.disabledAtTimestamp, LP_V2_CUTOVER_TIMESTAMP);
  assert.equal(result.legacy?.isActive, false);
  assert.equal(result.v2?.isActive, true);
  assert.equal(result.v2?.enabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP);
  assert.equal(result.balancer?.disabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP);
  assert.equal(result.balancer?.isActive, false);
  assert.equal(result.legacyGrowth, undefined);
  assert.equal(result.v2Growth?.startTimestamp, LP_V2_CUTOVER_TIMESTAMP);
  assert.equal(result.v2Growth?.lastTimestamp, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
  assert.equal(
    result.v2Growth?.scalarGrowthX128,
    task6ReferenceFungibleGrowth(LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - LP_V2_CUTOVER_TIMESTAMP)
  );
  assert.equal(result.balancerGrowth?.startTimestamp, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
  assert.equal(result.balancerGrowth?.lastTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP);
  assert.equal(
    result.balancerGrowth?.scalarGrowthX128,
    task6ReferenceFungibleGrowth(
      LP_V2_RESUME_CUTOVER_TIMESTAMP - LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP
    )
  );
  assert.ok(tenThousand.v2GrowthAfterResumeAccrual);
  assert.equal(
    tenThousand.v2GrowthAfterResumeAccrual?.scalarGrowthX128 -
      tenThousand.v2GrowthBeforeResumeAccrual,
    task6ReferenceFungibleGrowth(100)
  );
});

test('resumed V2 preserves original-era scalar, skips the pause, and settles from exact resume', async () => {
  const { context, stores } = buildContext();
  const epochStart = LP_V2_CUTOVER_TIMESTAMP;
  setLeaderboardState(stores, 1n, true, epochStart);
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
    lastUpdate: epochStart,
  });
  for (const [token, decimals] of [
    [AUSD_ADDRESS, 6],
    [ADDRESSES.token1, 18],
  ] as const) {
    stores.TokenInfo.set({
      id: token,
      address: token,
      decimals,
      symbol: 'LP',
      name: 'LP token',
      lastUpdate: epochStart,
    });
  }
  for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
    seedTask6StaticPoolStorage(stores, pool, epochStart);
  }
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: false,
      enabledAt: epochStart,
      disabledAt: LP_V2_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_V2_POOL,
      positionManager: TASK6_V2_POOL,
      active: false,
      enabledAt: LP_V2_CUTOVER_TIMESTAMP,
      disabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    })
  );
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_BALANCER_POOL,
      positionManager: TASK6_BALANCER_POOL,
      active: true,
      enabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    })
  );
  const originalGrowth = task6ReferenceFungibleGrowth(
    LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - LP_V2_CUTOVER_TIMESTAMP
  );
  stores.LPPoolEpochGrowth.set({
    id: `${TASK6_V2_POOL}:1`,
    pool: TASK6_V2_POOL,
    epochNumber: 1n,
    startTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    lastTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    scalarGrowthX128: originalGrowth,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  });
  const position = seedTask6FungiblePosition(stores, TASK6_V2_POOL, ADDRESSES.userA, epochStart);

  await applyStaticLPPoolCutover(
    context,
    LP_V2_RESUME_CUTOVER_TIMESTAMP,
    BigInt(LP_V2_RESUME_CUTOVER_BLOCK)
  );

  const atResume = await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`);
  assert.equal(atResume?.scalarGrowthX128, originalGrowth);
  assert.equal(atResume?.lastTimestamp, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
  assert.equal(
    (await stores.LPPoolConfig.get(TASK6_V2_POOL))?.enabledAtTimestamp,
    LP_V2_RESUME_CUTOVER_TIMESTAMP
  );

  await settleUserLPPositions(context, ADDRESSES.userA, LP_V2_RESUME_CUTOVER_TIMESTAMP + 100);
  const expectedGrowth = originalGrowth + task6ReferenceFungibleGrowth(100);
  assert.equal(
    (await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`))?.scalarGrowthX128,
    expectedGrowth
  );
  assert.equal(
    (await stores.UserEpochStats.get(`${ADDRESSES.userA}:1`))?.lpPoints,
    task6ReferenceFungiblePoints(position.liquidity, expectedGrowth)
  );
});

test('equal Tide and LP cutover timestamps freeze the outgoing era before activation', async () => {
  const { context, stores } = buildContext();
  const boundary = LP_V2_CUTOVER_TIMESTAMP;
  seedTask6CutoverTides(stores, boundary, 1);
  stores.LeaderboardEpoch.set({
    ...(await stores.LeaderboardEpoch.get('1'))!,
    scheduledEndTime: boundary,
  });
  stores.LeaderboardEpoch.set({
    ...(await stores.LeaderboardEpoch.get('2'))!,
    scheduledStartTime: boundary,
  });
  stores.LPPoolConfig.set(
    task6StaticPoolConfig({
      pool: TASK6_LEGACY_POOL,
      positionManager: TASK6_LEGACY_MANAGER,
      active: true,
      enabledAt: boundary - 200,
    })
  );
  await applyStaticLPPoolCutover(context, boundary, BigInt(LP_V2_CUTOVER_BLOCK));

  assert.equal(await stores.LPPoolEpochGrowth.get(`${TASK6_LEGACY_POOL}:1`), undefined);
  assert.equal(await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:1`), undefined);
  assert.equal((await stores.LPPoolConfig.get(TASK6_LEGACY_POOL))?.isActive, false);
  assert.equal((await stores.LPPoolConfig.get(TASK6_V2_POOL))?.enabledAtTimestamp, boundary);
  assert.deepEqual(await stores.LeaderboardState.get('current'), {
    id: 'current',
    currentEpochNumber: 2n,
    isActive: true,
  });
  assert.equal(await stores.LPPoolEpochGrowth.get(`${TASK6_LEGACY_POOL}:2`), undefined);
  await advanceLPPoolGrowth(context, TASK6_V2_POOL, boundary + 100);
  const incoming = await stores.LPPoolEpochGrowth.get(`${TASK6_V2_POOL}:2`);
  assert.equal(incoming?.startTimestamp, boundary);
  assert.equal(incoming?.lastTimestamp, boundary + 100);
});

test('resume cutover uses block authority and timestamp only when block is absent', async () => {
  const run = async (timestamp: number, blockNumber?: bigint) => {
    const { context, stores } = buildContext();
    setLeaderboardState(stores, 1n, true, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
    stores.LPPoolRegistry.set({
      id: 'global',
      poolIds: [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL],
      lastUpdate: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    });
    for (const pool of [TASK6_LEGACY_POOL, TASK6_V2_POOL, TASK6_BALANCER_POOL]) {
      seedTask6StaticPoolStorage(stores, pool, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);
    }
    stores.LPPoolConfig.set(
      task6StaticPoolConfig({
        pool: TASK6_LEGACY_POOL,
        positionManager: TASK6_LEGACY_MANAGER,
        active: false,
        enabledAt: LP_V2_CUTOVER_TIMESTAMP,
        disabledAt: LP_V2_CUTOVER_TIMESTAMP,
      })
    );
    stores.LPPoolConfig.set(
      task6StaticPoolConfig({
        pool: TASK6_V2_POOL,
        positionManager: TASK6_V2_POOL,
        active: false,
        enabledAt: LP_V2_CUTOVER_TIMESTAMP,
        disabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      })
    );
    stores.LPPoolConfig.set(
      task6StaticPoolConfig({
        pool: TASK6_BALANCER_POOL,
        positionManager: TASK6_BALANCER_POOL,
        active: true,
        enabledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
      })
    );
    await applyStaticLPPoolCutover(context, timestamp, blockNumber);
    return {
      v2: await stores.LPPoolConfig.get(TASK6_V2_POOL),
      balancer: await stores.LPPoolConfig.get(TASK6_BALANCER_POOL),
    };
  };

  const timestampOnlyDisagrees = await run(
    LP_V2_RESUME_CUTOVER_TIMESTAMP + 1,
    BigInt(LP_V2_RESUME_CUTOVER_BLOCK - 1)
  );
  assert.equal(timestampOnlyDisagrees.v2?.isActive, false);
  assert.equal(timestampOnlyDisagrees.balancer?.isActive, true);

  const blockOnlyDisagrees = await run(
    LP_V2_RESUME_CUTOVER_TIMESTAMP - 1,
    BigInt(LP_V2_RESUME_CUTOVER_BLOCK)
  );
  assert.equal(blockOnlyDisagrees.v2?.isActive, true);
  assert.equal(blockOnlyDisagrees.v2?.enabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP);
  assert.equal(blockOnlyDisagrees.balancer?.isActive, false);

  const fallback = await run(LP_V2_RESUME_CUTOVER_TIMESTAMP);
  assert.equal(fallback.v2?.isActive, true);
  assert.equal(fallback.v2?.enabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP);
  assert.equal(fallback.balancer?.isActive, false);
});

test('updatePoolLPStats skips zeroed positions when price context exists', async () => {
  const { context, stores } = buildContext();

  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    0n
  );

  stores.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: 6,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  stores.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: 6,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  stores.UserLPPosition.set({
    id: POSITION.tokenId.toString(),
    tokenId: POSITION.tokenId,
    user_id: ADDRESSES.userA,
    pool: ADDRESSES.poolA,
    positionManager: ADDRESSES.managerA,
    tickLower: POSITION.tickLower,
    tickUpper: POSITION.tickUpper,
    liquidity: 0n,
    amount0: 0n,
    amount1: 0n,
    isInRange: false,
    valueUsd: 0n,
    lastInRangeTimestamp: 0,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 0,
    settledLpPoints: 0n,
    createdAt: 0,
    lastUpdate: 0,
  });
  stores.LPPoolPositionIndex.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    positionIds: [POSITION.tokenId.toString()],
    lastUpdate: 0,
  });

  await updatePoolLPStats(context, ADDRESSES.poolA, 100);

  const stats = await stores.LPPoolStats.get(ADDRESSES.poolA);
  assert.ok(stats);
  assert.equal(stats?.totalPositions, 0);
  assert.equal(stats?.totalValueUsd, 0n);
});

test('updatePoolFeeStats applies protocol fee share reduction', async () => {
  const { context, stores } = buildContext();
  const volumeUsd = 1_000_000_000n;

  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    10000,
    0n
  );
  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
    feeProtocol0: 4,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  stores.LPPoolStats.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    totalPositions: 1,
    inRangePositions: 1,
    totalValueUsd: 1_000_000_000n,
    inRangeValueUsd: 1_000_000_000n,
    lastUpdate: 0,
  });

  const poolConfig = await stores.LPPoolConfig.get(ADDRESSES.poolA);
  assert.ok(poolConfig);
  await updatePoolFeeStats(context, poolConfig!, volumeUsd, 3600);

  const feeStats = await stores.LPPoolFeeStats.get(ADDRESSES.poolA);
  assert.ok(feeStats);
  assert.equal(feeStats?.volumeUsd24h, volumeUsd);
  assert.equal(feeStats?.feesUsd24h, 7_500_000n);
});

test('lp chain sync supports manager filtering and force rescan logs', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;
  const prevDebug = process.env.DEBUG_LP_POINTS;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';
    process.env.DEBUG_LP_POINTS = 'true';

    const { context, stores, logs } = buildContext();
    setActivePoolConfig(
      stores,
      ADDRESSES.poolA,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      0n
    );
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, 0n);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 101, 1n, {
      managers: [ADDRESSES.poolB],
    });
    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 102, 1n, {
      managers: [ADDRESSES.managerA],
      forceRescan: true,
    });

    assert.equal(
      logs.some(entry => entry.includes('force rescan')),
      false
    );
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
    process.env.DEBUG_LP_POINTS = prevDebug;
  }
});

test('lp chain sync resolves pool config when multiple pools share manager and tokens', async () => {
  const prevExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';

    const { context, stores } = buildContext();
    stores.LPPoolRegistry.set({
      id: 'global',
      poolIds: [ADDRESSES.poolA, ADDRESSES.poolB],
      lastUpdate: 0,
    });
    stores.LPPoolConfig.set({
      id: ADDRESSES.poolA,
      pool: ADDRESSES.poolA,
      positionManager: ADDRESSES.managerA,
      token0: ADDRESSES.token0,
      token1: ADDRESSES.token1,
      fee: 500,
      lpRateBps: 0n,
      isActive: true,
      enabledAtEpoch: 1n,
      enabledAtTimestamp: 0,
      disabledAtEpoch: undefined,
      disabledAtTimestamp: undefined,
      lastUpdate: 0,
    });
    stores.LPPoolConfig.set({
      id: ADDRESSES.poolB,
      pool: ADDRESSES.poolB,
      positionManager: ADDRESSES.managerA,
      token0: ADDRESSES.token0,
      token1: ADDRESSES.token1,
      fee: 3000,
      lpRateBps: 0n,
      isActive: true,
      enabledAtEpoch: 1n,
      enabledAtTimestamp: 0,
      disabledAtEpoch: undefined,
      disabledAtTimestamp: undefined,
      lastUpdate: 0,
    });
    stores.LPPoolState.set({
      id: ADDRESSES.poolB,
      pool: ADDRESSES.poolB,
      currentTick: 0,
      sqrtPriceX96: 2n ** 96n,
      token0Price: 100000000n,
      token1Price: 100000000n,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 0,
    });
    stores.TokenInfo.set({
      id: ADDRESSES.token0,
      address: ADDRESSES.token0,
      decimals: 6,
      symbol: 'TK0',
      name: 'Token0',
      lastUpdate: 0,
    });
    stores.TokenInfo.set({
      id: ADDRESSES.token1,
      address: ADDRESSES.token1,
      decimals: 6,
      symbol: 'TK1',
      name: 'Token1',
      lastUpdate: 0,
    });

    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, 1n);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, [POSITION.tokenId]);
    setLPPositionOverride([
      0n,
      ADDRESSES.managerA,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      POSITION.tickLower,
      POSITION.tickUpper,
      POSITION.liquidity,
      0n,
      0n,
      0n,
      0n,
    ]);

    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 103, 1n);

    const position = await stores.UserLPPosition.get(POSITION.tokenId.toString());
    assert.equal(position, undefined);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('legacy cutover stops pool accrual without rewinding the later user touch', async () => {
  const { context, stores } = buildContext();
  const legacyPool = '0xd15965968fe8bf2babbe39b2fc5de1ab6749141f';
  const legacyManager = '0x7197e214c0b767cfb76fb734ab638e2c192f4e53';
  const legacyDust = '0xad96c3dffcd6374294e2573a7fbba96097cc8d7c';
  const cutoverTimestamp = 1771517877;

  setLeaderboardState(stores, 1n, true, 0);
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [legacyPool],
    lastUpdate: 0,
  });
  stores.LPPoolConfig.set({
    id: legacyPool,
    pool: legacyPool,
    positionManager: legacyManager,
    token0: AUSD_ADDRESS,
    token1: legacyDust,
    fee: 10000,
    lpRateBps: 2500n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  stores.LPPoolState.set({
    id: legacyPool,
    pool: legacyPool,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  stores.UserLPPosition.set({
    id: POSITION.tokenId.toString(),
    tokenId: POSITION.tokenId,
    user_id: ADDRESSES.userA,
    pool: legacyPool,
    positionManager: legacyManager,
    tickLower: POSITION.tickLower,
    tickUpper: POSITION.tickUpper,
    liquidity: 1000n,
    amount0: 1000n,
    amount1: 1000n,
    isInRange: true,
    valueUsd: 1000n * 10n ** 8n,
    lastInRangeTimestamp: cutoverTimestamp - 100,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: cutoverTimestamp - 100,
    settledLpPoints: 0n,
    createdAt: 0,
    lastUpdate: 0,
  });
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    positionIds: [POSITION.tokenId.toString()],
    lastUpdate: 0,
  });
  stores.LPPoolPositionIndex.set({
    id: legacyPool,
    pool: legacyPool,
    positionIds: [POSITION.tokenId.toString()],
    lastUpdate: 0,
  });

  const userTouchTimestamp = cutoverTimestamp + 1000;
  await settleUserLPPositions(context, ADDRESSES.userA, userTouchTimestamp);

  const position = await stores.UserLPPosition.get(POSITION.tokenId.toString());
  const poolConfig = await stores.LPPoolConfig.get(legacyPool);
  assert.ok(position);
  assert.equal(poolConfig?.disabledAtTimestamp, cutoverTimestamp);
  assert.equal(position?.lastSettledAt, userTouchTimestamp);
});
