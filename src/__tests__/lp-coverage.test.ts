import assert from 'node:assert/strict';
import { test } from 'node:test';

process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

import { AUSD_ADDRESS } from '../helpers/constants';
import {
  getOrCreateLPPoolState,
  getOrCreateLPPoolStats,
  settleAllLPPoolPositions,
  settleUserLPPositions,
  syncUserLPPositionsFromChain,
  updatePoolFeeStats,
  updatePoolLPStats,
} from '../handlers/lp';
import { publicClient } from '../helpers/viem';
import {
  installViemMock,
  setLPBalanceOverride,
  setLPPositionOverride,
  setLPTokensOverride,
} from './viem-mock';
import type { handlerContext } from '../../generated';
import type {
  DustLockToken_t,
  LeaderboardConfig_t,
  LeaderboardEpoch_t,
  LeaderboardState_t,
  LeaderboardTotals_t,
  LPMintData_t,
  LPPoolConfig_t,
  LPPoolFeeStats_t,
  LPPoolPositionIndex_t,
  LPPoolRegistry_t,
  LPPoolState_t,
  LPPoolStats_t,
  LPPoolVolumeBucket_t,
  ScoreBucket_t,
  TokenInfo_t,
  TopK_t,
  TopKEntry_t,
  UserEpochStats_t,
  UserLeaderboardState_t,
  UserLPBaseline_t,
  UserLPPositionIndex_t,
  UserLPPosition_t,
  UserLPStats_t,
  UserPoints_t,
  UserTokenList_t,
  UserIndex_t,
  User_t,
  VotingPowerTier_t,
} from '../../generated/src/db/Entities.gen';

installViemMock();

type Store<T extends { readonly id: string }> = {
  get: (id: string) => Promise<T | undefined>;
  set: (entity: T) => void;
  deleteUnsafe: (id: string) => void;
};

function createStore<T extends { readonly id: string }>(initial?: T[]): Store<T> {
  const map = new Map<string, T>();
  if (initial) {
    for (const entry of initial) {
      map.set(entry.id, entry);
    }
  }
  return {
    get: async (id: string) => map.get(id),
    set: (entity: T & { id: string }) => {
      map.set(entity.id, entity);
    },
    deleteUnsafe: (id: string) => {
      map.delete(id);
    },
  };
}

function buildContext() {
  const stores = {
    LPPoolRegistry: createStore<LPPoolRegistry_t>(),
    LPPoolConfig: createStore<LPPoolConfig_t>(),
    UserLPBaseline: createStore<UserLPBaseline_t>(),
    UserLPPosition: createStore<UserLPPosition_t>(),
    UserLPPositionIndex: createStore<UserLPPositionIndex_t>(),
    LPPoolPositionIndex: createStore<LPPoolPositionIndex_t>(),
    LPPoolState: createStore<LPPoolState_t>(),
    TokenInfo: createStore<TokenInfo_t>(),
    UserLPStats: createStore<UserLPStats_t>(),
    LPPoolStats: createStore<LPPoolStats_t>(),
    LPPoolVolumeBucket: createStore<LPPoolVolumeBucket_t>(),
    LPPoolFeeStats: createStore<LPPoolFeeStats_t>(),
    LPMintData: createStore<LPMintData_t>(),
    LeaderboardState: createStore<LeaderboardState_t>(),
    LeaderboardEpoch: createStore<LeaderboardEpoch_t>(),
    LeaderboardConfig: createStore<LeaderboardConfig_t>(),
    LeaderboardTotals: createStore<LeaderboardTotals_t>(),
    ScoreBucket: createStore<ScoreBucket_t>(),
    UserEpochStats: createStore<UserEpochStats_t>(),
    UserLeaderboardState: createStore<UserLeaderboardState_t>(),
    UserTokenList: createStore<UserTokenList_t>(),
    DustLockToken: createStore<DustLockToken_t>(),
    VotingPowerTier: createStore<VotingPowerTier_t>(),
    UserPoints: createStore<UserPoints_t>(),
    User: createStore<User_t>(),
    UserIndex: createStore<UserIndex_t>(),
    TopK: createStore<TopK_t>(),
    TopKEntry: createStore<TopKEntry_t>(),
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

test('lp chain sync respects flags and missing registry', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'true';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'true';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';
    const { context } = buildContext();
    await syncUserLPPositionsFromChain(context, ADDRESSES.userA, 0);

    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = 'true';
    const contextMissing = { log: { debug: () => {} } } as unknown as handlerContext;
    await syncUserLPPositionsFromChain(contextMissing, ADDRESSES.userA, 0);
  } finally {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync logs missing balance and tokens', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;
  const prevDebug = process.env.DEBUG_LP_POINTS;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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

    assert.ok(logs.length > 0);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userB, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userB, undefined);
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
    process.env.DEBUG_LP_POINTS = prevDebug;
  }
});

test('lp chain sync handles missing position data and slot0 zero', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;
  const prevDebug = process.env.DEBUG_LP_POINTS;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
    process.env.DEBUG_LP_POINTS = prevDebug;
  }
});

test('lp chain sync creates positions and updates indices/prices', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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
    assert.ok(position);
    const baseline = await stores.UserLPBaseline.get(`${ADDRESSES.userA}:${ADDRESSES.managerA}`);
    assert.ok(baseline);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync uses token1 ausd pricing', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync uses token1 ausd pricing with higher token0 decimals', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync updates existing position indices', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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
    assert.equal(index?.lastUpdate, 300);
    const poolIndex = await stores.LPPoolPositionIndex.get(ADDRESSES.poolA);
    assert.equal(poolIndex?.lastUpdate, 300);
  } finally {
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userC, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userC, undefined);
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
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
    createdAt: 1000,
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

test('settleLPPoolPositions handles empty and active pools', async () => {
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
  setLeaderboardState(stores, 1n, false, 0, 500);

  stores.LPPoolState.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 100000000n,
    token1Price: 100000000n,
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

  await settleAllLPPoolPositions(context, 400);

  stores.UserLPPosition.set({
    id: POSITION.tokenId.toString(),
    tokenId: POSITION.tokenId,
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
    lastInRangeTimestamp: 100,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 100,
    settledLpPoints: 0n,
    createdAt: 100,
    lastUpdate: 100,
  });
  stores.LPPoolPositionIndex.set({
    id: ADDRESSES.poolA,
    pool: ADDRESSES.poolA,
    positionIds: [POSITION.tokenId.toString()],
    lastUpdate: 0,
  });
  stores.UserLPPositionIndex.set({
    id: ADDRESSES.userA,
    user_id: ADDRESSES.userA,
    positionIds: [POSITION.tokenId.toString()],
    lastUpdate: 0,
  });

  await settleAllLPPoolPositions(context, 600);

  const stats = await stores.LPPoolStats.get(ADDRESSES.poolA);
  assert.ok(stats);
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

test('lp chain sync skips when pool fee mismatches', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync skips when multiple configs do not match fee', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
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

test('settleAllLPPoolPositions skips stats when store missing', async () => {
  const { context, stores } = buildContext();
  const sparseContext = { ...context, LPPoolStats: undefined } as unknown as handlerContext;

  setActivePoolConfig(
    stores,
    ADDRESSES.poolA,
    ADDRESSES.managerA,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    0n
  );

  await settleAllLPPoolPositions(sparseContext, 100);
});

test('lp chain sync falls back when token decimals read fails', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;
  const originalRead = publicClient.readContract;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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
    assert.ok(position);
  } finally {
    publicClient.readContract = originalRead;
    setLPBalanceOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPTokensOverride(ADDRESSES.managerA, ADDRESSES.userA, undefined);
    setLPPositionOverride(undefined);
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('lp chain sync logs when slot0 is unavailable', async () => {
  const prevExternal = process.env.ENVIO_DISABLE_EXTERNAL_CALLS;
  const prevEth = process.env.ENVIO_DISABLE_ETH_CALLS;
  const prevSync = process.env.ENVIO_ENABLE_LP_CHAIN_SYNC;
  const originalRead = publicClient.readContract;

  try {
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = 'false';
    process.env.ENVIO_DISABLE_ETH_CALLS = 'false';
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
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS = prevExternal;
    process.env.ENVIO_DISABLE_ETH_CALLS = prevEth;
    process.env.ENVIO_ENABLE_LP_CHAIN_SYNC = prevSync;
  }
});

test('settleUserLPPositions uses single active pool config fallback', async () => {
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
  assert.ok(epochStats);
});
