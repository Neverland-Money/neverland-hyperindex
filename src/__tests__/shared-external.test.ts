import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { LEADERBOARD_START_BLOCK } from '../helpers/constants';
import { createDefaultReserve } from '../helpers/entityHelpers';
import { settlePointsForUser } from '../handlers/shared';
import {
  VIEM_PARTIAL_ADDRESS,
  VIEM_ERROR_ADDRESS,
  VIEM_NO_NFT_ADDRESS,
  VIEM_SECOND_NFT_ADDRESS,
  VIEM_ZERO_BALANCE_ADDRESS,
  installViemMock,
  setLPBalanceOverride,
  setLPPositionOverride,
  setLPTokensOverride,
} from './viem-mock';
import type { handlerContext } from '../../generated';
import type { t as MockDb } from '../../generated/src/TestHelpers_MockDb.gen';
import type {
  DustLockToken_t,
  LeaderboardConfig_t,
  LeaderboardEpoch_t,
  LeaderboardState_t,
  LPPoolConfig_t,
  LPPoolPositionIndex_t,
  LPPoolRegistry_t,
  LPPoolState_t,
  NFTMultiplierConfig_t,
  NFTPartnershipRegistryState_t,
  TokenInfo_t,
  UserEpochStats_t,
  UserLeaderboardState_t,
  UserLPBaseline_t,
  UserLPPosition_t,
  UserLPPositionIndex_t,
  UserLPStats_t,
  UserNFTBaseline_t,
  UserNFTOwnership_t,
  UserReserveList_t,
  UserTokenList_t,
  VotingPowerTier_t,
} from '../../generated/src/db/Entities.gen';

const ADDRESSES = {
  user: '0x000000000000000000000000000000000000e001',
  pool: '0x000000000000000000000000000000000000e002',
  asset: '0x000000000000000000000000000000000000e003',
  aToken: '0x000000000000000000000000000000000000e004',
  lpPool: '0x000000000000000000000000000000000000e005',
  positionManager: '0x000000000000000000000000000000000000e006',
  token0: '0x000000000000000000000000000000000000e007',
  token1: '0x000000000000000000000000000000000000e008',
};

const LP_TOKEN_ID = 1n;
const LP_TICK_LOWER = -120;
const LP_TICK_UPPER = 120;
const LP_LIQUIDITY = 1_000_000n;
const LP_SQRT_PRICE_X96 = 2n ** 96n;
const LP_PRICE_E8 = 100000000n;
const LP_DECIMALS = 6;

type TestHelpersApi = typeof import('../../generated').TestHelpers;

function loadTestHelpers(): TestHelpersApi {
  const cwd = process.cwd();
  const distTestRoot = path.join(cwd, 'dist-test');
  const generatedLink = path.join(distTestRoot, 'generated');

  const generatedIndex = path.join(generatedLink, 'index.js');
  if (!fs.existsSync(generatedIndex)) {
    if (fs.existsSync(generatedLink)) {
      fs.rmSync(generatedLink, { recursive: true, force: true });
    }
    fs.symlinkSync(path.join(cwd, 'generated'), generatedLink, 'dir');
  }

  const handlerModules = [
    'tokenization',
    'leaderboard',
    'leaderboardKeeper',
    'dustlock',
    'pool',
    'nft',
    'config',
    'rewards',
  ];
  for (const handler of handlerModules) {
    require(path.join(distTestRoot, 'src', 'handlers', `${handler}.js`));
  }

  return require(path.join(cwd, 'generated', 'src', 'TestHelpers.res.js'));
}

function createEventDataFactory() {
  let counter = 1;
  return (blockNumber: number, timestamp: number, srcAddress: string) => {
    const txHash = `0x${counter.toString(16).padStart(64, '0')}`;
    const mockEventData = {
      block: { number: blockNumber, timestamp },
      logIndex: counter,
      srcAddress,
      transaction: { hash: txHash },
    };
    counter += 1;
    return { mockEventData };
  };
}

type StoreEntity<T extends { readonly id: string }> = {
  get: (id: string) => Promise<T | undefined>;
  set: (entity: T) => void;
  deleteUnsafe: (id: string) => void;
};

function createStore<T extends { readonly id: string }>(): StoreEntity<T> {
  const map = new Map<string, T>();
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

test('settlements sync NFT ownership from chain when enabled', async () => {
  const previousExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const previousEth = process.env.DISABLE_ETH_CALLS;
  const previousChainSync = process.env.ENABLE_NFT_CHAIN_SYNC;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
  process.env.ENABLE_NFT_CHAIN_SYNC = 'true';
  installViemMock();

  try {
    const TestHelpers = loadTestHelpers();
    let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();

    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: BigInt(LEADERBOARD_START_BLOCK),
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });
    mockDb = mockDb.entities.LeaderboardConfig.set({
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
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.NFTMultiplierConfig.set({
      id: 'current',
      firstBonus: 40000n,
      decayRatio: 10000n,
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.NFTPartnershipRegistryState.set({
      id: 'current',
      activeCollections: [
        VIEM_PARTIAL_ADDRESS,
        VIEM_SECOND_NFT_ADDRESS,
        VIEM_ZERO_BALANCE_ADDRESS,
        VIEM_NO_NFT_ADDRESS,
        VIEM_ERROR_ADDRESS,
      ],
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.UserLeaderboardState.set({
      id: ADDRESSES.user,
      user_id: ADDRESSES.user,
      nftCount: 0n,
      nftMultiplier: 10000n,
      votingPower: 0n,
      vpTierIndex: 0n,
      vpMultiplier: 50000n,
      combinedMultiplier: 10000n,
      totalEpochsParticipated: 0n,
      lifetimePoints: 0n,
      currentEpochId: undefined,
      currentEpochRank: undefined,
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.Reserve.set({
      ...createDefaultReserve(
        `${ADDRESSES.asset}-${ADDRESSES.pool}`,
        ADDRESSES.pool,
        ADDRESSES.asset
      ),
      decimals: 6,
      liquidityIndex: 10n ** 27n,
      variableBorrowIndex: 10n ** 27n,
      lastUpdateTimestamp: 0,
      isActive: true,
      borrowingEnabled: true,
    });
    mockDb = mockDb.entities.SubToken.set({
      id: ADDRESSES.aToken,
      pool_id: ADDRESSES.pool,
      tokenContractImpl: undefined,
      underlyingAssetAddress: ADDRESSES.asset,
      underlyingAssetDecimals: 6,
    });
    mockDb = mockDb.entities.UserNFTOwnership.set({
      id: `${ADDRESSES.user}:${VIEM_PARTIAL_ADDRESS}`,
      user_id: ADDRESSES.user,
      partnership_id: VIEM_PARTIAL_ADDRESS,
      balance: 1n,
      hasNFT: true,
      lastCheckedAt: 0,
      lastCheckedBlock: 0n,
    });
    mockDb = mockDb.entities.UserNFTOwnership.set({
      id: `${ADDRESSES.user}:${VIEM_ZERO_BALANCE_ADDRESS}`,
      user_id: ADDRESSES.user,
      partnership_id: VIEM_ZERO_BALANCE_ADDRESS,
      balance: 1n,
      hasNFT: true,
      lastCheckedAt: 0,
      lastCheckedBlock: 0n,
    });

    const mint = TestHelpers.AToken.Mint.createMockEvent({
      caller: ADDRESSES.user,
      onBehalfOf: ADDRESSES.user,
      value: 10n,
      balanceIncrease: 0n,
      index: 10n ** 27n,
      ...eventData(LEADERBOARD_START_BLOCK + 1, 1000, ADDRESSES.aToken),
    });
    mockDb = await TestHelpers.AToken.Mint.processEvent({ event: mint, mockDb });

    const ownership = mockDb.entities.UserNFTOwnership.get(
      `${ADDRESSES.user}:${VIEM_PARTIAL_ADDRESS}`
    );
    assert.ok(ownership);
    assert.ok(mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${VIEM_SECOND_NFT_ADDRESS}`));
    assert.equal(
      mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${VIEM_ZERO_BALANCE_ADDRESS}`),
      undefined
    );
    assert.equal(
      mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${VIEM_NO_NFT_ADDRESS}`),
      undefined
    );
    assert.equal(
      mockDb.entities.UserLeaderboardState.get(ADDRESSES.user)?.combinedMultiplier,
      10000n
    );
    assert.ok(mockDb.entities.UserNFTBaseline.get(`${ADDRESSES.user}:${VIEM_PARTIAL_ADDRESS}`));
    assert.ok(mockDb.entities.UserNFTBaseline.get(`${ADDRESSES.user}:${VIEM_SECOND_NFT_ADDRESS}`));
    assert.ok(
      mockDb.entities.UserNFTBaseline.get(`${ADDRESSES.user}:${VIEM_ZERO_BALANCE_ADDRESS}`)
    );
    assert.ok(mockDb.entities.UserNFTBaseline.get(`${ADDRESSES.user}:${VIEM_NO_NFT_ADDRESS}`));
    assert.equal(
      mockDb.entities.UserNFTBaseline.get(`${ADDRESSES.user}:${VIEM_ERROR_ADDRESS}`),
      undefined
    );
  } finally {
    process.env.DISABLE_EXTERNAL_CALLS = previousExternal;
    process.env.DISABLE_ETH_CALLS = previousEth;
    process.env.ENABLE_NFT_CHAIN_SYNC = previousChainSync;
  }
});

test('settlements sync LP positions from chain when enabled', async () => {
  const previousExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const previousEth = process.env.DISABLE_ETH_CALLS;
  const previousChainSync = process.env.ENABLE_LP_CHAIN_SYNC;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
  process.env.ENABLE_LP_CHAIN_SYNC = 'true';
  installViemMock();

  setLPBalanceOverride(ADDRESSES.positionManager, ADDRESSES.user, 1n);
  setLPTokensOverride(ADDRESSES.positionManager, ADDRESSES.user, [LP_TOKEN_ID]);
  setLPPositionOverride([
    0n,
    ADDRESSES.user,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    LP_TICK_LOWER,
    LP_TICK_UPPER,
    LP_LIQUIDITY,
    0n,
    0n,
    0n,
    0n,
  ]);

  try {
    const leaderboardState = createStore<LeaderboardState_t>();
    const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
    const userEpochStats = createStore<UserEpochStats_t>();
    const userLeaderboardState = createStore<UserLeaderboardState_t>();
    const userReserveList = createStore<UserReserveList_t>();
    const userTokenList = createStore<UserTokenList_t>();
    const dustLockToken = createStore<DustLockToken_t>();
    const votingPowerTier = createStore<VotingPowerTier_t>();
    const leaderboardConfig = createStore<LeaderboardConfig_t>();
    const lpPoolRegistry = createStore<LPPoolRegistry_t>();
    const lpPoolConfig = createStore<LPPoolConfig_t>();
    const lpPoolState = createStore<LPPoolState_t>();
    const lpPoolPositionIndex = createStore<LPPoolPositionIndex_t>();
    const userLPPositionIndex = createStore<UserLPPositionIndex_t>();
    const userLPPosition = createStore<UserLPPosition_t>();
    const userLPStats = createStore<UserLPStats_t>();
    const userLPBaseline = createStore<UserLPBaseline_t>();
    const tokenInfo = createStore<TokenInfo_t>();

    leaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    leaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: BigInt(LEADERBOARD_START_BLOCK),
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });
    leaderboardConfig.set({
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
      lastUpdate: 0,
    });

    lpPoolRegistry.set({
      id: 'global',
      poolIds: [ADDRESSES.lpPool],
      lastUpdate: 0,
    });
    lpPoolConfig.set({
      id: ADDRESSES.lpPool,
      pool: ADDRESSES.lpPool,
      positionManager: ADDRESSES.positionManager,
      token0: ADDRESSES.token0,
      token1: ADDRESSES.token1,
      fee: undefined,
      lpRateBps: 0n,
      isActive: true,
      enabledAtEpoch: 1n,
      enabledAtTimestamp: 0,
      disabledAtEpoch: undefined,
      disabledAtTimestamp: undefined,
      lastUpdate: 0,
    });
    lpPoolState.set({
      id: ADDRESSES.lpPool,
      pool: ADDRESSES.lpPool,
      currentTick: 0,
      sqrtPriceX96: LP_SQRT_PRICE_X96,
      token0Price: LP_PRICE_E8,
      token1Price: LP_PRICE_E8,
      lastUpdate: 0,
    });
    tokenInfo.set({
      id: ADDRESSES.token0,
      address: ADDRESSES.token0,
      decimals: LP_DECIMALS,
      symbol: 'TK0',
      name: 'Token0',
      lastUpdate: 0,
    });
    tokenInfo.set({
      id: ADDRESSES.token1,
      address: ADDRESSES.token1,
      decimals: LP_DECIMALS,
      symbol: 'TK1',
      name: 'Token1',
      lastUpdate: 0,
    });

    const context = {
      LeaderboardState: leaderboardState,
      LeaderboardEpoch: leaderboardEpoch,
      LeaderboardConfig: leaderboardConfig,
      UserEpochStats: userEpochStats,
      UserLeaderboardState: userLeaderboardState,
      UserReserveList: userReserveList,
      UserTokenList: userTokenList,
      DustLockToken: dustLockToken,
      VotingPowerTier: votingPowerTier,
      LPPoolRegistry: lpPoolRegistry,
      LPPoolConfig: lpPoolConfig,
      LPPoolState: lpPoolState,
      LPPoolPositionIndex: lpPoolPositionIndex,
      UserLPPositionIndex: userLPPositionIndex,
      UserLPPosition: userLPPosition,
      UserLPStats: userLPStats,
      UserLPBaseline: userLPBaseline,
      TokenInfo: tokenInfo,
    } as unknown as handlerContext;

    await settlePointsForUser(
      context,
      ADDRESSES.user,
      null,
      1000,
      BigInt(LEADERBOARD_START_BLOCK + 1),
      { skipNftSync: true }
    );

    const position = await userLPPosition.get(LP_TOKEN_ID.toString());
    assert.ok(position);
    assert.equal(position?.pool, ADDRESSES.lpPool);
    assert.equal(position?.positionManager, ADDRESSES.positionManager);

    const baseline = await userLPBaseline.get(`${ADDRESSES.user}:${ADDRESSES.positionManager}`);
    assert.ok(baseline);
  } finally {
    process.env.DISABLE_EXTERNAL_CALLS = previousExternal;
    process.env.DISABLE_ETH_CALLS = previousEth;
    process.env.ENABLE_LP_CHAIN_SYNC = previousChainSync;
    setLPBalanceOverride(ADDRESSES.positionManager, ADDRESSES.user, undefined);
    setLPTokensOverride(ADDRESSES.positionManager, ADDRESSES.user, undefined);
    setLPPositionOverride(undefined);
  }
});

test('settlements skip NFT sync when no active partnerships', async () => {
  const previousExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const previousEth = process.env.DISABLE_ETH_CALLS;
  const previousChainSync = process.env.ENABLE_NFT_CHAIN_SYNC;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
  process.env.ENABLE_NFT_CHAIN_SYNC = 'true';
  installViemMock();

  try {
    const TestHelpers = loadTestHelpers();
    let mockDb: MockDb = TestHelpers.MockDb.createMockDb();

    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: BigInt(LEADERBOARD_START_BLOCK),
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });
    mockDb = mockDb.entities.NFTPartnershipRegistryState.set({
      id: 'current',
      activeCollections: [],
      lastUpdate: 0,
    });

    await settlePointsForUser(
      mockDb.entities as unknown as handlerContext,
      ADDRESSES.user,
      null,
      1000,
      BigInt(LEADERBOARD_START_BLOCK + 1)
    );

    assert.equal(
      mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${VIEM_PARTIAL_ADDRESS}`),
      undefined
    );
  } finally {
    process.env.DISABLE_EXTERNAL_CALLS = previousExternal;
    process.env.DISABLE_ETH_CALLS = previousEth;
    process.env.ENABLE_NFT_CHAIN_SYNC = previousChainSync;
  }
});

test('settlements skip nft sync when baseline exists', async () => {
  const previousExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const previousEth = process.env.DISABLE_ETH_CALLS;
  const previousChainSync = process.env.ENABLE_NFT_CHAIN_SYNC;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
  process.env.ENABLE_NFT_CHAIN_SYNC = 'true';
  installViemMock();

  try {
    const TestHelpers = loadTestHelpers();
    let mockDb: MockDb = TestHelpers.MockDb.createMockDb();

    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: BigInt(LEADERBOARD_START_BLOCK),
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });
    mockDb = mockDb.entities.NFTPartnershipRegistryState.set({
      id: 'current',
      activeCollections: [VIEM_PARTIAL_ADDRESS],
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.UserNFTBaseline.set({
      id: `${ADDRESSES.user}:${VIEM_PARTIAL_ADDRESS}`,
      user_id: ADDRESSES.user,
      partnership_id: VIEM_PARTIAL_ADDRESS,
      checkedAt: 0,
      checkedBlock: 0n,
    });

    await settlePointsForUser(
      mockDb.entities as unknown as handlerContext,
      ADDRESSES.user,
      null,
      1000,
      BigInt(LEADERBOARD_START_BLOCK + 1),
      { skipLPSync: true }
    );

    assert.equal(
      mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${VIEM_PARTIAL_ADDRESS}`),
      undefined
    );
  } finally {
    process.env.DISABLE_EXTERNAL_CALLS = previousExternal;
    process.env.DISABLE_ETH_CALLS = previousEth;
    process.env.ENABLE_NFT_CHAIN_SYNC = previousChainSync;
  }
});

test('settlements skip nft sync when registry state is missing', async () => {
  const previousExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const previousEth = process.env.DISABLE_ETH_CALLS;
  const previousChainSync = process.env.ENABLE_NFT_CHAIN_SYNC;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
  process.env.ENABLE_NFT_CHAIN_SYNC = 'true';
  installViemMock();

  try {
    const TestHelpers = loadTestHelpers();
    let mockDb: MockDb = TestHelpers.MockDb.createMockDb();

    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: BigInt(LEADERBOARD_START_BLOCK),
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });

    await settlePointsForUser(
      mockDb.entities as unknown as handlerContext,
      ADDRESSES.user,
      null,
      1000,
      BigInt(LEADERBOARD_START_BLOCK + 1),
      { skipLPSync: true }
    );

    assert.equal(
      mockDb.entities.UserNFTOwnership.get(`${ADDRESSES.user}:${VIEM_PARTIAL_ADDRESS}`),
      undefined
    );
  } finally {
    process.env.DISABLE_EXTERNAL_CALLS = previousExternal;
    process.env.DISABLE_ETH_CALLS = previousEth;
    process.env.ENABLE_NFT_CHAIN_SYNC = previousChainSync;
  }
});

test('settlements baseline uses zero checked block when no change and block missing', async () => {
  const previousExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const previousEth = process.env.DISABLE_ETH_CALLS;
  const previousChainSync = process.env.ENABLE_NFT_CHAIN_SYNC;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
  process.env.ENABLE_NFT_CHAIN_SYNC = 'true';
  installViemMock();

  try {
    const leaderboardState = createStore<LeaderboardState_t>();
    const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
    const userNFTBaseline = createStore<UserNFTBaseline_t>();
    const userNFTOwnership = createStore<UserNFTOwnership_t>();
    const userLeaderboardState = createStore<UserLeaderboardState_t>();
    const userEpochStats = createStore<UserEpochStats_t>();
    const userReserveList = createStore<UserReserveList_t>();
    const userTokenList = createStore<UserTokenList_t>();
    const dustLockToken = createStore<DustLockToken_t>();
    const votingPowerTier = createStore<VotingPowerTier_t>();
    const leaderboardConfig = createStore<LeaderboardConfig_t>();
    const nftMultiplierConfig = createStore<NFTMultiplierConfig_t>();
    const registryState = createStore<NFTPartnershipRegistryState_t>();

    leaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    leaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: BigInt(LEADERBOARD_START_BLOCK),
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });
    registryState.set({
      id: 'current',
      activeCollections: [VIEM_NO_NFT_ADDRESS],
      lastUpdate: 0,
    });

    const context = {
      LeaderboardState: leaderboardState,
      LeaderboardEpoch: leaderboardEpoch,
      UserNFTBaseline: userNFTBaseline,
      UserNFTOwnership: userNFTOwnership,
      UserLeaderboardState: userLeaderboardState,
      UserEpochStats: userEpochStats,
      UserReserveList: userReserveList,
      UserTokenList: userTokenList,
      DustLockToken: dustLockToken,
      VotingPowerTier: votingPowerTier,
      LeaderboardConfig: leaderboardConfig,
      NFTMultiplierConfig: nftMultiplierConfig,
      NFTPartnershipRegistryState: registryState,
    } as unknown as handlerContext;

    await settlePointsForUser(context, ADDRESSES.user, null, 1000, undefined as unknown as bigint);

    const baseline = await userNFTBaseline.get(`${ADDRESSES.user}:${VIEM_NO_NFT_ADDRESS}`);
    assert.equal(baseline?.checkedBlock, 0n);
  } finally {
    process.env.DISABLE_EXTERNAL_CALLS = previousExternal;
    process.env.DISABLE_ETH_CALLS = previousEth;
    process.env.ENABLE_NFT_CHAIN_SYNC = previousChainSync;
  }
});

test('settlements default lastCheckedBlock when block number is missing', async () => {
  const previousExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const previousEth = process.env.DISABLE_ETH_CALLS;
  const previousChainSync = process.env.ENABLE_NFT_CHAIN_SYNC;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
  process.env.ENABLE_NFT_CHAIN_SYNC = 'true';
  installViemMock();

  try {
    const leaderboardState = createStore<LeaderboardState_t>();
    const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
    const userNFTBaseline = createStore<UserNFTBaseline_t>();
    const userNFTOwnership = createStore<UserNFTOwnership_t>();
    const userLeaderboardState = createStore<UserLeaderboardState_t>();
    const userEpochStats = createStore<UserEpochStats_t>();
    const userReserveList = createStore<UserReserveList_t>();
    const userTokenList = createStore<UserTokenList_t>();
    const dustLockToken = createStore<DustLockToken_t>();
    const votingPowerTier = createStore<VotingPowerTier_t>();
    const leaderboardConfig = createStore<LeaderboardConfig_t>();
    const nftMultiplierConfig = createStore<NFTMultiplierConfig_t>();
    const registryState = createStore<NFTPartnershipRegistryState_t>();

    leaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    leaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: BigInt(LEADERBOARD_START_BLOCK),
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });
    registryState.set({
      id: 'current',
      activeCollections: [VIEM_PARTIAL_ADDRESS],
      lastUpdate: 0,
    });

    const context = {
      LeaderboardState: leaderboardState,
      LeaderboardEpoch: leaderboardEpoch,
      UserNFTBaseline: userNFTBaseline,
      UserNFTOwnership: userNFTOwnership,
      UserLeaderboardState: userLeaderboardState,
      UserEpochStats: userEpochStats,
      UserReserveList: userReserveList,
      UserTokenList: userTokenList,
      DustLockToken: dustLockToken,
      VotingPowerTier: votingPowerTier,
      LeaderboardConfig: leaderboardConfig,
      NFTMultiplierConfig: nftMultiplierConfig,
      NFTPartnershipRegistryState: registryState,
    } as unknown as handlerContext;

    await settlePointsForUser(context, ADDRESSES.user, null, 1000, undefined as unknown as bigint);

    const ownership = await userNFTOwnership.get(`${ADDRESSES.user}:${VIEM_PARTIAL_ADDRESS}`);
    assert.equal(ownership?.lastCheckedBlock, 0n);
    const baseline = await userNFTBaseline.get(`${ADDRESSES.user}:${VIEM_PARTIAL_ADDRESS}`);
    assert.equal(baseline?.checkedBlock, 0n);
  } finally {
    process.env.DISABLE_EXTERNAL_CALLS = previousExternal;
    process.env.DISABLE_ETH_CALLS = previousEth;
    process.env.ENABLE_NFT_CHAIN_SYNC = previousChainSync;
  }
});

test('settlements clamp nft count when removing last nft', async () => {
  const previousExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const previousEth = process.env.DISABLE_ETH_CALLS;
  const previousChainSync = process.env.ENABLE_NFT_CHAIN_SYNC;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
  process.env.ENABLE_NFT_CHAIN_SYNC = 'true';
  installViemMock();

  try {
    const leaderboardState = createStore<LeaderboardState_t>();
    const leaderboardEpoch = createStore<LeaderboardEpoch_t>();
    const userNFTBaseline = createStore<UserNFTBaseline_t>();
    const userNFTOwnership = createStore<UserNFTOwnership_t>();
    const userLeaderboardState = createStore<UserLeaderboardState_t>();
    const userEpochStats = createStore<UserEpochStats_t>();
    const userReserveList = createStore<UserReserveList_t>();
    const userTokenList = createStore<UserTokenList_t>();
    const dustLockToken = createStore<DustLockToken_t>();
    const votingPowerTier = createStore<VotingPowerTier_t>();
    const leaderboardConfig = createStore<LeaderboardConfig_t>();
    const nftMultiplierConfig = createStore<NFTMultiplierConfig_t>();
    const registryState = createStore<NFTPartnershipRegistryState_t>();

    leaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    leaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: BigInt(LEADERBOARD_START_BLOCK),
      startTime: 0,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });
    registryState.set({
      id: 'current',
      activeCollections: [VIEM_ZERO_BALANCE_ADDRESS],
      lastUpdate: 0,
    });
    userLeaderboardState.set({
      id: ADDRESSES.user,
      user_id: ADDRESSES.user,
      nftCount: 0n,
      nftMultiplier: 10000n,
      votingPower: 0n,
      vpTierIndex: 0n,
      vpMultiplier: 10000n,
      combinedMultiplier: 10000n,
      totalEpochsParticipated: 0n,
      lifetimePoints: 0n,
      currentEpochId: undefined,
      currentEpochRank: undefined,
      lastUpdate: 0,
    });
    userNFTOwnership.set({
      id: `${ADDRESSES.user}:${VIEM_ZERO_BALANCE_ADDRESS}`,
      user_id: ADDRESSES.user,
      partnership_id: VIEM_ZERO_BALANCE_ADDRESS,
      balance: 1n,
      hasNFT: true,
      lastCheckedAt: 0,
      lastCheckedBlock: 0n,
    });

    const context = {
      LeaderboardState: leaderboardState,
      LeaderboardEpoch: leaderboardEpoch,
      UserNFTBaseline: userNFTBaseline,
      UserNFTOwnership: userNFTOwnership,
      UserLeaderboardState: userLeaderboardState,
      UserEpochStats: userEpochStats,
      UserReserveList: userReserveList,
      UserTokenList: userTokenList,
      DustLockToken: dustLockToken,
      VotingPowerTier: votingPowerTier,
      LeaderboardConfig: leaderboardConfig,
      NFTMultiplierConfig: nftMultiplierConfig,
      NFTPartnershipRegistryState: registryState,
    } as unknown as handlerContext;

    await settlePointsForUser(
      context,
      ADDRESSES.user,
      null,
      1000,
      BigInt(LEADERBOARD_START_BLOCK + 1)
    );

    const state = await userLeaderboardState.get(ADDRESSES.user);
    assert.equal(state?.nftCount, 0n);
    assert.equal(
      await userNFTOwnership.get(`${ADDRESSES.user}:${VIEM_ZERO_BALANCE_ADDRESS}`),
      undefined
    );
  } finally {
    process.env.DISABLE_EXTERNAL_CALLS = previousExternal;
    process.env.DISABLE_ETH_CALLS = previousEth;
    process.env.ENABLE_NFT_CHAIN_SYNC = previousChainSync;
  }
});
