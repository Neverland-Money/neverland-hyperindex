import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TestHelpers } from './v3-test-helpers';

import {
  AUSD_ADDRESS,
  BALANCER_AUTORANGE_V3_POOL_ADDRESS,
  BALANCER_VAULT_ADDRESS,
  LP_BALANCER_AUTORANGE_CUTOVER_BLOCK,
  LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  LP_V2_RESUME_CUTOVER_BLOCK,
  LP_V2_RESUME_CUTOVER_TIMESTAMP,
  USDC_ADDRESS,
  ZERO_ADDRESS,
} from '../helpers/constants';
import * as lpHandlers from '../handlers/lp';
import { installViemMock, setLPPositionOverride } from './viem-mock';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';
installViemMock();

const TOKEN_ID = 1n;
const TICK_LOWER = -120;
const TICK_UPPER = 120;
const DECIMALS = 6;
const UNIT = 10n ** 6n;
const PRICE_E8 = 100000000n;
const AMOUNT0 = 1000n * UNIT;
const AMOUNT1 = 2000n * UNIT;
const EXPECTED_VALUE_USD = 3000n * 10n ** 8n;
const DUST_DECIMALS = 18;
const LEGACY_V3_POOL = '0xd15965968fe8bf2babbe39b2fc5de1ab6749141f';
const LEGACY_V3_POSITION_MANAGER = '0x7197e214c0b767cfb76fb734ab638e2c192f4e53';
const V2_POOL = '0x86dbf00485871c901c5129bd525348db96c2eb2d';
const BALANCER_POOL = BALANCER_AUTORANGE_V3_POOL_ADDRESS;
const DUST_ADDRESS = '0xad96c3dffcd6374294e2573a7fbba96097cc8d7c';
const LP_V2_CUTOVER_BLOCK = 56436798;
const LP_V2_CUTOVER_TIMESTAMP = 1771517877;

const ADDRESSES = {
  positionManager: '0x000000000000000000000000000000000000a001',
  pool: '0x000000000000000000000000000000000000a002',
  token0: '0x000000000000000000000000000000000000a003',
  token1: '0x000000000000000000000000000000000000a004',
  user: '0x000000000000000000000000000000000000a005',
};

type TestHelpersApi = typeof TestHelpers;

function loadTestHelpers(): TestHelpersApi {
  return TestHelpers;
}

function getLpV2ResumeCutoverPredicate() {
  const predicate = (
    lpHandlers as unknown as {
      isPastLpV2ResumeCutover?: (timestamp: number, blockNumber?: bigint) => boolean;
    }
  ).isPastLpV2ResumeCutover;
  assert.ok(predicate, 'resume cutover predicate must be exported for the boundary matrix');
  return predicate;
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

function seedLeaderboardConfig(
  TestHelpers: TestHelpersApi,
  mockDb: ReturnType<TestHelpersApi['MockDb']['createMockDb']>
) {
  return mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 0n,
    lpRateBps: 2500n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 0,
  });
}

test('increase liquidity without indexed mint data does not create from rpc', async () => {
  const prevEnableExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEnableEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
  process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
  try {
    setLPPositionOverride([
      0n,
      ZERO_ADDRESS,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      TICK_LOWER,
      TICK_UPPER,
      123n,
      0n,
      0n,
      0n,
      0n,
    ]);

    const TestHelpers = loadTestHelpers();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();

    mockDb = mockDb.entities.LPPoolRegistry.set({
      id: 'global',
      poolIds: [ADDRESSES.pool],
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.LPPoolConfig.set({
      id: ADDRESSES.pool,
      pool: ADDRESSES.pool,
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
    mockDb = mockDb.entities.LPPoolState.set({
      id: ADDRESSES.pool,
      pool: ADDRESSES.pool,
      currentTick: 0,
      sqrtPriceX96: 0n,
      token0Price: PRICE_E8,
      token1Price: PRICE_E8,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.TokenInfo.set({
      id: ADDRESSES.token0,
      address: ADDRESSES.token0,
      decimals: DECIMALS,
      symbol: 'TK0',
      name: 'Token0',
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.TokenInfo.set({
      id: ADDRESSES.token1,
      address: ADDRESSES.token1,
      decimals: DECIMALS,
      symbol: 'TK1',
      name: 'Token1',
      lastUpdate: 0,
    });

    const increaseMeta = eventData(100, 1000, ADDRESSES.positionManager);
    const txHash = increaseMeta.mockEventData.transaction.hash;

    const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
      tokenId: TOKEN_ID,
      liquidity: 123n,
      amount0: AMOUNT0,
      amount1: AMOUNT1,
      ...increaseMeta,
    });
    mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
      event: increase,
      mockDb,
    });

    // IncreaseLiquidity alone does not read positions from RPC; ownership arrives through Transfer.
    const positionAfterIncrease = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
    assert.equal(positionAfterIncrease, undefined);

    const transferMeta = {
      mockEventData: {
        block: increaseMeta.mockEventData.block,
        logIndex: increaseMeta.mockEventData.logIndex + 1,
        srcAddress: ADDRESSES.positionManager,
        transaction: { hash: txHash },
      },
    };
    const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
      from: ZERO_ADDRESS,
      to: ADDRESSES.user,
      tokenId: TOKEN_ID,
      ...transferMeta,
    });
    mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
      event: transfer,
      mockDb,
    });

    const position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
    assert.equal(position, undefined);

    // Mint data should be cleaned up after position creation
    const pendingKey = `pending:${TOKEN_ID.toString()}`;
    const pendingAfter = mockDb.entities.LPMintData.get(pendingKey);
    assert.equal(pendingAfter, undefined);

    setLPPositionOverride(undefined);
  } finally {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevEnableExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEnableEth;
  }
});

test('increase liquidity uses pool mint data when eth_call is unavailable', async () => {
  setLPPositionOverride(undefined);

  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();

  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [ADDRESSES.pool],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DECIMALS,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DECIMALS,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });

  const txHash = '0x' + '1'.repeat(64);
  const block = { number: 100, timestamp: 1000 };

  const poolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
    owner: ADDRESSES.positionManager,
    tickLower: BigInt(TICK_LOWER),
    tickUpper: BigInt(TICK_UPPER),
    amount: 123n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    mockEventData: {
      block,
      logIndex: 1,
      srcAddress: ADDRESSES.pool,
      transaction: { hash: txHash },
    },
  });
  mockDb = await TestHelpers.UniswapV3Pool.Mint.processEvent({ event: poolMint, mockDb });

  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 123n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    mockEventData: {
      block,
      logIndex: 2,
      srcAddress: ADDRESSES.positionManager,
      transaction: { hash: txHash },
    },
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: increase,
    mockDb,
  });

  // IncreaseLiquidity can create from the indexed Pool.Mint data without RPC.
  const positionAfterIncrease = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.ok(positionAfterIncrease);
  assert.equal(positionAfterIncrease?.tickLower, TICK_LOWER);
  assert.equal(positionAfterIncrease?.tickUpper, TICK_UPPER);

  // Pool mint data is cleaned once the position is created.
  const poolMintKey = `${ADDRESSES.pool}:${TICK_LOWER}:${TICK_UPPER}:${txHash}`;
  const poolMintData = mockDb.entities.LPMintData.get(poolMintKey);
  assert.equal(poolMintData, undefined);

  const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: TOKEN_ID,
    mockEventData: {
      block,
      logIndex: 3,
      srcAddress: ADDRESSES.positionManager,
      transaction: { hash: txHash },
    },
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: transfer,
    mockDb,
  });

  const position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.ok(position);
  assert.equal(position?.amount0, AMOUNT0);
  assert.equal(position?.amount1, AMOUNT1);
  assert.equal(position?.valueUsd, EXPECTED_VALUE_USD);
});

test('swap accrues lp points when position stays in range', async () => {
  const prevEnableExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEnableEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
  process.env.ENVIO_ENABLE_ETH_CALLS = 'true';
  try {
    setLPPositionOverride([
      0n,
      ZERO_ADDRESS,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      TICK_LOWER,
      TICK_UPPER,
      123n,
      0n,
      0n,
      0n,
      0n,
    ]);

    const TestHelpers = loadTestHelpers();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();

    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: 0n,
      startTime: 1000,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 0,
      scheduledEndTime: 0,
    });

    mockDb = mockDb.entities.LPPoolRegistry.set({
      id: 'global',
      poolIds: [ADDRESSES.pool],
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.LPPoolConfig.set({
      id: ADDRESSES.pool,
      pool: ADDRESSES.pool,
      positionManager: ADDRESSES.positionManager,
      token0: ADDRESSES.token0,
      token1: ADDRESSES.token1,
      fee: undefined,
      lpRateBps: 2000n,
      isActive: true,
      enabledAtEpoch: 1n,
      enabledAtTimestamp: 0,
      disabledAtEpoch: undefined,
      disabledAtTimestamp: undefined,
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.LPPoolState.set({
      id: ADDRESSES.pool,
      pool: ADDRESSES.pool,
      currentTick: 0,
      sqrtPriceX96: 0n,
      token0Price: PRICE_E8,
      token1Price: PRICE_E8,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.TokenInfo.set({
      id: ADDRESSES.token0,
      address: ADDRESSES.token0,
      decimals: DECIMALS,
      symbol: 'TK0',
      name: 'Token0',
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.TokenInfo.set({
      id: ADDRESSES.token1,
      address: ADDRESSES.token1,
      decimals: DECIMALS,
      symbol: 'TK1',
      name: 'Token1',
      lastUpdate: 0,
    });

    const increaseMeta = eventData(100, 1000, ADDRESSES.positionManager);
    const txHash = increaseMeta.mockEventData.transaction.hash;
    const poolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
      owner: ADDRESSES.positionManager,
      tickLower: BigInt(TICK_LOWER),
      tickUpper: BigInt(TICK_UPPER),
      amount: 123n,
      amount0: AMOUNT0,
      amount1: AMOUNT1,
      mockEventData: {
        block: increaseMeta.mockEventData.block,
        logIndex: increaseMeta.mockEventData.logIndex - 1,
        srcAddress: ADDRESSES.pool,
        transaction: { hash: txHash },
      },
    });
    mockDb = await TestHelpers.UniswapV3Pool.Mint.processEvent({ event: poolMint, mockDb });

    const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
      tokenId: TOKEN_ID,
      liquidity: 123n,
      amount0: AMOUNT0,
      amount1: AMOUNT1,
      ...increaseMeta,
    });
    mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
      event: increase,
      mockDb,
    });

    const transferMeta = {
      mockEventData: {
        block: increaseMeta.mockEventData.block,
        logIndex: increaseMeta.mockEventData.logIndex + 1,
        srcAddress: ADDRESSES.positionManager,
        transaction: { hash: txHash },
      },
    };
    const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
      from: ZERO_ADDRESS,
      to: ADDRESSES.user,
      tokenId: TOKEN_ID,
      ...transferMeta,
    });
    mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
      event: transfer,
      mockDb,
    });

    const swapMeta = eventData(101, 1000 + 3600, ADDRESSES.pool);
    const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
      sender: ADDRESSES.user,
      recipient: ADDRESSES.user,
      amount0: 0n,
      amount1: 0n,
      sqrtPriceX96: 0n,
      liquidity: 0n,
      tick: 10n,
      ...swapMeta,
    });
    mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
      event: swap,
      mockDb,
    });

    const epochStats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
    assert.ok(epochStats);
    assert.ok(epochStats?.lpPoints && epochStats.lpPoints > 0n);

    setLPPositionOverride(undefined);
  } finally {
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevEnableExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEnableEth;
  }
});

test('swap updates fee apr stats', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const tvlUsd = 1000n * 10n ** 8n;

  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    fee: 10000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolStats.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    totalPositions: 1,
    inRangePositions: 1,
    totalValueUsd: tvlUsd,
    inRangeValueUsd: tvlUsd,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DECIMALS,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DECIMALS,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });

  const swapMeta = eventData(200, 4000, ADDRESSES.pool);
  const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: ADDRESSES.user,
    recipient: ADDRESSES.user,
    amount0: -1_000_000n,
    amount1: 2_000_000n,
    sqrtPriceX96: 1n,
    liquidity: 0n,
    tick: 0n,
    ...swapMeta,
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
    event: swap,
    mockDb,
  });

  const feeStats = mockDb.entities.LPPoolFeeStats.get(ADDRESSES.pool);
  assert.ok(feeStats);
  // Volume is average: amount0=1M, amount1=2M at same price → (100000000 + 200000000) / 2 = 150000000
  assert.equal(feeStats?.volumeUsd24h, 150000000n);
  assert.equal(feeStats?.feesUsd24h, 1500000n);
  assert.equal(feeStats?.feeAprBps, 54n);
});

test('increase/decrease liquidity update existing position', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 1000,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 2000n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DECIMALS,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DECIMALS,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: TOKEN_ID.toString(),
    tokenId: TOKEN_ID,
    user_id: ADDRESSES.user,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: 100n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    isInRange: true,
    valueUsd: EXPECTED_VALUE_USD,
    lastInRangeTimestamp: 1000,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 1000,
    lastUpdate: 1000,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    positionIds: [TOKEN_ID.toString()],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionIds: [TOKEN_ID.toString()],
    lastUpdate: 0,
  });

  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 50n,
    amount0: 10n,
    amount1: 20n,
    ...eventData(300, 1200, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: increase,
    mockDb,
  });

  const increased = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.ok(increased);
  assert.equal(increased?.liquidity, 150n);

  const decrease = TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 25n,
    amount0: 5n,
    amount1: 10n,
    ...eventData(301, 1300, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.processEvent({
    event: decrease,
    mockDb,
  });

  const decreased = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.ok(decreased);
  assert.equal(decreased?.liquidity, 125n);
});

test('transfer burn and owner transfer update indices', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
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
  mockDb = mockDb.entities.UserLPPosition.set({
    id: TOKEN_ID.toString(),
    tokenId: TOKEN_ID,
    user_id: ADDRESSES.user,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: 100n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    isInRange: true,
    valueUsd: EXPECTED_VALUE_USD,
    lastInRangeTimestamp: 1000,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 1000,
    lastUpdate: 1000,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    positionIds: [TOKEN_ID.toString()],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionIds: [TOKEN_ID.toString()],
    lastUpdate: 0,
  });

  const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ADDRESSES.token0,
    tokenId: TOKEN_ID,
    ...eventData(400, 1500, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: transfer,
    mockDb,
  });

  const moved = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(moved?.user_id, ADDRESSES.token0);

  const burn = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ADDRESSES.token0,
    to: ZERO_ADDRESS,
    tokenId: TOKEN_ID,
    ...eventData(401, 1600, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: burn,
    mockDb,
  });

  const burned = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(burned?.liquidity, 0n);
  const index = mockDb.entities.UserLPPositionIndex.get(ADDRESSES.token0);
  assert.equal(index?.positionIds.length, 0);
});

test('transfer mint skips when pool config missing', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  setLPPositionOverride(undefined);

  const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: TOKEN_ID,
    ...eventData(500, 1700, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: transfer,
    mockDb,
  });

  const position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(position, undefined);
});

test('swap handles ausd pricing and empty positions', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const pool = '0x000000000000000000000000000000000000a777';
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager: ADDRESSES.positionManager,
    token0: AUSD_ADDRESS,
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
  mockDb = mockDb.entities.LPPoolStats.set({
    id: pool,
    pool,
    totalPositions: 0,
    inRangePositions: 0,
    totalValueUsd: 0n,
    inRangeValueUsd: 0n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: pool,
    pool,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: 6,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: 18,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });

  const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: ADDRESSES.user,
    recipient: ADDRESSES.user,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: 2n ** 96n,
    liquidity: 0n,
    tick: 10n,
    ...eventData(600, 1800, pool),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
    event: swap,
    mockDb,
  });

  const stats = mockDb.entities.LPPoolStats.get(pool);
  assert.ok(stats);
});

test('swap handles out-of-range positions', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const pool = '0x000000000000000000000000000000000000a888';
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager: ADDRESSES.positionManager,
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
  mockDb = mockDb.entities.LPPoolStats.set({
    id: pool,
    pool,
    totalPositions: 0,
    inRangePositions: 0,
    totalValueUsd: 0n,
    inRangeValueUsd: 0n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: pool,
    pool,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DECIMALS,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DECIMALS,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: TOKEN_ID.toString(),
    tokenId: TOKEN_ID,
    user_id: ADDRESSES.user,
    pool,
    positionManager: ADDRESSES.positionManager,
    tickLower: 1000,
    tickUpper: 2000,
    liquidity: 100n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    isInRange: false,
    valueUsd: EXPECTED_VALUE_USD,
    lastInRangeTimestamp: 0,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 0,
    settledLpPoints: 0n,
    createdAt: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: pool,
    pool,
    positionIds: [TOKEN_ID.toString()],
    lastUpdate: 0,
  });

  const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: ADDRESSES.user,
    recipient: ADDRESSES.user,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: 2n ** 96n,
    liquidity: 0n,
    tick: 0n,
    ...eventData(700, 1900, pool),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
    event: swap,
    mockDb,
  });

  const stats = mockDb.entities.LPPoolStats.get(pool);
  assert.ok(stats);
});

test('burn event handler is callable', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const burnMeta = eventData(800, 2000, ADDRESSES.pool);
  const burn = TestHelpers.UniswapV3Pool.Burn.createMockEvent({
    owner: ADDRESSES.user,
    tickLower: BigInt(TICK_LOWER),
    tickUpper: BigInt(TICK_UPPER),
    amount: 1n,
    amount0: 0n,
    amount1: 0n,
    ...burnMeta,
  });
  mockDb = await TestHelpers.UniswapV3Pool.Burn.processEvent({
    event: burn,
    mockDb,
  });

  const stats = mockDb.entities.ProtocolStats.get('1');
  assert.ok(stats);
  assert.equal(stats?.lastTxHash, burnMeta.mockEventData.transaction.hash);
  assert.equal(stats?.totalTransactions, 1n);
});

test('increase liquidity returns early when pending mint exists', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const pendingKey = `pending:${TOKEN_ID.toString()}`;
  mockDb = mockDb.entities.LPMintData.set({
    id: pendingKey,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    owner: ADDRESSES.user,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: 1n,
    amount0: 1n,
    amount1: 1n,
    txHash: '0xseed',
    timestamp: 0,
  });

  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 10n,
    amount0: 5n,
    amount1: 5n,
    ...eventData(900, 3000, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: increase,
    mockDb,
  });

  const pending = mockDb.entities.LPMintData.get(pendingKey);
  assert.ok(pending);
});

test('increase liquidity skips when missing position data and pool config', async () => {
  setLPPositionOverride(undefined);

  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 10n,
    amount0: 5n,
    amount1: 5n,
    ...eventData(910, 3010, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: increase,
    mockDb,
  });

  const pendingKey = `pending:${TOKEN_ID.toString()}`;
  const pending = mockDb.entities.LPMintData.get(pendingKey);
  assert.equal(pending, undefined);
});

test('increase liquidity updates in-range transitions', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 1000,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 2000n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DECIMALS,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DECIMALS,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: TOKEN_ID.toString(),
    tokenId: TOKEN_ID,
    user_id: ADDRESSES.user,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: 100n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    isInRange: false,
    valueUsd: EXPECTED_VALUE_USD,
    lastInRangeTimestamp: 0,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 1000,
    lastUpdate: 1000,
  });

  const enter = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 1n,
    amount0: 1n,
    amount1: 1n,
    ...eventData(920, 1100, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: enter,
    mockDb,
  });
  let position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(position?.lastInRangeTimestamp, 1100);

  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    currentTick: 1000,
    sqrtPriceX96: 2n ** 96n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  const exit = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 1n,
    amount0: 1n,
    amount1: 1n,
    ...eventData(930, 1200, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: exit,
    mockDb,
  });
  position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(position?.lastInRangeTimestamp, 0);
});

test('decrease liquidity updates in-range transitions', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 1000,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    token0: ADDRESSES.token0,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 2000n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DECIMALS,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DECIMALS,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: TOKEN_ID.toString(),
    tokenId: TOKEN_ID,
    user_id: ADDRESSES.user,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: 100n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    isInRange: false,
    valueUsd: EXPECTED_VALUE_USD,
    lastInRangeTimestamp: 0,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 1000,
    lastUpdate: 1000,
  });

  const enter = TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 0n,
    amount0: 0n,
    amount1: 0n,
    ...eventData(940, 1100, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.processEvent({
    event: enter,
    mockDb,
  });
  let position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(position?.lastInRangeTimestamp, 1100);

  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    currentTick: 1000,
    sqrtPriceX96: 2n ** 96n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  const exit = TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 0n,
    amount0: 0n,
    amount1: 0n,
    ...eventData(950, 1200, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.processEvent({
    event: exit,
    mockDb,
  });
  position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(position?.lastInRangeTimestamp, 0);
});

test('transfer mint uses ausd pricing for token0', async () => {
  setLPPositionOverride(undefined);

  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [ADDRESSES.pool],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    token0: AUSD_ADDRESS,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: PRICE_E8,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: 6,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: 18,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPMintData.set({
    id: `pending:${TOKEN_ID.toString()}`,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    owner: ADDRESSES.user,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: 0n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    txHash: '0xseed',
    timestamp: 0,
  });

  const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: TOKEN_ID,
    ...eventData(960, 1300, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: transfer,
    mockDb,
  });

  const position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.ok(position);
  assert.ok(position?.valueUsd > 0n);

  setLPPositionOverride(undefined);
});

test('transfer mint selects matching pool config when multiple fees exist', async () => {
  const poolA = '0x000000000000000000000000000000000000b100';
  const poolB = '0x000000000000000000000000000000000000b101';

  setLPPositionOverride([
    0n,
    ADDRESSES.positionManager,
    ADDRESSES.token0,
    ADDRESSES.token1,
    3000,
    TICK_LOWER,
    TICK_UPPER,
    0n,
    0n,
    0n,
    0n,
    0n,
  ]);

  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [poolA, poolB],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: poolA,
    pool: poolA,
    positionManager: ADDRESSES.positionManager,
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
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: poolB,
    pool: poolB,
    positionManager: ADDRESSES.positionManager,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: poolB,
    pool: poolB,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DECIMALS,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DECIMALS,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPMintData.set({
    id: `pending:${TOKEN_ID.toString()}`,
    pool: poolB,
    positionManager: ADDRESSES.positionManager,
    owner: ADDRESSES.user,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: 0n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    txHash: '0xseed',
    timestamp: 0,
  });

  const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: ADDRESSES.user,
    tokenId: TOKEN_ID,
    ...eventData(980, 1500, ADDRESSES.positionManager),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: transfer,
    mockDb,
  });

  const position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(position?.pool, poolB);

  setLPPositionOverride(undefined);
});

test('swap leaves out-of-range positions untouched', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    currentTick: 0,
    sqrtPriceX96: 2n ** 96n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DECIMALS,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DECIMALS,
    symbol: 'TK1',
    name: 'Token1',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: TOKEN_ID.toString(),
    tokenId: TOKEN_ID,
    user_id: ADDRESSES.user,
    pool: ADDRESSES.pool,
    positionManager: ADDRESSES.positionManager,
    tickLower: -100,
    tickUpper: 100,
    liquidity: 100n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    isInRange: false,
    valueUsd: EXPECTED_VALUE_USD,
    lastInRangeTimestamp: 0,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 0,
    settledLpPoints: 0n,
    createdAt: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: ADDRESSES.pool,
    pool: ADDRESSES.pool,
    positionIds: [TOKEN_ID.toString()],
    lastUpdate: 0,
  });

  const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: ADDRESSES.user,
    recipient: ADDRESSES.user,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: 2n ** 96n,
    liquidity: 0n,
    tick: 5000n,
    ...eventData(970, 1400, ADDRESSES.pool),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
    event: swap,
    mockDb,
  });

  const stats = mockDb.entities.LPPoolStats.get(ADDRESSES.pool);
  assert.ok(stats);
  assert.equal(stats?.totalPositions, 1);
  assert.equal(stats?.inRangePositions, 0);
});

test('uniswap v2 transfer + sync creates and values synthetic lp position', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b200';
  const user = ADDRESSES.user;

  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [v2Pool],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
    token0: USDC_ADDRESS,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: v2Pool,
    pool: v2Pool,
    reserve0: 0n,
    reserve1: 0n,
    lpTotalSupply: 0n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USD Coin',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });

  const mintTransfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: user,
    value: 1_000n,
    ...eventData(56436798, 2000, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: mintTransfer,
    mockDb,
  });

  const sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    ...eventData(56436799, 2600, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: sync,
    mockDb,
  });

  const positionId = `v2:${v2Pool}:${user}`;
  const position = mockDb.entities.UserLPPosition.get(positionId);
  assert.ok(position);
  assert.equal(position?.liquidity, 1_000n);
  assert.equal(position?.isInRange, true);
  assert.equal(position?.tickLower, -887272);
  assert.equal(position?.tickUpper, 887272);
  assert.ok((position?.valueUsd ?? 0n) > 0n);

  const v2State = mockDb.entities.LPPoolV2State.get(v2Pool);
  assert.equal(v2State?.lpTotalSupply, 1_000n);
  assert.equal(v2State?.reserve0, 1_000_000n * 10n ** 6n);
  assert.equal(v2State?.reserve1, 500_000n * 10n ** 18n);
});

test('uniswap v2 swap updates fee apr stats', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b201';
  const tvlUsd = 1_000n * 10n ** 8n;

  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
    token0: USDC_ADDRESS,
    token1: ADDRESSES.token0,
    fee: 3000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolStats.set({
    id: v2Pool,
    pool: v2Pool,
    totalPositions: 1,
    inRangePositions: 1,
    totalValueUsd: tvlUsd,
    inRangeValueUsd: tvlUsd,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USD Coin',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DECIMALS,
    symbol: 'TK0',
    name: 'Token0',
    lastUpdate: 0,
  });

  const swap = TestHelpers.UniswapV2Pair.Swap.createMockEvent({
    sender: ADDRESSES.user,
    amount0In: 1_000_000n,
    amount1In: 0n,
    amount0Out: 0n,
    amount1Out: 2_000_000n,
    to: ADDRESSES.user,
    ...eventData(200, 4000, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Swap.processEvent({
    event: swap,
    mockDb,
  });

  const feeStats = mockDb.entities.LPPoolFeeStats.get(v2Pool);
  assert.ok(feeStats);
  assert.equal(feeStats?.volumeUsd24h, 150000000n);
  assert.equal(feeStats?.feesUsd24h, 450000n);
  assert.equal(feeStats?.feeAprBps, 16n);
});

test('legacy uniswap v3 swap hardstops after cutover', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.LPPoolConfig.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    positionManager: LEGACY_V3_POSITION_MANAGER,
    token0: AUSD_ADDRESS,
    token1: ADDRESSES.token1,
    fee: 10000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });

  const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: ADDRESSES.user,
    recipient: ADDRESSES.user,
    amount0: -1_000_000n,
    amount1: 2_000_000n,
    sqrtPriceX96: 1n,
    liquidity: 0n,
    tick: 10n,
    ...eventData(LP_V2_CUTOVER_BLOCK + 1, LP_V2_CUTOVER_TIMESTAMP + 60, LEGACY_V3_POOL),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
    event: swap,
    mockDb,
  });

  const poolState = mockDb.entities.LPPoolState.get(LEGACY_V3_POOL);
  assert.equal(poolState?.currentTick, 0);
  assert.equal(poolState?.lastUpdate, 0);
  const feeStats = mockDb.entities.LPPoolFeeStats.get(LEGACY_V3_POOL);
  assert.equal(feeStats, undefined);
});

test('legacy position manager transfer hardstops after cutover', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  mockDb = mockDb.entities.UserLPPosition.set({
    id: TOKEN_ID.toString(),
    tokenId: TOKEN_ID,
    user_id: ADDRESSES.user,
    pool: LEGACY_V3_POOL,
    positionManager: LEGACY_V3_POSITION_MANAGER,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: 100n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    isInRange: true,
    valueUsd: EXPECTED_VALUE_USD,
    lastInRangeTimestamp: 1000,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 1000,
    lastUpdate: 1000,
  });

  const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ADDRESSES.token0,
    tokenId: TOKEN_ID,
    ...eventData(
      LP_V2_CUTOVER_BLOCK + 5,
      LP_V2_CUTOVER_TIMESTAMP + 3600,
      LEGACY_V3_POSITION_MANAGER
    ),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: transfer,
    mockDb,
  });

  const position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(position?.user_id, ADDRESSES.user);
  assert.equal(position?.lastUpdate, 1000);
});

test('uniswap v2 transfer between users updates both synthetic positions', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b202';
  const userA = ADDRESSES.user;
  const userB = '0x000000000000000000000000000000000000a006';
  const positionAId = `v2:${v2Pool}:${userA}`;

  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
    token0: USDC_ADDRESS,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: v2Pool,
    pool: v2Pool,
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    lpTotalSupply: 1_000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USD Coin',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: positionAId,
    tokenId: BigInt(userA),
    user_id: userA,
    pool: v2Pool,
    positionManager: v2Pool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1_000n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 0n,
    lastInRangeTimestamp: 2000,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 2000,
    settledLpPoints: 0n,
    createdAt: 2000,
    lastUpdate: 2000,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: userA,
    user_id: userA,
    positionIds: [positionAId],
    lastUpdate: 2000,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: v2Pool,
    pool: v2Pool,
    positionIds: [positionAId],
    lastUpdate: 2000,
  });

  const transfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: userA,
    to: userB,
    value: 250n,
    ...eventData(56436810, 5000, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: transfer,
    mockDb,
  });

  const updatedA = mockDb.entities.UserLPPosition.get(positionAId);
  assert.equal(updatedA?.liquidity, 750n);

  const positionBId = `v2:${v2Pool}:${userB}`;
  const updatedB = mockDb.entities.UserLPPosition.get(positionBId);
  assert.equal(updatedB?.liquidity, 250n);
});

test('uniswap v2 swap uses stablecoin fallback pricing when pool prices are zero', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b203';
  const tvlUsd = 1_000n * 10n ** 8n;

  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
    token0: USDC_ADDRESS,
    token1: AUSD_ADDRESS,
    fee: 3000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolStats.set({
    id: v2Pool,
    pool: v2Pool,
    totalPositions: 1,
    inRangePositions: 1,
    totalValueUsd: tvlUsd,
    inRangeValueUsd: tvlUsd,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USD Coin',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: DECIMALS,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 0,
  });

  const swap = TestHelpers.UniswapV2Pair.Swap.createMockEvent({
    sender: ADDRESSES.user,
    amount0In: 1_000_000n,
    amount1In: 0n,
    amount0Out: 0n,
    amount1Out: 1_000_000n,
    to: ADDRESSES.user,
    ...eventData(56436811, 6000, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Swap.processEvent({
    event: swap,
    mockDb,
  });

  const feeStats = mockDb.entities.LPPoolFeeStats.get(v2Pool);
  assert.ok(feeStats);
  assert.equal(feeStats?.volumeUsd24h, 100000000n);
  assert.equal(feeStats?.feesUsd24h, 300000n);
  assert.equal(feeStats?.feeAprBps, 10n);
});

test('uniswap v2 sync with no positions still updates pool stats', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b204';

  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
    token0: USDC_ADDRESS,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: v2Pool,
    pool: v2Pool,
    reserve0: 0n,
    reserve1: 0n,
    lpTotalSupply: 0n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USD Coin',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });

  const sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 100n,
    reserve1: 200n,
    ...eventData(56436812, 7000, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: sync,
    mockDb,
  });

  const poolStats = mockDb.entities.LPPoolStats.get(v2Pool);
  assert.ok(poolStats);
  assert.equal(poolStats?.totalPositions, 0);
  assert.equal(poolStats?.inRangePositions, 0);
});

test('uniswap v2 sync settles and writes epoch lp points when earned', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b205';
  const user = ADDRESSES.user;
  const positionId = `v2:${v2Pool}:${user}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: 1000,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
    token0: USDC_ADDRESS,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 2500n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: v2Pool,
    pool: v2Pool,
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    lpTotalSupply: 1_000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USD Coin',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: positionId,
    tokenId: BigInt(user),
    user_id: user,
    pool: v2Pool,
    positionManager: v2Pool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1_000n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 100n * 10n ** 8n,
    lastInRangeTimestamp: 1000,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 1000,
    lastUpdate: 1000,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: user,
    user_id: user,
    positionIds: [positionId],
    lastUpdate: 1000,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: v2Pool,
    pool: v2Pool,
    positionIds: [positionId],
    lastUpdate: 1000,
  });

  const sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    ...eventData(56436813, 4600, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: sync,
    mockDb,
  });

  const epochStats = mockDb.entities.UserEpochStats.get(`${user}:1`);
  assert.ok(epochStats);
  assert.ok((epochStats?.lpPoints ?? 0n) > 0n);
});

test('uniswap v2 burn transfer clamps total supply at zero', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b206';
  const user = ADDRESSES.user;
  const positionId = `v2:${v2Pool}:${user}`;

  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
    token0: USDC_ADDRESS,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: v2Pool,
    pool: v2Pool,
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    lpTotalSupply: 1_000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USD Coin',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: positionId,
    tokenId: BigInt(user),
    user_id: user,
    pool: v2Pool,
    positionManager: v2Pool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1_000n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 0n,
    lastInRangeTimestamp: 2000,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 2000,
    settledLpPoints: 0n,
    createdAt: 2000,
    lastUpdate: 2000,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: user,
    user_id: user,
    positionIds: [positionId],
    lastUpdate: 2000,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: v2Pool,
    pool: v2Pool,
    positionIds: [positionId],
    lastUpdate: 2000,
  });

  const burnTransfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: user,
    to: ZERO_ADDRESS,
    value: 2_000n,
    ...eventData(56436814, 8000, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: burnTransfer,
    mockDb,
  });

  const v2State = mockDb.entities.LPPoolV2State.get(v2Pool);
  assert.equal(v2State?.lpTotalSupply, 0n);
});

test('uniswap v3 initialize seeds tracked ausd pool state', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const pool = '0x000000000000000000000000000000000000b301';
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [pool],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager: ADDRESSES.positionManager,
    token0: AUSD_ADDRESS,
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
  mockDb = mockDb.entities.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: DECIMALS,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });

  const initialized = TestHelpers.UniswapV3Pool.Initialize.createMockEvent({
    sqrtPriceX96: 2n ** 96n,
    tick: 0n,
    ...eventData(1000, 1700000000, pool),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Initialize.processEvent({
    event: initialized,
    mockDb,
  });

  const state = mockDb.entities.LPPoolState.get(pool);
  assert.ok(state);
  assert.equal(state?.currentTick, 0);
  assert.equal(state?.token0Price, PRICE_E8);
  assert.ok((state?.token1Price ?? 0n) > 0n);
});

test('uniswap v3 set fee protocol updates pool state', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const pool = '0x000000000000000000000000000000000000b302';
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [pool],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager: ADDRESSES.positionManager,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: pool,
    pool,
    currentTick: 1,
    sqrtPriceX96: 2n ** 96n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 1,
    feeProtocol1: 2,
    lastUpdate: 0,
  });

  const event = TestHelpers.UniswapV3Pool.SetFeeProtocol.createMockEvent({
    feeProtocol0Old: 1n,
    feeProtocol1Old: 2n,
    feeProtocol0New: 6n,
    feeProtocol1New: 7n,
    ...eventData(1001, 1700000010, pool),
  });
  mockDb = await TestHelpers.UniswapV3Pool.SetFeeProtocol.processEvent({
    event,
    mockDb,
  });

  const state = mockDb.entities.LPPoolState.get(pool);
  assert.equal(state?.feeProtocol0, 6);
  assert.equal(state?.feeProtocol1, 7);
});

test('legacy uniswap v3 mint before cutover keeps hardcoded config path', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const poolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
    sender: ADDRESSES.user,
    owner: ADDRESSES.user,
    tickLower: BigInt(TICK_LOWER),
    tickUpper: BigInt(TICK_UPPER),
    amount: 111n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    ...eventData(LP_V2_CUTOVER_BLOCK - 1, LP_V2_CUTOVER_TIMESTAMP - 1, LEGACY_V3_POOL),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Mint.processEvent({ event: poolMint, mockDb });

  const legacyConfig = mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL);
  assert.ok(legacyConfig);

  const mintKey = `${LEGACY_V3_POOL}:${TICK_LOWER}:${TICK_UPPER}:${poolMint.transaction.hash}`;
  const cachedMint = mockDb.entities.LPMintData.get(mintKey);
  assert.ok(cachedMint);
});

test('uniswap v2 sync keeps fallback prices for non-stable pairs', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b303';
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
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
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 111n,
    token1Price: 222n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: v2Pool,
    pool: v2Pool,
    reserve0: 0n,
    reserve1: 0n,
    lpTotalSupply: 1000n,
    lastUpdate: 0,
  });

  const sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 5000n,
    reserve1: 7000n,
    ...eventData(1002, 1700000020, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: sync,
    mockDb,
  });

  const poolState = mockDb.entities.LPPoolState.get(v2Pool);
  assert.equal(poolState?.token0Price, 111n);
  assert.equal(poolState?.token1Price, 222n);
});

test('uniswap v2 sync derives prices when stable token is token1', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b304';
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
    token0: ADDRESSES.token0,
    token1: USDC_ADDRESS,
    fee: 3000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: v2Pool,
    pool: v2Pool,
    reserve0: 0n,
    reserve1: 0n,
    lpTotalSupply: 1000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token0,
    address: ADDRESSES.token0,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USDC',
    lastUpdate: 0,
  });

  const sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 500n * 10n ** 18n,
    reserve1: 1000n * 10n ** 6n,
    ...eventData(1003, 1700000030, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: sync,
    mockDb,
  });

  const poolState = mockDb.entities.LPPoolState.get(v2Pool);
  assert.equal(poolState?.token1Price, PRICE_E8);
  assert.ok((poolState?.token0Price ?? 0n) > 0n);
});

test('increase liquidity on legacy manager applies ausd-derived pricing path', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();

  const txHash = '0x' + '9'.repeat(64);
  const blockNumber = LP_V2_CUTOVER_BLOCK - 1;
  const timestamp = LP_V2_CUTOVER_TIMESTAMP - 1;
  const legacyDustToken = '0xad96c3dffcd6374294e2573a7fbba96097cc8d7c';

  mockDb = mockDb.entities.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: DECIMALS,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: legacyDustToken,
    address: legacyDustToken,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });

  const poolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
    sender: ADDRESSES.user,
    owner: ADDRESSES.user,
    tickLower: BigInt(TICK_LOWER),
    tickUpper: BigInt(TICK_UPPER),
    amount: 123n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    mockEventData: {
      block: { number: blockNumber, timestamp },
      logIndex: 1,
      srcAddress: LEGACY_V3_POOL,
      transaction: { hash: txHash },
    },
  });
  mockDb = await TestHelpers.UniswapV3Pool.Mint.processEvent({ event: poolMint, mockDb });

  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TOKEN_ID,
    liquidity: 123n,
    amount0: AMOUNT0,
    amount1: AMOUNT1,
    mockEventData: {
      block: { number: blockNumber, timestamp },
      logIndex: 2,
      srcAddress: LEGACY_V3_POSITION_MANAGER,
      transaction: { hash: txHash, from: ADDRESSES.user },
    },
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: increase,
    mockDb,
  });

  const position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.ok(position);
  assert.equal(position?.pool, LEGACY_V3_POOL);
});

test('transfer mint does not resolve missing position data from rpc', async () => {
  const prevEnableExternal = process.env.ENVIO_ENABLE_EXTERNAL_CALLS;
  const prevEnableEth = process.env.ENVIO_ENABLE_ETH_CALLS;
  process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'true';
  process.env.ENVIO_ENABLE_ETH_CALLS = 'true';

  try {
    setLPPositionOverride([
      0n,
      ADDRESSES.positionManager,
      ADDRESSES.token0,
      ADDRESSES.token1,
      3000,
      TICK_LOWER,
      TICK_UPPER,
      0n,
      0n,
      0n,
      0n,
      0n,
    ]);

    const TestHelpers = loadTestHelpers();
    let mockDb = TestHelpers.MockDb.createMockDb();
    const eventData = createEventDataFactory();

    mockDb = mockDb.entities.LPPoolRegistry.set({
      id: 'global',
      poolIds: [ADDRESSES.pool],
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.LPPoolConfig.set({
      id: ADDRESSES.pool,
      pool: ADDRESSES.pool,
      positionManager: ADDRESSES.positionManager,
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
    mockDb = mockDb.entities.LPPoolState.set({
      id: ADDRESSES.pool,
      pool: ADDRESSES.pool,
      currentTick: 0,
      sqrtPriceX96: 2n ** 96n,
      token0Price: PRICE_E8,
      token1Price: PRICE_E8,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.TokenInfo.set({
      id: ADDRESSES.token0,
      address: ADDRESSES.token0,
      decimals: DECIMALS,
      symbol: 'TK0',
      name: 'Token0',
      lastUpdate: 0,
    });
    mockDb = mockDb.entities.TokenInfo.set({
      id: ADDRESSES.token1,
      address: ADDRESSES.token1,
      decimals: DECIMALS,
      symbol: 'TK1',
      name: 'Token1',
      lastUpdate: 0,
    });

    const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
      from: ZERO_ADDRESS,
      to: ADDRESSES.user,
      tokenId: TOKEN_ID,
      ...eventData(1004, 1700000040, ADDRESSES.positionManager),
    });
    mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
      event: transfer,
      mockDb,
    });

    const position = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
    assert.equal(position, undefined);
  } finally {
    setLPPositionOverride(undefined);
    process.env.ENVIO_ENABLE_EXTERNAL_CALLS = prevEnableExternal;
    process.env.ENVIO_ENABLE_ETH_CALLS = prevEnableEth;
  }
});

test('uniswap v2 transfer settles existing synthetic position points', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b305';
  const user = ADDRESSES.user;
  const positionId = `v2:${v2Pool}:${user}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
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
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: v2Pool,
    pool: v2Pool,
    positionManager: v2Pool,
    token0: USDC_ADDRESS,
    token1: ADDRESSES.token1,
    fee: 3000,
    lpRateBps: 2500n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 0,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: v2Pool,
    pool: v2Pool,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: v2Pool,
    pool: v2Pool,
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    lpTotalSupply: 1_000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USDC',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: ADDRESSES.token1,
    address: ADDRESSES.token1,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: positionId,
    tokenId: BigInt(user),
    user_id: user,
    pool: v2Pool,
    positionManager: v2Pool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1_000n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 100n * 10n ** 8n,
    lastInRangeTimestamp: 1000,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 1000,
    settledLpPoints: 0n,
    createdAt: 1000,
    lastUpdate: 1000,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: user,
    user_id: user,
    positionIds: [positionId],
    lastUpdate: 1000,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: v2Pool,
    pool: v2Pool,
    positionIds: [positionId],
    lastUpdate: 1000,
  });

  const mintTransfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: user,
    value: 100n,
    ...eventData(1005, 5000, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: mintTransfer,
    mockDb,
  });

  const epochStats = mockDb.entities.UserEpochStats.get(`${user}:1`);
  assert.ok(epochStats);
  assert.ok((epochStats?.lpPoints ?? 0n) > 0n);
});

test('balancer autorange transfer before cutover tracks holder and accrues only after cutover', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seedLeaderboardConfig(TestHelpers, mockDb);
  const eventData = createEventDataFactory();
  const user = ADDRESSES.user;
  const positionId = `v2:${BALANCER_POOL}:${user}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
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
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USDC',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: DUST_ADDRESS,
    address: DUST_ADDRESS,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });

  const liquidityAdded = TestHelpers.BalancerVault.LiquidityAdded.createMockEvent({
    pool: BALANCER_POOL,
    liquidityProvider: user,
    kind: 0n,
    totalSupply: 1_000n,
    amountsAddedRaw: [1_000_000n * 10n ** 6n, 500_000n * 10n ** 18n],
    swapFeeAmountsRaw: [0n, 0n],
    ...eventData(
      LP_BALANCER_AUTORANGE_CUTOVER_BLOCK - 20,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 200,
      BALANCER_VAULT_ADDRESS
    ),
  });
  mockDb = await TestHelpers.BalancerVault.LiquidityAdded.processEvent({
    event: liquidityAdded,
    mockDb,
  });

  const preCutoverMint = TestHelpers.BalancerAutoRangePool.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: user,
    value: 100n,
    ...eventData(
      LP_BALANCER_AUTORANGE_CUTOVER_BLOCK - 10,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 100,
      BALANCER_POOL
    ),
  });
  mockDb = await TestHelpers.BalancerAutoRangePool.Transfer.processEvent({
    event: preCutoverMint,
    mockDb,
  });

  const preCutoverStats = mockDb.entities.UserEpochStats.get(`${user}:1`);
  assert.equal(preCutoverStats, undefined);
  const position = mockDb.entities.UserLPPosition.get(positionId);
  assert.ok(position);
  assert.equal(position?.lastInRangeTimestamp, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 100);

  const postCutoverSwap = TestHelpers.BalancerVault.Swap.createMockEvent({
    pool: BALANCER_POOL,
    tokenIn: DUST_ADDRESS,
    tokenOut: USDC_ADDRESS,
    amountIn: 10n * 10n ** 18n,
    amountOut: 5n * 10n ** 6n,
    swapFeePercentage: 10n ** 16n,
    swapFeeAmount: 10n ** 16n,
    ...eventData(
      LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 10,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 3600,
      BALANCER_VAULT_ADDRESS
    ),
  });
  mockDb = await TestHelpers.BalancerVault.Swap.processEvent({
    event: postCutoverSwap,
    mockDb,
  });

  const epochStats = mockDb.entities.UserEpochStats.get(`${user}:1`);
  assert.ok(epochStats);
  assert.ok((epochStats?.lpPoints ?? 0n) > 0n);

  const updatedPosition = mockDb.entities.UserLPPosition.get(positionId);
  assert.equal(updatedPosition?.liquidity, 100n);
  assert.equal(updatedPosition?.lastSettledAt, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 3600);

  const v2State = mockDb.entities.LPPoolV2State.get(BALANCER_POOL);
  assert.equal(v2State?.reserve0, 1_000_000n * 10n ** 6n - 5n * 10n ** 6n);
  assert.equal(v2State?.reserve1, 500_000n * 10n ** 18n + 10n * 10n ** 18n);
});

test('uniswap v2 cutover keeps transfer bookkeeping while v2 points are paused', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seedLeaderboardConfig(TestHelpers, mockDb);
  const eventData = createEventDataFactory();
  const user = ADDRESSES.user;
  const positionId = `v2:${V2_POOL}:${user}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
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
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [V2_POOL],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: V2_POOL,
    pool: V2_POOL,
    positionManager: V2_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 3000,
    lpRateBps: 2500n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: V2_POOL,
    pool: V2_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: V2_POOL,
    pool: V2_POOL,
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    lpTotalSupply: 1_000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: positionId,
    tokenId: BigInt(user),
    user_id: user,
    pool: V2_POOL,
    positionManager: V2_POOL,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1_000n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 100n * 10n ** 8n,
    lastInRangeTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
    settledLpPoints: 0n,
    createdAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
    lastUpdate: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: user,
    user_id: user,
    positionIds: [positionId],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: V2_POOL,
    pool: V2_POOL,
    positionIds: [positionId],
    lastUpdate: 0,
  });

  const postCutoverTransfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: user,
    value: 100n,
    ...eventData(
      LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 60,
      V2_POOL
    ),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: postCutoverTransfer,
    mockDb,
  });

  const v2Config = mockDb.entities.LPPoolConfig.get(V2_POOL);
  assert.equal(v2Config?.isActive, false);
  assert.equal(v2Config?.disabledAtTimestamp, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);

  const balancerConfig = mockDb.entities.LPPoolConfig.get(BALANCER_POOL);
  assert.equal(balancerConfig?.isActive, true);
  assert.equal(balancerConfig?.enabledAtTimestamp, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP);

  const epochStats = mockDb.entities.UserEpochStats.get(`${user}:1`);
  assert.ok(epochStats);
  assert.ok((epochStats?.lpPoints ?? 0n) > 0n);

  const updatedV2Position = mockDb.entities.UserLPPosition.get(positionId);
  assert.equal(updatedV2Position?.liquidity, 1_100n);
  assert.equal(updatedV2Position?.lastSettledAt, LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 60);
});

test('all balancer event families hard-stop without mutation after the v2 resume transition', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seedLeaderboardConfig(TestHelpers, mockDb);
  const eventData = createEventDataFactory();
  const user = ADDRESSES.user;
  const balancerPositionId = `v2:${BALANCER_POOL}:${user}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
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
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [V2_POOL, BALANCER_POOL],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: V2_POOL,
    pool: V2_POOL,
    positionManager: V2_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 3000,
    lpRateBps: 2500n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: BALANCER_POOL,
    pool: BALANCER_POOL,
    positionManager: BALANCER_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 10000,
    lpRateBps: 2500n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: BALANCER_POOL,
    pool: BALANCER_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: BALANCER_POOL,
    pool: BALANCER_POOL,
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    lpTotalSupply: 1_000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USD Coin',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: DUST_ADDRESS,
    address: DUST_ADDRESS,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: balancerPositionId,
    tokenId: BigInt(user),
    user_id: user,
    pool: BALANCER_POOL,
    positionManager: BALANCER_POOL,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1_000n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 100n * 10n ** 8n,
    lastInRangeTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP - 3600,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: LP_V2_RESUME_CUTOVER_TIMESTAMP - 3600,
    settledLpPoints: 0n,
    createdAt: LP_V2_RESUME_CUTOVER_TIMESTAMP - 3600,
    lastUpdate: LP_V2_RESUME_CUTOVER_TIMESTAMP - 3600,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: user,
    user_id: user,
    positionIds: [balancerPositionId],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: BALANCER_POOL,
    pool: BALANCER_POOL,
    positionIds: [balancerPositionId],
    lastUpdate: 0,
  });

  const postResumeTransfer = TestHelpers.BalancerAutoRangePool.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: user,
    value: 100n,
    ...eventData(
      LP_V2_RESUME_CUTOVER_BLOCK + 1,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 60,
      BALANCER_POOL
    ),
  });
  mockDb = await TestHelpers.BalancerAutoRangePool.Transfer.processEvent({
    event: postResumeTransfer,
    mockDb,
  });

  const balancerConfig = mockDb.entities.LPPoolConfig.get(BALANCER_POOL);
  assert.equal(balancerConfig?.isActive, false);
  assert.equal(balancerConfig?.disabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP);

  const v2Config = mockDb.entities.LPPoolConfig.get(V2_POOL);
  assert.equal(v2Config?.isActive, true);
  assert.equal(v2Config?.enabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP);

  const epochStats = mockDb.entities.UserEpochStats.get(`${user}:1`);
  assert.ok(epochStats);
  assert.ok((epochStats?.lpPoints ?? 0n) > 0n);

  // The resume transfer itself is hard-stopped on the Balancer pool: it only
  // gets force-settled at the cutover boundary, no new liquidity is minted.
  const settledBalancerPosition = mockDb.entities.UserLPPosition.get(balancerPositionId);
  assert.equal(settledBalancerPosition?.liquidity, 1_000n);
  assert.equal(settledBalancerPosition?.lastSettledAt, LP_V2_RESUME_CUTOVER_TIMESTAMP);

  const boundaryBalancerConfig = mockDb.entities.LPPoolConfig.get(BALANCER_POOL);
  const boundaryV2Config = mockDb.entities.LPPoolConfig.get(V2_POOL);
  const boundaryPoolState = mockDb.entities.LPPoolState.get(BALANCER_POOL);
  const boundaryV2State = mockDb.entities.LPPoolV2State.get(BALANCER_POOL);
  const boundaryPosition = mockDb.entities.UserLPPosition.get(balancerPositionId);
  const boundaryEpochStats = mockDb.entities.UserEpochStats.get(`${user}:1`);
  const boundaryFeeStats = mockDb.entities.LPPoolFeeStats.get(BALANCER_POOL);
  assert.equal(mockDb.entities.ProtocolStats.get('1'), undefined);

  const ignoredTransfer = TestHelpers.BalancerAutoRangePool.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: user,
    value: 50n,
    ...eventData(
      LP_V2_RESUME_CUTOVER_BLOCK + 2,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 120,
      BALANCER_POOL
    ),
  });
  mockDb = await TestHelpers.BalancerAutoRangePool.Transfer.processEvent({
    event: ignoredTransfer,
    mockDb,
  });

  const ignoredLiquidityAdded = TestHelpers.BalancerVault.LiquidityAdded.createMockEvent({
    pool: BALANCER_POOL,
    liquidityProvider: user,
    kind: 0n,
    totalSupply: 2_000n,
    amountsAddedRaw: [100n * 10n ** 6n, 50n * 10n ** 18n],
    swapFeeAmountsRaw: [0n, 0n],
    ...eventData(
      LP_V2_RESUME_CUTOVER_BLOCK + 3,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 180,
      BALANCER_VAULT_ADDRESS
    ),
  });
  mockDb = await TestHelpers.BalancerVault.LiquidityAdded.processEvent({
    event: ignoredLiquidityAdded,
    mockDb,
  });

  const ignoredLiquidityRemoved = TestHelpers.BalancerVault.LiquidityRemoved.createMockEvent({
    pool: BALANCER_POOL,
    liquidityProvider: user,
    kind: 0n,
    totalSupply: 500n,
    amountsRemovedRaw: [25n * 10n ** 6n, 10n * 10n ** 18n],
    swapFeeAmountsRaw: [0n, 0n],
    ...eventData(
      LP_V2_RESUME_CUTOVER_BLOCK + 4,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 240,
      BALANCER_VAULT_ADDRESS
    ),
  });
  mockDb = await TestHelpers.BalancerVault.LiquidityRemoved.processEvent({
    event: ignoredLiquidityRemoved,
    mockDb,
  });

  const ignoredSwap = TestHelpers.BalancerVault.Swap.createMockEvent({
    pool: BALANCER_POOL,
    tokenIn: DUST_ADDRESS,
    tokenOut: USDC_ADDRESS,
    amountIn: 10n * 10n ** 18n,
    amountOut: 5n * 10n ** 6n,
    swapFeePercentage: 10n ** 16n,
    swapFeeAmount: 10n ** 16n,
    ...eventData(
      LP_V2_RESUME_CUTOVER_BLOCK + 5,
      LP_V2_RESUME_CUTOVER_TIMESTAMP + 300,
      BALANCER_VAULT_ADDRESS
    ),
  });
  mockDb = await TestHelpers.BalancerVault.Swap.processEvent({ event: ignoredSwap, mockDb });

  assert.deepEqual(mockDb.entities.LPPoolConfig.get(BALANCER_POOL), boundaryBalancerConfig);
  assert.deepEqual(mockDb.entities.LPPoolConfig.get(V2_POOL), boundaryV2Config);
  assert.deepEqual(mockDb.entities.LPPoolState.get(BALANCER_POOL), boundaryPoolState);
  assert.deepEqual(mockDb.entities.LPPoolV2State.get(BALANCER_POOL), boundaryV2State);
  assert.deepEqual(mockDb.entities.UserLPPosition.get(balancerPositionId), boundaryPosition);
  assert.deepEqual(mockDb.entities.UserEpochStats.get(`${user}:1`), boundaryEpochStats);
  assert.deepEqual(mockDb.entities.LPPoolFeeStats.get(BALANCER_POOL), boundaryFeeStats);
  assert.equal(mockDb.entities.ProtocolStats.get('1'), undefined);
});

test('uniswap v2 keeps accruing points after the resume cutover (does not freeze at the stale balancer cap)', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seedLeaderboardConfig(TestHelpers, mockDb);
  const eventData = createEventDataFactory();
  const user = ADDRESSES.user;
  const positionId = `v2:${V2_POOL}:${user}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
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
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [V2_POOL, BALANCER_POOL],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: V2_POOL,
    pool: V2_POOL,
    positionManager: V2_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 3000,
    lpRateBps: 2500n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: BALANCER_POOL,
    pool: BALANCER_POOL,
    positionManager: BALANCER_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 10000,
    lpRateBps: 2500n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: V2_POOL,
    pool: V2_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: V2_POOL,
    pool: V2_POOL,
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    lpTotalSupply: 1_000n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: positionId,
    tokenId: BigInt(user),
    user_id: user,
    pool: V2_POOL,
    positionManager: V2_POOL,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1_000n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 100n * 10n ** 8n,
    lastInRangeTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
    settledLpPoints: 0n,
    createdAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
    lastUpdate: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: user,
    user_id: user,
    positionIds: [positionId],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: V2_POOL,
    pool: V2_POOL,
    positionIds: [positionId],
    lastUpdate: 0,
  });

  // First V2 event after resume: triggers the Balancer -> V2 transition and
  // settles the position up to the resume cutover.
  const firstPostResumeSync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 1, LP_V2_RESUME_CUTOVER_TIMESTAMP + 60, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: firstPostResumeSync,
    mockDb,
  });

  const pointsAfterFirstEvent = mockDb.entities.UserEpochStats.get(`${user}:1`)?.lpPoints ?? 0n;
  assert.ok(pointsAfterFirstEvent > 0n);
  const positionAfterFirstEvent = mockDb.entities.UserLPPosition.get(positionId);
  assert.equal(positionAfterFirstEvent?.lastSettledAt, LP_V2_RESUME_CUTOVER_TIMESTAMP + 60);

  // Second V2 event, an hour later: if the accrual cap were still stuck at the
  // stale Balancer cutover timestamp, no further points would accrue here.
  const secondPostResumeSync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 2, LP_V2_RESUME_CUTOVER_TIMESTAMP + 3660, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: secondPostResumeSync,
    mockDb,
  });

  const pointsAfterSecondEvent = mockDb.entities.UserEpochStats.get(`${user}:1`)?.lpPoints ?? 0n;
  assert.ok(pointsAfterSecondEvent > pointsAfterFirstEvent);
  const positionAfterSecondEvent = mockDb.entities.UserLPPosition.get(positionId);
  assert.equal(positionAfterSecondEvent?.lastSettledAt, LP_V2_RESUME_CUTOVER_TIMESTAMP + 3660);
});

test('uniswap v2 replays paused transfer and sync bookkeeping without accrual', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seedLeaderboardConfig(TestHelpers, mockDb);
  const eventData = createEventDataFactory();
  const holderA = ADDRESSES.user;
  const burnedHolder = ADDRESSES.token0;
  const holderC = ADDRESSES.token1;
  const holderAPositionId = `v2:${V2_POOL}:${holderA}`;
  const burnedPositionId = `v2:${V2_POOL}:${burnedHolder}`;
  const holderCPositionId = `v2:${V2_POOL}:${holderC}`;
  const baselineUniqueUsers = 41;
  const baselineTransactions = 17n;
  const baselineTxHash = `0x${'ab'.repeat(32)}`;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 7n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '7',
    epochNumber: 7n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [V2_POOL, BALANCER_POOL],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: V2_POOL,
    pool: V2_POOL,
    positionManager: V2_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 3000,
    lpRateBps: 2500n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    lastUpdate: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: BALANCER_POOL,
    pool: BALANCER_POOL,
    positionManager: BALANCER_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 10000,
    lpRateBps: 2500n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: V2_POOL,
    pool: V2_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: V2_POOL,
    pool: V2_POOL,
    reserve0: 0n,
    reserve1: 0n,
    lpTotalSupply: 0n,
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: USDC_ADDRESS,
    address: USDC_ADDRESS,
    decimals: DECIMALS,
    symbol: 'USDC',
    name: 'USD Coin',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: DUST_ADDRESS,
    address: DUST_ADDRESS,
    decimals: DUST_DECIMALS,
    symbol: 'DUST',
    name: 'Dust',
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.ProtocolStats.set({
    id: '1',
    tvlUsd: 1,
    suppliesUsd: 2,
    borrowsUsd: 3,
    availableUsd: 4,
    combinedTvlUsd: 5,
    combinedSuppliesUsd: 6,
    combinedBorrowsUsd: 7,
    combinedAvailableUsd: 8,
    tvlE8: 1n,
    suppliesE8: 2n,
    borrowsE8: 3n,
    availableE8: 4n,
    combinedTvlE8: 5n,
    combinedSuppliesE8: 6n,
    combinedBorrowsE8: 7n,
    combinedAvailableE8: 8n,
    totalRevenueUsd: 9,
    supplyRevenueUsd: 10,
    protocolRevenueUsd: 11,
    updatedAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 60,
    totalTransactions: baselineTransactions,
    totalSelfRepayVolume: 12n,
    totalSelfRepayCount: 13n,
    totalDustTransfers: 14n,
    uniqueUsers: baselineUniqueUsers,
    lastTxTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 60,
    lastTxHash: baselineTxHash,
  });

  const pausedBlock = LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1;
  const pausedTimestamp = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 60;
  const pausedMint = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: holderA,
    value: 1_000n,
    ...eventData(pausedBlock, pausedTimestamp, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: pausedMint, mockDb });

  const pausedHolderTransfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: holderA,
    to: burnedHolder,
    value: 400n,
    ...eventData(pausedBlock + 1, pausedTimestamp + 60, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: pausedHolderTransfer,
    mockDb,
  });

  const pausedBurn = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: burnedHolder,
    to: ZERO_ADDRESS,
    value: 400n,
    ...eventData(pausedBlock + 2, pausedTimestamp + 120, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: pausedBurn, mockDb });

  const pausedCurrentHolderTransfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: holderA,
    to: holderC,
    value: 100n,
    ...eventData(pausedBlock + 3, pausedTimestamp + 180, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: pausedCurrentHolderTransfer,
    mockDb,
  });

  const pausedSync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 600n * 10n ** 6n,
    reserve1: 300n * 10n ** 18n,
    ...eventData(pausedBlock + 4, pausedTimestamp + 240, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({ event: pausedSync, mockDb });

  const pausedV2State = mockDb.entities.LPPoolV2State.get(V2_POOL);
  assert.equal(pausedV2State?.lpTotalSupply, 600n);
  assert.equal(pausedV2State?.reserve0, 600n * 10n ** 6n);
  assert.equal(pausedV2State?.reserve1, 300n * 10n ** 18n);

  const pausedPoolState = mockDb.entities.LPPoolState.get(V2_POOL);
  assert.equal(pausedPoolState?.token0Price, PRICE_E8);
  assert.equal(pausedPoolState?.token1Price, 2n * PRICE_E8);

  const pausedHolderA = mockDb.entities.UserLPPosition.get(holderAPositionId);
  assert.equal(pausedHolderA?.liquidity, 500n);
  assert.equal(pausedHolderA?.amount0, 500n * 10n ** 6n);
  assert.equal(pausedHolderA?.amount1, 250n * 10n ** 18n);
  assert.equal(pausedHolderA?.valueUsd, 1_000n * PRICE_E8);
  assert.equal(pausedHolderA?.accumulatedInRangeSeconds, 0n);
  assert.equal(pausedHolderA?.settledLpPoints, 0n);

  const pausedBurnedHolder = mockDb.entities.UserLPPosition.get(burnedPositionId);
  assert.equal(pausedBurnedHolder?.liquidity, 0n);
  assert.equal(pausedBurnedHolder?.amount0, 0n);
  assert.equal(pausedBurnedHolder?.amount1, 0n);
  assert.equal(pausedBurnedHolder?.valueUsd, 0n);
  assert.equal(pausedBurnedHolder?.accumulatedInRangeSeconds, 0n);
  assert.equal(pausedBurnedHolder?.settledLpPoints, 0n);

  const pausedHolderC = mockDb.entities.UserLPPosition.get(holderCPositionId);
  assert.equal(pausedHolderC?.liquidity, 100n);
  assert.equal(pausedHolderC?.amount0, 100n * 10n ** 6n);
  assert.equal(pausedHolderC?.amount1, 50n * 10n ** 18n);
  assert.equal(pausedHolderC?.valueUsd, 200n * PRICE_E8);
  assert.equal(pausedHolderC?.accumulatedInRangeSeconds, 0n);
  assert.equal(pausedHolderC?.settledLpPoints, 0n);

  assert.deepEqual(mockDb.entities.UserLPPositionIndex.get(holderA)?.positionIds, [
    holderAPositionId,
  ]);
  assert.deepEqual(mockDb.entities.UserLPPositionIndex.get(burnedHolder)?.positionIds, []);
  assert.deepEqual(mockDb.entities.UserLPPositionIndex.get(holderC)?.positionIds, [
    holderCPositionId,
  ]);
  assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(V2_POOL)?.positionIds, [
    holderAPositionId,
    holderCPositionId,
  ]);
  assert.equal(mockDb.entities.UserEpochStats.get(`${holderA}:7`), undefined);
  assert.equal(mockDb.entities.UserEpochStats.get(`${burnedHolder}:7`), undefined);
  assert.equal(mockDb.entities.UserEpochStats.get(`${holderC}:7`), undefined);
  assert.equal(mockDb.entities.LPPoolFeeStats.get(V2_POOL), undefined);
  const pausedProtocolStats = mockDb.entities.ProtocolStats.get('1');
  assert.equal(pausedProtocolStats?.uniqueUsers, baselineUniqueUsers);
  assert.equal(pausedProtocolStats?.totalTransactions, baselineTransactions);
  assert.equal(pausedProtocolStats?.lastTxHash, baselineTxHash);
  assert.equal(mockDb.entities.User.get(holderA), undefined);
  assert.equal(mockDb.entities.User.get(burnedHolder), undefined);
  assert.equal(mockDb.entities.User.get(holderC), undefined);

  const resumeSync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 600n * 10n ** 6n,
    reserve1: 300n * 10n ** 18n,
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 1, LP_V2_RESUME_CUTOVER_TIMESTAMP + 60, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({ event: resumeSync, mockDb });

  const resumedV2Config = mockDb.entities.LPPoolConfig.get(V2_POOL);
  assert.equal(resumedV2Config?.isActive, true);
  assert.equal(resumedV2Config?.enabledAtEpoch, 7n);
  assert.equal(resumedV2Config?.enabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP);
  assert.equal(resumedV2Config?.disabledAtEpoch, undefined);
  assert.equal(resumedV2Config?.disabledAtTimestamp, undefined);
  const resumedProtocolStats = mockDb.entities.ProtocolStats.get('1');
  assert.equal(resumedProtocolStats?.uniqueUsers, baselineUniqueUsers + 2);
  assert.equal(resumedProtocolStats?.totalTransactions, baselineTransactions + 1n);
  assert.ok(mockDb.entities.User.get(holderA));
  assert.ok(mockDb.entities.User.get(holderC));
  assert.equal(mockDb.entities.User.get(burnedHolder), undefined);

  const expectedHolderAPoints =
    (1_000n * PRICE_E8 * 2500n * 60n * 10n ** 18n) / (PRICE_E8 * 10_000n * 86_400n);
  const expectedHolderCPoints =
    (200n * PRICE_E8 * 2500n * 60n * 10n ** 18n) / (PRICE_E8 * 10_000n * 86_400n);
  const resumedHolderA = mockDb.entities.UserLPPosition.get(holderAPositionId);
  const resumedHolderC = mockDb.entities.UserLPPosition.get(holderCPositionId);
  assert.equal(resumedHolderA?.accumulatedInRangeSeconds, 60n);
  assert.equal(resumedHolderA?.settledLpPoints, expectedHolderAPoints);
  assert.equal(resumedHolderC?.accumulatedInRangeSeconds, 60n);
  assert.equal(resumedHolderC?.settledLpPoints, expectedHolderCPoints);
  assert.equal(mockDb.entities.UserEpochStats.get(`${holderA}:7`)?.lpPoints, expectedHolderAPoints);
  assert.equal(mockDb.entities.UserEpochStats.get(`${holderC}:7`)?.lpPoints, expectedHolderCPoints);
  assert.equal(mockDb.entities.UserEpochStats.get(`${burnedHolder}:7`), undefined);
  assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(V2_POOL)?.positionIds, [
    holderAPositionId,
    holderCPositionId,
  ]);

  const idempotentResumeSync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 600n * 10n ** 6n,
    reserve1: 300n * 10n ** 18n,
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 2, LP_V2_RESUME_CUTOVER_TIMESTAMP + 60, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: idempotentResumeSync,
    mockDb,
  });

  const idempotentHolderA = mockDb.entities.UserLPPosition.get(holderAPositionId);
  const idempotentHolderC = mockDb.entities.UserLPPosition.get(holderCPositionId);
  assert.equal(idempotentHolderA?.accumulatedInRangeSeconds, 60n);
  assert.equal(idempotentHolderA?.settledLpPoints, expectedHolderAPoints);
  assert.equal(idempotentHolderC?.accumulatedInRangeSeconds, 60n);
  assert.equal(idempotentHolderC?.settledLpPoints, expectedHolderCPoints);
  assert.equal(mockDb.entities.UserEpochStats.get(`${holderA}:7`)?.lpPoints, expectedHolderAPoints);
  assert.equal(mockDb.entities.UserEpochStats.get(`${holderC}:7`)?.lpPoints, expectedHolderCPoints);
  assert.equal(mockDb.entities.ProtocolStats.get('1')?.uniqueUsers, baselineUniqueUsers + 2);
  assert.ok(mockDb.entities.User.get(holderA));
  assert.ok(mockDb.entities.User.get(holderC));
  assert.equal(mockDb.entities.User.get(burnedHolder), undefined);
});

test('lp v2 resume boundary is block-authoritative with timestamp fallback', () => {
  const isPastResume = getLpV2ResumeCutoverPredicate();

  assert.equal(isPastResume(1783827555, 87190221n), false);
  assert.equal(isPastResume(1783827615, 87190221n), false);
  assert.equal(isPastResume(1783827495, 87190222n), true);
  assert.equal(isPastResume(1783827555, 87190222n), true);
  assert.equal(isPastResume(1783827554), false);
  assert.equal(isPastResume(1783827555), true);
});
