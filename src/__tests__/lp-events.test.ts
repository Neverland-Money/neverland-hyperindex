import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { AUSD_ADDRESS, ZERO_ADDRESS } from '../helpers/constants';
import { installViemMock, setLPPositionOverride } from './viem-mock';

process.env.DISABLE_EXTERNAL_CALLS = 'true';
process.env.DISABLE_ETH_CALLS = 'true';
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

const ADDRESSES = {
  positionManager: '0x000000000000000000000000000000000000a001',
  pool: '0x000000000000000000000000000000000000a002',
  token0: '0x000000000000000000000000000000000000a003',
  token1: '0x000000000000000000000000000000000000a004',
  user: '0x000000000000000000000000000000000000a005',
};

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
    'lp',
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

test('increase liquidity before transfer uses cached mint amounts', async () => {
  const prevDisableExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const prevDisableEth = process.env.DISABLE_ETH_CALLS;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
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

    // IncreaseLiquidity now creates position directly (no pending data)
    const positionAfterIncrease = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
    assert.ok(positionAfterIncrease);
    assert.equal(positionAfterIncrease?.amount0, AMOUNT0);
    assert.equal(positionAfterIncrease?.amount1, AMOUNT1);

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
    assert.ok(position);
    assert.equal(position?.amount0, AMOUNT0);
    assert.equal(position?.amount1, AMOUNT1);
    assert.equal(position?.valueUsd, EXPECTED_VALUE_USD);

    // Mint data should be cleaned up after position creation
    const pendingKey = `pending:${TOKEN_ID.toString()}`;
    const pendingAfter = mockDb.entities.LPMintData.get(pendingKey);
    assert.equal(pendingAfter, undefined);

    setLPPositionOverride(undefined);
  } finally {
    process.env.DISABLE_EXTERNAL_CALLS = prevDisableExternal;
    process.env.DISABLE_ETH_CALLS = prevDisableEth;
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

  // IncreaseLiquidity now creates position directly using Pool.Mint data
  const positionAfterIncrease = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.ok(positionAfterIncrease);
  assert.equal(positionAfterIncrease?.tickLower, TICK_LOWER);
  assert.equal(positionAfterIncrease?.tickUpper, TICK_UPPER);

  // Pool mint data should be cleaned up
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
  const prevDisableExternal = process.env.DISABLE_EXTERNAL_CALLS;
  const prevDisableEth = process.env.DISABLE_ETH_CALLS;
  process.env.DISABLE_EXTERNAL_CALLS = 'false';
  process.env.DISABLE_ETH_CALLS = 'false';
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
    process.env.DISABLE_EXTERNAL_CALLS = prevDisableExternal;
    process.env.DISABLE_ETH_CALLS = prevDisableEth;
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
  assert.equal(feeStats?.volumeUsd24h, 200000000n);
  assert.equal(feeStats?.feesUsd24h, 2000000n);
  assert.equal(feeStats?.feeAprBps, 73n);
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
