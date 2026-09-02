import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import {
  TestHelpers,
  getRegisteredEventHandler,
  processEvents,
  entityStores as snapshotEntityStores,
  type EntityRow,
} from './v3-test-helpers';

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
import { LP_GROWTH_Q128 } from '../helpers/lpGrowthMath';
import { getAmountsForLiquidity, getSqrtRatioAtTick } from '../helpers/uniswapV3';
import * as lpHandlers from '../handlers/lp';
import { lpPoolEpochGrowthId } from '../handlers/lpGrowth';
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

const TASK4_POOL = '0x000000000000000000000000000000000000c401';
const TASK4_MANAGER = '0x000000000000000000000000000000000000c402';
const TASK4_PAIRED_TOKEN = '0x000000000000000000000000000000000000c403';
const TASK4_USER = '0x000000000000000000000000000000000000c404';
const TASK4_USER_2 = '0x000000000000000000000000000000000000c405';
const TASK4_POSITION_ID = '4001';
const TASK4_TOKEN_ID = 4001n;
const TASK4_LIQUIDITY = 10n ** 18n;
const TASK4_RATE_BPS = 10_000n;
const Q96 = 1n << 96n;
const Q192 = 1n << 192n;
const POINTS_SCALE = 10n ** 18n;
const POINTS_DENOMINATOR = PRICE_E8 * 10_000n * 86_400n;
const TASK5_POOL = '0x000000000000000000000000000000000000c501';
const TASK5_TOKEN = '0x000000000000000000000000000000000000c502';
const TASK5_USER_A = '0x000000000000000000000000000000000000c503';
const TASK5_USER_B = '0x000000000000000000000000000000000000c504';
const TASK5_START = 100;
const TASK5_EVENT = 200;
const TASK5_RATE_BPS = 10_000n;

type MockDb = ReturnType<TestHelpersApi['MockDb']['createMockDb']>;

type V3ReferenceInterval = {
  sqrtPriceX96: bigint;
  tick: number;
  tickLower: number;
  tickUpper: number;
  liquidity: bigint;
  stableTokenIndex: 0 | 1;
  stableTokenDecimals: number;
  seconds: number;
  rateBps: bigint;
};

function ceilDiv(numerator: bigint, denominator: bigint): bigint {
  return numerator === 0n ? 0n : (numerator + denominator - 1n) / denominator;
}

function maxBigInt(...values: bigint[]): bigint {
  return values.reduce((maximum, value) => (value > maximum ? value : maximum));
}

function absoluteDifference(left: bigint, right: bigint): bigint {
  return left >= right ? left - right : right - left;
}

function referenceV3Points(input: V3ReferenceInterval): bigint {
  if (input.seconds === 0 || input.tick < input.tickLower || input.tick >= input.tickUpper) {
    return 0n;
  }
  const { amount0, amount1 } = getAmountsForLiquidity(
    input.sqrtPriceX96,
    input.tickLower,
    input.tickUpper,
    input.liquidity
  );
  const stableScale = 10n ** BigInt(input.stableTokenDecimals);
  const stableValue = input.stableTokenIndex === 0 ? amount0 : amount1;
  const priceX192 = input.sqrtPriceX96 * input.sqrtPriceX96;
  const pairedValue =
    input.stableTokenIndex === 0 ? (amount1 * Q192) / priceX192 : (amount0 * priceX192) / Q192;
  const valueE8 = ((stableValue + pairedValue) * PRICE_E8) / stableScale;
  return (valueE8 * input.rateBps * BigInt(input.seconds) * POINTS_SCALE) / POINTS_DENOMINATOR;
}

function referenceFungibleGrowthX128(input: {
  reserve0: bigint;
  reserve1: bigint;
  token0PriceE8: bigint;
  token1PriceE8: bigint;
  token0Decimals: number;
  token1Decimals: number;
  totalSupply: bigint;
  seconds: number;
  rateBps: bigint;
}): bigint {
  if (input.totalSupply <= 0n || input.seconds <= 0 || input.rateBps <= 0n) return 0n;
  const poolValueE8 =
    (input.reserve0 * input.token0PriceE8) / 10n ** BigInt(input.token0Decimals) +
    (input.reserve1 * input.token1PriceE8) / 10n ** BigInt(input.token1Decimals);
  const valuePerShareE8X128 = (poolValueE8 * LP_GROWTH_Q128) / input.totalSupply;
  return valuePerShareE8X128 * input.rateBps * BigInt(input.seconds);
}

function referenceFungiblePoints(liquidity: bigint, growthX128: bigint): bigint {
  return (liquidity * growthX128 * POINTS_SCALE) / (LP_GROWTH_Q128 * POINTS_DENOMINATOR);
}

function v3ReferenceRoundingBound(input: V3ReferenceInterval): bigint {
  if (input.seconds === 0 || input.tick < input.tickLower || input.tick >= input.tickUpper) {
    return 0n;
  }
  const stableScale = 10n ** BigInt(input.stableTokenDecimals);
  const priceX192 = input.sqrtPriceX96 * input.sqrtPriceX96;
  const pairedRawUnitInStable =
    input.stableTokenIndex === 0 ? ceilDiv(Q192, priceX192) : ceilDiv(priceX192, Q192);
  const referenceRawFloorBound = 2n + pairedRawUnitInStable;
  const referenceValueE8FloorBound = ceilDiv(referenceRawFloorBound * PRICE_E8, stableScale) + 1n;
  const referencePointsBound =
    ceilDiv(
      referenceValueE8FloorBound * input.rateBps * BigInt(input.seconds) * POINTS_SCALE,
      POINTS_DENOMINATOR
    ) + 1n;

  const sqrtLowerX96 = getSqrtRatioAtTick(input.tickLower);
  const sqrtUpperX96 = getSqrtRatioAtTick(input.tickUpper);
  const largestMomentCoefficient = maxBigInt(
    2n,
    ceilDiv(Q96, sqrtUpperX96),
    ceilDiv(sqrtLowerX96, Q96)
  );
  const momentCoreX128Bound = 5n * largestMomentCoefficient + 2n;
  const momentGrowthX128Bound = ceilDiv(momentCoreX128Bound * PRICE_E8, stableScale) + 1n;
  const lazyPointsBound =
    ceilDiv(
      input.liquidity * momentGrowthX128Bound * POINTS_SCALE,
      LP_GROWTH_Q128 * POINTS_DENOMINATOR
    ) + 1n;
  return referencePointsBound + lazyPointsBound;
}

function seedTask4V3Fixture(
  mockDb: MockDb,
  input: {
    startTimestamp?: number;
    currentTick?: number;
    sqrtPriceX96?: bigint;
    includeState?: boolean;
    includePosition?: boolean;
    liquidity?: bigint;
    user?: string;
  } = {}
): MockDb {
  const startTimestamp = input.startTimestamp ?? 100;
  const currentTick = input.currentTick ?? 0;
  const sqrtPriceX96 = input.sqrtPriceX96 ?? getSqrtRatioAtTick(currentTick);
  const includeState = input.includeState ?? true;
  const includePosition = input.includePosition ?? true;
  const user = input.user ?? TASK4_USER;
  const liquidity = input.liquidity ?? TASK4_LIQUIDITY;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 41_231_451n,
    startTime: startTimestamp,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [TASK4_POOL],
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: TASK4_POOL,
    pool: TASK4_POOL,
    positionManager: TASK4_MANAGER,
    token0: AUSD_ADDRESS,
    token1: TASK4_PAIRED_TOKEN,
    fee: 0,
    lpRateBps: TASK4_RATE_BPS,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: startTimestamp,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: 6,
    symbol: 'AUSD',
    name: 'AUSD',
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: TASK4_PAIRED_TOKEN,
    address: TASK4_PAIRED_TOKEN,
    decimals: 6,
    symbol: 'PAIR',
    name: 'Paired token',
    lastUpdate: startTimestamp,
  });
  if (includeState) {
    mockDb = mockDb.entities.LPPoolState.set({
      id: TASK4_POOL,
      pool: TASK4_POOL,
      currentTick,
      sqrtPriceX96,
      token0Price: PRICE_E8,
      token1Price: PRICE_E8,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: startTimestamp,
    });
  }
  if (includePosition) {
    const amounts = getAmountsForLiquidity(sqrtPriceX96, TICK_LOWER, TICK_UPPER, liquidity);
    const isInRange = currentTick >= TICK_LOWER && currentTick < TICK_UPPER;
    // Under the per-swap fanout, settleLPPosition derives points from position.valueUsd
    // (lp.ts gates on `valueUsd > 0n`). The lazy model took points from liquidity x growth,
    // so a zero here was harmless then and silently zeroes every accrual now. Seed the same
    // value the fanout itself would compute; both fixture tokens are 6-decimal.
    const valueUsd = lpHandlers.calculatePositionValueUsd(
      amounts.amount0,
      amounts.amount1,
      PRICE_E8,
      PRICE_E8,
      6,
      6
    );
    mockDb = mockDb.entities.UserLPPosition.set({
      id: TASK4_POSITION_ID,
      tokenId: TASK4_TOKEN_ID,
      user_id: user,
      pool: TASK4_POOL,
      positionManager: TASK4_MANAGER,
      tickLower: TICK_LOWER,
      tickUpper: TICK_UPPER,
      liquidity,
      amount0: amounts.amount0,
      amount1: amounts.amount1,
      isInRange,
      valueUsd,
      lastInRangeTimestamp: isInRange ? startTimestamp : 0,
      accumulatedInRangeSeconds: 0n,
      lastSettledAt: startTimestamp,
      settledLpPoints: 0n,
      createdAt: startTimestamp,
      lastUpdate: startTimestamp,
    });
    mockDb = mockDb.entities.UserLPPositionIndex.set({
      id: user,
      user_id: user,
      positionIds: [TASK4_POSITION_ID],
      lastUpdate: startTimestamp,
    });
    mockDb = mockDb.entities.LPPoolPositionIndex.set({
      id: TASK4_POOL,
      pool: TASK4_POOL,
      positionIds: [TASK4_POSITION_ID],
      lastUpdate: startTimestamp,
    });
  }
  return mockDb;
}

function createInstrumentedHandlerContext(mockDb: MockDb, isPreload: boolean) {
  const stores = new Map(
    Array.from(snapshotEntityStores(mockDb), ([entityName, rows]) => [entityName, new Map(rows)])
  );
  const getCounts = new Map<string, number>();
  const getIds = new Map<string, string[]>();
  const setCounts = new Map<string, number>();
  const operationTrace: string[] = [];

  const storeFor = (entityName: string) => ({
    async get(id: string) {
      operationTrace.push(`get|${entityName}|${id}`);
      getCounts.set(entityName, (getCounts.get(entityName) ?? 0) + 1);
      const ids = getIds.get(entityName) ?? [];
      ids.push(id);
      getIds.set(entityName, ids);
      return stores.get(entityName)?.get(id);
    },
    async getWhere() {
      operationTrace.push(`scan|${entityName}|getWhere`);
      getCounts.set(entityName, (getCounts.get(entityName) ?? 0) + 1);
      return Array.from(stores.get(entityName)?.values() ?? []);
    },
    async getAll() {
      operationTrace.push(`scan|${entityName}|getAll`);
      getCounts.set(entityName, (getCounts.get(entityName) ?? 0) + 1);
      return Array.from(stores.get(entityName)?.values() ?? []);
    },
    set(row: { id: string }) {
      operationTrace.push(`set|${entityName}|${row.id}`);
      setCounts.set(entityName, (setCounts.get(entityName) ?? 0) + 1);
      if (isPreload) return;
      let rows = stores.get(entityName);
      if (!rows) {
        rows = new Map();
        stores.set(entityName, rows);
      }
      rows.set(row.id, row);
    },
    deleteUnsafe(id: string) {
      operationTrace.push(`delete|${entityName}|${id}`);
      if (!isPreload) stores.get(entityName)?.delete(id);
    },
  });
  const entityStores = new Map<string, ReturnType<typeof storeFor>>();
  const context = new Proxy(
    {
      isPreload,
      log: { debug() {} },
    } as Record<string, unknown>,
    {
      get(target, property: string) {
        if (property in target) return target[property];
        let store = entityStores.get(property);
        if (!store) {
          store = storeFor(property);
          entityStores.set(property, store);
        }
        return store;
      },
    }
  );

  return { context, stores, getCounts, getIds, setCounts, operationTrace };
}

type RegisteredEventHandler = Awaited<ReturnType<typeof getRegisteredEventHandler>>;
type RegisteredHandlerEvent = Parameters<RegisteredEventHandler>[0]['event'];

async function traceRegisteredEventBatch(
  mockDb: MockDb,
  entries: readonly {
    handler: RegisteredEventHandler;
    event: RegisteredHandlerEvent;
  }[]
) {
  const preloadProbe = createInstrumentedHandlerContext(mockDb, true);
  for (const entry of entries) {
    await entry.handler({ event: entry.event, context: preloadProbe.context });
  }

  const orderedProbe = createInstrumentedHandlerContext(mockDb, false);
  for (const entry of entries) {
    await entry.handler({ event: entry.event, context: orderedProbe.context });
  }

  return { preloadProbe, orderedProbe };
}

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

function seedTask5FungibleFixture(
  mockDb: MockDb,
  input: {
    pool?: string;
    positionManager?: string;
    token0?: string;
    token1?: string;
    fee?: number;
    startTimestamp?: number;
    active?: boolean;
    reserve0?: bigint;
    reserve1?: bigint;
    totalSupply?: bigint;
    includePoolState?: boolean;
    includeV2State?: boolean;
    includeGrowth?: boolean;
    growthX128?: bigint;
    growthLastTimestamp?: number;
    positions?: readonly {
      user: string;
      liquidity: bigint;
      createdAt?: number;
      cursorBaselineX128?: bigint | null;
      cursorLastSettledAt?: number;
    }[];
  } = {}
): MockDb {
  const pool = input.pool ?? TASK5_POOL;
  const positionManager = input.positionManager ?? pool;
  const token0 = input.token0 ?? USDC_ADDRESS;
  const token1 = input.token1 ?? TASK5_TOKEN;
  const fee = input.fee ?? 3000;
  const startTimestamp = input.startTimestamp ?? TASK5_START;
  const active = input.active ?? true;
  const reserve0 = input.reserve0 ?? 500n;
  const reserve1 = input.reserve1 ?? 500n;
  const totalSupply = input.totalSupply ?? 1_000n;
  const growthLastTimestamp = input.growthLastTimestamp ?? startTimestamp;
  const positions = input.positions ?? [];

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: startTimestamp,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [pool],
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: pool,
    pool,
    positionManager,
    token0,
    token1,
    fee,
    lpRateBps: TASK5_RATE_BPS,
    isActive: active,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: startTimestamp,
    disabledAtEpoch: active ? undefined : 1n,
    disabledAtTimestamp: active ? undefined : growthLastTimestamp,
    lastUpdate: growthLastTimestamp,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: token0,
    address: token0,
    decimals: 0,
    symbol: 'USDC0',
    name: 'USDC test units',
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.TokenInfo.set({
    id: token1,
    address: token1,
    decimals: 0,
    symbol: 'T5',
    name: 'Task 5 token',
    lastUpdate: startTimestamp,
  });
  if (input.includePoolState ?? true) {
    mockDb = mockDb.entities.LPPoolState.set({
      id: pool,
      pool,
      currentTick: 0,
      sqrtPriceX96: 0n,
      token0Price: PRICE_E8,
      token1Price: PRICE_E8,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: startTimestamp,
    });
  }
  if (input.includeV2State ?? true) {
    mockDb = mockDb.entities.LPPoolV2State.set({
      id: pool,
      pool,
      reserve0,
      reserve1,
      lpTotalSupply: totalSupply,
      lastUpdate: startTimestamp,
    });
  }
  if (input.includeGrowth ?? true) {
    mockDb = mockDb.entities.LPPoolEpochGrowth.set({
      id: lpPoolEpochGrowthId(pool, 1n),
      pool,
      epochNumber: 1n,
      startTimestamp,
      lastTimestamp: growthLastTimestamp,
      scalarGrowthX128: input.growthX128 ?? 0n,
      isFrozen: false,
      frozenAt: undefined,
      lastUpdate: growthLastTimestamp,
    });
  }

  const positionIds: string[] = [];
  for (const seed of positions) {
    const positionId = `v2:${pool}:${seed.user}`;
    positionIds.push(positionId);
    const createdAt = seed.createdAt ?? startTimestamp;
    mockDb = mockDb.entities.UserLPPosition.set({
      id: positionId,
      tokenId: BigInt(seed.user),
      user_id: seed.user,
      pool,
      positionManager,
      tickLower: -887272,
      tickUpper: 887272,
      liquidity: seed.liquidity,
      amount0: 0n,
      amount1: 0n,
      isInRange: seed.liquidity > 0n,
      valueUsd: 0n,
      lastInRangeTimestamp: seed.liquidity > 0n ? createdAt : 0,
      accumulatedInRangeSeconds: 0n,
      lastSettledAt: createdAt,
      settledLpPoints: 0n,
      createdAt,
      lastUpdate: createdAt,
    });
    mockDb = mockDb.entities.UserLPPositionIndex.set({
      id: seed.user,
      user_id: seed.user,
      positionIds: [positionId],
      lastUpdate: createdAt,
    });
    if (seed.cursorBaselineX128 !== null) {
      mockDb = mockDb.entities.UserLPEpochCursor.set({
        id: `${positionId}:1`,
        position_id: positionId,
        user_id: seed.user,
        pool,
        epochNumber: 1n,
        growthBaselineX128: seed.cursorBaselineX128 ?? 0n,
        lastSettledAt: seed.cursorLastSettledAt ?? createdAt,
        lastUpdate: seed.cursorLastSettledAt ?? createdAt,
      });
    }
  }
  if (positionIds.length > 0) {
    mockDb = mockDb.entities.LPPoolPositionIndex.set({
      id: pool,
      pool,
      positionIds,
      lastUpdate: startTimestamp,
    });
  }
  return mockDb;
}

function seedTask5BalancerFixture(
  mockDb: MockDb,
  positions: readonly {
    user: string;
    liquidity: bigint;
    createdAt?: number;
    cursorBaselineX128?: bigint | null;
    cursorLastSettledAt?: number;
  }[] = []
): MockDb {
  const startTimestamp = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP;
  mockDb = seedTask5FungibleFixture(mockDb, {
    pool: BALANCER_POOL,
    positionManager: BALANCER_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 10_000,
    startTimestamp,
    positions,
  });
  mockDb = mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    lpRateBps: TASK5_RATE_BPS,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [LEGACY_V3_POOL, V2_POOL, BALANCER_POOL],
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    positionManager: LEGACY_V3_POSITION_MANAGER,
    token0: AUSD_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 10_000,
    lpRateBps: TASK5_RATE_BPS,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: TASK5_START,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: V2_POOL,
    pool: V2_POOL,
    positionManager: V2_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 3000,
    lpRateBps: TASK5_RATE_BPS,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: startTimestamp,
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP,
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
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: V2_POOL,
    pool: V2_POOL,
    reserve0: 0n,
    reserve1: 0n,
    lpTotalSupply: 0n,
    lastUpdate: startTimestamp,
  });
  return mockDb;
}

function seedTask5CanonicalV2Fixture(
  mockDb: MockDb,
  positions: readonly {
    user: string;
    liquidity: bigint;
    createdAt?: number;
    cursorBaselineX128?: bigint | null;
    cursorLastSettledAt?: number;
  }[] = []
): MockDb {
  const startTimestamp = LP_V2_CUTOVER_TIMESTAMP;
  mockDb = seedTask5FungibleFixture(mockDb, {
    pool: V2_POOL,
    positionManager: V2_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 3000,
    startTimestamp,
    positions,
  });
  mockDb = mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    lpRateBps: TASK5_RATE_BPS,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [LEGACY_V3_POOL, V2_POOL],
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    positionManager: LEGACY_V3_POSITION_MANAGER,
    token0: AUSD_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 10_000,
    lpRateBps: TASK5_RATE_BPS,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: TASK5_START,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: startTimestamp,
    lastUpdate: startTimestamp,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: startTimestamp,
  });
  return mockDb;
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

function seedTask6UnclosedLegacyBoundary(mockDb: MockDb): MockDb {
  const epochStart = LP_V2_CUTOVER_TIMESTAMP - 1_000;
  mockDb = seedLeaderboardConfig(TestHelpers, mockDb);
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: BigInt(LP_V2_CUTOVER_BLOCK - 1_000),
    startTime: epochStart,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [LEGACY_V3_POOL],
    lastUpdate: epochStart,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    positionManager: LEGACY_V3_POSITION_MANAGER,
    token0: AUSD_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 10_000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: epochStart,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: epochStart,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    currentTick: 0,
    sqrtPriceX96: Q96,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: epochStart,
  });
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
    lastInRangeTimestamp: epochStart,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: epochStart,
    settledLpPoints: 0n,
    createdAt: epochStart,
    lastUpdate: epochStart,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    positionIds: [TOKEN_ID.toString()],
    lastUpdate: epochStart,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    positionIds: [
      TOKEN_ID.toString(),
      ...Array.from({ length: 9_999 }, (_, index) => `legacy:${index}`),
    ],
    lastUpdate: epochStart,
  });
  return mockDb;
}

function seedTask6OneSidedBalancerResume(mockDb: MockDb, disabledAtTimestamp: number): MockDb {
  const epochStart = LP_V2_CUTOVER_TIMESTAMP - 1_000;
  const balancerPositionId = `v2:${BALANCER_POOL}:${ADDRESSES.user}`;
  mockDb = seedTask6UnclosedLegacyBoundary(mockDb);
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [LEGACY_V3_POOL, BALANCER_POOL],
    lastUpdate: epochStart,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: BALANCER_POOL,
    pool: BALANCER_POOL,
    positionManager: BALANCER_POOL,
    token0: USDC_ADDRESS,
    token1: DUST_ADDRESS,
    fee: 10_000,
    lpRateBps: 0n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    disabledAtEpoch: 1n,
    disabledAtTimestamp,
    lastUpdate: disabledAtTimestamp,
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
    lastUpdate: disabledAtTimestamp,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: BALANCER_POOL,
    pool: BALANCER_POOL,
    reserve0: 1_000_000n,
    reserve1: 2_000_000n,
    lpTotalSupply: 1_000n,
    lastUpdate: disabledAtTimestamp,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: balancerPositionId,
    tokenId: BigInt(ADDRESSES.user),
    user_id: ADDRESSES.user,
    pool: BALANCER_POOL,
    positionManager: BALANCER_POOL,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 100n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 100n * PRICE_E8,
    lastInRangeTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    settledLpPoints: 0n,
    createdAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    lastUpdate: disabledAtTimestamp,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: BALANCER_POOL,
    pool: BALANCER_POOL,
    positionIds: [
      balancerPositionId,
      ...Array.from({ length: 9_999 }, (_, index) => `balancer:${index}`),
    ],
    lastUpdate: disabledAtTimestamp,
  });
  return mockDb;
}

function scheduleTask6EpochEnd(mockDb: MockDb, scheduledEndTime: number): MockDb {
  const epoch = mockDb.entities.LeaderboardEpoch.get('1');
  assert.ok(epoch);
  return mockDb.entities.LeaderboardEpoch.set({
    ...epoch,
    scheduledEndTime,
  });
}

function scheduleTask6TwoTideTail(
  mockDb: MockDb,
  firstBoundaryTime: number,
  secondEndTime: number
): MockDb {
  mockDb = scheduleTask6EpochEnd(mockDb, firstBoundaryTime);
  return mockDb.entities.LeaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: 0n,
    startTime: 0,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: firstBoundaryTime,
    scheduledEndTime: secondEndTime,
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

test('registered IncreaseLiquidity touches its user and republishes pool stats', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb());
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: TASK4_POOL,
    pool: TASK4_POOL,
    positionIds: [
      TASK4_POSITION_ID,
      ...Array.from({ length: 10_000 }, (_, index) => `fake-${index}`),
    ],
    lastUpdate: 100,
  });
  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TASK4_TOKEN_ID,
    liquidity: 0n,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block: { number: 41_231_500, timestamp: 200 },
      logIndex: 1,
      srcAddress: TASK4_MANAGER,
      transaction: { hash: '0x' + 'd'.repeat(64) },
    },
  });
  const increaseHandler = await getRegisteredEventHandler(
    'NonfungiblePositionManager',
    'IncreaseLiquidity'
  );

  const preloadProbe = createInstrumentedHandlerContext(mockDb, true);
  await increaseHandler({ event: increase, context: preloadProbe.context });
  const orderedProbe = createInstrumentedHandlerContext(mockDb, false);
  await increaseHandler({ event: increase, context: orderedProbe.context });

  assert.ok((orderedProbe.getCounts.get('UserLPPositionIndex') ?? 0) > 0);
  assert.ok((orderedProbe.getCounts.get('LPPoolPositionIndex') ?? 0) > 0);
});

test('inactive known V3 pool creates NFTs through both correlation orderings', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb(), {
    includePosition: false,
  });
  const eventData = createEventDataFactory();
  const growthSwap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: TASK4_USER,
    recipient: TASK4_USER,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: Q96,
    liquidity: 0n,
    tick: 0n,
    ...eventData(41_231_502, 150, TASK4_POOL),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
    event: growthSwap,
    mockDb,
  });
  const activeConfig = mockDb.entities.LPPoolConfig.get(TASK4_POOL);
  assert.ok(activeConfig);
  mockDb = mockDb.entities.LPPoolConfig.set({
    ...activeConfig,
    isActive: false,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 150,
    lastUpdate: 150,
  });

  const increaseFirstTokenId = 4201n;
  const increaseFirstId = increaseFirstTokenId.toString();
  const txHash = '0x' + '4'.repeat(64);
  const block = { number: 41_231_503, timestamp: 160 };
  const poolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
    sender: TASK4_USER,
    owner: TASK4_MANAGER,
    tickLower: BigInt(TICK_LOWER),
    tickUpper: BigInt(TICK_UPPER),
    amount: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block,
      logIndex: 1,
      srcAddress: TASK4_POOL,
      transaction: { hash: txHash },
    },
  });
  mockDb = await TestHelpers.UniswapV3Pool.Mint.processEvent({ event: poolMint, mockDb });
  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: increaseFirstTokenId,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block,
      logIndex: 2,
      srcAddress: TASK4_MANAGER,
      transaction: { hash: txHash, from: TASK4_USER },
    },
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: increase,
    mockDb,
  });
  const authoritativeTransfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: TASK4_USER_2,
    tokenId: increaseFirstTokenId,
    mockEventData: {
      block,
      logIndex: 3,
      srcAddress: TASK4_MANAGER,
      transaction: { hash: txHash },
    },
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: authoritativeTransfer,
    mockDb,
  });

  const transferFirstTokenId = 4202n;
  const transferFirstId = transferFirstTokenId.toString();
  mockDb = mockDb.entities.LPMintData.set({
    id: `pending:${transferFirstId}`,
    pool: TASK4_POOL,
    positionManager: TASK4_MANAGER,
    owner: TASK4_USER,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    txHash: '0xpending-inactive',
    timestamp: 170,
  });
  const transferFirst = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: TASK4_USER,
    tokenId: transferFirstTokenId,
    ...eventData(41_231_504, 170, TASK4_MANAGER),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: transferFirst,
    mockDb,
  });

  const increaseFirstPosition = mockDb.entities.UserLPPosition.get(increaseFirstId);
  const transferFirstPosition = mockDb.entities.UserLPPosition.get(transferFirstId);
  assert.deepEqual(
    {
      increaseFirstOwner: increaseFirstPosition?.user_id,
      increaseFirstSettledAt: increaseFirstPosition?.lastSettledAt,
      transferFirstOwner: transferFirstPosition?.user_id,
      transferFirstSettledAt: transferFirstPosition?.lastSettledAt,
    },
    {
      increaseFirstOwner: TASK4_USER_2,
      increaseFirstSettledAt: 160,
      transferFirstOwner: TASK4_USER,
      transferFirstSettledAt: 170,
    }
  );
  assert.deepEqual(
    new Set(mockDb.entities.LPPoolPositionIndex.get(TASK4_POOL)?.positionIds),
    new Set([increaseFirstId, transferFirstId])
  );
  assert.ok(
    mockDb.entities.UserLPPositionIndex.get(TASK4_USER_2)?.positionIds.includes(increaseFirstId)
  );
  assert.ok(
    mockDb.entities.UserLPPositionIndex.get(TASK4_USER)?.positionIds.includes(transferFirstId)
  );
  assert.equal(mockDb.entities.LPPoolConfig.get(TASK4_POOL)?.isActive, false);
});

test('direct core V3 mints do not leave position-manager correlation rows', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb(), {
    includePosition: false,
  });
  const poolConfig = mockDb.entities.LPPoolConfig.get(TASK4_POOL);
  assert.ok(poolConfig);
  mockDb = mockDb.entities.LPPoolConfig.set({
    ...poolConfig,
    isActive: false,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 150,
    lastUpdate: 150,
  });
  const poolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
    sender: TASK4_USER,
    owner: TASK4_USER,
    tickLower: BigInt(TICK_LOWER),
    tickUpper: BigInt(TICK_UPPER),
    amount: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    ...createEventDataFactory()(41_231_505, 170, TASK4_POOL),
  });

  mockDb = await TestHelpers.UniswapV3Pool.Mint.processEvent({ event: poolMint, mockDb });

  assert.deepEqual(mockDb.entities.LPMintData.getAll(), []);
});

test('canonical V3 mint batch attributes one position to the Transfer recipient', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb(), {
    includePosition: false,
  });
  const tokenId = 4301n;
  const positionId = tokenId.toString();
  const txHash = '0x' + '6'.repeat(64);
  const block = { number: 41_231_506, timestamp: 180 };
  const poolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
    sender: TASK4_USER,
    owner: TASK4_MANAGER,
    tickLower: BigInt(TICK_LOWER),
    tickUpper: BigInt(TICK_UPPER),
    amount: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block,
      logIndex: 1,
      srcAddress: TASK4_POOL,
      transaction: { hash: txHash, from: TASK4_USER },
    },
  });
  const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: TASK4_USER_2,
    tokenId,
    mockEventData: {
      block,
      logIndex: 2,
      srcAddress: TASK4_MANAGER,
      transaction: { hash: txHash, from: TASK4_USER },
    },
  });
  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block,
      logIndex: 3,
      srcAddress: TASK4_MANAGER,
      transaction: { hash: txHash, from: TASK4_USER },
    },
  });

  const batch = await processEvents({ events: [poolMint, transfer, increase], mockDb });
  mockDb = batch.mockDb;
  const position = mockDb.entities.UserLPPosition.get(positionId);
  assert.deepEqual(
    {
      eventsProcessed: batch.changes.reduce(
        (total, change) => total + Number(change.eventsProcessed ?? 0),
        0
      ),
      matchingPositions: mockDb.entities.UserLPPosition.getAll().filter(
        (candidate: EntityRow) => candidate.id === positionId
      ).length,
      owner: position?.user_id,
      liquidity: position?.liquidity,
      settledAt: position?.lastSettledAt,
      poolIndex: mockDb.entities.LPPoolPositionIndex.get(TASK4_POOL)?.positionIds,
      recipientIndex: mockDb.entities.UserLPPositionIndex.get(TASK4_USER_2)?.positionIds,
      payerIndex: mockDb.entities.UserLPPositionIndex.get(TASK4_USER)?.positionIds ?? [],
    },
    {
      eventsProcessed: 3,
      matchingPositions: 1,
      owner: TASK4_USER_2,
      liquidity: TASK4_LIQUIDITY,
      settledAt: 180,
      poolIndex: [positionId],
      recipientIndex: [positionId],
      payerIndex: [],
    }
  );
  assert.deepEqual(mockDb.entities.LPMintData.getAll(), []);
  assert.deepEqual(mockDb.entities.LPPendingMintOwner.getAll(), []);
});

test('IncreaseLiquidity rejects a mismatched pending mint owner correlation', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb(), {
    includePosition: false,
  });
  const tokenId = 4302n;
  const txHash = '0x' + '7'.repeat(64);
  const block = { number: 41_231_507, timestamp: 181 };
  const poolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
    sender: TASK4_USER,
    owner: TASK4_MANAGER,
    tickLower: BigInt(TICK_LOWER),
    tickUpper: BigInt(TICK_UPPER),
    amount: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block,
      logIndex: 1,
      srcAddress: TASK4_POOL,
      transaction: { hash: txHash, from: TASK4_USER },
    },
  });
  mockDb = await TestHelpers.UniswapV3Pool.Mint.processEvent({ event: poolMint, mockDb });
  mockDb = mockDb.entities.LPPendingMintOwner.set({
    id: `${TASK4_MANAGER}:${tokenId.toString()}`,
    tokenId,
    positionManager: TASK4_MANAGER,
    owner: TASK4_USER_2,
    txHash: '0x' + '8'.repeat(64),
    timestamp: 181,
  });
  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block,
      logIndex: 2,
      srcAddress: TASK4_MANAGER,
      transaction: { hash: txHash, from: TASK4_USER },
    },
  });

  await assert.rejects(
    () =>
      TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
        event: increase,
        mockDb,
      }),
    /pending LP mint owner mismatch/
  );
});

test('untracked V3 mint batch consumes its pending owner without creating a position', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb(), {
    includePosition: false,
  });
  const tokenId = 4304n;
  const txHash = '0x' + 'a'.repeat(64);
  const block = { number: 41_231_509, timestamp: 195 };
  const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: TASK4_USER_2,
    tokenId,
    mockEventData: {
      block,
      logIndex: 1,
      srcAddress: TASK4_MANAGER,
      transaction: { hash: txHash, from: TASK4_USER },
    },
  });
  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block,
      logIndex: 2,
      srcAddress: TASK4_MANAGER,
      transaction: { hash: txHash, from: TASK4_USER },
    },
  });

  const batch = await processEvents({ events: [transfer, increase], mockDb });
  mockDb = batch.mockDb;

  assert.equal(mockDb.entities.UserLPPosition.get(tokenId.toString()), undefined);
  assert.deepEqual(mockDb.entities.LPPendingMintOwner.getAll(), []);
  assert.deepEqual(mockDb.entities.LPMintData.getAll(), []);
});

test('same-tx identical mint vectors consume correlation before the next NFT', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb());
  const newTokenId = 4303n;
  const newPositionId = newTokenId.toString();
  const newTickLower = 240;
  const newTickUpper = 360;
  const liquidityDelta = TASK4_LIQUIDITY / 2n;
  const txHash = '0x' + '9'.repeat(64);
  const block = { number: 41_231_508, timestamp: 190 };
  const eventData = (logIndex: number) => ({
    mockEventData: {
      block,
      logIndex,
      transaction: { hash: txHash, from: TASK4_USER },
    },
  });
  const existingPoolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
    sender: TASK4_USER,
    owner: TASK4_MANAGER,
    tickLower: BigInt(TICK_LOWER),
    tickUpper: BigInt(TICK_UPPER),
    amount: liquidityDelta,
    amount0: 0n,
    amount1: 0n,
    ...eventData(1),
    mockEventData: { ...eventData(1).mockEventData, srcAddress: TASK4_POOL },
  });
  const existingIncrease = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent(
    {
      tokenId: TASK4_TOKEN_ID,
      liquidity: liquidityDelta,
      amount0: 0n,
      amount1: 0n,
      ...eventData(2),
      mockEventData: { ...eventData(2).mockEventData, srcAddress: TASK4_MANAGER },
    }
  );
  const newPoolMint = TestHelpers.UniswapV3Pool.Mint.createMockEvent({
    sender: TASK4_USER,
    owner: TASK4_MANAGER,
    tickLower: BigInt(newTickLower),
    tickUpper: BigInt(newTickUpper),
    amount: liquidityDelta,
    amount0: 0n,
    amount1: 0n,
    ...eventData(3),
    mockEventData: { ...eventData(3).mockEventData, srcAddress: TASK4_POOL },
  });
  const newTransfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: TASK4_USER_2,
    tokenId: newTokenId,
    ...eventData(4),
    mockEventData: { ...eventData(4).mockEventData, srcAddress: TASK4_MANAGER },
  });
  const newIncrease = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: newTokenId,
    liquidity: liquidityDelta,
    amount0: 0n,
    amount1: 0n,
    ...eventData(5),
    mockEventData: { ...eventData(5).mockEventData, srcAddress: TASK4_MANAGER },
  });

  const batch = await processEvents({
    events: [existingPoolMint, existingIncrease, newPoolMint, newTransfer, newIncrease],
    mockDb,
  });
  mockDb = batch.mockDb;
  const existingPosition = mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID);
  const newPosition = mockDb.entities.UserLPPosition.get(newPositionId);
  assert.deepEqual(
    {
      eventsProcessed: batch.changes.reduce(
        (total, change) => total + Number(change.eventsProcessed ?? 0),
        0
      ),
      existingLiquidity: existingPosition?.liquidity,
      newOwner: newPosition?.user_id,
      newLiquidity: newPosition?.liquidity,
      newTickLower: newPosition?.tickLower,
      newTickUpper: newPosition?.tickUpper,
      poolIndex: mockDb.entities.LPPoolPositionIndex.get(TASK4_POOL)?.positionIds,
      payerIndex: mockDb.entities.UserLPPositionIndex.get(TASK4_USER)?.positionIds,
      recipientIndex: mockDb.entities.UserLPPositionIndex.get(TASK4_USER_2)?.positionIds,
    },
    {
      eventsProcessed: 5,
      existingLiquidity: TASK4_LIQUIDITY + liquidityDelta,
      newOwner: TASK4_USER_2,
      newLiquidity: liquidityDelta,
      newTickLower,
      newTickUpper,
      poolIndex: [TASK4_POSITION_ID, newPositionId],
      payerIndex: [TASK4_POSITION_ID],
      recipientIndex: [newPositionId],
    }
  );
  assert.deepEqual(mockDb.entities.LPMintData.getAll(), []);
  assert.deepEqual(mockDb.entities.LPPendingMintOwner.getAll(), []);
});

test('Increase and Decrease settle old liquidity before resetting the new baseline', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb());
  const eventData = createEventDataFactory();
  const firstSwap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: TASK4_USER,
    recipient: TASK4_USER,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: Q96,
    liquidity: 0n,
    tick: 0n,
    ...eventData(41_231_510, 200, TASK4_POOL),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({ event: firstSwap, mockDb });
  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TASK4_TOKEN_ID,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    ...eventData(41_231_511, 200, TASK4_MANAGER),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: increase,
    mockDb,
  });
  const firstPoints = mockDb.entities.UserEpochStats.get(`${TASK4_USER}:1`)?.lpPoints ?? 0n;
  const firstExpected = referenceV3Points({
    sqrtPriceX96: Q96,
    tick: 0,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: TASK4_LIQUIDITY,
    stableTokenIndex: 0,
    stableTokenDecimals: 6,
    seconds: 100,
    rateBps: TASK4_RATE_BPS,
  });
  assert.ok(firstPoints > 0n);
  assert.equal(
    mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID)?.liquidity,
    2n * TASK4_LIQUIDITY
  );

  const secondSwap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: TASK4_USER,
    recipient: TASK4_USER,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: Q96,
    liquidity: 0n,
    tick: 0n,
    ...eventData(41_231_512, 300, TASK4_POOL),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({ event: secondSwap, mockDb });
  const decrease = TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.createMockEvent({
    tokenId: TASK4_TOKEN_ID,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    ...eventData(41_231_513, 300, TASK4_MANAGER),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.processEvent({
    event: decrease,
    mockDb,
  });
  const totalPoints = mockDb.entities.UserEpochStats.get(`${TASK4_USER}:1`)?.lpPoints ?? 0n;
  const totalExpected = 3n * firstExpected;
  const bound =
    3n *
    v3ReferenceRoundingBound({
      sqrtPriceX96: Q96,
      tick: 0,
      tickLower: TICK_LOWER,
      tickUpper: TICK_UPPER,
      liquidity: TASK4_LIQUIDITY,
      stableTokenIndex: 0,
      stableTokenDecimals: 6,
      seconds: 100,
      rateBps: TASK4_RATE_BPS,
    });
  assert.ok(
    absoluteDifference(totalPoints, totalExpected) <= bound,
    `actual=${totalPoints.toString()} expected=${totalExpected.toString()} bound=${bound.toString()}`
  );
  assert.equal(mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID)?.liquidity, TASK4_LIQUIDITY);
  assert.equal(mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID)?.lastSettledAt, 300);

  const underflow = TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.createMockEvent({
    tokenId: TASK4_TOKEN_ID,
    liquidity: TASK4_LIQUIDITY + 1n,
    amount0: 0n,
    amount1: 0n,
    ...eventData(41_231_514, 300, TASK4_MANAGER),
  });
  await assert.rejects(
    () =>
      TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.processEvent({
        event: underflow,
        mockDb,
      }),
    /LP position liquidity underflow/
  );
});

test('V3 owner transfer attributes old accrual then reassigns the position', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb());
  const eventData = createEventDataFactory();
  const firstSwap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: TASK4_USER,
    recipient: TASK4_USER,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: Q96,
    liquidity: 0n,
    tick: 0n,
    ...eventData(41_231_520, 200, TASK4_POOL),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({ event: firstSwap, mockDb });
  const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: TASK4_USER,
    to: TASK4_USER_2,
    tokenId: TASK4_TOKEN_ID,
    ...eventData(41_231_521, 200, TASK4_MANAGER),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: transfer,
    mockDb,
  });
  const oldOwnerPoints = mockDb.entities.UserEpochStats.get(`${TASK4_USER}:1`)?.lpPoints ?? 0n;
  assert.ok(oldOwnerPoints > 0n);
  assert.equal(mockDb.entities.UserEpochStats.get(`${TASK4_USER_2}:1`), undefined);
  assert.equal(mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID)?.user_id, TASK4_USER_2);

  const secondSwap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: TASK4_USER_2,
    recipient: TASK4_USER_2,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: Q96,
    liquidity: 0n,
    tick: 0n,
    ...eventData(41_231_522, 300, TASK4_POOL),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({ event: secondSwap, mockDb });
  const settleNewOwner = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TASK4_TOKEN_ID,
    liquidity: 0n,
    amount0: 0n,
    amount1: 0n,
    ...eventData(41_231_523, 300, TASK4_MANAGER),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: settleNewOwner,
    mockDb,
  });
  assert.equal(mockDb.entities.UserEpochStats.get(`${TASK4_USER}:1`)?.lpPoints, oldOwnerPoints);
  assert.ok((mockDb.entities.UserEpochStats.get(`${TASK4_USER_2}:1`)?.lpPoints ?? 0n) > 0n);
});

test('inactive V3 owner transfers settle and refresh current snapshots', async () => {
  const TestHelpers = loadTestHelpers();
  for (const scenario of [
    { name: 'ordinary transfer', from: TASK4_USER },
    { name: 'mint-owner correction', from: ZERO_ADDRESS },
  ]) {
    let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb());
    const eventData = createEventDataFactory();
    const stalePosition = mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID);
    assert.ok(stalePosition);
    const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
      sender: TASK4_USER,
      recipient: TASK4_USER,
      amount0: 0n,
      amount1: 0n,
      sqrtPriceX96: getSqrtRatioAtTick(200),
      liquidity: 0n,
      tick: 200n,
      ...eventData(41_231_524, 200, TASK4_POOL),
    });
    mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({ event: swap, mockDb });
    const activeConfig = mockDb.entities.LPPoolConfig.get(TASK4_POOL);
    assert.ok(activeConfig);
    mockDb = mockDb.entities.LPPoolConfig.set({
      ...activeConfig,
      isActive: false,
      disabledAtEpoch: 1n,
      disabledAtTimestamp: 200,
      lastUpdate: 200,
    });

    const transfer = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
      from: scenario.from,
      to: TASK4_USER_2,
      tokenId: TASK4_TOKEN_ID,
      ...eventData(41_231_525, 200, TASK4_MANAGER),
    });
    mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
      event: transfer,
      mockDb,
    });

    const position = mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID);
    const newOwnerStats = mockDb.entities.UserLPStats.get(TASK4_USER_2);
    assert.equal(position?.user_id, TASK4_USER_2, scenario.name);
    assert.equal(position?.amount0, 0n, scenario.name);
    assert.notEqual(position?.amount1, stalePosition.amount1, scenario.name);
    assert.equal(position?.isInRange, false, scenario.name);
    assert.equal(position?.lastInRangeTimestamp, 0, scenario.name);
    assert.ok((position?.valueUsd ?? 0n) > 0n, scenario.name);
    assert.equal(newOwnerStats?.totalPositions, 1, scenario.name);
    assert.equal(newOwnerStats?.inRangePositions, 0, scenario.name);
    assert.equal(newOwnerStats?.totalValueUsd, position?.valueUsd, scenario.name);
    assert.equal(newOwnerStats?.inRangeValueUsd, 0n, scenario.name);
    assert.ok(
      (mockDb.entities.UserEpochStats.get(`${TASK4_USER}:1`)?.lpPoints ?? 0n) > 0n,
      scenario.name
    );
    assert.equal(position?.user_id, TASK4_USER_2, scenario.name);
    assert.equal(mockDb.entities.LPPoolConfig.get(TASK4_POOL)?.isActive, false, scenario.name);
  }
});

test('inactive V3 liquidity mutations settle and recalculate an existing position', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb());
  const eventData = createEventDataFactory();
  const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: TASK4_USER,
    recipient: TASK4_USER,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: getSqrtRatioAtTick(200),
    liquidity: 0n,
    tick: 200n,
    ...eventData(41_231_526, 200, TASK4_POOL),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({ event: swap, mockDb });
  const activeConfig = mockDb.entities.LPPoolConfig.get(TASK4_POOL);
  assert.ok(activeConfig);
  mockDb = mockDb.entities.LPPoolConfig.set({
    ...activeConfig,
    isActive: false,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 200,
    lastUpdate: 200,
  });

  const increase = TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
    tokenId: TASK4_TOKEN_ID,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    ...eventData(41_231_527, 200, TASK4_MANAGER),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent({
    event: increase,
    mockDb,
  });
  const positionAfterIncrease = mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID);
  const pointsAfterIncrease = mockDb.entities.UserEpochStats.get(`${TASK4_USER}:1`)?.lpPoints ?? 0n;

  const decrease = TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.createMockEvent({
    tokenId: TASK4_TOKEN_ID,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    ...eventData(41_231_528, 200, TASK4_MANAGER),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.processEvent({
    event: decrease,
    mockDb,
  });
  const positionAfterDecrease = mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID);

  assert.equal(positionAfterIncrease?.liquidity, 2n * TASK4_LIQUIDITY);
  assert.equal(positionAfterIncrease?.amount0, 0n);
  assert.equal(positionAfterIncrease?.isInRange, false);
  assert.equal(positionAfterIncrease?.lastInRangeTimestamp, 0);
  assert.ok((positionAfterIncrease?.valueUsd ?? 0n) > 0n);
  assert.ok(pointsAfterIncrease > 0n);
  assert.equal(positionAfterDecrease?.liquidity, TASK4_LIQUIDITY);
  assert.equal(positionAfterDecrease?.amount0, 0n);
  assert.equal(positionAfterDecrease?.isInRange, false);
  assert.equal(positionAfterDecrease?.lastInRangeTimestamp, 0);
  assert.ok((positionAfterDecrease?.valueUsd ?? 0n) > 0n);
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${TASK4_USER}:1`)?.lpPoints,
    pointsAfterIncrease
  );
  assert.equal(mockDb.entities.LPPoolConfig.get(TASK4_POOL)?.isActive, false);
});

test('full V3 decrease clears the position snapshot and user aggregate', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb());
  const decrease = TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.createMockEvent({
    tokenId: TASK4_TOKEN_ID,
    liquidity: TASK4_LIQUIDITY,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block: { number: 41_231_529, timestamp: 200 },
      logIndex: 1,
      srcAddress: TASK4_MANAGER,
      transaction: { hash: '0x' + '3'.repeat(64) },
    },
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.processEvent({
    event: decrease,
    mockDb,
  });

  const position = mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID);
  const stats = mockDb.entities.UserLPStats.get(TASK4_USER);
  assert.deepEqual(
    {
      liquidity: position?.liquidity,
      amount0: position?.amount0,
      amount1: position?.amount1,
      isInRange: position?.isInRange,
      valueUsd: position?.valueUsd,
      lastInRangeTimestamp: position?.lastInRangeTimestamp,
      totalPositions: stats?.totalPositions,
      inRangePositions: stats?.inRangePositions,
      totalValueUsd: stats?.totalValueUsd,
      inRangeValueUsd: stats?.inRangeValueUsd,
    },
    {
      liquidity: 0n,
      amount0: 0n,
      amount1: 0n,
      isInRange: false,
      valueUsd: 0n,
      lastInRangeTimestamp: 0,
      totalPositions: 0,
      inRangePositions: 0,
      totalValueUsd: 0n,
      inRangeValueUsd: 0n,
    }
  );
});

test('legacy V3 decrease bootstraps its known config before the hard stop', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = mockDb.entities.LPPoolState.set({
    id: LEGACY_V3_POOL,
    pool: LEGACY_V3_POOL,
    currentTick: 0,
    sqrtPriceX96: Q96,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP - 2,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: TASK4_POSITION_ID,
    tokenId: TASK4_TOKEN_ID,
    user_id: TASK4_USER,
    pool: LEGACY_V3_POOL,
    positionManager: LEGACY_V3_POSITION_MANAGER,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    liquidity: TASK4_LIQUIDITY,
    amount0: TASK4_LIQUIDITY,
    amount1: TASK4_LIQUIDITY,
    isInRange: true,
    valueUsd: PRICE_E8,
    lastInRangeTimestamp: LP_V2_CUTOVER_TIMESTAMP - 2,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: LP_V2_CUTOVER_TIMESTAMP - 2,
    settledLpPoints: 0n,
    createdAt: LP_V2_CUTOVER_TIMESTAMP - 2,
    lastUpdate: LP_V2_CUTOVER_TIMESTAMP - 2,
  });
  const decrease = TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.createMockEvent({
    tokenId: TASK4_TOKEN_ID,
    liquidity: TASK4_LIQUIDITY / 2n,
    amount0: 0n,
    amount1: 0n,
    mockEventData: {
      block: { number: LP_V2_CUTOVER_BLOCK - 1, timestamp: LP_V2_CUTOVER_TIMESTAMP - 1 },
      logIndex: 1,
      srcAddress: LEGACY_V3_POSITION_MANAGER,
      transaction: { hash: '0x' + '5'.repeat(64) },
    },
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.processEvent({
    event: decrease,
    mockDb,
  });

  assert.ok(mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL));
  assert.equal(
    mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID)?.liquidity,
    TASK4_LIQUIDITY / 2n
  );
});

test('V3 burn settles accrual before zeroing liquidity', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask4V3Fixture(TestHelpers.MockDb.createMockDb());
  const eventData = createEventDataFactory();
  const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
    sender: TASK4_USER,
    recipient: TASK4_USER,
    amount0: 0n,
    amount1: 0n,
    sqrtPriceX96: Q96,
    liquidity: 0n,
    tick: 0n,
    ...eventData(41_231_530, 200, TASK4_POOL),
  });
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({ event: swap, mockDb });
  const burn = TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
    from: TASK4_USER,
    to: ZERO_ADDRESS,
    tokenId: TASK4_TOKEN_ID,
    ...eventData(41_231_531, 200, TASK4_MANAGER),
  });
  mockDb = await TestHelpers.NonfungiblePositionManager.Transfer.processEvent({
    event: burn,
    mockDb,
  });
  assert.ok((mockDb.entities.UserEpochStats.get(`${TASK4_USER}:1`)?.lpPoints ?? 0n) > 0n);
  assert.equal(mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID)?.liquidity, 0n);
  assert.equal(mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID)?.lastInRangeTimestamp, 0);
  const burned = mockDb.entities.UserLPPosition.get(TASK4_POSITION_ID);
  assert.equal(burned?.user_id, TASK4_USER);
  assert.equal(burned?.lastSettledAt, 200);
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
  const positionBeforeSwap = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());

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

  assert.deepEqual(mockDb.entities.UserLPPosition.get(TOKEN_ID.toString()), positionBeforeSwap);
  // The position itself is untouched, but the swap still republishes pool aggregates.
  const stats = mockDb.entities.LPPoolStats.get(ADDRESSES.pool);
  assert.equal(stats?.totalPositions, 1);
  assert.equal(stats?.inRangePositions, 0);
  assert.equal(mockDb.entities.LPPoolState.get(ADDRESSES.pool)?.currentTick, 5000);
});

test('uniswap v2 transfer creates a synthetic position while sync leaves it lazy', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();

  const v2Pool = '0x000000000000000000000000000000000000b200';
  const user = ADDRESSES.user;
  const positionId = `v2:${v2Pool}:${user}`;

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
  const positionBeforeSync = mockDb.entities.UserLPPosition.get(positionId);

  const sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    ...eventData(56436799, 2600, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: sync,
    mockDb,
  });

  const position = mockDb.entities.UserLPPosition.get(positionId);
  assert.deepEqual(position, positionBeforeSync);
  assert.ok(position);
  assert.equal(position?.liquidity, 1_000n);
  assert.equal(position?.isInRange, true);
  assert.equal(position?.tickLower, -887272);
  assert.equal(position?.tickUpper, 887272);
  assert.equal(position?.valueUsd, 0n);

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

test('registered legacy V3 Swap closes the first boundary before its exact-or-later hard stop', async () => {
  const TestHelpers = loadTestHelpers();
  const triggers = [
    {
      name: 'exact',
      block: LP_V2_CUTOVER_BLOCK,
      timestamp: LP_V2_CUTOVER_TIMESTAMP,
    },
    {
      name: 'later',
      block: LP_V2_CUTOVER_BLOCK + 1,
      timestamp: LP_V2_CUTOVER_TIMESTAMP + 60,
    },
  ] as const;

  for (const trigger of triggers) {
    const eventData = createEventDataFactory();
    let mockDb = seedTask6UnclosedLegacyBoundary(TestHelpers.MockDb.createMockDb());
    const poolStateBefore = mockDb.entities.LPPoolState.get(LEGACY_V3_POOL);
    const holderIndexBefore = mockDb.entities.LPPoolPositionIndex.get(LEGACY_V3_POOL);
    const swap = TestHelpers.UniswapV3Pool.Swap.createMockEvent({
      sender: ADDRESSES.user,
      recipient: ADDRESSES.user,
      amount0: -1_000_000n,
      amount1: 2_000_000n,
      sqrtPriceX96: 2n * Q96,
      liquidity: 123n,
      tick: 10n,
      ...eventData(trigger.block, trigger.timestamp, LEGACY_V3_POOL),
    });

    mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({ event: swap, mockDb });

    const legacyConfig = mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL);
    assert.equal(legacyConfig?.isActive, false, trigger.name);
    assert.equal(legacyConfig?.disabledAtTimestamp, LP_V2_CUTOVER_TIMESTAMP, trigger.name);
    assert.equal(
      mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(LEGACY_V3_POOL, 1n)),
      undefined,
      trigger.name
    );
    const v2Config = mockDb.entities.LPPoolConfig.get(V2_POOL);
    assert.equal(v2Config?.isActive, true, trigger.name);
    assert.equal(v2Config?.enabledAtTimestamp, LP_V2_CUTOVER_TIMESTAMP, trigger.name);
    assert.deepEqual(mockDb.entities.LPPoolState.get(LEGACY_V3_POOL), poolStateBefore);
    assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(LEGACY_V3_POOL), holderIndexBefore);
    assert.equal(mockDb.entities.UserEpochStats.getAll().length, 0, trigger.name);
    assert.equal(mockDb.entities.LPPoolFeeStats.get(LEGACY_V3_POOL), undefined, trigger.name);
    assert.equal(mockDb.entities.ProtocolStats.getAll().length, 0, trigger.name);
  }
});

test('registered legacy V3 Swap completes the post-cutover scheduled Tide tail before retiring', async () => {
  const TestHelpers = loadTestHelpers();
  const eventTimestamp = LP_V2_CUTOVER_TIMESTAMP + 60;
  const scheduledEndTime = LP_V2_CUTOVER_TIMESTAMP + 30;
  const eventBlock = LP_V2_CUTOVER_BLOCK + 1;
  let mockDb = scheduleTask6EpochEnd(
    seedTask6UnclosedLegacyBoundary(TestHelpers.MockDb.createMockDb()),
    scheduledEndTime
  );
  const bodyBefore = {
    poolState: mockDb.entities.LPPoolState.get(LEGACY_V3_POOL),
    position: mockDb.entities.UserLPPosition.get(TOKEN_ID.toString()),
    poolIndex: mockDb.entities.LPPoolPositionIndex.get(LEGACY_V3_POOL),
    userIndex: mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user),
  };
  const createSwap = (block: number, timestamp: number) =>
    TestHelpers.UniswapV3Pool.Swap.createMockEvent({
      sender: ADDRESSES.user,
      recipient: ADDRESSES.user,
      amount0: -1_000_000n,
      amount1: 2_000_000n,
      sqrtPriceX96: 2n * Q96,
      liquidity: 123n,
      tick: 10n,
      ...createEventDataFactory()(block, timestamp, LEGACY_V3_POOL),
    });

  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
    event: createSwap(eventBlock, eventTimestamp),
    mockDb,
  });

  assert.deepEqual(mockDb.entities.LeaderboardState.get('current'), {
    id: 'current',
    currentEpochNumber: 1n,
    isActive: false,
  });
  assert.deepEqual(mockDb.entities.LeaderboardEpoch.get('1'), {
    id: '1',
    epochNumber: 1n,
    startBlock: BigInt(LP_V2_CUTOVER_BLOCK - 1_000),
    startTime: LP_V2_CUTOVER_TIMESTAMP - 1_000,
    endBlock: BigInt(eventBlock),
    endTime: scheduledEndTime,
    isActive: false,
    duration: 1_030n,
    scheduledStartTime: 0,
    scheduledEndTime,
  });
  assert.equal(
    mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(LEGACY_V3_POOL, 1n)),
    undefined
  );
  const v2Growth = mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n));
  assert.ok(v2Growth);
  assert.equal(v2Growth.lastTimestamp, scheduledEndTime);
  assert.equal(v2Growth.isFrozen, true);
  assert.equal(v2Growth.frozenAt, scheduledEndTime);
  assert.deepEqual(mockDb.entities.LPPoolState.get(LEGACY_V3_POOL), bodyBefore.poolState);
  // The Tide-close sweep settles holders, so a position legitimately gains settlement
  // bookkeeping here. Identity and liquidity must still be untouched by the hard stop.
  const positionAfter = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
  assert.equal(positionAfter?.id, bodyBefore.position?.id);
  assert.equal(positionAfter?.user_id, bodyBefore.position?.user_id);
  assert.equal(positionAfter?.liquidity, bodyBefore.position?.liquidity);
  assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(LEGACY_V3_POOL), bodyBefore.poolIndex);
  assert.deepEqual(mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user), bodyBefore.userIndex);
  assert.equal(mockDb.entities.LPPoolFeeStats.getAll().length, 0);
  assert.equal(mockDb.entities.UserEpochStats.getAll().length, 0);
  assert.equal(mockDb.entities.ProtocolStats.getAll().length, 0);

  const chronologyAfterFirstEvent = {
    state: mockDb.entities.LeaderboardState.get('current'),
    epoch: mockDb.entities.LeaderboardEpoch.get('1'),
    legacyConfig: mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL),
    v2Config: mockDb.entities.LPPoolConfig.get(V2_POOL),
    legacyGrowth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(LEGACY_V3_POOL, 1n)),
    v2Growth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n)),
  };
  mockDb = await TestHelpers.UniswapV3Pool.Swap.processEvent({
    event: createSwap(eventBlock + 1, eventTimestamp + 60),
    mockDb,
  });
  assert.deepEqual(
    {
      state: mockDb.entities.LeaderboardState.get('current'),
      epoch: mockDb.entities.LeaderboardEpoch.get('1'),
      legacyConfig: mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL),
      v2Config: mockDb.entities.LPPoolConfig.get(V2_POOL),
      legacyGrowth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(LEGACY_V3_POOL, 1n)),
      v2Growth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n)),
    },
    chronologyAfterFirstEvent
  );
  assert.equal(mockDb.entities.ProtocolStats.getAll().length, 0);
});

test('every registered legacy V3 and position-manager family closes chronology before hard-stop', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  const eventBlock = LP_V2_CUTOVER_BLOCK + 1;
  const eventTimestamp = LP_V2_CUTOVER_TIMESTAMP + 60;
  const firstTailBoundary = LP_V2_CUTOVER_TIMESTAMP + 20;
  const secondTailEnd = LP_V2_CUTOVER_TIMESTAMP + 40;
  const poolEventData = () => eventData(eventBlock, eventTimestamp, LEGACY_V3_POOL);
  const managerEventData = () => eventData(eventBlock, eventTimestamp, LEGACY_V3_POSITION_MANAGER);
  const cases = [
    {
      name: 'UniswapV3Pool.Initialize',
      event: TestHelpers.UniswapV3Pool.Initialize.createMockEvent({
        sqrtPriceX96: 2n * Q96,
        tick: 10n,
        ...poolEventData(),
      }),
      process: TestHelpers.UniswapV3Pool.Initialize.processEvent,
    },
    {
      name: 'UniswapV3Pool.Swap',
      event: TestHelpers.UniswapV3Pool.Swap.createMockEvent({
        sender: ADDRESSES.user,
        recipient: ADDRESSES.user,
        amount0: -1_000_000n,
        amount1: 2_000_000n,
        sqrtPriceX96: 2n * Q96,
        liquidity: 123n,
        tick: 10n,
        ...poolEventData(),
      }),
      process: TestHelpers.UniswapV3Pool.Swap.processEvent,
    },
    {
      name: 'UniswapV3Pool.SetFeeProtocol',
      event: TestHelpers.UniswapV3Pool.SetFeeProtocol.createMockEvent({
        feeProtocol0Old: 0n,
        feeProtocol1Old: 0n,
        feeProtocol0New: 6n,
        feeProtocol1New: 7n,
        ...poolEventData(),
      }),
      process: TestHelpers.UniswapV3Pool.SetFeeProtocol.processEvent,
    },
    {
      name: 'UniswapV3Pool.Mint',
      event: TestHelpers.UniswapV3Pool.Mint.createMockEvent({
        sender: ADDRESSES.user,
        owner: LEGACY_V3_POSITION_MANAGER,
        tickLower: BigInt(TICK_LOWER),
        tickUpper: BigInt(TICK_UPPER),
        amount: 123n,
        amount0: AMOUNT0,
        amount1: AMOUNT1,
        ...poolEventData(),
      }),
      process: TestHelpers.UniswapV3Pool.Mint.processEvent,
    },
    {
      name: 'UniswapV3Pool.Burn',
      event: TestHelpers.UniswapV3Pool.Burn.createMockEvent({
        owner: ADDRESSES.user,
        tickLower: BigInt(TICK_LOWER),
        tickUpper: BigInt(TICK_UPPER),
        amount: 1n,
        amount0: 1n,
        amount1: 1n,
        ...poolEventData(),
      }),
      process: TestHelpers.UniswapV3Pool.Burn.processEvent,
    },
    {
      name: 'NonfungiblePositionManager.IncreaseLiquidity',
      event: TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.createMockEvent({
        tokenId: TOKEN_ID,
        liquidity: 10n,
        amount0: 1n,
        amount1: 1n,
        ...managerEventData(),
      }),
      process: TestHelpers.NonfungiblePositionManager.IncreaseLiquidity.processEvent,
    },
    {
      name: 'NonfungiblePositionManager.DecreaseLiquidity',
      event: TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.createMockEvent({
        tokenId: TOKEN_ID,
        liquidity: 10n,
        amount0: 1n,
        amount1: 1n,
        ...managerEventData(),
      }),
      process: TestHelpers.NonfungiblePositionManager.DecreaseLiquidity.processEvent,
    },
    {
      name: 'NonfungiblePositionManager.Transfer',
      event: TestHelpers.NonfungiblePositionManager.Transfer.createMockEvent({
        from: ADDRESSES.user,
        to: ADDRESSES.token0,
        tokenId: TOKEN_ID,
        ...managerEventData(),
      }),
      process: TestHelpers.NonfungiblePositionManager.Transfer.processEvent,
    },
  ] as const;

  for (const item of cases) {
    let mockDb = scheduleTask6TwoTideTail(
      seedTask6UnclosedLegacyBoundary(TestHelpers.MockDb.createMockDb()),
      firstTailBoundary,
      secondTailEnd
    );
    const poolStateBefore = mockDb.entities.LPPoolState.get(LEGACY_V3_POOL);
    const positionBefore = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
    const poolIndexBefore = mockDb.entities.LPPoolPositionIndex.get(LEGACY_V3_POOL);
    const userIndexBefore = mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user);

    mockDb = await item.process({ event: item.event, mockDb });

    const legacyConfig = mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL);
    const v2Config = mockDb.entities.LPPoolConfig.get(V2_POOL);
    assert.equal(legacyConfig?.isActive, false, item.name);
    assert.equal(legacyConfig?.disabledAtTimestamp, LP_V2_CUTOVER_TIMESTAMP, item.name);
    assert.equal(
      mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(LEGACY_V3_POOL, 1n)),
      undefined,
      item.name
    );
    assert.equal(v2Config?.isActive, true, item.name);
    assert.equal(v2Config?.enabledAtTimestamp, LP_V2_CUTOVER_TIMESTAMP, item.name);
    assert.deepEqual(
      mockDb.entities.LeaderboardState.get('current'),
      { id: 'current', currentEpochNumber: 2n, isActive: false },
      item.name
    );
    assert.equal(mockDb.entities.LeaderboardEpoch.get('1')?.endTime, firstTailBoundary, item.name);
    assert.equal(
      mockDb.entities.LeaderboardEpoch.get('1')?.endBlock,
      BigInt(eventBlock),
      item.name
    );
    assert.equal(mockDb.entities.LeaderboardEpoch.get('1')?.isActive, false, item.name);
    assert.equal(
      mockDb.entities.LeaderboardEpoch.get('2')?.startTime,
      firstTailBoundary,
      item.name
    );
    assert.equal(mockDb.entities.LeaderboardEpoch.get('2')?.endTime, secondTailEnd, item.name);
    assert.equal(
      mockDb.entities.LeaderboardEpoch.get('2')?.endBlock,
      BigInt(eventBlock),
      item.name
    );
    assert.equal(mockDb.entities.LeaderboardEpoch.get('2')?.isActive, false, item.name);
    for (const [epochNumber, frozenAt] of [
      [1n, firstTailBoundary],
      [2n, secondTailEnd],
    ] as const) {
      const growth = mockDb.entities.LPPoolEpochGrowth.get(
        lpPoolEpochGrowthId(V2_POOL, epochNumber)
      );
      assert.ok(growth, `${item.name} V2 epoch ${epochNumber.toString()}`);
      assert.equal(growth.isFrozen, true, item.name);
      assert.equal(growth.frozenAt, frozenAt, item.name);
      assert.equal(growth.lastTimestamp, frozenAt, item.name);
    }
    assert.deepEqual(mockDb.entities.LPPoolState.get(LEGACY_V3_POOL), poolStateBefore, item.name);
    // The Tide-close sweep settles holders, so a position legitimately gains settlement
    // bookkeeping here. Identity and liquidity must still be untouched by the hard stop.
    const positionAfter = mockDb.entities.UserLPPosition.get(TOKEN_ID.toString());
    assert.equal(positionAfter?.id, positionBefore?.id);
    assert.equal(positionAfter?.user_id, positionBefore?.user_id);
    assert.equal(positionAfter?.liquidity, positionBefore?.liquidity);
    assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(LEGACY_V3_POOL), poolIndexBefore);
    assert.deepEqual(mockDb.entities.UserLPPositionIndex.get(ADDRESSES.user), userIndexBefore);
    assert.equal(mockDb.entities.LPMintData.getAll().length, 0, item.name);
    assert.equal(mockDb.entities.LPPendingMintOwner.getAll().length, 0, item.name);
    assert.equal(mockDb.entities.LPPoolFeeStats.getAll().length, 0, item.name);
    assert.equal(mockDb.entities.UserEpochStats.getAll().length, 0, item.name);
    assert.equal(mockDb.entities.ProtocolStats.getAll().length, 0, item.name);
  }
});

test('registered Balancer Transfer repairs one-sided resume markers before hard-stop', async () => {
  const TestHelpers = loadTestHelpers();
  const markers = [
    {
      name: 'noncanonical inactive marker',
      disabledAt: LP_V2_RESUME_CUTOVER_TIMESTAMP - 500,
    },
    {
      name: 'lone exact resume marker',
      disabledAt: LP_V2_RESUME_CUTOVER_TIMESTAMP,
    },
  ] as const;

  for (const marker of markers) {
    const eventData = createEventDataFactory();
    let mockDb = seedTask6OneSidedBalancerResume(
      TestHelpers.MockDb.createMockDb(),
      marker.disabledAt
    );
    const balancerPositionId = `v2:${BALANCER_POOL}:${ADDRESSES.user}`;
    const poolStateBefore = mockDb.entities.LPPoolState.get(BALANCER_POOL);
    const v2StateBefore = mockDb.entities.LPPoolV2State.get(BALANCER_POOL);
    const positionBefore = mockDb.entities.UserLPPosition.get(balancerPositionId);
    const holderIndexBefore = mockDb.entities.LPPoolPositionIndex.get(BALANCER_POOL);
    const transfer = TestHelpers.BalancerAutoRangePool.Transfer.createMockEvent({
      from: ZERO_ADDRESS,
      to: ADDRESSES.user,
      value: 500n,
      ...eventData(
        LP_V2_RESUME_CUTOVER_BLOCK + 1,
        LP_V2_RESUME_CUTOVER_TIMESTAMP + 60,
        BALANCER_POOL
      ),
    });

    mockDb = await TestHelpers.BalancerAutoRangePool.Transfer.processEvent({
      event: transfer,
      mockDb,
    });

    const legacyConfig = mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL);
    const v2Config = mockDb.entities.LPPoolConfig.get(V2_POOL);
    const balancerConfig = mockDb.entities.LPPoolConfig.get(BALANCER_POOL);
    assert.equal(legacyConfig?.isActive, false, marker.name);
    assert.equal(legacyConfig?.disabledAtTimestamp, LP_V2_CUTOVER_TIMESTAMP, marker.name);
    assert.equal(
      mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(LEGACY_V3_POOL, 1n)),
      undefined,
      marker.name
    );
    assert.equal(v2Config?.isActive, true, marker.name);
    assert.equal(v2Config?.enabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP, marker.name);
    assert.equal(balancerConfig?.isActive, false, marker.name);
    assert.equal(balancerConfig?.disabledAtTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP, marker.name);
    assert.deepEqual(mockDb.entities.LPPoolState.get(BALANCER_POOL), poolStateBefore);
    assert.deepEqual(mockDb.entities.LPPoolV2State.get(BALANCER_POOL), v2StateBefore);
    assert.deepEqual(mockDb.entities.UserLPPosition.get(balancerPositionId), positionBefore);
    assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(BALANCER_POOL), holderIndexBefore);
    assert.equal(mockDb.entities.UserEpochStats.getAll().length, 0, marker.name);
    assert.equal(mockDb.entities.ProtocolStats.getAll().length, 0, marker.name);
  }
});

test('registered Balancer Transfer completes the post-resume scheduled Tide tail before retiring', async () => {
  const TestHelpers = loadTestHelpers();
  const eventTimestamp = LP_V2_RESUME_CUTOVER_TIMESTAMP + 60;
  const scheduledEndTime = LP_V2_RESUME_CUTOVER_TIMESTAMP + 30;
  const eventBlock = LP_V2_RESUME_CUTOVER_BLOCK + 1;
  let mockDb = scheduleTask6EpochEnd(
    seedTask6OneSidedBalancerResume(
      TestHelpers.MockDb.createMockDb(),
      LP_V2_RESUME_CUTOVER_TIMESTAMP
    ),
    scheduledEndTime
  );
  const balancerPositionId = `v2:${BALANCER_POOL}:${ADDRESSES.user}`;
  const bodyBefore = {
    poolState: mockDb.entities.LPPoolState.get(BALANCER_POOL),
    v2State: mockDb.entities.LPPoolV2State.get(BALANCER_POOL),
    position: mockDb.entities.UserLPPosition.get(balancerPositionId),
    poolIndex: mockDb.entities.LPPoolPositionIndex.get(BALANCER_POOL),
  };
  const createTransfer = (block: number, timestamp: number) =>
    TestHelpers.BalancerAutoRangePool.Transfer.createMockEvent({
      from: ZERO_ADDRESS,
      to: ADDRESSES.user,
      value: 500n,
      ...createEventDataFactory()(block, timestamp, BALANCER_POOL),
    });

  mockDb = await TestHelpers.BalancerAutoRangePool.Transfer.processEvent({
    event: createTransfer(eventBlock, eventTimestamp),
    mockDb,
  });

  assert.deepEqual(mockDb.entities.LeaderboardState.get('current'), {
    id: 'current',
    currentEpochNumber: 1n,
    isActive: false,
  });
  assert.equal(mockDb.entities.LeaderboardEpoch.get('1')?.endBlock, BigInt(eventBlock));
  assert.equal(mockDb.entities.LeaderboardEpoch.get('1')?.endTime, scheduledEndTime);
  assert.equal(mockDb.entities.LeaderboardEpoch.get('1')?.isActive, false);
  assert.equal(
    mockDb.entities.LeaderboardEpoch.get('1')?.duration,
    BigInt(scheduledEndTime - (LP_V2_CUTOVER_TIMESTAMP - 1_000))
  );
  const resumedV2Growth = mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n));
  assert.ok(resumedV2Growth);
  assert.equal(resumedV2Growth.lastTimestamp, scheduledEndTime);
  assert.equal(resumedV2Growth.isFrozen, true);
  assert.equal(resumedV2Growth.frozenAt, scheduledEndTime);
  assert.deepEqual(mockDb.entities.LPPoolState.get(BALANCER_POOL), bodyBefore.poolState);
  assert.deepEqual(mockDb.entities.LPPoolV2State.get(BALANCER_POOL), bodyBefore.v2State);
  assert.deepEqual(mockDb.entities.UserLPPosition.get(balancerPositionId), bodyBefore.position);
  assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(BALANCER_POOL), bodyBefore.poolIndex);
  assert.equal(mockDb.entities.LPPoolFeeStats.getAll().length, 0);
  assert.equal(mockDb.entities.UserEpochStats.getAll().length, 0);
  assert.equal(mockDb.entities.ProtocolStats.getAll().length, 0);

  const chronologyAfterFirstEvent = {
    state: mockDb.entities.LeaderboardState.get('current'),
    epoch: mockDb.entities.LeaderboardEpoch.get('1'),
    legacyConfig: mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL),
    v2Config: mockDb.entities.LPPoolConfig.get(V2_POOL),
    balancerConfig: mockDb.entities.LPPoolConfig.get(BALANCER_POOL),
    v2Growth: resumedV2Growth,
  };
  mockDb = await TestHelpers.BalancerAutoRangePool.Transfer.processEvent({
    event: createTransfer(eventBlock + 1, eventTimestamp + 60),
    mockDb,
  });
  assert.deepEqual(
    {
      state: mockDb.entities.LeaderboardState.get('current'),
      epoch: mockDb.entities.LeaderboardEpoch.get('1'),
      legacyConfig: mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL),
      v2Config: mockDb.entities.LPPoolConfig.get(V2_POOL),
      balancerConfig: mockDb.entities.LPPoolConfig.get(BALANCER_POOL),
      v2Growth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n)),
    },
    chronologyAfterFirstEvent
  );
  assert.deepEqual(mockDb.entities.LPPoolState.get(BALANCER_POOL), bodyBefore.poolState);
  assert.deepEqual(mockDb.entities.LPPoolV2State.get(BALANCER_POOL), bodyBefore.v2State);
  assert.deepEqual(mockDb.entities.UserLPPosition.get(balancerPositionId), bodyBefore.position);
  assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(BALANCER_POOL), bodyBefore.poolIndex);
  assert.equal(mockDb.entities.ProtocolStats.getAll().length, 0);
});

test('every registered Balancer family repairs chronology once and ignores the retired body', async () => {
  const TestHelpers = loadTestHelpers();
  const eventBlock = LP_V2_RESUME_CUTOVER_BLOCK + 1;
  const eventTimestamp = LP_V2_RESUME_CUTOVER_TIMESTAMP + 60;
  const firstTailBoundary = LP_V2_RESUME_CUTOVER_TIMESTAMP + 20;
  const secondTailEnd = LP_V2_RESUME_CUTOVER_TIMESTAMP + 40;
  const cases = [
    {
      name: 'BalancerAutoRangePool.Transfer',
      create: (block: number, timestamp: number) =>
        TestHelpers.BalancerAutoRangePool.Transfer.createMockEvent({
          from: ZERO_ADDRESS,
          to: ADDRESSES.user,
          value: 500n,
          ...createEventDataFactory()(block, timestamp, BALANCER_POOL),
        }),
      process: TestHelpers.BalancerAutoRangePool.Transfer.processEvent,
    },
    {
      name: 'BalancerVault.LiquidityAdded',
      create: (block: number, timestamp: number) =>
        TestHelpers.BalancerVault.LiquidityAdded.createMockEvent({
          pool: BALANCER_POOL,
          liquidityProvider: ADDRESSES.user,
          kind: 0n,
          totalSupply: 2_000n,
          amountsAddedRaw: [100n, 200n],
          swapFeeAmountsRaw: [1n, 2n],
          ...createEventDataFactory()(block, timestamp, BALANCER_VAULT_ADDRESS),
        }),
      process: TestHelpers.BalancerVault.LiquidityAdded.processEvent,
    },
    {
      name: 'BalancerVault.LiquidityRemoved',
      create: (block: number, timestamp: number) =>
        TestHelpers.BalancerVault.LiquidityRemoved.createMockEvent({
          pool: BALANCER_POOL,
          liquidityProvider: ADDRESSES.user,
          kind: 0n,
          totalSupply: 500n,
          amountsRemovedRaw: [100n, 200n],
          swapFeeAmountsRaw: [1n, 2n],
          ...createEventDataFactory()(block, timestamp, BALANCER_VAULT_ADDRESS),
        }),
      process: TestHelpers.BalancerVault.LiquidityRemoved.processEvent,
    },
    {
      name: 'BalancerVault.Swap',
      create: (block: number, timestamp: number) =>
        TestHelpers.BalancerVault.Swap.createMockEvent({
          pool: BALANCER_POOL,
          tokenIn: DUST_ADDRESS,
          tokenOut: USDC_ADDRESS,
          amountIn: 100n,
          amountOut: 50n,
          swapFeePercentage: 10n ** 16n,
          swapFeeAmount: 1n,
          ...createEventDataFactory()(block, timestamp, BALANCER_VAULT_ADDRESS),
        }),
      process: TestHelpers.BalancerVault.Swap.processEvent,
    },
  ] as const;

  for (const item of cases) {
    let mockDb = scheduleTask6TwoTideTail(
      seedTask6OneSidedBalancerResume(
        TestHelpers.MockDb.createMockDb(),
        LP_V2_RESUME_CUTOVER_TIMESTAMP - 500
      ),
      firstTailBoundary,
      secondTailEnd
    );
    const balancerPositionId = `v2:${BALANCER_POOL}:${ADDRESSES.user}`;
    const bodyBefore = {
      poolState: mockDb.entities.LPPoolState.get(BALANCER_POOL),
      v2State: mockDb.entities.LPPoolV2State.get(BALANCER_POOL),
      position: mockDb.entities.UserLPPosition.get(balancerPositionId),
      poolIndex: mockDb.entities.LPPoolPositionIndex.get(BALANCER_POOL),
    };
    const event = item.create(eventBlock, eventTimestamp);

    mockDb = await item.process({ event, mockDb });

    assert.equal(mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL)?.isActive, false, item.name);
    assert.equal(
      mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL)?.disabledAtTimestamp,
      LP_V2_CUTOVER_TIMESTAMP,
      item.name
    );
    assert.equal(
      mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(LEGACY_V3_POOL, 1n)),
      undefined,
      item.name
    );
    assert.equal(mockDb.entities.LPPoolConfig.get(V2_POOL)?.isActive, true, item.name);
    assert.equal(
      mockDb.entities.LPPoolConfig.get(V2_POOL)?.enabledAtTimestamp,
      LP_V2_RESUME_CUTOVER_TIMESTAMP,
      item.name
    );
    assert.equal(mockDb.entities.LPPoolConfig.get(BALANCER_POOL)?.isActive, false, item.name);
    assert.equal(
      mockDb.entities.LPPoolConfig.get(BALANCER_POOL)?.disabledAtTimestamp,
      LP_V2_RESUME_CUTOVER_TIMESTAMP,
      item.name
    );
    assert.deepEqual(
      mockDb.entities.LeaderboardState.get('current'),
      { id: 'current', currentEpochNumber: 2n, isActive: false },
      item.name
    );
    assert.equal(mockDb.entities.LeaderboardEpoch.get('1')?.endTime, firstTailBoundary, item.name);
    assert.equal(
      mockDb.entities.LeaderboardEpoch.get('1')?.endBlock,
      BigInt(eventBlock),
      item.name
    );
    assert.equal(
      mockDb.entities.LeaderboardEpoch.get('2')?.startTime,
      firstTailBoundary,
      item.name
    );
    assert.equal(mockDb.entities.LeaderboardEpoch.get('2')?.endTime, secondTailEnd, item.name);
    assert.equal(
      mockDb.entities.LeaderboardEpoch.get('2')?.endBlock,
      BigInt(eventBlock),
      item.name
    );
    for (const [epochNumber, frozenAt] of [
      [1n, firstTailBoundary],
      [2n, secondTailEnd],
    ] as const) {
      const growth = mockDb.entities.LPPoolEpochGrowth.get(
        lpPoolEpochGrowthId(V2_POOL, epochNumber)
      );
      assert.ok(growth, `${item.name} V2 epoch ${epochNumber.toString()}`);
      assert.equal(growth.isFrozen, true, item.name);
      assert.equal(growth.frozenAt, frozenAt, item.name);
      assert.equal(growth.lastTimestamp, frozenAt, item.name);
    }
    assert.deepEqual(mockDb.entities.LPPoolState.get(BALANCER_POOL), bodyBefore.poolState);
    assert.deepEqual(mockDb.entities.LPPoolV2State.get(BALANCER_POOL), bodyBefore.v2State);
    assert.deepEqual(mockDb.entities.UserLPPosition.get(balancerPositionId), bodyBefore.position);
    assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(BALANCER_POOL), bodyBefore.poolIndex);
    assert.equal(mockDb.entities.UserEpochStats.getAll().length, 0, item.name);
    assert.equal(mockDb.entities.LPPoolFeeStats.getAll().length, 0, item.name);
    assert.equal(mockDb.entities.ProtocolStats.getAll().length, 0, item.name);

    const boundaryState = {
      legacyConfig: mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL),
      v2Config: mockDb.entities.LPPoolConfig.get(V2_POOL),
      balancerConfig: mockDb.entities.LPPoolConfig.get(BALANCER_POOL),
      legacyGrowth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(LEGACY_V3_POOL, 1n)),
      v2Growth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n)),
      v2NextGrowth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 2n)),
      balancerGrowth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(BALANCER_POOL, 1n)),
      state: mockDb.entities.LeaderboardState.get('current'),
      firstEpoch: mockDb.entities.LeaderboardEpoch.get('1'),
      secondEpoch: mockDb.entities.LeaderboardEpoch.get('2'),
    };
    const replay = item.create(eventBlock + 1, eventTimestamp + 60);
    mockDb = await item.process({ event: replay, mockDb });
    assert.deepEqual(
      {
        legacyConfig: mockDb.entities.LPPoolConfig.get(LEGACY_V3_POOL),
        v2Config: mockDb.entities.LPPoolConfig.get(V2_POOL),
        balancerConfig: mockDb.entities.LPPoolConfig.get(BALANCER_POOL),
        legacyGrowth: mockDb.entities.LPPoolEpochGrowth.get(
          lpPoolEpochGrowthId(LEGACY_V3_POOL, 1n)
        ),
        v2Growth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n)),
        v2NextGrowth: mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 2n)),
        balancerGrowth: mockDb.entities.LPPoolEpochGrowth.get(
          lpPoolEpochGrowthId(BALANCER_POOL, 1n)
        ),
        state: mockDb.entities.LeaderboardState.get('current'),
        firstEpoch: mockDb.entities.LeaderboardEpoch.get('1'),
        secondEpoch: mockDb.entities.LeaderboardEpoch.get('2'),
      },
      boundaryState,
      item.name
    );
    assert.deepEqual(mockDb.entities.LPPoolState.get(BALANCER_POOL), bodyBefore.poolState);
    assert.deepEqual(mockDb.entities.LPPoolV2State.get(BALANCER_POOL), bodyBefore.v2State);
    assert.deepEqual(mockDb.entities.UserLPPosition.get(balancerPositionId), bodyBefore.position);
    assert.deepEqual(mockDb.entities.LPPoolPositionIndex.get(BALANCER_POOL), bodyBefore.poolIndex);
    assert.equal(mockDb.entities.UserEpochStats.getAll().length, 0, item.name);
    assert.equal(mockDb.entities.LPPoolFeeStats.getAll().length, 0, item.name);
    assert.equal(mockDb.entities.ProtocolStats.getAll().length, 0, item.name);
  }
});

test('static-era hard-stop guards follow chronology in every affected registered family', () => {
  const source = readFileSync(path.join(process.cwd(), 'src', 'handlers', 'lp.ts'), 'utf8');
  const families = [
    ...['Initialize', 'Swap', 'SetFeeProtocol', 'Mint', 'Burn'].map(event => ({
      contract: 'UniswapV3Pool',
      event,
      guard: 'if (isLegacyV3PoolHardStopped',
    })),
    ...['IncreaseLiquidity', 'DecreaseLiquidity', 'Transfer'].map(event => ({
      contract: 'NonfungiblePositionManager',
      event,
      guard: 'if (isLegacyV3ManagerHardStopped',
    })),
    {
      contract: 'BalancerAutoRangePool',
      event: 'Transfer',
      guard: 'if (isBalancerPoolHardStopped',
    },
    ...['LiquidityAdded', 'LiquidityRemoved', 'Swap'].map(event => ({
      contract: 'BalancerVault',
      event,
      guard: 'if (isBalancerPoolHardStopped',
    })),
  ];

  // v2 registers as `<Contract>.<Event>.handler(async ({ event, context }) => {`, with the
  // arrow sometimes wrapped onto the next line. A body ends where the next top-level
  // registration begins.
  const nextRegistrationPattern = /\n[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.(?:handler|contractRegister)\(/g;
  for (const family of families) {
    const registrationPattern = new RegExp(
      `\\n${family.contract}\\.${family.event}\\.handler\\(\\s*async \\(\\{ event, context \\}\\) => \\{`
    );
    const registration = registrationPattern.exec(source);
    assert.ok(registration, `${family.contract}.${family.event} registration`);
    const bodyStart = registration.index + registration[0].length;
    nextRegistrationPattern.lastIndex = bodyStart;
    const nextRegistration = nextRegistrationPattern.exec(source);
    const body = source.slice(bodyStart, nextRegistration ? nextRegistration.index : source.length);
    const chronologyIndex = body.indexOf('await applyStaticLPPoolCutover');
    const hardStopIndex = body.indexOf(family.guard);
    const hardStopReturnIndex = body.indexOf('return;', hardStopIndex);
    const scheduledTailIndex = body.indexOf('await applyScheduledEpochTransitions', hardStopIndex);
    assert.ok(chronologyIndex >= 0, `${family.contract}.${family.event} chronology`);
    assert.ok(hardStopIndex >= 0, `${family.contract}.${family.event} hard stop`);
    assert.ok(
      chronologyIndex < hardStopIndex,
      `${family.contract}.${family.event} must apply chronology before hard stop`
    );
    assert.ok(
      scheduledTailIndex > hardStopIndex && scheduledTailIndex < hardStopReturnIndex,
      `${family.contract}.${family.event} must apply the event-time Tide tail before returning`
    );
  }

  assert.equal(source.includes('isBalancerResumeTransitionComplete'), false);
});

test('registered hard-stop chronology is preload-safe, ordered, and holder-cardinality invariant', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  const cases = [
    {
      name: 'legacy Swap',
      scheduledEndTime: LP_V2_CUTOVER_TIMESTAMP + 30,
      mockDb: scheduleTask6EpochEnd(
        seedTask6UnclosedLegacyBoundary(TestHelpers.MockDb.createMockDb()),
        LP_V2_CUTOVER_TIMESTAMP + 30
      ),
      handler: await getRegisteredEventHandler('UniswapV3Pool', 'Swap'),
      event: TestHelpers.UniswapV3Pool.Swap.createMockEvent({
        sender: ADDRESSES.user,
        recipient: ADDRESSES.user,
        amount0: -1_000_000n,
        amount1: 2_000_000n,
        sqrtPriceX96: 2n * Q96,
        liquidity: 123n,
        tick: 10n,
        ...eventData(LP_V2_CUTOVER_BLOCK + 1, LP_V2_CUTOVER_TIMESTAMP + 60, LEGACY_V3_POOL),
      }),
    },
    {
      name: 'Balancer Transfer',
      scheduledEndTime: LP_V2_RESUME_CUTOVER_TIMESTAMP + 30,
      mockDb: scheduleTask6EpochEnd(
        seedTask6OneSidedBalancerResume(
          TestHelpers.MockDb.createMockDb(),
          LP_V2_RESUME_CUTOVER_TIMESTAMP
        ),
        LP_V2_RESUME_CUTOVER_TIMESTAMP + 30
      ),
      handler: await getRegisteredEventHandler('BalancerAutoRangePool', 'Transfer'),
      event: TestHelpers.BalancerAutoRangePool.Transfer.createMockEvent({
        from: ZERO_ADDRESS,
        to: ADDRESSES.user,
        value: 500n,
        ...eventData(
          LP_V2_RESUME_CUTOVER_BLOCK + 1,
          LP_V2_RESUME_CUTOVER_TIMESTAMP + 60,
          BALANCER_POOL
        ),
      }),
    },
  ] as const;

  for (const item of cases) {
    const trace = await traceRegisteredEventBatch(item.mockDb, [
      { handler: item.handler, event: item.event },
    ]);
    assert.equal(
      trace.preloadProbe.stores.get('LPPoolConfig')?.get(LEGACY_V3_POOL)?.isActive,
      true,
      item.name
    );
    assert.equal(trace.preloadProbe.stores.get('LPPoolConfig')?.get(V2_POOL), undefined, item.name);
    assert.equal(
      trace.orderedProbe.stores.get('LPPoolConfig')?.get(LEGACY_V3_POOL)?.isActive,
      false,
      item.name
    );
    assert.equal(
      trace.orderedProbe.stores.get('LPPoolConfig')?.get(LEGACY_V3_POOL)?.disabledAtTimestamp,
      LP_V2_CUTOVER_TIMESTAMP,
      item.name
    );
    assert.equal(
      trace.orderedProbe.stores.get('LPPoolConfig')?.get(V2_POOL)?.isActive,
      true,
      item.name
    );
    assert.equal(
      trace.orderedProbe.stores.get('LeaderboardState')?.get('current')?.isActive,
      false,
      item.name
    );
    assert.equal(
      trace.orderedProbe.stores.get('LPPoolEpochGrowth')?.get(lpPoolEpochGrowthId(V2_POOL, 1n))
        ?.frozenAt,
      item.scheduledEndTime,
      item.name
    );
    for (const probe of [trace.preloadProbe, trace.orderedProbe]) {
      // The Tide-close holder sweep (FINDING 003) legitimately reads position/holder state to
      // credit holders no event ever touched. Fee and protocol aggregates must still see no
      // work at a hard stop, which is what this guard now pins.
      for (const entity of ['LPPoolFeeStats', 'ProtocolStats', 'ProtocolStatsSnapshot']) {
        assert.equal(probe.getCounts.get(entity) ?? 0, 0, `${item.name} ${entity} reads`);
        assert.equal(probe.setCounts.get(entity) ?? 0, 0, `${item.name} ${entity} writes`);
      }
    }
  }
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

test('uniswap v2 sync with no positions leaves pool stats absent', async () => {
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
  assert.equal(poolStats, undefined);
  assert.equal(mockDb.entities.LPPoolV2State.get(v2Pool)?.reserve0, 100n);
  assert.equal(mockDb.entities.LPPoolV2State.get(v2Pool)?.reserve1, 200n);
});

test('uniswap v2 sync advances old scalar growth without touching positions or users', async () => {
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
  const positionBeforeSync = mockDb.entities.UserLPPosition.get(positionId);

  const sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
    reserve0: 1_000_000n * 10n ** 6n,
    reserve1: 500_000n * 10n ** 18n,
    ...eventData(56436813, 4600, v2Pool),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: sync,
    mockDb,
  });

  assert.deepEqual(mockDb.entities.UserLPPosition.get(positionId), positionBeforeSync);
  assert.equal(mockDb.entities.UserEpochStats.get(`${user}:1`), undefined);
  const growth = mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(v2Pool, 1n));
  assert.ok(growth);
  assert.equal(growth?.lastTimestamp, 4600);
  assert.ok((growth?.scalarGrowthX128 ?? 0n) > 0n);

  const settle = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user,
    timestamp: 4600n,
    ...eventData(56_436_814, 4600, '0x000000000000000000000000000000000000beef'),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: settle, mockDb });
  const firstSettlement = mockDb.entities.UserEpochStats.get(`${user}:1`);
  assert.ok((firstSettlement?.lpPoints ?? 0n) > 0n);

  const repeat = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user,
    timestamp: 4600n,
    ...eventData(56_436_815, 4600, '0x000000000000000000000000000000000000beef'),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: repeat, mockDb });
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${user}:1`)?.lpPoints,
    firstSettlement?.lpPoints
  );
});

test('uniswap v2 burn transfer clamps balance and supply for a mid-life bootstrapped pool', async () => {
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
  // The pair is indexed from LP_V2_CUTOVER_BLOCK, so mints that created the outstanding
  // supply were never seen and a real burn can exceed tracked state. Production clamps
  // at zero; failing closed here would make mainnet history unindexable.
  const afterBurn = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: burnTransfer,
    mockDb,
  });
  assert.equal(afterBurn.entities.LPPoolV2State.get(v2Pool)?.lpTotalSupply, 0n);
  assert.equal(afterBurn.entities.UserLPPosition.get(positionId)?.liquidity, 0n);
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
    owner: LEGACY_V3_POSITION_MANAGER,
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
    owner: LEGACY_V3_POSITION_MANAGER,
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
  mockDb = mockDb.entities.LPPoolEpochGrowth.set({
    id: lpPoolEpochGrowthId(v2Pool, 1n),
    pool: v2Pool,
    epochNumber: 1n,
    startTimestamp: 1000,
    lastTimestamp: 1000,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: 1000,
  });
  mockDb = mockDb.entities.UserLPEpochCursor.set({
    id: `${positionId}:1`,
    position_id: positionId,
    user_id: user,
    pool: v2Pool,
    epochNumber: 1n,
    growthBaselineX128: 0n,
    lastSettledAt: 1000,
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

test('balancer autorange tracks before cutover and advances lazy growth after cutover', async () => {
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
  const positionBeforeSwap = position;

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
  assert.equal(epochStats, undefined);

  const updatedPosition = mockDb.entities.UserLPPosition.get(positionId);
  assert.deepEqual(updatedPosition, positionBeforeSwap);
  assert.ok(
    (mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(BALANCER_POOL, 1n))
      ?.scalarGrowthX128 ?? 0n) > 0n
  );

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
  mockDb = mockDb.entities.LPPoolEpochGrowth.set({
    id: lpPoolEpochGrowthId(V2_POOL, 1n),
    pool: V2_POOL,
    epochNumber: 1n,
    startTimestamp: LP_V2_CUTOVER_TIMESTAMP,
    lastTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP - 3600,
  });
  mockDb = mockDb.entities.UserLPEpochCursor.set({
    id: `${positionId}:1`,
    position_id: positionId,
    user_id: user,
    pool: V2_POOL,
    epochNumber: 1n,
    growthBaselineX128: 0n,
    lastSettledAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    lastUpdate: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
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
  mockDb = mockDb.entities.LPPoolEpochGrowth.set({
    id: lpPoolEpochGrowthId(BALANCER_POOL, 1n),
    pool: BALANCER_POOL,
    epochNumber: 1n,
    startTimestamp: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP,
    lastTimestamp: LP_V2_RESUME_CUTOVER_TIMESTAMP - 3600,
    scalarGrowthX128: 0n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: LP_V2_RESUME_CUTOVER_TIMESTAMP - 3600,
  });
  mockDb = mockDb.entities.UserLPEpochCursor.set({
    id: `${balancerPositionId}:1`,
    position_id: balancerPositionId,
    user_id: user,
    pool: BALANCER_POOL,
    epochNumber: 1n,
    growthBaselineX128: 0n,
    lastSettledAt: LP_V2_RESUME_CUTOVER_TIMESTAMP - 3600,
    lastUpdate: LP_V2_RESUME_CUTOVER_TIMESTAMP - 3600,
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

  // The resume transfer itself is hard-stopped on the Balancer pool. It closes
  // pool growth without touching holders; the stored interval remains
  // claimable through the explicit keeper user-settlement path.
  assert.equal(mockDb.entities.UserEpochStats.get(`${user}:1`), undefined);
  const untouchedBalancerPosition = mockDb.entities.UserLPPosition.get(balancerPositionId);
  assert.equal(untouchedBalancerPosition?.liquidity, 1_000n);
  assert.equal(untouchedBalancerPosition?.lastSettledAt, LP_V2_RESUME_CUTOVER_TIMESTAMP - 3600);
  const closedGrowth = mockDb.entities.LPPoolEpochGrowth.get(
    lpPoolEpochGrowthId(BALANCER_POOL, 1n)
  );
  assert.equal(closedGrowth?.lastTimestamp, LP_V2_RESUME_CUTOVER_TIMESTAMP);
  assert.ok((closedGrowth?.scalarGrowthX128 ?? 0n) > 0n);

  const explicitSettlement = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user,
    timestamp: BigInt(LP_V2_RESUME_CUTOVER_TIMESTAMP),
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 1, LP_V2_RESUME_CUTOVER_TIMESTAMP, ADDRESSES.user),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({
    event: explicitSettlement,
    mockDb,
  });
  const epochStats = mockDb.entities.UserEpochStats.get(`${user}:1`);
  assert.ok(epochStats);
  assert.ok((epochStats?.lpPoints ?? 0n) > 0n);
  const settledBalancerPosition = mockDb.entities.UserLPPosition.get(balancerPositionId);
  assert.equal(settledBalancerPosition?.lastSettledAt, LP_V2_RESUME_CUTOVER_TIMESTAMP);

  const boundaryBalancerConfig = mockDb.entities.LPPoolConfig.get(BALANCER_POOL);
  const boundaryV2Config = mockDb.entities.LPPoolConfig.get(V2_POOL);
  const boundaryPoolState = mockDb.entities.LPPoolState.get(BALANCER_POOL);
  const boundaryV2State = mockDb.entities.LPPoolV2State.get(BALANCER_POOL);
  const boundaryPosition = mockDb.entities.UserLPPosition.get(balancerPositionId);
  const boundaryEpochStats = mockDb.entities.UserEpochStats.get(`${user}:1`);
  const boundaryFeeStats = mockDb.entities.LPPoolFeeStats.get(BALANCER_POOL);
  const boundaryProtocolStats = mockDb.entities.ProtocolStats.get('1');
  assert.ok(boundaryProtocolStats);

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
  assert.deepEqual(mockDb.entities.ProtocolStats.get('1'), boundaryProtocolStats);
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
  mockDb = mockDb.entities.UserLPEpochCursor.set({
    id: `${positionId}:1`,
    position_id: positionId,
    user_id: user,
    pool: V2_POOL,
    epochNumber: 1n,
    growthBaselineX128: 0n,
    lastSettledAt: LP_V2_RESUME_CUTOVER_TIMESTAMP,
    lastUpdate: LP_V2_RESUME_CUTOVER_TIMESTAMP,
  });
  const positionBeforeResumeSync = mockDb.entities.UserLPPosition.get(positionId);

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
  assert.equal(pointsAfterFirstEvent, 0n);
  const positionAfterFirstEvent = mockDb.entities.UserLPPosition.get(positionId);
  assert.deepEqual(positionAfterFirstEvent, positionBeforeResumeSync);
  const growthAfterFirstEvent =
    mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n))?.scalarGrowthX128 ?? 0n;
  assert.ok(growthAfterFirstEvent > 0n);

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
  assert.equal(pointsAfterSecondEvent, 0n);
  const positionAfterSecondEvent = mockDb.entities.UserLPPosition.get(positionId);
  assert.deepEqual(positionAfterSecondEvent, positionBeforeResumeSync);
  const growthAfterSecondEvent =
    mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n))?.scalarGrowthX128 ?? 0n;
  assert.ok(growthAfterSecondEvent > growthAfterFirstEvent);

  const touch = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: user,
    to: user,
    value: 1n,
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 3, LP_V2_RESUME_CUTOVER_TIMESTAMP + 3660, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: touch, mockDb });
  assert.ok((mockDb.entities.UserEpochStats.get(`${user}:1`)?.lpPoints ?? 0n) > 0n);
  assert.equal(
    mockDb.entities.UserLPPosition.get(positionId)?.lastSettledAt,
    LP_V2_RESUME_CUTOVER_TIMESTAMP + 3660
  );
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
  assert.equal(pausedHolderA?.amount0, 0n);
  assert.equal(pausedHolderA?.amount1, 0n);
  assert.equal(pausedHolderA?.valueUsd, 0n);
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
  assert.equal(pausedHolderC?.amount0, 0n);
  assert.equal(pausedHolderC?.amount1, 0n);
  assert.equal(pausedHolderC?.valueUsd, 0n);
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
  const afterResumeSyncProtocolStats = mockDb.entities.ProtocolStats.get('1');
  assert.equal(afterResumeSyncProtocolStats?.uniqueUsers, baselineUniqueUsers);
  assert.equal(afterResumeSyncProtocolStats?.totalTransactions, baselineTransactions + 1n);
  assert.equal(mockDb.entities.User.get(holderA), undefined);
  assert.equal(mockDb.entities.User.get(holderC), undefined);
  assert.equal(mockDb.entities.User.get(burnedHolder), undefined);
  assert.equal(mockDb.entities.UserEpochStats.get(`${holderA}:7`), undefined);
  assert.equal(mockDb.entities.UserEpochStats.get(`${holderC}:7`), undefined);

  const touchA = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: holderA,
    to: holderA,
    value: 1n,
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 2, LP_V2_RESUME_CUTOVER_TIMESTAMP + 60, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: touchA, mockDb });
  const touchC = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: holderC,
    to: holderC,
    value: 1n,
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 3, LP_V2_RESUME_CUTOVER_TIMESTAMP + 60, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: touchC, mockDb });
  const resumedProtocolStats = mockDb.entities.ProtocolStats.get('1');
  assert.equal(resumedProtocolStats?.uniqueUsers, baselineUniqueUsers + 2);
  assert.equal(resumedProtocolStats?.totalTransactions, baselineTransactions + 3n);
  assert.ok(mockDb.entities.User.get(holderA));
  assert.ok(mockDb.entities.User.get(holderC));

  const expectedHolderAPoints =
    (1_000n * PRICE_E8 * 2500n * 60n * 10n ** 18n) / (PRICE_E8 * 10_000n * 86_400n);
  const expectedHolderCPoints =
    (200n * PRICE_E8 * 2500n * 60n * 10n ** 18n) / (PRICE_E8 * 10_000n * 86_400n);
  const resumedHolderA = mockDb.entities.UserLPPosition.get(holderAPositionId);
  const resumedHolderC = mockDb.entities.UserLPPosition.get(holderCPositionId);
  assert.equal(resumedHolderA?.accumulatedInRangeSeconds, 0n);
  assert.equal(resumedHolderA?.settledLpPoints, expectedHolderAPoints);
  assert.equal(resumedHolderC?.accumulatedInRangeSeconds, 0n);
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
    ...eventData(LP_V2_RESUME_CUTOVER_BLOCK + 4, LP_V2_RESUME_CUTOVER_TIMESTAMP + 60, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({
    event: idempotentResumeSync,
    mockDb,
  });

  const idempotentHolderA = mockDb.entities.UserLPPosition.get(holderAPositionId);
  const idempotentHolderC = mockDb.entities.UserLPPosition.get(holderCPositionId);
  assert.equal(idempotentHolderA?.accumulatedInRangeSeconds, 0n);
  assert.equal(idempotentHolderA?.settledLpPoints, expectedHolderAPoints);
  assert.equal(idempotentHolderC?.accumulatedInRangeSeconds, 0n);
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

test('uniswap v2 sync store calls are constant for one versus ten thousand indexed positions', async () => {
  const handler = await getRegisteredEventHandler('UniswapV2Pair', 'Sync');
  const TestHelpers = loadTestHelpers();

  async function trace(positionCount: number) {
    let mockDb = seedTask5CanonicalV2Fixture(TestHelpers.MockDb.createMockDb());
    mockDb = mockDb.entities.LPPoolPositionIndex.set({
      id: V2_POOL,
      pool: V2_POOL,
      positionIds: Array.from({ length: positionCount }, (_, index) => `fake:${index}`),
      lastUpdate: LP_V2_CUTOVER_TIMESTAMP,
    });
    const event = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
      reserve0: 600n,
      reserve1: 400n,
      ...createEventDataFactory()(LP_V2_CUTOVER_BLOCK + 1, LP_V2_CUTOVER_TIMESTAMP + 100, V2_POOL),
    });
    return await traceRegisteredEventBatch(mockDb, [{ handler, event }]);
  }

  const one = await trace(1);
  const tenThousand = await trace(10_000);
  for (const pass of ['preloadProbe', 'orderedProbe'] as const) {
    assert.deepEqual(one[pass].getCounts, tenThousand[pass].getCounts);
    assert.deepEqual(one[pass].setCounts, tenThousand[pass].setCounts);
    for (const entity of [
      'LPPoolPositionIndex',
      'UserLPPosition',
      'UserLPPositionIndex',
      'UserLPStats',
      'UserEpochStats',
    ]) {
      assert.equal(one[pass].getCounts.get(entity) ?? 0, 0, `${pass} ${entity} reads`);
      assert.equal(one[pass].setCounts.get(entity) ?? 0, 0, `${pass} ${entity} writes`);
    }
  }
});

test('balancer vault market events advance old scalar state with no holder or user work', async () => {
  const TestHelpers = loadTestHelpers();
  const timestamp = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 100;
  const blockNumber = LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1;
  const expectedGrowth = referenceFungibleGrowthX128({
    reserve0: 500n,
    reserve1: 500n,
    token0PriceE8: PRICE_E8,
    token1PriceE8: PRICE_E8,
    token0Decimals: 0,
    token1Decimals: 0,
    totalSupply: 1_000n,
    seconds: 100,
    rateBps: TASK5_RATE_BPS,
  });
  const eventData = createEventDataFactory();
  const cases = [
    {
      eventName: 'LiquidityAdded',
      event: TestHelpers.BalancerVault.LiquidityAdded.createMockEvent({
        pool: BALANCER_POOL,
        liquidityProvider: TASK5_USER_A,
        kind: 0n,
        totalSupply: 1_100n,
        amountsAddedRaw: [10n, 20n],
        swapFeeAmountsRaw: [0n, 0n],
        ...eventData(blockNumber, timestamp, BALANCER_VAULT_ADDRESS),
      }),
      reserve0: 510n,
      reserve1: 520n,
      supply: 1_100n,
    },
    {
      eventName: 'LiquidityRemoved',
      event: TestHelpers.BalancerVault.LiquidityRemoved.createMockEvent({
        pool: BALANCER_POOL,
        liquidityProvider: TASK5_USER_A,
        kind: 0n,
        totalSupply: 900n,
        amountsRemovedRaw: [10n, 20n],
        swapFeeAmountsRaw: [0n, 0n],
        ...eventData(blockNumber, timestamp, BALANCER_VAULT_ADDRESS),
      }),
      reserve0: 490n,
      reserve1: 480n,
      supply: 900n,
    },
    {
      eventName: 'Swap',
      event: TestHelpers.BalancerVault.Swap.createMockEvent({
        pool: BALANCER_POOL,
        tokenIn: USDC_ADDRESS,
        tokenOut: DUST_ADDRESS,
        amountIn: 10n,
        amountOut: 20n,
        swapFeePercentage: 10n ** 16n,
        swapFeeAmount: 0n,
        ...eventData(blockNumber, timestamp, BALANCER_VAULT_ADDRESS),
      }),
      reserve0: 510n,
      reserve1: 480n,
      supply: 1_000n,
    },
  ];

  for (const input of cases) {
    const handler = await getRegisteredEventHandler('BalancerVault', input.eventName);
    const trace = await traceRegisteredEventBatch(
      seedTask5BalancerFixture(TestHelpers.MockDb.createMockDb()),
      [{ handler, event: input.event }]
    );
    const growth = trace.orderedProbe.stores
      .get('LPPoolEpochGrowth')
      ?.get(lpPoolEpochGrowthId(BALANCER_POOL, 1n));
    assert.equal(growth?.scalarGrowthX128, expectedGrowth, input.eventName);
    assert.equal(growth?.lastTimestamp, timestamp, input.eventName);
    const state = trace.orderedProbe.stores.get('LPPoolV2State')?.get(BALANCER_POOL);
    assert.equal(state?.reserve0, input.reserve0, input.eventName);
    assert.equal(state?.reserve1, input.reserve1, input.eventName);
    assert.equal(state?.lpTotalSupply, input.supply, input.eventName);

    for (const pass of [trace.preloadProbe, trace.orderedProbe]) {
      for (const entity of [
        'LPPoolPositionIndex',
        'UserLPPosition',
        'UserLPPositionIndex',
        'UserLPStats',
        'UserEpochStats',
        'User',
      ]) {
        assert.equal(pass.getCounts.get(entity) ?? 0, 0, `${input.eventName} ${entity} reads`);
        assert.equal(pass.setCounts.get(entity) ?? 0, 0, `${input.eventName} ${entity} writes`);
      }
    }
  }
});

test('all four fungible market families have identical complete traces for one versus ten thousand position IDs', async () => {
  const TestHelpers = loadTestHelpers();
  const indexedPositionIds = (positionId: string, count: number) => [
    positionId,
    ...Array.from({ length: count - 1 }, (_, index) => `unread:${index}`),
  ];
  const setPoolIndex = (
    mockDb: MockDb,
    pool: string,
    positionId: string,
    count: number,
    lastUpdate: number
  ) =>
    mockDb.entities.LPPoolPositionIndex.set({
      id: pool,
      pool,
      positionIds: indexedPositionIds(positionId, count),
      lastUpdate,
    });
  const fungiblePosition = (pool: string) => `v2:${pool}:${TASK5_USER_A}`;
  const traceV2Sync = async (positionCount: number) => {
    let mockDb = seedTask5CanonicalV2Fixture(TestHelpers.MockDb.createMockDb(), [
      { user: TASK5_USER_A, liquidity: 100n },
    ]);
    mockDb = setPoolIndex(
      mockDb,
      V2_POOL,
      fungiblePosition(V2_POOL),
      positionCount,
      LP_V2_CUTOVER_TIMESTAMP
    );
    const event = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
      reserve0: 600n,
      reserve1: 400n,
      ...createEventDataFactory()(LP_V2_CUTOVER_BLOCK + 1, LP_V2_CUTOVER_TIMESTAMP + 100, V2_POOL),
    });
    const handler = await getRegisteredEventHandler('UniswapV2Pair', 'Sync');
    return await traceRegisteredEventBatch(mockDb, [{ handler, event }]);
  };
  const traceBalancer = async (
    eventName: 'Swap' | 'LiquidityAdded' | 'LiquidityRemoved',
    positionCount: number
  ) => {
    let mockDb = seedTask5BalancerFixture(TestHelpers.MockDb.createMockDb(), [
      { user: TASK5_USER_A, liquidity: 100n },
    ]);
    mockDb = setPoolIndex(
      mockDb,
      BALANCER_POOL,
      fungiblePosition(BALANCER_POOL),
      positionCount,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP
    );
    const eventData = createEventDataFactory()(
      LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 100,
      BALANCER_VAULT_ADDRESS
    );
    const event =
      eventName === 'Swap'
        ? TestHelpers.BalancerVault.Swap.createMockEvent({
            pool: BALANCER_POOL,
            tokenIn: USDC_ADDRESS,
            tokenOut: DUST_ADDRESS,
            amountIn: 10n,
            amountOut: 20n,
            swapFeePercentage: 10n ** 16n,
            swapFeeAmount: 0n,
            ...eventData,
          })
        : eventName === 'LiquidityAdded'
          ? TestHelpers.BalancerVault.LiquidityAdded.createMockEvent({
              pool: BALANCER_POOL,
              liquidityProvider: TASK5_USER_A,
              kind: 0n,
              totalSupply: 1_100n,
              amountsAddedRaw: [10n, 20n],
              swapFeeAmountsRaw: [0n, 0n],
              ...eventData,
            })
          : TestHelpers.BalancerVault.LiquidityRemoved.createMockEvent({
              pool: BALANCER_POOL,
              liquidityProvider: TASK5_USER_A,
              kind: 0n,
              totalSupply: 900n,
              amountsRemovedRaw: [10n, 20n],
              swapFeeAmountsRaw: [0n, 0n],
              ...eventData,
            });
    const handler = await getRegisteredEventHandler('BalancerVault', eventName);
    return await traceRegisteredEventBatch(mockDb, [{ handler, event }]);
  };
  const cases = [
    { name: 'UniswapV2Pair.Sync', trace: traceV2Sync },
    {
      name: 'BalancerVault.Swap',
      trace: (positionCount: number) => traceBalancer('Swap', positionCount),
    },
    {
      name: 'BalancerVault.LiquidityAdded',
      trace: (positionCount: number) => traceBalancer('LiquidityAdded', positionCount),
    },
    {
      name: 'BalancerVault.LiquidityRemoved',
      trace: (positionCount: number) => traceBalancer('LiquidityRemoved', positionCount),
    },
  ];
  const positionEntities = new Set([
    'LPPoolPositionIndex',
    'UserLPPositionIndex',
    'UserLPPosition',
  ]);
  const traceSummary: Array<{ name: string; preload: number; ordered: number }> = [];

  for (const input of cases) {
    const one = await input.trace(1);
    const tenThousand = await input.trace(10_000);
    for (const pass of ['preloadProbe', 'orderedProbe'] as const) {
      assert.ok(one[pass].operationTrace.length > 0, `${input.name} ${pass} trace is nonempty`);
      assert.deepEqual(
        one[pass].operationTrace,
        tenThousand[pass].operationTrace,
        `${input.name} ${pass} complete trace`
      );
      const forbiddenOperations = one[pass].operationTrace.filter(operation => {
        const [, entityName] = operation.split('|');
        return positionEntities.has(entityName);
      });
      assert.deepEqual(
        forbiddenOperations,
        [],
        `${input.name} ${pass} performs no position reads, writes, or scans`
      );
    }
    traceSummary.push({
      name: input.name,
      preload: one.preloadProbe.operationTrace.length,
      ordered: one.orderedProbe.operationTrace.length,
    });
  }
  assert.deepEqual(traceSummary, [
    { name: 'UniswapV2Pair.Sync', preload: 43, ordered: 42 },
    { name: 'BalancerVault.Swap', preload: 82, ordered: 80 },
    { name: 'BalancerVault.LiquidityAdded', preload: 51, ordered: 49 },
    { name: 'BalancerVault.LiquidityRemoved', preload: 51, ordered: 49 },
  ]);
});

test('fungible transfer attributes old growth exactly before changing balances', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  let mockDb = seedTask5FungibleFixture(TestHelpers.MockDb.createMockDb(), {
    positions: [
      { user: TASK5_USER_A, liquidity: 600n },
      { user: TASK5_USER_B, liquidity: 400n },
    ],
  });
  const expectedGrowth = referenceFungibleGrowthX128({
    reserve0: 500n,
    reserve1: 500n,
    token0PriceE8: PRICE_E8,
    token1PriceE8: PRICE_E8,
    token0Decimals: 0,
    token1Decimals: 0,
    totalSupply: 1_000n,
    seconds: 100,
    rateBps: TASK5_RATE_BPS,
  });
  const transfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: TASK5_USER_A,
    to: TASK5_USER_B,
    value: 100n,
    ...eventData(40_000_000, TASK5_EVENT, TASK5_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: transfer, mockDb });

  assert.equal(
    mockDb.entities.UserEpochStats.get(`${TASK5_USER_A}:1`)?.lpPoints,
    referenceFungiblePoints(600n, expectedGrowth)
  );
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${TASK5_USER_B}:1`)?.lpPoints,
    referenceFungiblePoints(400n, expectedGrowth)
  );
  assert.equal(
    mockDb.entities.UserLPPosition.get(`v2:${TASK5_POOL}:${TASK5_USER_A}`)?.liquidity,
    500n
  );
  assert.equal(
    mockDb.entities.UserLPPosition.get(`v2:${TASK5_POOL}:${TASK5_USER_B}`)?.liquidity,
    500n
  );
  assert.equal(mockDb.entities.LPPoolV2State.get(TASK5_POOL)?.lpTotalSupply, 1_000n);
  assert.equal(
    mockDb.entities.UserLPEpochCursor.get(`v2:${TASK5_POOL}:${TASK5_USER_A}:1`)?.growthBaselineX128,
    expectedGrowth
  );
  assert.equal(
    mockDb.entities.UserLPEpochCursor.get(`v2:${TASK5_POOL}:${TASK5_USER_B}:1`)?.growthBaselineX128,
    expectedGrowth
  );

  const secondGrowth = referenceFungibleGrowthX128({
    reserve0: 500n,
    reserve1: 500n,
    token0PriceE8: PRICE_E8,
    token1PriceE8: PRICE_E8,
    token0Decimals: 0,
    token1Decimals: 0,
    totalSupply: 1_000n,
    seconds: 100,
    rateBps: TASK5_RATE_BPS,
  });
  const reverseTransfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: TASK5_USER_B,
    to: TASK5_USER_A,
    value: 100n,
    ...eventData(40_000_001, TASK5_EVENT + 100, TASK5_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: reverseTransfer,
    mockDb,
  });

  assert.equal(
    mockDb.entities.UserEpochStats.get(`${TASK5_USER_A}:1`)?.lpPoints,
    referenceFungiblePoints(600n, expectedGrowth) + referenceFungiblePoints(500n, secondGrowth)
  );
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${TASK5_USER_B}:1`)?.lpPoints,
    referenceFungiblePoints(400n, expectedGrowth) + referenceFungiblePoints(500n, secondGrowth)
  );
  assert.equal(
    mockDb.entities.UserLPPosition.get(`v2:${TASK5_POOL}:${TASK5_USER_A}`)?.liquidity,
    600n
  );
  assert.equal(
    mockDb.entities.UserLPPosition.get(`v2:${TASK5_POOL}:${TASK5_USER_B}`)?.liquidity,
    400n
  );
  assert.equal(mockDb.entities.LPPoolV2State.get(TASK5_POOL)?.lpTotalSupply, 1_000n);
  assert.equal(
    mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(TASK5_POOL, 1n))?.scalarGrowthX128,
    expectedGrowth + secondGrowth
  );
});

test('fungible mint and burn attribute the interval to old supply and old balances', async () => {
  const TestHelpers = loadTestHelpers();
  const expectedGrowth = referenceFungibleGrowthX128({
    reserve0: 500n,
    reserve1: 500n,
    token0PriceE8: PRICE_E8,
    token1PriceE8: PRICE_E8,
    token0Decimals: 0,
    token1Decimals: 0,
    totalSupply: 1_000n,
    seconds: 100,
    rateBps: TASK5_RATE_BPS,
  });
  for (const input of [
    { from: ZERO_ADDRESS, to: TASK5_USER_A, supply: 1_100n, liquidity: 700n },
    { from: TASK5_USER_A, to: ZERO_ADDRESS, supply: 900n, liquidity: 500n },
  ]) {
    const eventData = createEventDataFactory();
    let mockDb = seedTask5FungibleFixture(TestHelpers.MockDb.createMockDb(), {
      positions: [{ user: TASK5_USER_A, liquidity: 600n }],
    });
    const transfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
      from: input.from,
      to: input.to,
      value: 100n,
      ...eventData(40_000_000, TASK5_EVENT, TASK5_POOL),
    });
    mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: transfer, mockDb });
    assert.equal(
      mockDb.entities.UserEpochStats.get(`${TASK5_USER_A}:1`)?.lpPoints,
      referenceFungiblePoints(600n, expectedGrowth)
    );
    assert.equal(mockDb.entities.LPPoolV2State.get(TASK5_POOL)?.lpTotalSupply, input.supply);
    assert.equal(
      mockDb.entities.UserLPPosition.get(`v2:${TASK5_POOL}:${TASK5_USER_A}`)?.liquidity,
      input.liquidity
    );

    const secondGrowth = referenceFungibleGrowthX128({
      reserve0: 500n,
      reserve1: 500n,
      token0PriceE8: PRICE_E8,
      token1PriceE8: PRICE_E8,
      token0Decimals: 0,
      token1Decimals: 0,
      totalSupply: input.supply,
      seconds: 100,
      rateBps: TASK5_RATE_BPS,
    });
    const secondTouch = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
      from: TASK5_USER_A,
      to: TASK5_USER_A,
      value: 1n,
      ...eventData(40_000_001, TASK5_EVENT + 100, TASK5_POOL),
    });
    mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
      event: secondTouch,
      mockDb,
    });

    assert.equal(
      mockDb.entities.UserEpochStats.get(`${TASK5_USER_A}:1`)?.lpPoints,
      referenceFungiblePoints(600n, expectedGrowth) +
        referenceFungiblePoints(input.liquidity, secondGrowth)
    );
    assert.equal(mockDb.entities.LPPoolV2State.get(TASK5_POOL)?.lpTotalSupply, input.supply);
    assert.equal(
      mockDb.entities.UserLPPosition.get(`v2:${TASK5_POOL}:${TASK5_USER_A}`)?.liquidity,
      input.liquidity
    );
    assert.equal(
      mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(TASK5_POOL, 1n))?.scalarGrowthX128,
      expectedGrowth + secondGrowth
    );
    assert.equal(
      mockDb.entities.UserLPEpochCursor.get(`v2:${TASK5_POOL}:${TASK5_USER_A}:1`)
        ?.growthBaselineX128,
      expectedGrowth + secondGrowth
    );
  }
});

test('fungible self-transfer settles and resets once while full burn stores a zero snapshot', async () => {
  const TestHelpers = loadTestHelpers();
  const expectedGrowth = referenceFungibleGrowthX128({
    reserve0: 500n,
    reserve1: 500n,
    token0PriceE8: PRICE_E8,
    token1PriceE8: PRICE_E8,
    token0Decimals: 0,
    token1Decimals: 0,
    totalSupply: 100n,
    seconds: 100,
    rateBps: TASK5_RATE_BPS,
  });
  let selfDb = seedTask5FungibleFixture(TestHelpers.MockDb.createMockDb(), {
    totalSupply: 100n,
    positions: [{ user: TASK5_USER_A, liquidity: 100n }],
  });
  const selfTransfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: TASK5_USER_A,
    to: TASK5_USER_A,
    value: 40n,
    ...createEventDataFactory()(40_000_000, TASK5_EVENT, TASK5_POOL),
  });
  selfDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({
    event: selfTransfer,
    mockDb: selfDb,
  });
  assert.equal(
    selfDb.entities.UserLPPosition.get(`v2:${TASK5_POOL}:${TASK5_USER_A}`)?.liquidity,
    100n
  );
  assert.equal(
    selfDb.entities.UserEpochStats.get(`${TASK5_USER_A}:1`)?.lpPoints,
    referenceFungiblePoints(100n, expectedGrowth)
  );
  assert.equal(
    selfDb.entities.UserLPEpochCursor.get(`v2:${TASK5_POOL}:${TASK5_USER_A}:1`)?.growthBaselineX128,
    expectedGrowth
  );

  let burnDb = seedTask5FungibleFixture(TestHelpers.MockDb.createMockDb(), {
    totalSupply: 100n,
    positions: [{ user: TASK5_USER_A, liquidity: 100n }],
  });
  const burn = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: TASK5_USER_A,
    to: ZERO_ADDRESS,
    value: 100n,
    ...createEventDataFactory()(40_000_000, TASK5_EVENT, TASK5_POOL),
  });
  burnDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: burn, mockDb: burnDb });
  const zero = burnDb.entities.UserLPPosition.get(`v2:${TASK5_POOL}:${TASK5_USER_A}`);
  assert.equal(zero?.liquidity, 0n);
  assert.equal(zero?.amount0, 0n);
  assert.equal(zero?.amount1, 0n);
  assert.equal(zero?.valueUsd, 0n);
  assert.equal(zero?.isInRange, false);
  assert.equal(
    burnDb.entities.UserLPEpochCursor.get(`v2:${TASK5_POOL}:${TASK5_USER_A}:1`)?.growthBaselineX128,
    expectedGrowth
  );
  assert.deepEqual(burnDb.entities.LPPoolPositionIndex.get(TASK5_POOL)?.positionIds, []);
});

test('fungible first observation and same-timestamp events never back-credit or duplicate growth', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = seedTask5FungibleFixture(TestHelpers.MockDb.createMockDb(), {
    includePoolState: false,
    includeV2State: false,
    includeGrowth: false,
  });
  const eventData = createEventDataFactory();
  for (const reserves of [
    { reserve0: 500n, reserve1: 500n },
    { reserve0: 600n, reserve1: 400n },
  ]) {
    const sync = TestHelpers.UniswapV2Pair.Sync.createMockEvent({
      ...reserves,
      ...eventData(40_000_000, TASK5_EVENT, TASK5_POOL),
    });
    mockDb = await TestHelpers.UniswapV2Pair.Sync.processEvent({ event: sync, mockDb });
  }
  const growth = mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(TASK5_POOL, 1n));
  assert.equal(growth?.startTimestamp, TASK5_EVENT);
  assert.equal(growth?.lastTimestamp, TASK5_EVENT);
  assert.equal(growth?.scalarGrowthX128, 0n);

  const mint = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: ZERO_ADDRESS,
    to: TASK5_USER_A,
    value: 100n,
    ...eventData(40_000_001, TASK5_EVENT, TASK5_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: mint, mockDb });
  assert.equal(
    mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(TASK5_POOL, 1n))?.scalarGrowthX128,
    0n
  );
  assert.equal(
    mockDb.entities.UserLPEpochCursor.get(`v2:${TASK5_POOL}:${TASK5_USER_A}:1`)?.growthBaselineX128,
    0n
  );
});

test('first observed balancer Vault state starts at its observation and same-time transitions add no growth', async () => {
  const TestHelpers = loadTestHelpers();
  const timestamp = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 100;
  const blockNumber = LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1;
  let mockDb = seedTask5BalancerFixture(TestHelpers.MockDb.createMockDb());
  // v2's MockDb store method is `delete`; `deleteUnsafe` is the handler-context name.
  mockDb = mockDb.entities.LPPoolState.delete(BALANCER_POOL);
  mockDb = mockDb.entities.LPPoolV2State.delete(BALANCER_POOL);
  mockDb = mockDb.entities.LPPoolEpochGrowth.delete(lpPoolEpochGrowthId(BALANCER_POOL, 1n));

  const eventData = createEventDataFactory();
  const liquidityAdded = TestHelpers.BalancerVault.LiquidityAdded.createMockEvent({
    pool: BALANCER_POOL,
    liquidityProvider: TASK5_USER_A,
    kind: 0n,
    totalSupply: 1_000n,
    amountsAddedRaw: [500n, 500n],
    swapFeeAmountsRaw: [0n, 0n],
    ...eventData(blockNumber, timestamp, BALANCER_VAULT_ADDRESS),
  });
  mockDb = await TestHelpers.BalancerVault.LiquidityAdded.processEvent({
    event: liquidityAdded,
    mockDb,
  });

  const firstGrowth = mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(BALANCER_POOL, 1n));
  assert.equal(firstGrowth?.startTimestamp, timestamp);
  assert.equal(firstGrowth?.lastTimestamp, timestamp);
  assert.equal(firstGrowth?.scalarGrowthX128, 0n);

  const swap = TestHelpers.BalancerVault.Swap.createMockEvent({
    pool: BALANCER_POOL,
    tokenIn: USDC_ADDRESS,
    tokenOut: DUST_ADDRESS,
    amountIn: 10n,
    amountOut: 20n,
    swapFeePercentage: 10n ** 16n,
    swapFeeAmount: 0n,
    ...eventData(blockNumber, timestamp, BALANCER_VAULT_ADDRESS),
  });
  mockDb = await TestHelpers.BalancerVault.Swap.processEvent({ event: swap, mockDb });

  const sameTimeGrowth = mockDb.entities.LPPoolEpochGrowth.get(
    lpPoolEpochGrowthId(BALANCER_POOL, 1n)
  );
  assert.equal(sameTimeGrowth?.startTimestamp, timestamp);
  assert.equal(sameTimeGrowth?.lastTimestamp, timestamp);
  assert.equal(sameTimeGrowth?.scalarGrowthX128, 0n);
  assert.equal(mockDb.entities.LPPoolV2State.get(BALANCER_POOL)?.reserve0, 510n);
  assert.equal(mockDb.entities.LPPoolV2State.get(BALANCER_POOL)?.reserve1, 480n);
  assert.equal(mockDb.entities.LPPoolV2State.get(BALANCER_POOL)?.lpTotalSupply, 1_000n);
});

test('paused fungible replay preserves stored outgoing growth without creating users or gifting it to a recipient', async () => {
  const TestHelpers = loadTestHelpers();
  const storedGrowth = 5n * LP_GROWTH_Q128;
  const pausedStart = LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP;
  const senderPositionId = `v2:${V2_POOL}:${TASK5_USER_A}`;
  let mockDb = seedTask5BalancerFixture(TestHelpers.MockDb.createMockDb());
  mockDb = mockDb.entities.LPPoolState.set({
    id: V2_POOL,
    pool: V2_POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: PRICE_E8,
    token1Price: PRICE_E8,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: pausedStart,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: V2_POOL,
    pool: V2_POOL,
    reserve0: 500n,
    reserve1: 500n,
    lpTotalSupply: 100n,
    lastUpdate: pausedStart,
  });
  mockDb = mockDb.entities.LPPoolEpochGrowth.set({
    id: lpPoolEpochGrowthId(V2_POOL, 1n),
    pool: V2_POOL,
    epochNumber: 1n,
    startTimestamp: pausedStart,
    lastTimestamp: pausedStart,
    scalarGrowthX128: storedGrowth,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: pausedStart,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: senderPositionId,
    tokenId: BigInt(TASK5_USER_A),
    user_id: TASK5_USER_A,
    pool: V2_POOL,
    positionManager: V2_POOL,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 100n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 0n,
    lastInRangeTimestamp: pausedStart,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: pausedStart,
    settledLpPoints: 0n,
    createdAt: pausedStart,
    lastUpdate: pausedStart,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: TASK5_USER_A,
    user_id: TASK5_USER_A,
    positionIds: [senderPositionId],
    lastUpdate: pausedStart,
  });
  mockDb = mockDb.entities.LPPoolPositionIndex.set({
    id: V2_POOL,
    pool: V2_POOL,
    positionIds: [senderPositionId],
    lastUpdate: pausedStart,
  });
  mockDb = mockDb.entities.UserLPEpochCursor.set({
    id: `${senderPositionId}:1`,
    position_id: senderPositionId,
    user_id: TASK5_USER_A,
    pool: V2_POOL,
    epochNumber: 1n,
    growthBaselineX128: 0n,
    lastSettledAt: pausedStart,
    lastUpdate: pausedStart,
  });
  const eventData = createEventDataFactory();
  const transfer = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: TASK5_USER_A,
    to: TASK5_USER_B,
    value: 40n,
    ...eventData(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1, pausedStart + 100, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: transfer, mockDb });
  const earned = referenceFungiblePoints(100n, storedGrowth);
  assert.equal(mockDb.entities.UserEpochStats.get(`${TASK5_USER_A}:1`)?.lpPoints, earned);
  assert.equal(mockDb.entities.UserEpochStats.get(`${TASK5_USER_B}:1`), undefined);
  assert.equal(mockDb.entities.User.get(TASK5_USER_A), undefined);
  assert.equal(mockDb.entities.User.get(TASK5_USER_B), undefined);
  assert.equal(
    mockDb.entities.UserLPEpochCursor.get(`v2:${V2_POOL}:${TASK5_USER_B}:1`)?.growthBaselineX128,
    storedGrowth
  );
  assert.equal(
    mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(V2_POOL, 1n))?.lastTimestamp,
    pausedStart
  );

  const repeat = TestHelpers.UniswapV2Pair.Transfer.createMockEvent({
    from: TASK5_USER_B,
    to: TASK5_USER_A,
    value: 10n,
    ...eventData(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 2, pausedStart + 200, V2_POOL),
  });
  mockDb = await TestHelpers.UniswapV2Pair.Transfer.processEvent({ event: repeat, mockDb });
  assert.equal(mockDb.entities.UserEpochStats.get(`${TASK5_USER_A}:1`)?.lpPoints, earned);
  assert.equal(mockDb.entities.UserEpochStats.get(`${TASK5_USER_B}:1`), undefined);
  assert.equal(mockDb.entities.User.get(TASK5_USER_A), undefined);
  assert.equal(mockDb.entities.User.get(TASK5_USER_B), undefined);
});

test('balancer fungible transfer uses the same old-growth attribution as uniswap v2', async () => {
  const TestHelpers = loadTestHelpers();
  const eventData = createEventDataFactory();
  let mockDb = seedTask5BalancerFixture(TestHelpers.MockDb.createMockDb(), [
    { user: TASK5_USER_A, liquidity: 600n, createdAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP },
    { user: TASK5_USER_B, liquidity: 400n, createdAt: LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP },
  ]);
  const expectedGrowth = referenceFungibleGrowthX128({
    reserve0: 500n,
    reserve1: 500n,
    token0PriceE8: PRICE_E8,
    token1PriceE8: PRICE_E8,
    token0Decimals: 0,
    token1Decimals: 0,
    totalSupply: 1_000n,
    seconds: 100,
    rateBps: TASK5_RATE_BPS,
  });
  const transfer = TestHelpers.BalancerAutoRangePool.Transfer.createMockEvent({
    from: TASK5_USER_A,
    to: TASK5_USER_B,
    value: 100n,
    ...eventData(
      LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 1,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 100,
      BALANCER_POOL
    ),
  });
  mockDb = await TestHelpers.BalancerAutoRangePool.Transfer.processEvent({
    event: transfer,
    mockDb,
  });
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${TASK5_USER_A}:1`)?.lpPoints,
    referenceFungiblePoints(600n, expectedGrowth)
  );
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${TASK5_USER_B}:1`)?.lpPoints,
    referenceFungiblePoints(400n, expectedGrowth)
  );
  assert.equal(
    mockDb.entities.UserLPPosition.get(`v2:${BALANCER_POOL}:${TASK5_USER_A}`)?.liquidity,
    500n
  );
  assert.equal(
    mockDb.entities.UserLPPosition.get(`v2:${BALANCER_POOL}:${TASK5_USER_B}`)?.liquidity,
    500n
  );

  const secondGrowth = referenceFungibleGrowthX128({
    reserve0: 500n,
    reserve1: 500n,
    token0PriceE8: PRICE_E8,
    token1PriceE8: PRICE_E8,
    token0Decimals: 0,
    token1Decimals: 0,
    totalSupply: 1_000n,
    seconds: 100,
    rateBps: TASK5_RATE_BPS,
  });
  const reverseTransfer = TestHelpers.BalancerAutoRangePool.Transfer.createMockEvent({
    from: TASK5_USER_B,
    to: TASK5_USER_A,
    value: 100n,
    ...eventData(
      LP_BALANCER_AUTORANGE_CUTOVER_BLOCK + 2,
      LP_BALANCER_AUTORANGE_CUTOVER_TIMESTAMP + 200,
      BALANCER_POOL
    ),
  });
  mockDb = await TestHelpers.BalancerAutoRangePool.Transfer.processEvent({
    event: reverseTransfer,
    mockDb,
  });

  assert.equal(
    mockDb.entities.UserEpochStats.get(`${TASK5_USER_A}:1`)?.lpPoints,
    referenceFungiblePoints(600n, expectedGrowth) + referenceFungiblePoints(500n, secondGrowth)
  );
  assert.equal(
    mockDb.entities.UserEpochStats.get(`${TASK5_USER_B}:1`)?.lpPoints,
    referenceFungiblePoints(400n, expectedGrowth) + referenceFungiblePoints(500n, secondGrowth)
  );
  assert.equal(
    mockDb.entities.UserLPPosition.get(`v2:${BALANCER_POOL}:${TASK5_USER_A}`)?.liquidity,
    600n
  );
  assert.equal(
    mockDb.entities.UserLPPosition.get(`v2:${BALANCER_POOL}:${TASK5_USER_B}`)?.liquidity,
    400n
  );
  assert.equal(
    mockDb.entities.LPPoolEpochGrowth.get(lpPoolEpochGrowthId(BALANCER_POOL, 1n))?.scalarGrowthX128,
    expectedGrowth + secondGrowth
  );
});
