// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. This file does not import the `v3-test-helpers` seam, so without this
// a bare `node --test` invocation would inherit them from the repo `.env` via envio's
// dotenv. Redundant under `pnpm run test`, which loads the same module via `--import`.
import './test-env-preload';

import assert from 'node:assert/strict';
import { test } from 'node:test';

import { AUSD_ADDRESS, USDC_ADDRESS, USDT0_ADDRESS, normalizeAddress } from '../helpers/constants';
import { LP_GROWTH_Q128 } from '../helpers/lpGrowthMath';
import {
  getLPPoolTokenDecimals,
  isFungibleLPPoolConfig,
  isStableUsdToken,
} from '../handlers/lpEntityHelpers';
import {
  advanceLPPoolGrowth,
  freezeLPGrowthForEpoch,
  lpPoolEpochGrowthId,
  readLPPositionGrowthX128,
  resetLPPositionGrowthBaseline,
  settleLPPositionGrowth,
  settleLPPositionGrowthAfterPoolAdvance,
  userLPEpochCursorId,
} from '../handlers/lpGrowth';

import type { handlerContext } from '../../generated';

type Row = { id: string } & Record<string, unknown>;

function createStore(initial: readonly Row[] = []) {
  const rows = new Map(initial.map(row => [row.id, row]));
  const getIds: string[] = [];
  const setRows: Row[] = [];

  return {
    get: async (id: string) => {
      getIds.push(id);
      return rows.get(id);
    },
    set: (row: Row) => {
      setRows.push(row);
      rows.set(row.id, row);
    },
    rows,
    getIds,
    setRows,
    get getCalls() {
      return getIds.length;
    },
  };
}

function buildContext() {
  const stores = {
    LeaderboardState: createStore(),
    LeaderboardEpoch: createStore(),
    LPPoolRegistry: createStore(),
    LPPoolConfig: createStore(),
    LPPoolState: createStore(),
    LPPoolV2State: createStore(),
    LPPoolEpochGrowth: createStore(),
    UserLPEpochCursor: createStore(),
    TokenInfo: createStore(),
    LPPoolPositionIndex: createStore(),
    UserLPPositionIndex: createStore(),
    UserLPPosition: createStore(),
  };

  return { stores, context: stores as unknown as handlerContext };
}

function resetPassContext(
  context: handlerContext,
  cursorStore: ReturnType<typeof createStore>,
  isPreload: boolean
): handlerContext {
  return {
    ...(context as unknown as Record<string, unknown>),
    isPreload,
    UserLPEpochCursor: isPreload
      ? {
          ...cursorStore,
          set: (_row: Row) => {},
        }
      : cursorStore,
  } as unknown as handlerContext;
}

const POOL_MIXED = '0x000000000000000000000000000000000000AaBb';
const POOL = normalizeAddress(POOL_MIXED);
const STABLE = '0x00000000000000000000000000000000000000a1';
const OTHER = '0x00000000000000000000000000000000000000b2';
const USER_MIXED = '0x000000000000000000000000000000000000CcDd';
const USER = normalizeAddress(USER_MIXED);
const ONE_POINT_GROWTH_X128 = LP_GROWTH_Q128 * 100_000_000n * 10_000n * 86_400n;

function buildScalarCursorFixture(input?: {
  createdAt?: number;
  growthX128?: bigint;
  growthTimestamp?: number;
  isActive?: boolean;
}) {
  const fixture = buildContext();
  const isActive = input?.isActive ?? true;
  fixture.stores.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive,
  });
  fixture.stores.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: isActive ? undefined : 2n,
    endTime: isActive ? 400 : 300,
    isActive,
    duration: isActive ? undefined : 200n,
    scheduledStartTime: 100,
    scheduledEndTime: 300,
  });
  fixture.stores.LPPoolConfig.set({
    id: POOL,
    pool: POOL,
    positionManager: POOL,
    token0: STABLE,
    token1: OTHER,
    fee: 3000,
    lpRateBps: 0n,
    isActive,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 100,
    disabledAtEpoch: isActive ? undefined : 1n,
    disabledAtTimestamp: isActive ? undefined : 200,
    lastUpdate: input?.growthTimestamp ?? 100,
  });
  fixture.stores.LPPoolState.set({
    id: POOL,
    pool: POOL,
    currentTick: 0,
    sqrtPriceX96: 1n << 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: input?.growthTimestamp ?? 100,
  });
  fixture.stores.LPPoolV2State.set({
    id: POOL,
    pool: POOL,
    reserve0: 1n,
    reserve1: 1n,
    lpTotalSupply: 1n,
    lastUpdate: input?.growthTimestamp ?? 100,
  });
  fixture.stores.LPPoolEpochGrowth.set({
    id: lpPoolEpochGrowthId(POOL, 1n),
    pool: POOL,
    epochNumber: 1n,
    startTimestamp: 100,
    lastTimestamp: input?.growthTimestamp ?? 200,
    scalarGrowthX128: input?.growthX128 ?? ONE_POINT_GROWTH_X128,
    isFrozen: !isActive,
    frozenAt: isActive ? undefined : 300,
    lastUpdate: input?.growthTimestamp ?? 200,
  });
  fixture.stores.TokenInfo.set({ id: STABLE, address: STABLE, decimals: 0, lastUpdate: 0 });
  fixture.stores.TokenInfo.set({ id: OTHER, address: OTHER, decimals: 0, lastUpdate: 0 });

  const position = {
    id: `v2:${POOL_MIXED}:${USER_MIXED}`,
    tokenId: 0n,
    user_id: USER_MIXED,
    pool: POOL_MIXED,
    positionManager: POOL_MIXED,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1n,
    amount0: 1n,
    amount1: 1n,
    isInRange: true,
    valueUsd: 200_000_000n,
    lastInRangeTimestamp: 50,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 50,
    settledLpPoints: 0n,
    createdAt: input?.createdAt ?? 99,
    lastUpdate: 50,
  };
  return { ...fixture, position };
}

test('growth IDs normalize every address-bearing component', () => {
  assert.equal(lpPoolEpochGrowthId(POOL_MIXED, 2n), `${POOL}:2`);
  assert.equal(
    userLPEpochCursorId(`v2:${POOL_MIXED}:0x000000000000000000000000000000000000CCDD`, 2n),
    `v2:${POOL}:0x000000000000000000000000000000000000ccdd:2`
  );
});

test('moved pool-shape and stable-token predicates preserve normalized semantics', () => {
  assert.equal(isFungibleLPPoolConfig({ pool: POOL_MIXED, positionManager: POOL }), true);
  assert.equal(isFungibleLPPoolConfig({ pool: POOL, positionManager: OTHER }), false);
  assert.equal(isStableUsdToken(AUSD_ADDRESS.toUpperCase()), true);
  assert.equal(isStableUsdToken(USDC_ADDRESS), true);
  assert.equal(isStableUsdToken(USDT0_ADDRESS), true);
  assert.equal(isStableUsdToken(OTHER), false);
});

test('moved decimal lookup keeps stable/DUST fallbacks and explicit stored values', async () => {
  const { context, stores } = buildContext();
  assert.deepEqual(
    await getLPPoolTokenDecimals(context, { token0: AUSD_ADDRESS, token1: OTHER }, 1),
    { token0Decimals: 6, token1Decimals: 18 }
  );

  stores.TokenInfo.set({
    id: AUSD_ADDRESS,
    address: AUSD_ADDRESS,
    decimals: 8,
    lastUpdate: 2,
  });
  stores.TokenInfo.set({ id: OTHER, address: OTHER, decimals: 0, lastUpdate: 2 });
  assert.deepEqual(await getLPPoolTokenDecimals(context, { token0: AUSD_ADDRESS, token1: OTHER }), {
    token0Decimals: 8,
    token1Decimals: 0,
  });
});

test('scalar growth starts at the later pool boundary and never reads positions', async () => {
  const { context, stores } = buildContext();
  stores.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  stores.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: undefined,
    endTime: 300,
    isActive: true,
    duration: 200n,
    scheduledStartTime: 100,
    scheduledEndTime: 300,
  });
  stores.LPPoolConfig.set({
    id: POOL,
    pool: POOL,
    positionManager: POOL_MIXED,
    token0: STABLE,
    token1: OTHER,
    fee: 3000,
    lpRateBps: 10_000n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 120,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 120,
  });
  stores.LPPoolState.set({
    id: POOL,
    pool: POOL,
    currentTick: 0,
    sqrtPriceX96: 1n << 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 120,
  });
  stores.LPPoolV2State.set({
    id: POOL,
    pool: POOL,
    reserve0: 50n,
    reserve1: 50n,
    lpTotalSupply: 1_000n,
    lastUpdate: 120,
  });
  stores.TokenInfo.set({ id: STABLE, address: STABLE, decimals: 0, lastUpdate: 0 });
  stores.TokenInfo.set({ id: OTHER, address: OTHER, decimals: 0, lastUpdate: 0 });

  await advanceLPPoolGrowth(context, POOL_MIXED, 200);

  const header = stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL_MIXED, 1n));
  assert.equal(header?.pool, POOL);
  assert.equal(header?.startTimestamp, 120);
  assert.equal(header?.lastTimestamp, 200);
  assert.ok((header?.scalarGrowthX128 as bigint) > 0n);
  assert.equal(stores.LPPoolPositionIndex.getCalls, 0);
  assert.equal(stores.UserLPPositionIndex.getCalls, 0);
  assert.equal(stores.UserLPPosition.getCalls, 0);
});

test('scalar growth caps at disable/end, skips re-enable gaps, and rejects invalid clocks', async () => {
  const { context, stores } = buildContext();
  stores.LeaderboardState.set({ id: 'current', currentEpochNumber: 1n, isActive: true });
  stores.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: 2n,
    endTime: 300,
    isActive: true,
    duration: 200n,
    scheduledStartTime: 100,
    scheduledEndTime: 300,
  });
  const config = {
    id: POOL,
    pool: POOL,
    positionManager: POOL,
    token0: STABLE,
    token1: OTHER,
    fee: 3000,
    lpRateBps: 1n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 100,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 240,
    lastUpdate: 100,
  };
  stores.LPPoolConfig.set(config);
  stores.LPPoolState.set({
    id: POOL,
    pool: POOL,
    currentTick: 0,
    sqrtPriceX96: 1n << 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 100,
  });
  stores.LPPoolV2State.set({
    id: POOL,
    pool: POOL,
    reserve0: 500n,
    reserve1: 500n,
    lpTotalSupply: 1_000n,
    lastUpdate: 100,
  });
  stores.TokenInfo.set({ id: STABLE, address: STABLE, decimals: 0, lastUpdate: 0 });
  stores.TokenInfo.set({ id: OTHER, address: OTHER, decimals: 0, lastUpdate: 0 });

  await advanceLPPoolGrowth(context, POOL, 400);
  let growth = stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n));
  assert.equal(growth?.lastTimestamp, 240);
  assert.equal(growth?.scalarGrowthX128, 140n * 100_000_000n * LP_GROWTH_Q128);

  stores.LPPoolConfig.set({
    ...config,
    enabledAtTimestamp: 260,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 260,
  });
  await advanceLPPoolGrowth(context, POOL, 280);
  growth = stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n));
  assert.equal(growth?.scalarGrowthX128, 160n * 100_000_000n * LP_GROWTH_Q128);

  await advanceLPPoolGrowth(context, POOL, 400);
  growth = stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n));
  assert.equal(growth?.lastTimestamp, 300);
  assert.equal(growth?.scalarGrowthX128, 180n * 100_000_000n * LP_GROWTH_Q128);
  await assert.rejects(() => advanceLPPoolGrowth(context, POOL, 299), /moved backward/);

  stores.LPPoolEpochGrowth.set({ ...growth!, isFrozen: true, frozenAt: undefined });
  const writesBeforeFrozenAdvance = stores.LPPoolEpochGrowth.setRows.length;
  await advanceLPPoolGrowth(context, POOL, 300);
  assert.equal(stores.LPPoolEpochGrowth.setRows.length, writesBeforeFrozenAdvance);
  await assert.rejects(() => advanceLPPoolGrowth(context, POOL, 301), /cannot advance frozen/);
});

test('zero scalar state advances time without back-credit', async () => {
  {
    const { context, stores } = buildContext();
    stores.LeaderboardState.set({ id: 'current', currentEpochNumber: 1n, isActive: true });
    stores.LeaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: 1n,
      startTime: 100,
      endBlock: undefined,
      endTime: 300,
      isActive: true,
      duration: 200n,
      scheduledStartTime: 100,
      scheduledEndTime: 300,
    });
    stores.LPPoolConfig.set({
      id: POOL,
      pool: POOL,
      positionManager: POOL,
      token0: STABLE,
      token1: OTHER,
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
      id: POOL,
      pool: POOL,
      currentTick: 0,
      sqrtPriceX96: 0n,
      token0Price: 0n,
      token1Price: 0n,
      feeProtocol0: 0,
      feeProtocol1: 0,
      lastUpdate: 100,
    });
    await advanceLPPoolGrowth(context, POOL, 200);
    const growth = stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n));
    assert.equal(growth?.lastTimestamp, 200);
    assert.equal(growth?.scalarGrowthX128, 0n);
  }
});

test('position growth reads the scalar header without any position-index lookup', async () => {
  const { context, stores } = buildContext();
  stores.LPPoolEpochGrowth.set({
    id: lpPoolEpochGrowthId(POOL, 1n),
    pool: POOL,
    epochNumber: 1n,
    startTimestamp: 100,
    lastTimestamp: 200,
    scalarGrowthX128: 123_456n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: 200,
  });
  stores.LPPoolConfig.set({
    id: POOL,
    pool: POOL,
    positionManager: POOL_MIXED,
    token0: STABLE,
    token1: OTHER,
    fee: 3000,
    lpRateBps: 1n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 100,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 200,
    lastUpdate: 200,
  });
  const position = {
    id: `v2:${POOL_MIXED}:0x000000000000000000000000000000000000CCDD`,
    tokenId: 1n,
    user_id: '0x000000000000000000000000000000000000ccdd',
    pool: POOL_MIXED,
    positionManager: POOL_MIXED,
    tickLower: -120,
    tickUpper: 120,
    liquidity: 10n,
    amount0: 0n,
    amount1: 0n,
    isInRange: true,
    valueUsd: 0n,
    lastInRangeTimestamp: 100,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 100,
    settledLpPoints: 0n,
    createdAt: 100,
    lastUpdate: 100,
  };

  assert.equal(await readLPPositionGrowthX128(context, position, 1n), 123_456n);
  assert.equal(stores.LPPoolPositionIndex.getCalls, 0);
  assert.equal(stores.UserLPPositionIndex.getCalls, 0);
  assert.equal(stores.UserLPPosition.getCalls, 0);
});

test('missing, inactive, frozen, and pre-boundary state are deterministic no-ops', async () => {
  const { context, stores } = buildContext();
  await advanceLPPoolGrowth(context, POOL, 200);
  stores.LeaderboardState.set({ id: 'current', currentEpochNumber: 0n, isActive: true });
  await advanceLPPoolGrowth(context, POOL, 200);
  stores.LeaderboardState.set({ id: 'current', currentEpochNumber: 1n, isActive: false });
  await advanceLPPoolGrowth(context, POOL, 200);
  stores.LeaderboardState.set({ id: 'current', currentEpochNumber: 1n, isActive: true });
  await advanceLPPoolGrowth(context, POOL, 200);

  stores.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 300,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 300,
    scheduledEndTime: 500,
  });
  await advanceLPPoolGrowth(context, POOL, 200);
  const config = {
    id: POOL,
    pool: POOL,
    positionManager: POOL,
    token0: STABLE,
    token1: OTHER,
    fee: 3000,
    lpRateBps: 1n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 300,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 300,
  };
  stores.LPPoolConfig.set(config);
  await advanceLPPoolGrowth(context, POOL, 400);
  stores.LPPoolConfig.set({ ...config, isActive: true });
  await advanceLPPoolGrowth(context, POOL, 200);
  stores.LPPoolState.set({
    id: POOL,
    pool: POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 300,
  });
  await advanceLPPoolGrowth(context, POOL, 200);
  assert.equal(stores.LPPoolEpochGrowth.rows.size, 0);

  stores.LPPoolEpochGrowth.set({
    id: lpPoolEpochGrowthId(POOL, 1n),
    pool: POOL,
    epochNumber: 1n,
    startTimestamp: 300,
    lastTimestamp: 300,
    scalarGrowthX128: 0n,
    isFrozen: true,
    frozenAt: 300,
    lastUpdate: 300,
  });
  const scalarContext = buildContext();
  scalarContext.stores.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  scalarContext.stores.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 100,
    scheduledEndTime: 300,
  });
  scalarContext.stores.LPPoolConfig.set({ ...config, isActive: true, enabledAtTimestamp: 100 });
  scalarContext.stores.LPPoolState.set({
    id: POOL,
    pool: POOL,
    currentTick: 0,
    sqrtPriceX96: 0n,
    token0Price: 0n,
    token1Price: 0n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 100,
  });
  await freezeLPGrowthForEpoch(buildContext().context, 1n, 300);
  const emptyRegistry = buildContext();
  emptyRegistry.stores.LPPoolRegistry.set({ id: 'global', poolIds: [], lastUpdate: 0 });
  await freezeLPGrowthForEpoch(emptyRegistry.context, 1n, 300);
  assert.equal(emptyRegistry.stores.LPPoolEpochGrowth.setRows.length, 0);
});

test('freeze uses the explicit epoch and freezes active and inactive registered growth only', async () => {
  const { context, stores } = buildContext();
  const inactivePoolMixed = '0x000000000000000000000000000000000000AaBc';
  const inactivePool = normalizeAddress(inactivePoolMixed);
  const missingPoolMixed = '0x000000000000000000000000000000000000AaBd';
  const missingPool = normalizeAddress(missingPoolMixed);
  stores.LeaderboardState.set({ id: 'current', currentEpochNumber: 2n, isActive: false });
  stores.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 1n,
    startTime: 100,
    endBlock: 2n,
    endTime: 300,
    isActive: false,
    duration: 200n,
    scheduledStartTime: 100,
    scheduledEndTime: 300,
  });
  stores.LeaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: 3n,
    startTime: 400,
    endBlock: undefined,
    endTime: undefined,
    isActive: false,
    duration: undefined,
    scheduledStartTime: 400,
    scheduledEndTime: 600,
  });
  stores.LPPoolRegistry.set({
    id: 'global',
    poolIds: [POOL_MIXED, inactivePoolMixed, missingPoolMixed],
    lastUpdate: 300,
  });
  stores.LPPoolConfig.set({
    id: POOL,
    pool: POOL,
    positionManager: POOL,
    token0: STABLE,
    token1: OTHER,
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
    id: POOL,
    pool: POOL,
    currentTick: 0,
    sqrtPriceX96: 1n << 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 100,
  });
  stores.LPPoolV2State.set({
    id: POOL,
    pool: POOL,
    reserve0: 50n,
    reserve1: 50n,
    lpTotalSupply: 1_000n,
    lastUpdate: 100,
  });
  stores.TokenInfo.set({ id: STABLE, address: STABLE, decimals: 0, lastUpdate: 0 });
  stores.TokenInfo.set({ id: OTHER, address: OTHER, decimals: 0, lastUpdate: 0 });
  stores.LPPoolConfig.set({
    id: inactivePool,
    pool: inactivePool,
    positionManager: inactivePool,
    token0: STABLE,
    token1: OTHER,
    fee: 3000,
    lpRateBps: 10_000n,
    isActive: false,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 100,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 200,
    lastUpdate: 200,
  });
  stores.LPPoolEpochGrowth.set({
    id: lpPoolEpochGrowthId(inactivePool, 1n),
    pool: inactivePool,
    epochNumber: 1n,
    startTimestamp: 100,
    lastTimestamp: 200,
    scalarGrowthX128: 999n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: 200,
  });

  await freezeLPGrowthForEpoch(context, 1n, 300);

  const activeGrowth = stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n));
  assert.equal(activeGrowth?.isFrozen, true);
  assert.equal(activeGrowth?.lastTimestamp, 300);
  assert.equal(activeGrowth?.frozenAt, 300);
  assert.ok((activeGrowth?.scalarGrowthX128 as bigint) > 0n);
  const inactiveGrowth = stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(inactivePool, 1n));
  assert.equal(inactiveGrowth?.isFrozen, true);
  assert.equal(inactiveGrowth?.lastTimestamp, 300);
  assert.equal(inactiveGrowth?.frozenAt, 300);
  assert.equal(inactiveGrowth?.scalarGrowthX128, 999n);
  assert.equal(stores.LPPoolEpochGrowth.rows.has(lpPoolEpochGrowthId(missingPool, 1n)), false);
  assert.equal(stores.LPPoolEpochGrowth.rows.has(lpPoolEpochGrowthId(POOL, 2n)), false);

  const writesAfterFirstFreeze = stores.LPPoolEpochGrowth.setRows.length;
  const rowsAfterFirstFreeze = new Map(stores.LPPoolEpochGrowth.rows);
  await freezeLPGrowthForEpoch(context, 1n, 300);
  assert.equal(stores.LPPoolEpochGrowth.setRows.length, writesAfterFirstFreeze);
  assert.deepEqual(stores.LPPoolEpochGrowth.rows, rowsAfterFirstFreeze);
  assert.equal(stores.LPPoolPositionIndex.getCalls, 0);
  assert.equal(stores.UserLPPositionIndex.getCalls, 0);
  assert.equal(stores.UserLPPosition.getCalls, 0);
});

test('freeze rejects reversed or conflicting finality clocks and keeps same-end repeats idempotent', async () => {
  function buildInactiveFreezeContext(growth: Row) {
    const fixture = buildContext();
    fixture.stores.LPPoolRegistry.set({ id: 'global', poolIds: [POOL_MIXED], lastUpdate: 300 });
    fixture.stores.LPPoolConfig.set({
      id: POOL,
      pool: POOL,
      positionManager: POOL,
      token0: STABLE,
      token1: OTHER,
      fee: 3000,
      lpRateBps: 1n,
      isActive: false,
      enabledAtEpoch: 1n,
      enabledAtTimestamp: 100,
      disabledAtEpoch: 1n,
      disabledAtTimestamp: 200,
      lastUpdate: 200,
    });
    fixture.stores.LPPoolEpochGrowth.set(growth);
    fixture.stores.LPPoolEpochGrowth.setRows.length = 0;
    return fixture;
  }

  const reversed = buildInactiveFreezeContext({
    id: lpPoolEpochGrowthId(POOL, 1n),
    pool: POOL,
    epochNumber: 1n,
    startTimestamp: 100,
    lastTimestamp: 400,
    scalarGrowthX128: 999n,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: 400,
  });
  await assert.rejects(
    () => freezeLPGrowthForEpoch(reversed.context, 1n, 300),
    /LP growth freeze timestamp moved backward/
  );
  assert.equal(reversed.stores.LPPoolEpochGrowth.setRows.length, 0);
  assert.equal(
    reversed.stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n))?.isFrozen,
    false
  );

  const frozenGrowth = {
    id: lpPoolEpochGrowthId(POOL, 1n),
    pool: POOL,
    epochNumber: 1n,
    startTimestamp: 100,
    lastTimestamp: 300,
    scalarGrowthX128: 999n,
    isFrozen: true,
    frozenAt: 300,
    lastUpdate: 300,
  };
  const conflicting = buildInactiveFreezeContext(frozenGrowth);
  await assert.rejects(
    () => freezeLPGrowthForEpoch(conflicting.context, 1n, 301),
    /LP growth already frozen at a different timestamp/
  );
  assert.equal(conflicting.stores.LPPoolEpochGrowth.setRows.length, 0);

  const sameEnd = buildInactiveFreezeContext(frozenGrowth);
  await freezeLPGrowthForEpoch(sameEnd.context, 1n, 300);
  assert.equal(sameEnd.stores.LPPoolEpochGrowth.setRows.length, 0);
  assert.deepEqual(
    sameEnd.stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n)),
    frozenGrowth
  );
});

test('position carried into a Tide settles from zero growth and reports the Tide boundary', async () => {
  const { context, stores, position } = buildScalarCursorFixture();

  const settlement = await settleLPPositionGrowth(context, position, 200);

  assert.deepEqual(settlement, {
    epochNumber: 1n,
    growthBaselineX128: 0n,
    currentGrowthX128: ONE_POINT_GROWTH_X128,
    pointsEarned: 1_000_000_000_000_000_000n,
    accrualStartTimestamp: 100,
    accrualEndTimestamp: 200,
    settledAt: 200,
  });
  assert.deepEqual(stores.UserLPEpochCursor.rows.get(userLPEpochCursorId(position.id, 1n)), {
    id: userLPEpochCursorId(position.id, 1n),
    position_id: normalizeAddress(position.id),
    user_id: USER,
    pool: POOL,
    epochNumber: 1n,
    growthBaselineX128: ONE_POINT_GROWTH_X128,
    lastSettledAt: 200,
    lastUpdate: 200,
  });
});

test('mid-Tide reset excludes growth before creation and a missing reset fails closed', async () => {
  const { context, stores, position } = buildScalarCursorFixture({
    createdAt: 110,
    growthTimestamp: 110,
    growthX128: ONE_POINT_GROWTH_X128,
  });
  stores.LeaderboardEpoch.set({
    ...stores.LeaderboardEpoch.rows.get('1')!,
    endTime: undefined,
  });
  stores.UserLPEpochCursor.getIds.length = 0;

  await resetLPPositionGrowthBaseline(context, position, 1n, 110);
  assert.deepEqual(stores.UserLPEpochCursor.getIds, [userLPEpochCursorId(position.id, 1n)]);
  stores.LPPoolEpochGrowth.set({
    ...stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n))!,
    lastTimestamp: 200,
    scalarGrowthX128: 3n * ONE_POINT_GROWTH_X128,
    lastUpdate: 200,
  });

  const settlement = await settleLPPositionGrowth(context, position, 200);
  assert.equal(settlement.growthBaselineX128, ONE_POINT_GROWTH_X128);
  assert.equal(settlement.currentGrowthX128, 3n * ONE_POINT_GROWTH_X128);
  assert.equal(settlement.pointsEarned, 2_000_000_000_000_000_000n);
  assert.equal(settlement.accrualStartTimestamp, 110);
  assert.equal(settlement.accrualEndTimestamp, 200);

  const missing = buildScalarCursorFixture({ createdAt: 110 });
  await assert.rejects(
    () => settleLPPositionGrowth(missing.context, missing.position, 200),
    /missing LP cursor for mid-Tide position/
  );
  assert.equal(missing.stores.UserLPEpochCursor.setRows.length, 0);
});

test('repeated settlement consumes only growth after the stored cursor baseline', async () => {
  const { context, stores, position } = buildScalarCursorFixture();
  const first = await settleLPPositionGrowth(context, position, 200);
  stores.LPPoolEpochGrowth.set({
    ...stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n))!,
    lastTimestamp: 300,
    scalarGrowthX128: 3n * ONE_POINT_GROWTH_X128,
    lastUpdate: 300,
  });

  const second = await settleLPPositionGrowth(context, position, 300);

  assert.equal(second.growthBaselineX128, first.currentGrowthX128);
  assert.equal(first.pointsEarned + second.pointsEarned, 3_000_000_000_000_000_000n);
  assert.equal(second.accrualStartTimestamp, 200);
  assert.equal(second.accrualEndTimestamp, 300);
});

test('owner transfer settles the old owner then resets one cursor for disjoint new-owner growth', async () => {
  const { context, stores, position } = buildScalarCursorFixture();
  const oldOwnerSettlement = await settleLPPositionGrowth(context, position, 200);
  const newOwner = '0x000000000000000000000000000000000000EeFf';
  const transferred = { ...position, user_id: newOwner, lastSettledAt: 200 };
  await resetLPPositionGrowthBaseline(context, transferred, 1n, 200);
  stores.LPPoolEpochGrowth.set({
    ...stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n))!,
    lastTimestamp: 300,
    scalarGrowthX128: 2n * ONE_POINT_GROWTH_X128,
    lastUpdate: 300,
  });

  const newOwnerSettlement = await settleLPPositionGrowth(context, transferred, 300);
  const cursor = stores.UserLPEpochCursor.rows.get(userLPEpochCursorId(position.id, 1n));

  assert.equal(oldOwnerSettlement.pointsEarned, 1_000_000_000_000_000_000n);
  assert.equal(newOwnerSettlement.pointsEarned, 1_000_000_000_000_000_000n);
  assert.equal(newOwnerSettlement.accrualStartTimestamp, 200);
  assert.equal(cursor?.user_id, normalizeAddress(newOwner));
  assert.equal(cursor?.lastSettledAt, 300);
});

test('active-Tide disabled-pool touch claims stored growth at the user mutation boundary', async () => {
  const { context, stores, position } = buildScalarCursorFixture();
  const activePool = normalizeAddress('0x0000000000000000000000000000000000000B0b');
  stores.LPPoolConfig.set({
    ...stores.LPPoolConfig.rows.get(POOL)!,
    isActive: false,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 200,
    lastUpdate: 200,
  });
  stores.LPPoolConfig.set({
    ...stores.LPPoolConfig.rows.get(POOL)!,
    id: activePool,
    pool: activePool,
    positionManager: activePool,
    isActive: true,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
  });

  const settlement = await settleLPPositionGrowth(context, position, 250);
  const growth = stores.LPPoolEpochGrowth.rows.get(lpPoolEpochGrowthId(POOL, 1n));
  const cursor = stores.UserLPEpochCursor.rows.get(userLPEpochCursorId(position.id, 1n));

  assert.equal(settlement.pointsEarned, 1_000_000_000_000_000_000n);
  assert.equal(settlement.accrualStartTimestamp, 100);
  assert.equal(settlement.accrualEndTimestamp, 250);
  assert.equal(settlement.settledAt, 250);
  assert.equal(cursor?.lastSettledAt, 250);
  assert.equal(growth?.lastTimestamp, 200);
  assert.equal(stores.LPPoolConfig.getIds.includes(activePool), false);
});

test('inactive Tide caps the user cursor at epoch end rather than pool disable', async () => {
  const { context, stores, position } = buildScalarCursorFixture({ isActive: false });

  const settlement = await settleLPPositionGrowth(context, position, 350);
  const cursor = stores.UserLPEpochCursor.rows.get(userLPEpochCursorId(position.id, 1n));

  assert.equal(settlement.pointsEarned, 1_000_000_000_000_000_000n);
  assert.equal(settlement.accrualStartTimestamp, 100);
  assert.equal(settlement.accrualEndTimestamp, 300);
  assert.equal(settlement.settledAt, 300);
  assert.equal(cursor?.lastSettledAt, 300);
});

test('gap mutation reset keeps the ended-Tide cursor capped and repeat settlement monotonic', async () => {
  const { context, stores, position } = buildScalarCursorFixture({ isActive: false });

  const first = await settleLPPositionGrowth(context, position, 350);
  const mutatedPosition = {
    ...position,
    liquidity: 2n,
    lastSettledAt: first.settledAt,
    lastUpdate: 350,
  };
  await resetLPPositionGrowthBaseline(context, mutatedPosition, 1n, 350);

  const afterReset = stores.UserLPEpochCursor.rows.get(userLPEpochCursorId(mutatedPosition.id, 1n));
  assert.equal(afterReset?.lastSettledAt, 300);
  assert.equal(afterReset?.lastUpdate, 350);

  const repeated = await settleLPPositionGrowth(context, mutatedPosition, 360);
  const afterRepeat = stores.UserLPEpochCursor.rows.get(
    userLPEpochCursorId(mutatedPosition.id, 1n)
  );

  assert.equal(repeated.pointsEarned, 0n);
  assert.equal(repeated.accrualStartTimestamp, 300);
  assert.equal(repeated.accrualEndTimestamp, 300);
  assert.equal(repeated.settledAt, 300);
  assert.equal(afterRepeat?.lastSettledAt, 300);
  assert.equal(afterRepeat?.lastUpdate, 360);
});

test('reset rejects negative liquidity before any growth, epoch, or cursor read', async () => {
  const { context, stores, position } = buildScalarCursorFixture();
  stores.LPPoolEpochGrowth.getIds.length = 0;
  stores.LPPoolConfig.getIds.length = 0;
  stores.LeaderboardEpoch.getIds.length = 0;
  stores.UserLPEpochCursor.getIds.length = 0;
  stores.UserLPEpochCursor.setRows.length = 0;

  await assert.rejects(
    () =>
      resetLPPositionGrowthBaseline(
        resetPassContext(context, stores.UserLPEpochCursor, false),
        { ...position, liquidity: -1n },
        1n,
        200
      ),
    /LP position liquidity cannot be negative/
  );

  assert.equal(stores.LPPoolEpochGrowth.getIds.length, 0);
  assert.equal(stores.LPPoolConfig.getIds.length, 0);
  assert.equal(stores.LeaderboardEpoch.getIds.length, 0);
  assert.equal(stores.UserLPEpochCursor.getIds.length, 0);
  assert.equal(stores.UserLPEpochCursor.setRows.length, 0);
});

test('reset rejects negative current growth after requesting the deterministic cursor', async () => {
  const { context, stores, position } = buildScalarCursorFixture({ growthX128: -1n });
  const cursorId = userLPEpochCursorId(position.id, 1n);
  stores.UserLPEpochCursor.getIds.length = 0;
  stores.UserLPEpochCursor.setRows.length = 0;

  await assert.rejects(
    () =>
      resetLPPositionGrowthBaseline(
        resetPassContext(context, stores.UserLPEpochCursor, false),
        position,
        1n,
        200
      ),
    /LP position growth cannot be negative/
  );

  assert.deepEqual(stores.UserLPEpochCursor.getIds, [cursorId]);
  assert.equal(stores.UserLPEpochCursor.setRows.length, 0);
  assert.equal(stores.UserLPEpochCursor.rows.size, 0);
});

const malformedResetCursorCases = [
  {
    name: 'wrong position',
    overrides: { position_id: 'wrong-position' },
    expectedError: /LP cursor position mismatch/,
  },
  {
    name: 'wrong pool',
    overrides: { pool: OTHER },
    expectedError: /LP cursor pool mismatch/,
  },
  {
    name: 'wrong epoch',
    overrides: { epochNumber: 2n },
    expectedError: /LP cursor epoch mismatch/,
  },
  {
    name: 'negative growth baseline',
    overrides: { growthBaselineX128: -1n },
    expectedError: /LP cursor growth baseline cannot be negative/,
  },
  {
    name: 'negative settled timestamp',
    overrides: { lastSettledAt: -1 },
    expectedError: /LP cursor settled timestamp cannot be negative/,
  },
  {
    name: 'negative update timestamp',
    overrides: { lastUpdate: -1 },
    expectedError: /LP cursor update timestamp cannot be negative/,
  },
  {
    name: 'future accounting clock',
    overrides: { lastSettledAt: 201 },
    expectedError: /LP cursor timestamp moved backward/,
  },
] as const;

for (const isPreload of [true, false]) {
  for (const malformedResetCursorCase of malformedResetCursorCases) {
    const passName = isPreload ? 'preload' : 'ordered';
    test(`reset ${passName} rejects ${malformedResetCursorCase.name} without rewriting state`, async () => {
      const { context, stores, position } = buildScalarCursorFixture();
      const cursorId = userLPEpochCursorId(position.id, 1n);
      const malformedCursor = {
        id: cursorId,
        position_id: normalizeAddress(position.id),
        user_id: USER,
        pool: POOL,
        epochNumber: 1n,
        growthBaselineX128: ONE_POINT_GROWTH_X128,
        lastSettledAt: 200,
        lastUpdate: 200,
        ...malformedResetCursorCase.overrides,
      };
      stores.UserLPEpochCursor.set(malformedCursor);
      stores.UserLPEpochCursor.setRows.length = 0;

      await assert.rejects(
        () =>
          resetLPPositionGrowthBaseline(
            resetPassContext(context, stores.UserLPEpochCursor, isPreload),
            position,
            1n,
            200
          ),
        malformedResetCursorCase.expectedError
      );

      assert.equal(stores.UserLPEpochCursor.setRows.length, 0);
      assert.deepEqual(stores.UserLPEpochCursor.rows.get(cursorId), malformedCursor);
    });
  }
}

const unsettledResetCursorCases = [
  {
    name: 'growth baseline',
    overrides: { growthBaselineX128: 0n },
    expectedError: /LP cursor growth is not settled before reset/,
  },
  {
    name: 'accounting clock',
    overrides: { lastSettledAt: 150, lastUpdate: 150 },
    expectedError: /LP cursor timestamp is not settled before reset/,
  },
] as const;

for (const unsettledResetCursorCase of unsettledResetCursorCases) {
  test(`ordered reset rejects an unconsumed ${unsettledResetCursorCase.name}`, async () => {
    const { context, stores, position } = buildScalarCursorFixture();
    const cursorId = userLPEpochCursorId(position.id, 1n);
    const existingCursor = {
      id: cursorId,
      position_id: normalizeAddress(position.id),
      user_id: USER,
      pool: POOL,
      epochNumber: 1n,
      growthBaselineX128: ONE_POINT_GROWTH_X128,
      lastSettledAt: 200,
      lastUpdate: 200,
      ...unsettledResetCursorCase.overrides,
    };
    stores.UserLPEpochCursor.set(existingCursor);
    stores.UserLPEpochCursor.setRows.length = 0;

    await assert.rejects(
      () =>
        resetLPPositionGrowthBaseline(
          resetPassContext(context, stores.UserLPEpochCursor, false),
          position,
          1n,
          200
        ),
      unsettledResetCursorCase.expectedError
    );

    assert.equal(stores.UserLPEpochCursor.setRows.length, 0);
    assert.deepEqual(stores.UserLPEpochCursor.rows.get(cursorId), existingCursor);
  });
}

test('preload then ordered transfer reset permits the owner change only after settlement proof', async () => {
  const { context, stores, position } = buildScalarCursorFixture();
  const cursorId = userLPEpochCursorId(position.id, 1n);
  const existingCursor = {
    id: cursorId,
    position_id: normalizeAddress(position.id),
    user_id: USER,
    pool: POOL,
    epochNumber: 1n,
    growthBaselineX128: 0n,
    lastSettledAt: 100,
    lastUpdate: 100,
  };
  const newOwner = '0x000000000000000000000000000000000000EeFf';
  const transferred = { ...position, user_id: newOwner, lastSettledAt: 200, lastUpdate: 200 };
  stores.UserLPEpochCursor.set(existingCursor);
  stores.UserLPEpochCursor.setRows.length = 0;

  const preloadContext = resetPassContext(context, stores.UserLPEpochCursor, true);
  await settleLPPositionGrowthAfterPoolAdvance(preloadContext, position, 200);
  stores.UserLPEpochCursor.getIds.length = 0;
  await resetLPPositionGrowthBaseline(preloadContext, transferred, 1n, 200);

  assert.deepEqual(stores.UserLPEpochCursor.getIds, [cursorId]);
  assert.equal(stores.UserLPEpochCursor.setRows.length, 0);
  assert.deepEqual(stores.UserLPEpochCursor.rows.get(cursorId), existingCursor);

  const orderedContext = resetPassContext(context, stores.UserLPEpochCursor, false);
  await settleLPPositionGrowthAfterPoolAdvance(orderedContext, position, 200);
  stores.UserLPEpochCursor.getIds.length = 0;
  await resetLPPositionGrowthBaseline(orderedContext, transferred, 1n, 200);

  const transferredCursor = stores.UserLPEpochCursor.rows.get(cursorId);
  assert.deepEqual(stores.UserLPEpochCursor.getIds, [cursorId]);
  assert.equal(transferredCursor?.user_id, normalizeAddress(newOwner));
  assert.equal(transferredCursor?.growthBaselineX128, ONE_POINT_GROWTH_X128);
  assert.equal(transferredCursor?.lastSettledAt, 200);
  assert.equal(transferredCursor?.lastUpdate, 200);
});

test('ordered same-owner liquidity reset consumes only an already-settled cursor', async () => {
  const { context, stores, position } = buildScalarCursorFixture();
  const cursorId = userLPEpochCursorId(position.id, 1n);
  stores.UserLPEpochCursor.set({
    id: cursorId,
    position_id: normalizeAddress(position.id),
    user_id: USER,
    pool: POOL,
    epochNumber: 1n,
    growthBaselineX128: 0n,
    lastSettledAt: 100,
    lastUpdate: 100,
  });
  stores.UserLPEpochCursor.setRows.length = 0;
  const orderedContext = resetPassContext(context, stores.UserLPEpochCursor, false);

  await settleLPPositionGrowthAfterPoolAdvance(orderedContext, position, 200);
  stores.UserLPEpochCursor.getIds.length = 0;
  await resetLPPositionGrowthBaseline(
    orderedContext,
    { ...position, liquidity: 2n, lastSettledAt: 200, lastUpdate: 200 },
    1n,
    200
  );

  const resetCursor = stores.UserLPEpochCursor.rows.get(cursorId);
  assert.deepEqual(stores.UserLPEpochCursor.getIds, [cursorId]);
  assert.equal(resetCursor?.user_id, USER);
  assert.equal(resetCursor?.growthBaselineX128, ONE_POINT_GROWTH_X128);
  assert.equal(resetCursor?.lastSettledAt, 200);
  assert.equal(resetCursor?.lastUpdate, 200);
});

test('a prior-Tide pool disable cannot invert or rewind the current Tide cursor interval', async () => {
  const { context, stores, position } = buildScalarCursorFixture();
  stores.LeaderboardState.set({ id: 'current', currentEpochNumber: 2n, isActive: true });
  stores.LeaderboardEpoch.set({
    id: '2',
    epochNumber: 2n,
    startBlock: 3n,
    startTime: 400,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 400,
    scheduledEndTime: 600,
  });
  stores.LPPoolConfig.set({
    ...stores.LPPoolConfig.rows.get(POOL)!,
    isActive: false,
    disabledAtEpoch: 1n,
    disabledAtTimestamp: 200,
    lastUpdate: 200,
  });

  const settlement = await settleLPPositionGrowth(context, position, 500);
  const cursor = stores.UserLPEpochCursor.rows.get(userLPEpochCursorId(position.id, 2n));

  assert.deepEqual(settlement, {
    epochNumber: 2n,
    growthBaselineX128: 0n,
    currentGrowthX128: 0n,
    pointsEarned: 0n,
    accrualStartTimestamp: 400,
    accrualEndTimestamp: 500,
    settledAt: 500,
  });
  assert.equal(cursor?.lastSettledAt, 500);
  assert.ok(settlement.accrualStartTimestamp <= settlement.accrualEndTimestamp);
});

const malformedCursorCases = [
  {
    name: 'wrong position',
    overrides: { position_id: 'wrong-position' },
    expectedError: /LP cursor position mismatch/,
  },
  {
    name: 'wrong owner',
    overrides: { user_id: OTHER },
    expectedError: /LP cursor owner mismatch/,
  },
  {
    name: 'wrong pool',
    overrides: { pool: OTHER },
    expectedError: /LP cursor pool mismatch/,
  },
  {
    name: 'wrong epoch',
    overrides: { epochNumber: 2n },
    expectedError: /LP cursor epoch mismatch/,
  },
  {
    name: 'future accounting clock',
    overrides: { lastSettledAt: 201 },
    expectedError: /LP cursor timestamp moved backward/,
  },
  {
    name: 'negative growth baseline',
    overrides: { growthBaselineX128: -1n },
    expectedError: /LP cursor growth baseline cannot be negative/,
  },
  {
    name: 'negative settled timestamp',
    overrides: { lastSettledAt: -1 },
    expectedError: /LP cursor settled timestamp cannot be negative/,
  },
  {
    name: 'negative update timestamp',
    overrides: { lastUpdate: -1 },
    expectedError: /LP cursor update timestamp cannot be negative/,
  },
] as const;

for (const malformedCursorCase of malformedCursorCases) {
  test(`cursor settlement rejects ${malformedCursorCase.name} before rewriting state`, async () => {
    const { context, stores, position } = buildScalarCursorFixture();
    const cursorId = userLPEpochCursorId(position.id, 1n);
    const malformedCursor = {
      id: cursorId,
      position_id: normalizeAddress(position.id),
      user_id: USER,
      pool: POOL,
      epochNumber: 1n,
      growthBaselineX128: 0n,
      lastSettledAt: 150,
      lastUpdate: 150,
      ...malformedCursorCase.overrides,
    };
    stores.UserLPEpochCursor.set(malformedCursor);
    stores.UserLPEpochCursor.setRows.length = 0;

    await assert.rejects(
      () => settleLPPositionGrowth(context, position, 200),
      malformedCursorCase.expectedError
    );

    assert.equal(stores.UserLPEpochCursor.setRows.length, 0);
    assert.deepEqual(stores.UserLPEpochCursor.rows.get(cursorId), malformedCursor);
  });
}

test('zero-liquidity position can consume its growth baseline without earning points', async () => {
  const { context, stores, position } = buildScalarCursorFixture();
  const zeroLiquidityPosition = { ...position, liquidity: 0n };

  const settlement = await settleLPPositionGrowth(context, zeroLiquidityPosition, 200);
  const cursor = stores.UserLPEpochCursor.rows.get(
    userLPEpochCursorId(zeroLiquidityPosition.id, 1n)
  );

  assert.equal(settlement.pointsEarned, 0n);
  assert.equal(cursor?.growthBaselineX128, ONE_POINT_GROWTH_X128);
  assert.equal(cursor?.lastSettledAt, 200);
});

test('negative-liquidity position rejects before consuming or writing its growth cursor', async () => {
  const { context, stores, position } = buildScalarCursorFixture();
  stores.UserLPEpochCursor.setRows.length = 0;

  await assert.rejects(
    () => settleLPPositionGrowth(context, { ...position, liquidity: -1n }, 200),
    /LP position liquidity cannot be negative/
  );

  assert.equal(stores.UserLPEpochCursor.setRows.length, 0);
  assert.equal(stores.UserLPEpochCursor.rows.size, 0);
});

test('cursor settlement returns complete zero shapes and rejects a negative growth delta', async () => {
  const missingState = buildContext();
  const position = buildScalarCursorFixture().position;
  assert.deepEqual(await settleLPPositionGrowth(missingState.context, position, 222), {
    epochNumber: 0n,
    growthBaselineX128: 0n,
    currentGrowthX128: 0n,
    pointsEarned: 0n,
    accrualStartTimestamp: 222,
    accrualEndTimestamp: 222,
    settledAt: 222,
  });

  const epochZero = buildContext();
  epochZero.stores.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 0n,
    isActive: true,
  });
  assert.deepEqual(await settleLPPositionGrowth(epochZero.context, position, 222), {
    epochNumber: 0n,
    growthBaselineX128: 0n,
    currentGrowthX128: 0n,
    pointsEarned: 0n,
    accrualStartTimestamp: 222,
    accrualEndTimestamp: 222,
    settledAt: 222,
  });

  const missingEpoch = buildContext();
  missingEpoch.stores.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 7n,
    isActive: true,
  });
  assert.deepEqual(await settleLPPositionGrowth(missingEpoch.context, position, 223), {
    epochNumber: 7n,
    growthBaselineX128: 0n,
    currentGrowthX128: 0n,
    pointsEarned: 0n,
    accrualStartTimestamp: 223,
    accrualEndTimestamp: 223,
    settledAt: 223,
  });

  const negative = buildScalarCursorFixture();
  negative.stores.UserLPEpochCursor.set({
    id: userLPEpochCursorId(negative.position.id, 1n),
    position_id: normalizeAddress(negative.position.id),
    user_id: USER,
    pool: POOL,
    epochNumber: 1n,
    growthBaselineX128: 2n * ONE_POINT_GROWTH_X128,
    lastSettledAt: 150,
    lastUpdate: 150,
  });
  negative.stores.UserLPEpochCursor.setRows.length = 0;
  await assert.rejects(
    () => settleLPPositionGrowth(negative.context, negative.position, 200),
    /LP growth delta cannot be negative/
  );
  assert.equal(negative.stores.UserLPEpochCursor.setRows.length, 0);
});

test('unreadable pool growth never destroys a nonzero cursor baseline', async () => {
  const { stores, context, position } = buildScalarCursorFixture();
  const cursorId = userLPEpochCursorId(position.id, 1n);
  const baselineX128 = ONE_POINT_GROWTH_X128 / 2n;
  stores.UserLPEpochCursor.set({
    id: cursorId,
    position_id: normalizeAddress(position.id),
    user_id: normalizeAddress(position.user_id),
    pool: normalizeAddress(position.pool),
    epochNumber: 1n,
    growthBaselineX128: baselineX128,
    lastSettledAt: 150,
    lastUpdate: 150,
  });

  // The growth header becomes unreadable while a nonzero baseline is already persisted.
  // This is the live-mainnet state that crashed the indexer at block 46,514,762: the read
  // must report "unknown", never coerce to 0n and burn the baseline.
  stores.LPPoolEpochGrowth.rows.delete(lpPoolEpochGrowthId(POOL, 1n));

  const settlement = await settleLPPositionGrowthAfterPoolAdvance(context, position, 250);

  assert.equal(settlement.pointsEarned, 0n);
  const cursor = stores.UserLPEpochCursor.rows.get(cursorId) as unknown as {
    growthBaselineX128: bigint;
  };
  assert.equal(cursor.growthBaselineX128, baselineX128);
});

test('unreadable pool growth with a zero baseline stays a benign no-op', async () => {
  const { stores, context, position } = buildScalarCursorFixture();
  stores.LPPoolEpochGrowth.rows.delete(lpPoolEpochGrowthId(POOL, 1n));

  const settlement = await settleLPPositionGrowthAfterPoolAdvance(context, position, 250);

  assert.equal(settlement.pointsEarned, 0n);
  assert.equal(settlement.currentGrowthX128, 0n);
});

test('unreadable pool growth never rebaselines a cursor on reset', async () => {
  const { stores, context, position } = buildScalarCursorFixture();
  const cursorId = userLPEpochCursorId(position.id, 1n);
  const baselineX128 = ONE_POINT_GROWTH_X128 / 2n;
  stores.UserLPEpochCursor.set({
    id: cursorId,
    position_id: normalizeAddress(position.id),
    user_id: normalizeAddress(position.user_id),
    pool: normalizeAddress(position.pool),
    epochNumber: 1n,
    growthBaselineX128: baselineX128,
    lastSettledAt: 150,
    lastUpdate: 150,
  });
  stores.LPPoolEpochGrowth.rows.delete(lpPoolEpochGrowthId(POOL, 1n));

  await resetLPPositionGrowthBaseline(context, position, 1n, 250);

  const cursor = stores.UserLPEpochCursor.rows.get(cursorId) as unknown as {
    growthBaselineX128: bigint;
  };
  assert.equal(cursor.growthBaselineX128, baselineX128);
});

test('readLPPositionGrowthX128 reports unknown rather than zero for a missing header', async () => {
  const { stores, context, position } = buildScalarCursorFixture();
  stores.LPPoolEpochGrowth.rows.delete(lpPoolEpochGrowthId(POOL, 1n));
  assert.equal(await readLPPositionGrowthX128(context, position, 1n), undefined);
});

test('readLPPositionGrowthX128 reports unknown when the pool config is missing', async () => {
  // The header alone cannot be trusted: without a config there is no way to know the pool
  // still accrues, and reporting 0n here would let a nonzero cursor baseline read negative.
  const { stores, context, position } = buildScalarCursorFixture();
  stores.LPPoolConfig.rows.delete(POOL);
  assert.equal(await readLPPositionGrowthX128(context, position, 1n), undefined);
});

test('a zero growth reading never inverts a nonzero cursor baseline', async () => {
  const { stores, context, position } = buildScalarCursorFixture({ growthX128: 0n });
  const cursorId = userLPEpochCursorId(position.id, 1n);
  const baselineX128 = ONE_POINT_GROWTH_X128 / 4n;
  stores.UserLPEpochCursor.set({
    id: cursorId,
    position_id: normalizeAddress(position.id),
    user_id: normalizeAddress(position.user_id),
    pool: normalizeAddress(position.pool),
    epochNumber: 1n,
    growthBaselineX128: baselineX128,
    lastSettledAt: 150,
    lastUpdate: 150,
  });

  // The pool reads back zero growth (unpriced/uninitialized valuation inputs) while a
  // nonzero baseline is already persisted. That reading carries no information, so the
  // cursor must survive intact rather than being treated as negative growth.
  const settlement = await settleLPPositionGrowthAfterPoolAdvance(context, position, 250);

  assert.equal(settlement.pointsEarned, 0n);
  const cursor = stores.UserLPEpochCursor.rows.get(cursorId) as unknown as {
    growthBaselineX128: bigint;
  };
  assert.equal(cursor.growthBaselineX128, baselineX128);
});
