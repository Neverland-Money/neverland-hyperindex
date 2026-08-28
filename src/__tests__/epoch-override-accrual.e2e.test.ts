import assert from 'node:assert/strict';
import { test } from 'node:test';

import { createDefaultReserve } from '../helpers/entityHelpers';
import { LEADERBOARD_START_BLOCK } from '../helpers/constants';
import { TestHelpers, type MockDb } from './v3-test-helpers';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

const DAY = 86400;
const RAY = 10n ** 27n;
const DECIMALS = 6;
const UNIT = 10n ** 6n;

// Real tide-9 numbers: the previous tide ended 04:00 UTC, the EpochStart event
// landed a few minutes later carrying the wrong start, and the tide should run
// from 05:00 UTC.
const PREVIOUS_END = 1787889600;
const EVENT_AT = 1787890000;
const TIDE_START = 1787893200;
const WRONG_START = 1790442000;
const PRE = PREVIOUS_END - 3600;
const ACCRUE_AT = TIDE_START + 5 * DAY;

const ADDRESSES = {
  user: '0x0000000000000000000000000000000000000001',
  asset: '0x00000000000000000000000000000000000000a1',
  pool: '0x00000000000000000000000000000000000000b1',
  aToken: '0x00000000000000000000000000000000000000c1',
  vToken: '0x00000000000000000000000000000000000000d1',
  epochManager: '0x00000000000000000000000000000000000000e1',
  leaderboardConfig: '0x00000000000000000000000000000000000000f1',
};

function createEventDataFactory(seed: number) {
  let counter = seed;
  return (blockNumber: number, timestamp: number, srcAddress: string) => {
    counter += 1;
    return {
      mockEventData: {
        block: { number: blockNumber, timestamp },
        logIndex: counter,
        srcAddress,
        transaction: { hash: `0x${counter.toString(16).padStart(64, '0')}` },
      },
    };
  };
}

function seedBaseState(mockDb: MockDb): MockDb {
  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  let next = mockDb;
  next = next.entities.Protocol.set({ id: '1' });
  next = next.entities.Pool.set({
    id: ADDRESSES.pool,
    addressProviderId: 0n,
    protocol_id: '1',
    pool: undefined,
    poolCollateralManager: undefined,
    poolConfiguratorImpl: undefined,
    poolConfigurator: undefined,
    poolDataProviderImpl: undefined,
    poolImpl: undefined,
    proxyPriceProvider: undefined,
    bridgeProtocolFee: undefined,
    flashloanPremiumToProtocol: undefined,
    flashloanPremiumTotal: undefined,
    active: true,
    paused: false,
    lastUpdateTimestamp: PRE,
  });
  next = next.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: PRE,
    isActive: true,
    borrowingEnabled: true,
  });
  for (const token of [ADDRESSES.aToken, ADDRESSES.vToken]) {
    next = next.entities.SubToken.set({
      id: token,
      pool_id: ADDRESSES.pool,
      tokenContractImpl: undefined,
      underlyingAssetAddress: ADDRESSES.asset,
      underlyingAssetDecimals: DECIMALS,
    });
  }
  next = next.entities.PriceOracleAsset.set({
    id: ADDRESSES.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: PRE,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });
  return next;
}

/**
 * Runs one tide end to end: a user holding a supply and a borrow across the
 * boundary, the EpochStart event carrying `eventStartTime`, then an event five
 * days into the tide that forces accrual.
 */
async function runTide(epochNumber: bigint, eventStartTime: bigint) {
  const eventData = createEventDataFactory(Number(epochNumber) * 1000);
  const baseBlock = LEADERBOARD_START_BLOCK + 100;
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seedBaseState(mockDb);

  // The previous tide is closed, so this one is next in line.
  const previous = epochNumber - 1n;
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: previous,
    isActive: false,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: previous.toString(),
    epochNumber: previous,
    startBlock: BigInt(baseBlock - 50),
    startTime: PRE - DAY,
    endBlock: BigInt(baseBlock - 2),
    endTime: PREVIOUS_END,
    isActive: false,
    duration: BigInt(PREVIOUS_END - (PRE - DAY)),
    scheduledStartTime: PRE - DAY,
    scheduledEndTime: PREVIOUS_END,
  });

  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
      depositRateBps: 10000n,
      borrowRateBps: 20000n,
      vpRateBps: 0n,
      supplyDailyBonus: 0n,
      borrowDailyBonus: 0n,
      repayDailyBonus: 0n,
      withdrawDailyBonus: 0n,
      cooldownSeconds: 0n,
      minDailyBonusUsd: 0n,
      timestamp: BigInt(PRE),
      ...eventData(baseBlock - 1, PRE, ADDRESSES.leaderboardConfig),
    }),
    mockDb,
  });

  mockDb = await TestHelpers.AToken.Mint.processEvent({
    event: TestHelpers.AToken.Mint.createMockEvent({
      caller: ADDRESSES.user,
      onBehalfOf: ADDRESSES.user,
      value: 1000n * UNIT,
      balanceIncrease: 0n,
      index: RAY,
      ...eventData(baseBlock - 1, PRE, ADDRESSES.aToken),
    }),
    mockDb,
  });

  mockDb = await TestHelpers.VariableDebtToken.Mint.processEvent({
    event: TestHelpers.VariableDebtToken.Mint.createMockEvent({
      caller: ADDRESSES.user,
      onBehalfOf: ADDRESSES.user,
      value: 500n * UNIT,
      balanceIncrease: 0n,
      index: RAY,
      ...eventData(baseBlock - 1, PRE + 1, ADDRESSES.vToken),
    }),
    mockDb,
  });

  // The EpochStart event lands before the tide is meant to begin.
  mockDb = await TestHelpers.EpochManager.EpochStart.processEvent({
    event: TestHelpers.EpochManager.EpochStart.createMockEvent({
      epochNumber,
      startTime: eventStartTime,
      ...eventData(baseBlock, EVENT_AT, ADDRESSES.epochManager),
    }),
    mockDb,
  });

  // Five days into the tide, ordinary activity forces accrual.
  mockDb = await TestHelpers.AToken.Mint.processEvent({
    event: TestHelpers.AToken.Mint.createMockEvent({
      caller: ADDRESSES.user,
      onBehalfOf: ADDRESSES.user,
      value: 0n,
      balanceIncrease: 0n,
      index: RAY,
      ...eventData(baseBlock + 500, ACCRUE_AT, ADDRESSES.aToken),
    }),
    mockDb,
  });

  return {
    epoch: mockDb.entities.LeaderboardEpoch.get(epochNumber.toString()),
    state: mockDb.entities.LeaderboardState.get('current'),
    stats: mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:${epochNumber}`),
  };
}

test('an overridden tide runs and accrues exactly like a normally scheduled one', async () => {
  // Tide 9: the chain says it starts 2026-09-26, the override says 2026-08-28.
  const overridden = await runTide(9n, BigInt(WRONG_START));
  // Control: the same tide, honestly scheduled on-chain for the same start.
  const control = await runTide(8n, BigInt(TIDE_START));

  // The tide is started and ongoing.
  assert.equal(overridden.epoch?.startTime, TIDE_START, 'tide 9 starts at the override');
  assert.equal(overridden.epoch?.isActive, true, 'tide 9 is ongoing');
  assert.equal(overridden.epoch?.endTime, undefined, 'tide 9 has not ended');
  assert.equal(overridden.state?.currentEpochNumber, 9n);
  assert.equal(overridden.state?.isActive, true);

  // Accrual matches the control tide exactly, and is measured from 05:00 - five
  // days of 1000 USD supplied and 500 USD borrowed.
  assert.ok(overridden.stats, 'tide 9 accrued user stats');
  assert.equal(overridden.stats?.depositPoints, control.stats?.depositPoints);
  assert.equal(overridden.stats?.borrowPoints, control.stats?.borrowPoints);
  assert.equal(overridden.stats?.totalPoints, control.stats?.totalPoints);

  const deposit = Number(overridden.stats?.depositPoints ?? 0n) / 1e18;
  const borrow = Number(overridden.stats?.borrowPoints ?? 0n) / 1e18;
  assert.ok(Math.abs(deposit - 5000) < 1e-6, `deposit points ${deposit} != 5000`);
  assert.ok(Math.abs(borrow - 5000) < 1e-6, `borrow points ${borrow} != 5000`);
});
