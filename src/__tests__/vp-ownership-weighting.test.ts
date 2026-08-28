import assert from 'node:assert/strict';
import { test } from 'node:test';

import { createDefaultReserve } from '../helpers/entityHelpers';
import { LEADERBOARD_START_BLOCK, VP_OWNERSHIP_WEIGHTING_FROM } from '../helpers/constants';
import { TestHelpers, type MockDb } from './v3-test-helpers';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

const DAY = 86400;
const RAY = 10n ** 27n;
const DECIMALS = 6;
const UNIT = 10n ** 6n;
const VP_UNIT = 10n ** 18n;

// Epoch 9's window, which is where the weighting switches on.
const EPOCH_START = VP_OWNERSHIP_WEIGHTING_FROM;
const SETTLE_AT = EPOCH_START + 10 * DAY;
const MIDPOINT = EPOCH_START + 5 * DAY;
const EPOCH_END = EPOCH_START + 30 * DAY;

const ADDRESSES = {
  holder: '0x0000000000000000000000000000000000000a01',
  recipient: '0x0000000000000000000000000000000000000a02',
  asset: '0x00000000000000000000000000000000000000a1',
  pool: '0x00000000000000000000000000000000000000b1',
  aToken: '0x00000000000000000000000000000000000000c1',
  vToken: '0x00000000000000000000000000000000000000d1',
  leaderboardConfig: '0x00000000000000000000000000000000000000f1',
};

function eventDataFactory(seed: number) {
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

/**
 * One user holding a permanent veDUST lock across the window, settled at
 * SETTLE_AT. `acquiredAt` is when the ownership row says they took the token;
 * `undefined` means no row at all.
 */
async function runHolder(
  user: string,
  tokenId: bigint,
  acquiredAt: number | undefined,
  withTiers = false
) {
  const eventData = eventDataFactory(Number(tokenId) * 1000);
  const baseBlock = LEADERBOARD_START_BLOCK + 100;
  let mockDb: MockDb = TestHelpers.MockDb.createMockDb();

  const reserveId = `${ADDRESSES.asset}-${ADDRESSES.pool}`;
  mockDb = mockDb.entities.Protocol.set({ id: '1' });
  mockDb = mockDb.entities.Pool.set({
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
    lastUpdateTimestamp: EPOCH_START,
  });
  mockDb = mockDb.entities.Reserve.set({
    ...createDefaultReserve(reserveId, ADDRESSES.pool, ADDRESSES.asset),
    decimals: DECIMALS,
    liquidityIndex: RAY,
    variableBorrowIndex: RAY,
    liquidityRate: 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: EPOCH_START,
    isActive: true,
    borrowingEnabled: true,
  });
  mockDb = mockDb.entities.SubToken.set({
    id: ADDRESSES.aToken,
    pool_id: ADDRESSES.pool,
    tokenContractImpl: undefined,
    underlyingAssetAddress: ADDRESSES.asset,
    underlyingAssetDecimals: DECIMALS,
  });
  mockDb = mockDb.entities.PriceOracleAsset.set({
    id: ADDRESSES.asset,
    oracle_id: '',
    priceSource: '',
    dependentAssets: [],
    priceType: '',
    platform: '',
    priceInEth: 100000000n,
    isFallbackRequired: false,
    lastUpdateTimestamp: EPOCH_START,
    priceCacheExpiry: 0,
    fromChainlinkSourcesRegistry: false,
    lastPriceUsd: 1,
    cumulativeUsdPriceHours: 0,
    resetTimestamp: 0,
    resetCumulativeUsdPriceHours: 0,
  });

  // Epoch 9 is live.
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 9n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '9',
    epochNumber: 9n,
    startBlock: BigInt(baseBlock - 10),
    startTime: EPOCH_START,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: EPOCH_START,
    scheduledEndTime: EPOCH_END,
  });

  // A permanent lock, so voting power is constant across the window and the
  // arithmetic is exact.
  mockDb = mockDb.entities.DustLockToken.set({
    id: tokenId.toString(),
    owner: user,
    lockedAmount: 1000n * VP_UNIT,
    end: 0,
    isPermanent: true,
    createdAt: EPOCH_START,
    updatedAt: EPOCH_START,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  mockDb = mockDb.entities.UserTokenList.set({
    id: user,
    user_id: user,
    tokenIds: [tokenId],
    lastUpdate: EPOCH_START,
  });
  if (acquiredAt !== undefined) {
    mockDb = mockDb.entities.UserTokenOwnership.set({
      id: `${user}:${tokenId}`,
      user_id: user,
      tokenId,
      acquiredAt,
    });
  }

  mockDb = await TestHelpers.LeaderboardConfig.ConfigSnapshot.processEvent({
    event: TestHelpers.LeaderboardConfig.ConfigSnapshot.createMockEvent({
      depositRateBps: 0n,
      borrowRateBps: 0n,
      vpRateBps: 10000n,
      supplyDailyBonus: 0n,
      borrowDailyBonus: 0n,
      repayDailyBonus: 0n,
      withdrawDailyBonus: 0n,
      cooldownSeconds: 0n,
      minDailyBonusUsd: 0n,
      timestamp: BigInt(EPOCH_START),
      ...eventData(baseBlock - 1, EPOCH_START, ADDRESSES.leaderboardConfig),
    }),
    mockDb,
  });

  if (withTiers) {
    // Two tiers, with the threshold between the full and the halved average, so
    // a segment that blends the two lands in the lower one.
    for (const [tierIndex, minVotingPower, multiplierBps] of [
      [0n, 0n, 10000n],
      [1n, 800n * VP_UNIT, 20000n],
    ] as Array<[bigint, bigint, bigint]>) {
      mockDb = await TestHelpers.VotingPowerMultiplier.TierAdded.processEvent({
        event: TestHelpers.VotingPowerMultiplier.TierAdded.createMockEvent({
          tierIndex,
          minVotingPower,
          multiplierBps,
          ...eventData(baseBlock, EPOCH_START, ADDRESSES.leaderboardConfig),
        }),
        mockDb,
      });
    }
  }

  // Any activity settles the user, which is what runs VP accrual.
  mockDb = await TestHelpers.AToken.Mint.processEvent({
    event: TestHelpers.AToken.Mint.createMockEvent({
      caller: user,
      onBehalfOf: user,
      value: 1000n * UNIT,
      balanceIncrease: 0n,
      index: RAY,
      ...eventData(baseBlock + 100, SETTLE_AT, ADDRESSES.aToken),
    }),
    mockDb,
  });

  return mockDb.entities.UserEpochStats.get(`${user}:9`);
}

test('a token held all window earns full VP; one acquired midway earns half', async () => {
  const wholeWindow = await runHolder(ADDRESSES.holder, 1n, EPOCH_START);
  const halfWindow = await runHolder(ADDRESSES.recipient, 2n, MIDPOINT);

  const full = Number(wholeWindow?.vpPointsWithMultiplier ?? 0n);
  const half = Number(halfWindow?.vpPointsWithMultiplier ?? 0n);

  assert.ok(full > 0, 'the long-term holder earns VP points');
  assert.ok(half > 0, 'the mid-window recipient still earns for the time they held it');

  // Acquired at the midpoint of a 10-day window => half the credit, not all of
  // it. Before this fix `half` equalled `full`.
  const ratio = half / full;
  assert.ok(Math.abs(ratio - 0.5) < 0.01, `expected ~0.5 of the full credit, got ${ratio}`);
});

test('a token acquired before the window is not penalized', async () => {
  const early = await runHolder(ADDRESSES.holder, 3n, EPOCH_START - 30 * DAY);
  const noRecord = await runHolder(ADDRESSES.holder, 4n, undefined);

  assert.equal(
    early?.vpPointsWithMultiplier,
    noRecord?.vpPointsWithMultiplier,
    'an ownership row predating the window scores the same as no row at all'
  );
  assert.ok(Number(early?.vpPointsWithMultiplier ?? 0n) > 0);
});

test('a mid-window acquisition splits the multiplier segment', async () => {
  // 1000 VP held all window clears the 800 VP tier; the same token acquired at
  // the midpoint averages 500 over the window and does not. Without splitting
  // the segment at acquisition the recipient would be scored at the low tier
  // for the entire window, including the half they genuinely held it.
  const fullHolder = await runHolder(ADDRESSES.holder, 5n, EPOCH_START, true);
  const midAcquirer = await runHolder(ADDRESSES.recipient, 6n, MIDPOINT, true);

  const full = Number(fullHolder?.vpPointsWithMultiplier ?? 0n);
  const mid = Number(midAcquirer?.vpPointsWithMultiplier ?? 0n);

  assert.ok(full > 0 && mid > 0, 'both holders earn VP points');

  // Expected ratio is exactly 0.375:
  //   base points - 5 days of 1000 VP against 10 days       => 0.5
  //   multiplier  - [start, mid] holds nothing (1x), [mid, end]
  //                 holds 1000 VP (2x), time-weighted       => 1.5x vs 2x = 0.75
  //   0.5 * 0.75                                            => 0.375
  //
  // Without splitting at acquisition the window blends to 500 VP, misses the
  // 800 VP tier entirely and scores 1x throughout, giving 0.25. The remaining
  // gap to a "pure" 0.5 is not this fix: the combined multiplier is weighted by
  // time across the window rather than by where points were actually earned,
  // which is pre-existing behaviour for every user.
  const ratio = mid / full;
  assert.ok(ratio < 0.5, `a half-window holder cannot match a full one (got ${ratio})`);
  assert.ok(ratio > 0.25, `the held half must carry its own tier, not the blended one (${ratio})`);
  assert.ok(Math.abs(ratio - 0.375) < 0.01, `expected ~0.375, got ${ratio}`);
});
