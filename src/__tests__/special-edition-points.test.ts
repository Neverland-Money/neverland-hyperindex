import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TestHelpers, type MockDb } from './v3-test-helpers';
import { LEADERBOARD_START_BLOCK } from '../helpers/constants';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';
process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = '';

// End-to-end proof that the special-edition multiplier BOOSTS accrued points, the
// same additive-join way an NFT-collection multiplier does. Two identical pure-VP
// users (same permanent veDUST lock => flat VP, same accrual window) are settled by
// the keeper; one holds a SHINY special edition (perTokenBoostBps=500 => 1.05x), the
// other holds none. The member must accrue exactly 1.05x the non-member's VP points.

const MEMBER = '0x000000000000000000000000000000000000d001';
const BASE = '0x000000000000000000000000000000000000d002';
const KEEPER = '0x000000000000000000000000000000000000d0ff';
const ONE = 10n ** 18n;
const T0 = 1767434400;
const BLOCK = Number(LEADERBOARD_START_BLOCK) + 1000;
const BOOST_BPS = 500n; // SHINY per-token boost => multiplier 10000 + 500 = 10500 (1.05x)

function ev(counter: number, blockNumber: number, timestamp: number) {
  return {
    mockEventData: {
      block: { number: blockNumber, timestamp },
      logIndex: counter,
      srcAddress: KEEPER,
      transaction: { hash: `0x${counter.toString(16).padStart(64, '0')}` },
    },
  };
}

function seed(mockDb: MockDb) {
  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: 0n,
    startTime: T0,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: T0,
    scheduledEndTime: 0,
  });
  mockDb = mockDb.entities.LeaderboardConfig.set({
    id: 'global',
    depositRateBps: 0n,
    borrowRateBps: 0n,
    vpRateBps: 10000n,
    lpRateBps: 0n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 0,
  });
  // identical permanent veDUST lock (flat VP) for both users
  for (const [owner, tokenId] of [
    [MEMBER, 9001n],
    [BASE, 9002n],
  ]) {
    mockDb = mockDb.entities.DustLockToken.set({
      id: tokenId.toString(),
      owner,
      lockedAmount: 1000n * ONE,
      end: 0,
      isPermanent: true,
      createdAt: T0,
      updatedAt: T0,
      lastDepositType: undefined,
      selfRepayEnabled: false,
      rewardReceiver: undefined,
    });
    mockDb = mockDb.entities.UserTokenList.set({
      id: owner,
      user_id: owner,
      tokenIds: [tokenId],
      lastUpdate: T0,
    });
  }
  // SHINY special edition, held only by MEMBER (count 1 from the start of the window)
  mockDb = mockDb.entities.SpecialEditionRegistryState.set({
    id: 'current',
    editionIds: [1n],
    lastUpdate: T0,
  });
  mockDb = mockDb.entities.SpecialEditionConfig.set({
    id: '1',
    editionId: 1n,
    key: 'SHINY',
    name: 'Shiny veDUST',
    perTokenBoostBps: BOOST_BPS,
    enabled: true,
    exists: true,
    createdAt: T0,
    updatedAt: T0,
    changeTimestamps: [T0],
    boostBpsHistory: [BOOST_BPS],
    enabledHistory: [1n],
  });
  mockDb = mockDb.entities.UserSpecialEditionState.set({
    id: `${MEMBER}:1`,
    user_id: MEMBER,
    editionId: 1n,
    tokenCount: 1n,
    countTimestamps: [T0],
    tokenCountHistory: [1n],
    updatedAt: T0,
  });
  return mockDb;
}

async function settleTwice(mockDb: MockDb, user: string, c: number) {
  // first settle establishes the VP accrual cursor; second accrues over [T0, T0+1 day]
  const s0 = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user,
    timestamp: BigInt(T0),
    ...ev(c, BLOCK, T0),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: s0, mockDb });
  const t1 = T0 + 86_400;
  const s1 = TestHelpers.LeaderboardKeeper.UserSettled.createMockEvent({
    user,
    timestamp: BigInt(t1),
    ...ev(c + 1, BLOCK + 1, t1),
  });
  mockDb = await TestHelpers.LeaderboardKeeper.UserSettled.processEvent({ event: s1, mockDb });
  return mockDb;
}

test('special-edition membership boosts accrued VP points by its multiplier (like an NFT collection)', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seed(mockDb);
  mockDb = await settleTwice(mockDb, MEMBER, 10);
  mockDb = await settleTwice(mockDb, BASE, 20);

  const member = mockDb.entities.UserEpochStats.get(`${MEMBER}:1`);
  const base = mockDb.entities.UserEpochStats.get(`${BASE}:1`);
  assert.ok(member && base, 'both users accrued epoch stats');
  const mp = member.vpPointsWithMultiplier;
  const bp = base.vpPointsWithMultiplier;
  assert.ok(bp > 0n, 'baseline (no special edition) accrued VP points');
  assert.ok(mp > 0n, 'member accrued VP points');

  // The special-edition holder accrues exactly the SHINY multiplier more. The boost is
  // applied as floor(rawPoints * 10500 / 10000) (applyCombinedMultiplierScaled), and the
  // non-member's points are the un-floored raw (* 10000 / 10000 === raw), so the member's
  // points equal the non-member's run through the same integer-floored 1.05x. Verified
  // with exact bigint arithmetic — no float ratio, no precision loss.
  assert.equal(
    mp,
    (bp * 10500n) / 10000n,
    `special-edition member should accrue floor(1.05x) of the non-member (member=${mp}, base=${bp})`
  );

  // And the multiplier must be reflected on the leaderboard state, exactly like NFT.
  const memberState = mockDb.entities.UserLeaderboardState.get(MEMBER);
  assert.equal(
    memberState?.specialEditionMultiplier,
    10500n,
    'member specialEditionMultiplier = 10000 + 1*500'
  );
  // additive join with neutral nft/vp: 1x + (se 1.05x - 1x) = 1.05x.
  assert.equal(
    memberState?.combinedMultiplier,
    10500n,
    'combined join = nft(1x) + se(+5%) + vp(1x)'
  );
  const baseState = mockDb.entities.UserLeaderboardState.get(BASE);
  assert.equal(
    baseState?.specialEditionMultiplier,
    10000n,
    'non-member specialEditionMultiplier stays neutral 1x'
  );
});
