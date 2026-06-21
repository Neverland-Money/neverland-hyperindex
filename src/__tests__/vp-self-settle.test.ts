import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TestHelpers } from './v3-test-helpers';
import { LEADERBOARD_START_BLOCK, MAX_LOCK_TIME } from '../helpers/constants';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';

// These tests verify Part A of the VP-points fix: veDUST mutate handlers
// self-settle the owner's accrued voting-power points BEFORE mutating the lock,
// so the time-integral awards the decaying slope (including all the way to 0)
// independent of the keeper / backfill gate. The governing rule under test:
// NEVER gate on instantaneous VP — settle before mutate; let the integral decide.

const USER = '0x000000000000000000000000000000000000c001';
const USER2 = '0x000000000000000000000000000000000000c002';
const SRC = '0x000000000000000000000000000000000000c0ff';

const ONE = 10n ** 18n;
const T0 = 1767434400; // epoch-1 start
const YEAR = Number(MAX_LOCK_TIME); // 31_536_000
const BLOCK = Number(LEADERBOARD_START_BLOCK) + 1000;

function eventDataFactory() {
  let counter = 1;
  return (blockNumber: number, timestamp: number) => {
    const txHash = `0x${counter.toString(16).padStart(64, '0')}`;
    const mockEventData = {
      block: { number: blockNumber, timestamp },
      logIndex: counter,
      srcAddress: SRC,
      transaction: { hash: txHash },
    };
    counter += 1;
    return { mockEventData };
  };
}

// Seed the minimal leaderboard state needed for VP accrual: an active epoch and a
// config with a non-zero VP rate. Returns the mockDb.
function seedLeaderboard(mockDb: ReturnType<typeof TestHelpers.MockDb.createMockDb>) {
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
    vpRateBps: 10000n, // 1.0 / day
    lpRateBps: 0n,
    supplyDailyBonus: 0,
    borrowDailyBonus: 0,
    repayDailyBonus: 0,
    withdrawDailyBonus: 0,
    cooldownSeconds: 0,
    minDailyBonusUsd: 0,
    lastUpdate: 0,
  });
  return mockDb;
}

function seedDecayingLock(
  mockDb: ReturnType<typeof TestHelpers.MockDb.createMockDb>,
  owner: string,
  tokenId: bigint,
  amount: bigint,
  end: number
) {
  mockDb = mockDb.entities.DustLockToken.set({
    id: tokenId.toString(),
    owner,
    lockedAmount: amount,
    end,
    isPermanent: false,
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
  return mockDb;
}

test('Withdraw self-settles the pre-withdraw decaying VP slope', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seedLeaderboard(mockDb);
  mockDb = seedDecayingLock(mockDb, USER, 1n, 1000n * ONE, T0 + YEAR);
  const ev = eventDataFactory();

  const withdraw = TestHelpers.DustLock.Withdraw.createMockEvent({
    provider: USER,
    tokenId: 1n,
    value: 1000n * ONE,
    ...ev(BLOCK, T0 + 86_400), // one day into the lock
  });
  mockDb = await TestHelpers.DustLock.Withdraw.processEvent({ event: withdraw, mockDb });

  const stats = mockDb.entities.UserEpochStats.get(`${USER}:1`);
  assert.ok(stats, 'withdraw created the epoch stats row via self-settle');
  assert.ok(
    stats.vpPointsWithMultiplier > 0n,
    'the one-day VP slope was accrued before the lock was drained'
  );
});

test('Withdraw after full decay credits the same as withdraw exactly at decay end (zero tail, no over-credit)', async () => {
  const decayEnd = T0 + 86_400; // lock fully decays to 0 after one day

  // Run A: withdraw long AFTER full decay.
  let dbA = TestHelpers.MockDb.createMockDb();
  dbA = seedLeaderboard(dbA);
  dbA = seedDecayingLock(dbA, USER, 1n, 1000n * ONE, decayEnd);
  const evA = eventDataFactory();
  const wA = TestHelpers.DustLock.Withdraw.createMockEvent({
    provider: USER,
    tokenId: 1n,
    value: 1000n * ONE,
    ...evA(BLOCK, T0 + 200_000), // well past decayEnd
  });
  dbA = await TestHelpers.DustLock.Withdraw.processEvent({ event: wA, mockDb: dbA });
  const pointsA = dbA.entities.UserEpochStats.get(`${USER}:1`)?.vpPointsWithMultiplier ?? 0n;

  // Run B: withdraw exactly AT decay end.
  let dbB = TestHelpers.MockDb.createMockDb();
  dbB = seedLeaderboard(dbB);
  dbB = seedDecayingLock(dbB, USER, 1n, 1000n * ONE, decayEnd);
  const evB = eventDataFactory();
  const wB = TestHelpers.DustLock.Withdraw.createMockEvent({
    provider: USER,
    tokenId: 1n,
    value: 1000n * ONE,
    ...evB(BLOCK, decayEnd),
  });
  dbB = await TestHelpers.DustLock.Withdraw.processEvent({ event: wB, mockDb: dbB });
  const pointsB = dbB.entities.UserEpochStats.get(`${USER}:1`)?.vpPointsWithMultiplier ?? 0n;

  assert.ok(pointsA > 0n, 'the decaying slope up to full decay was credited');
  // A real over-credit for the dead tail [end, withdraw] would be on the order of
  // the active points themselves (the window is ~2.3x longer). The two paths only
  // differ by float-rounding noise (the accrual goes through Number()+Math.floor),
  // so require equality to within a few wei on a ~1.37e18 value.
  const diff = pointsA > pointsB ? pointsA - pointsB : pointsB - pointsA;
  assert.ok(
    diff <= 1_000n,
    `no points for the dead tail beyond the lock end — the integral follows the slope to 0 (diff=${diff} on ${pointsB})`
  );
});

test('Transfer settles the FROM owner (outgoing VP) in the correct epoch', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seedLeaderboard(mockDb);
  mockDb = seedDecayingLock(mockDb, USER, 1n, 1000n * ONE, T0 + YEAR);
  const ev = eventDataFactory();

  const transfer = TestHelpers.DustLock.Transfer.createMockEvent({
    from: USER,
    to: USER2,
    tokenId: 1n,
    ...ev(BLOCK, T0 + 86_400),
  });
  mockDb = await TestHelpers.DustLock.Transfer.processEvent({ event: transfer, mockDb });

  const fromStats = mockDb.entities.UserEpochStats.get(`${USER}:1`);
  assert.ok(fromStats, 'the FROM owner got an epoch stats row');
  assert.ok(
    fromStats.vpPointsWithMultiplier > 0n,
    'the FROM owner accrued the VP they held up to the transfer'
  );

  // the receiver accrues 0 for the just-received token (its accrual starts now)
  const toStats = mockDb.entities.UserEpochStats.get(`${USER2}:1`);
  const toPoints = toStats?.vpPointsWithMultiplier ?? 0n;
  assert.equal(toPoints, 0n, 'the receiver is not credited for time before it held the token');
});

test('self-settle is idempotent: a second withdraw at the same time adds no points', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = seedLeaderboard(mockDb);
  mockDb = seedDecayingLock(mockDb, USER, 1n, 1000n * ONE, T0 + YEAR);
  const ev = eventDataFactory();

  const w1 = TestHelpers.DustLock.Withdraw.createMockEvent({
    provider: USER,
    tokenId: 1n,
    value: 500n * ONE,
    ...ev(BLOCK, T0 + 86_400),
  });
  mockDb = await TestHelpers.DustLock.Withdraw.processEvent({ event: w1, mockDb });
  const after1 = mockDb.entities.UserEpochStats.get(`${USER}:1`)?.vpPointsWithMultiplier ?? 0n;
  assert.ok(after1 > 0n, 'first settle accrued points');

  // second withdraw at the SAME timestamp: the accrual cursor is already at this
  // time, so the window is empty and no further points are added.
  const w2 = TestHelpers.DustLock.Withdraw.createMockEvent({
    provider: USER,
    tokenId: 1n,
    value: 500n * ONE,
    ...ev(BLOCK, T0 + 86_400),
  });
  mockDb = await TestHelpers.DustLock.Withdraw.processEvent({ event: w2, mockDb });
  const after2 = mockDb.entities.UserEpochStats.get(`${USER}:1`)?.vpPointsWithMultiplier ?? 0n;

  assert.equal(after2, after1, 'a repeat settle in the same instant is a no-op (no double-count)');
});
