import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TestHelpers } from './v3-test-helpers';

import {
  AUSD_ADDRESS,
  DUST_LOCK_START_BLOCK,
  LEADERBOARD_START_BLOCK,
  ZERO_ADDRESS,
} from '../helpers/constants';
import { LP_GROWTH_Q128 } from '../helpers/lpGrowthMath';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';

const ADDRESSES = {
  dustLock: '0x000000000000000000000000000000000000a001',
  user: '0x000000000000000000000000000000000000a002',
  userTwo: '0x000000000000000000000000000000000000a003',
  treasury: '0x000000000000000000000000000000000000a004',
  lpPool: '0x000000000000000000000000000000000000a005',
  lpToken: '0x000000000000000000000000000000000000a006',
};

const LP_POSITION_ID = `v2:${ADDRESSES.lpPool}:${ADDRESSES.user}`;
const ONE_POINT_GROWTH_X128 = LP_GROWTH_Q128 * 100_000_000n * 10_000n * 86_400n;

function loadTestHelpers() {
  return TestHelpers;
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

test('dust lock lifecycle events update tokens and voting power', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const startBlock = DUST_LOCK_START_BLOCK + 1;

  mockDb = mockDb.entities.UserTokenList.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    tokenIds: [1n],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.DustLockToken.set({
    id: '1',
    owner: ADDRESSES.user,
    lockedAmount: 100n,
    end: 4000,
    isPermanent: false,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  mockDb = mockDb.entities.DustLockToken.set({
    id: '2',
    owner: ADDRESSES.userTwo,
    lockedAmount: 0n,
    end: 0,
    isPermanent: false,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });

  const deposit = TestHelpers.DustLock.Deposit.createMockEvent({
    provider: ADDRESSES.user,
    tokenId: 1n,
    value: 10n,
    locktime: 5000n,
    depositType: 1n,
    ...eventData(startBlock, 1000, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Deposit.processEvent({ event: deposit, mockDb });

  const depositNoList = TestHelpers.DustLock.Deposit.createMockEvent({
    provider: ADDRESSES.userTwo,
    tokenId: 2n,
    value: 5n,
    locktime: 6000n,
    depositType: 2n,
    ...eventData(startBlock + 1, 1010, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Deposit.processEvent({ event: depositNoList, mockDb });

  const withdraw = TestHelpers.DustLock.Withdraw.createMockEvent({
    provider: ADDRESSES.user,
    tokenId: 1n,
    value: 3n,
    ...eventData(startBlock + 2, 1020, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Withdraw.processEvent({ event: withdraw, mockDb });

  const earlyWithdraw = TestHelpers.DustLock.EarlyWithdraw.createMockEvent({
    provider: ADDRESSES.user,
    tokenId: 1n,
    value: 5n,
    amountReturned: 2n,
    ...eventData(startBlock + 3, 1030, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.EarlyWithdraw.processEvent({ event: earlyWithdraw, mockDb });

  const lockPermanent = TestHelpers.DustLock.LockPermanent.createMockEvent({
    tokenId: 1n,
    amount: 50n,
    ...eventData(startBlock + 4, 1040, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.LockPermanent.processEvent({ event: lockPermanent, mockDb });

  const unlockPermanent = TestHelpers.DustLock.UnlockPermanent.createMockEvent({
    tokenId: 1n,
    amount: 47n,
    ts: 7000n,
    ...eventData(startBlock + 5, 1050, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.UnlockPermanent.processEvent({
    event: unlockPermanent,
    mockDb,
  });

  const supply = TestHelpers.DustLock.Supply.createMockEvent({
    prevSupply: 100n,
    supply: 120n,
    ...eventData(startBlock + 6, 1060, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Supply.processEvent({ event: supply, mockDb });

  const merge = TestHelpers.DustLock.Merge.createMockEvent({
    sender: ADDRESSES.user,
    from: 1n,
    to: 2n,
    amountFrom: 10n,
    amountTo: 5n,
    amountFinal: 15n,
    locktime: 8000n,
    ...eventData(startBlock + 7, 1070, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Merge.processEvent({ event: merge, mockDb });

  const split = TestHelpers.DustLock.Split.createMockEvent({
    sender: ADDRESSES.user,
    from: 2n,
    tokenId1: 3n,
    tokenId2: 4n,
    splitAmount1: 4n,
    splitAmount2: 6n,
    locktime: 9000n,
    ...eventData(startBlock + 8, 1080, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Split.processEvent({ event: split, mockDb });

  mockDb = mockDb.entities.UserTokenList.set({
    id: ADDRESSES.userTwo,
    user_id: ADDRESSES.userTwo,
    tokenIds: [4n],
    lastUpdate: 0,
  });

  const transfer = TestHelpers.DustLock.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ADDRESSES.userTwo,
    tokenId: 4n,
    ...eventData(startBlock + 9, 1090, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Transfer.processEvent({ event: transfer, mockDb });

  const token = mockDb.entities.DustLockToken.get('1');
  assert.ok(token);
  assert.ok(mockDb.entities.DustLockDeposit.get(`${deposit.transaction.hash}-${deposit.logIndex}`));
  assert.ok(
    mockDb.entities.DustLockWithdraw.get(`${withdraw.transaction.hash}-${withdraw.logIndex}`)
  );
  assert.ok(
    mockDb.entities.DustLockEarlyWithdraw.get(
      `${earlyWithdraw.transaction.hash}-${earlyWithdraw.logIndex}`
    )
  );
  assert.ok(
    mockDb.entities.DustLockPermanentLock.get(
      `${lockPermanent.transaction.hash}-${lockPermanent.logIndex}`
    )
  );
  assert.ok(
    mockDb.entities.DustLockPermanentUnlock.get(
      `${unlockPermanent.transaction.hash}-${unlockPermanent.logIndex}`
    )
  );
  assert.ok(
    mockDb.entities.DustLockSupplyHistory.get(`${supply.transaction.hash}-${supply.logIndex}`)
  );
  assert.ok(mockDb.entities.DustLockMerge.get(`${merge.transaction.hash}-${merge.logIndex}`));
  assert.ok(mockDb.entities.DustLockSplit.get(`${split.transaction.hash}-${split.logIndex}`));

  const tokenList = mockDb.entities.UserTokenList.get(ADDRESSES.userTwo);
  assert.equal(tokenList?.tokenIds.length, 1);
});

test('generated DustLock deposit consumes LP growth before mutating the owned lock', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();
  const timestamp = 1000;
  const blockNumber = LEADERBOARD_START_BLOCK + 100;

  mockDb = mockDb.entities.LeaderboardState.set({
    id: 'current',
    currentEpochNumber: 1n,
    isActive: true,
  });
  mockDb = mockDb.entities.LeaderboardEpoch.set({
    id: '1',
    epochNumber: 1n,
    startBlock: BigInt(DUST_LOCK_START_BLOCK),
    startTime: 900,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 900,
    scheduledEndTime: 2000,
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
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.UserTokenList.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    tokenIds: [1n],
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.DustLockToken.set({
    id: '1',
    owner: ADDRESSES.user,
    lockedAmount: 100n,
    end: 5000,
    isPermanent: false,
    createdAt: 800,
    updatedAt: 900,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  mockDb = mockDb.entities.LPPoolRegistry.set({
    id: 'global',
    poolIds: [ADDRESSES.lpPool],
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.LPPoolConfig.set({
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    positionManager: ADDRESSES.lpPool,
    token0: AUSD_ADDRESS,
    token1: ADDRESSES.lpToken,
    fee: 3000,
    lpRateBps: 0n,
    isActive: true,
    enabledAtEpoch: 1n,
    enabledAtTimestamp: 900,
    disabledAtEpoch: undefined,
    disabledAtTimestamp: undefined,
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.LPPoolState.set({
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    currentTick: 0,
    sqrtPriceX96: 1n << 96n,
    token0Price: 100_000_000n,
    token1Price: 100_000_000n,
    feeProtocol0: 0,
    feeProtocol1: 0,
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.LPPoolV2State.set({
    id: ADDRESSES.lpPool,
    pool: ADDRESSES.lpPool,
    reserve0: 1n,
    reserve1: 1n,
    lpTotalSupply: 1n,
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.LPPoolEpochGrowth.set({
    id: `${ADDRESSES.lpPool}:1`,
    pool: ADDRESSES.lpPool,
    epochNumber: 1n,
    startTimestamp: 900,
    lastTimestamp: timestamp,
    scalarGrowthX128: ONE_POINT_GROWTH_X128,
    isFrozen: false,
    frozenAt: undefined,
    lastUpdate: timestamp,
  });
  mockDb = mockDb.entities.UserLPPosition.set({
    id: LP_POSITION_ID,
    tokenId: 0n,
    user_id: ADDRESSES.user,
    pool: ADDRESSES.lpPool,
    positionManager: ADDRESSES.lpPool,
    tickLower: -887272,
    tickUpper: 887272,
    liquidity: 1n,
    amount0: 1n,
    amount1: 1n,
    isInRange: true,
    valueUsd: 200_000_000n,
    lastInRangeTimestamp: 900,
    accumulatedInRangeSeconds: 0n,
    lastSettledAt: 900,
    settledLpPoints: 0n,
    createdAt: 899,
    lastUpdate: 900,
  });
  mockDb = mockDb.entities.UserLPPositionIndex.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    positionIds: [LP_POSITION_ID],
    lastUpdate: 900,
  });

  const deposit = TestHelpers.DustLock.Deposit.createMockEvent({
    provider: ADDRESSES.user,
    tokenId: 1n,
    value: 10n,
    locktime: 6000n,
    depositType: 1n,
    mockEventData: {
      block: { number: blockNumber, timestamp },
      logIndex: 77,
      srcAddress: ADDRESSES.dustLock,
      transaction: { hash: `0x${'cd'.repeat(32)}` },
    },
  });
  mockDb = await TestHelpers.DustLock.Deposit.processEvent({ event: deposit, mockDb });

  const cursor = mockDb.entities.UserLPEpochCursor.get(`${LP_POSITION_ID}:1`);
  const stats = mockDb.entities.UserEpochStats.get(`${ADDRESSES.user}:1`);
  const token = mockDb.entities.DustLockToken.get('1');
  assert.equal(cursor?.lastSettledAt, timestamp);
  assert.equal(cursor?.growthBaselineX128, ONE_POINT_GROWTH_X128);
  assert.equal(stats?.lpPoints, 1_000_000_000_000_000_000n);
  assert.equal(token?.lockedAmount, 110n);
});

test('dust lock admin events emit audit records', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const startBlock = DUST_LOCK_START_BLOCK + 100;

  const penalty = TestHelpers.DustLock.EarlyWithdrawPenaltyUpdated.createMockEvent({
    oldPenalty: 1n,
    newPenalty: 2n,
    ...eventData(startBlock, 1200, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.EarlyWithdrawPenaltyUpdated.processEvent({
    event: penalty,
    mockDb,
  });

  const treasury = TestHelpers.DustLock.EarlyWithdrawTreasuryUpdated.createMockEvent({
    oldTreasury: ADDRESSES.user,
    newTreasury: ADDRESSES.treasury,
    ...eventData(startBlock + 1, 1210, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.EarlyWithdrawTreasuryUpdated.processEvent({
    event: treasury,
    mockDb,
  });

  const minLock = TestHelpers.DustLock.MinLockAmountUpdated.createMockEvent({
    oldAmount: 10n,
    newAmount: 12n,
    ...eventData(startBlock + 2, 1220, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.MinLockAmountUpdated.processEvent({
    event: minLock,
    mockDb,
  });

  const revenue = TestHelpers.DustLock.RevenueRewardUpdated.createMockEvent({
    oldReward: ADDRESSES.user,
    newReward: ADDRESSES.treasury,
    ...eventData(startBlock + 3, 1230, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.RevenueRewardUpdated.processEvent({
    event: revenue,
    mockDb,
  });

  const splitPermission = TestHelpers.DustLock.SplitPermissionUpdated.createMockEvent({
    account: ADDRESSES.user,
    allowed: true,
    ...eventData(startBlock + 4, 1240, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.SplitPermissionUpdated.processEvent({
    event: splitPermission,
    mockDb,
  });

  const teamProposed = TestHelpers.DustLock.TeamProposed.createMockEvent({
    currentTeam: ADDRESSES.user,
    proposedTeam: ADDRESSES.userTwo,
    ...eventData(startBlock + 5, 1250, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.TeamProposed.processEvent({
    event: teamProposed,
    mockDb,
  });

  const teamAccepted = TestHelpers.DustLock.TeamAccepted.createMockEvent({
    oldTeam: ADDRESSES.user,
    newTeam: ADDRESSES.userTwo,
    ...eventData(startBlock + 6, 1260, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.TeamAccepted.processEvent({
    event: teamAccepted,
    mockDb,
  });

  const teamCancelled = TestHelpers.DustLock.TeamProposalCancelled.createMockEvent({
    currentTeam: ADDRESSES.user,
    cancelledTeam: ADDRESSES.userTwo,
    ...eventData(startBlock + 7, 1270, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.TeamProposalCancelled.processEvent({
    event: teamCancelled,
    mockDb,
  });

  const baseUri = TestHelpers.DustLock.BaseURIUpdated.createMockEvent({
    oldBaseURI: 'old',
    newBaseURI: 'new',
    ...eventData(startBlock + 8, 1280, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.BaseURIUpdated.processEvent({
    event: baseUri,
    mockDb,
  });

  const metadata = TestHelpers.DustLock.MetadataUpdate.createMockEvent({
    _tokenId: 1n,
    ...eventData(startBlock + 9, 1290, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.MetadataUpdate.processEvent({
    event: metadata,
    mockDb,
  });

  assert.ok(
    mockDb.entities.DustLockAdminEvent.get(`${penalty.transaction.hash}-${penalty.logIndex}`)
  );
  assert.ok(
    mockDb.entities.DustLockAdminEvent.get(`${metadata.transaction.hash}-${metadata.logIndex}`)
  );
});

test('split recalculates voting power when owner exists', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const startBlock = DUST_LOCK_START_BLOCK + 10;

  mockDb = mockDb.entities.UserTokenList.set({
    id: ADDRESSES.user,
    user_id: ADDRESSES.user,
    tokenIds: [3n],
    lastUpdate: 0,
  });
  mockDb = mockDb.entities.DustLockToken.set({
    id: '3',
    owner: ADDRESSES.user,
    lockedAmount: 100n,
    end: 4000,
    isPermanent: false,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });

  const split = TestHelpers.DustLock.Split.createMockEvent({
    sender: ADDRESSES.user,
    from: 3n,
    tokenId1: 3n,
    tokenId2: 4n,
    splitAmount1: 40n,
    splitAmount2: 60n,
    locktime: 9000n,
    ...eventData(startBlock, 1200, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Split.processEvent({ event: split, mockDb });

  const state = mockDb.entities.UserLeaderboardState.get(ADDRESSES.user);
  assert.ok(state);
});

test('withdrawals clamp balances and burns clear ownership', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const startBlock = DUST_LOCK_START_BLOCK + 20;

  mockDb = mockDb.entities.DustLockToken.set({
    id: '5',
    owner: ADDRESSES.user,
    lockedAmount: 5n,
    end: 0,
    isPermanent: false,
    createdAt: 0,
    updatedAt: 0,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });

  const withdraw = TestHelpers.DustLock.Withdraw.createMockEvent({
    provider: ADDRESSES.user,
    tokenId: 5n,
    value: 10n,
    ...eventData(startBlock, 1400, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Withdraw.processEvent({ event: withdraw, mockDb });

  const earlyWithdraw = TestHelpers.DustLock.EarlyWithdraw.createMockEvent({
    provider: ADDRESSES.user,
    tokenId: 5n,
    value: 10n,
    amountReturned: 0n,
    ...eventData(startBlock + 1, 1410, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.EarlyWithdraw.processEvent({
    event: earlyWithdraw,
    mockDb,
  });

  const burn = TestHelpers.DustLock.Transfer.createMockEvent({
    from: ADDRESSES.user,
    to: ZERO_ADDRESS,
    tokenId: 5n,
    ...eventData(startBlock + 2, 1420, ADDRESSES.dustLock),
  });
  mockDb = await TestHelpers.DustLock.Transfer.processEvent({ event: burn, mockDb });

  const token = mockDb.entities.DustLockToken.get('5');
  assert.equal(token?.lockedAmount, 0n);
  assert.equal(token?.owner, '');
});

test('a self-transfer does not reset when the holder acquired the token', async () => {
  const TestHelpers = loadTestHelpers();
  let mockDb = TestHelpers.MockDb.createMockDb();
  const eventData = createEventDataFactory();
  const startBlock = DUST_LOCK_START_BLOCK;

  // Mint: the holder takes ownership at 1000.
  mockDb = await TestHelpers.DustLock.Transfer.processEvent({
    event: TestHelpers.DustLock.Transfer.createMockEvent({
      from: ZERO_ADDRESS,
      to: ADDRESSES.user,
      tokenId: 77n,
      ...eventData(startBlock + 1, 1000, ADDRESSES.dustLock),
    }),
    mockDb,
  });
  assert.equal(mockDb.entities.UserTokenOwnership.get(`${ADDRESSES.user}:77`)?.acquiredAt, 1000);

  // Sending the token to yourself changes nothing about who holds it. Churning
  // the list here would look like a fresh acquisition and silently cut the
  // holder's own VP credit for time they never stopped holding it.
  mockDb = await TestHelpers.DustLock.Transfer.processEvent({
    event: TestHelpers.DustLock.Transfer.createMockEvent({
      from: ADDRESSES.user,
      to: ADDRESSES.user,
      tokenId: 77n,
      ...eventData(startBlock + 2, 9000, ADDRESSES.dustLock),
    }),
    mockDb,
  });

  assert.equal(
    mockDb.entities.UserTokenOwnership.get(`${ADDRESSES.user}:77`)?.acquiredAt,
    1000,
    'a self-transfer must not restart the ownership clock'
  );
  assert.deepEqual(mockDb.entities.UserTokenList.get(ADDRESSES.user)?.tokenIds, [77n]);
});
