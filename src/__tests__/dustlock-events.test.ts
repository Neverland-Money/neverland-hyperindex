import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { DUST_LOCK_START_BLOCK, ZERO_ADDRESS } from '../helpers/constants';

process.env.DISABLE_EXTERNAL_CALLS = 'true';
process.env.DISABLE_ETH_CALLS = 'true';

const ADDRESSES = {
  dustLock: '0x000000000000000000000000000000000000a001',
  user: '0x000000000000000000000000000000000000a002',
  userTwo: '0x000000000000000000000000000000000000a003',
  treasury: '0x000000000000000000000000000000000000a004',
};

function loadTestHelpers() {
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
