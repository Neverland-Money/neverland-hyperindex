/**
 * DustLock (veNFT) Event Handlers
 */

import {
  recalculateUserTotalVP,
  recordProtocolTransaction,
  getOrCreateUser,
  updateUserTokenList,
  updateUserVotingPower,
  settlePointsForAllReserves,
} from './shared';
import {
  DUST_LOCK_START_BLOCK,
  MAX_LOCK_TIME,
  ZERO_ADDRESS,
  normalizeAddress,
} from '../helpers/constants';
import { handleDustLockSpecialEditionTransfer } from './specialEditions';

import { DustLock } from '../../generated';
import type { handlerContext } from '../../generated';

function shouldUpdateVotingPower(blockNumber: number): boolean {
  return blockNumber >= DUST_LOCK_START_BLOCK;
}

// Self-settle a VP owner's accrued points BEFORE we mutate their lock for this
// event. Settling first lets the time-integral award the OLD (pre-mutate) VP
// slope up to `timestamp` — including the slope all the way down to 0 on a full
// withdraw or a fully-decayed lock. CRITICAL: never gate this on instantaneous
// VP (a "VP == 0 now" check would drop the legitimate decay tail); always settle
// before the lock write and let `calculateAverageVPFromStorage` decide.
//
// veDUST state changes do not otherwise trigger point accrual, so without this a
// pure-VP user only accrues when the keeper settles them — which the backfill
// gate (ENVIO_LEADERBOARD_LIVE_EPOCH) skips for closed past epochs. Cheap for
// pure-VP users (empty reserve loop, no eth_call) and idempotent: the monotonic
// per-(user,epoch) `lastVPAccrualTimestamp` cursor makes repeat settles no-ops.
async function settleVpOwnerBeforeMutate(
  context: handlerContext,
  owner: string | undefined,
  timestamp: number,
  blockNumber: bigint
): Promise<void> {
  if (!owner || owner === '' || owner === ZERO_ADDRESS) return;
  if (Number(blockNumber) < DUST_LOCK_START_BLOCK) return;
  await settlePointsForAllReserves(context, owner, timestamp, blockNumber, {
    ignoreCooldown: true,
    skipNftSync: true,
    skipLPChainSync: true,
  });
}

const SECONDS_PER_WEEK = 7 * 24 * 60 * 60;

async function getOrInitDustLockToken(context: handlerContext, tokenId: string, timestamp: number) {
  let token = await context.DustLockToken.get(tokenId);
  if (!token) {
    token = {
      id: tokenId,
      owner: '',
      lockedAmount: 0n,
      end: 0,
      isPermanent: false,
      createdAt: timestamp,
      updatedAt: timestamp,
      lastDepositType: undefined,
      selfRepayEnabled: false,
      rewardReceiver: undefined,
    };
  }
  return token;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function createAdminEventId(event: any): string {
  return `${event.transaction.hash}-${event.logIndex}`;
}

DustLock.Deposit.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const provider = normalizeAddress(event.params.provider);
  await getOrCreateUser(context, provider);

  const tokenId = event.params.tokenId.toString();
  const depositId = `${event.transaction.hash}-${event.logIndex}`;
  const depositType = event.params.depositType.toString();

  const token = await getOrInitDustLockToken(context, tokenId, Number(event.block.timestamp));

  // Settle the owner's accrued VP on the OLD amount before increasing it.
  await settleVpOwnerBeforeMutate(
    context,
    token.owner,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const lockedAmount = token.lockedAmount + event.params.value;
  const locktime = Number(event.params.locktime);

  // locktime = 0 indicates an infinite/permanent lock
  const isPermanent = locktime === 0;

  context.DustLockToken.set({
    ...token,
    lockedAmount,
    end: locktime,
    isPermanent,
    lastDepositType: depositType,
    updatedAt: Number(event.block.timestamp),
  });

  context.DustLockDeposit.set({
    id: depositId,
    provider,
    tokenId,
    depositType,
    value: event.params.value,
    locktime: Number(event.params.locktime),
    txHash: event.transaction.hash,
    timestamp: Number(event.block.timestamp),
  });

  if (token.owner && token.owner !== '' && shouldUpdateVotingPower(Number(event.block.number))) {
    await recalculateUserTotalVP(
      context,
      token.owner,
      Number(event.block.timestamp),
      event.transaction.hash,
      'DEPOSIT',
      Number(event.logIndex),
      BigInt(event.block.number)
    );
  }
});

DustLock.Withdraw.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const provider = normalizeAddress(event.params.provider);
  await getOrCreateUser(context, provider);

  const tokenId = event.params.tokenId.toString();
  const withdrawId = `${event.transaction.hash}-${event.logIndex}`;

  context.DustLockWithdraw.set({
    id: withdrawId,
    provider,
    tokenId,
    value: event.params.value,
    txHash: event.transaction.hash,
    timestamp: Number(event.block.timestamp),
  });

  const token = await getOrInitDustLockToken(context, tokenId, Number(event.block.timestamp));

  // Settle the owner's accrued VP (the decaying slope, possibly to 0) before the
  // lock is drained — otherwise the integral would see the zeroed amount.
  await settleVpOwnerBeforeMutate(
    context,
    token.owner,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const newAmount =
    token.lockedAmount > event.params.value ? token.lockedAmount - event.params.value : 0n;

  context.DustLockToken.set({
    ...token,
    lockedAmount: newAmount,
    updatedAt: Number(event.block.timestamp),
  });

  if (token.owner && token.owner !== '' && shouldUpdateVotingPower(Number(event.block.number))) {
    await updateUserVotingPower(
      context,
      token.owner,
      event.params.tokenId,
      0n,
      Number(event.block.timestamp),
      event.transaction.hash,
      'WITHDRAW',
      Number(event.logIndex)
    );
  }
});

DustLock.EarlyWithdraw.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const provider = normalizeAddress(event.params.provider);
  await getOrCreateUser(context, provider);

  const tokenId = event.params.tokenId.toString();
  const withdrawId = `${event.transaction.hash}-${event.logIndex}`;
  const penalty = event.params.value - event.params.amountReturned;

  context.DustLockEarlyWithdraw.set({
    id: withdrawId,
    provider,
    tokenId,
    value: event.params.value,
    amountReturned: event.params.amountReturned,
    penalty,
    txHash: event.transaction.hash,
    timestamp: Number(event.block.timestamp),
  });

  const token = await getOrInitDustLockToken(context, tokenId, Number(event.block.timestamp));

  // Settle the owner's accrued VP slope before the lock is drained.
  await settleVpOwnerBeforeMutate(
    context,
    token.owner,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const newAmount =
    token.lockedAmount > event.params.value ? token.lockedAmount - event.params.value : 0n;

  context.DustLockToken.set({
    ...token,
    lockedAmount: newAmount,
    updatedAt: Number(event.block.timestamp),
  });

  if (token.owner && token.owner !== '' && shouldUpdateVotingPower(Number(event.block.number))) {
    await updateUserVotingPower(
      context,
      token.owner,
      event.params.tokenId,
      0n,
      Number(event.block.timestamp),
      event.transaction.hash,
      'EARLY_WITHDRAW',
      Number(event.logIndex)
    );
  }
});

DustLock.LockPermanent.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenId = event.params.tokenId.toString();
  const id = `${event.transaction.hash}-${event.logIndex}`;

  const token = await getOrInitDustLockToken(context, tokenId, Number(event.block.timestamp));

  // Settle the decaying-slope era before VP becomes a flat permanent value.
  await settleVpOwnerBeforeMutate(
    context,
    token.owner,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  context.DustLockToken.set({
    ...token,
    isPermanent: true,
    end: 0,
    lockedAmount: event.params.amount,
    updatedAt: Number(event.block.timestamp),
  });

  context.DustLockPermanentLock.set({
    id,
    tokenId,
    owner: token.owner,
    amount: event.params.amount,
    txHash: event.transaction.hash,
    timestamp: Number(event.block.timestamp),
  });

  if (token.owner && token.owner !== '' && shouldUpdateVotingPower(Number(event.block.number))) {
    await recalculateUserTotalVP(
      context,
      token.owner,
      Number(event.block.timestamp),
      event.transaction.hash,
      'LOCK_PERMANENT',
      Number(event.logIndex),
      BigInt(event.block.number)
    );
  }
});

DustLock.UnlockPermanent.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenId = event.params.tokenId.toString();
  const id = `${event.transaction.hash}-${event.logIndex}`;
  const unlockTimestamp = Number(event.params.ts);
  const unlockEnd =
    Math.floor((unlockTimestamp + Number(MAX_LOCK_TIME)) / SECONDS_PER_WEEK) * SECONDS_PER_WEEK;

  const token = await getOrInitDustLockToken(context, tokenId, Number(event.block.timestamp));

  // Settle the flat permanent-VP era before decay resumes from the new end.
  await settleVpOwnerBeforeMutate(
    context,
    token.owner,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  context.DustLockToken.set({
    ...token,
    isPermanent: false,
    end: unlockEnd,
    lockedAmount: event.params.amount,
    updatedAt: Number(event.block.timestamp),
  });

  context.DustLockPermanentUnlock.set({
    id,
    tokenId,
    owner: token.owner,
    txHash: event.transaction.hash,
    timestamp: Number(event.block.timestamp),
  });

  if (token.owner && token.owner !== '' && shouldUpdateVotingPower(Number(event.block.number))) {
    await recalculateUserTotalVP(
      context,
      token.owner,
      Number(event.block.timestamp),
      event.transaction.hash,
      'UNLOCK_PERMANENT',
      Number(event.logIndex),
      BigInt(event.block.number)
    );
  }
});

DustLock.Supply.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.DustLockSupplyHistory.set({
    id,
    prevSupply: event.params.prevSupply,
    supply: event.params.supply,
    timestamp: Number(event.block.timestamp),
  });
});

DustLock.Merge.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const sender = normalizeAddress(event.params.sender);
  await getOrCreateUser(context, sender);

  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.DustLockMerge.set({
    id,
    sender,
    fromToken: event.params.from.toString(),
    toToken: event.params.to.toString(),
    amountFrom: event.params.amountFrom,
    amountTo: event.params.amountTo,
    amountFinal: event.params.amountFinal,
    locktime: Number(event.params.locktime),
    txHash: event.transaction.hash,
    timestamp: Number(event.block.timestamp),
  });

  const toToken = await getOrInitDustLockToken(
    context,
    event.params.to.toString(),
    Number(event.block.timestamp)
  );

  // Settle the surviving token's owner before its amount/end change. The burned
  // `from` token's owner is settled by the paired Transfer(->0x0) it emits.
  await settleVpOwnerBeforeMutate(
    context,
    toToken.owner,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  context.DustLockToken.set({
    ...toToken,
    lockedAmount: event.params.amountFinal,
    end: Number(event.params.locktime),
    updatedAt: Number(event.block.timestamp),
  });

  if (
    toToken.owner &&
    toToken.owner !== '' &&
    shouldUpdateVotingPower(Number(event.block.number))
  ) {
    await recalculateUserTotalVP(
      context,
      toToken.owner,
      Number(event.block.timestamp),
      event.transaction.hash,
      'MERGE',
      Number(event.logIndex),
      BigInt(event.block.number)
    );
  }
});

DustLock.Split.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const sender = normalizeAddress(event.params.sender);
  await getOrCreateUser(context, sender);

  const id = `${event.transaction.hash}-${event.logIndex}`;

  context.DustLockSplit.set({
    id,
    sender,
    fromToken: event.params.from.toString(),
    tokenId1: event.params.tokenId1.toString(),
    tokenId2: event.params.tokenId2.toString(),
    splitAmount1: event.params.splitAmount1,
    splitAmount2: event.params.splitAmount2,
    locktime: Number(event.params.locktime),
    txHash: event.transaction.hash,
    timestamp: Number(event.block.timestamp),
  });

  const token1 = await getOrInitDustLockToken(
    context,
    event.params.tokenId1.toString(),
    Number(event.block.timestamp)
  );

  // Settle the source owner once before the split rewrites the position. Both new
  // tokens share this owner, so a single settle covers the whole position.
  await settleVpOwnerBeforeMutate(
    context,
    token1.owner,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  context.DustLockToken.set({
    ...token1,
    lockedAmount: event.params.splitAmount1,
    end: Number(event.params.locktime),
    updatedAt: Number(event.block.timestamp),
  });

  const token2 = await getOrInitDustLockToken(
    context,
    event.params.tokenId2.toString(),
    Number(event.block.timestamp)
  );
  context.DustLockToken.set({
    ...token2,
    lockedAmount: event.params.splitAmount2,
    end: Number(event.params.locktime),
    updatedAt: Number(event.block.timestamp),
  });

  if (token1.owner && token1.owner !== '' && shouldUpdateVotingPower(Number(event.block.number))) {
    await recalculateUserTotalVP(
      context,
      token1.owner,
      Number(event.block.timestamp),
      event.transaction.hash,
      'SPLIT',
      Number(event.logIndex),
      BigInt(event.block.number)
    );
  }
});

DustLock.Transfer.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const tokenId = event.params.tokenId.toString();
  const from = normalizeAddress(event.params.from);
  const to = normalizeAddress(event.params.to);
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);

  // Settle BOTH parties before the token moves. `from` is settled while the token
  // still counts in their list — capturing the outgoing decay slope (including to
  // 0 on a burn). `to` is settled before the token is added to their list, so it
  // accrues 0 for the just-received token (its accrual starts now). The helper
  // no-ops for the zero address.
  await settleVpOwnerBeforeMutate(context, from, timestamp, blockNumber);
  await settleVpOwnerBeforeMutate(context, to, timestamp, blockNumber);

  await handleDustLockSpecialEditionTransfer(
    context,
    event.params.tokenId,
    from,
    to,
    timestamp,
    blockNumber,
    event.transaction.hash,
    Number(event.logIndex)
  );

  const token = await getOrInitDustLockToken(context, tokenId, timestamp);
  const isBurn = to === ZERO_ADDRESS;
  context.DustLockToken.set({
    ...token,
    owner: isBurn ? '' : to,
    lockedAmount: isBurn ? 0n : token.lockedAmount,
    end: isBurn ? 0 : token.end,
    isPermanent: isBurn ? false : token.isPermanent,
    updatedAt: timestamp,
  });

  if (from !== ZERO_ADDRESS) {
    await updateUserTokenList(context, from, event.params.tokenId, timestamp, 'remove');
  }
  if (to !== ZERO_ADDRESS) {
    await updateUserTokenList(context, to, event.params.tokenId, timestamp, 'add');
  }

  if (from !== ZERO_ADDRESS) {
    await getOrCreateUser(context, from);
  }
  if (to !== ZERO_ADDRESS) {
    await getOrCreateUser(context, to);
  }

  if (shouldUpdateVotingPower(Number(event.block.number))) {
    if (from !== ZERO_ADDRESS) {
      await recalculateUserTotalVP(
        context,
        from,
        timestamp,
        event.transaction.hash,
        'TRANSFER_OUT',
        Number(event.logIndex),
        blockNumber
      );
    }
    if (to !== ZERO_ADDRESS) {
      await recalculateUserTotalVP(
        context,
        to,
        timestamp,
        event.transaction.hash,
        'TRANSFER_IN',
        Number(event.logIndex),
        blockNumber
      );
    }
  }
});

DustLock.EarlyWithdrawPenaltyUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'EarlyWithdrawPenaltyUpdated',
    addressOne: undefined,
    addressTwo: undefined,
    oldValue: event.params.oldPenalty,
    newValue: event.params.newPenalty,
    boolValue: undefined,
    tokenId: undefined,
    stringOne: undefined,
    stringTwo: undefined,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

DustLock.EarlyWithdrawTreasuryUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'EarlyWithdrawTreasuryUpdated',
    addressOne: normalizeAddress(event.params.oldTreasury),
    addressTwo: normalizeAddress(event.params.newTreasury),
    oldValue: undefined,
    newValue: undefined,
    boolValue: undefined,
    tokenId: undefined,
    stringOne: undefined,
    stringTwo: undefined,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

DustLock.MinLockAmountUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'MinLockAmountUpdated',
    addressOne: undefined,
    addressTwo: undefined,
    oldValue: event.params.oldAmount,
    newValue: event.params.newAmount,
    boolValue: undefined,
    tokenId: undefined,
    stringOne: undefined,
    stringTwo: undefined,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

DustLock.RevenueRewardUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'RevenueRewardUpdated',
    addressOne: normalizeAddress(event.params.oldReward),
    addressTwo: normalizeAddress(event.params.newReward),
    oldValue: undefined,
    newValue: undefined,
    boolValue: undefined,
    tokenId: undefined,
    stringOne: undefined,
    stringTwo: undefined,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

DustLock.SplitPermissionUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'SplitPermissionUpdated',
    addressOne: normalizeAddress(event.params.account),
    addressTwo: undefined,
    oldValue: undefined,
    newValue: undefined,
    boolValue: event.params.allowed,
    tokenId: undefined,
    stringOne: undefined,
    stringTwo: undefined,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

DustLock.TeamProposed.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'TeamProposed',
    addressOne: normalizeAddress(event.params.currentTeam),
    addressTwo: normalizeAddress(event.params.proposedTeam),
    oldValue: undefined,
    newValue: undefined,
    boolValue: undefined,
    tokenId: undefined,
    stringOne: undefined,
    stringTwo: undefined,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

DustLock.TeamAccepted.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'TeamAccepted',
    addressOne: normalizeAddress(event.params.oldTeam),
    addressTwo: normalizeAddress(event.params.newTeam),
    oldValue: undefined,
    newValue: undefined,
    boolValue: undefined,
    tokenId: undefined,
    stringOne: undefined,
    stringTwo: undefined,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

DustLock.TeamProposalCancelled.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'TeamProposalCancelled',
    addressOne: normalizeAddress(event.params.currentTeam),
    addressTwo: normalizeAddress(event.params.cancelledTeam),
    oldValue: undefined,
    newValue: undefined,
    boolValue: undefined,
    tokenId: undefined,
    stringOne: undefined,
    stringTwo: undefined,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

DustLock.BaseURIUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'BaseURIUpdated',
    addressOne: undefined,
    addressTwo: undefined,
    oldValue: undefined,
    newValue: undefined,
    boolValue: undefined,
    tokenId: undefined,
    stringOne: event.params.oldBaseURI,
    stringTwo: event.params.newBaseURI,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});

DustLock.MetadataUpdate.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = createAdminEventId(event);
  context.DustLockAdminEvent.set({
    id,
    eventType: 'MetadataUpdate',
    addressOne: undefined,
    addressTwo: undefined,
    oldValue: undefined,
    newValue: undefined,
    boolValue: undefined,
    tokenId: event.params._tokenId.toString(),
    stringOne: undefined,
    stringTwo: undefined,
    timestamp: Number(event.block.timestamp),
    txHash: event.transaction.hash,
  });
});
