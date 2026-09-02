// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. This file does not import the `v3-test-helpers` seam, so without this
// a bare `node --test` invocation would inherit them from the repo `.env` via envio's
// dotenv. Redundant under `pnpm run test`, which loads the same module via `--import`.
import './test-env-preload';

import assert from 'node:assert/strict';
import { test } from 'node:test';

import { DUST_LOCK_START_BLOCK } from '../helpers/constants';
import {
  addTierToIndex,
  calculateNFTMultiplierFromUser,
  calculateVPMultiplier,
  createMultiplierSnapshot,
  findVPTierIndex,
  invalidateNFTPartnershipCache,
  recalculateUserTotalVP,
  recordProtocolTransaction,
  removeTierFromIndex,
  updateUserVotingPower,
} from '../handlers/shared';

import type { UserLeaderboardState, handlerContext } from '../../generated';

process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

type Row = { id: string } & Record<string, unknown>;

function store(initial: readonly Row[] = []) {
  const rows = new Map(initial.map(row => [row.id, row]));
  return {
    get: async (id: string) => rows.get(id),
    set: (row: Row) => rows.set(row.id, row),
    deleteUnsafe: (id: string) => rows.delete(id),
    rows,
  };
}

function legacyState(user: string, nftMultiplier = 12000n) {
  return {
    id: user,
    user_id: user,
    nftCount: 1n,
    nftMultiplier,
    votingPower: 0n,
    vpTierIndex: 0n,
    vpMultiplier: 10000n,
    combinedMultiplier: nftMultiplier,
    totalEpochsParticipated: 0n,
    lifetimePoints: 0n,
    currentEpochId: undefined,
    currentEpochRank: undefined,
    lastUpdate: 0,
  } as unknown as UserLeaderboardState;
}

test('clearing the partnership cache refreshes every owned collection', async () => {
  const user = '0x000000000000000000000000000000000000a001';
  const collectionA = '0x000000000000000000000000000000000000a002';
  const collectionB = '0x000000000000000000000000000000000000a003';
  const nftPartnership = store([
    {
      id: collectionA,
      collection: collectionA,
      name: 'A',
      active: true,
      staticBoostBps: 100n,
      startTimestamp: 0,
      endTimestamp: undefined,
      addedAt: 0,
      lastUpdate: 0,
    },
    {
      id: collectionB,
      collection: collectionB,
      name: 'B',
      active: true,
      staticBoostBps: 200n,
      startTimestamp: 0,
      endTimestamp: undefined,
      addedAt: 0,
      lastUpdate: 0,
    },
  ]);
  const context = {
    ProtocolStats: store(),
    ProtocolStatsSnapshot: store(),
    LeaderboardState: store(),
    LeaderboardEpoch: store(),
    NFTPartnershipRegistryState: store([
      { id: 'current', activeCollections: [collectionA, collectionB], lastUpdate: 0 },
    ]),
    UserNFTOwnership: store([
      {
        id: `${user}:${collectionA}`,
        user_id: user,
        partnership_id: collectionA,
        balance: 1n,
        hasNFT: true,
        lastCheckedAt: 0,
        lastCheckedBlock: 0n,
      },
      {
        id: `${user}:${collectionB}`,
        user_id: user,
        partnership_id: collectionB,
        balance: 1n,
        hasNFT: true,
        lastCheckedAt: 0,
        lastCheckedBlock: 0n,
      },
    ]),
    NFTPartnership: nftPartnership,
    NFTMultiplierConfig: store(),
    UserLeaderboardState: store(),
  } as unknown as handlerContext;

  await recordProtocolTransaction(context, '0x01', 1, 1n);
  assert.equal(await calculateNFTMultiplierFromUser(context, user), 10300n);
  nftPartnership.set({
    ...(await nftPartnership.get(collectionA))!,
    staticBoostBps: 300n,
  });
  nftPartnership.set({
    ...(await nftPartnership.get(collectionB))!,
    staticBoostBps: 400n,
  });
  assert.equal(await calculateNFTMultiplierFromUser(context, user), 10300n);

  invalidateNFTPartnershipCache();
  assert.equal(await calculateNFTMultiplierFromUser(context, user), 10700n);
});

test('indexed voting-power tiers ignore missing and inactive rows and sort active rows', async () => {
  const tiers = store([
    {
      id: '2',
      tierIndex: 2n,
      minVotingPower: 200n,
      multiplierBps: 30000n,
      createdAt: 0,
      lastUpdate: 0,
      isActive: true,
    },
    {
      id: '0',
      tierIndex: 0n,
      minVotingPower: 0n,
      multiplierBps: 15000n,
      createdAt: 0,
      lastUpdate: 0,
      isActive: true,
    },
    {
      id: '1',
      tierIndex: 1n,
      minVotingPower: 100n,
      multiplierBps: 20000n,
      createdAt: 0,
      lastUpdate: 0,
      isActive: false,
    },
  ]);
  const context = {
    VotingPowerTierIndex: store([
      { id: 'current', activeTierIds: ['2', 'missing', '1', '0'], lastUpdate: 0 },
    ]),
    VotingPowerTier: tiers,
  } as unknown as handlerContext;

  assert.equal(await calculateVPMultiplier(context, 150n), 15000n);
  assert.equal(await findVPTierIndex(context, 150n), 0n);
  assert.equal(await calculateVPMultiplier(context, 250n), 30000n);
  assert.equal(await findVPTierIndex(context, 250n), 2n);

  tiers.set({
    id: '3',
    tierIndex: 3n,
    minVotingPower: 300n,
    multiplierBps: 60000n,
    createdAt: 0,
    lastUpdate: 0,
    isActive: true,
  });
  const cappedContext = {
    VotingPowerTierIndex: store([{ id: 'current', activeTierIds: ['3'], lastUpdate: 0 }]),
    VotingPowerTier: tiers,
  } as unknown as handlerContext;
  assert.equal(await calculateVPMultiplier(cappedContext, 300n), 50000n);
});

test('tier index addition refreshes duplicates and removal tolerates a missing index', async () => {
  const index = store([{ id: 'current', activeTierIds: ['1'], lastUpdate: 1 }]);
  const context = { VotingPowerTierIndex: index } as unknown as handlerContext;
  await addTierToIndex(context, '1', 2);
  assert.deepEqual((await index.get('current'))?.activeTierIds, ['1']);
  assert.equal((await index.get('current'))?.lastUpdate, 2);

  const missing = store();
  await removeTierFromIndex({ VotingPowerTierIndex: missing } as unknown as handlerContext, '1', 3);
  assert.equal(missing.rows.size, 0);
});

test('legacy leaderboard rows default special editions to neutral in updates and snapshots', async () => {
  const user = '0xlegacy';
  const leaderboard = store([legacyState(user) as unknown as Row]);
  const tiers = store([
    {
      id: '0',
      tierIndex: 0n,
      minVotingPower: 0n,
      multiplierBps: 15000n,
      createdAt: 0,
      lastUpdate: 0,
      isActive: true,
    },
  ]);
  const snapshots = store();
  const history = store();
  const context = {
    UserLeaderboardState: leaderboard,
    VotingPowerTier: tiers,
    UserMultiplierSnapshot: snapshots,
    UserVotingPowerHistory: history,
  } as unknown as handlerContext;

  await updateUserVotingPower(context, user, 1n, 100n, 10, '0x10', 'LEGACY', 1);
  assert.equal((await leaderboard.get(user))?.combinedMultiplier, 17000n);
  assert.equal((await snapshots.get(`${user}:10:0x10:1`))?.specialEditionCount, 0n);
  assert.equal((await snapshots.get(`${user}:10:0x10:1`))?.specialEditionMultiplier, 10000n);

  createMultiplierSnapshot(
    context,
    {
      id: user,
      nftCount: 1n,
      nftMultiplier: 12000n,
      votingPower: 100n,
      vpMultiplier: 15000n,
      combinedMultiplier: 17000n,
    },
    11,
    '0x11',
    'LEGACY_DIRECT',
    2
  );
  assert.equal((await snapshots.get(`${user}:11:0x11:2`))?.specialEditionCount, 0n);
  assert.equal((await snapshots.get(`${user}:11:0x11:2`))?.specialEditionMultiplier, 10000n);
});

test('legacy special-edition defaults hold for empty and populated token recalculation', async () => {
  const emptyUser = '0xempty';
  const emptyLeaderboard = store([legacyState(emptyUser) as unknown as Row]);
  const emptyContext = {
    UserTokenList: store(),
    UserLeaderboardState: emptyLeaderboard,
  } as unknown as handlerContext;
  await recalculateUserTotalVP(
    emptyContext,
    emptyUser,
    10,
    '0x20',
    'EMPTY',
    0,
    BigInt(DUST_LOCK_START_BLOCK)
  );
  assert.equal((await emptyLeaderboard.get(emptyUser))?.combinedMultiplier, 12000n);

  const tokenUser = '0xtoken';
  const tokenLeaderboard = store([legacyState(tokenUser) as unknown as Row]);
  const context = {
    UserTokenList: store([{ id: tokenUser, user_id: tokenUser, tokenIds: [7n], lastUpdate: 0 }]),
    DustLockToken: store([
      {
        id: '7',
        owner: tokenUser,
        lockedAmount: 100n,
        end: 0,
        isPermanent: true,
        createdAt: 0,
        updatedAt: 0,
        lastDepositType: undefined,
        selfRepayEnabled: false,
        rewardReceiver: undefined,
      },
    ]),
    UserLeaderboardState: tokenLeaderboard,
    VotingPowerTier: store([
      {
        id: '0',
        tierIndex: 0n,
        minVotingPower: 0n,
        multiplierBps: 15000n,
        createdAt: 0,
        lastUpdate: 0,
        isActive: true,
      },
    ]),
    UserMultiplierSnapshot: store(),
    UserVotingPowerHistory: store(),
  } as unknown as handlerContext;
  await recalculateUserTotalVP(
    context,
    tokenUser,
    10,
    '0x21',
    'TOKEN',
    0,
    BigInt(DUST_LOCK_START_BLOCK)
  );
  assert.equal((await tokenLeaderboard.get(tokenUser))?.combinedMultiplier, 17000n);
});
