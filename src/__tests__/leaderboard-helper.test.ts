// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. This file does not import the `v3-test-helpers` seam, so without this
// a bare `node --test` invocation would inherit them from the repo `.env` via envio's
// dotenv. Redundant under `pnpm run test`, which loads the same module via `--import`.
import './test-env-preload';

import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  removeUserFromLeaderboards,
  updateAllTimeLeaderboard,
  updateLeaderboard,
} from '../helpers/leaderboard';

import type {
  LeaderboardBlacklist,
  LeaderboardEpoch,
  LeaderboardState,
  LeaderboardTotals,
  ScoreBucket,
  UserIndex,
  UserLeaderboardState,
  EvmOnEventContext as handlerContext,
} from 'envio';
type Entity = { readonly id: string };

function createStore<T extends Entity>(initial: readonly T[] = []) {
  const rows = new Map(initial.map(row => [row.id, row]));
  return {
    get: async (id: string) => rows.get(id),
    set: (row: T) => rows.set(row.id, row),
    deleteUnsafe: (id: string) => rows.delete(id),
    rows,
  };
}

function epoch(number: bigint): LeaderboardEpoch {
  return {
    id: number.toString(),
    epochNumber: number,
    startBlock: 1n,
    startTime: 1,
    endBlock: undefined,
    endTime: undefined,
    isActive: true,
    duration: undefined,
    scheduledStartTime: 1,
    scheduledEndTime: undefined,
  };
}

function buildContext(options?: { state?: LeaderboardState; includeBlacklistStore?: boolean }) {
  const state =
    options && 'state' in options
      ? options.state
      : { id: 'current', currentEpochNumber: 7n, isActive: true };
  const stores = {
    LeaderboardState: createStore<LeaderboardState>(state ? [state] : []),
    LeaderboardEpoch: createStore<LeaderboardEpoch>([epoch(7n)]),
    LeaderboardBlacklist: createStore<LeaderboardBlacklist>(),
    UserIndex: createStore<UserIndex>(),
    ScoreBucket: createStore<ScoreBucket>(),
    LeaderboardTotals: createStore<LeaderboardTotals>(),
    UserLeaderboardState: createStore<UserLeaderboardState>(),
  };
  const context = {
    ...stores,
    ...(options?.includeBlacklistStore === false ? { LeaderboardBlacklist: undefined } : {}),
  } as unknown as handlerContext;
  return { context, stores };
}

test('leaderboard histogram uses stable boundary buckets and caps its exponential tail', async () => {
  const { context, stores } = buildContext({ includeBlacklistStore: false });
  const cases = [
    ['head', 0.05, 0, 0, 0.1],
    ['tenth', 0.1, 1, 0.1, 0.5],
    ['half', 0.5, 2, 0.5, 1],
    ['one', 1, 3, 1, 2],
    ['two', 2, 4, 2, 4],
    ['tail', 2 ** 200, 119, 2 ** 116, 2 ** 117],
  ] as const;

  for (const [user, points, bucketIndex, lower, upper] of cases) {
    await updateLeaderboard(context, user, points, 100 + bucketIndex);
    const index = await stores.UserIndex.get(`${user}:7`);
    const bucket = await stores.ScoreBucket.get(`epoch:7:b:${bucketIndex}`);
    assert.equal(index?.bucketIndex, bucketIndex);
    assert.equal(bucket?.lower, lower);
    assert.equal(bucket?.upper, upper);
    assert.ok((bucket?.count ?? 0) >= 1);
  }

  assert.equal((await stores.LeaderboardTotals.get('epoch:7'))?.totalUsers, cases.length);
  assert.equal((await stores.LeaderboardTotals.get('global'))?.totalUsers, cases.length);
});

test('current leaderboard keeps one membership across moves and fully clears it at zero', async () => {
  const { context, stores } = buildContext();

  await updateLeaderboard(context, 'alice', 1, 10);
  await updateLeaderboard(context, 'alice', 1.5, 11);
  assert.equal((await stores.ScoreBucket.get('epoch:7:b:3'))?.count, 1);
  assert.equal((await stores.LeaderboardTotals.get('epoch:7'))?.totalUsers, 1);

  await updateLeaderboard(context, 'alice', 2, 12);
  assert.equal((await stores.ScoreBucket.get('epoch:7:b:3'))?.count, 0);
  assert.equal((await stores.ScoreBucket.get('epoch:7:b:4'))?.count, 1);
  assert.equal((await stores.ScoreBucket.get('b:3'))?.count, 0);
  assert.equal((await stores.ScoreBucket.get('b:4'))?.count, 1);
  assert.equal((await stores.LeaderboardTotals.get('epoch:7'))?.totalUsers, 1);

  await updateLeaderboard(context, 'alice', 0, 13);
  assert.equal((await stores.UserIndex.get('alice:7'))?.bucketIndex, -1);
  assert.equal(await stores.UserIndex.get('alice'), undefined);
  assert.equal((await stores.ScoreBucket.get('epoch:7:b:4'))?.count, 0);
  assert.equal((await stores.ScoreBucket.get('b:4'))?.count, 0);
  assert.equal((await stores.LeaderboardTotals.get('epoch:7'))?.totalUsers, 0);
  assert.equal((await stores.LeaderboardTotals.get('global'))?.totalUsers, 0);
  assert.equal((await stores.UserLeaderboardState.get('alice'))?.lastUpdate, 13);
});

test('current leaderboard synchronizes epoch and global indexes on a same-bucket update', async () => {
  const { context, stores } = buildContext();

  await updateLeaderboard(context, 'alice', 1, 10);
  await updateLeaderboard(context, 'alice', 1.5, 11);

  const epochIndex = await stores.UserIndex.get('alice:7');
  const globalIndex = await stores.UserIndex.get('alice');
  assert.equal(epochIndex?.points, 1.5);
  assert.equal(epochIndex?.updatedAt, 11);
  assert.equal(globalIndex?.points, 1.5);
  assert.equal(globalIndex?.updatedAt, 11);
  assert.equal((await stores.ScoreBucket.get('epoch:7:b:3'))?.count, 1);
  assert.equal((await stores.ScoreBucket.get('b:3'))?.count, 1);
  assert.equal((await stores.LeaderboardTotals.get('epoch:7'))?.totalUsers, 1);
  assert.equal((await stores.LeaderboardTotals.get('global'))?.totalUsers, 1);
});

test('all-time leaderboard changes stay isolated from frontend current-epoch rows', async () => {
  const { context, stores } = buildContext();

  await updateAllTimeLeaderboard(context, 'alice', 1, 10);
  await updateAllTimeLeaderboard(context, 'alice', 1.5, 11);
  assert.equal((await stores.UserIndex.get('alice:0'))?.points, 1.5);
  assert.equal(await stores.UserIndex.get('alice'), undefined);

  await updateAllTimeLeaderboard(context, 'alice', 2, 12);
  await updateAllTimeLeaderboard(context, 'alice', 0, 13);

  assert.equal((await stores.UserIndex.get('alice:0'))?.bucketIndex, -1);
  assert.equal((await stores.ScoreBucket.get('epoch:0:b:3'))?.count, 0);
  assert.equal((await stores.ScoreBucket.get('epoch:0:b:4'))?.count, 0);
  assert.equal((await stores.LeaderboardTotals.get('epoch:0'))?.totalUsers, 0);
  assert.equal(await stores.UserIndex.get('alice'), undefined);
  assert.equal(await stores.ScoreBucket.get('b:3'), undefined);
  assert.equal(await stores.LeaderboardTotals.get('global'), undefined);
  assert.equal(await stores.UserLeaderboardState.get('alice'), undefined);
});

test('leaderboard guards reject blacklisted users and missing current-epoch state', async () => {
  const blacklisted = buildContext();
  blacklisted.stores.LeaderboardBlacklist.set({
    id: 'alice',
    user_id: 'alice',
    isBlacklisted: true,
    lastUpdate: 1,
  });
  await updateLeaderboard(blacklisted.context, 'alice', 1, 1);
  await updateAllTimeLeaderboard(blacklisted.context, 'alice', 1, 1);
  assert.equal(blacklisted.stores.UserIndex.rows.size, 0);

  const noState = buildContext({ state: undefined });
  await updateLeaderboard(noState.context, 'alice', 1, 1);
  assert.equal(noState.stores.UserIndex.rows.size, 0);

  const epochZero = buildContext({
    state: { id: 'current', currentEpochNumber: 0n, isActive: false },
  });
  await updateLeaderboard(epochZero.context, 'alice', 1, 1);
  assert.equal(epochZero.stores.UserIndex.rows.size, 0);

  const missingEpoch = buildContext();
  missingEpoch.stores.LeaderboardEpoch.rows.clear();
  await updateLeaderboard(missingEpoch.context, 'alice', 1, 1);
  assert.equal(missingEpoch.stores.UserIndex.rows.size, 0);
});

test('leaderboard removal is idempotent and saturates stale counters', async () => {
  const empty = buildContext({ state: undefined });
  await removeUserFromLeaderboards(empty.context, 'alice', 1);
  assert.equal(empty.stores.UserIndex.rows.size, 0);

  const inactiveIndex = buildContext();
  inactiveIndex.stores.UserIndex.set({
    id: 'alice:7',
    user: 'alice',
    epochNumber: 7n,
    bucketIndex: -1,
    points: 0,
    updatedAt: 1,
  });
  await removeUserFromLeaderboards(inactiveIndex.context, 'alice', 2);
  assert.equal(await inactiveIndex.stores.UserIndex.get('alice:7'), undefined);

  const populated = buildContext();
  populated.stores.UserIndex.set({
    id: 'alice:7',
    user: 'alice',
    epochNumber: 7n,
    bucketIndex: 3,
    points: 1,
    updatedAt: 1,
  });
  populated.stores.ScoreBucket.set({
    id: 'epoch:7:b:3',
    epochNumber: 7n,
    index: 3,
    lower: 1,
    upper: 2,
    count: 0,
    updatedAt: 1,
  });
  populated.stores.LeaderboardTotals.set({
    id: 'epoch:7',
    epochNumber: 7n,
    totalUsers: 0,
    updatedAt: 1,
  });
  await removeUserFromLeaderboards(populated.context, 'alice', 2);
  await removeUserFromLeaderboards(populated.context, 'alice', 3);
  assert.equal((await populated.stores.ScoreBucket.get('epoch:7:b:3'))?.count, 0);
  assert.equal((await populated.stores.LeaderboardTotals.get('epoch:7'))?.totalUsers, 0);
  assert.equal(await populated.stores.UserIndex.get('alice:7'), undefined);

  const missingRows = buildContext();
  missingRows.stores.UserIndex.set({
    id: 'alice:7',
    user: 'alice',
    epochNumber: 7n,
    bucketIndex: 3,
    points: 1,
    updatedAt: 1,
  });
  await removeUserFromLeaderboards(missingRows.context, 'alice', 2);
  assert.equal(missingRows.stores.ScoreBucket.rows.size, 0);
  assert.equal(missingRows.stores.LeaderboardTotals.rows.size, 0);
});

test('leaderboard counters cap and non-finite scores fail closed', async () => {
  const capped = buildContext();
  capped.stores.ScoreBucket.set({
    id: 'epoch:7:b:3',
    epochNumber: 7n,
    index: 3,
    lower: 1,
    upper: 2,
    count: 2_147_483_647,
    updatedAt: 1,
  });
  capped.stores.LeaderboardTotals.set({
    id: 'epoch:7',
    epochNumber: 7n,
    totalUsers: 2_147_483_647,
    updatedAt: 1,
  });
  await updateLeaderboard(capped.context, 'alice', 1, 2);
  assert.equal((await capped.stores.ScoreBucket.get('epoch:7:b:3'))?.count, 2_147_483_647);
  assert.equal((await capped.stores.LeaderboardTotals.get('epoch:7'))?.totalUsers, 2_147_483_647);
  assert.equal((await capped.stores.UserIndex.get('alice:7'))?.bucketIndex, 3);

  for (const invalid of [Number.NaN, Number.POSITIVE_INFINITY]) {
    const current = buildContext();
    await updateLeaderboard(current.context, 'alice', invalid, 3);
    assert.equal((await current.stores.UserIndex.get('alice:7'))?.bucketIndex, -1);
    assert.equal(current.stores.ScoreBucket.rows.size, 0);

    const allTime = buildContext();
    await updateAllTimeLeaderboard(allTime.context, 'alice', invalid, 3);
    assert.equal((await allTime.stores.UserIndex.get('alice:0'))?.bucketIndex, -1);
    assert.equal(allTime.stores.ScoreBucket.rows.size, 0);
  }
});

test('leaderboard point transitions saturate stale bucket and total counters', async () => {
  const zeroed = buildContext();
  zeroed.stores.UserIndex.set({
    id: 'alice:7',
    user: 'alice',
    epochNumber: 7n,
    bucketIndex: 3,
    points: 1,
    updatedAt: 1,
  });
  zeroed.stores.ScoreBucket.set({
    id: 'epoch:7:b:3',
    epochNumber: 7n,
    index: 3,
    lower: 1,
    upper: 2,
    count: 0,
    updatedAt: 1,
  });
  zeroed.stores.LeaderboardTotals.set({
    id: 'epoch:7',
    epochNumber: 7n,
    totalUsers: 0,
    updatedAt: 1,
  });
  await updateLeaderboard(zeroed.context, 'alice', 0, 2);
  assert.equal((await zeroed.stores.ScoreBucket.get('epoch:7:b:3'))?.count, 0);
  assert.equal((await zeroed.stores.LeaderboardTotals.get('epoch:7'))?.totalUsers, 0);

  const moved = buildContext();
  moved.stores.UserIndex.set({
    id: 'alice:7',
    user: 'alice',
    epochNumber: 7n,
    bucketIndex: 3,
    points: 1,
    updatedAt: 1,
  });
  moved.stores.ScoreBucket.set({
    id: 'epoch:7:b:3',
    epochNumber: 7n,
    index: 3,
    lower: 1,
    upper: 2,
    count: 0,
    updatedAt: 1,
  });
  moved.stores.LeaderboardTotals.set({
    id: 'epoch:7',
    epochNumber: 7n,
    totalUsers: 1,
    updatedAt: 1,
  });
  await updateLeaderboard(moved.context, 'alice', 2, 2);
  assert.equal((await moved.stores.ScoreBucket.get('epoch:7:b:3'))?.count, 0);
  assert.equal((await moved.stores.ScoreBucket.get('epoch:7:b:4'))?.count, 1);
  assert.equal((await moved.stores.LeaderboardTotals.get('epoch:7'))?.totalUsers, 1);
});
