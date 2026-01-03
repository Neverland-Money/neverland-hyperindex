/**
 * TopK + Histogram Leaderboard System
 * Migrated from neverland-subgraphs
 */

import type { handlerContext } from '../../generated';
import { getOrCreateUserLeaderboardState } from '../handlers/shared';

const MAX_TOP_K = 100;
const MAX_BUCKETS = 120;
const ALL_TIME_EPOCH_NUMBER = 0n;

type TopKEntryEntity = {
  id: string;
  epochNumber: bigint;
  userId: string;
  points: number;
  rank: number;
};

function normalizePoints(points: number): number {
  if (!Number.isFinite(points) || points < 0) {
    return 0;
  }
  return points;
}

function sortTopKEntries(entries: TopKEntryEntity[]): TopKEntryEntity[] {
  return [...entries].sort((a, b) => {
    if (a.points !== b.points) {
      return b.points - a.points;
    }
    /* c8 ignore start */
    if (a.userId === b.userId) {
      return 0;
    }
    /* c8 ignore end */
    return a.userId < b.userId ? -1 : 1;
  });
}

export async function isUserBlacklisted(context: handlerContext, userId: string): Promise<boolean> {
  const blacklistStore = (
    context as unknown as {
      LeaderboardBlacklist?: {
        get: (id: string) => Promise<{ isBlacklisted?: boolean } | undefined>;
      };
    }
  ).LeaderboardBlacklist;
  if (!blacklistStore) {
    return false;
  }
  const record = await blacklistStore.get(userId);
  return record?.isBlacklisted ?? false;
}

/**
 * Deterministic bucket index for a given points value
 * Linear head: [0, 0.1), [0.1, 0.5), [0.5, 1)
 * Exponential tail: [1, 2), [2, 4), [4, 8), [8, 16), ...
 */
export function bucketIndexFor(points: number): number {
  if (points < 0) return 0;
  if (points < 0.1) return 0;
  if (points < 0.5) return 1;
  if (points < 1) return 2;

  let idx = 3;
  let bound = 1;

  while (idx < MAX_BUCKETS - 1 && points >= bound * 2) {
    bound = bound * 2;
    idx++;
  }

  return idx;
}

/**
 * Get bucket bounds for a given index
 */
export function bucketBounds(index: number): [number, number] {
  if (index === 0) return [0, 0.1];
  if (index === 1) return [0.1, 0.5];
  if (index === 2) return [0.5, 1];

  const exp = index - 3;
  let lower = 1;
  let upper = 2;

  for (let i = 0; i < exp; i++) {
    lower = lower * 2;
    upper = upper * 2;
  }

  return [lower, upper];
}

async function getOrInitTopK(context: handlerContext, epochNumber: bigint, idOverride?: string) {
  const id = idOverride ?? `epoch:${epochNumber}`;
  let topK = await context.TopK.get(id);
  if (!topK) {
    topK = {
      id,
      epochNumber,
      k: MAX_TOP_K,
      entries: [],
      updatedAt: 0,
    };
    context.TopK.set(topK);
  }
  return topK;
}

async function removeUserFromTopK(
  context: handlerContext,
  epochNumber: bigint,
  userId: string,
  timestamp: number,
  idOverride?: string
) {
  const id = idOverride ?? `epoch:${epochNumber}`;
  const topK = await context.TopK.get(id);
  if (!topK || !topK.entries || topK.entries.length === 0) {
    return;
  }

  const remaining: TopKEntryEntity[] = [];
  let removed = false;

  for (const entryId of topK.entries) {
    const entry = await context.TopKEntry.get(entryId);
    if (!entry) continue;
    if (entry.userId === userId) {
      removed = true;
      context.TopKEntry.deleteUnsafe(entryId);
      continue;
    }
    remaining.push({ ...entry, points: normalizePoints(entry.points) });
  }

  if (!removed) return;

  const sortedEntries = sortTopKEntries(remaining);
  const nextEntries: string[] = [];

  for (let i = 0; i < sortedEntries.length; i++) {
    const entry = sortedEntries[i];
    nextEntries.push(entry.id);
    context.TopKEntry.set({
      ...entry,
      rank: i + 1,
    });
  }

  context.TopK.set({
    ...topK,
    entries: nextEntries,
    updatedAt: timestamp,
  });
}

async function removeUserFromLeaderboardForEpoch(
  context: handlerContext,
  userId: string,
  epochNumber: bigint,
  timestamp: number,
  syncGlobal: boolean
) {
  const userIndexId = `${userId}:${epochNumber}`;
  const userIndex = await context.UserIndex.get(userIndexId);
  if (userIndex) {
    if (userIndex.bucketIndex >= 0) {
      const bucketId = `epoch:${epochNumber}:b:${userIndex.bucketIndex}`;
      const bucket = await context.ScoreBucket.get(bucketId);
      if (bucket) {
        const updatedBucket = {
          ...bucket,
          count: bucket.count > 0 ? bucket.count - 1 : 0,
          updatedAt: timestamp,
        };
        context.ScoreBucket.set(updatedBucket);
        if (syncGlobal) {
          await syncGlobalScoreBucket(context, epochNumber, updatedBucket);
        }
      }
    }

    const totalsId = `epoch:${epochNumber}`;
    const totals = await context.LeaderboardTotals.get(totalsId);
    if (totals) {
      const nextTotal = totals.totalUsers > 0 ? totals.totalUsers - 1 : 0;
      context.LeaderboardTotals.set({
        ...totals,
        totalUsers: nextTotal,
        updatedAt: timestamp,
      });
      if (syncGlobal) {
        context.LeaderboardTotals.set({
          id: 'global',
          epochNumber,
          totalUsers: nextTotal,
          updatedAt: timestamp,
        });
      }
    }

    context.UserIndex.deleteUnsafe(userIndexId);
  }

  if (syncGlobal) {
    const globalUserIndex = await context.UserIndex.get(userId);
    if (globalUserIndex) {
      context.UserIndex.deleteUnsafe(userId);
    }
  }

  await removeUserFromTopK(context, epochNumber, userId, timestamp);
  if (syncGlobal) {
    await removeUserFromTopK(context, epochNumber, userId, timestamp, 'global');
  }
}

export async function removeUserFromLeaderboards(
  context: handlerContext,
  userId: string,
  timestamp: number
): Promise<void> {
  const state = await context.LeaderboardState.get('current');
  if (state && state.currentEpochNumber > 0n) {
    await removeUserFromLeaderboardForEpoch(
      context,
      userId,
      state.currentEpochNumber,
      timestamp,
      true
    );
  }

  await removeUserFromLeaderboardForEpoch(context, userId, ALL_TIME_EPOCH_NUMBER, timestamp, false);
}

async function syncGlobalTopK(
  context: handlerContext,
  epochNumber: bigint,
  entries: TopKEntryEntity[],
  timestamp: number
) {
  const globalTopK = await getOrInitTopK(context, epochNumber, 'global');
  const nextEntries: string[] = [];

  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    const globalEntryId = `global:${entry.userId}`;
    nextEntries.push(globalEntryId);
    context.TopKEntry.set({
      id: globalEntryId,
      epochNumber,
      userId: entry.userId,
      points: entry.points,
      rank: i + 1,
    });
  }

  const keepIds = new Set(nextEntries);
  for (const entryId of globalTopK.entries ?? []) {
    if (!keepIds.has(entryId)) {
      context.TopKEntry.deleteUnsafe(entryId);
    }
  }

  context.TopK.set({
    ...globalTopK,
    epochNumber,
    entries: nextEntries,
    updatedAt: timestamp,
  });
}

async function getOrInitLeaderboardTotals(context: handlerContext, epochNumber: bigint) {
  const id = `epoch:${epochNumber}`;
  let totals = await context.LeaderboardTotals.get(id);
  if (!totals) {
    totals = {
      id,
      epochNumber,
      totalUsers: 0,
      updatedAt: 0,
    };
    context.LeaderboardTotals.set(totals);
  }
  return totals;
}

async function getOrInitScoreBucket(
  context: handlerContext,
  epochNumber: bigint,
  index: number,
  timestamp: number
) {
  const id = `epoch:${epochNumber}:b:${index}`;
  let bucket = await context.ScoreBucket.get(id);
  if (!bucket) {
    const bounds = bucketBounds(index);
    bucket = {
      id,
      epochNumber,
      index,
      lower: bounds[0],
      upper: bounds[1],
      count: 0,
      updatedAt: timestamp,
    };
    context.ScoreBucket.set(bucket);
  }
  return bucket;
}

async function syncGlobalScoreBucket(
  context: handlerContext,
  epochNumber: bigint,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  bucket: any
) {
  const state = await context.LeaderboardState.get('current');
  if (!state || state.currentEpochNumber !== epochNumber) return;

  const globalBucketId = `b:${bucket.index}`;
  let globalBucket = await context.ScoreBucket.get(globalBucketId);
  if (!globalBucket) {
    const bounds = bucketBounds(bucket.index);
    context.ScoreBucket.set({
      id: globalBucketId,
      epochNumber,
      index: bucket.index,
      lower: bounds[0],
      upper: bounds[1],
      count: bucket.count,
      updatedAt: bucket.updatedAt || 0,
    });
  } else {
    context.ScoreBucket.set({
      ...globalBucket,
      count: bucket.count,
    });
  }
}

/**
 * Update TopK with new user points
 * Maintains sorted order (desc by points), max length k
 */
async function upsertTopK(
  context: handlerContext,
  epochNumber: bigint,
  userId: string,
  points: number,
  timestamp: number,
  syncGlobal = true
) {
  const normalizedPoints = normalizePoints(points);
  const topK = await getOrInitTopK(context, epochNumber);
  const entryMap = new Map<string, TopKEntryEntity>();

  for (const entryId of topK.entries ?? []) {
    const entry = await context.TopKEntry.get(entryId);
    if (!entry) continue;
    entryMap.set(entry.userId, {
      ...entry,
      points: normalizePoints(entry.points),
    });
  }

  entryMap.set(userId, {
    id: `epoch:${epochNumber.toString()}:${userId}`,
    epochNumber,
    userId,
    points: normalizedPoints,
    rank: 0,
  });

  const sortedEntries = sortTopKEntries([...entryMap.values()]);
  const topEntries = sortedEntries.slice(0, MAX_TOP_K);
  const nextEntries: string[] = [];

  for (let i = 0; i < topEntries.length; i++) {
    const entry = topEntries[i];
    nextEntries.push(entry.id);
    context.TopKEntry.set({
      ...entry,
      rank: i + 1,
    });
  }

  const keepIds = new Set(nextEntries);
  for (const entryId of topK.entries ?? []) {
    if (!keepIds.has(entryId)) {
      context.TopKEntry.deleteUnsafe(entryId);
    }
  }

  context.TopK.set({
    ...topK,
    entries: nextEntries,
    updatedAt: timestamp,
  });

  if (syncGlobal) {
    await syncGlobalTopK(context, epochNumber, topEntries, timestamp);
  }
}

type LeaderboardUpdateOptions = {
  epochNumber: bigint;
  syncGlobal: boolean;
  updateUserState: boolean;
};

async function updateLeaderboardForEpoch(
  context: handlerContext,
  userId: string,
  newPoints: number,
  timestamp: number,
  options: LeaderboardUpdateOptions
): Promise<void> {
  const epochNumber = options.epochNumber;
  const syncGlobal = options.syncGlobal;
  const updateUserState = options.updateUserState;

  newPoints = normalizePoints(newPoints);

  // Load or init UserIndex
  const userIndexId = `${userId}:${epochNumber}`;
  const existingUserIndex = await context.UserIndex.get(userIndexId);
  const hadPointsBefore = (existingUserIndex?.bucketIndex ?? -1) >= 0;
  const oldBucketIndex = existingUserIndex?.bucketIndex ?? -1;

  const userIndex = existingUserIndex ?? {
    id: userIndexId,
    user: userId,
    epochNumber,
    bucketIndex: -1,
    points: 0,
    updatedAt: timestamp,
  };

  // Handle zero-point users
  const isZeroNow = newPoints === 0;
  let didDecrementOldBucket = false;

  if (isZeroNow) {
    if (oldBucketIndex >= 0) {
      const oldBucket = await getOrInitScoreBucket(context, epochNumber, oldBucketIndex, timestamp);
      const updatedOldBucket = {
        ...oldBucket,
        count: oldBucket.count > 0 ? oldBucket.count - 1 : 0,
        updatedAt: timestamp,
      };
      context.ScoreBucket.set(updatedOldBucket);
      if (syncGlobal) {
        await syncGlobalScoreBucket(context, epochNumber, updatedOldBucket);
      }
      didDecrementOldBucket = true;
    }

    context.UserIndex.set({
      ...userIndex,
      points: newPoints,
      bucketIndex: -1,
      updatedAt: timestamp,
    });

    if (updateUserState) {
      const userState = await getOrCreateUserLeaderboardState(context, userId, timestamp);
      context.UserLeaderboardState.set({
        ...userState,
        lastUpdate: timestamp,
      });
    }

    if (hadPointsBefore) {
      const totals = await getOrInitLeaderboardTotals(context, epochNumber);
      context.LeaderboardTotals.set({
        ...totals,
        totalUsers: totals.totalUsers > 0 ? totals.totalUsers - 1 : 0,
        updatedAt: timestamp,
      });
    }

    await upsertTopK(context, epochNumber, userId, newPoints, timestamp, syncGlobal);
    return;
  }

  // Compute new bucket index
  const newBucketIndex = bucketIndexFor(newPoints);

  // Skip bucket updates if staying in same bucket
  if (oldBucketIndex >= 0 && oldBucketIndex === newBucketIndex) {
    context.UserIndex.set({
      ...userIndex,
      points: newPoints,
      updatedAt: timestamp,
    });
    await upsertTopK(context, epochNumber, userId, newPoints, timestamp, syncGlobal);
    return;
  }

  // Decrement old bucket
  if (oldBucketIndex >= 0 && !didDecrementOldBucket) {
    const oldBucket = await getOrInitScoreBucket(context, epochNumber, oldBucketIndex, timestamp);
    const updatedOldBucket = {
      ...oldBucket,
      count: oldBucket.count > 0 ? oldBucket.count - 1 : 0,
      updatedAt: timestamp,
    };
    context.ScoreBucket.set(updatedOldBucket);
    if (syncGlobal) {
      await syncGlobalScoreBucket(context, epochNumber, updatedOldBucket);
    }
  }

  // Increment new bucket
  const newBucket = await getOrInitScoreBucket(context, epochNumber, newBucketIndex, timestamp);
  const newCount = newBucket.count < 2147483647 ? newBucket.count + 1 : newBucket.count;
  const updatedNewBucket = {
    ...newBucket,
    count: newCount,
    updatedAt: timestamp,
  };
  context.ScoreBucket.set(updatedNewBucket);
  if (syncGlobal) {
    await syncGlobalScoreBucket(context, epochNumber, updatedNewBucket);
  }

  // Update UserIndex
  context.UserIndex.set({
    ...userIndex,
    points: newPoints,
    bucketIndex: newBucketIndex,
    updatedAt: timestamp,
  });

  if (updateUserState) {
    const userState = await getOrCreateUserLeaderboardState(context, userId, timestamp);
    context.UserLeaderboardState.set({
      ...userState,
      lastUpdate: timestamp,
    });
  }

  // Increment total users if crossing 0 → >0 threshold
  if (!hadPointsBefore && newPoints > 0) {
    const totals = await getOrInitLeaderboardTotals(context, epochNumber);
    const newTotalUsers =
      totals.totalUsers < 2147483647 ? totals.totalUsers + 1 : totals.totalUsers;
    context.LeaderboardTotals.set({
      ...totals,
      totalUsers: newTotalUsers,
      updatedAt: timestamp,
    });
  }

  // Update TopK
  await upsertTopK(context, epochNumber, userId, newPoints, timestamp, syncGlobal);

  if (!syncGlobal) {
    return;
  }

  // Sync global UserIndex for frontend
  const globalUserIndex = {
    id: userId,
    user: userId,
    epochNumber,
    points: newPoints,
    bucketIndex: newBucketIndex,
    updatedAt: timestamp,
  };
  context.UserIndex.set(globalUserIndex);

  // Sync global LeaderboardTotals
  const epochTotals = await context.LeaderboardTotals.get(`epoch:${epochNumber}`);
  if (epochTotals) {
    context.LeaderboardTotals.set({
      id: 'global',
      epochNumber,
      totalUsers: epochTotals.totalUsers,
      updatedAt: timestamp,
    });
  }
}

/**
 * Main entry point: Update leaderboard when user's points change
 */
export async function updateLeaderboard(
  context: handlerContext,
  userId: string,
  newPoints: number,
  timestamp: number
): Promise<void> {
  if (await isUserBlacklisted(context, userId)) {
    return;
  }
  const state = await context.LeaderboardState.get('current');
  if (!state || state.currentEpochNumber === 0n) {
    return;
  }
  const epochNumber = state.currentEpochNumber;

  const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
  if (!epoch) {
    return;
  }

  await updateLeaderboardForEpoch(context, userId, newPoints, timestamp, {
    epochNumber,
    syncGlobal: true,
    updateUserState: true,
  });
}

export async function updateAllTimeLeaderboard(
  context: handlerContext,
  userId: string,
  lifetimePoints: number,
  timestamp: number
): Promise<void> {
  if (await isUserBlacklisted(context, userId)) {
    return;
  }
  await updateLeaderboardForEpoch(context, userId, lifetimePoints, timestamp, {
    epochNumber: ALL_TIME_EPOCH_NUMBER,
    syncGlobal: false,
    updateUserState: false,
  });
}
