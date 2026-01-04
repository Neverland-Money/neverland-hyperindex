/**
 * Shared helper functions used across all event handlers
 */

import type { handlerContext } from '../../generated';
import type { PriceOracleAsset_t } from '../../generated/src/db/Entities.gen';
import {
  AUSD_ADDRESS,
  DEFAULT_BORROW_RATE_BPS,
  DEFAULT_DEPOSIT_RATE_BPS,
  DUST_LOCK_START_BLOCK,
  EARNAUSD_ADDRESS,
  LEADERBOARD_START_BLOCK,
  MAX_MULTIPLIER,
  POINTS_SCALE,
  SECONDS_PER_DAY,
  USDC_ADDRESS,
  USDT0_ADDRESS,
  WBTC_ADDRESS,
  WETH_ADDRESS,
  WMON_ADDRESS,
  SHMON_ADDRESS,
  ZERO_ADDRESS,
  normalizeAddress,
  toScaledPoints,
} from '../helpers/constants';
import { readNFTBalance } from '../helpers/viem';
import { calculateVotingPower, getCurrentDay } from '../helpers/points';
import {
  calculateCompoundedInterest,
  calculateLinearInterest,
  rayMul,
  toDecimal,
} from '../helpers/math';
import { createDefaultUser } from '../helpers/entityHelpers';
import { getTestnetBonusBps } from '../helpers/testnetTiers';

const BASIS_POINTS = 10000n;
const HOURS_PER_DAY_BI = BigInt(SECONDS_PER_DAY / 3600);
const MAX_VP_TIERS = 20;
const MAX_VP_MULTIPLIER = 50000n;
const MAX_COMBINED_MULTIPLIER = 100000n;
const DEFAULT_COOLDOWN_SECONDS = 3600;
const E8_DIVISOR = 1e8;
const VP_DECIMALS = 1e18;
const RAY = 10n ** 27n;
export function shouldUseEthCalls(): boolean {
  return !(
    process.env.ENVIO_DISABLE_EXTERNAL_CALLS === 'true' ||
    process.env.ENVIO_DISABLE_ETH_CALLS === 'true'
  );
}

function shouldSyncNFTOwnershipFromChain(): boolean {
  return shouldUseEthCalls() && process.env.ENVIO_ENABLE_NFT_CHAIN_SYNC === 'true';
}

function shouldSyncLPPositionsFromChain(): boolean {
  return shouldUseEthCalls() && process.env.ENVIO_ENABLE_LP_CHAIN_SYNC === 'true';
}

// ============================================
// User & Protocol Helpers
// ============================================

export async function getOrCreateUser(context: handlerContext, userId: string) {
  const normalizedUserId = normalizeAddress(userId);
  let user = await context.User.get(normalizedUserId);
  if (!user) {
    user = createDefaultUser(normalizedUserId);
    context.User.set(user);

    let ps = await context.ProtocolStats.get('1');
    if (ps) {
      context.ProtocolStats.set({
        ...ps,
        uniqueUsers: ps.uniqueUsers + 1,
      });
    }
  }
  return user;
}

export async function getOrCreateProtocolStats(context: handlerContext, timestamp: number) {
  let ps = await context.ProtocolStats.get('1');
  if (!ps) {
    ps = {
      id: '1',
      tvlUsd: 0,
      suppliesUsd: 0,
      borrowsUsd: 0,
      availableUsd: 0,
      tvlE8: 0n,
      suppliesE8: 0n,
      borrowsE8: 0n,
      availableE8: 0n,
      totalRevenueUsd: 0,
      supplyRevenueUsd: 0,
      protocolRevenueUsd: 0,
      updatedAt: timestamp,
      totalTransactions: 0n,
      totalSelfRepayVolume: 0n,
      totalSelfRepayCount: 0n,
      totalDustTransfers: 0n,
      uniqueUsers: 0,
      lastTxTimestamp: timestamp,
      lastTxHash: '0x',
    };
    context.ProtocolStats.set(ps);
  }
  return ps;
}

export async function recordProtocolTransaction(
  context: handlerContext,
  txHash: string,
  timestamp: number,
  blockNumber?: bigint
  // logIndex?: number,
  // eventLabel?: string
): Promise<void> {
  // const blockLabel = blockNumber !== undefined ? blockNumber.toString() : 'n/a';
  // const logIndexLabel = logIndex !== undefined ? logIndex.toString() : 'n/a';
  // const eventPrefix = eventLabel ? `${eventLabel} ` : '';
  // context.log.debug(
  //   `${eventPrefix}event received tx=${txHash} ts=${timestamp} block=${blockLabel} logIndex=${logIndexLabel}`
  // );

  await applyScheduledEpochTransitions(context, timestamp, blockNumber);
  let ps = await getOrCreateProtocolStats(context, timestamp);
  if (ps.lastTxHash !== txHash) {
    const updatedPs = {
      ...ps,
      totalTransactions: ps.totalTransactions + 1n,
      lastTxHash: txHash,
      lastTxTimestamp: timestamp,
    };
    context.ProtocolStats.set(updatedPs);

    // Create periodic snapshots
    if (blockNumber !== undefined) {
      await maybeCreateProtocolSnapshot(context, updatedPs, txHash, timestamp, blockNumber);
    }
  }
}

async function maybeCreateProtocolSnapshot(
  context: handlerContext,
  ps: {
    tvlUsd: number;
    suppliesUsd: number;
    borrowsUsd: number;
    availableUsd: number;
    tvlE8: bigint;
    suppliesE8: bigint;
    borrowsE8: bigint;
    availableE8: bigint;
    totalRevenueUsd: number;
    supplyRevenueUsd: number;
    protocolRevenueUsd: number;
    totalTransactions: bigint;
    uniqueUsers: number;
  },
  txHash: string,
  timestamp: number,
  blockNumber: bigint
): Promise<void> {
  // Store one snapshot per timestamp; later events in the same second overwrite.
  const snapshotId = `${timestamp}`;
  context.ProtocolStatsSnapshot.set({
    id: snapshotId,
    timestamp,
    blockNumber,
    txHash,
    tvlUsd: ps.tvlUsd,
    suppliesUsd: ps.suppliesUsd,
    borrowsUsd: ps.borrowsUsd,
    availableUsd: ps.availableUsd,
    tvlE8: ps.tvlE8,
    suppliesE8: ps.suppliesE8,
    borrowsE8: ps.borrowsE8,
    availableE8: ps.availableE8,
    totalRevenueUsd: ps.totalRevenueUsd,
    supplyRevenueUsd: ps.supplyRevenueUsd,
    protocolRevenueUsd: ps.protocolRevenueUsd,
    totalTransactions: ps.totalTransactions,
    uniqueUsers: ps.uniqueUsers,
  });
}

const MAX_SCHEDULED_TRANSITIONS = 5;

export async function applyScheduledEpochTransitions(
  context: handlerContext,
  timestamp: number,
  blockNumber?: bigint
): Promise<void> {
  const state = await context.LeaderboardState.get('current');
  let currentEpochNumber = state?.currentEpochNumber ?? 0n;
  let isActive = state?.isActive ?? false;
  let updated = false;

  if (currentEpochNumber === 0n) {
    isActive = false;
  }

  for (let i = 0; i < MAX_SCHEDULED_TRANSITIONS; i += 1) {
    if (isActive) {
      const epoch = await context.LeaderboardEpoch.get(currentEpochNumber.toString());
      if (!epoch) break;

      const scheduledEndTime = epoch.scheduledEndTime ?? 0;
      if (scheduledEndTime > 0 && scheduledEndTime <= timestamp) {
        const endTime =
          epoch.endTime !== undefined && epoch.endTime > 0
            ? Math.min(epoch.endTime, scheduledEndTime)
            : scheduledEndTime;
        const duration =
          epoch.startTime > 0 && endTime >= epoch.startTime
            ? BigInt(endTime - epoch.startTime)
            : undefined;
        const endBlock =
          epoch.endBlock && epoch.endBlock > 0n
            ? epoch.endBlock
            : blockNumber !== undefined
              ? blockNumber
              : epoch.endBlock;

        context.LeaderboardEpoch.set({
          ...epoch,
          endBlock,
          endTime,
          isActive: false,
          duration,
        });
        const { settleAllLPPoolPositions } = await import('./lp');
        await settleAllLPPoolPositions(context, endTime);

        isActive = false;
        updated = true;
        continue;
      }

      const nextEpochNumber = currentEpochNumber + 1n;
      const nextEpoch = await context.LeaderboardEpoch.get(nextEpochNumber.toString());
      const scheduledStartTime = nextEpoch?.scheduledStartTime ?? 0;

      if (scheduledStartTime > 0 && scheduledStartTime <= timestamp) {
        const endTime =
          epoch.endTime !== undefined && epoch.endTime > 0
            ? Math.min(epoch.endTime, scheduledStartTime)
            : scheduledStartTime;
        const duration =
          epoch.startTime > 0 && endTime >= epoch.startTime
            ? BigInt(endTime - epoch.startTime)
            : undefined;
        const endBlock =
          epoch.endBlock && epoch.endBlock > 0n
            ? epoch.endBlock
            : blockNumber !== undefined
              ? blockNumber
              : epoch.endBlock;

        context.LeaderboardEpoch.set({
          ...epoch,
          endBlock,
          endTime,
          isActive: false,
          duration,
        });
        const { settleAllLPPoolPositions } = await import('./lp');
        await settleAllLPPoolPositions(context, endTime);

        const startTime =
          nextEpoch && nextEpoch.startTime > 0
            ? Math.min(nextEpoch.startTime, scheduledStartTime)
            : scheduledStartTime;
        const startBlock =
          nextEpoch?.startBlock && nextEpoch.startBlock > 0n
            ? nextEpoch.startBlock
            : blockNumber !== undefined
              ? blockNumber
              : (nextEpoch?.startBlock ?? 0n);

        if (nextEpoch) {
          context.LeaderboardEpoch.set({
            ...nextEpoch,
            startBlock,
            startTime,
            endBlock: undefined,
            endTime: undefined,
            isActive: true,
            duration: undefined,
          });
        }

        currentEpochNumber = nextEpochNumber;
        isActive = true;
        updated = true;
        continue;
      }

      break;
    }

    const nextEpochNumber = currentEpochNumber === 0n ? 1n : currentEpochNumber + 1n;
    const nextEpoch = await context.LeaderboardEpoch.get(nextEpochNumber.toString());
    if (!nextEpoch) break;

    const scheduledStartTime = nextEpoch.scheduledStartTime ?? 0;
    if (scheduledStartTime > 0 && scheduledStartTime <= timestamp) {
      const startTime =
        nextEpoch.startTime > 0
          ? Math.min(nextEpoch.startTime, scheduledStartTime)
          : scheduledStartTime;
      const startBlock =
        nextEpoch.startBlock > 0n
          ? nextEpoch.startBlock
          : blockNumber !== undefined
            ? blockNumber
            : nextEpoch.startBlock;

      context.LeaderboardEpoch.set({
        ...nextEpoch,
        startBlock,
        startTime,
        endBlock: undefined,
        endTime: undefined,
        isActive: true,
        duration: undefined,
      });

      currentEpochNumber = nextEpochNumber;
      isActive = true;
      updated = true;
      continue;
    }

    break;
  }

  if (updated) {
    context.LeaderboardState.set({
      id: 'current',
      currentEpochNumber,
      isActive,
    });
  }
}

// ============================================
// Token List Helpers
// ============================================

export async function updateUserTokenList(
  context: handlerContext,
  userAddress: string,
  tokenId: bigint,
  timestamp: number,
  action: 'add' | 'remove'
): Promise<void> {
  const normalizedUser = normalizeAddress(userAddress);
  let userTokens = await context.UserTokenList.get(normalizedUser);
  let tokenIds: bigint[] = userTokens?.tokenIds || [];

  if (action === 'add') {
    let alreadyExists = false;
    for (const existing of tokenIds) {
      if (existing === tokenId) {
        alreadyExists = true;
        break;
      }
    }
    if (!alreadyExists) {
      tokenIds = [...tokenIds, tokenId];
    }
  } else {
    tokenIds = tokenIds.filter((id: bigint) => id !== tokenId);
  }

  context.UserTokenList.set({
    id: normalizedUser,
    user_id: normalizedUser,
    tokenIds,
    lastUpdate: timestamp,
  });
}

export async function addReserveToUserList(
  context: handlerContext,
  userAddress: string,
  reserveId: string,
  timestamp: number
): Promise<void> {
  const normalizedUser = normalizeAddress(userAddress);
  const normalizedReserveId = reserveId.toLowerCase();
  let userReserves = await context.UserReserveList.get(normalizedUser);
  const reserveIds = userReserves?.reserveIds || [];

  if (!reserveIds.includes(normalizedReserveId)) {
    const nextIds = [...reserveIds, normalizedReserveId];
    context.UserReserveList.set({
      id: normalizedUser,
      user_id: normalizedUser,
      reserveIds: nextIds,
      lastUpdate: timestamp,
    });
    return;
  }

  if (userReserves) {
    context.UserReserveList.set({
      ...userReserves,
      lastUpdate: timestamp,
    });
  }
}

// ============================================
// Voting Power Helpers
// ============================================

async function calculateTokenVotingPower(
  token: { lockedAmount: bigint; end: number; isPermanent: boolean },
  currentTimestamp: number
): Promise<bigint> {
  return calculateVotingPower(token.lockedAmount, token.end, token.isPermanent, currentTimestamp);
}

export async function calculateAverageTokenVotingPower(
  token: { lockedAmount: bigint; end: number; isPermanent: boolean },
  startTimestamp: number,
  endTimestamp: number
): Promise<bigint> {
  if (endTimestamp <= startTimestamp) {
    return calculateTokenVotingPower(token, endTimestamp);
  }

  if (token.lockedAmount === 0n) return 0n;
  if (token.isPermanent) return token.lockedAmount;
  if (token.end <= startTimestamp) return 0n;

  const effectiveEnd = Math.min(endTimestamp, token.end);
  const vpStart = await calculateTokenVotingPower(token, startTimestamp);
  const vpEnd = await calculateTokenVotingPower(token, effectiveEnd);
  const activeDuration = effectiveEnd - startTimestamp;
  const totalDuration = endTimestamp - startTimestamp;

  const avgOverActive = (vpStart + vpEnd) / 2n;
  if (endTimestamp <= token.end || totalDuration === activeDuration) {
    return avgOverActive;
  }

  return (avgOverActive * BigInt(activeDuration)) / BigInt(totalDuration);
}

export async function calculateCurrentVPFromStorage(
  context: handlerContext,
  userAddress: string,
  currentTimestamp: number
): Promise<bigint> {
  const normalizedUser = normalizeAddress(userAddress);
  const userTokens = await context.UserTokenList.get(normalizedUser);
  if (!userTokens || !userTokens.tokenIds) {
    return 0n;
  }

  let totalVP = 0n;
  for (const tokenId of userTokens.tokenIds) {
    const token = await context.DustLockToken.get(tokenId.toString());
    if (!token) continue;

    const vp = await calculateTokenVotingPower(token, currentTimestamp);
    totalVP += vp;
  }

  return totalVP;
}

async function calculateAverageVPFromStorage(
  context: handlerContext,
  userAddress: string,
  startTimestamp: number,
  endTimestamp: number
): Promise<bigint> {
  if (endTimestamp <= startTimestamp) {
    return calculateCurrentVPFromStorage(context, userAddress, endTimestamp);
  }

  const normalizedUser = normalizeAddress(userAddress);
  const userTokens = await context.UserTokenList.get(normalizedUser);
  if (!userTokens || !userTokens.tokenIds) {
    return 0n;
  }

  let totalVP = 0n;
  for (const tokenId of userTokens.tokenIds) {
    const token = await context.DustLockToken.get(tokenId.toString());
    if (!token) continue;

    const vp = await calculateAverageTokenVotingPower(token, startTimestamp, endTimestamp);
    totalVP += vp;
  }

  return totalVP;
}

export async function getOrCreateUserLeaderboardState(
  context: handlerContext,
  userId: string,
  timestamp: number
) {
  const normalizedUserId = normalizeAddress(userId);
  let state = await context.UserLeaderboardState.get(normalizedUserId);
  if (!state) {
    state = {
      id: normalizedUserId,
      user_id: normalizedUserId,
      nftCount: 0n,
      nftMultiplier: 10000n,
      votingPower: 0n,
      vpTierIndex: 0n,
      vpMultiplier: 10000n,
      combinedMultiplier: 10000n,
      totalEpochsParticipated: 0n,
      lifetimePoints: 0n,
      currentEpochId: undefined,
      currentEpochRank: undefined,
      lastUpdate: timestamp,
    };
    context.UserLeaderboardState.set(state);
  }
  return state;
}

export async function calculateVPMultiplier(
  context: handlerContext,
  votingPower: bigint
): Promise<bigint> {
  let multiplier = BASIS_POINTS;

  for (let i = 0; i < MAX_VP_TIERS; i++) {
    const tier = await context.VotingPowerTier.get(i.toString());
    if (!tier || !tier.isActive) continue;

    if (votingPower >= tier.minVotingPower) {
      multiplier = tier.multiplierBps;
    } else {
      break;
    }
  }

  if (multiplier > MAX_VP_MULTIPLIER) {
    multiplier = MAX_VP_MULTIPLIER;
  }

  return multiplier;
}

export async function findVPTierIndex(
  context: handlerContext,
  votingPower: bigint
): Promise<bigint> {
  let tierIndex = 0n;

  for (let i = 0; i < MAX_VP_TIERS; i++) {
    const tier = await context.VotingPowerTier.get(i.toString());
    if (!tier || !tier.isActive) continue;

    if (votingPower >= tier.minVotingPower) {
      tierIndex = tier.tierIndex;
    } else {
      break;
    }
  }

  return tierIndex;
}

export async function calculateNFTMultiplierFromCount(
  context: handlerContext,
  collectionCount: bigint
): Promise<bigint> {
  const MIN_MULTIPLIER_BPS = BASIS_POINTS;

  if (collectionCount <= 0n) {
    return MIN_MULTIPLIER_BPS;
  }

  const config = await context.NFTMultiplierConfig.get('current');
  if (!config) {
    return MIN_MULTIPLIER_BPS;
  }

  let totalMultiplier = MIN_MULTIPLIER_BPS;
  let currentBonus = config.firstBonus;

  for (let i = 0n; i < collectionCount; i++) {
    totalMultiplier = totalMultiplier + currentBonus;
    currentBonus = (currentBonus * config.decayRatio) / BASIS_POINTS;
  }

  const MAX_NFT_MULTIPLIER = 50000n;
  if (totalMultiplier > MAX_NFT_MULTIPLIER) {
    totalMultiplier = MAX_NFT_MULTIPLIER;
  }

  return totalMultiplier;
}

async function syncUserNFTOwnershipFromChain(
  context: handlerContext,
  userId: string,
  timestamp: number,
  blockNumber?: bigint
): Promise<void> {
  const normalizedUserId = normalizeAddress(userId);
  if (!shouldSyncNFTOwnershipFromChain()) {
    return;
  }

  // Baseline each (user, collection) once, then rely on transfer events.

  const registryState = await context.NFTPartnershipRegistryState.get('current');
  const activeCollections = registryState?.activeCollections ?? [];
  if (activeCollections.length === 0) {
    return;
  }

  let state = null as null | Awaited<ReturnType<typeof getOrCreateUserLeaderboardState>>;

  for (const collection of activeCollections) {
    const normalizedCollection = normalizeAddress(collection);
    const baselineId = `${normalizedUserId}:${normalizedCollection}`;
    const baseline = await context.UserNFTBaseline.get(baselineId);
    if (baseline) {
      continue;
    }

    const ownershipId = `${normalizedUserId}:${normalizedCollection}`;
    const ownership = await context.UserNFTOwnership.get(ownershipId);
    const oldBalance = ownership?.balance ?? 0n;

    const balance = await readNFTBalance(normalizedCollection, normalizedUserId, blockNumber);
    if (balance === null) {
      continue;
    }

    const hasNFT = balance > 0n;
    const wasOwning = oldBalance > 0n;

    if (hasNFT) {
      context.UserNFTOwnership.set({
        id: ownershipId,
        user_id: normalizedUserId,
        partnership_id: normalizedCollection,
        balance,
        hasNFT,
        lastCheckedAt: timestamp,
        lastCheckedBlock: blockNumber ?? 0n,
      });
    } else if (ownership) {
      context.UserNFTOwnership.deleteUnsafe(ownershipId);
    }

    if (wasOwning === hasNFT) {
      context.UserNFTBaseline.set({
        id: baselineId,
        user_id: normalizedUserId,
        partnership_id: normalizedCollection,
        checkedAt: timestamp,
        checkedBlock: blockNumber ?? 0n,
      });
      continue;
    }

    if (!state) {
      state = await getOrCreateUserLeaderboardState(context, normalizedUserId, timestamp);
    }

    let newNftCount = state.nftCount;
    if (hasNFT && !wasOwning) {
      newNftCount = state.nftCount + 1n;
    } else if (!hasNFT && wasOwning) {
      newNftCount = state.nftCount > 0n ? state.nftCount - 1n : 0n;
    }

    const newNftMultiplier = await calculateNFTMultiplierFromCount(context, newNftCount);
    let combinedMultiplier = (newNftMultiplier * state.vpMultiplier) / BASIS_POINTS;
    if (combinedMultiplier > MAX_COMBINED_MULTIPLIER) {
      combinedMultiplier = MAX_COMBINED_MULTIPLIER;
    }

    state = {
      ...state,
      nftCount: newNftCount,
      nftMultiplier: newNftMultiplier,
      combinedMultiplier,
      lastUpdate: timestamp,
    };

    context.UserLeaderboardState.set(state);

    context.UserNFTBaseline.set({
      id: baselineId,
      user_id: normalizedUserId,
      partnership_id: normalizedCollection,
      checkedAt: timestamp,
      checkedBlock: blockNumber ?? 0n,
    });
  }
}

export function createVPHistoryEntry(
  context: handlerContext,
  userAddress: string,
  tokenId: bigint,
  votingPower: bigint,
  timestamp: number,
  txHash: string,
  eventType: string,
  logIndex: number
): void {
  const normalizedUser = normalizeAddress(userAddress);
  const historyId = `${normalizedUser}:${timestamp}:${txHash}:${logIndex}`;
  context.UserVotingPowerHistory.set({
    id: historyId,
    user: normalizedUser,
    tokenId,
    votingPower,
    timestamp,
    txHash,
    eventType,
  });
}

export async function updateUserVotingPower(
  context: handlerContext,
  userAddress: string,
  tokenId: bigint,
  newVotingPower: bigint,
  timestamp: number,
  txHash: string,
  eventType: string,
  logIndex: number
): Promise<void> {
  const normalizedUser = normalizeAddress(userAddress);
  const state = await getOrCreateUserLeaderboardState(context, normalizedUser, timestamp);
  const oldVP = state.votingPower;

  const vpMultiplier = await calculateVPMultiplier(context, newVotingPower);
  const vpTierIndex = await findVPTierIndex(context, newVotingPower);

  let combinedMultiplier = (state.nftMultiplier * vpMultiplier) / BASIS_POINTS;
  if (combinedMultiplier > MAX_COMBINED_MULTIPLIER) {
    combinedMultiplier = MAX_COMBINED_MULTIPLIER;
  }

  const updatedState = {
    ...state,
    votingPower: newVotingPower,
    vpMultiplier,
    vpTierIndex,
    combinedMultiplier,
    lastUpdate: timestamp,
  };

  context.UserLeaderboardState.set(updatedState);

  if (oldVP !== newVotingPower) {
    createMultiplierSnapshot(context, updatedState, timestamp, txHash, eventType, logIndex);
  }

  createVPHistoryEntry(
    context,
    normalizedUser,
    tokenId,
    newVotingPower,
    timestamp,
    txHash,
    eventType,
    logIndex
  );
}

// ============================================
// Price Oracle Helpers
// ============================================

export async function getAssetPriceUSD(
  context: handlerContext,
  assetAddress: string,
  timestamp?: number
): Promise<number> {
  const normalizedAsset = normalizeAddress(assetAddress);
  let priceOracle = await context.PriceOracleAsset.get(normalizedAsset);
  if (!priceOracle) {
    await ensureAssetPrice(context, normalizedAsset, timestamp ?? 0);
    priceOracle = await context.PriceOracleAsset.get(normalizedAsset);
    if (!priceOracle) return 0;
  }

  // Use lastPriceUsd if available, otherwise calculate from priceInEth
  if (priceOracle.lastPriceUsd && priceOracle.lastPriceUsd > 0) {
    return priceOracle.lastPriceUsd;
  }

  const USD_PRECISION = 1e8;
  return Number(priceOracle.priceInEth) / USD_PRECISION;
}

export async function ensureAssetPrice(
  context: handlerContext,
  assetAddress: string,
  timestamp: number
): Promise<void> {
  const normalizedAsset = normalizeAddress(assetAddress);
  const existing = await context.PriceOracleAsset.get(normalizedAsset);
  if (existing && existing.lastUpdateTimestamp > 0) return;

  let seedTimestamp = timestamp;
  if (timestamp > 0) {
    type LeaderboardStores = Pick<handlerContext, 'LeaderboardState' | 'LeaderboardEpoch'>;
    const stores: Partial<LeaderboardStores> = context;
    const state = await stores.LeaderboardState?.get('current');
    if (state && state.currentEpochNumber > 0n) {
      const epoch = await stores.LeaderboardEpoch?.get(state.currentEpochNumber.toString());
      if (epoch?.startTime && epoch.startTime > 0 && epoch.startTime < seedTimestamp) {
        // Seed the price timestamp at epoch start so first settlement accrues from epoch start.
        seedTimestamp = epoch.startTime;
      }
    }
  }

  const assetLc = normalizedAsset;
  let priceInEth = 100000000n; // default $1.00 (1e8)

  if (assetLc === USDC_ADDRESS || assetLc === USDT0_ADDRESS) {
    priceInEth = 100000000n;
  } else if (assetLc === AUSD_ADDRESS || assetLc === EARNAUSD_ADDRESS) {
    priceInEth = 100000000n;
  } else if (assetLc === WETH_ADDRESS) {
    priceInEth = 445000000000n;
  } else if (assetLc === WBTC_ADDRESS) {
    priceInEth = 12000000000000n;
  } else if (assetLc === SHMON_ADDRESS) {
    priceInEth = 22000000000n;
  } else if (assetLc === WMON_ADDRESS) {
    priceInEth = 320000000n;
  }

  const lastPriceUsd = Number(priceInEth) / 1e8;

  context.PriceOracleAsset.set({
    id: normalizedAsset,
    oracle_id: existing?.oracle_id || '',
    priceSource: existing?.priceSource || '',
    dependentAssets: existing?.dependentAssets || [],
    priceType: existing?.priceType || '',
    platform: existing?.platform || '',
    priceInEth,
    isFallbackRequired: false,
    lastUpdateTimestamp: seedTimestamp,
    priceCacheExpiry: existing?.priceCacheExpiry || 0,
    fromChainlinkSourcesRegistry: existing?.fromChainlinkSourcesRegistry || false,
    lastPriceUsd,
    cumulativeUsdPriceHours: existing?.cumulativeUsdPriceHours || 0,
    resetTimestamp: existing?.resetTimestamp || 0,
    resetCumulativeUsdPriceHours: existing?.resetCumulativeUsdPriceHours || 0,
  });
}

export function createMultiplierSnapshot(
  context: handlerContext,
  state: {
    id: string;
    nftCount: bigint;
    nftMultiplier: bigint;
    votingPower: bigint;
    vpMultiplier: bigint;
    combinedMultiplier: bigint;
  },
  timestamp: number,
  txHash: string,
  changeReason: string,
  logIndex?: number
): void {
  const snapshotId = `${state.id}:${timestamp}:${txHash}:${logIndex ?? 0}`;
  context.UserMultiplierSnapshot.set({
    id: snapshotId,
    user_id: state.id,
    timestamp,
    nftCount: state.nftCount,
    nftMultiplier: state.nftMultiplier,
    votingPower: state.votingPower,
    vpMultiplier: state.vpMultiplier,
    combinedMultiplier: state.combinedMultiplier,
    changeReason,
    txHash,
  });
}

export async function recalculateUserTotalVP(
  context: handlerContext,
  userAddress: string,
  timestamp: number,
  txHash: string,
  eventType: string,
  logIndex: number,
  blockNumber?: bigint
): Promise<void> {
  if (blockNumber !== undefined && Number(blockNumber) < DUST_LOCK_START_BLOCK) {
    return;
  }

  const userTokens = await context.UserTokenList.get(userAddress);
  if (!userTokens || !userTokens.tokenIds || userTokens.tokenIds.length === 0) {
    const state = await getOrCreateUserLeaderboardState(context, userAddress, timestamp);

    context.UserLeaderboardState.set({
      ...state,
      votingPower: 0n,
      vpMultiplier: BASIS_POINTS,
      vpTierIndex: 0n,
      combinedMultiplier: state.nftMultiplier,
      lastUpdate: timestamp,
    });
    return;
  }

  let totalVotingPower = 0n;
  let maxVotingPower = 0n;
  let maxTokenId = 0n;

  for (const tokenId of userTokens.tokenIds) {
    const token = await context.DustLockToken.get(tokenId.toString());
    if (!token) continue;

    const vp = await calculateTokenVotingPower(token, timestamp);
    totalVotingPower += vp;

    if (vp > maxVotingPower) {
      maxVotingPower = vp;
      maxTokenId = tokenId;
    }
  }

  const state = await getOrCreateUserLeaderboardState(context, userAddress, timestamp);
  const oldVP = state.votingPower;

  const vpMultiplier = await calculateVPMultiplier(context, totalVotingPower);
  const vpTierIndex = await findVPTierIndex(context, totalVotingPower);

  let combinedMultiplier = (state.nftMultiplier * vpMultiplier) / BASIS_POINTS;
  if (combinedMultiplier > MAX_COMBINED_MULTIPLIER) {
    combinedMultiplier = MAX_COMBINED_MULTIPLIER;
  }

  const updatedState = {
    ...state,
    votingPower: totalVotingPower,
    vpMultiplier,
    vpTierIndex,
    combinedMultiplier,
    lastUpdate: timestamp,
  };

  context.UserLeaderboardState.set(updatedState);

  // Compare oldVP to totalVotingPower (not maxVotingPower) to avoid false snapshots for multi-token users
  if (oldVP !== totalVotingPower) {
    createMultiplierSnapshot(context, updatedState, timestamp, txHash, eventType, logIndex);
  }

  if (maxTokenId > 0n) {
    createVPHistoryEntry(
      context,
      userAddress,
      maxTokenId,
      maxVotingPower,
      timestamp,
      txHash,
      eventType,
      logIndex
    );
  }
}

// ============================================
// Points Settlement Helpers
// ============================================

export async function getOrCreateUserReservePoints(
  context: handlerContext,
  userId: string,
  reserveId: string,
  _timestamp: number
) {
  const normalizedUserId = normalizeAddress(userId);
  const normalizedReserveId = reserveId.toLowerCase();
  const id = `${normalizedUserId}:${normalizedReserveId}`;
  let urp = await context.UserReservePoints.get(id);
  if (!urp) {
    urp = {
      id,
      user_id: normalizedUserId,
      reserve_id: normalizedReserveId,
      lastUpdateTimestamp: 0,
      lastDepositUsd: 0,
      lastBorrowUsd: 0,
      lastDepositIndex: 0,
      lastBorrowIndex: 0,
      lastDepositTokens: 0,
      lastBorrowTokens: 0,
      depositPoints: 0n,
      borrowPoints: 0n,
      totalPoints: 0n,
      resetTimestamp: 0,
      resetDepositIndex: 0,
      resetBorrowIndex: 0,
    };
    context.UserReservePoints.set(urp);
  }
  return urp;
}

export async function getOrCreateUserEpochStats(
  context: handlerContext,
  userId: string,
  epochNumber: bigint,
  timestamp: number
) {
  const normalizedUserId = normalizeAddress(userId);
  const id = `${normalizedUserId}:${epochNumber}`;
  let stats = await context.UserEpochStats.get(id);
  if (!stats) {
    stats = {
      id,
      user_id: normalizedUserId,
      epochNumber,
      depositPoints: 0n,
      borrowPoints: 0n,
      lpPoints: 0n,
      dailySupplyPoints: 0n,
      dailyBorrowPoints: 0n,
      dailyRepayPoints: 0n,
      dailyWithdrawPoints: 0n,
      dailyVPPoints: 0n,
      dailyLPPoints: 0n,
      manualAwardPoints: 0n,
      depositMultiplierBps: BASIS_POINTS,
      borrowMultiplierBps: BASIS_POINTS,
      vpMultiplierBps: BASIS_POINTS,
      lpMultiplierBps: BASIS_POINTS,
      depositPointsWithMultiplier: 0n,
      borrowPointsWithMultiplier: 0n,
      vpPointsWithMultiplier: 0n,
      lpPointsWithMultiplier: 0n,
      lastSupplyPointsDay: -1,
      lastBorrowPointsDay: -1,
      lastRepayPointsDay: -1,
      lastWithdrawPointsDay: -1,
      lastVPPointsDay: -1,
      totalPoints: 0n,
      totalPointsWithMultiplier: 0n,
      totalMultiplierBps: BASIS_POINTS,
      lastAppliedMultiplierBps: BASIS_POINTS,
      testnetBonusBps: 0n,
      rank: 0,
      firstSeenAt: timestamp,
      lastUpdatedAt: 0,
    };
    context.UserEpochStats.set(stats);
  }
  return stats;
}

export async function getOrCreateUserDailyActivity(
  context: handlerContext,
  userId: string,
  day: number,
  timestamp: number
) {
  const normalizedUserId = normalizeAddress(userId);
  const state = await context.LeaderboardState.get('current');
  const epochNumber = state && state.isActive ? state.currentEpochNumber.toString() : '0';
  const id = `${normalizedUserId}:${epochNumber}:${day}`;
  let activity = await context.UserDailyActivity.get(id);
  if (!activity) {
    activity = {
      id,
      user_id: normalizedUserId,
      day,
      hasSupplied: false,
      hasBorrowed: false,
      hasRepaid: false,
      hasWithdrawn: false,
      supplyTimestamp: undefined,
      borrowTimestamp: undefined,
      repayTimestamp: undefined,
      withdrawTimestamp: undefined,
      updatedAt: timestamp,
      dailySupplyUsdHighwater: 0,
      dailyBorrowUsdHighwater: 0,
      dailyRepayUsdHighwater: 0,
      dailyWithdrawUsdHighwater: 0,
    };
    context.UserDailyActivity.set(activity);
  }
  return activity;
}

function recomputeEpochTotalPoints(stats: {
  depositPoints: bigint;
  borrowPoints: bigint;
  lpPoints: bigint;
  dailySupplyPoints: bigint;
  dailyBorrowPoints: bigint;
  dailyRepayPoints: bigint;
  dailyWithdrawPoints: bigint;
  dailyVPPoints: bigint;
  dailyLPPoints: bigint;
  manualAwardPoints: bigint;
}): bigint {
  return (
    stats.depositPoints +
    stats.borrowPoints +
    stats.lpPoints +
    stats.dailySupplyPoints +
    stats.dailyBorrowPoints +
    stats.dailyRepayPoints +
    stats.dailyWithdrawPoints +
    stats.dailyVPPoints +
    stats.dailyLPPoints +
    stats.manualAwardPoints
  );
}

export async function updateLifetimePoints(
  context: handlerContext,
  userId: string,
  epochStats: {
    epochNumber: bigint;
    lastUpdatedAt: number;
  }
): Promise<void> {
  const normalizedUserId = normalizeAddress(userId);
  let userPoints = await context.UserPoints.get(normalizedUserId);
  if (!userPoints) {
    userPoints = {
      id: normalizedUserId,
      user_id: normalizedUserId,
      lifetimeDepositPoints: 0n,
      lifetimeBorrowPoints: 0n,
      lifetimeDailySupplyPoints: 0n,
      lifetimeDailyBorrowPoints: 0n,
      lifetimeDailyRepayPoints: 0n,
      lifetimeDailyWithdrawPoints: 0n,
      lifetimeDailyVPPoints: 0n,
      lifetimeTotalPoints: 0n,
      epochsParticipated: [],
      lifetimeEpochsIncluded: [],
      lastUpdatedAt: epochStats.lastUpdatedAt,
    };
    context.UserPoints.set(userPoints);

    const user = await context.User.get(normalizedUserId);
    if (user) {
      context.User.set({
        ...user,
        points_id: userPoints.id,
      });
    }
  }

  const existingEpochs = userPoints.epochsParticipated || [];
  const hasEpoch = existingEpochs.some(epoch => epoch === epochStats.epochNumber);
  const epochs = hasEpoch ? existingEpochs : [...existingEpochs, epochStats.epochNumber];

  let lifetimeDeposit = 0n;
  let lifetimeBorrow = 0n;
  let lifetimeDailySupply = 0n;
  let lifetimeDailyBorrow = 0n;
  let lifetimeDailyRepay = 0n;
  let lifetimeDailyWithdraw = 0n;
  let lifetimeDailyVP = 0n;
  let lifetimeTotal = 0n;

  for (const epoch of epochs) {
    const stats = await context.UserEpochStats.get(`${normalizedUserId}:${epoch}`);
    if (!stats) continue;

    lifetimeDeposit += stats.depositPoints;
    lifetimeBorrow += stats.borrowPoints;
    lifetimeDailySupply += stats.dailySupplyPoints;
    lifetimeDailyBorrow += stats.dailyBorrowPoints;
    lifetimeDailyRepay += stats.dailyRepayPoints;
    lifetimeDailyWithdraw += stats.dailyWithdrawPoints;
    lifetimeDailyVP += stats.dailyVPPoints;
    lifetimeTotal += stats.totalPoints;
  }

  context.UserPoints.set({
    ...userPoints,
    lifetimeDepositPoints: lifetimeDeposit,
    lifetimeBorrowPoints: lifetimeBorrow,
    lifetimeDailySupplyPoints: lifetimeDailySupply,
    lifetimeDailyBorrowPoints: lifetimeDailyBorrow,
    lifetimeDailyRepayPoints: lifetimeDailyRepay,
    lifetimeDailyWithdrawPoints: lifetimeDailyWithdraw,
    lifetimeDailyVPPoints: lifetimeDailyVP,
    lifetimeTotalPoints: lifetimeTotal,
    epochsParticipated: epochs,
    lifetimeEpochsIncluded: epochs,
    lastUpdatedAt: epochStats.lastUpdatedAt,
  });

  if (lifetimeTotal > 0n) {
    const userState = await getOrCreateUserLeaderboardState(
      context,
      normalizedUserId,
      epochStats.lastUpdatedAt
    );
    context.UserLeaderboardState.set({
      ...userState,
      lifetimePoints: lifetimeTotal,
      lastUpdate: epochStats.lastUpdatedAt,
    });

    // Only update all-time leaderboard if leaderboard is active
    const leaderboardState = await context.LeaderboardState.get('current');
    if (leaderboardState && leaderboardState.currentEpochNumber > 0n) {
      const { updateAllTimeLeaderboard } = await import('../helpers/leaderboard');
      await updateAllTimeLeaderboard(
        context,
        normalizedUserId,
        Number(lifetimeTotal) / 1e18,
        epochStats.lastUpdatedAt
      );
    }
  }
}

type VotingPowerState = {
  votingPower: bigint;
  vpMultiplier: bigint;
  vpTierIndex: bigint;
  combinedMultiplierBps: bigint;
};

export async function refreshUserVotingPowerState(
  context: handlerContext,
  userId: string,
  timestamp: number
): Promise<VotingPowerState> {
  const normalizedUserId = normalizeAddress(userId);
  const state = await getOrCreateUserLeaderboardState(context, normalizedUserId, timestamp);
  const currentVP = await calculateCurrentVPFromStorage(context, normalizedUserId, timestamp);
  const vpMultiplier = await calculateVPMultiplier(context, currentVP);
  const vpTierIndex = await findVPTierIndex(context, currentVP);

  let combinedMultiplierBps = (state.nftMultiplier * vpMultiplier) / BASIS_POINTS;
  if (combinedMultiplierBps > MAX_COMBINED_MULTIPLIER) {
    combinedMultiplierBps = MAX_COMBINED_MULTIPLIER;
  }

  if (
    state.votingPower !== currentVP ||
    state.vpMultiplier !== vpMultiplier ||
    state.vpTierIndex !== vpTierIndex ||
    state.combinedMultiplier !== combinedMultiplierBps
  ) {
    context.UserLeaderboardState.set({
      ...state,
      votingPower: currentVP,
      vpMultiplier,
      vpTierIndex,
      combinedMultiplier: combinedMultiplierBps,
      lastUpdate: timestamp,
    });
  }

  return {
    votingPower: currentVP,
    vpMultiplier,
    vpTierIndex,
    combinedMultiplierBps,
  };
}

export async function calculateAverageCombinedMultiplierBps(
  context: handlerContext,
  userId: string,
  startTimestamp: number,
  endTimestamp: number
): Promise<bigint> {
  const normalizedUserId = normalizeAddress(userId);
  const state = await getOrCreateUserLeaderboardState(context, normalizedUserId, endTimestamp);
  const averageVP = await calculateAverageVPFromStorage(
    context,
    normalizedUserId,
    startTimestamp,
    endTimestamp
  );
  const vpMultiplier = await calculateVPMultiplier(context, averageVP);

  let combinedMultiplierBps = (state.nftMultiplier * vpMultiplier) / BASIS_POINTS;
  if (combinedMultiplierBps > MAX_COMBINED_MULTIPLIER) {
    combinedMultiplierBps = MAX_COMBINED_MULTIPLIER;
  }

  return combinedMultiplierBps;
}

export async function applyMultipliersForUser(
  context: handlerContext,
  userId: string,
  rawPoints: number,
  timestamp: number,
  precomputed?: { combinedMultiplierBps: bigint }
): Promise<number> {
  const normalizedUserId = normalizeAddress(userId);
  const combinedMultiplierBps =
    precomputed?.combinedMultiplierBps ??
    (await refreshUserVotingPowerState(context, normalizedUserId, timestamp)).combinedMultiplierBps;
  let multiplier = Number(combinedMultiplierBps) / 10000;

  if (multiplier > MAX_MULTIPLIER) {
    multiplier = MAX_MULTIPLIER;
  }

  return rawPoints * multiplier;
}

export function applyCombinedMultiplierScaled(
  rawPointsScaled: bigint,
  combinedMultiplierBps: bigint
): bigint {
  let effectiveMultiplierBps = combinedMultiplierBps;
  const maxMultiplierBps = BigInt(MAX_MULTIPLIER * 10000);
  if (effectiveMultiplierBps > maxMultiplierBps) {
    effectiveMultiplierBps = maxMultiplierBps;
  }
  // rawPointsScaled * multiplierBps / 10000
  return (rawPointsScaled * effectiveMultiplierBps) / 10000n;
}

export function computeTotalPointsWithMultiplier(
  stats: {
    depositPointsWithMultiplier: bigint;
    borrowPointsWithMultiplier: bigint;
    vpPointsWithMultiplier: bigint;
    lpPointsWithMultiplier: bigint;
    dailySupplyPoints: bigint;
    dailyBorrowPoints: bigint;
    dailyRepayPoints: bigint;
    dailyWithdrawPoints: bigint;
    dailyLPPoints: bigint;
    manualAwardPoints: bigint;
  },
  userId?: string,
  epochNumber?: bigint
): bigint {
  const basePoints =
    stats.depositPointsWithMultiplier +
    stats.borrowPointsWithMultiplier +
    stats.vpPointsWithMultiplier +
    stats.lpPointsWithMultiplier +
    stats.dailySupplyPoints +
    stats.dailyBorrowPoints +
    stats.dailyRepayPoints +
    stats.dailyWithdrawPoints +
    stats.dailyLPPoints +
    stats.manualAwardPoints;

  // Apply testnet tier bonus for epoch 1 only
  if (epochNumber === 1n && userId) {
    const testnetBonusBps = getTestnetBonusBps(normalizeAddress(userId));
    if (testnetBonusBps > 0n) {
      // bonus is additive: base * (10000 + bonusBps) / 10000
      return (basePoints * (10000n + testnetBonusBps)) / 10000n;
    }
  }

  return basePoints;
}

export function getReserveNormalizedIncome(
  reserve: {
    liquidityIndex: bigint;
    liquidityRate: bigint;
    lastUpdateTimestamp: number;
  },
  timestamp: number
): bigint {
  if (reserve.liquidityIndex === 0n) {
    return 0n;
  }

  const lastUpdate = BigInt(reserve.lastUpdateTimestamp);
  const now = BigInt(timestamp);
  if (now <= lastUpdate) {
    return reserve.liquidityIndex;
  }

  const cumulated = calculateLinearInterest(reserve.liquidityRate, lastUpdate, now);
  return rayMul(RAY + cumulated, reserve.liquidityIndex);
}

export function getReserveNormalizedVariableDebt(
  reserve: {
    variableBorrowIndex: bigint;
    variableBorrowRate: bigint;
    lastUpdateTimestamp: number;
  },
  timestamp: number
): bigint {
  if (reserve.variableBorrowIndex === 0n) {
    return 0n;
  }

  const lastUpdate = BigInt(reserve.lastUpdateTimestamp);
  const now = BigInt(timestamp);
  if (now <= lastUpdate) {
    return reserve.variableBorrowIndex;
  }

  const cumulated = calculateCompoundedInterest(reserve.variableBorrowRate, lastUpdate, now);
  return rayMul(cumulated, reserve.variableBorrowIndex);
}

type IndexOverride = {
  liquidityIndex: bigint;
  variableBorrowIndex: bigint;
};

async function getEpochEndIndexOverride(
  context: handlerContext,
  reserveId: string,
  epochNumber: bigint,
  balanceTimestamp: number,
  reserveLastUpdateTimestamp: number
): Promise<IndexOverride | undefined> {
  /* c8 ignore start */
  if (balanceTimestamp >= reserveLastUpdateTimestamp) {
    return undefined;
  }
  /* c8 ignore end */

  const snapshotId = `epochEnd:${epochNumber.toString()}:${reserveId}`;
  const snapshot = await context.ReserveParamsHistoryItem.get(snapshotId);
  if (!snapshot || snapshot.timestamp !== balanceTimestamp) {
    return undefined;
  }

  return {
    liquidityIndex: snapshot.liquidityIndex,
    variableBorrowIndex: snapshot.variableBorrowIndex,
  };
}

export function getCurrentBalancesFromScaled(
  reserve: {
    liquidityIndex: bigint;
    liquidityRate: bigint;
    variableBorrowIndex: bigint;
    variableBorrowRate: bigint;
    lastUpdateTimestamp: number;
  },
  userReserve: {
    scaledATokenBalance: bigint;
    scaledVariableDebt: bigint;
    currentATokenBalance: bigint;
    currentVariableDebt: bigint;
    currentStableDebt: bigint;
  },
  timestamp: number,
  indexOverride?: IndexOverride
): { supply: bigint; variableDebt: bigint; totalDebt: bigint } {
  if (timestamp < reserve.lastUpdateTimestamp && !indexOverride) {
    return {
      supply: userReserve.currentATokenBalance,
      variableDebt: userReserve.currentVariableDebt,
      totalDebt: userReserve.currentVariableDebt + userReserve.currentStableDebt,
    };
  }

  const liquidityIndex =
    indexOverride?.liquidityIndex ?? getReserveNormalizedIncome(reserve, timestamp);
  const variableBorrowIndex =
    indexOverride?.variableBorrowIndex ?? getReserveNormalizedVariableDebt(reserve, timestamp);

  const supply =
    userReserve.scaledATokenBalance > 0n && liquidityIndex > 0n
      ? rayMul(userReserve.scaledATokenBalance, liquidityIndex)
      : userReserve.currentATokenBalance;

  const variableDebt =
    userReserve.scaledVariableDebt > 0n && variableBorrowIndex > 0n
      ? rayMul(userReserve.scaledVariableDebt, variableBorrowIndex)
      : userReserve.currentVariableDebt;

  return {
    supply,
    variableDebt,
    totalDebt: variableDebt + userReserve.currentStableDebt,
  };
}

export async function updatePriceOracleIndex(
  context: handlerContext,
  oracleAsset: PriceOracleAsset_t,
  timestamp: number
) {
  const idxBefore = oracleAsset.cumulativeUsdPriceHours;
  let cumulativeUsdPriceHours = oracleAsset.cumulativeUsdPriceHours;
  const priceUsd =
    oracleAsset.lastPriceUsd > 0
      ? oracleAsset.lastPriceUsd
      : Number(oracleAsset.priceInEth) / E8_DIVISOR;

  let resetTimestamp = oracleAsset.resetTimestamp || 0;
  let resetCumulativeUsdPriceHours = oracleAsset.resetCumulativeUsdPriceHours || 0;

  const state = await context.LeaderboardState.get('current');
  const currentEpoch =
    state && state.isActive
      ? await context.LeaderboardEpoch.get(state.currentEpochNumber.toString())
      : null;
  const epochStartTs = currentEpoch?.startTime || 0;
  const isFirstUpdateInEpoch =
    epochStartTs > 0 && resetTimestamp < epochStartTs && timestamp >= epochStartTs;

  if (oracleAsset.lastUpdateTimestamp > 0 && priceUsd > 0) {
    let accumulateFromTs = oracleAsset.lastUpdateTimestamp;

    // If this is the first update in a new epoch and the last update was after epoch start,
    // backfill from epoch start instead (assume current price was valid since epoch start)
    if (isFirstUpdateInEpoch && oracleAsset.lastUpdateTimestamp > epochStartTs) {
      accumulateFromTs = epochStartTs;
    }

    const dtSeconds = timestamp - accumulateFromTs;
    if (dtSeconds > 0) {
      cumulativeUsdPriceHours += priceUsd * (dtSeconds / 3600);
    }
  }

  if (currentEpoch && isFirstUpdateInEpoch) {
    let baseline = idxBefore;
    if (
      oracleAsset.lastUpdateTimestamp > 0 &&
      oracleAsset.lastUpdateTimestamp < epochStartTs &&
      priceUsd > 0
    ) {
      const secondsToStart = epochStartTs - oracleAsset.lastUpdateTimestamp;
      if (secondsToStart > 0) {
        baseline += priceUsd * (secondsToStart / 3600);
      }
    }

    resetTimestamp = epochStartTs;
    resetCumulativeUsdPriceHours = baseline;
  }

  const updated = {
    ...oracleAsset,
    cumulativeUsdPriceHours,
    resetTimestamp,
    resetCumulativeUsdPriceHours,
    lastUpdateTimestamp: timestamp,
  };

  context.PriceOracleAsset.set(updated);

  return { updated, idxBefore };
}

type AccrueOptions = {
  skipPointAccrual?: boolean;
  combinedMultiplierBps?: bigint;
};

export async function accruePointsForUserReserve(
  context: handlerContext,
  userId: string,
  reserveId: string,
  timestamp: number,
  blockNumber?: bigint,
  options?: AccrueOptions
): Promise<void> {
  const normalizedUserId = normalizeAddress(userId);
  const normalizedReserveId = reserveId.toLowerCase();
  const reserve = await context.Reserve.get(normalizedReserveId);
  if (!reserve) return;

  const userReserveId = `${normalizedUserId}-${normalizedReserveId}`;
  const userReserve = await context.UserReserve.get(userReserveId);
  if (!userReserve) return;

  const leaderboardState = await context.LeaderboardState.get('current');
  if (!leaderboardState || leaderboardState.currentEpochNumber === 0n) {
    return;
  }

  const isActiveEpoch = leaderboardState.isActive;
  let targetEpochNumber = leaderboardState.currentEpochNumber;
  let epochEndTime = 0;

  if (!isActiveEpoch) {
    const targetEpoch = await context.LeaderboardEpoch.get(targetEpochNumber.toString());
    if (!targetEpoch || targetEpoch.endTime === undefined) return;
    epochEndTime = targetEpoch.endTime;
  }

  const skipPointAccrual = options?.skipPointAccrual ?? false;
  const balanceTimestamp =
    !skipPointAccrual && !isActiveEpoch && epochEndTime > 0 && timestamp > epochEndTime
      ? epochEndTime
      : timestamp;
  let urp = await getOrCreateUserReservePoints(
    context,
    normalizedUserId,
    normalizedReserveId,
    timestamp
  );
  const isEpochOver = !isActiveEpoch && epochEndTime > 0 && timestamp > epochEndTime;
  if (!skipPointAccrual && isEpochOver && urp.lastUpdateTimestamp >= epochEndTime) {
    return;
  }
  const indexOverride =
    balanceTimestamp < reserve.lastUpdateTimestamp
      ? await getEpochEndIndexOverride(
          context,
          normalizedReserveId,
          targetEpochNumber,
          balanceTimestamp,
          reserve.lastUpdateTimestamp
        )
      : undefined;
  const { supply: currentSupplyBI, variableDebt: currentBorrowBI } = getCurrentBalancesFromScaled(
    reserve,
    userReserve,
    balanceTimestamp,
    indexOverride
  );

  await ensureAssetPrice(context, reserve.underlyingAsset, timestamp);
  const oracleAsset = await context.PriceOracleAsset.get(reserve.price);
  if (!oracleAsset) return;

  const { updated: updatedOracle, idxBefore } = await updatePriceOracleIndex(
    context,
    oracleAsset,
    timestamp
  );

  let idx = updatedOracle.cumulativeUsdPriceHours;

  // Cap the price index at epoch end so gap settlements don't earn extra points.
  if (!isActiveEpoch && epochEndTime > 0 && timestamp > epochEndTime) {
    const lastPriceUsd = updatedOracle.lastPriceUsd;
    if (lastPriceUsd > 0) {
      const gapSeconds = timestamp - epochEndTime;
      const gapAccumulation = lastPriceUsd * (gapSeconds / 3600);
      idx = Math.max(0, idx - gapAccumulation);
    }
  }

  if (targetEpochNumber === 0n) return;

  const epochStatsId = `${normalizedUserId}:${targetEpochNumber}`;
  let epochStats = await context.UserEpochStats.get(epochStatsId);
  if (!epochStats) {
    /* c8 ignore next */
    if (!isActiveEpoch && currentSupplyBI === 0n && currentBorrowBI === 0n) {
      return;
    }

    epochStats = {
      id: epochStatsId,
      user_id: normalizedUserId,
      epochNumber: targetEpochNumber,
      depositPoints: 0n,
      borrowPoints: 0n,
      lpPoints: 0n,
      dailySupplyPoints: 0n,
      dailyBorrowPoints: 0n,
      dailyRepayPoints: 0n,
      dailyWithdrawPoints: 0n,
      dailyVPPoints: 0n,
      dailyLPPoints: 0n,
      manualAwardPoints: 0n,
      depositMultiplierBps: BASIS_POINTS,
      borrowMultiplierBps: BASIS_POINTS,
      vpMultiplierBps: BASIS_POINTS,
      lpMultiplierBps: BASIS_POINTS,
      depositPointsWithMultiplier: 0n,
      borrowPointsWithMultiplier: 0n,
      vpPointsWithMultiplier: 0n,
      lpPointsWithMultiplier: 0n,
      lastSupplyPointsDay: -1,
      lastBorrowPointsDay: -1,
      lastRepayPointsDay: -1,
      lastWithdrawPointsDay: -1,
      lastVPPointsDay: -1,
      totalPoints: 0n,
      totalPointsWithMultiplier: 0n,
      totalMultiplierBps: BASIS_POINTS,
      lastAppliedMultiplierBps: BASIS_POINTS,
      testnetBonusBps: 0n,
      rank: 0,
      firstSeenAt: timestamp,
      lastUpdatedAt: timestamp,
    };
  }

  const targetEpoch = await context.LeaderboardEpoch.get(targetEpochNumber.toString());
  if (!targetEpoch) return;
  const epochStartTs = targetEpoch.startTime;

  const config = await context.LeaderboardConfig.get('global');
  const depositRateBps = config?.depositRateBps ?? DEFAULT_DEPOSIT_RATE_BPS;
  const borrowRateBps = config?.borrowRateBps ?? DEFAULT_BORROW_RATE_BPS;

  if (urp.resetTimestamp < epochStartTs) {
    const useResetBaseline =
      updatedOracle.resetTimestamp >= epochStartTs &&
      updatedOracle.resetTimestamp <= epochStartTs + 60;

    const baselineIndex = useResetBaseline ? updatedOracle.resetCumulativeUsdPriceHours : idxBefore;
    urp = {
      ...urp,
      resetTimestamp: epochStartTs,
      resetDepositIndex: baselineIndex,
      resetBorrowIndex: baselineIndex,
    };
  }

  let depositPointsScaled = 0n;
  let borrowPointsScaled = 0n;

  const currentDepositTokens = toDecimal(currentSupplyBI, reserve.decimals);
  const currentBorrowTokens = toDecimal(currentBorrowBI, reserve.decimals);
  const useEpochBaseline = urp.lastUpdateTimestamp < epochStartTs;
  const toScaledTokens = (tokens: number, decimals: number): bigint => {
    if (!Number.isFinite(tokens) || tokens <= 0) return 0n;
    const scale = Math.pow(10, decimals);
    return BigInt(Math.floor(tokens * scale));
  };

  const depositTokensScaledForAccrual = useEpochBaseline
    ? currentSupplyBI
    : toScaledTokens(urp.lastDepositTokens, reserve.decimals);
  const borrowTokensScaledForAccrual = useEpochBaseline
    ? currentBorrowBI
    : toScaledTokens(urp.lastBorrowTokens, reserve.decimals);
  // First settle in a new epoch: treat current balances as the epoch-entry baseline.
  const fallbackPriceUsdE8 =
    updatedOracle.priceInEth > 0n
      ? updatedOracle.priceInEth
      : updatedOracle.lastPriceUsd > 0
        ? BigInt(Math.floor(updatedOracle.lastPriceUsd * 1e8))
        : 0n;
  const fallbackDeltaE8 =
    useEpochBaseline && fallbackPriceUsdE8 > 0n && balanceTimestamp > epochStartTs
      ? (fallbackPriceUsdE8 * BigInt(balanceTimestamp - epochStartTs)) / 3600n
      : 0n;
  const decimalsScale = 10n ** BigInt(reserve.decimals);
  const pointsDenominator = decimalsScale * 10n ** 8n * BASIS_POINTS * HOURS_PER_DAY_BI;

  if (depositTokensScaledForAccrual > 0n) {
    const lastIndex = Math.max(urp.lastDepositIndex, urp.resetDepositIndex);
    let deltaE8 = idx > lastIndex ? BigInt(Math.floor((idx - lastIndex) * 1e8)) : 0n;
    if (useEpochBaseline && deltaE8 === 0n && fallbackDeltaE8 > 0n) {
      deltaE8 = fallbackDeltaE8;
    }
    if (deltaE8 > 0n) {
      depositPointsScaled =
        (depositTokensScaledForAccrual * deltaE8 * depositRateBps * POINTS_SCALE) /
        pointsDenominator;
    }
  }

  if (borrowTokensScaledForAccrual > 0n) {
    const lastIndex = Math.max(urp.lastBorrowIndex, urp.resetBorrowIndex);
    let deltaE8 = idx > lastIndex ? BigInt(Math.floor((idx - lastIndex) * 1e8)) : 0n;
    if (useEpochBaseline && deltaE8 === 0n && fallbackDeltaE8 > 0n) {
      deltaE8 = fallbackDeltaE8;
    }
    if (deltaE8 > 0n) {
      borrowPointsScaled =
        (borrowTokensScaledForAccrual * deltaE8 * borrowRateBps * POINTS_SCALE) / pointsDenominator;
    }
  }

  let currentDepositUsd = 0;
  let currentBorrowUsd = 0;

  if (updatedOracle.priceInEth > 0n) {
    const priceUsd = Number(updatedOracle.priceInEth) / E8_DIVISOR;
    currentDepositUsd = currentDepositTokens * priceUsd;
    currentBorrowUsd = currentBorrowTokens * priceUsd;
  }

  if (skipPointAccrual) {
    depositPointsScaled = 0n;
    borrowPointsScaled = 0n;
  }

  const updatedUrp = {
    ...urp,
    depositPoints: urp.depositPoints + depositPointsScaled,
    borrowPoints: urp.borrowPoints + borrowPointsScaled,
    lastDepositTokens: currentDepositTokens,
    lastBorrowTokens: currentBorrowTokens,
    lastDepositUsd: currentDepositUsd,
    lastBorrowUsd: currentBorrowUsd,
    lastDepositIndex: idx,
    lastBorrowIndex: idx,
    lastUpdateTimestamp: timestamp,
  };

  context.UserReservePoints.set({
    ...updatedUrp,
    totalPoints: updatedUrp.depositPoints + updatedUrp.borrowPoints,
  });

  if (!skipPointAccrual && (depositPointsScaled > 0n || borrowPointsScaled > 0n)) {
    epochStats = {
      ...epochStats,
      depositPoints: epochStats.depositPoints + depositPointsScaled,
      borrowPoints: epochStats.borrowPoints + borrowPointsScaled,
      totalPoints: recomputeEpochTotalPoints({
        ...epochStats,
        depositPoints: epochStats.depositPoints + depositPointsScaled,
        borrowPoints: epochStats.borrowPoints + borrowPointsScaled,
      }),
      lastUpdatedAt: timestamp,
    };

    const accrualStartTimestamp = useEpochBaseline ? epochStartTs : urp.lastUpdateTimestamp;
    const accrualEndTimestamp = balanceTimestamp;
    const combinedMultiplierBps =
      accrualEndTimestamp > accrualStartTimestamp
        ? await calculateAverageCombinedMultiplierBps(
            context,
            normalizedUserId,
            accrualStartTimestamp,
            accrualEndTimestamp
          )
        : (options?.combinedMultiplierBps ??
          (await refreshUserVotingPowerState(context, normalizedUserId, timestamp))
            .combinedMultiplierBps);

    let depositPointsWithMultiplier = epochStats.depositPointsWithMultiplier;
    let borrowPointsWithMultiplier = epochStats.borrowPointsWithMultiplier;
    let depositMultiplierBps = epochStats.depositMultiplierBps;
    let borrowMultiplierBps = epochStats.borrowMultiplierBps;

    if (depositPointsScaled > 0n) {
      depositPointsWithMultiplier += applyCombinedMultiplierScaled(
        depositPointsScaled,
        combinedMultiplierBps
      );
      depositMultiplierBps = combinedMultiplierBps;
    }

    if (borrowPointsScaled > 0n) {
      borrowPointsWithMultiplier += applyCombinedMultiplierScaled(
        borrowPointsScaled,
        combinedMultiplierBps
      );
      borrowMultiplierBps = combinedMultiplierBps;
    }

    const totalPointsWithMultiplier = computeTotalPointsWithMultiplier(
      {
        ...epochStats,
        depositPointsWithMultiplier,
        borrowPointsWithMultiplier,
        vpPointsWithMultiplier: epochStats.vpPointsWithMultiplier,
      },
      normalizedUserId,
      targetEpochNumber
    );

    const testnetBonusBps = targetEpochNumber === 1n ? getTestnetBonusBps(normalizedUserId) : 0n;
    epochStats = {
      ...epochStats,
      depositMultiplierBps,
      borrowMultiplierBps,
      depositPointsWithMultiplier,
      borrowPointsWithMultiplier,
      totalPointsWithMultiplier,
      totalMultiplierBps: combinedMultiplierBps,
      lastAppliedMultiplierBps: combinedMultiplierBps,
      testnetBonusBps,
    };

    context.UserEpochStats.set(epochStats);
    await updateLifetimePoints(context, normalizedUserId, epochStats);

    const finalPoints = Number(totalPointsWithMultiplier) / 1e18;
    const { updateLeaderboard } = await import('../helpers/leaderboard');
    await updateLeaderboard(context, normalizedUserId, finalPoints, timestamp);
  }
}

export async function syncUserReservePointsBaseline(
  context: handlerContext,
  userId: string,
  reserveId: string,
  timestamp: number,
  blockNumber: bigint
): Promise<void> {
  if (Number(blockNumber) < LEADERBOARD_START_BLOCK) return;

  // Update baseline after a balance change without accruing points.
  const normalizedUserId = normalizeAddress(userId);
  const normalizedReserveId = reserveId.toLowerCase();
  await accruePointsForUserReserve(
    context,
    normalizedUserId,
    normalizedReserveId,
    timestamp,
    blockNumber,
    {
      skipPointAccrual: true,
    }
  );
}

export async function settlePointsForUser(
  context: handlerContext,
  userId: string,
  reserveId: string | null,
  timestamp: number,
  blockNumber: bigint,
  options?: { ignoreCooldown?: boolean; skipNftSync?: boolean; skipLPSync?: boolean }
): Promise<void> {
  const normalizedUserId = normalizeAddress(userId);
  const normalizedReserveId = reserveId ? reserveId.toLowerCase() : null;
  const leaderboardState = await context.LeaderboardState.get('current');
  // Allow settlements during gap; accrual is capped at the epoch end.
  if (Number(blockNumber) < LEADERBOARD_START_BLOCK) {
    await refreshUserVotingPowerState(context, normalizedUserId, timestamp);
    return;
  }

  if (!leaderboardState || leaderboardState.currentEpochNumber === 0n) {
    await refreshUserVotingPowerState(context, normalizedUserId, timestamp);
    return;
  }

  const epoch = await context.LeaderboardEpoch.get(leaderboardState.currentEpochNumber.toString());
  if (!epoch) return;

  const balanceTimestamp =
    !leaderboardState.isActive && epoch.endTime && timestamp > epoch.endTime
      ? epoch.endTime
      : timestamp;

  if (!options?.skipNftSync) {
    await syncUserNFTOwnershipFromChain(context, normalizedUserId, timestamp, blockNumber);
  }
  if (!options?.skipLPSync && shouldSyncLPPositionsFromChain()) {
    const { syncUserLPPositionsFromChain } = await import('./lp');
    await syncUserLPPositionsFromChain(context, normalizedUserId, timestamp, blockNumber);
  }
  if (!options?.skipLPSync) {
    const { settleUserLPPositions } = await import('./lp');
    await settleUserLPPositions(context, normalizedUserId, timestamp, blockNumber);
  }
  const vpState = await refreshUserVotingPowerState(context, normalizedUserId, timestamp);

  const config = await context.LeaderboardConfig.get('global');
  const cooldownSeconds = config?.cooldownSeconds ?? DEFAULT_COOLDOWN_SECONDS;

  let epochStats = await getOrCreateUserEpochStats(
    context,
    normalizedUserId,
    leaderboardState.currentEpochNumber,
    timestamp
  );
  const previousEpochUpdatedAt = epochStats.lastUpdatedAt;

  const ignoreCooldown = options?.ignoreCooldown ?? false;
  let inCooldown = false;
  if (!ignoreCooldown && leaderboardState.isActive && epochStats.lastUpdatedAt > 0) {
    const elapsed = timestamp - epochStats.lastUpdatedAt;
    if (elapsed < cooldownSeconds) {
      inCooldown = true;
    }
  }

  if (normalizedReserveId) {
    await addReserveToUserList(context, normalizedUserId, normalizedReserveId, timestamp);
  }

  const userReserveList = await context.UserReserveList.get(normalizedUserId);
  const reserveIds =
    userReserveList?.reserveIds ?? (normalizedReserveId ? [normalizedReserveId] : []);

  let totalSupplyUsd = 0;
  let totalBorrowUsd = 0;

  if (reserveIds.length > 0) {
    for (const userReserveId of reserveIds) {
      const fullUserReserveId = `${normalizedUserId}-${userReserveId}`;
      const userReserve = await context.UserReserve.get(fullUserReserveId);
      if (!userReserve) continue;

      const reserve = await context.Reserve.get(userReserveId);
      if (!reserve) continue;

      const indexOverride =
        balanceTimestamp < reserve.lastUpdateTimestamp
          ? await getEpochEndIndexOverride(
              context,
              userReserveId,
              leaderboardState.currentEpochNumber,
              balanceTimestamp,
              reserve.lastUpdateTimestamp
            )
          : undefined;
      const { supply: currentSupplyBI, totalDebt: currentDebtBI } = getCurrentBalancesFromScaled(
        reserve,
        userReserve,
        balanceTimestamp,
        indexOverride
      );

      const hasSupply = currentSupplyBI > 0n;
      const hasDebt = currentDebtBI > 0n;
      if (!hasSupply && !hasDebt) continue;

      const shouldSettle =
        ignoreCooldown ||
        !inCooldown ||
        (normalizedReserveId ? userReserveId === normalizedReserveId : false);
      if (shouldSettle) {
        await accruePointsForUserReserve(
          context,
          normalizedUserId,
          userReserveId,
          timestamp,
          blockNumber,
          {
            combinedMultiplierBps: vpState.combinedMultiplierBps,
          }
        );
      }

      await ensureAssetPrice(context, reserve.underlyingAsset, timestamp);
      const priceOracleAsset = await context.PriceOracleAsset.get(reserve.price);
      if (!priceOracleAsset || priceOracleAsset.priceInEth === 0n) continue;

      const priceUsd = Number(priceOracleAsset.priceInEth) / E8_DIVISOR;
      const supplyTokens = toDecimal(currentSupplyBI, reserve.decimals);
      const borrowTokens = toDecimal(currentDebtBI, reserve.decimals);
      totalSupplyUsd += supplyTokens * priceUsd;
      totalBorrowUsd += borrowTokens * priceUsd;
    }

    const currentDay = getCurrentDay(timestamp);
    const activity = await getOrCreateUserDailyActivity(
      context,
      normalizedUserId,
      currentDay,
      timestamp
    );

    context.UserDailyActivity.set({
      ...activity,
      dailySupplyUsdHighwater: Math.max(totalSupplyUsd, activity.dailySupplyUsdHighwater),
      dailyBorrowUsdHighwater: Math.max(totalBorrowUsd, activity.dailyBorrowUsdHighwater),
      updatedAt: timestamp,
    });
  }

  const epochStatsId = `${normalizedUserId}:${leaderboardState.currentEpochNumber}`;
  const storedEpochStats = await context.UserEpochStats.get(epochStatsId);
  if (storedEpochStats) {
    epochStats = storedEpochStats;
  }

  const shouldSettleVp = ignoreCooldown || !inCooldown || normalizedReserveId !== null;
  const vpRateBps = config?.vpRateBps ?? 0n;
  if (shouldSettleVp && vpRateBps > 0n) {
    const vpAccrualStart = Math.max(epoch.startTime, previousEpochUpdatedAt);
    const vpAccrualEnd = balanceTimestamp;

    if (vpAccrualEnd > vpAccrualStart) {
      const averageVP = await calculateAverageVPFromStorage(
        context,
        normalizedUserId,
        vpAccrualStart,
        vpAccrualEnd
      );

      if (averageVP > 0n) {
        const avgVpUnits = Number(averageVP) / VP_DECIMALS;
        const vpDailyRate = Number(vpRateBps) / 10000;
        const vpRatePerSecond = vpDailyRate / SECONDS_PER_DAY;
        const vpPointsEarned = avgVpUnits * vpRatePerSecond * (vpAccrualEnd - vpAccrualStart);

        if (vpPointsEarned > 0) {
          // Convert to scaled BigInt
          const vpPointsScaled = toScaledPoints(vpPointsEarned);

          const state = await getOrCreateUserLeaderboardState(context, normalizedUserId, timestamp);
          const avgVpMultiplier = await calculateVPMultiplier(context, averageVP);
          let combinedMultiplierBps = (state.nftMultiplier * avgVpMultiplier) / BASIS_POINTS;
          if (combinedMultiplierBps > MAX_COMBINED_MULTIPLIER) {
            combinedMultiplierBps = MAX_COMBINED_MULTIPLIER;
          }

          const nextDailyVpPoints = epochStats.dailyVPPoints + vpPointsScaled;
          const nextVpPointsWithMultiplier =
            epochStats.vpPointsWithMultiplier +
            applyCombinedMultiplierScaled(vpPointsScaled, combinedMultiplierBps);

          const baseStats = {
            ...epochStats,
            dailyVPPoints: nextDailyVpPoints,
            vpPointsWithMultiplier: nextVpPointsWithMultiplier,
            vpMultiplierBps: combinedMultiplierBps,
            totalMultiplierBps: combinedMultiplierBps,
            lastAppliedMultiplierBps: combinedMultiplierBps,
            lastUpdatedAt: timestamp,
          };

          const totalPoints = recomputeEpochTotalPoints(baseStats);
          const totalPointsWithMultiplier = computeTotalPointsWithMultiplier(
            baseStats,
            normalizedUserId,
            leaderboardState.currentEpochNumber
          );

          const testnetBonusBps =
            leaderboardState.currentEpochNumber === 1n ? getTestnetBonusBps(normalizedUserId) : 0n;
          const updatedStats = {
            ...baseStats,
            totalPoints,
            totalPointsWithMultiplier,
            testnetBonusBps,
          };

          context.UserEpochStats.set(updatedStats);
          await updateLifetimePoints(context, normalizedUserId, updatedStats);

          const finalPoints = Number(updatedStats.totalPointsWithMultiplier) / 1e18;
          const { updateLeaderboard } = await import('../helpers/leaderboard');
          await updateLeaderboard(context, normalizedUserId, finalPoints, timestamp);
        }
      }
    }
  }
}

export async function settlePointsForAllReserves(
  context: handlerContext,
  userId: string,
  timestamp: number,
  blockNumber: bigint,
  options?: { ignoreCooldown?: boolean; skipNftSync?: boolean; skipLPSync?: boolean }
): Promise<void> {
  await settlePointsForUser(
    context,
    normalizeAddress(userId),
    null,
    timestamp,
    blockNumber,
    options
  );
}

export async function awardDailySupplyPoints(
  context: handlerContext,
  userId: string,
  timestamp: number,
  _blockNumber?: bigint
): Promise<void> {
  const normalizedUserId = normalizeAddress(userId);
  const leaderboardState = await context.LeaderboardState.get('current');
  if (
    !leaderboardState ||
    leaderboardState.currentEpochNumber === 0n ||
    !leaderboardState.isActive
  ) {
    return;
  }

  const epochNumber = leaderboardState.currentEpochNumber;
  const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
  if (!epoch) return;
  const currentDay = getCurrentDay(timestamp);

  const config = await context.LeaderboardConfig.get('global');
  if (!config || config.supplyDailyBonus === 0) return;

  const epochStats = await getOrCreateUserEpochStats(
    context,
    normalizedUserId,
    epochNumber,
    timestamp
  );
  const activity = await getOrCreateUserDailyActivity(
    context,
    normalizedUserId,
    currentDay,
    timestamp
  );

  context.UserDailyActivity.set({
    ...activity,
    hasSupplied: true,
    supplyTimestamp: activity.hasSupplied ? activity.supplyTimestamp : timestamp,
    updatedAt: timestamp,
  });

  if (epochStats.lastSupplyPointsDay === currentDay) return;

  // Check anti-dust threshold
  if (activity.dailySupplyUsdHighwater < config.minDailyBonusUsd) return;

  const dailyBonus = config.supplyDailyBonus;
  const dailyBonusScaled = toScaledPoints(dailyBonus);
  const updatedStats = {
    ...epochStats,
    dailySupplyPoints: epochStats.dailySupplyPoints + dailyBonusScaled,
    lastSupplyPointsDay: currentDay,
    lastUpdatedAt: timestamp,
  };
  const totalPoints = recomputeEpochTotalPoints(updatedStats);
  const vpState = await refreshUserVotingPowerState(context, normalizedUserId, timestamp);
  const totalPointsWithMultiplier = computeTotalPointsWithMultiplier(
    updatedStats,
    normalizedUserId,
    epochNumber
  );

  const testnetBonusBps = epochNumber === 1n ? getTestnetBonusBps(normalizedUserId) : 0n;
  context.UserEpochStats.set({
    ...updatedStats,
    totalPoints,
    totalPointsWithMultiplier,
    totalMultiplierBps: vpState.combinedMultiplierBps,
    lastAppliedMultiplierBps: vpState.combinedMultiplierBps,
    testnetBonusBps,
  });

  await updateLifetimePoints(context, normalizedUserId, {
    epochNumber: epochStats.epochNumber,
    lastUpdatedAt: timestamp,
  });

  const finalPoints = Number(totalPointsWithMultiplier) / 1e18;
  const { updateLeaderboard } = await import('../helpers/leaderboard');
  await updateLeaderboard(context, normalizedUserId, finalPoints, timestamp);
}

export async function awardDailyBorrowPoints(
  context: handlerContext,
  userId: string,
  timestamp: number,
  _blockNumber?: bigint
): Promise<void> {
  const normalizedUserId = normalizeAddress(userId);
  const leaderboardState = await context.LeaderboardState.get('current');
  if (
    !leaderboardState ||
    leaderboardState.currentEpochNumber === 0n ||
    !leaderboardState.isActive
  ) {
    return;
  }

  const epochNumber = leaderboardState.currentEpochNumber;
  const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
  if (!epoch) return;
  const currentDay = getCurrentDay(timestamp);

  const config = await context.LeaderboardConfig.get('global');
  if (!config || config.borrowDailyBonus === 0) return;

  const epochStats = await getOrCreateUserEpochStats(
    context,
    normalizedUserId,
    epochNumber,
    timestamp
  );
  const activity = await getOrCreateUserDailyActivity(
    context,
    normalizedUserId,
    currentDay,
    timestamp
  );

  context.UserDailyActivity.set({
    ...activity,
    hasBorrowed: true,
    borrowTimestamp: activity.hasBorrowed ? activity.borrowTimestamp : timestamp,
    updatedAt: timestamp,
  });

  if (epochStats.lastBorrowPointsDay === currentDay) return;

  // Check anti-dust threshold
  if (activity.dailyBorrowUsdHighwater < config.minDailyBonusUsd) return;

  const dailyBonus = config.borrowDailyBonus;
  const dailyBonusScaled = toScaledPoints(dailyBonus);
  const updatedStats = {
    ...epochStats,
    dailyBorrowPoints: epochStats.dailyBorrowPoints + dailyBonusScaled,
    lastBorrowPointsDay: currentDay,
    lastUpdatedAt: timestamp,
  };
  const totalPoints = recomputeEpochTotalPoints(updatedStats);
  const vpState = await refreshUserVotingPowerState(context, normalizedUserId, timestamp);
  const totalPointsWithMultiplier = computeTotalPointsWithMultiplier(
    updatedStats,
    normalizedUserId,
    epochNumber
  );

  const testnetBonusBps = epochNumber === 1n ? getTestnetBonusBps(normalizedUserId) : 0n;
  context.UserEpochStats.set({
    ...updatedStats,
    totalPoints,
    totalPointsWithMultiplier,
    totalMultiplierBps: vpState.combinedMultiplierBps,
    lastAppliedMultiplierBps: vpState.combinedMultiplierBps,
    testnetBonusBps,
  });

  await updateLifetimePoints(context, normalizedUserId, {
    epochNumber: epochStats.epochNumber,
    lastUpdatedAt: timestamp,
  });

  const finalPoints = Number(totalPointsWithMultiplier) / 1e18;
  const { updateLeaderboard } = await import('../helpers/leaderboard');
  await updateLeaderboard(context, normalizedUserId, finalPoints, timestamp);
}

export async function awardDailyRepayPoints(
  context: handlerContext,
  userId: string,
  timestamp: number,
  _blockNumber?: bigint
): Promise<void> {
  const normalizedUserId = normalizeAddress(userId);
  const leaderboardState = await context.LeaderboardState.get('current');
  if (!leaderboardState || leaderboardState.currentEpochNumber === 0n || !leaderboardState.isActive)
    return;

  const epochNumber = leaderboardState.currentEpochNumber;
  const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
  if (!epoch) return;
  const currentDay = getCurrentDay(timestamp);

  const config = await context.LeaderboardConfig.get('global');
  if (!config || config.repayDailyBonus === 0) return;

  const epochStats = await getOrCreateUserEpochStats(
    context,
    normalizedUserId,
    epochNumber,
    timestamp
  );
  const activity = await getOrCreateUserDailyActivity(
    context,
    normalizedUserId,
    currentDay,
    timestamp
  );

  context.UserDailyActivity.set({
    ...activity,
    hasRepaid: true,
    repayTimestamp: activity.hasRepaid ? activity.repayTimestamp : timestamp,
    updatedAt: timestamp,
  });

  if (epochStats.lastRepayPointsDay === currentDay) return;

  if (activity.dailyRepayUsdHighwater < config.minDailyBonusUsd) return;

  const dailyBonus = config.repayDailyBonus;
  const dailyBonusScaled = toScaledPoints(dailyBonus);

  const updatedStats = {
    ...epochStats,
    dailyRepayPoints: epochStats.dailyRepayPoints + dailyBonusScaled,
    lastRepayPointsDay: currentDay,
    lastUpdatedAt: timestamp,
  };
  const totalPoints = recomputeEpochTotalPoints(updatedStats);
  const vpState = await refreshUserVotingPowerState(context, normalizedUserId, timestamp);
  const totalPointsWithMultiplier = computeTotalPointsWithMultiplier(
    updatedStats,
    normalizedUserId,
    epochNumber
  );

  const testnetBonusBps = epochNumber === 1n ? getTestnetBonusBps(normalizedUserId) : 0n;
  context.UserEpochStats.set({
    ...updatedStats,
    totalPoints,
    totalPointsWithMultiplier,
    totalMultiplierBps: vpState.combinedMultiplierBps,
    lastAppliedMultiplierBps: vpState.combinedMultiplierBps,
    testnetBonusBps,
  });

  await updateLifetimePoints(context, normalizedUserId, {
    epochNumber: epochStats.epochNumber,
    lastUpdatedAt: timestamp,
  });

  const finalPoints = Number(totalPointsWithMultiplier) / 1e18;
  const { updateLeaderboard } = await import('../helpers/leaderboard');
  await updateLeaderboard(context, normalizedUserId, finalPoints, timestamp);
}

export async function awardDailyWithdrawPoints(
  context: handlerContext,
  userId: string,
  timestamp: number,
  _blockNumber?: bigint
): Promise<void> {
  const normalizedUserId = normalizeAddress(userId);
  const leaderboardState = await context.LeaderboardState.get('current');
  if (!leaderboardState || leaderboardState.currentEpochNumber === 0n || !leaderboardState.isActive)
    return;

  const epochNumber = leaderboardState.currentEpochNumber;
  const epoch = await context.LeaderboardEpoch.get(epochNumber.toString());
  if (!epoch) return;
  const currentDay = getCurrentDay(timestamp);

  const config = await context.LeaderboardConfig.get('global');
  if (!config || config.withdrawDailyBonus === 0) return;

  const epochStats = await getOrCreateUserEpochStats(
    context,
    normalizedUserId,
    epochNumber,
    timestamp
  );
  const activity = await getOrCreateUserDailyActivity(
    context,
    normalizedUserId,
    currentDay,
    timestamp
  );

  context.UserDailyActivity.set({
    ...activity,
    hasWithdrawn: true,
    withdrawTimestamp: activity.hasWithdrawn ? activity.withdrawTimestamp : timestamp,
    updatedAt: timestamp,
  });

  if (epochStats.lastWithdrawPointsDay === currentDay) return;

  if (activity.dailyWithdrawUsdHighwater < config.minDailyBonusUsd) return;

  const dailyBonus = config.withdrawDailyBonus;
  const dailyBonusScaled = toScaledPoints(dailyBonus);

  const updatedStats = {
    ...epochStats,
    dailyWithdrawPoints: epochStats.dailyWithdrawPoints + dailyBonusScaled,
    lastWithdrawPointsDay: currentDay,
    lastUpdatedAt: timestamp,
  };
  const totalPoints = recomputeEpochTotalPoints(updatedStats);
  const vpState = await refreshUserVotingPowerState(context, normalizedUserId, timestamp);
  const totalPointsWithMultiplier = computeTotalPointsWithMultiplier(
    updatedStats,
    normalizedUserId,
    epochNumber
  );

  const testnetBonusBps = epochNumber === 1n ? getTestnetBonusBps(normalizedUserId) : 0n;
  context.UserEpochStats.set({
    ...updatedStats,
    totalPoints,
    totalPointsWithMultiplier,
    totalMultiplierBps: vpState.combinedMultiplierBps,
    lastAppliedMultiplierBps: vpState.combinedMultiplierBps,
    testnetBonusBps,
  });

  await updateLifetimePoints(context, normalizedUserId, {
    epochNumber: epochStats.epochNumber,
    lastUpdatedAt: timestamp,
  });

  const finalPoints = Number(totalPointsWithMultiplier) / 1e18;
  const { updateLeaderboard } = await import('../helpers/leaderboard');
  await updateLeaderboard(context, normalizedUserId, finalPoints, timestamp);
}

export { ZERO_ADDRESS };
