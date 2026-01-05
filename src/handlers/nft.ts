/**
 * NFT Partnership Event Handlers
 * NFTPartnershipRegistry, PartnerNFT
 *
 * ## NFT Balance Tracking Strategy:
 *
 * 1. **Bootstrap Phase** (One-time, after partnership added):
 *    - First settlement for a user baselines their balance for the new collection
 *    - Sets initial UserNFTOwnership records for existing holders
 *    - Users get multiplier bonus from existing holdings immediately
 *
 * 2. **Ongoing Tracking** (Automatic):
 *    - Transfer events maintain accurate balances going forward
 *
 * This approach eliminates the need for periodic syncing while ensuring:
 * - No missed holdings from before partnership activation
 * - Automatic accurate tracking after bootstrap
 * - Users accumulate points from held NFTs without needing to transfer
 *
 * ## NFT Multiplier System:
 *
 * Each partnership can use one of two multiplier types:
 *
 * 1. **Static Boost** (staticBoostBps > 0):
 *    - Fixed percentage boost per collection owned
 *    - Example: 10k Squad provides +2000 bps (20%) flat boost
 *    - Does not decay with additional NFTs
 *
 * 2. **Geometric Decay** (staticBoostBps = null or 0):
 *    - First NFT: +firstBonus (e.g., 1000 bps = 10%)
 *    - Each additional: previous * decayRatio / 10000 (e.g., 90%)
 *    - Example progression: 10%, 9%, 8.1%, 7.29%...
 *
 * Total multiplier = base (10000) + sum of static boosts + sum of decay boosts
 * Capped at MAX_NFT_MULTIPLIER (50000 = 5x)
 *
 * ## Event Handling for staticBoostBps:
 *
 * - undefined in event = preserve existing value (old contracts, resync-safe)
 * - 0 in event = explicitly decay-based NFT
 * - >0 in event = static boost value in basis points
 */

import {
  NFTPartnershipRegistry,
  PartnerNFT,
  The10kSquad,
  Overnads,
  SolveilPass,
} from '../../generated';
import {
  getOrCreateUserLeaderboardState,
  createMultiplierSnapshot,
  ZERO_ADDRESS,
  recordProtocolTransaction,
  settlePointsForUser,
  calculateNFTMultiplierFromUser,
} from './shared';
import { normalizeAddress } from '../helpers/constants';
import type { handlerContext } from '../../generated';

async function getOrCreateRegistryState(context: handlerContext, timestamp: number) {
  let state = await context.NFTPartnershipRegistryState.get('current');
  if (!state) {
    state = {
      id: 'current',
      activeCollections: [],
      lastUpdate: timestamp,
    };
    context.NFTPartnershipRegistryState.set(state);
  }
  return state;
}

async function updateActiveCollections(
  context: handlerContext,
  collection: string,
  isActive: boolean,
  timestamp: number
): Promise<void> {
  const normalizedCollection = normalizeAddress(collection);
  const state = await getOrCreateRegistryState(context, timestamp);
  const isTracked = state.activeCollections.includes(normalizedCollection);
  let activeCollections = state.activeCollections;

  if (isActive && !isTracked) {
    activeCollections = [...state.activeCollections, normalizedCollection];
  } else if (!isActive && isTracked) {
    activeCollections = state.activeCollections.filter(entry => entry !== normalizedCollection);
  }

  context.NFTPartnershipRegistryState.set({
    ...state,
    activeCollections,
    lastUpdate: timestamp,
  });
}

// ============================================
// NFTPartnershipRegistry Handlers
// ============================================

NFTPartnershipRegistry.PartnershipAdded.contractRegister(({ event, context }) => {
  context.addPartnerNFT(normalizeAddress(event.params.collection));
});

NFTPartnershipRegistry.PartnershipAdded.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = normalizeAddress(event.params.collection);
  const timestamp = Number(event.block.timestamp);

  // The first settlement after a partnership is added can baseline ownership
  // via a one-time on-chain balance check per user.

  // Extract staticBoostBps if present in event (future contract versions)
  const paramsWithBoost = event.params as typeof event.params & { staticBoostBps?: bigint };

  // Check if partnership already exists (for resync)
  const existing = await context.NFTPartnership.get(id);

  // Only update staticBoostBps if explicitly provided in event
  // undefined = not in event (old contracts), preserve existing value
  // 0 = explicitly decay-based NFT
  // >0 = static boost value
  const staticBoostBps =
    paramsWithBoost.staticBoostBps !== undefined
      ? paramsWithBoost.staticBoostBps
      : existing?.staticBoostBps;

  context.NFTPartnership.set({
    id,
    collection: id,
    name: event.params.name,
    active: event.params.active,
    staticBoostBps,
    startTimestamp: Number(event.params.startTimestamp),
    endTimestamp: event.params.endTimestamp > 0n ? Number(event.params.endTimestamp) : undefined,
    addedAt: timestamp,
    lastUpdate: timestamp,
  });

  // Update global config with values from the event
  context.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: event.params.currentFirstBonus,
    decayRatio: event.params.currentDecayRatio,
    lastUpdate: timestamp,
  });

  await updateActiveCollections(context, id, event.params.active, timestamp);
});

NFTPartnershipRegistry.PartnershipUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = normalizeAddress(event.params.collection);

  const partnership = await context.NFTPartnership.get(id);
  if (partnership) {
    // Extract staticBoostBps if present in event
    const paramsWithBoost = event.params as typeof event.params & { staticBoostBps?: bigint };

    // Only update staticBoostBps if explicitly provided
    const staticBoostBps =
      paramsWithBoost.staticBoostBps !== undefined
        ? paramsWithBoost.staticBoostBps
        : partnership.staticBoostBps;

    context.NFTPartnership.set({
      ...partnership,
      name: event.params.name,
      active: event.params.active,
      staticBoostBps,
      startTimestamp: Number(event.params.startTimestamp),
      endTimestamp: event.params.endTimestamp > 0n ? Number(event.params.endTimestamp) : undefined,
      lastUpdate: Number(event.block.timestamp),
    });
  }

  await updateActiveCollections(context, id, event.params.active, Number(event.block.timestamp));
});

NFTPartnershipRegistry.PartnershipRemoved.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = normalizeAddress(event.params.collection);
  const timestamp = Number(event.block.timestamp);

  const partnership = await context.NFTPartnership.get(id);
  if (partnership) {
    context.NFTPartnership.set({
      ...partnership,
      active: false,
      lastUpdate: timestamp,
    });
  }

  await updateActiveCollections(context, id, false, timestamp);
});

NFTPartnershipRegistry.MultiplierParamsUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const id = event.params.timestamp.toString();

  context.NFTMultiplierSnapshot.set({
    id,
    firstBonus: event.params.newFirstBonus,
    decayRatio: event.params.newDecayRatio,
    activePartnershipCount: event.params.totalActivePartnerships,
    timestamp: Number(event.params.timestamp),
    txHash: event.transaction.hash,
  });

  context.NFTMultiplierConfig.set({
    id: 'current',
    firstBonus: event.params.newFirstBonus,
    decayRatio: event.params.newDecayRatio,
    lastUpdate: Number(event.params.timestamp),
  });
});

// ============================================
// PartnerNFT Handlers
// ============================================

PartnerNFT.Transfer.contractRegister(({ event, context }) => {
  context.addPartnerNFT(normalizeAddress(event.srcAddress));
});

PartnerNFT.Transfer.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const nftContract = normalizeAddress(event.srcAddress);
  const from = normalizeAddress(event.params.from);
  const to = normalizeAddress(event.params.to);
  const timestamp = Number(event.block.timestamp);

  // Self-transfer: no balance change, only update lastChecked timestamp
  if (from === to && from !== ZERO_ADDRESS) {
    const ownershipId = `${from}:${nftContract}`;
    const ownership = await context.UserNFTOwnership.get(ownershipId);
    if (ownership) {
      context.UserNFTOwnership.set({
        ...ownership,
        lastCheckedAt: timestamp,
        lastCheckedBlock: BigInt(event.block.number),
      });
    }
    return;
  }

  // Helper to fetch actual balance from contract and update state
  async function updateNFTOwnership(userAddress: string, delta: number) {
    const normalizedUser = normalizeAddress(userAddress);

    const ownershipId = `${normalizedUser}:${nftContract}`;
    let ownership = await context.UserNFTOwnership.get(ownershipId);
    const oldBalance = ownership?.balance || 0n;

    let newBalance = oldBalance + BigInt(delta);
    if (newBalance < 0n) newBalance = 0n; // Prevent negative balances

    const hasNFT = newBalance > 0n;
    const wasOwning = oldBalance > 0n;

    if (hasNFT) {
      context.UserNFTOwnership.set({
        id: ownershipId,
        user_id: normalizedUser,
        partnership_id: nftContract,
        balance: newBalance,
        hasNFT,
        lastCheckedAt: timestamp,
        lastCheckedBlock: BigInt(event.block.number),
      });
    } else if (ownership) {
      context.UserNFTOwnership.deleteUnsafe(ownershipId);
    }

    // Update multiplier only if collection ownership changed (0 <-> >0)
    if (wasOwning !== hasNFT) {
      const state = await getOrCreateUserLeaderboardState(context, normalizedUser, timestamp);
      const oldMultiplier = state.nftMultiplier;

      let newNftCount = state.nftCount;
      if (hasNFT && !wasOwning) {
        newNftCount = state.nftCount + 1n;
      } else if (!hasNFT && wasOwning) {
        newNftCount = state.nftCount > 0n ? state.nftCount - 1n : 0n;
      }

      // Update state first so calculateNFTMultiplierFromUser can read current ownership
      context.UserLeaderboardState.set({
        ...state,
        nftCount: newNftCount,
        lastUpdate: timestamp,
      });

      const newNftMultiplier = await calculateNFTMultiplierFromUser(context, normalizedUser);

      let combinedMultiplier = (newNftMultiplier * state.vpMultiplier) / 10000n;
      if (combinedMultiplier > 100000n) combinedMultiplier = 100000n;

      context.UserLeaderboardState.set({
        ...state,
        nftCount: newNftCount,
        nftMultiplier: newNftMultiplier,
        combinedMultiplier,
        lastUpdate: timestamp,
      });

      await settlePointsForUser(
        context,
        normalizedUser,
        null,
        timestamp,
        BigInt(event.block.number),
        {
          ignoreCooldown: true,
          skipNftSync: true,
        }
      );

      if (oldMultiplier !== newNftMultiplier) {
        const changeReason = hasNFT
          ? `NFT_RECEIVED:${nftContract}`
          : `NFT_TRANSFERRED:${nftContract}`;
        createMultiplierSnapshot(
          context,
          {
            ...state,
            nftCount: newNftCount,
            nftMultiplier: newNftMultiplier,
            combinedMultiplier,
          },
          timestamp,
          event.transaction.hash,
          changeReason,
          Number(event.logIndex)
        );
      }
    }
  }

  // Update both sender and receiver using event-driven deltas.
  if (from !== ZERO_ADDRESS) {
    await updateNFTOwnership(from, -1);
  }
  if (to !== ZERO_ADDRESS) {
    await updateNFTOwnership(to, +1);
  }
});

// ============================================
// Static NFT Collection Handlers (reuse PartnerNFT logic)
// ============================================

// The10kSquad uses same handler as PartnerNFT
The10kSquad.Transfer.handler(async ({ event, context }) => {
  // Forward to PartnerNFT handler logic - same event structure
  await handleNFTTransfer(event, context);
});

// Overnads uses same handler as PartnerNFT
Overnads.Transfer.handler(async ({ event, context }) => {
  await handleNFTTransfer(event, context);
});

// SolveilPass uses same handler as PartnerNFT
SolveilPass.Transfer.handler(async ({ event, context }) => {
  await handleNFTTransfer(event, context);
});

// Shared NFT Transfer handler logic
async function handleNFTTransfer(
  event: {
    srcAddress: string;
    params: { from: string; to: string; tokenId: bigint };
    block: { timestamp: number; number: number };
    transaction: { hash: string };
    logIndex: number;
  },
  context: handlerContext
) {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
  const nftContract = normalizeAddress(event.srcAddress);
  const from = normalizeAddress(event.params.from);
  const to = normalizeAddress(event.params.to);
  const timestamp = Number(event.block.timestamp);

  // Self-transfer: no balance change, only update lastChecked timestamp
  if (from === to && from !== ZERO_ADDRESS) {
    const ownershipId = `${from}:${nftContract}`;
    const ownership = await context.UserNFTOwnership.get(ownershipId);
    if (ownership) {
      context.UserNFTOwnership.set({
        ...ownership,
        lastCheckedAt: timestamp,
        lastCheckedBlock: BigInt(event.block.number),
      });
    }
    return;
  }

  // Helper to update NFT ownership
  async function updateNFTOwnership(userAddress: string, delta: number) {
    const normalizedUser = normalizeAddress(userAddress);

    const ownershipId = `${normalizedUser}:${nftContract}`;
    const ownership = await context.UserNFTOwnership.get(ownershipId);
    const oldBalance = ownership?.balance || 0n;

    let newBalance = oldBalance + BigInt(delta);
    if (newBalance < 0n) newBalance = 0n;

    const hasNFT = newBalance > 0n;
    const wasOwning = oldBalance > 0n;

    if (hasNFT) {
      context.UserNFTOwnership.set({
        id: ownershipId,
        user_id: normalizedUser,
        partnership_id: nftContract,
        balance: newBalance,
        hasNFT,
        lastCheckedAt: timestamp,
        lastCheckedBlock: BigInt(event.block.number),
      });
    } else if (ownership) {
      context.UserNFTOwnership.deleteUnsafe(ownershipId);
    }

    // Update multiplier only if collection ownership changed (0 <-> >0)
    if (wasOwning !== hasNFT) {
      const state = await getOrCreateUserLeaderboardState(context, normalizedUser, timestamp);
      const oldMultiplier = state.nftMultiplier;

      let newNftCount = state.nftCount;
      if (hasNFT && !wasOwning) {
        newNftCount = state.nftCount + 1n;
      } else if (!hasNFT && wasOwning) {
        newNftCount = state.nftCount > 0n ? state.nftCount - 1n : 0n;
      }

      // Update state first so calculateNFTMultiplierFromUser can read current ownership
      context.UserLeaderboardState.set({
        ...state,
        nftCount: newNftCount,
        lastUpdate: timestamp,
      });

      const newNftMultiplier = await calculateNFTMultiplierFromUser(context, normalizedUser);

      let combinedMultiplier = (newNftMultiplier * state.vpMultiplier) / 10000n;
      if (combinedMultiplier > 100000n) combinedMultiplier = 100000n;

      context.UserLeaderboardState.set({
        ...state,
        nftCount: newNftCount,
        nftMultiplier: newNftMultiplier,
        combinedMultiplier,
        lastUpdate: timestamp,
      });

      await settlePointsForUser(
        context,
        normalizedUser,
        null,
        timestamp,
        BigInt(event.block.number),
        {
          ignoreCooldown: true,
          skipNftSync: true,
        }
      );

      if (oldMultiplier !== newNftMultiplier) {
        const changeReason = hasNFT
          ? `NFT_RECEIVED:${nftContract}`
          : `NFT_TRANSFERRED:${nftContract}`;
        createMultiplierSnapshot(
          context,
          {
            ...state,
            nftCount: newNftCount,
            nftMultiplier: newNftMultiplier,
            combinedMultiplier,
          },
          timestamp,
          event.transaction.hash,
          changeReason,
          Number(event.logIndex)
        );
      }
    }
  }

  // Update both sender and receiver
  if (from !== ZERO_ADDRESS) {
    await updateNFTOwnership(from, -1);
  }
  if (to !== ZERO_ADDRESS) {
    await updateNFTOwnership(to, +1);
  }
}
