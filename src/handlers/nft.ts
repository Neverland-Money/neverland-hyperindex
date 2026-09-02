/**
 * NFT Partnership Event Handlers
 * NFTPartnershipRegistry, PartnerNFT
 *
 * ## NFT Balance Tracking Strategy:
 *
 * - PartnershipAdded registers a dynamic collection from that block forward;
 *   Envio does not backfill earlier transfers.
 * - Indexed Transfer events maintain ownership balances from the registration
 *   boundary onward.
 * - Production is event-only. Holdings that predate the indexed boundary are
 *   not reconstructed with an on-chain balance read.
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
 * - The current PartnershipAdded topic always applies its emitted value: 0
 *   selects geometric decay and a positive value selects a static boost.
 * - PartnershipAddedLegacy has no staticBoostBps field. It preserves an
 *   existing indexed value during replay, otherwise leaving the boost unset.
 * - PartnershipUpdated has no staticBoostBps field and preserves the existing
 *   value while updating the remaining partnership configuration.
 */

import {
  LilStars,
  NFTPartnershipRegistry,
  Overnads,
  PartnerNFT,
  RealNads,
  The10kSquad,
} from '../../generated';
import {
  getOrCreateUserLeaderboardState,
  createMultiplierSnapshot,
  ZERO_ADDRESS,
  recordProtocolTransaction,
  settlePointsForUser,
  calculateNFTMultiplierFromUser,
  composeCombinedMultiplierBps,
  writeNFTMultiplierConfig,
  writeNFTPartnership,
  writeNFTRegistryState,
} from './shared';
import { normalizeAddress, isStaticNftCollection } from '../helpers/constants';
import type { handlerContext } from '../../generated';

async function getOrCreateRegistryState(context: handlerContext, timestamp: number) {
  let state = await context.NFTPartnershipRegistryState.get('current');
  if (!state) {
    state = {
      id: 'current',
      activeCollections: [],
      lastUpdate: timestamp,
    };
    writeNFTRegistryState(context, state);
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

  writeNFTRegistryState(context, {
    ...state,
    activeCollections,
    lastUpdate: timestamp,
  });
}

type NFTPartnershipAddedParams = {
  readonly collection: string;
  readonly name: string;
  readonly active: boolean;
  readonly startTimestamp: bigint;
  readonly endTimestamp: bigint;
  readonly currentFirstBonus: bigint;
  readonly currentDecayRatio: bigint;
};

/** Apply a decoded PartnershipAdded payload to indexed state. */
async function applyNFTPartnershipAdded(
  context: handlerContext,
  params: NFTPartnershipAddedParams,
  emittedStaticBoostBps: bigint | undefined,
  timestamp: number
): Promise<void> {
  const id = normalizeAddress(params.collection);
  const staticBoostBps =
    emittedStaticBoostBps ?? (await context.NFTPartnership.get(id))?.staticBoostBps;

  writeNFTPartnership(context, {
    id,
    collection: id,
    name: params.name,
    active: params.active,
    staticBoostBps,
    startTimestamp: Number(params.startTimestamp),
    endTimestamp: params.endTimestamp > 0n ? Number(params.endTimestamp) : undefined,
    addedAt: timestamp,
    lastUpdate: timestamp,
  });

  writeNFTMultiplierConfig(context, {
    id: 'current',
    firstBonus: params.currentFirstBonus,
    decayRatio: params.currentDecayRatio,
    lastUpdate: timestamp,
  });

  await updateActiveCollections(context, id, params.active, timestamp);
}

type NFTPartnershipAddedEvent = {
  readonly params: NFTPartnershipAddedParams;
  readonly block: { readonly number: number; readonly timestamp: number };
  readonly transaction: { readonly hash: string };
};

async function handleNFTPartnershipAdded(
  context: handlerContext,
  event: NFTPartnershipAddedEvent,
  emittedStaticBoostBps: bigint | undefined
): Promise<void> {
  const timestamp = Number(event.block.timestamp);
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    timestamp,
    BigInt(event.block.number)
  );

  // Dynamic registration is forward-only; this event starts Transfer tracking
  // and does not reconstruct balances held before the registration block.
  await applyNFTPartnershipAdded(context, event.params, emittedStaticBoostBps, timestamp);
}

function registerPartnerNFTCollection(
  registration: (address: string) => void,
  rawCollection: string
): void {
  const collection = normalizeAddress(rawCollection);
  // Collections that are already statically configured in config.yaml (with
  // their own Transfer handler) must not also be registered as dynamic
  // PartnerNFT contracts. Envio does not dedupe across contract names, so that
  // would double-dispatch Transfer logs and double-count balances/multipliers.
  if (isStaticNftCollection(collection)) return;
  registration(collection);
}

// ============================================
// NFTPartnershipRegistry Handlers
// ============================================

NFTPartnershipRegistry.PartnershipAdded.contractRegister(async ({ event, context }) => {
  registerPartnerNFTCollection((a: string) => context.addPartnerNFT(a), event.params.collection);
});

NFTPartnershipRegistry.PartnershipAddedLegacy.contractRegister(async ({ event, context }) => {
  registerPartnerNFTCollection((a: string) => context.addPartnerNFT(a), event.params.collection);
});

NFTPartnershipRegistry.PartnershipAdded.handler(async ({ event, context }) => {
  await handleNFTPartnershipAdded(context, event, event.params.staticBoostBps);
});

NFTPartnershipRegistry.PartnershipAddedLegacy.handler(async ({ event, context }) => {
  await handleNFTPartnershipAdded(context, event, undefined);
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
    writeNFTPartnership(context, {
      ...partnership,
      name: event.params.name,
      active: event.params.active,
      // PartnershipUpdated has no staticBoostBps input, so the spread preserves it.
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
    writeNFTPartnership(context, {
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

  writeNFTMultiplierConfig(context, {
    id: 'current',
    firstBonus: event.params.newFirstBonus,
    decayRatio: event.params.newDecayRatio,
    lastUpdate: Number(event.params.timestamp),
  });
});

// ============================================
// PartnerNFT Handlers
// ============================================

PartnerNFT.Transfer.contractRegister(async ({ event, context }) => {
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

    const writeOwnership = () => {
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
    };

    // Update multiplier only if collection ownership changed (0 <-> >0)
    if (wasOwning !== hasNFT) {
      const preChangeState = await getOrCreateUserLeaderboardState(
        context,
        normalizedUser,
        timestamp
      );
      const oldMultiplier = preChangeState.nftMultiplier;

      await settlePointsForUser(
        context,
        normalizedUser,
        null,
        timestamp,
        BigInt(event.block.number),
        {
          ignoreCooldown: true,
        }
      );

      const state = await getOrCreateUserLeaderboardState(context, normalizedUser, timestamp);

      let newNftCount = state.nftCount;
      if (hasNFT && !wasOwning) {
        newNftCount = state.nftCount + 1n;
      } else if (!hasNFT && wasOwning) {
        newNftCount = state.nftCount > 0n ? state.nftCount - 1n : 0n;
      }

      writeOwnership();

      // Update state first so calculateNFTMultiplierFromUser can read current ownership
      context.UserLeaderboardState.set({
        ...state,
        nftCount: newNftCount,
        lastUpdate: timestamp,
      });

      const newNftMultiplier = await calculateNFTMultiplierFromUser(context, normalizedUser);
      const combinedMultiplier = composeCombinedMultiplierBps(
        newNftMultiplier,
        state.specialEditionMultiplier,
        state.vpMultiplier
      );

      context.UserLeaderboardState.set({
        ...state,
        nftCount: newNftCount,
        nftMultiplier: newNftMultiplier,
        combinedMultiplier,
        lastUpdate: timestamp,
      });

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
    } else {
      writeOwnership();
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

// LilStars uses same handler as PartnerNFT
LilStars.Transfer.handler(async ({ event, context }) => {
  await handleNFTTransfer(event, context);
});

// RealNads uses same handler as PartnerNFT
RealNads.Transfer.handler(async ({ event, context }) => {
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

    const writeOwnership = () => {
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
    };

    // Update multiplier only if collection ownership changed (0 <-> >0)
    if (wasOwning !== hasNFT) {
      const preChangeState = await getOrCreateUserLeaderboardState(
        context,
        normalizedUser,
        timestamp
      );
      const oldMultiplier = preChangeState.nftMultiplier;

      await settlePointsForUser(
        context,
        normalizedUser,
        null,
        timestamp,
        BigInt(event.block.number),
        {
          ignoreCooldown: true,
        }
      );

      const state = await getOrCreateUserLeaderboardState(context, normalizedUser, timestamp);

      let newNftCount = state.nftCount;
      if (hasNFT && !wasOwning) {
        newNftCount = state.nftCount + 1n;
      } else if (!hasNFT && wasOwning) {
        newNftCount = state.nftCount > 0n ? state.nftCount - 1n : 0n;
      }

      writeOwnership();

      // Update state first so calculateNFTMultiplierFromUser can read current ownership
      context.UserLeaderboardState.set({
        ...state,
        nftCount: newNftCount,
        lastUpdate: timestamp,
      });

      const newNftMultiplier = await calculateNFTMultiplierFromUser(context, normalizedUser);
      const combinedMultiplier = composeCombinedMultiplierBps(
        newNftMultiplier,
        state.specialEditionMultiplier,
        state.vpMultiplier
      );

      context.UserLeaderboardState.set({
        ...state,
        nftCount: newNftCount,
        nftMultiplier: newNftMultiplier,
        combinedMultiplier,
        lastUpdate: timestamp,
      });

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
    } else {
      writeOwnership();
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
