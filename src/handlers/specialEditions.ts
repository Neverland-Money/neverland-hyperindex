/**
 * SpecialEditionRegistry event handlers.
 *
 * Special editions are token-bound DustLock metadata used as an independent
 * leaderboard multiplier source. Eligibility activates only from registry
 * events; app timestamps are intentionally not used for scoring.
 */

import { normalizeAddress } from '../helpers/constants';
import {
  ZERO_ADDRESS,
  applyUserSpecialEditionDelta,
  getOrCreateSpecialEditionRegistryState,
  recordProtocolTransaction,
  settlePointsForUser,
} from './shared';

import { SpecialEditionRegistry } from '../../generated';
import type { handlerContext } from '../../generated';

const SPECIAL_EDITION_TRANSFER_OUT = 'SPECIAL_EDITION_TRANSFER_OUT';
const SPECIAL_EDITION_TRANSFER_IN = 'SPECIAL_EDITION_TRANSFER_IN';

function asBigInt(value: bigint | number | string): bigint {
  return BigInt(value);
}

function eventTimestamp(
  eventTimestampValue: bigint | number | string,
  blockTimestamp: number
): number {
  const parsed = Number(eventTimestampValue);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : blockTimestamp;
}

function eventLogIndex(logIndex: bigint | number | string | undefined): number {
  return Number(logIndex ?? 0);
}

function editionConfigId(editionId: bigint): string {
  return editionId.toString();
}

function membershipId(tokenId: bigint, editionId: bigint): string {
  return `${tokenId.toString()}:${editionId.toString()}`;
}

async function getOrCreateTokenState(context: handlerContext, tokenId: bigint, timestamp: number) {
  const id = tokenId.toString();
  let state = await context.SpecialEditionTokenState.get(id);
  if (!state) {
    state = {
      id,
      tokenId,
      editionBitmap: 0n,
      editionIds: [],
      updatedAt: timestamp,
    };
    context.SpecialEditionTokenState.set(state);
  }
  return state;
}

function applyEditionId(ids: readonly bigint[], editionId: bigint, active: boolean): bigint[] {
  const exists = ids.some(id => id === editionId);
  if (active && !exists) return [...ids, editionId].sort((a, b) => Number(a - b));
  if (!active && exists) return ids.filter(id => id !== editionId);
  return [...ids];
}

function updateBitmap(currentBitmap: bigint, editionId: bigint, active: boolean): bigint {
  const mask = 1n << editionId;
  return active ? currentBitmap | mask : currentBitmap & ~mask;
}

async function writeConfigSnapshot(
  context: handlerContext,
  editionId: bigint,
  timestamp: number,
  txHash: string,
  logIndex: number,
  changeReason: string
) {
  const config = await context.SpecialEditionConfig.get(editionConfigId(editionId));
  if (!config) return;

  context.SpecialEditionConfigSnapshot.set({
    id: `${editionId.toString()}:${timestamp}:${txHash}:${logIndex}:${changeReason}`,
    editionId,
    key: config.key,
    name: config.name,
    perTokenBoostBps: config.perTokenBoostBps,
    enabled: config.enabled,
    timestamp,
    txHash,
    logIndex,
    changeReason,
  });
}

async function settleOwnerBeforeSpecialEditionChange(
  context: handlerContext,
  owner: string,
  timestamp: number,
  blockNumber: bigint
) {
  const normalizedOwner = normalizeAddress(owner);
  if (normalizedOwner === ZERO_ADDRESS) return;

  await settlePointsForUser(context, normalizedOwner, null, timestamp, blockNumber, {
    ignoreCooldown: true,
    skipNftSync: true,
  });
}

async function applyMembershipChange(
  context: handlerContext,
  tokenId: bigint,
  editionId: bigint,
  active: boolean,
  sourceHash: string,
  timestamp: number,
  blockNumber: bigint,
  txHash: string,
  logIndex: number,
  reason: string,
  emittedBitmap?: bigint
) {
  const existing = await context.SpecialEditionTokenMembership.get(
    membershipId(tokenId, editionId)
  );
  if (existing?.active === active) {
    return;
  }

  const token = await context.DustLockToken.get(tokenId.toString());
  const owner = token?.owner ? normalizeAddress(token.owner) : ZERO_ADDRESS;
  await settleOwnerBeforeSpecialEditionChange(context, owner, timestamp, blockNumber);

  const tokenState = await getOrCreateTokenState(context, tokenId, timestamp);
  const editionIds = applyEditionId(tokenState.editionIds, editionId, active);
  const editionBitmap =
    emittedBitmap !== undefined
      ? emittedBitmap
      : updateBitmap(tokenState.editionBitmap, editionId, active);

  context.SpecialEditionTokenState.set({
    ...tokenState,
    editionBitmap,
    editionIds,
    updatedAt: timestamp,
  });

  context.SpecialEditionTokenMembership.set({
    id: membershipId(tokenId, editionId),
    tokenId,
    editionId,
    active,
    sourceHash,
    registeredAt: existing?.registeredAt ?? timestamp,
    correctedAt: reason === 'REGISTERED' ? existing?.correctedAt : timestamp,
    txHash,
    logIndex,
  });

  if (owner !== ZERO_ADDRESS) {
    await applyUserSpecialEditionDelta(
      context,
      owner,
      editionId,
      active ? 1n : -1n,
      timestamp,
      txHash,
      reason,
      logIndex
    );
  }
}

export async function handleDustLockSpecialEditionTransfer(
  context: handlerContext,
  tokenId: bigint,
  from: string,
  to: string,
  timestamp: number,
  blockNumber: bigint,
  txHash: string,
  logIndex: number
) {
  const normalizedFrom = normalizeAddress(from);
  const normalizedTo = normalizeAddress(to);
  if (normalizedFrom === normalizedTo) return;

  const tokenState = await context.SpecialEditionTokenState.get(tokenId.toString());
  if (!tokenState || tokenState.editionIds.length === 0) return;

  if (normalizedFrom !== ZERO_ADDRESS) {
    await settleOwnerBeforeSpecialEditionChange(context, normalizedFrom, timestamp, blockNumber);
    for (const editionId of tokenState.editionIds) {
      const membership = await context.SpecialEditionTokenMembership.get(
        membershipId(tokenId, editionId)
      );
      if (!membership?.active) continue;
      await applyUserSpecialEditionDelta(
        context,
        normalizedFrom,
        editionId,
        -1n,
        timestamp,
        txHash,
        SPECIAL_EDITION_TRANSFER_OUT,
        logIndex
      );
    }
  }

  if (normalizedTo !== ZERO_ADDRESS) {
    await settleOwnerBeforeSpecialEditionChange(context, normalizedTo, timestamp, blockNumber);
    for (const editionId of tokenState.editionIds) {
      const membership = await context.SpecialEditionTokenMembership.get(
        membershipId(tokenId, editionId)
      );
      if (!membership?.active) continue;
      await applyUserSpecialEditionDelta(
        context,
        normalizedTo,
        editionId,
        1n,
        timestamp,
        txHash,
        SPECIAL_EDITION_TRANSFER_IN,
        logIndex
      );
    }
  }
}

SpecialEditionRegistry.EditionCreated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const editionId = asBigInt(event.params.editionId);
  const timestamp = eventTimestamp(event.params.timestamp, Number(event.block.timestamp));
  const registry = await getOrCreateSpecialEditionRegistryState(context, timestamp);
  const editionIds = registry.editionIds.some(id => id === editionId)
    ? registry.editionIds
    : [...registry.editionIds, editionId].sort((a, b) => Number(a - b));

  context.SpecialEditionRegistryState.set({
    ...registry,
    editionIds,
    lastUpdate: timestamp,
  });

  context.SpecialEditionConfig.set({
    id: editionConfigId(editionId),
    editionId,
    key: event.params.key,
    name: event.params.name,
    perTokenBoostBps: event.params.perTokenBoostBps,
    enabled: event.params.enabled,
    exists: true,
    createdAt: timestamp,
    updatedAt: timestamp,
    changeTimestamps: [timestamp],
    boostBpsHistory: [event.params.perTokenBoostBps],
    enabledHistory: [event.params.enabled ? 1n : 0n],
  });

  await writeConfigSnapshot(
    context,
    editionId,
    timestamp,
    event.transaction.hash,
    eventLogIndex(event.logIndex),
    'EDITION_CREATED'
  );
});

SpecialEditionRegistry.EditionConfigured.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const editionId = asBigInt(event.params.editionId);
  const timestamp = eventTimestamp(event.params.timestamp, Number(event.block.timestamp));
  const id = editionConfigId(editionId);
  const existing = await context.SpecialEditionConfig.get(id);

  context.SpecialEditionConfig.set({
    id,
    editionId,
    key: existing?.key ?? '',
    name: event.params.name,
    perTokenBoostBps: event.params.newPerTokenBoostBps,
    enabled: existing?.enabled ?? true,
    exists: true,
    createdAt: existing?.createdAt ?? timestamp,
    updatedAt: timestamp,
    changeTimestamps: [...(existing?.changeTimestamps ?? []), timestamp],
    boostBpsHistory: [...(existing?.boostBpsHistory ?? []), event.params.newPerTokenBoostBps],
    enabledHistory: [...(existing?.enabledHistory ?? []), (existing?.enabled ?? true) ? 1n : 0n],
  });

  const registry = await getOrCreateSpecialEditionRegistryState(context, timestamp);
  if (!registry.editionIds.some(current => current === editionId)) {
    context.SpecialEditionRegistryState.set({
      ...registry,
      editionIds: [...registry.editionIds, editionId].sort((a, b) => Number(a - b)),
      lastUpdate: timestamp,
    });
  }

  await writeConfigSnapshot(
    context,
    editionId,
    timestamp,
    event.transaction.hash,
    eventLogIndex(event.logIndex),
    'EDITION_CONFIGURED'
  );
});

SpecialEditionRegistry.EditionEnabledUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  const editionId = asBigInt(event.params.editionId);
  const timestamp = eventTimestamp(event.params.timestamp, Number(event.block.timestamp));
  const id = editionConfigId(editionId);
  const existing = await context.SpecialEditionConfig.get(id);

  context.SpecialEditionConfig.set({
    id,
    editionId,
    key: existing?.key ?? '',
    name: existing?.name ?? '',
    perTokenBoostBps: existing?.perTokenBoostBps ?? 0n,
    enabled: event.params.newEnabled,
    exists: true,
    createdAt: existing?.createdAt ?? timestamp,
    updatedAt: timestamp,
    changeTimestamps: [...(existing?.changeTimestamps ?? []), timestamp],
    boostBpsHistory: [...(existing?.boostBpsHistory ?? []), existing?.perTokenBoostBps ?? 0n],
    enabledHistory: [...(existing?.enabledHistory ?? []), event.params.newEnabled ? 1n : 0n],
  });

  const registry = await getOrCreateSpecialEditionRegistryState(context, timestamp);
  if (!registry.editionIds.some(current => current === editionId)) {
    context.SpecialEditionRegistryState.set({
      ...registry,
      editionIds: [...registry.editionIds, editionId].sort((a, b) => Number(a - b)),
      lastUpdate: timestamp,
    });
  }

  await writeConfigSnapshot(
    context,
    editionId,
    timestamp,
    event.transaction.hash,
    eventLogIndex(event.logIndex),
    'EDITION_ENABLED_UPDATED'
  );
});

SpecialEditionRegistry.SpecialEditionRegistered.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  await applyMembershipChange(
    context,
    event.params.tokenId,
    asBigInt(event.params.editionId),
    true,
    event.params.sourceHash,
    eventTimestamp(event.params.timestamp, Number(event.block.timestamp)),
    BigInt(event.block.number),
    event.transaction.hash,
    eventLogIndex(event.logIndex),
    'REGISTERED',
    event.params.tokenEditionBitmap
  );
});

SpecialEditionRegistry.MembershipCorrected.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );

  await applyMembershipChange(
    context,
    event.params.tokenId,
    asBigInt(event.params.editionId),
    event.params.newMember,
    event.params.sourceHash,
    eventTimestamp(event.params.timestamp, Number(event.block.timestamp)),
    BigInt(event.block.number),
    event.transaction.hash,
    eventLogIndex(event.logIndex),
    `CORRECTION:${event.params.reason}`
  );
});

SpecialEditionRegistry.SpecialEditionRegistrationBatch.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
});

SpecialEditionRegistry.PublisherUpdated.handler(async ({ event, context }) => {
  await recordProtocolTransaction(
    context,
    event.transaction.hash,
    Number(event.block.timestamp),
    BigInt(event.block.number)
  );
});
