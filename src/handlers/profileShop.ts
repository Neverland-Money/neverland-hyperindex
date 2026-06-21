/**
 * NeverlandProfileItemsSeller event handlers.
 *
 * The profile shop index is event-only. The seller contract emits all catalogue
 * configuration and purchase-line data needed for deterministic lookup without
 * receipt reads or historic chain calls.
 */

import { normalizeAddress } from '../helpers/constants';
import { getOrCreateUser, recordProtocolTransaction } from './shared';

import { NeverlandProfileItemsSeller } from '../../generated';
import type { handlerContext } from '../../generated';

const CATEGORY_PERMANENT = 'Permanent';
const CATEGORY_CONSUMABLE = 'Consumable';
const OWNERSHIP_TRANSFER_STARTED = 'OwnershipTransferStarted';
const OWNERSHIP_TRANSFERRED = 'OwnershipTransferred';

function normalizeBytes32(value: string): string {
  return value.toLowerCase();
}

function eventId(event: {
  transaction: { hash: string };
  logIndex: bigint | number | string;
}): string {
  return `${event.transaction.hash}-${Number(event.logIndex)}`;
}

function sellerAddress(event: { srcAddress: string }): string {
  return normalizeAddress(event.srcAddress);
}

function categoryName(category: bigint | number | string): string {
  const value = Number(category);
  if (value === 0) return CATEGORY_PERMANENT;
  if (value === 1) return CATEGORY_CONSUMABLE;
  return `Unknown:${value}`;
}

function isPermanentCategory(category: string): boolean {
  return category === CATEGORY_PERMANENT;
}

function firstTimestamp(current: number | undefined, timestamp: number): number {
  return current === undefined || current === 0 ? timestamp : current;
}

async function getOrCreateShopState(context: handlerContext, seller: string, timestamp: number) {
  let state = await context.ProfileShopState.get(seller);
  if (!state) {
    state = {
      id: seller,
      seller,
      owner: undefined,
      pendingOwner: undefined,
      fundsRecipient: undefined,
      paused: false,
      initializedVersion: undefined,
      itemCount: 0n,
      purchaseCount: 0n,
      purchaseLineCount: 0n,
      totalQuantitySold: 0n,
      totalRevenueDust: 0n,
      lastPurchaseId: 0n,
      firstEventAt: timestamp,
      lastUpdate: timestamp,
    };
    context.ProfileShopState.set(state);
  }
  return state;
}

function itemId(seller: string, itemSlugHash: string): string {
  return `${seller}:${itemSlugHash}`;
}

function purchaseId(seller: string, contractPurchaseId: bigint): string {
  return `${seller}:${contractPurchaseId.toString()}`;
}

function userItemId(seller: string, user: string, itemSlugHash: string): string {
  return `${seller}:${user}:${itemSlugHash}`;
}

function userStatsId(seller: string, user: string): string {
  return `${seller}:${user}`;
}

async function getOrCreateShopItem(
  context: handlerContext,
  seller: string,
  itemSlugHash: string,
  itemSlug: string,
  category: string,
  unitPrice: bigint
) {
  const id = itemId(seller, itemSlugHash);
  let item = await context.ProfileShopItem.get(id);
  if (!item) {
    item = {
      id,
      seller,
      itemSlugHash,
      itemSlug,
      price: unitPrice,
      category,
      available: false,
      configured: false,
      configCount: 0n,
      totalQuantityPurchased: 0n,
      totalPurchaseLines: 0n,
      totalRevenueDust: 0n,
      firstConfiguredAt: undefined,
      lastConfiguredAt: undefined,
      firstPurchasedAt: undefined,
      lastPurchasedAt: undefined,
    };
    context.ProfileShopItem.set(item);
  }
  return item;
}

async function getOrCreateUserItem(
  context: handlerContext,
  seller: string,
  user: string,
  itemSlugHash: string,
  itemSlug: string,
  category: string
) {
  const id = userItemId(seller, user, itemSlugHash);
  let item = await context.ProfileShopUserItem.get(id);
  if (!item) {
    item = {
      id,
      seller,
      user_id: user,
      itemSlugHash,
      itemSlug,
      category,
      permanentOwned: false,
      consumableQuantity: 0n,
      totalQuantityReceived: 0n,
      totalValueReceivedDust: 0n,
      firstPurchasedAt: undefined,
      lastPurchasedAt: undefined,
      lastPurchaseTxHash: undefined,
    };
    context.ProfileShopUserItem.set(item);
  }
  return item;
}

async function getOrCreateUserStats(
  context: handlerContext,
  seller: string,
  user: string,
  timestamp: number
) {
  const id = userStatsId(seller, user);
  let stats = await context.ProfileShopUserStats.get(id);
  if (!stats) {
    stats = {
      id,
      seller,
      user_id: user,
      buyerPurchaseCount: 0n,
      buyerLineCount: 0n,
      buyerQuantity: 0n,
      buyerSpentDust: 0n,
      recipientPurchaseCount: 0n,
      recipientLineCount: 0n,
      recipientQuantity: 0n,
      recipientValueDust: 0n,
      permanentItemsOwned: 0n,
      consumableUnitsReceived: 0n,
      firstPurchaseAt: undefined,
      lastPurchaseAt: undefined,
      updatedAt: timestamp,
    };
    context.ProfileShopUserStats.set(stats);
  }
  return stats;
}

async function updateUserStats(
  context: handlerContext,
  seller: string,
  buyer: string,
  recipient: string,
  isNewPurchase: boolean,
  quantity: bigint,
  totalPrice: bigint,
  permanentOwnedDelta: bigint,
  consumableQuantityDelta: bigint,
  timestamp: number
) {
  await getOrCreateUser(context, buyer);
  if (buyer !== recipient) {
    await getOrCreateUser(context, recipient);
  }

  const [buyerStats, recipientStats] =
    buyer === recipient
      ? [await getOrCreateUserStats(context, seller, buyer, timestamp), undefined]
      : await Promise.all([
          getOrCreateUserStats(context, seller, buyer, timestamp),
          getOrCreateUserStats(context, seller, recipient, timestamp),
        ]);
  const nextBuyerStats = {
    ...buyerStats,
    buyerPurchaseCount: buyerStats.buyerPurchaseCount + (isNewPurchase ? 1n : 0n),
    buyerLineCount: buyerStats.buyerLineCount + 1n,
    buyerQuantity: buyerStats.buyerQuantity + quantity,
    buyerSpentDust: buyerStats.buyerSpentDust + totalPrice,
    firstPurchaseAt: firstTimestamp(buyerStats.firstPurchaseAt, timestamp),
    lastPurchaseAt: timestamp,
    updatedAt: timestamp,
  };

  if (buyer === recipient) {
    context.ProfileShopUserStats.set({
      ...nextBuyerStats,
      recipientPurchaseCount: nextBuyerStats.recipientPurchaseCount + (isNewPurchase ? 1n : 0n),
      recipientLineCount: nextBuyerStats.recipientLineCount + 1n,
      recipientQuantity: nextBuyerStats.recipientQuantity + quantity,
      recipientValueDust: nextBuyerStats.recipientValueDust + totalPrice,
      permanentItemsOwned: nextBuyerStats.permanentItemsOwned + permanentOwnedDelta,
      consumableUnitsReceived: nextBuyerStats.consumableUnitsReceived + consumableQuantityDelta,
    });
    return;
  }

  context.ProfileShopUserStats.set(nextBuyerStats);

  if (!recipientStats) {
    throw new Error(`Missing profile shop recipient stats for ${recipient}`);
  }

  context.ProfileShopUserStats.set({
    ...recipientStats,
    recipientPurchaseCount: recipientStats.recipientPurchaseCount + (isNewPurchase ? 1n : 0n),
    recipientLineCount: recipientStats.recipientLineCount + 1n,
    recipientQuantity: recipientStats.recipientQuantity + quantity,
    recipientValueDust: recipientStats.recipientValueDust + totalPrice,
    permanentItemsOwned: recipientStats.permanentItemsOwned + permanentOwnedDelta,
    consumableUnitsReceived: recipientStats.consumableUnitsReceived + consumableQuantityDelta,
    firstPurchaseAt: firstTimestamp(recipientStats.firstPurchaseAt, timestamp),
    lastPurchaseAt: timestamp,
    updatedAt: timestamp,
  });
}

NeverlandProfileItemsSeller.ItemConfigured.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const seller = sellerAddress(event);
  const slugHash = normalizeBytes32(event.params.itemSlugHash);
  const category = categoryName(event.params.category);
  const [current, state] = await Promise.all([
    context.ProfileShopItem.get(itemId(seller, slugHash)),
    getOrCreateShopState(context, seller, timestamp),
  ]);
  const isNewConfiguredItem = !current?.configured;

  context.ProfileShopItem.set({
    id: itemId(seller, slugHash),
    seller,
    itemSlugHash: slugHash,
    itemSlug: event.params.itemSlug,
    price: event.params.newPrice,
    category,
    available: event.params.available,
    configured: true,
    configCount: (current?.configCount ?? 0n) + 1n,
    totalQuantityPurchased: current?.totalQuantityPurchased ?? 0n,
    totalPurchaseLines: current?.totalPurchaseLines ?? 0n,
    totalRevenueDust: current?.totalRevenueDust ?? 0n,
    firstConfiguredAt: current?.firstConfiguredAt ?? timestamp,
    lastConfiguredAt: timestamp,
    firstPurchasedAt: current?.firstPurchasedAt,
    lastPurchasedAt: current?.lastPurchasedAt,
  });

  context.ProfileShopItemConfigHistory.set({
    id: eventId(event),
    seller,
    itemSlugHash: slugHash,
    itemSlug: event.params.itemSlug,
    oldPrice: event.params.oldPrice,
    newPrice: event.params.newPrice,
    category,
    available: event.params.available,
    timestamp,
    blockNumber,
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
  });

  context.ProfileShopState.set({
    ...state,
    itemCount: state.itemCount + (isNewConfiguredItem ? 1n : 0n),
    firstEventAt: firstTimestamp(state.firstEventAt, timestamp),
    lastUpdate: timestamp,
  });
});

NeverlandProfileItemsSeller.ItemPurchased.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const seller = sellerAddress(event);
  const lineId = eventId(event);
  const existingLine = await context.ProfileShopPurchaseLine.get(lineId);
  if (existingLine) return;

  const buyer = normalizeAddress(event.params.buyer);
  const recipient = normalizeAddress(event.params.recipient);
  const slugHash = normalizeBytes32(event.params.itemSlugHash);
  const category = categoryName(event.params.category);
  const permanent = isPermanentCategory(category);
  const purchaseEntityId = purchaseId(seller, event.params.purchaseId);
  const [purchase, shopItem, userItem, state] = await Promise.all([
    context.ProfileShopPurchase.get(purchaseEntityId),
    getOrCreateShopItem(
      context,
      seller,
      slugHash,
      event.params.itemSlug,
      category,
      event.params.unitPrice
    ),
    getOrCreateUserItem(context, seller, recipient, slugHash, event.params.itemSlug, category),
    getOrCreateShopState(context, seller, timestamp),
  ]);
  const isNewPurchase = !purchase;
  const lineIndex = purchase ? Number(purchase.lineCount) : 0;
  const isGift = buyer !== recipient;

  context.ProfileShopPurchaseLine.set({
    id: lineId,
    seller,
    purchase_id: purchaseEntityId,
    purchaseId: event.params.purchaseId,
    lineIndex,
    buyer,
    recipient,
    isGift,
    itemSlugHash: slugHash,
    itemSlug: event.params.itemSlug,
    category,
    quantity: event.params.quantity,
    unitPrice: event.params.unitPrice,
    totalPrice: event.params.totalPrice,
    timestamp,
    blockNumber,
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
  });

  context.ProfileShopPurchase.set(
    purchase
      ? {
          ...purchase,
          lineCount: purchase.lineCount + 1n,
          totalPrice: purchase.totalPrice + event.params.totalPrice,
          lineIds: [...purchase.lineIds, lineId],
          lastLogIndex: Number(event.logIndex),
        }
      : {
          id: purchaseEntityId,
          seller,
          purchaseId: event.params.purchaseId,
          buyer,
          recipient,
          isGift,
          lineCount: 1n,
          totalPrice: event.params.totalPrice,
          lineIds: [lineId],
          timestamp,
          blockNumber,
          txHash: event.transaction.hash,
          firstLogIndex: Number(event.logIndex),
          lastLogIndex: Number(event.logIndex),
        }
  );

  context.ProfileShopItem.set({
    ...shopItem,
    itemSlug: event.params.itemSlug,
    price: shopItem.configured ? shopItem.price : event.params.unitPrice,
    category,
    totalQuantityPurchased: shopItem.totalQuantityPurchased + event.params.quantity,
    totalPurchaseLines: shopItem.totalPurchaseLines + 1n,
    totalRevenueDust: shopItem.totalRevenueDust + event.params.totalPrice,
    firstPurchasedAt: firstTimestamp(shopItem.firstPurchasedAt, timestamp),
    lastPurchasedAt: timestamp,
  });

  const permanentOwnedDelta = permanent && !userItem.permanentOwned ? 1n : 0n;
  const consumableQuantityDelta = permanent ? 0n : event.params.quantity;
  context.ProfileShopUserItem.set({
    ...userItem,
    itemSlug: event.params.itemSlug,
    category,
    permanentOwned: userItem.permanentOwned || permanent,
    consumableQuantity: userItem.consumableQuantity + consumableQuantityDelta,
    totalQuantityReceived: userItem.totalQuantityReceived + event.params.quantity,
    totalValueReceivedDust: userItem.totalValueReceivedDust + event.params.totalPrice,
    firstPurchasedAt: firstTimestamp(userItem.firstPurchasedAt, timestamp),
    lastPurchasedAt: timestamp,
    lastPurchaseTxHash: event.transaction.hash,
  });

  await updateUserStats(
    context,
    seller,
    buyer,
    recipient,
    isNewPurchase,
    event.params.quantity,
    event.params.totalPrice,
    permanentOwnedDelta,
    consumableQuantityDelta,
    timestamp
  );

  context.ProfileShopState.set({
    ...state,
    purchaseCount: state.purchaseCount + (isNewPurchase ? 1n : 0n),
    purchaseLineCount: state.purchaseLineCount + 1n,
    totalQuantitySold: state.totalQuantitySold + event.params.quantity,
    totalRevenueDust: state.totalRevenueDust + event.params.totalPrice,
    lastPurchaseId:
      event.params.purchaseId > state.lastPurchaseId
        ? event.params.purchaseId
        : state.lastPurchaseId,
    firstEventAt: firstTimestamp(state.firstEventAt, timestamp),
    lastUpdate: timestamp,
  });
});

NeverlandProfileItemsSeller.FundsRecipientUpdated.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const seller = sellerAddress(event);
  const state = await getOrCreateShopState(context, seller, timestamp);
  const newFundsRecipient = normalizeAddress(event.params.newFundsRecipient);

  context.ProfileShopState.set({
    ...state,
    fundsRecipient: newFundsRecipient,
    firstEventAt: firstTimestamp(state.firstEventAt, timestamp),
    lastUpdate: timestamp,
  });

  context.ProfileShopFundsRecipientUpdate.set({
    id: eventId(event),
    seller,
    oldFundsRecipient: normalizeAddress(event.params.oldFundsRecipient),
    newFundsRecipient,
    timestamp,
    blockNumber,
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
  });
});

NeverlandProfileItemsSeller.OwnershipTransferStarted.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const seller = sellerAddress(event);
  const state = await getOrCreateShopState(context, seller, timestamp);
  const newOwner = normalizeAddress(event.params.newOwner);

  context.ProfileShopState.set({
    ...state,
    pendingOwner: newOwner,
    firstEventAt: firstTimestamp(state.firstEventAt, timestamp),
    lastUpdate: timestamp,
  });

  context.ProfileShopOwnershipEvent.set({
    id: eventId(event),
    seller,
    eventType: OWNERSHIP_TRANSFER_STARTED,
    previousOwner: normalizeAddress(event.params.previousOwner),
    newOwner,
    timestamp,
    blockNumber,
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
  });
});

NeverlandProfileItemsSeller.OwnershipTransferred.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const seller = sellerAddress(event);
  const state = await getOrCreateShopState(context, seller, timestamp);
  const newOwner = normalizeAddress(event.params.newOwner);

  context.ProfileShopState.set({
    ...state,
    owner: newOwner,
    pendingOwner: undefined,
    firstEventAt: firstTimestamp(state.firstEventAt, timestamp),
    lastUpdate: timestamp,
  });

  context.ProfileShopOwnershipEvent.set({
    id: eventId(event),
    seller,
    eventType: OWNERSHIP_TRANSFERRED,
    previousOwner: normalizeAddress(event.params.previousOwner),
    newOwner,
    timestamp,
    blockNumber,
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
  });
});

NeverlandProfileItemsSeller.Paused.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const seller = sellerAddress(event);
  const state = await getOrCreateShopState(context, seller, timestamp);

  context.ProfileShopState.set({
    ...state,
    paused: true,
    firstEventAt: firstTimestamp(state.firstEventAt, timestamp),
    lastUpdate: timestamp,
  });

  context.ProfileShopPauseEvent.set({
    id: eventId(event),
    seller,
    account: normalizeAddress(event.params.account),
    paused: true,
    timestamp,
    blockNumber,
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
  });
});

NeverlandProfileItemsSeller.Unpaused.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const seller = sellerAddress(event);
  const state = await getOrCreateShopState(context, seller, timestamp);

  context.ProfileShopState.set({
    ...state,
    paused: false,
    firstEventAt: firstTimestamp(state.firstEventAt, timestamp),
    lastUpdate: timestamp,
  });

  context.ProfileShopPauseEvent.set({
    id: eventId(event),
    seller,
    account: normalizeAddress(event.params.account),
    paused: false,
    timestamp,
    blockNumber,
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
  });
});

NeverlandProfileItemsSeller.Initialized.handler(async ({ event, context }) => {
  const timestamp = Number(event.block.timestamp);
  const blockNumber = BigInt(event.block.number);
  await recordProtocolTransaction(context, event.transaction.hash, timestamp, blockNumber);

  const seller = sellerAddress(event);
  const state = await getOrCreateShopState(context, seller, timestamp);
  const version = Number(event.params.version);

  context.ProfileShopState.set({
    ...state,
    initializedVersion: version,
    firstEventAt: firstTimestamp(state.firstEventAt, timestamp),
    lastUpdate: timestamp,
  });

  context.ProfileShopInitialized.set({
    id: eventId(event),
    seller,
    version,
    timestamp,
    blockNumber,
    txHash: event.transaction.hash,
    logIndex: Number(event.logIndex),
  });
});
