import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TestHelpers } from './v3-test-helpers';

process.env.ENVIO_ENABLE_EXTERNAL_CALLS = 'false';
process.env.ENVIO_ENABLE_ETH_CALLS = 'false';

const SELLER = '0xcee8d6d42a37a9d721b41cefa16dab7f0d0a14eb';
const BUYER = '0x000000000000000000000000000000000000a111';
const RECIPIENT = '0x000000000000000000000000000000000000a222';
const OWNER = '0x000000000000000000000000000000000000a333';
const NEXT_OWNER = '0x000000000000000000000000000000000000a444';
const FUNDS_RECIPIENT = '0x000000000000000000000000000000000000a555';
const NEW_FUNDS_RECIPIENT = '0x000000000000000000000000000000000000a666';

const PERMANENT_SLUG = 'title-tide-rider';
const PERMANENT_HASH = '0xe0efbca5026bd51c368115f4f16c9df26b320450346d25a9d076be011807d37a';
const CONSUMABLE_SLUG = 'consumable-fairy-wand';
const CONSUMABLE_HASH = '0x02270f741a3c4bc0cd6fbb2b77c33654063b20edcb115fe390a7f9482449b5e6';

function txHash(txSuffix: number) {
  return `0x${txSuffix.toString(16).padStart(64, '0')}`;
}

function eventData(blockNumber: number, timestamp: number, txSuffix: number, logIndex = txSuffix) {
  return {
    mockEventData: {
      block: { number: blockNumber, timestamp },
      logIndex,
      srcAddress: SELLER,
      transaction: {
        hash: txHash(txSuffix),
      },
    },
  };
}

async function configurePermanent(mockDb: ReturnType<typeof TestHelpers.MockDb.createMockDb>) {
  const event = TestHelpers.NeverlandProfileItemsSeller.ItemConfigured.createMockEvent({
    itemSlugHash: PERMANENT_HASH,
    itemSlug: PERMANENT_SLUG,
    oldPrice: 0n,
    newPrice: 100n,
    category: 0,
    available: true,
    ...eventData(100, 1000, 1),
  });
  return await TestHelpers.NeverlandProfileItemsSeller.ItemConfigured.processEvent({
    event,
    mockDb,
  });
}

async function configureConsumable(mockDb: ReturnType<typeof TestHelpers.MockDb.createMockDb>) {
  const event = TestHelpers.NeverlandProfileItemsSeller.ItemConfigured.createMockEvent({
    itemSlugHash: CONSUMABLE_HASH,
    itemSlug: CONSUMABLE_SLUG,
    oldPrice: 0n,
    newPrice: 5n,
    category: 1,
    available: true,
    ...eventData(101, 1010, 2),
  });
  return await TestHelpers.NeverlandProfileItemsSeller.ItemConfigured.processEvent({
    event,
    mockDb,
  });
}

test('profile shop item configuration builds catalogue and history', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();

  mockDb = await configurePermanent(mockDb);

  const itemId = `${SELLER}:${PERMANENT_HASH}`;
  let item = mockDb.entities.ProfileShopItem.get(itemId);
  assert.equal(item?.itemSlug, PERMANENT_SLUG);
  assert.equal(item?.price, 100n);
  assert.equal(item?.category, 'Permanent');
  assert.equal(item?.available, true);
  assert.equal(item?.configured, true);
  assert.equal(item?.configCount, 1n);

  let state = mockDb.entities.ProfileShopState.get(SELLER);
  assert.equal(state?.itemCount, 1n);
  assert.equal(state?.purchaseCount, 0n);

  const history = mockDb.entities.ProfileShopItemConfigHistory.get(`${txHash(1)}-1`);
  assert.equal(history?.oldPrice, 0n);
  assert.equal(history?.newPrice, 100n);
  assert.equal(history?.itemSlugHash, PERMANENT_HASH);

  const reconfigure = TestHelpers.NeverlandProfileItemsSeller.ItemConfigured.createMockEvent({
    itemSlugHash: PERMANENT_HASH,
    itemSlug: PERMANENT_SLUG,
    oldPrice: 100n,
    newPrice: 150n,
    category: 0,
    available: false,
    ...eventData(102, 1020, 3),
  });
  mockDb = await TestHelpers.NeverlandProfileItemsSeller.ItemConfigured.processEvent({
    event: reconfigure,
    mockDb,
  });

  item = mockDb.entities.ProfileShopItem.get(itemId);
  assert.equal(item?.price, 150n);
  assert.equal(item?.available, false);
  assert.equal(item?.configCount, 2n);

  state = mockDb.entities.ProfileShopState.get(SELLER);
  assert.equal(state?.itemCount, 1n);
});

test('profile shop purchases expose checkout, line, item, and user lookups', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = await configurePermanent(mockDb);
  mockDb = await configureConsumable(mockDb);

  const purchaseTxHash = txHash(10);
  const permanentPurchase = TestHelpers.NeverlandProfileItemsSeller.ItemPurchased.createMockEvent({
    buyer: BUYER,
    recipient: RECIPIENT,
    itemSlugHash: PERMANENT_HASH,
    purchaseId: 7n,
    itemSlug: PERMANENT_SLUG,
    category: 0,
    quantity: 1n,
    unitPrice: 100n,
    totalPrice: 100n,
    mockEventData: {
      block: { number: 110, timestamp: 1100 },
      logIndex: 10,
      srcAddress: SELLER,
      transaction: { hash: purchaseTxHash },
    },
  });
  mockDb = await TestHelpers.NeverlandProfileItemsSeller.ItemPurchased.processEvent({
    event: permanentPurchase,
    mockDb,
  });

  const consumablePurchase = TestHelpers.NeverlandProfileItemsSeller.ItemPurchased.createMockEvent({
    buyer: BUYER,
    recipient: RECIPIENT,
    itemSlugHash: CONSUMABLE_HASH,
    purchaseId: 7n,
    itemSlug: CONSUMABLE_SLUG,
    category: 1,
    quantity: 3n,
    unitPrice: 5n,
    totalPrice: 15n,
    mockEventData: {
      block: { number: 110, timestamp: 1100 },
      logIndex: 11,
      srcAddress: SELLER,
      transaction: { hash: purchaseTxHash },
    },
  });
  mockDb = await TestHelpers.NeverlandProfileItemsSeller.ItemPurchased.processEvent({
    event: consumablePurchase,
    mockDb,
  });

  const purchaseId = `${SELLER}:7`;
  const purchase = mockDb.entities.ProfileShopPurchase.get(purchaseId);
  assert.equal(purchase?.buyer, BUYER);
  assert.equal(purchase?.recipient, RECIPIENT);
  assert.equal(purchase?.isGift, true);
  assert.equal(purchase?.lineCount, 2n);
  assert.equal(purchase?.totalPrice, 115n);
  assert.deepEqual(purchase?.lineIds, [`${purchaseTxHash}-10`, `${purchaseTxHash}-11`]);

  const firstLine = mockDb.entities.ProfileShopPurchaseLine.get(`${purchaseTxHash}-10`);
  assert.equal(firstLine?.purchase_id, purchaseId);
  assert.equal(firstLine?.lineIndex, 0);
  assert.equal(firstLine?.itemSlug, PERMANENT_SLUG);
  assert.equal(firstLine?.category, 'Permanent');

  const permanentItem = mockDb.entities.ProfileShopUserItem.get(
    `${SELLER}:${RECIPIENT}:${PERMANENT_HASH}`
  );
  assert.equal(permanentItem?.permanentOwned, true);
  assert.equal(permanentItem?.totalQuantityReceived, 1n);

  const consumableItem = mockDb.entities.ProfileShopUserItem.get(
    `${SELLER}:${RECIPIENT}:${CONSUMABLE_HASH}`
  );
  assert.equal(consumableItem?.permanentOwned, false);
  assert.equal(consumableItem?.consumableQuantity, 3n);
  assert.equal(consumableItem?.totalValueReceivedDust, 15n);

  const buyerStats = mockDb.entities.ProfileShopUserStats.get(`${SELLER}:${BUYER}`);
  assert.equal(buyerStats?.buyerPurchaseCount, 1n);
  assert.equal(buyerStats?.buyerLineCount, 2n);
  assert.equal(buyerStats?.buyerQuantity, 4n);
  assert.equal(buyerStats?.buyerSpentDust, 115n);
  assert.equal(buyerStats?.recipientPurchaseCount, 0n);

  const recipientStats = mockDb.entities.ProfileShopUserStats.get(`${SELLER}:${RECIPIENT}`);
  assert.equal(recipientStats?.recipientPurchaseCount, 1n);
  assert.equal(recipientStats?.recipientLineCount, 2n);
  assert.equal(recipientStats?.recipientQuantity, 4n);
  assert.equal(recipientStats?.recipientValueDust, 115n);
  assert.equal(recipientStats?.permanentItemsOwned, 1n);
  assert.equal(recipientStats?.consumableUnitsReceived, 3n);

  const state = mockDb.entities.ProfileShopState.get(SELLER);
  assert.equal(state?.purchaseCount, 1n);
  assert.equal(state?.purchaseLineCount, 2n);
  assert.equal(state?.totalQuantitySold, 4n);
  assert.equal(state?.totalRevenueDust, 115n);
  assert.equal(state?.lastPurchaseId, 7n);
});

test('profile shop admin events update state and audit entities', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();

  const initialized = TestHelpers.NeverlandProfileItemsSeller.Initialized.createMockEvent({
    version: 1,
    ...eventData(200, 2000, 20),
  });
  mockDb = await TestHelpers.NeverlandProfileItemsSeller.Initialized.processEvent({
    event: initialized,
    mockDb,
  });

  const transferStarted =
    TestHelpers.NeverlandProfileItemsSeller.OwnershipTransferStarted.createMockEvent({
      previousOwner: OWNER,
      newOwner: NEXT_OWNER,
      ...eventData(201, 2010, 21),
    });
  mockDb = await TestHelpers.NeverlandProfileItemsSeller.OwnershipTransferStarted.processEvent({
    event: transferStarted,
    mockDb,
  });

  const transferred = TestHelpers.NeverlandProfileItemsSeller.OwnershipTransferred.createMockEvent({
    previousOwner: OWNER,
    newOwner: NEXT_OWNER,
    ...eventData(202, 2020, 22),
  });
  mockDb = await TestHelpers.NeverlandProfileItemsSeller.OwnershipTransferred.processEvent({
    event: transferred,
    mockDb,
  });

  const funds = TestHelpers.NeverlandProfileItemsSeller.FundsRecipientUpdated.createMockEvent({
    oldFundsRecipient: FUNDS_RECIPIENT,
    newFundsRecipient: NEW_FUNDS_RECIPIENT,
    ...eventData(203, 2030, 23),
  });
  mockDb = await TestHelpers.NeverlandProfileItemsSeller.FundsRecipientUpdated.processEvent({
    event: funds,
    mockDb,
  });

  const paused = TestHelpers.NeverlandProfileItemsSeller.Paused.createMockEvent({
    account: NEXT_OWNER,
    ...eventData(204, 2040, 24),
  });
  mockDb = await TestHelpers.NeverlandProfileItemsSeller.Paused.processEvent({
    event: paused,
    mockDb,
  });

  const unpaused = TestHelpers.NeverlandProfileItemsSeller.Unpaused.createMockEvent({
    account: NEXT_OWNER,
    ...eventData(205, 2050, 25),
  });
  mockDb = await TestHelpers.NeverlandProfileItemsSeller.Unpaused.processEvent({
    event: unpaused,
    mockDb,
  });

  const state = mockDb.entities.ProfileShopState.get(SELLER);
  assert.equal(state?.initializedVersion, 1);
  assert.equal(state?.owner, NEXT_OWNER);
  assert.equal(state?.pendingOwner, undefined);
  assert.equal(state?.fundsRecipient, NEW_FUNDS_RECIPIENT);
  assert.equal(state?.paused, false);

  const ownershipEvent = mockDb.entities.ProfileShopOwnershipEvent.get(`${txHash(22)}-22`);
  assert.equal(ownershipEvent?.eventType, 'OwnershipTransferred');
  assert.equal(ownershipEvent?.newOwner, NEXT_OWNER);

  const fundsEvent = mockDb.entities.ProfileShopFundsRecipientUpdate.get(`${txHash(23)}-23`);
  assert.equal(fundsEvent?.newFundsRecipient, NEW_FUNDS_RECIPIENT);

  const pauseEvent = mockDb.entities.ProfileShopPauseEvent.get(`${txHash(25)}-25`);
  assert.equal(pauseEvent?.paused, false);
});
