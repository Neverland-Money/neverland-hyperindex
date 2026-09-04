import assert from 'node:assert/strict';
import { test } from 'node:test';

import { ZERO_ADDRESS } from '../helpers/constants';
import {
  TestHelpers,
  getRegisteredEventHandler,
  type MockDb,
  type EntityRow,
} from './v3-test-helpers';

import type { EvmOnEventContext as handlerContext } from 'envio';
process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';

const REGISTRY = '0x000000000000000000000000000000000000e001';
const DUST_LOCK = '0x000000000000000000000000000000000000e002';
const ALICE = '0x000000000000000000000000000000000000e003';
const BOB = '0x000000000000000000000000000000000000e004';
const PUBLISHER = '0x000000000000000000000000000000000000e005';

function eventData(counter: number, timestamp: number, srcAddress = REGISTRY) {
  return {
    mockEventData: {
      block: { number: counter, timestamp },
      logIndex: counter,
      srcAddress,
      transaction: { hash: `0x${counter.toString(16).padStart(64, '0')}` },
    },
  };
}

type SpecialEditionEvent = ReturnType<
  typeof TestHelpers.SpecialEditionRegistry.EditionCreated.createMockEvent
>;

type RegisteredHandler = (args: {
  event: SpecialEditionEvent;
  context: handlerContext;
}) => Promise<void>;

// v2 keeps each handler on the event's own register (see the seam); the v3 global
// registration table this used to read does not exist here.
async function getRegisteredSpecialEditionHandler(eventName: string): Promise<RegisteredHandler> {
  return (await getRegisteredEventHandler(
    'SpecialEditionRegistry',
    eventName
  )) as RegisteredHandler;
}

function createPreloadContext(mockDb: MockDb): handlerContext {
  const stores = new Map<string, object>();
  const target = {
    isPreload: true,
    log: {
      debug: (_message: unknown) => {},
      info: (_message: unknown) => {},
      warn: (_message: unknown) => {},
      error: (_message: unknown) => {},
    },
  };

  return new Proxy(target, {
    get(current, property) {
      if (property in current) return current[property as keyof typeof current];
      if (typeof property !== 'string') return undefined;

      let store = stores.get(property);
      if (!store) {
        const entity = mockDb.entities[property];
        store = {
          get: async (id: string) => entity.get(id),
          getAll: async () => entity.getAll(),
          getWhere: async () => entity.getAll(),
          getOrCreate: async (row: { id: string }) => entity.get(row.id) ?? row,
          set: (_row: unknown) => {},
          deleteUnsafe: (_id: string) => {},
        };
        stores.set(property, store);
      }
      return store;
    },
  }) as unknown as handlerContext;
}

async function createEdition(
  mockDb: MockDb,
  editionId: bigint,
  counter: number,
  timestamp: bigint,
  blockTimestamp: number,
  boost = 500n,
  enabled = true
) {
  const event = TestHelpers.SpecialEditionRegistry.EditionCreated.createMockEvent({
    editionId,
    key: `KEY_${editionId.toString()}`,
    name: `Edition ${editionId.toString()}`,
    perTokenBoostBps: boost,
    enabled,
    timestamp,
    totalEditions: editionId,
    ...eventData(counter, blockTimestamp),
  });
  return TestHelpers.SpecialEditionRegistry.EditionCreated.processEvent({ event, mockDb });
}

test('special-edition preload callbacks fulfill for new and out-of-order config events', async () => {
  const events = [
    [
      'EditionCreated',
      TestHelpers.SpecialEditionRegistry.EditionCreated.createMockEvent({
        editionId: 10n,
        key: 'NEW',
        name: 'New edition',
        perTokenBoostBps: 500n,
        enabled: true,
        timestamp: 1000n,
        totalEditions: 1n,
        ...eventData(100, 1000),
      }),
    ],
    [
      'EditionConfigured',
      TestHelpers.SpecialEditionRegistry.EditionConfigured.createMockEvent({
        editionId: 11n,
        name: 'Out-of-order config',
        oldPerTokenBoostBps: 0n,
        newPerTokenBoostBps: 600n,
        timestamp: 1010n,
        ...eventData(101, 1010),
      }),
    ],
    [
      'EditionEnabledUpdated',
      TestHelpers.SpecialEditionRegistry.EditionEnabledUpdated.createMockEvent({
        editionId: 12n,
        oldEnabled: true,
        newEnabled: false,
        timestamp: 1020n,
        ...eventData(102, 1020),
      }),
    ],
  ] as const;

  const results = await Promise.allSettled(
    events.map(async ([eventName, event]) => {
      const handler = await getRegisteredSpecialEditionHandler(eventName);
      await handler({
        event,
        context: createPreloadContext(TestHelpers.MockDb.createMockDb()),
      });
    })
  );

  assert.deepEqual(
    results.map(result => result.status),
    ['fulfilled', 'fulfilled', 'fulfilled']
  );
});

test('special-edition registry lifecycle records sorted config history and audit-only events', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = await createEdition(mockDb, 2n, 1, 0n, 100);
  mockDb = await createEdition(mockDb, 1n, 2, 90n, 101);
  mockDb = await createEdition(mockDb, 1n, 9, 90n, 101, 500n, false);

  const configured = TestHelpers.SpecialEditionRegistry.EditionConfigured.createMockEvent({
    editionId: 2n,
    name: 'Edition Two Updated',
    oldPerTokenBoostBps: 500n,
    newPerTokenBoostBps: 700n,
    timestamp: 110n,
    ...eventData(3, 110),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.EditionConfigured.processEvent({
    event: configured,
    mockDb,
  });

  const disabled = TestHelpers.SpecialEditionRegistry.EditionEnabledUpdated.createMockEvent({
    editionId: 2n,
    oldEnabled: true,
    newEnabled: false,
    timestamp: 120n,
    ...eventData(4, 120),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.EditionEnabledUpdated.processEvent({
    event: disabled,
    mockDb,
  });

  const configuredWhileDisabled =
    TestHelpers.SpecialEditionRegistry.EditionConfigured.createMockEvent({
      editionId: 2n,
      name: 'Edition Two Disabled',
      oldPerTokenBoostBps: 700n,
      newPerTokenBoostBps: 800n,
      timestamp: 125n,
      ...eventData(10, 125),
    });
  mockDb = await TestHelpers.SpecialEditionRegistry.EditionConfigured.processEvent({
    event: configuredWhileDisabled,
    mockDb,
  });

  const enabledAgain = TestHelpers.SpecialEditionRegistry.EditionEnabledUpdated.createMockEvent({
    editionId: 2n,
    oldEnabled: false,
    newEnabled: true,
    timestamp: 126n,
    ...eventData(11, 126),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.EditionEnabledUpdated.processEvent({
    event: enabledAgain,
    mockDb,
  });

  const replayedConfig = TestHelpers.SpecialEditionRegistry.EditionConfigured.createMockEvent({
    editionId: 3n,
    name: 'Replay Config',
    oldPerTokenBoostBps: 0n,
    newPerTokenBoostBps: 300n,
    timestamp: 130n,
    ...eventData(5, 130),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.EditionConfigured.processEvent({
    event: replayedConfig,
    mockDb,
  });

  const replayedEnabled = TestHelpers.SpecialEditionRegistry.EditionEnabledUpdated.createMockEvent({
    editionId: 4n,
    oldEnabled: true,
    newEnabled: false,
    timestamp: 140n,
    ...eventData(6, 140),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.EditionEnabledUpdated.processEvent({
    event: replayedEnabled,
    mockDb,
  });

  assert.deepEqual(mockDb.entities.SpecialEditionRegistryState.get('current')?.editionIds, [
    1n,
    2n,
    3n,
    4n,
  ]);
  const editionTwo = mockDb.entities.SpecialEditionConfig.get('2');
  assert.equal(editionTwo?.createdAt, 100);
  assert.equal(editionTwo?.name, 'Edition Two Disabled');
  assert.equal(editionTwo?.perTokenBoostBps, 800n);
  assert.equal(editionTwo?.enabled, true);
  assert.deepEqual(editionTwo?.changeTimestamps, [100, 110, 120, 125, 126]);
  assert.deepEqual(editionTwo?.boostBpsHistory, [500n, 700n, 700n, 800n, 800n]);
  assert.deepEqual(editionTwo?.enabledHistory, [1n, 1n, 0n, 0n, 1n]);

  const replayThree = mockDb.entities.SpecialEditionConfig.get('3');
  assert.equal(replayThree?.key, '');
  assert.equal(replayThree?.enabled, true);
  assert.deepEqual(replayThree?.enabledHistory, [1n]);
  const replayFour = mockDb.entities.SpecialEditionConfig.get('4');
  assert.equal(replayFour?.key, '');
  assert.equal(replayFour?.name, '');
  assert.equal(replayFour?.perTokenBoostBps, 0n);
  assert.equal(replayFour?.enabled, false);

  const reasons = mockDb.entities.SpecialEditionConfigSnapshot.getAll().map(
    (row: EntityRow) => row.changeReason
  );
  assert.deepEqual(reasons, [
    'EDITION_CREATED',
    'EDITION_CREATED',
    'EDITION_CREATED',
    'EDITION_CONFIGURED',
    'EDITION_ENABLED_UPDATED',
    'EDITION_CONFIGURED',
    'EDITION_ENABLED_UPDATED',
    'EDITION_CONFIGURED',
    'EDITION_ENABLED_UPDATED',
  ]);
  assert.equal(mockDb.entities.SpecialEditionConfigSnapshot.getAll()[0]?.timestamp, 100);

  const stateBeforeAudit = mockDb.entities.SpecialEditionRegistryState.get('current');
  const configCountBeforeAudit = mockDb.entities.SpecialEditionConfig.getAll().length;
  const transactionCountBeforeAudit = mockDb.entities.ProtocolStats.get('1')?.totalTransactions;
  const batch = TestHelpers.SpecialEditionRegistry.SpecialEditionRegistrationBatch.createMockEvent({
    editionId: 2n,
    requestedCount: 10n,
    registeredCount: 9n,
    timestamp: 150n,
    ...eventData(7, 150),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.SpecialEditionRegistrationBatch.processEvent({
    event: batch,
    mockDb,
  });
  const publisher = TestHelpers.SpecialEditionRegistry.PublisherUpdated.createMockEvent({
    oldPublisher: ZERO_ADDRESS,
    newPublisher: PUBLISHER,
    ...eventData(8, 160),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.PublisherUpdated.processEvent({
    event: publisher,
    mockDb,
  });

  assert.deepEqual(mockDb.entities.SpecialEditionRegistryState.get('current'), stateBeforeAudit);
  assert.equal(mockDb.entities.SpecialEditionConfig.getAll().length, configCountBeforeAudit);
  assert.equal(
    mockDb.entities.ProtocolStats.get('1')?.totalTransactions,
    (transactionCountBeforeAudit ?? 0n) + 2n
  );
});

test('special-edition membership is idempotent, correctable, and follows active token ownership', async () => {
  let mockDb = TestHelpers.MockDb.createMockDb();
  mockDb = await createEdition(mockDb, 1n, 20, 200n, 200);
  mockDb = mockDb.entities.DustLockToken.set({
    id: '11',
    owner: ALICE,
    lockedAmount: 100n,
    end: 0,
    isPermanent: true,
    createdAt: 190,
    updatedAt: 190,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });

  const registered = TestHelpers.SpecialEditionRegistry.SpecialEditionRegistered.createMockEvent({
    tokenId: 11n,
    editionId: 1n,
    sourceHash: `0x${'11'.padStart(64, '0')}`,
    tokenEditionBitmap: 2n,
    editionTokenCount: 1n,
    timestamp: 210n,
    ...eventData(21, 210),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.SpecialEditionRegistered.processEvent({
    event: registered,
    mockDb,
  });

  assert.equal(mockDb.entities.SpecialEditionTokenState.get('11')?.editionBitmap, 2n);
  assert.deepEqual(mockDb.entities.SpecialEditionTokenState.get('11')?.editionIds, [1n]);
  assert.equal(mockDb.entities.SpecialEditionTokenMembership.get('11:1')?.active, true);
  assert.equal(mockDb.entities.UserSpecialEditionState.get(`${ALICE}:1`)?.tokenCount, 1n);
  assert.equal(
    mockDb.entities.UserSpecialEditionAggregate.get(ALICE)?.specialEditionMultiplier,
    10500n
  );
  assert.equal(mockDb.entities.UserLeaderboardState.get(ALICE)?.combinedMultiplier, 10500n);
  assert.equal(mockDb.entities.UserSpecialEditionHistory.getAll().length, 1);

  mockDb = await TestHelpers.SpecialEditionRegistry.SpecialEditionRegistered.processEvent({
    event: registered,
    mockDb,
  });
  assert.equal(mockDb.entities.UserSpecialEditionHistory.getAll().length, 1);
  assert.deepEqual(mockDb.entities.UserSpecialEditionState.get(`${ALICE}:1`)?.tokenCountHistory, [
    0n,
    1n,
  ]);

  const correctedOff = TestHelpers.SpecialEditionRegistry.MembershipCorrected.createMockEvent({
    tokenId: 11n,
    editionId: 1n,
    oldMember: true,
    newMember: false,
    sourceHash: `0x${'12'.padStart(64, '0')}`,
    reason: 'REMOVE',
    timestamp: 220n,
    ...eventData(22, 220),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.MembershipCorrected.processEvent({
    event: correctedOff,
    mockDb,
  });
  assert.equal(mockDb.entities.SpecialEditionTokenState.get('11')?.editionBitmap, 0n);
  assert.deepEqual(mockDb.entities.SpecialEditionTokenState.get('11')?.editionIds, []);
  assert.equal(mockDb.entities.SpecialEditionTokenMembership.get('11:1')?.registeredAt, 210);
  assert.equal(mockDb.entities.SpecialEditionTokenMembership.get('11:1')?.correctedAt, 220);
  assert.equal(mockDb.entities.UserSpecialEditionState.get(`${ALICE}:1`)?.tokenCount, 0n);

  const correctedOn = TestHelpers.SpecialEditionRegistry.MembershipCorrected.createMockEvent({
    tokenId: 11n,
    editionId: 1n,
    oldMember: false,
    newMember: true,
    sourceHash: `0x${'13'.padStart(64, '0')}`,
    reason: 'RESTORE',
    timestamp: 230n,
    ...eventData(23, 230),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.MembershipCorrected.processEvent({
    event: correctedOn,
    mockDb,
  });
  assert.equal(mockDb.entities.SpecialEditionTokenState.get('11')?.editionBitmap, 2n);
  assert.equal(mockDb.entities.UserSpecialEditionState.get(`${ALICE}:1`)?.tokenCount, 1n);
  assert.deepEqual(
    mockDb.entities.UserSpecialEditionHistory.getAll().map((row: EntityRow) => row.changeReason),
    ['REGISTERED', 'CORRECTION:REMOVE', 'CORRECTION:RESTORE']
  );

  mockDb = mockDb.entities.SpecialEditionTokenState.set({
    id: '11',
    tokenId: 11n,
    editionBitmap: 6n,
    editionIds: [1n, 2n],
    updatedAt: 230,
  });
  mockDb = mockDb.entities.SpecialEditionTokenMembership.set({
    id: '11:2',
    tokenId: 11n,
    editionId: 2n,
    active: false,
    sourceHash: `0x${'14'.padStart(64, '0')}`,
    registeredAt: 230,
    correctedAt: 230,
    txHash: `0x${'14'.padStart(64, '0')}`,
    logIndex: 24,
  });

  for (const [counter, from, to] of [
    [24, ALICE, BOB],
    [25, BOB, ZERO_ADDRESS],
    [26, ZERO_ADDRESS, ALICE],
  ] as const) {
    const transfer = TestHelpers.DustLock.Transfer.createMockEvent({
      from,
      to,
      tokenId: 11n,
      ...eventData(counter, 230 + counter, DUST_LOCK),
    });
    mockDb = await TestHelpers.DustLock.Transfer.processEvent({ event: transfer, mockDb });
  }

  assert.equal(mockDb.entities.UserSpecialEditionState.get(`${ALICE}:1`)?.tokenCount, 1n);
  assert.equal(mockDb.entities.UserSpecialEditionState.get(`${BOB}:1`)?.tokenCount, 0n);
  assert.equal(mockDb.entities.UserSpecialEditionState.get(`${ALICE}:2`), undefined);
  assert.equal(mockDb.entities.UserSpecialEditionState.get(`${BOB}:2`), undefined);
  const transferReasons = mockDb.entities.UserSpecialEditionHistory.getAll()
    .map((row: EntityRow) => row.changeReason)
    .filter((reason: string) => reason.startsWith('SPECIAL_EDITION_TRANSFER'));
  assert.deepEqual(transferReasons, [
    'SPECIAL_EDITION_TRANSFER_OUT',
    'SPECIAL_EDITION_TRANSFER_IN',
    'SPECIAL_EDITION_TRANSFER_OUT',
    'SPECIAL_EDITION_TRANSFER_IN',
  ]);

  const absentOwner = TestHelpers.SpecialEditionRegistry.SpecialEditionRegistered.createMockEvent({
    tokenId: 99n,
    editionId: 1n,
    sourceHash: `0x${'15'.padStart(64, '0')}`,
    tokenEditionBitmap: 2n,
    editionTokenCount: 2n,
    timestamp: 300n,
    ...eventData(30, 300),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.SpecialEditionRegistered.processEvent({
    event: absentOwner,
    mockDb,
  });
  assert.equal(mockDb.entities.SpecialEditionTokenMembership.get('99:1')?.active, true);
  assert.equal(mockDb.entities.SpecialEditionTokenState.get('99')?.editionBitmap, 2n);
  assert.equal(
    mockDb.entities.UserSpecialEditionHistory.getAll().some(
      (row: EntityRow) => row.txHash === absentOwner.transaction.hash
    ),
    false
  );

  mockDb = mockDb.entities.DustLockToken.set({
    id: '12',
    owner: ALICE,
    lockedAmount: 1n,
    end: 0,
    isPermanent: true,
    createdAt: 300,
    updatedAt: 300,
    lastDepositType: undefined,
    selfRepayEnabled: false,
    rewardReceiver: undefined,
  });
  mockDb = mockDb.entities.SpecialEditionTokenState.set({
    id: '12',
    tokenId: 12n,
    editionBitmap: 2n,
    editionIds: [1n],
    updatedAt: 300,
  });
  mockDb = mockDb.entities.SpecialEditionTokenMembership.set({
    id: '12:1',
    tokenId: 12n,
    editionId: 1n,
    active: false,
    sourceHash: `0x${'16'.padStart(64, '0')}`,
    registeredAt: 290,
    correctedAt: 295,
    txHash: `0x${'16'.padStart(64, '0')}`,
    logIndex: 31,
  });
  const recovered = TestHelpers.SpecialEditionRegistry.SpecialEditionRegistered.createMockEvent({
    tokenId: 12n,
    editionId: 1n,
    sourceHash: `0x${'17'.padStart(64, '0')}`,
    tokenEditionBitmap: 2n,
    editionTokenCount: 3n,
    timestamp: 310n,
    ...eventData(31, 310),
  });
  mockDb = await TestHelpers.SpecialEditionRegistry.SpecialEditionRegistered.processEvent({
    event: recovered,
    mockDb,
  });
  assert.deepEqual(mockDb.entities.SpecialEditionTokenState.get('12')?.editionIds, [1n]);
  assert.equal(mockDb.entities.SpecialEditionTokenMembership.get('12:1')?.correctedAt, 295);
});
