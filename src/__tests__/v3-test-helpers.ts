// Envio V3 does not generate the old TestHelpers package. These tests still
// need direct handler invocation semantics, so this compatibility layer reads
// the callbacks registered by indexer.onEvent/contractRegister.
/* eslint-disable @typescript-eslint/no-explicit-any */
// @ts-expect-error Internal Envio registry used only by the test adapter.
import * as HandlerRegister from 'envio/src/HandlerRegister.res.mjs';
// @ts-expect-error Internal Envio config used only to flush lazy registrations.
import * as Config from 'envio/src/Config.res.mjs';

import '../handlers/config';
import '../handlers/dustlock';
import '../handlers/leaderboard';
import '../handlers/leaderboardKeeper';
import '../handlers/lp';
import '../handlers/nft';
import '../handlers/pool';
import '../handlers/profileShop';
import '../handlers/rewards';
import '../handlers/specialEditions';
import '../handlers/tokenization';

// The leaderboard backfill gate (ENVIO_LEADERBOARD_LIVE_EPOCH) is an operator-only
// setting for live backfills; force it off here so a populated .env can never gate
// mid-epoch keeper settlements during tests. Gate-specific tests set it explicitly.
process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = '';

const CHAIN_ID = 143;

type AnyEntity = { readonly id: string } & Record<string, any>;
type EntityStore = Map<string, AnyEntity>;

type EntityOps = {
  get: (id: string) => any;
  getAll: () => any[];
  set: (entity: any) => MockDb;
  deleteUnsafe: (id: string) => MockDb;
};

export type MockDb = {
  readonly __stores: Map<string, EntityStore>;
  readonly __registeredAddresses: Array<{ contract: string; address: string }>;
  readonly entities: Record<string, EntityOps>;
};

type MockEventData = {
  block?: { number?: number; timestamp?: number; hash?: string };
  transaction?: { hash?: string; from?: string; to?: string };
  logIndex?: number;
  srcAddress?: string;
};

type MockEvent = {
  contractName: string;
  eventName: string;
  chainId: number;
  params: Record<string, unknown>;
  block: { number: number; timestamp: number; hash?: string };
  transaction: { hash: string; from?: string; to?: string };
  logIndex: number;
  srcAddress?: string;
};

type ProcessArgs = {
  event: MockEvent;
  mockDb: MockDb;
};

function getStore(mockDb: MockDb, entityName: string): EntityStore {
  let store = mockDb.__stores.get(entityName);
  if (!store) {
    store = new Map();
    mockDb.__stores.set(entityName, store);
  }
  return store;
}

function createEntityOps(mockDb: MockDb, entityName: string): EntityOps {
  return {
    get(id: string) {
      return getStore(mockDb, entityName).get(id);
    },
    getAll() {
      return Array.from(getStore(mockDb, entityName).values());
    },
    set(entity: AnyEntity) {
      getStore(mockDb, entityName).set(entity.id, entity);
      return mockDb;
    },
    deleteUnsafe(id: string) {
      getStore(mockDb, entityName).delete(id);
      return mockDb;
    },
  };
}

function createMockDb(): MockDb {
  const mockDb = {
    __stores: new Map<string, EntityStore>(),
    __registeredAddresses: [],
    entities: {} as Record<string, EntityOps>,
  } satisfies MockDb;

  mockDb.entities = new Proxy({} as Record<string, EntityOps>, {
    get(target, property: string) {
      if (!target[property]) {
        target[property] = createEntityOps(mockDb, property);
      }
      return target[property];
    },
  }) as Record<string, EntityOps>;

  return mockDb;
}

let registryInitialized = false;

function ensureRegistryInitialized(): void {
  if (registryInitialized) return;
  HandlerRegister.startRegistration(Config.loadWithoutRegistrations().ecosystem);
  HandlerRegister.finishRegistration();
  registryInitialized = true;
}

function normalizeEventData(
  input?: { mockEventData?: MockEventData } | MockEventData
): MockEventData {
  if (!input) return {};
  if ('mockEventData' in input && input.mockEventData) return input.mockEventData;
  return input as MockEventData;
}

function createMockEvent(contractName: string, eventName: string) {
  return (
    params: Record<string, unknown>,
    input?: { mockEventData?: MockEventData } | MockEventData
  ): MockEvent => {
    const eventParams = { ...params };
    const inlineInput =
      'mockEventData' in eventParams && eventParams.mockEventData
        ? { mockEventData: eventParams.mockEventData as MockEventData }
        : undefined;
    delete eventParams.mockEventData;

    const eventData = normalizeEventData(input ?? inlineInput);
    const blockNumber = eventData.block?.number ?? 1;
    const logIndex = eventData.logIndex ?? 0;
    const txHash = eventData.transaction?.hash ?? `0x${blockNumber.toString(16).padStart(64, '0')}`;

    return {
      contractName,
      eventName,
      chainId: CHAIN_ID,
      params: eventParams,
      block: {
        number: blockNumber,
        timestamp: eventData.block?.timestamp ?? blockNumber,
        hash: eventData.block?.hash,
      },
      transaction: {
        hash: txHash,
        from: eventData.transaction?.from,
        to: eventData.transaction?.to,
      },
      logIndex,
      srcAddress: eventData.srcAddress,
    };
  };
}

function matchFilterValue(actual: unknown, expected: unknown): boolean {
  if (expected && typeof expected === 'object' && !Array.isArray(expected)) {
    const filter = expected as Record<string, unknown>;
    if ('_eq' in filter) return actual === filter._eq;
    if ('_neq' in filter) return actual !== filter._neq;
    if ('_in' in filter && Array.isArray(filter._in)) return filter._in.includes(actual);
    if ('_nin' in filter && Array.isArray(filter._nin)) return !filter._nin.includes(actual);
  }
  return actual === expected;
}

function matchesFilter(entity: AnyEntity, filter: Record<string, unknown>): boolean {
  return Object.entries(filter).every(([field, expected]) =>
    matchFilterValue(entity[field], expected)
  );
}

function createContextEntityOps(mockDb: MockDb, entityName: string) {
  return {
    async get(id: string) {
      return getStore(mockDb, entityName).get(id);
    },
    async getOrThrow(id: string, message?: string) {
      const entity = getStore(mockDb, entityName).get(id);
      if (!entity) throw new Error(message ?? `${entityName} ${id} not found`);
      return entity;
    },
    async getWhere(filter: Record<string, unknown>) {
      return Array.from(getStore(mockDb, entityName).values()).filter(entity =>
        matchesFilter(entity, filter)
      );
    },
    async getOrCreate(entity: AnyEntity) {
      const existing = getStore(mockDb, entityName).get(entity.id);
      if (existing) return existing;
      getStore(mockDb, entityName).set(entity.id, entity);
      return entity;
    },
    set(entity: AnyEntity) {
      getStore(mockDb, entityName).set(entity.id, entity);
    },
    deleteUnsafe(id: string) {
      getStore(mockDb, entityName).delete(id);
    },
  };
}

const testLogger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
  trace() {},
  fatal() {},
};

function createHandlerContext(mockDb: MockDb) {
  const entityOps = new Map<string, ReturnType<typeof createContextEntityOps>>();

  return new Proxy(
    {
      log: testLogger,
      isPreload: false,
    },
    {
      get(target, property: string) {
        if (property in target) return target[property as keyof typeof target];
        if (!entityOps.has(property)) {
          entityOps.set(property, createContextEntityOps(mockDb, property));
        }
        return entityOps.get(property);
      },
    }
  );
}

function createContractRegisterContext(mockDb: MockDb) {
  return {
    log: testLogger,
    chain: new Proxy(
      { id: CHAIN_ID },
      {
        get(target, property: string) {
          if (property in target) return target[property as keyof typeof target];
          return {
            add(address: string) {
              mockDb.__registeredAddresses.push({ contract: property, address });
            },
          };
        },
      }
    ),
  };
}

async function processEvent({ event, mockDb }: ProcessArgs): Promise<MockDb> {
  ensureRegistryInitialized();

  const contractRegister = HandlerRegister.getContractRegister(event.contractName, event.eventName);
  if (contractRegister) {
    await contractRegister({
      event,
      context: createContractRegisterContext(mockDb),
    });
  }

  const handler = HandlerRegister.getHandler(event.contractName, event.eventName);
  if (!handler) {
    throw new Error(`No handler registered for ${event.contractName}.${event.eventName}`);
  }
  await handler({
    event,
    context: createHandlerContext(mockDb),
  });

  return mockDb;
}

function createContractHelpers(contractName: string) {
  return new Proxy(
    {},
    {
      get(_target, eventName: string) {
        return {
          createMockEvent: createMockEvent(contractName, eventName),
          processEvent,
        };
      },
    }
  );
}

export const TestHelpers = new Proxy(
  {
    MockDb: { createMockDb },
  } as Record<string, unknown>,
  {
    get(target, property: string) {
      if (property in target) return target[property];
      const helpers = createContractHelpers(property);
      target[property] = helpers;
      return helpers;
    },
  }
) as {
  MockDb: { createMockDb: () => MockDb };
} & Record<
  string,
  Record<
    string,
    { createMockEvent: ReturnType<typeof createMockEvent>; processEvent: typeof processEvent }
  >
>;
