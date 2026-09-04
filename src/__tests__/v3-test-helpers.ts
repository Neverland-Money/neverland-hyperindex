/**
 * Compatibility seam between the suite and envio v3's test API.
 *
 * v3 replaced the immutable `MockDb` with a mutable `createTestIndexer()` whose reads are
 * async. The suite is written against the v2 shape (`createMockEvent` -> `processEvent`,
 * threading a db that is copied on every `set`), so rather than rewrite ~630 call sites this
 * module reimplements that shape on top of v3:
 *
 *   - a `MockDb` is a plain snapshot (`entityName -> id -> row`); `set`/`delete` copy it, so
 *     the immutable threading the tests rely on still holds;
 *   - `processEvent` builds a fresh test indexer, seeds it from the snapshot, simulates the
 *     one event, and reads the result back into a new snapshot. Reads stay synchronous
 *     because they hit the snapshot, not the indexer.
 *
 * Handlers register themselves by side effect when their module is first imported, and
 * `handlers/registry.ts` records each registration so a handler can be looked up by contract
 * and event.
 */
// Mirrors `test-env-preload.ts`: a test file that imports this seam but is run without the
// runner `--import` preload must still get the safe operator settings, because the handler
// modules imported below pull in `helpers/prefill.ts`. Assigned, never deleted, so envio's
// dotenv import cannot repopulate them from the repo `.env`.
process.env.PREFILL_HISTORIC_EPOCHS ??= 'false';
// Must match TEST_PREFILL_DIR_SENTINEL in src/helpers/prefill.ts.
process.env.PREFILL_DATA_DIR ??= '__NEVERLAND_TEST_PREFILL_DIR_MUST_BE_OVERRIDDEN__';
process.env.NEVERLAND_TEST_ENV ??= '1';

import fs from 'node:fs';
import path from 'node:path';

import { lookupContractRegister, lookupHandler } from '../handlers/registry';

// Side-effect imports: each module's top-level `indexer.onEvent(...)` calls run on first
// import, and `registry.ts` records them. Without these the lookup table is empty.
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

export const CHAIN_ID = 143;
const DEFAULT_ADDRESS = '0x1111111111111111111111111111111111111111';
const CHAIN_START_BLOCK = 32587107;

export type EntityRow = { readonly id: string } & Record<string, any>;
type Snapshot = Map<string, Map<string, EntityRow>>;

// ---------------------------------------------------------------------------
// Handler registry
// ---------------------------------------------------------------------------

type AnyHandler = (args: { event: any; context: any }) => Promise<void> | void;
// Every `indexer.onEvent` handler in this codebase is async, so the lookup narrows the return
// to a promise; `assert.rejects` on a handler call needs a thenable, not `void | Promise`.
type EventHandler = (args: { event: any; context: any }) => Promise<void>;

/** Look a handler up by contract/event so a test can drive it with its own context. */
export async function getRegisteredEventHandler(contractName: string, eventName: string) {
  const handler = lookupHandler(contractName, eventName);
  if (!handler) throw new Error(`missing registered handler for ${contractName}.${eventName}`);
  return handler as EventHandler;
}

/** The `contractRegister` callback for an event, for tests that observe dynamic registration. */
export async function getRegisteredContractRegister(contractName: string, eventName: string) {
  const register = lookupContractRegister(contractName, eventName);
  if (!register) throw new Error(`missing contractRegister for ${contractName}.${eventName}`);
  return register as AnyHandler;
}

// ---------------------------------------------------------------------------
// Block numbers
// ---------------------------------------------------------------------------

function loadContractStartBlocks(): Map<string, number> {
  const starts = new Map<string, number>();
  const config = fs.readFileSync(path.join(process.cwd(), 'config.yaml'), 'utf8');
  let currentContract: string | undefined;
  for (const line of config.split('\n')) {
    const contract = line.match(/^ {6}- name:\s*(\S+)\s*$/);
    if (contract) {
      currentContract = contract[1];
      starts.set(currentContract, CHAIN_START_BLOCK);
      continue;
    }
    const startBlock = line.match(/^ {8}start_block:\s*(\d+)\s*$/);
    if (currentContract && startBlock) starts.set(currentContract, Number(startBlock[1]));
  }
  return starts;
}

const CONTRACT_START_BLOCKS = loadContractStartBlocks();

// ---------------------------------------------------------------------------
// Event parameter defaults
// ---------------------------------------------------------------------------
//
// v2's `createMockEvent` filled every ABI parameter a case did not mention, so handlers could
// read fields the case had no opinion about. Cases rely on that: the aToken ones set only the
// metadata fields and still reach a handler that reads `params.treasury`. The signatures in
// config.yaml carry the parameter names and types, which is enough to rebuild those defaults.

const ZERO_ADDR = '0x0000000000000000000000000000000000000000';

function defaultForType(type: string): unknown {
  if (type.endsWith(']')) return [];
  if (type === 'address') return ZERO_ADDR;
  if (type === 'bool') return false;
  if (type === 'string') return '';
  if (type.startsWith('bytes')) return '0x';
  if (type.startsWith('uint') || type.startsWith('int')) return 0n;
  return undefined;
}

function loadEventParamDefaults(): Map<string, Record<string, unknown>> {
  const out = new Map<string, Record<string, unknown>>();
  const config = fs.readFileSync(path.join(process.cwd(), 'config.yaml'), 'utf8');
  let currentContract: string | undefined;
  for (const line of config.split('\n')) {
    const contract = line.match(/^ {6}- name:\s*(\S+)\s*$/);
    if (contract) {
      currentContract = contract[1];
      continue;
    }
    const event = line.match(/^\s+- event:\s*([A-Za-z0-9_]+)\((.*)\)\s*$/);
    if (!event || !currentContract) continue;
    const defaults: Record<string, unknown> = {};
    for (const raw of event[2].split(',')) {
      const parts = raw
        .trim()
        .split(/\s+/)
        .filter(w => w !== 'indexed');
      if (parts.length < 2) continue;
      const value = defaultForType(parts[0]);
      if (value !== undefined) defaults[parts[parts.length - 1]] = value;
    }
    out.set(`${currentContract}.${event[1]}`, defaults);
  }
  return out;
}

const EVENT_PARAM_DEFAULTS = loadEventParamDefaults();

/**
 * An event below its contract's `start_block` is filtered out before reaching the handler, so
 * a small literal block number in a test is rebased above that floor rather than dropped.
 */
export function normalizeTestBlockNumber(
  blockNumber: number | undefined,
  contractName?: string
): number {
  const startBlock = contractName
    ? (CONTRACT_START_BLOCKS.get(contractName) ?? CHAIN_START_BLOCK)
    : CHAIN_START_BLOCK;
  if (blockNumber === undefined) return startBlock;
  return blockNumber < startBlock ? startBlock + blockNumber : blockNumber;
}

// ---------------------------------------------------------------------------
// MockDb
// ---------------------------------------------------------------------------

function copy(snapshot: Snapshot): Snapshot {
  const out: Snapshot = new Map();
  for (const [name, rows] of snapshot) out.set(name, new Map(rows));
  return out;
}

export class MockDb {
  readonly __snapshot: Snapshot;
  readonly entities: Record<string, EntityOps>;

  constructor(snapshot: Snapshot = new Map()) {
    this.__snapshot = snapshot;
    const self = this;
    this.entities = new Proxy({} as Record<string, EntityOps>, {
      get: (_target, prop: string) => new EntityOps(self, prop),
      has: () => true,
      ownKeys: () => [...self.__snapshot.keys()],
      getOwnPropertyDescriptor: () => ({ enumerable: true, configurable: true }),
    });
  }
}

class EntityOps {
  constructor(
    private readonly db: MockDb,
    private readonly name: string
  ) {}

  get(id: string): EntityRow | undefined {
    return this.db.__snapshot.get(this.name)?.get(id);
  }

  getAll(): EntityRow[] {
    return [...(this.db.__snapshot.get(this.name)?.values() ?? [])];
  }

  set(row: EntityRow): MockDb {
    const next = copy(this.db.__snapshot);
    if (!next.has(this.name)) next.set(this.name, new Map());
    next.get(this.name)!.set(row.id, row);
    return new MockDb(next);
  }

  delete(id: string): MockDb {
    const next = copy(this.db.__snapshot);
    next.get(this.name)?.delete(id);
    return new MockDb(next);
  }

  deleteUnsafe(id: string): MockDb {
    return this.delete(id);
  }
}

export type MockEvent = {
  contract: string;
  event: string;
  params: Record<string, unknown>;
  srcAddress?: string;
  logIndex?: number;
  block?: Record<string, unknown>;
  transaction?: Record<string, unknown>;
};

/** Snapshot of every entity store as `Map<entityName, Map<id, row>>`. */
export function entityStores(mockDb: MockDb): Snapshot {
  return copy(mockDb.__snapshot);
}

// ---------------------------------------------------------------------------
// Processing
// ---------------------------------------------------------------------------
//
// Events are dispatched straight to the registered handler with a context built over the
// snapshot, rather than through `indexer.process({ simulate })`.
//
// v3's simulate path routes an event only if its `srcAddress` is an indexed address for that
// contract. The suite predates that rule and uses synthetic addresses (0x...9001) precisely so
// that markets sharing an asset can be told apart, and several cases assert on ids derived
// from those addresses. Routing them through simulate would mean rewriting the fixtures to the
// real deployed addresses and losing the distinctions the cases exist to make, so the seam
// keeps v2's "the handler receives exactly the event you built" contract instead.

type Ctx = Record<string, unknown>;

function makeContext(snapshot: Snapshot, isPreload = false): Ctx {
  const entityOps = (name: string) => ({
    get: async (id: string) => snapshot.get(name)?.get(id),
    getOrThrow: async (id: string, message?: string) => {
      const row = snapshot.get(name)?.get(id);
      if (!row) throw new Error(message ?? `${name} ${id} not found`);
      return row;
    },
    getOrCreate: async (row: EntityRow) => snapshot.get(name)?.get(row.id) ?? row,
    getWhere: async (filter: Record<string, { _eq?: unknown }>) =>
      [...(snapshot.get(name)?.values() ?? [])].filter(row =>
        Object.entries(filter).every(
          ([field, op]) => op?._eq === undefined || (row as any)[field] === op._eq
        )
      ),
    set: (row: EntityRow) => {
      if (isPreload) return; // writes are no-ops in the preload pass, as in production
      if (!snapshot.has(name)) snapshot.set(name, new Map());
      snapshot.get(name)!.set(row.id, row);
    },
    deleteUnsafe: (id: string) => {
      if (isPreload) return;
      snapshot.get(name)?.delete(id);
    },
  });

  const cache = new Map<string, ReturnType<typeof entityOps>>();
  const base: Ctx = {
    isPreload,
    chain: { id: CHAIN_ID, isRealtime: false },
    log: {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: () => {},
    },
  };

  return new Proxy(base, {
    get: (target, prop: string) => {
      if (prop in target) return target[prop];
      let ops = cache.get(prop);
      if (!ops) {
        ops = entityOps(prop);
        cache.set(prop, ops);
      }
      return ops;
    },
    has: () => true,
  });
}

function makeEvent(event: MockEvent) {
  return {
    params: event.params,
    srcAddress: event.srcAddress ?? DEFAULT_ADDRESS,
    logIndex: event.logIndex ?? 0,
    chainId: CHAIN_ID,
    block: { timestamp: 0, hash: '0x', number: 0, ...(event.block ?? {}) },
    transaction: { hash: '0x', from: undefined, ...(event.transaction ?? {}) },
  };
}

/**
 * Context for a `contractRegister` callback: `chain.<Contract>.add(address)` and nothing else.
 * Addresses are collected rather than acted on, since a simulated run has no fetch state to
 * register them against.
 */
function makeRegisterContext(added: string[]) {
  const chain = new Proxy(
    { id: CHAIN_ID },
    {
      get: (target, prop: string) =>
        prop in target
          ? (target as Record<string, unknown>)[prop]
          : { add: (address: string) => added.push(address) },
      has: () => true,
    }
  );
  return { chain, log: { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} } };
}

async function runSimulation(events: readonly MockEvent[], mockDb: MockDb): Promise<MockDb> {
  const snapshot = copy(mockDb.__snapshot);
  const context = makeContext(snapshot);
  const preloadContext = makeContext(snapshot, true);
  for (const raw of events) {
    const handler = lookupHandler(raw.contract, raw.event);
    if (!handler) {
      throw new Error(`no handler registered for ${raw.contract}.${raw.event}`);
    }
    const event = makeEvent(raw);
    // An event with both a registration and a handler runs the registration first, the way
    // the indexer does: the addresses a block discovers are added before its events are
    // processed. Cases that assert on what was registered drive the callback themselves via
    // `getRegisteredContractRegister`, so what it returns here is intentionally discarded.
    const register = lookupContractRegister(raw.contract, raw.event);
    if (register) {
      await register({ event, context: makeRegisterContext([]) });
    }
    // Production runs every handler twice: a concurrent preload pass that only reads (writes
    // are no-ops), then the sequential pass that writes. Cases assert on the result of the
    // second, but the first is what exercises the `isPreload` guards.
    await handler({ event, context: preloadContext });
    await handler({ event, context });
  }
  return new MockDb(snapshot);
}

export type ProcessEventsResult = {
  mockDb: MockDb;
  changes: readonly Record<string, any>[];
};

/** Batch form: every event is simulated in order against one indexer. */
export async function processEvents({
  events,
  mockDb,
}: {
  events: readonly MockEvent[];
  mockDb: MockDb;
}): Promise<ProcessEventsResult> {
  const next = await runSimulation(events, mockDb);
  return { mockDb: next, changes: events.map(() => ({ eventsProcessed: 1 })) };
}

// ---------------------------------------------------------------------------
// TestHelpers facade
// ---------------------------------------------------------------------------

/**
 * `TestHelpers.<Contract>.<Event>.createMockEvent(...)` / `.processEvent(...)`, plus
 * `TestHelpers.MockDb.createMockDb()`, resolved lazily so any contract/event name in
 * `config.yaml` works without enumerating them here.
 */
export const TestHelpers: any = new Proxy(
  {},
  {
    get: (_target, contract: string) => {
      if (contract === 'MockDb') return { createMockDb: () => new MockDb() };
      if (contract === 'Addresses') {
        return {
          defaultAddress: DEFAULT_ADDRESS,
          mockAddresses: Array.from(
            { length: 20 },
            (_, i) => `0x${(i + 1).toString(16).padStart(40, '0')}`
          ),
        };
      }
      return new Proxy(
        {},
        {
          get: (_t, event: string) => ({
            createMockEvent: (args: Record<string, any> = {}): MockEvent => {
              const { mockEventData, ...params } = args;
              const meta = (mockEventData ?? {}) as Record<string, any>;
              return {
                contract,
                event,
                params: { ...(EVENT_PARAM_DEFAULTS.get(`${contract}.${event}`) ?? {}), ...params },
                srcAddress: meta.srcAddress,
                logIndex: meta.logIndex,
                block: meta.block,
                transaction: meta.transaction,
              };
            },
            processEvent: async ({ event, mockDb }: { event: MockEvent; mockDb: MockDb }) =>
              runSimulation([event], mockDb),
          }),
        }
      );
    },
  }
);
