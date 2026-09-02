// Envio V2 (envio ^2.32) DOES generate a native TestHelpers package (unlike V3).
// This module is the single compatibility seam: it loads the native generated
// TestHelpers at runtime and re-exports it under the same name the test suite
// already imports (`import { TestHelpers } from './v3-test-helpers'`), so the
// individual test files need no per-file TestHelpers changes.
//
// The native TestHelpers API used by the tests:
//   TestHelpers.MockDb.createMockDb()
//   TestHelpers.<Contract>.<Event>.createMockEvent({ ...params, mockEventData })
//   TestHelpers.<Contract>.<Event>.processEvent({ event, mockDb }) -> Promise<MockDb>
//   mockDb.entities.<Entity>.get(id) / .set(entity) / .getAll()
//
// Everything here loads lazily through `require`. A static `import ... from
// '../../generated'` would be emitted as a top-of-file require and run BEFORE
// the dist-test/generated symlink below exists.
//
// Routing note: v2 binds the handler explicitly per `processEvent` call
// (`mockEventRegisters.set(event, register)`), never by `srcAddress`. Synthetic
// addresses therefore reach their handlers without any wildcard shim.
/* eslint-disable @typescript-eslint/no-explicit-any */
import fs from 'node:fs';
import path from 'node:path';

// Operator-only backfill gate (ENVIO_LEADERBOARD_LIVE_EPOCH) must be OFF during
// tests so a populated .env can never gate mid-epoch keeper settlements. The
// gate-specific tests set it explicitly.
process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = '';

// Operator settings must never leak into tests. `src/__tests__/test-env-preload.ts` is the
// authoritative copy and runs for the whole suite; these lines repeat it for a direct
// single-file invocation. Assign, never delete: dotenv repopulates absent keys.
process.env.PREFILL_HISTORIC_EPOCHS = 'false';
process.env.PREFILL_DATA_DIR = '__NEVERLAND_TEST_PREFILL_DIR_MUST_BE_OVERRIDDEN__';
process.env.NEVERLAND_TEST_ENV = '1';

const CHAIN_START_BLOCK = 32_587_107;

// Handler modules whose `Contract.Event.handler(...)` registrations must run
// before any processEvent call. They are required (post-symlink) inside the
// loader so their `'../../generated'` imports resolve against the symlink.
const HANDLER_MODULES = [
  'config',
  'dustlock',
  'leaderboard',
  'leaderboardKeeper',
  'lp',
  'nft',
  'pool',
  'profileShop',
  'rewards',
  'specialEditions',
  'tokenization',
];

function loadNativeTestHelpers(): any {
  const cwd = process.cwd();
  const distTestRoot = path.join(cwd, 'dist-test');
  const generatedLink = path.join(distTestRoot, 'generated');

  // Compiled handler/test JS imports `'../../generated'`, which resolves to
  // dist-test/generated — symlink it to the real ./generated before requiring.
  if (!fs.existsSync(path.join(generatedLink, 'index.js'))) {
    if (fs.existsSync(generatedLink)) {
      fs.rmSync(generatedLink, { recursive: true, force: true });
    }
    fs.symlinkSync(path.join(cwd, 'generated'), generatedLink, 'dir');
  }

  for (const handler of HANDLER_MODULES) {
    require(path.join(distTestRoot, 'src', 'handlers', `${handler}.js`));
  }

  return require(path.join(cwd, 'generated', 'src', 'TestHelpers.res.js'));
}

export const TestHelpers: any = loadNativeTestHelpers();

// Loose alias preserved for the few tests that annotate `: MockDb`. The native
// MockDb is structurally a proxy with `.entities.<Entity>.{get,set,getAll}` and
// is threaded immutably through processEvent.
export type MockDb = any;

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

export type ProcessEventsResult = {
  mockDb: MockDb;
  changes: readonly Record<string, any>[];
};

/**
 * Batch form. v2's MockDb processes a list natively and returns only the next db, so the
 * per-event change records the suite sums for `eventsProcessed` are synthesized here: every
 * event in the list is processed, one record each.
 */
export async function processEvents({
  events,
  mockDb,
}: {
  events: readonly any[];
  mockDb: MockDb;
}): Promise<ProcessEventsResult> {
  const next = await (mockDb as any).processEvents(events);
  return { mockDb: next, changes: events.map(() => ({ eventsProcessed: 1 })) };
}

/**
 * Look a handler up by contract/event so a test can drive it with its own context.
 *
 * v2 stores each handler on the event's own register the moment the handler module calls
 * `Contract.Event.handler(...)`, and `loadNativeTestHelpers()` above has already loaded the
 * compiled handler modules, so the function is read straight from `Types`. Do NOT route this
 * through `Generated.registerAllHandlers()`: that re-requires every handler from its
 * `config.yaml` path (`src/handlers/*.ts`), which Node 22.18's native type stripping loads under
 * ESM resolution rules, and the handlers' `import ... from '../../generated'` directory import
 * then throws ERR_UNSUPPORTED_DIR_IMPORT. Production never hits this because `envio start`
 * runs under ts-node.
 */
export async function getRegisteredEventHandler(contractName: string, eventName: string) {
  const types = require(path.join(process.cwd(), 'generated', 'src', 'Types.res.js'));
  const handler = types?.[contractName]?.[eventName]?.handlerRegister?.handler;
  if (typeof handler !== 'function') {
    throw new Error(`missing registered handler for ${contractName}.${eventName}`);
  }
  return handler as (args: { event: any; context: any }) => Promise<void>;
}

/**
 * The `contractRegister` callback for an event, from the same per-event register. Dynamic
 * registration never runs through `processEvent`, so a test that wants to observe what a
 * registration event would add to the indexer drives this directly with its own context.
 */
export async function getRegisteredContractRegister(contractName: string, eventName: string) {
  const types = require(path.join(process.cwd(), 'generated', 'src', 'Types.res.js'));
  const register = types?.[contractName]?.[eventName]?.handlerRegister?.contractRegister;
  if (typeof register !== 'function') {
    throw new Error(`missing contractRegister for ${contractName}.${eventName}`);
  }
  return register as (args: { event: any; context: any }) => Promise<void> | void;
}

export type EntityRow = { readonly id: string } & Record<string, any>;

/**
 * Snapshot of every entity store as `Map<entityName, Map<id, row>>`.
 *
 * v2's MockDb exposes per-entity `get`/`getAll`/`set` rather than a raw store map, so tests
 * that used to read the seam's internal `__stores` build the same shape from `getAll()`.
 */
export function entityStores(mockDb: MockDb): Map<string, Map<string, EntityRow>> {
  const out = new Map<string, Map<string, EntityRow>>();
  const entities = (mockDb as any).entities ?? {};
  for (const name of Object.keys(entities)) {
    const ops = entities[name];
    if (!ops || typeof ops.getAll !== 'function') continue;
    const rows = new Map<string, EntityRow>();
    for (const row of ops.getAll() as EntityRow[]) rows.set(row.id, row);
    out.set(name, rows);
  }
  return out;
}
