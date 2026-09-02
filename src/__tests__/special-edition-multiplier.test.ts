// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. This file does not import the `v3-test-helpers` seam, so without this
// a bare `node --test` invocation would inherit them from the repo `.env` via envio's
// dotenv. Redundant under `pnpm run test`, which loads the same module via `--import`.
import './test-env-preload';

import assert from 'node:assert/strict';
import { test } from 'node:test';

import { ZERO_ADDRESS } from '../helpers/constants';

import {
  applyUserSpecialEditionDelta,
  calculateAverageSpecialEditionMultiplierBps,
  calculateSpecialEditionMultiplierFromUser,
} from '../handlers/shared';

import type { handlerContext } from '../../generated';

type Row = { id: string } & Record<string, unknown>;

function store(initial: readonly Row[] = []) {
  const rows = new Map(initial.map(row => [row.id, row]));
  return {
    get: async (id: string) => rows.get(id),
    set: (row: Row) => rows.set(row.id, row),
    deleteUnsafe: (id: string) => rows.delete(id),
    rows,
  };
}

function buildContext() {
  const stores = {
    SpecialEditionRegistryState: store(),
    SpecialEditionConfig: store(),
    UserSpecialEditionState: store(),
    UserSpecialEditionAggregate: store(),
    UserLeaderboardState: store(),
    UserMultiplierSnapshot: store(),
    UserSpecialEditionHistory: store(),
    UserTokenList: store(),
    UserTokenOwnership: store(),
  };
  return { context: stores as unknown as handlerContext, stores };
}

function seedEdition(
  stores: ReturnType<typeof buildContext>['stores'],
  options: { enabled?: boolean; exists?: boolean; boost?: bigint } = {}
) {
  stores.SpecialEditionRegistryState.set({
    id: 'current',
    editionIds: [1n],
    lastUpdate: 0,
  });
  stores.SpecialEditionConfig.set({
    id: '1',
    editionId: 1n,
    key: 'SHINY',
    name: 'Shiny',
    perTokenBoostBps: options.boost ?? 500n,
    enabled: options.enabled ?? true,
    exists: options.exists ?? true,
    createdAt: 0,
    updatedAt: 0,
    changeTimestamps: [0],
    boostBpsHistory: [options.boost ?? 500n],
    enabledHistory: [options.enabled === false ? 0n : 1n],
  });
}

test('special-edition multiplier handles enabled, disabled, zero, missing, and absent user state', async () => {
  const enabled = buildContext();
  seedEdition(enabled.stores);
  enabled.stores.UserSpecialEditionState.set({
    id: 'alice:1',
    user_id: 'alice',
    editionId: 1n,
    tokenCount: 2n,
    countTimestamps: [0],
    tokenCountHistory: [2n],
    updatedAt: 0,
  });
  assert.equal(
    await calculateSpecialEditionMultiplierFromUser(enabled.context, 'alice', 1),
    11000n
  );
  assert.equal(await calculateSpecialEditionMultiplierFromUser(enabled.context, 'bob', 1), 10000n);

  const disabled = buildContext();
  seedEdition(disabled.stores, { enabled: false });
  disabled.stores.UserSpecialEditionState.set({
    id: 'alice:1',
    user_id: 'alice',
    editionId: 1n,
    tokenCount: 1n,
    countTimestamps: [0],
    tokenCountHistory: [1n],
    updatedAt: 0,
  });
  assert.equal(
    await calculateSpecialEditionMultiplierFromUser(disabled.context, 'alice', 1),
    10000n
  );

  const zeroBoost = buildContext();
  seedEdition(zeroBoost.stores, { boost: 0n });
  zeroBoost.stores.UserSpecialEditionState.set({
    id: 'alice:1',
    user_id: 'alice',
    editionId: 1n,
    tokenCount: 1n,
    countTimestamps: [0],
    tokenCountHistory: [1n],
    updatedAt: 0,
  });
  assert.equal(
    await calculateSpecialEditionMultiplierFromUser(zeroBoost.context, 'alice', 1),
    10000n
  );

  const nonexistent = buildContext();
  seedEdition(nonexistent.stores, { exists: false });
  assert.equal(
    await calculateSpecialEditionMultiplierFromUser(nonexistent.context, 'alice', 1),
    10000n
  );

  const missingConfig = buildContext();
  missingConfig.stores.SpecialEditionRegistryState.set({
    id: 'current',
    editionIds: [1n],
    lastUpdate: 0,
  });
  assert.equal(
    await calculateSpecialEditionMultiplierFromUser(missingConfig.context, 'alice', 1),
    10000n
  );

  const missingStores = {} as handlerContext;
  assert.equal(await calculateSpecialEditionMultiplierFromUser(missingStores, 'alice', 1), 10000n);
});

test('legacy partial multiplier histories fall back to current indexed values', async () => {
  const historyShapes = [
    { timestamps: undefined, values: [1n] },
    { timestamps: [0], values: undefined },
    { timestamps: [], values: [1n] },
    { timestamps: [0], values: [] },
  ] as const;

  for (const shape of historyShapes) {
    const { context, stores } = buildContext();
    stores.SpecialEditionRegistryState.set({ id: 'current', editionIds: [1n], lastUpdate: 0 });
    stores.SpecialEditionConfig.set({
      id: '1',
      editionId: 1n,
      key: 'LEGACY',
      name: 'Legacy',
      perTokenBoostBps: 500n,
      enabled: true,
      exists: true,
      createdAt: 0,
      updatedAt: 0,
      changeTimestamps: shape.timestamps,
      boostBpsHistory: shape.values,
      enabledHistory: shape.values,
    });
    stores.UserSpecialEditionState.set({
      id: 'alice:1',
      user_id: 'alice',
      editionId: 1n,
      tokenCount: 1n,
      countTimestamps: shape.timestamps,
      tokenCountHistory: shape.values,
      updatedAt: 0,
    });

    assert.equal(await calculateSpecialEditionMultiplierFromUser(context, 'alice', 1), 10500n);
  }

  assert.equal(
    await calculateAverageSpecialEditionMultiplierBps({} as handlerContext, 'alice', 0, 1),
    10000n
  );

  const beforeHistory = buildContext();
  seedEdition(beforeHistory.stores);
  beforeHistory.stores.SpecialEditionConfig.set({
    ...beforeHistory.stores.SpecialEditionConfig.rows.get('1')!,
    changeTimestamps: [10],
    boostBpsHistory: [500n],
    enabledHistory: [1n],
  });
  beforeHistory.stores.UserSpecialEditionState.set({
    id: 'alice:1',
    user_id: 'alice',
    editionId: 1n,
    tokenCount: 1n,
    countTimestamps: [10],
    tokenCountHistory: [1n],
    updatedAt: 10,
  });
  assert.equal(
    await calculateSpecialEditionMultiplierFromUser(beforeHistory.context, 'alice', 5),
    10500n
  );

  const missingConfig = buildContext();
  missingConfig.stores.SpecialEditionRegistryState.set({
    id: 'current',
    editionIds: [1n],
    lastUpdate: 0,
  });
  assert.equal(
    await calculateAverageSpecialEditionMultiplierBps(missingConfig.context, 'alice', 0, 10),
    10000n
  );
});

test('special-edition average follows configuration, count, and ownership boundaries', async () => {
  const { context, stores } = buildContext();
  stores.SpecialEditionRegistryState.set({ id: 'current', editionIds: [1n], lastUpdate: 0 });
  stores.SpecialEditionConfig.set({
    id: '1',
    editionId: 1n,
    key: 'SHINY',
    name: 'Shiny',
    perTokenBoostBps: 200n,
    enabled: false,
    exists: true,
    createdAt: 0,
    updatedAt: 20,
    changeTimestamps: [0, 10, 20],
    boostBpsHistory: [100n, 200n, 200n],
    enabledHistory: [1n, 1n, 0n],
  });
  stores.UserSpecialEditionState.set({
    id: 'alice:1',
    user_id: 'alice',
    editionId: 1n,
    tokenCount: 2n,
    countTimestamps: [0, 15],
    tokenCountHistory: [1n, 2n],
    updatedAt: 15,
  });
  stores.UserTokenList.set({ id: 'alice', user_id: 'alice', tokenIds: [99n], lastUpdate: 12 });
  stores.UserTokenOwnership.set({
    id: 'alice:99',
    user_id: 'alice',
    tokenId: 99n,
    acquiredAt: 12,
    lastUpdate: 12,
  });

  assert.equal(await calculateAverageSpecialEditionMultiplierBps(context, 'alice', 0, 30), 10133n);
  assert.equal(await calculateAverageSpecialEditionMultiplierBps(context, 'alice', 15, 15), 10400n);
  assert.equal(await calculateAverageSpecialEditionMultiplierBps(context, 'alice', 20, 10), 10200n);
});

test('special-edition delta saturates stale removals and fails closed without required stores', async () => {
  const { context, stores } = buildContext();
  seedEdition(stores);
  stores.UserSpecialEditionState.set({
    id: 'alice:1',
    user_id: 'alice',
    editionId: 1n,
    tokenCount: 1n,
    countTimestamps: [0],
    tokenCountHistory: [1n],
    updatedAt: 0,
  });

  await applyUserSpecialEditionDelta(context, 'alice', 1n, -5n, 10, '0x01', 'STALE_OUT', 1);
  const userState = stores.UserSpecialEditionState.rows.get('alice:1');
  assert.equal(userState?.tokenCount, 0n);
  assert.deepEqual(userState?.tokenCountHistory, [1n, 0n]);
  assert.equal(stores.UserSpecialEditionAggregate.rows.get('alice')?.specialEditionCount, 0n);
  assert.equal(stores.UserLeaderboardState.rows.get('alice')?.specialEditionMultiplier, 10000n);
  assert.equal(stores.UserSpecialEditionHistory.rows.size, 1);
  assert.equal(stores.UserMultiplierSnapshot.rows.size, 1);

  await applyUserSpecialEditionDelta(context, ZERO_ADDRESS, 1n, 1n, 11, '0x02', 'ZERO', 2);
  assert.equal(stores.UserSpecialEditionHistory.rows.size, 1);

  const missing = { UserSpecialEditionState: store() } as unknown as handlerContext;
  await applyUserSpecialEditionDelta(missing, 'alice', 1n, 1n, 12, '0x03', 'MISSING', 3);
});
