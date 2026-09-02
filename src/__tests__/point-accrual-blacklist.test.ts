// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. This file does not import the `v3-test-helpers` seam, so without this
// a bare `node --test` invocation would inherit them from the repo `.env` via envio's
// dotenv. Redundant under `pnpm run test`, which loads the same module via `--import`.
import './test-env-preload';

import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  POINT_ACCRUAL_BLACKLIST,
  POINT_ACCRUAL_BLACKLIST_FROM,
  isPointAccrualBlacklisted,
} from '../helpers/constants';
import {
  awardDailyBorrowPoints,
  awardDailyRepayPoints,
  awardDailySupplyPoints,
  awardDailyWithdrawPoints,
  settlePointsForUser,
} from '../handlers/shared';

import type { handlerContext } from '../../generated';

// A Foundation multisig, and one of the tide-draw entries.
const MULTISIG = '0x49a18e0ffeb2a1254922675a854a0818b46446e2';
const DRAW = '0x909b176220b7e782c0f3ceccab4b19d2c433c6bb';
const INNOCENT = '0x000000000000000000000000000000000000dead';

test('the blacklist is the deduplicated union of both sources', () => {
  // 14 tide-draw entries + 12 multisigs, with 0x909b1762... in both.
  assert.equal(POINT_ACCRUAL_BLACKLIST.size, 25);
  assert.ok(POINT_ACCRUAL_BLACKLIST.has(MULTISIG));
  assert.ok(POINT_ACCRUAL_BLACKLIST.has(DRAW));
  assert.ok(!POINT_ACCRUAL_BLACKLIST.has(INNOCENT));
  // Stored lowercase so a normalized address matches without per-call casing work.
  for (const entry of POINT_ACCRUAL_BLACKLIST) {
    assert.equal(entry, entry.toLowerCase(), entry);
  }
});

test('blacklisting starts exactly at the Tide 9 boundary', () => {
  // Tides 1-8 were scored and paid under the old rules and must not move.
  assert.equal(isPointAccrualBlacklisted(MULTISIG, POINT_ACCRUAL_BLACKLIST_FROM - 1), false);
  assert.equal(isPointAccrualBlacklisted(MULTISIG, POINT_ACCRUAL_BLACKLIST_FROM), true);
  assert.equal(isPointAccrualBlacklisted(MULTISIG, POINT_ACCRUAL_BLACKLIST_FROM + 1), true);
  // An address that is not listed is never gated, on either side of the boundary.
  assert.equal(isPointAccrualBlacklisted(INNOCENT, POINT_ACCRUAL_BLACKLIST_FROM + 1), false);
});

// A context that throws on any access: if a gated path touches a single store, the
// test fails loudly rather than silently proving nothing.
function trapContext(): handlerContext {
  return new Proxy(
    {},
    {
      get(_target, prop) {
        if (prop === 'isPreload') return false;
        throw new Error(`gated path touched context.${String(prop)}`);
      },
    }
  ) as unknown as handlerContext;
}

test('every accrual entry point is a no-op for a blacklisted address', async () => {
  const after = POINT_ACCRUAL_BLACKLIST_FROM + 3600;
  const context = trapContext();

  for (const address of [MULTISIG, DRAW]) {
    await settlePointsForUser(context, address, null, after, 99_999_999n);
    await awardDailySupplyPoints(context, address, after);
    await awardDailyBorrowPoints(context, address, after);
    await awardDailyRepayPoints(context, address, after);
    await awardDailyWithdrawPoints(context, address, after);
  }
});

test('a checksummed blacklist entry still matches after normalization', () => {
  // blacklist.json stores mixed case and the multisigs are checksummed; the accrual
  // sites normalize before the lookup, so the stored form must be lowercase.
  const checksummed = '0x49a18e0FfEb2A1254922675a854a0818B46446E2';
  assert.equal(isPointAccrualBlacklisted(checksummed, POINT_ACCRUAL_BLACKLIST_FROM), false);
  assert.equal(
    isPointAccrualBlacklisted(checksummed.toLowerCase(), POINT_ACCRUAL_BLACKLIST_FROM),
    true
  );
});
