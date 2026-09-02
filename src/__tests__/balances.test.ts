// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. This file does not import the `v3-test-helpers` seam, so without this
// a bare `node --test` invocation would inherit them from the repo `.env` via envio's
// dotenv. Redundant under `pnpm run test`, which loads the same module via `--import`.
import './test-env-preload';

import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  getCurrentBalancesFromScaled,
  getReserveNormalizedIncome,
  getReserveNormalizedVariableDebt,
} from '../handlers/shared';

const RAY = 10n ** 27n;

test('getCurrentBalancesFromScaled falls back to stored balances for past timestamps', () => {
  const reserve = {
    liquidityIndex: 2n * RAY,
    liquidityRate: 0n,
    variableBorrowIndex: 3n * RAY,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: 1000,
  };

  const userReserve = {
    scaledATokenBalance: 1000n,
    scaledDebt: 500n,
    currentATokenBalance: 123n,
    currentDebt: 456n,
  };

  const balances = getCurrentBalancesFromScaled(reserve, userReserve, 900);

  assert.equal(balances.supply, 123n);
  assert.equal(balances.debt, 456n);
});

test('getCurrentBalancesFromScaled uses normalized indices at current timestamps', () => {
  const reserve = {
    liquidityIndex: 2n * RAY,
    liquidityRate: 0n,
    variableBorrowIndex: 3n * RAY,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: 1000,
  };

  const userReserve = {
    scaledATokenBalance: 1000n,
    scaledDebt: 500n,
    currentATokenBalance: 0n,
    currentDebt: 0n,
  };

  const balances = getCurrentBalancesFromScaled(reserve, userReserve, 2000);

  assert.equal(balances.supply, 2000n);
  assert.equal(balances.debt, 1500n);
});

test('getCurrentBalancesFromScaled uses override indices for historical timestamps', () => {
  const reserve = {
    liquidityIndex: 1n * RAY,
    liquidityRate: 0n,
    variableBorrowIndex: 1n * RAY,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: 2000,
  };

  const userReserve = {
    scaledATokenBalance: 1000n,
    scaledDebt: 500n,
    currentATokenBalance: 10n,
    currentDebt: 20n,
  };

  const balances = getCurrentBalancesFromScaled(reserve, userReserve, 1000, {
    liquidityIndex: 2n * RAY,
    variableBorrowIndex: 3n * RAY,
  });

  assert.equal(balances.supply, 2000n);
  assert.equal(balances.debt, 1500n);
});

test('reserve normalized helpers return early for zero or stale timestamps', () => {
  const reserve = {
    liquidityIndex: 0n,
    liquidityRate: 0n,
    variableBorrowIndex: 0n,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: 100,
  };

  assert.equal(getReserveNormalizedIncome(reserve, 200), 0n);
  assert.equal(getReserveNormalizedVariableDebt(reserve, 200), 0n);

  const reserveFresh = {
    liquidityIndex: 2n * RAY,
    liquidityRate: 0n,
    variableBorrowIndex: 3n * RAY,
    variableBorrowRate: 0n,
    lastUpdateTimestamp: 200,
  };

  assert.equal(getReserveNormalizedIncome(reserveFresh, 200), 2n * RAY);
  assert.equal(getReserveNormalizedVariableDebt(reserveFresh, 200), 3n * RAY);
});
