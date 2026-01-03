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
    scaledVariableDebt: 500n,
    currentATokenBalance: 123n,
    currentVariableDebt: 456n,
    currentStableDebt: 0n,
  };

  const balances = getCurrentBalancesFromScaled(reserve, userReserve, 900);

  assert.equal(balances.supply, 123n);
  assert.equal(balances.variableDebt, 456n);
  assert.equal(balances.totalDebt, 456n);
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
    scaledVariableDebt: 500n,
    currentATokenBalance: 0n,
    currentVariableDebt: 0n,
    currentStableDebt: 0n,
  };

  const balances = getCurrentBalancesFromScaled(reserve, userReserve, 2000);

  assert.equal(balances.supply, 2000n);
  assert.equal(balances.variableDebt, 1500n);
  assert.equal(balances.totalDebt, 1500n);
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
    scaledVariableDebt: 500n,
    currentATokenBalance: 10n,
    currentVariableDebt: 20n,
    currentStableDebt: 0n,
  };

  const balances = getCurrentBalancesFromScaled(reserve, userReserve, 1000, {
    liquidityIndex: 2n * RAY,
    variableBorrowIndex: 3n * RAY,
  });

  assert.equal(balances.supply, 2000n);
  assert.equal(balances.variableDebt, 1500n);
  assert.equal(balances.totalDebt, 1500n);
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
