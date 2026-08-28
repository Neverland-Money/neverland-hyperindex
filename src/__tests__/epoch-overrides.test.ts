import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  EPOCH_1_END_TIME_OVERRIDE,
  EPOCH_1_START_BLOCK_OVERRIDE,
  EPOCH_1_START_TIME_OVERRIDE,
  EPOCH_DATES_OVERRIDES,
  getEpochDatesOverride,
} from '../helpers/constants';

// This file owns the bootstrap env switch, so it runs in its own process and
// cannot leak the flipped value into the handler suites.

test('an overridden epoch returns its dates', () => {
  const tideNine = getEpochDatesOverride(9n);
  assert.equal(tideNine?.startTime, 1787893200);
  assert.equal(tideNine?.endTime, 1790442000);
  assert.equal(tideNine?.startBlock, undefined);
  assert.equal(tideNine?.bootstrap, undefined);
});

test('an epoch with no entry returns null', () => {
  assert.equal(getEpochDatesOverride(8n), null);
  assert.equal(getEpochDatesOverride(10n), null);
  assert.equal(getEpochDatesOverride(0), null);
});

test('a bootstrapped epoch follows the bootstrap switch', () => {
  const previous = process.env.ENVIO_DISABLE_BOOTSTRAP;
  try {
    process.env.ENVIO_DISABLE_BOOTSTRAP = 'true';
    assert.equal(getEpochDatesOverride(1n), null, 'disabled bootstrap hides epoch 1');

    process.env.ENVIO_DISABLE_BOOTSTRAP = 'false';
    assert.equal(getEpochDatesOverride(1n)?.startTime, 1767434400);

    delete process.env.ENVIO_DISABLE_BOOTSTRAP;
    assert.equal(getEpochDatesOverride(1n)?.endTime, 1769983200);
  } finally {
    if (previous === undefined) delete process.env.ENVIO_DISABLE_BOOTSTRAP;
    else process.env.ENVIO_DISABLE_BOOTSTRAP = previous;
  }
});

test('epoch 1 bootstrap constants stay in step with the table', () => {
  assert.equal(EPOCH_1_START_TIME_OVERRIDE, EPOCH_DATES_OVERRIDES['1'].startTime);
  assert.equal(EPOCH_1_END_TIME_OVERRIDE, EPOCH_DATES_OVERRIDES['1'].endTime);
  assert.equal(EPOCH_1_START_BLOCK_OVERRIDE, EPOCH_DATES_OVERRIDES['1'].startBlock);
  assert.equal(EPOCH_1_START_BLOCK_OVERRIDE, 46264051);
});
