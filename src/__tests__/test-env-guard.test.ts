/**
 * Pins the test environment itself.
 *
 * Deliberately imports neither `v3-test-helpers` nor any handler: 13 of the suite's test
 * files bypass that seam, so this file must prove the RUNNER-level preload
 * (`--import ./dist-test/src/__tests__/test-env-preload.js`, the compiled form of
 * `src/__tests__/test-env-preload.ts` that the v2 scripts run) is what protects them.
 *
 * Run it through `pnpm run test`; invoked directly with a bare `node --test` no preload is
 * loaded and the first assertion fails by design, which is the intended signal.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import {
  TEST_PREFILL_DIR_SENTINEL,
  isPrefillEnabled,
  loadPrefilledTides,
  resetPrefillCache,
} from '../helpers/prefill';

// The scripts run the compiled suite from dist-test, so the flag names the compiled preload.
const PRELOAD_FLAG = '--import ./dist-test/src/__tests__/test-env-preload.js';
const CONCURRENCY_FLAG = '--test-concurrency=2';
const TEST_SCRIPTS = ['test', 'test:coverage', 'test:coverage:check'];

/** Reads package.json as data. Nothing is executed. */
function readScripts(): Record<string, string> {
  const pkg = JSON.parse(fs.readFileSync(path.resolve(process.cwd(), 'package.json'), 'utf8')) as {
    scripts: Record<string, string>;
  };
  return pkg.scripts;
}

/** Restores whatever the preload left in place, so ordering between tests cannot matter. */
function withDataDir<T>(dir: string, run: () => T): T {
  const prev = process.env.PREFILL_DATA_DIR;
  process.env.PREFILL_DATA_DIR = dir;
  resetPrefillCache();
  try {
    return run();
  } finally {
    process.env.PREFILL_DATA_DIR = prev ?? TEST_PREFILL_DIR_SENTINEL;
    resetPrefillCache();
  }
}

test('the runner preload armed a safe environment', () => {
  assert.equal(
    process.env.NEVERLAND_TEST_ENV,
    '1',
    'test-env-preload.ts did not run -- use `pnpm run test`, not a bare `node --test`'
  );
  assert.equal(process.env.PREFILL_HISTORIC_EPOCHS, 'false');
  assert.equal(process.env.PREFILL_DATA_DIR, TEST_PREFILL_DIR_SENTINEL);
  assert.equal(isPrefillEnabled(), false, 'ordinary tests must run with prefill off');
});

test('the preload sentinel matches the one the guard compares against', () => {
  // The preload is dependency-free by design, so the literal is duplicated. Pin them equal.
  const preload = fs.readFileSync(
    path.resolve(process.cwd(), 'src/__tests__/test-env-preload.ts'),
    'utf8'
  );
  assert.ok(
    preload.includes(`'${TEST_PREFILL_DIR_SENTINEL}'`),
    'test-env-preload.ts and helpers/prefill.ts disagree on the sentinel value'
  );
});

test('an un-overridden data directory fails loudly rather than loading anything', () => {
  withDataDir(TEST_PREFILL_DIR_SENTINEL, () => {
    assert.throws(() => loadPrefilledTides(), /still the test sentinel/);
  });
});

test('ordinary tests cannot load the production data directory', () => {
  // Self-guarding: if the sentinel were not armed the guard would be inert and this test
  // would read the real 31 MB directory. Refuse to point at it unless the tripwire is live.
  assert.equal(process.env.NEVERLAND_TEST_ENV, '1', 'refusing to run without the tripwire armed');
  withDataDir(path.resolve(process.cwd(), 'data'), () => {
    assert.throws(() => loadPrefilledTides(), /must not read the production data\/ directory/);
  });
});

test('every test script loads the environment preload', () => {
  // Stops a fourth script being added without the guard.
  const scripts = readScripts();
  for (const name of TEST_SCRIPTS) {
    assert.ok(scripts[name], `missing script ${name}`);
    assert.ok(scripts[name].includes(PRELOAD_FLAG), `script "${name}" must load ${PRELOAD_FLAG}`);
  }
});

test('every test script caps runner concurrency', () => {
  // Bounds how many workers run at once. This limits blast radius; it is NOT a memory
  // boundary -- only a cgroup limit is that.
  const scripts = readScripts();
  for (const name of TEST_SCRIPTS) {
    assert.ok(
      scripts[name].includes(CONCURRENCY_FLAG),
      `script "${name}" must pass ${CONCURRENCY_FLAG}`
    );
  }
});

test('every test file pins the operator settings', () => {
  // Each test file must reach the preload, either directly or through the `v3-test-helpers`
  // seam, so a bare `node --test <file>` cannot inherit prefill settings from `.env`.
  // This file is exempt: it proves the RUNNER-level preload works without either import.
  const dir = path.resolve(process.cwd(), 'src/__tests__');
  const exempt = new Set(['test-env-guard.test.ts']);
  const offenders = fs
    .readdirSync(dir)
    .filter(name => name.endsWith('.test.ts') && !exempt.has(name))
    .filter(name => {
      const source = fs.readFileSync(path.join(dir, name), 'utf8');
      return !source.includes('test-env-preload') && !source.includes('v3-test-helpers');
    });

  assert.deepEqual(
    offenders,
    [],
    `these test files import neither test-env-preload nor v3-test-helpers: ${offenders.join(', ')}`
  );
});

test('the tripwire is inert when the sentinel is absent (production behavior)', () => {
  // Proves the guard is test-only: with NEVERLAND_TEST_ENV unset it does not intervene.
  // Uses a FIXTURE directory -- never the production one, which is the whole point.
  const prevEnv = process.env.NEVERLAND_TEST_ENV;
  const prevOn = process.env.PREFILL_HISTORIC_EPOCHS;
  delete process.env.NEVERLAND_TEST_ENV;
  process.env.PREFILL_HISTORIC_EPOCHS = 'true';
  try {
    withDataDir(path.resolve(process.cwd(), 'src/__tests__/fixtures/prefill-tides'), () => {
      assert.equal(loadPrefilledTides().length, 3, 'fixtures load normally with the guard off');
    });
  } finally {
    process.env.NEVERLAND_TEST_ENV = prevEnv ?? '1';
    process.env.PREFILL_HISTORIC_EPOCHS = prevOn ?? 'false';
    resetPrefillCache();
  }
});
