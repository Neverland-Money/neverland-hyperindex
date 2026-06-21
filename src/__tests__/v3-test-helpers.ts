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
/* eslint-disable @typescript-eslint/no-explicit-any */
import fs from 'node:fs';
import path from 'node:path';

// Operator-only backfill gate (ENVIO_LEADERBOARD_LIVE_EPOCH) must be OFF during
// tests so a populated .env can never gate mid-epoch keeper settlements. The
// gate-specific tests set it explicitly.
process.env.ENVIO_LEADERBOARD_LIVE_EPOCH = '';

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
