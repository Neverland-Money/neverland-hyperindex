import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import zlib from 'node:zlib';

import {
  TEST_PREFILL_DIR_SENTINEL,
  isPrefillEnabled,
  isPrefilledEpoch,
  isPrefilledTimestamp,
  loadPrefillSnapshot,
  loadPrefilledTides,
  prefillDataDir,
  prefillHistoricEpochsIfNeeded,
  prefilledBeforeTimestamp,
  resetPrefillCache,
  setUserEpochStats,
} from '../helpers/prefill';

import { TestHelpers } from './v3-test-helpers';

import type { handlerContext } from '../../generated';

/** Past every fixture tide's endTime, so the boundary write is due. Prefill now applies
 *  at the boundary -- the first event at or after the first non-prefilled Tide -- so a
 *  timestamp inside the prefilled span writes nothing. */
const AFTER_BOUNDARY = 1_000_000_000;

const FIXTURES = path.resolve(process.cwd(), 'src/__tests__/fixtures/prefill-tides');
const BIGINT_FIXTURES = path.resolve(process.cwd(), 'src/__tests__/fixtures/prefill-bigint');
const NEXT_OVERRIDE_FIXTURES = path.resolve(
  process.cwd(),
  'src/__tests__/fixtures/prefill-next-override'
);
const NO_ENDTIME_FIXTURES = path.resolve(
  process.cwd(),
  'src/__tests__/fixtures/prefill-no-endtime'
);

const REAL_DATA_DIR = path.resolve(process.cwd(), 'data');

/**
 * Enables prefill against a fixture directory. The directory is required, and the real
 * `data/` is refused: those are 31 MB of production tides, and loading them into a test
 * worker once cost ~57 GiB of RSS and took the host down with it.
 */
async function withPrefill<T>(dir: string, run: () => T | Promise<T>): Promise<T> {
  if (path.resolve(dir) === REAL_DATA_DIR) {
    throw new Error('tests must use a fixture directory, never the real data/');
  }
  const prevOn = process.env.PREFILL_HISTORIC_EPOCHS;
  const prevDir = process.env.PREFILL_DATA_DIR;
  process.env.PREFILL_HISTORIC_EPOCHS = 'true';
  process.env.PREFILL_DATA_DIR = dir;
  resetPrefillCache();
  try {
    // MUST await: returning the promise would restore the env before the callback resolves.
    return await run();
  } finally {
    // Restore the caller's exact value when there was one; otherwise fall back to the safe
    // baseline rather than deleting, so a later `import 'dotenv/config'` cannot repopulate
    // the key from the repo `.env`.
    process.env.PREFILL_HISTORIC_EPOCHS = prevOn ?? 'false';
    process.env.PREFILL_DATA_DIR = prevDir ?? TEST_PREFILL_DIR_SENTINEL;
    resetPrefillCache();
  }
}

/**
 * Runs with prefill explicitly off. Importing `envio` loads the repo `.env`, which sets
 * `PREFILL_HISTORIC_EPOCHS=true`, so "disabled" cannot be left to ambient state -- and it
 * must be assigned rather than deleted, because dotenv repopulates absent keys.
 */
async function withoutPrefill<T>(run: () => T | Promise<T>): Promise<T> {
  const prevOn = process.env.PREFILL_HISTORIC_EPOCHS;
  process.env.PREFILL_HISTORIC_EPOCHS = 'false';
  resetPrefillCache();
  try {
    return await run();
  } finally {
    process.env.PREFILL_HISTORIC_EPOCHS = prevOn ?? 'false';
    resetPrefillCache();
  }
}

/** Records what a handler wrote, and answers `get` from what is already recorded. */
function recordingContext() {
  const epochs = new Map<string, Record<string, unknown>>();
  const stats = new Map<string, Record<string, unknown>>();
  // Counts `set` calls so a re-run can be detected without clearing the store -- the
  // already-applied marker is itself a UserEpochStats row, so clearing would erase it.
  const writes = { stats: 0 };
  const context = {
    LeaderboardEpoch: {
      get: async (id: string) => epochs.get(id),
      set: (row: Record<string, unknown>) => epochs.set(String(row.id), row),
    },
    UserEpochStats: {
      get: async (id: string) => stats.get(id),
      set: (row: Record<string, unknown>) => {
        writes.stats += 1;
        stats.set(String(row.id), row);
      },
    },
  } as unknown as handlerContext;
  return { context, epochs, stats, writes };
}

test('prefill is off unless the env var is exactly "true"', () => {
  const prev = process.env.PREFILL_HISTORIC_EPOCHS;
  try {
    delete process.env.PREFILL_HISTORIC_EPOCHS;
    assert.equal(isPrefillEnabled(), false);
    process.env.PREFILL_HISTORIC_EPOCHS = 'false';
    assert.equal(isPrefillEnabled(), false);
    process.env.PREFILL_HISTORIC_EPOCHS = '1';
    assert.equal(isPrefillEnabled(), false, 'only the literal "true" enables it');
    process.env.PREFILL_HISTORIC_EPOCHS = 'true';
    assert.equal(isPrefillEnabled(), true);
  } finally {
    if (prev === undefined) delete process.env.PREFILL_HISTORIC_EPOCHS;
    else process.env.PREFILL_HISTORIC_EPOCHS = prev;
  }
});

test('while disabled nothing is read and no timestamp is covered', async () => {
  await withoutPrefill(async () => {
    assert.equal(prefilledBeforeTimestamp(), 0);
    assert.equal(isPrefilledTimestamp(1), false);
    const { context, epochs, stats } = recordingContext();
    await prefillHistoricEpochsIfNeeded(context, AFTER_BOUNDARY);
    assert.equal(epochs.size, 0);
    assert.equal(stats.size, 0);
  });
});

test('the data directory defaults next to the project and is overridable', () => {
  const prev = process.env.PREFILL_DATA_DIR;
  try {
    delete process.env.PREFILL_DATA_DIR;
    assert.equal(prefillDataDir(), path.resolve(process.cwd(), 'data'));
    process.env.PREFILL_DATA_DIR = '/tmp/elsewhere';
    assert.equal(prefillDataDir(), '/tmp/elsewhere');
  } finally {
    if (prev === undefined) delete process.env.PREFILL_DATA_DIR;
    else process.env.PREFILL_DATA_DIR = prev;
  }
});

test('discovery is the file listing, ordered by tide', async () => {
  await withPrefill(FIXTURES, () => {
    const tides = loadPrefilledTides();
    assert.deepEqual(
      tides.map(t => t.tide),
      [1, 2, 3]
    );
    // Cached: a second call must not re-read.
    assert.equal(loadPrefilledTides(), tides);
  });
});

test('coverage is bounded just past the last tide end', async () => {
  await withPrefill(FIXTURES, () => {
    // Exclusive bound: the fixtures declare no start for tide 4, so it falls back to one
    // second past tide 2's end of 5000.
    assert.equal(prefilledBeforeTimestamp(), 5001);
    assert.equal(isPrefilledTimestamp(4999), true);
    assert.equal(isPrefilledTimestamp(5000), true, 'the boundary instant is still prefilled');
    assert.equal(isPrefilledTimestamp(5001), false, 'the next instant accrues normally');
  });
});

test('a missing data directory is a no-op, not a crash', async () => {
  await withPrefill('/nonexistent/prefill/dir', async () => {
    assert.deepEqual(loadPrefilledTides(), []);
    assert.equal(prefilledBeforeTimestamp(), 0);
    const { context, epochs } = recordingContext();
    await prefillHistoricEpochsIfNeeded(context, AFTER_BOUNDARY);
    assert.equal(epochs.size, 0);
  });
});

test('tides are written verbatim with BigInt fields revived', async () => {
  await withPrefill(FIXTURES, async () => {
    const { context, epochs, stats } = recordingContext();
    await prefillHistoricEpochsIfNeeded(context, AFTER_BOUNDARY);

    assert.deepEqual([...epochs.keys()].sort(), ['1', '2', '3']);
    assert.equal(stats.size, 3, 'tide 1 has one user, tide 2 has two, tide 3 has none');

    const epoch = epochs.get('2')!;
    assert.equal(epoch.epochNumber, 2n, 'numeric strings become BigInt');
    assert.equal(epoch.endTime, 5000, 'Int fields stay numbers');
    assert.equal(epoch.isActive, false);

    const row = stats.get('0xaaa:2')!;
    assert.equal(row.totalPointsWithMultiplier, 1000n);
    assert.equal(row.user_id, '0xaaa', 'strings are untouched');
    assert.equal(row.rank, 1);
  });
});

test('nothing is written while the timestamp is still inside the prefilled span', async () => {
  await withPrefill(FIXTURES, async () => {
    const { context, epochs, stats } = recordingContext();
    const boundary = prefilledBeforeTimestamp();
    assert.ok(boundary > 0, 'the fixtures must define a boundary for this test to mean anything');

    await prefillHistoricEpochsIfNeeded(context, boundary - 1);
    assert.equal(epochs.size, 0, 'a timestamp inside the span writes nothing');
    assert.equal(stats.size, 0);

    // The image describes the state the span ENDS in. Writing it at the first event instead
    // let the span replay on top of it, which silently reverted rows to mid-span values.
    await prefillHistoricEpochsIfNeeded(context, boundary);
    assert.ok(epochs.size > 0, 'the first event at the boundary writes the image');
  });
});

test('prefill is idempotent once the last tide is present', async () => {
  await withPrefill(FIXTURES, async () => {
    const { context, epochs, stats, writes } = recordingContext();
    await prefillHistoricEpochsIfNeeded(context, AFTER_BOUNDARY);
    assert.equal(stats.size, 3);
    assert.equal(writes.stats, 3);

    // The store is left intact: the marker the second pass looks for is one of these rows.
    await prefillHistoricEpochsIfNeeded(context, AFTER_BOUNDARY);
    assert.equal(writes.stats, 3, 'the marker short-circuits the second pass');
    assert.equal(epochs.size, 3);
  });
});

test('a tide file with no endTime neither crashes nor extends coverage', async () => {
  await withPrefill(FIXTURES, () => {
    // tide-3 carries no endTime. It must still load, and must not drag the covered span
    // backwards or forwards -- coverage stays at tide 2's end.
    assert.equal(loadPrefilledTides().length, 3);
    assert.equal(prefilledBeforeTimestamp(), 5001);
    assert.equal(isPrefilledTimestamp(5001), false);
  });
});

test('closing a prefilled Tide neither sweeps holders nor freezes growth', async () => {
  // Reaches the guard in freezeLPForEpochEnd: the close path for a Tide whose figures are
  // already stored must not run the LP holder sweep or write a growth freeze, because both
  // would overwrite settled values.
  await withPrefill(FIXTURES, async () => {
    let mockDb = TestHelpers.MockDb.createMockDb();
    mockDb = mockDb.entities.LeaderboardState.set({
      id: 'current',
      currentEpochNumber: 1n,
      isActive: true,
    });
    mockDb = mockDb.entities.LeaderboardEpoch.set({
      id: '1',
      epochNumber: 1n,
      startBlock: 1n,
      startTime: 1000,
      endBlock: undefined,
      endTime: undefined,
      isActive: true,
      duration: undefined,
      scheduledStartTime: 1000,
      scheduledEndTime: 2000,
    });

    // endTime 2000 sits inside the prefilled span (fixtures run to 5000).
    const epochEnd = TestHelpers.EpochManager.EpochEnd.createMockEvent({
      epochNumber: 1n,
      endTime: 2000n,
      block: { number: 500, timestamp: 2000 },
      logIndex: 1,
      srcAddress: '0x0000000000000000000000000000000000009001',
      transaction: { hash: `0x${'9'.repeat(64)}` },
    });
    mockDb = await TestHelpers.EpochManager.EpochEnd.processEvent({ event: epochEnd, mockDb });

    assert.deepEqual(
      mockDb.entities.LPPoolEpochGrowth.getAll(),
      [],
      'a prefilled Tide writes no growth freeze'
    );
  });
});

test('bare integer literals above 2^53 survive the read exactly', async () => {
  // The real tide files carry unquoted integers, which `JSON.parse` reads into a double:
  // 172866260298447819119 lands as ...814656 and 13306925150384721 as ...720. Every point
  // total above 2^53 was being rounded before it reached Postgres.
  await withPrefill(BIGINT_FIXTURES, () => {
    const [doc] = loadPrefilledTides();
    const row = doc.userEpochStats[0] as Record<string, unknown>;

    assert.equal(BigInt(row.depositPoints as string), 172866260298447819119n);
    assert.equal(BigInt(row.totalPoints as string), 13306925150384721n);
    assert.equal(BigInt(row.manualAwardPoints as string), 1000000000000000000000n);
    assert.notEqual(BigInt(row.depositPoints as string), 172866260298447814656n);

    // Values that are not BigInt fields keep their JSON type.
    assert.equal(row.id, '0xaaa:1');
    assert.equal(row.rank, 1);
    assert.equal(row.lastSupplyPointsDay, -1);
  });
});

test('reviving a bare-literal tide writes exact BigInts', async () => {
  await withPrefill(BIGINT_FIXTURES, async () => {
    const { context, stats } = recordingContext();
    await prefillHistoricEpochsIfNeeded(context, AFTER_BOUNDARY);
    const row = stats.get('0xaaa:1')!;
    assert.equal(row.depositPoints, 172866260298447819119n);
    assert.equal(row.totalPoints, 13306925150384721n);
    assert.equal(row.epochNumber, 1n);
  });
});

test('a prefilled tide is recognized by number, and only while prefill is on', async () => {
  await withPrefill(FIXTURES, () => {
    assert.equal(isPrefilledEpoch(1n), true);
    assert.equal(isPrefilledEpoch(3), true, 'accepts number as well as bigint');
    assert.equal(isPrefilledEpoch(4n), false, 'tide 4 has no file');
  });
  await withoutPrefill(() => {
    assert.equal(isPrefilledEpoch(1n), false, 'disabled: nothing is prefilled');
  });
});

test('writes onto a prefilled tide are dropped, later tides pass through', async () => {
  // The timestamp gate cannot catch these: settlement walks backwards, so an event in a
  // later tide still credits time held during a prefilled one.
  await withPrefill(FIXTURES, () => {
    const { context, stats } = recordingContext();

    setUserEpochStats(context, { id: '0xaaa:1', epochNumber: 1n } as never);
    assert.equal(stats.size, 0, 'tide 1 is settled and paid -- the write is dropped');

    setUserEpochStats(context, { id: '0xaaa:4', epochNumber: 4n } as never);
    assert.equal(stats.size, 1, 'tide 4 is not prefilled and accrues normally');
    assert.equal(stats.get('0xaaa:4')!.epochNumber, 4n);
  });
});

test('with prefill off every tide accepts writes', async () => {
  await withoutPrefill(() => {
    const { context, stats } = recordingContext();
    setUserEpochStats(context, { id: '0xaaa:1', epochNumber: 1n } as never);
    assert.equal(stats.size, 1);
  });
});

test('the fixture guard refuses the real data directory', async () => {
  await assert.rejects(
    () => withPrefill(path.resolve(process.cwd(), 'data'), () => undefined),
    /never the real data/
  );
});

test('the preload pass writes nothing and revives no rows', async () => {
  // Writes are no-ops during preload, but without the early return the loop still revives
  // every row into throwaway objects on both passes.
  await withPrefill(FIXTURES, async () => {
    const { context, epochs, stats } = recordingContext();
    const preloadContext = { ...context, isPreload: true } as unknown as handlerContext;
    await prefillHistoricEpochsIfNeeded(preloadContext, AFTER_BOUNDARY);
    assert.equal(epochs.size, 0, 'preload must not seed epochs');
    assert.equal(stats.size, 0, 'preload must not seed stats');

    // The ordered pass still does the work.
    await prefillHistoricEpochsIfNeeded(context, AFTER_BOUNDARY);
    assert.equal(epochs.size, 3);
    assert.equal(stats.size, 3);
  });
});

test('the bound prefers the next tide declared start over the last tide end', async () => {
  // Tides do not abut. When the tide after the last prefilled one has a declared start, that
  // start is the bound, so the gap between them stays guarded.
  await withPrefill(NEXT_OVERRIDE_FIXTURES, () => {
    assert.equal(prefilledBeforeTimestamp(), 1787893200, "Tide 9's declared start");
    assert.equal(isPrefilledTimestamp(1787889600), true, "Tide 8's end is still prefilled");
    assert.equal(isPrefilledTimestamp(1787891000), true, 'the gap hour is guarded too');
    assert.equal(isPrefilledTimestamp(1787893200), false, 'Tide 9 accrues normally');
  });
});

test('a prefilled set with no endTime anywhere covers nothing', async () => {
  // Neither an override for the next tide nor any endTime to fall back on: rather than
  // guess a bound, cover nothing and let the tide be indexed normally.
  await withPrefill(NO_ENDTIME_FIXTURES, () => {
    assert.equal(loadPrefilledTides().length, 1);
    assert.equal(prefilledBeforeTimestamp(), 0);
    assert.equal(isPrefilledTimestamp(6000), false);
  });
});

// ---------------------------------------------------------------------------------------------
// Artifact resolution and the boundary snapshot. Every fixture below is generated into a
// throwaway directory: the tripwire admits any directory that is neither the sentinel nor the
// real data/, and nothing here comes anywhere near the production artifacts.
// ---------------------------------------------------------------------------------------------

/** A scratch directory for one test, removed on the way out. */
async function withScratchDir<T>(run: (dir: string) => T | Promise<T>): Promise<T> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nvl-prefill-'));
  try {
    return await run(dir);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('prefillDataDir resolves the override, then cwd/data, then the parent data/', async () => {
  // Only the path is computed here; no artifact is ever read, so the real data/ is safe.
  const prevDir = process.env.PREFILL_DATA_DIR;
  const prevCwd = process.cwd();
  const repoData = path.resolve(prevCwd, 'data');
  try {
    process.env.PREFILL_DATA_DIR = '/explicit/override';
    assert.equal(prefillDataDir(), '/explicit/override');

    delete process.env.PREFILL_DATA_DIR;
    // cwd holds data/ (the repo root).
    assert.equal(prefillDataDir(), repoData);

    // cwd is generated/, the directory `envio start` runs from: no data/ here, but the parent
    // has it. This is the production case the fallback exists for.
    process.chdir(path.resolve(prevCwd, 'generated'));
    assert.equal(prefillDataDir(), repoData);

    // Neither cwd nor its parent has data/: the cwd form is returned so the caller's error
    // names the directory it looked in.
    await withScratchDir(scratch => {
      const nested = path.join(scratch, 'nested');
      fs.mkdirSync(nested);
      process.chdir(nested);
      assert.equal(prefillDataDir(), path.join(nested, 'data'));
    });
  } finally {
    process.chdir(prevCwd);
    process.env.PREFILL_DATA_DIR = prevDir ?? TEST_PREFILL_DIR_SENTINEL;
  }
});

test('a gzipped tide loads, is preferred over its plain twin, and the pair dedupes to one', async () => {
  await withScratchDir(async scratch => {
    const plain = fs.readFileSync(path.join(FIXTURES, 'tide-1.json'));
    // Same tide in both forms; the .gz carries a distinguishing endTime so the choice is visible.
    const gzDoc = JSON.parse(plain.toString('utf8')) as { epoch: { endTime: number } };
    gzDoc.epoch.endTime = 4242;
    fs.writeFileSync(path.join(scratch, 'tide-1.json'), plain);
    fs.writeFileSync(path.join(scratch, 'tide-1.json.gz'), zlib.gzipSync(JSON.stringify(gzDoc)));
    await withPrefill(scratch, () => {
      const tides = loadPrefilledTides();
      assert.equal(tides.length, 1, 'tide-1.json and tide-1.json.gz are the same tide');
      assert.equal(tides[0].epoch.endTime, 4242, 'the .gz form wins when both are present');
    });
  });
});

test('a tide entry that cannot be read is an error, not a silent gap', async () => {
  await withScratchDir(async scratch => {
    // A directory matching the tide pattern is listed by readdir but readable as neither form.
    fs.mkdirSync(path.join(scratch, 'tide-7.json'));
    await withPrefill(scratch, () => {
      assert.throws(() => loadPrefilledTides(), /prefill: unreadable tide file .*tide-7\.json/);
    });
  });
});

test('the boundary snapshot loads once, is cached, and is absent without error', async () => {
  await withScratchDir(async scratch => {
    await withPrefill(scratch, () => {
      assert.equal(loadPrefillSnapshot(), null, 'no file is a valid state');
      assert.equal(loadPrefillSnapshot(), null, 'absence is cached too');
    });
    fs.writeFileSync(
      path.join(scratch, 'prefill-snapshot.json'),
      JSON.stringify({ boundaryBlock: 99, exportedAt: 'x', source: 'test', tables: {} })
    );
    await withPrefill(scratch, () => {
      const first = loadPrefillSnapshot();
      assert.equal(first?.boundaryBlock, 99);
      assert.equal(loadPrefillSnapshot(), first, 'second read is the cached object');
    });
  });
});

test('the boundary snapshot is written after the tides with typed cells', async () => {
  await withScratchDir(async scratch => {
    fs.copyFileSync(path.join(FIXTURES, 'tide-1.json'), path.join(scratch, 'tide-1.json'));
    fs.writeFileSync(
      path.join(scratch, 'prefill-snapshot.json'),
      JSON.stringify({
        boundaryBlock: 99,
        exportedAt: 'x',
        source: 'test',
        tables: {
          UserIndex: {
            columns: [
              { name: 'id', type: 'text' },
              { name: 'points', type: 'bigint' },
              { name: 'active', type: 'bool' },
              { name: 'note', type: 'text' },
              { name: 'tags', type: 'textArray' },
            ],
            rowCount: 2,
            // 2^53 + 1 is the case JSON.parse would silently round if it were a bare literal.
            rows: [
              ['u1', '9007199254740993', true, null, ['a', 'b']],
              ['u2', '0', false, 'kept', []],
            ],
          },
        },
      })
    );
    const { context, stats } = recordingContext();
    const written: Record<string, unknown>[] = [];
    (context as unknown as Record<string, unknown>).UserIndex = {
      set: (row: Record<string, unknown>) => written.push(row),
    };
    await withPrefill(scratch, async () => {
      await prefillHistoricEpochsIfNeeded(context, AFTER_BOUNDARY);
    });
    assert.ok(stats.size > 0, 'the tide itself was written first');
    assert.deepEqual(written, [
      { id: 'u1', points: 9007199254740993n, active: true, note: undefined, tags: ['a', 'b'] },
      { id: 'u2', points: 0n, active: false, note: 'kept', tags: [] },
    ]);
  });
});

test('a snapshot table with no entity store is a schema mismatch and throws', async () => {
  await withScratchDir(async scratch => {
    fs.copyFileSync(path.join(FIXTURES, 'tide-1.json'), path.join(scratch, 'tide-1.json'));
    fs.writeFileSync(
      path.join(scratch, 'prefill-snapshot.json'),
      JSON.stringify({
        boundaryBlock: 99,
        exportedAt: 'x',
        source: 'test',
        tables: {
          NoSuchEntity: { columns: [{ name: 'id', type: 'text' }], rowCount: 1, rows: [['x']] },
        },
      })
    );
    const { context } = recordingContext();
    await withPrefill(scratch, async () => {
      await assert.rejects(
        () => prefillHistoricEpochsIfNeeded(context, AFTER_BOUNDARY),
        /prefill snapshot: no entity store for "NoSuchEntity"/
      );
    });
  });
});
