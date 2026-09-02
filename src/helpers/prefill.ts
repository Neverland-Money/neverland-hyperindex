/**
 * Prefill of settled historic Tides from `data/tide-<n>.json`.
 *
 * Tides 1-8 are settled and paid. Recomputing them is both wasteful and wrong: every
 * correctness fix since they were scored moves the numbers away from what was distributed
 * (see CHANGELOG.md). When `PREFILL_HISTORIC_EPOCHS` is on, whichever tide files exist are
 * written verbatim and the leaderboard does no settlement at all for the span they cover.
 *
 * Discovery is the file listing -- no separate config. Drop a tide file in, it is prefilled;
 * remove it, and that tide is indexed normally again.
 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';

import { EPOCH_DATES_OVERRIDES } from './constants';

import type { handlerContext, UserEpochStats } from '../../generated';

/** Read at call time, not module load, so a deployment (or a test) can flip it. */
export function isPrefillEnabled(): boolean {
  return process.env.PREFILL_HISTORIC_EPOCHS === 'true';
}

/**
 * Where the tide files live. Overridable so a container can mount them elsewhere.
 *
 * `envio start` launches the indexer from the generated subproject, so the process cwd is
 * `<repo>/generated`, not `<repo>`. Resolving `data` against cwd alone would therefore point at
 * `<repo>/generated/data`, which does not exist, and prefill would silently load nothing. Fall
 * back to the parent directory so the repo's own `data/` is found either way. Under test
 * `PREFILL_DATA_DIR` is always set, so the fallback only runs when a test unsets it on purpose.
 */
export function prefillDataDir(): string {
  const override = process.env.PREFILL_DATA_DIR;
  if (override !== undefined) return override;

  const fromCwd = path.resolve(process.cwd(), 'data');
  if (fs.existsSync(fromCwd)) return fromCwd;

  const fromParent = path.resolve(process.cwd(), '..', 'data');
  return fs.existsSync(fromParent) ? fromParent : fromCwd;
}

/**
 * Placeholder the test preload assigns to `PREFILL_DATA_DIR`. It is deliberately not a real
 * path: a test that reaches the loader without naming its own fixture directory trips the
 * guard below instead of silently reading whatever `prefillDataDir()` would default to.
 */
export const TEST_PREFILL_DIR_SENTINEL = '__NEVERLAND_TEST_PREFILL_DIR_MUST_BE_OVERRIDDEN__';

/**
 * Test-only tripwire; production is untouched because nothing there sets `NEVERLAND_TEST_ENV`.
 *
 * Under test, two resolutions are defects rather than fallbacks: the repository's production
 * `data/` (31 MB, 28,403 rows -- loading it into a test worker is what exhausted the host),
 * and the sentinel, which means prefill was reached without a fixture directory. Both fail
 * loudly here rather than at whatever the loader would have done next.
 */
function assertFixtureDirUnderTest(dir: string): void {
  if (process.env.NEVERLAND_TEST_ENV !== '1') return;
  if (dir === TEST_PREFILL_DIR_SENTINEL) {
    throw new Error(
      'prefill: PREFILL_DATA_DIR is still the test sentinel. A test that enables prefill must ' +
        'point it at its own fixture directory.'
    );
  }
  if (path.resolve(dir) === path.resolve(process.cwd(), 'data')) {
    throw new Error(
      'prefill: tests must not read the production data/ directory. Point PREFILL_DATA_DIR at ' +
        'a fixture directory.'
    );
  }
}

type PrefilledTide = {
  tide: number;
  epoch: Record<string, unknown>;
  userEpochStats: Record<string, unknown>[];
};

/** Fields stored as BigInt in the schema but serialized as JSON strings/numbers. */
export const BIGINT_FIELDS = new Set([
  'epochNumber',
  'depositPoints',
  'borrowPoints',
  'lpPoints',
  'dailySupplyPoints',
  'dailyBorrowPoints',
  'dailyRepayPoints',
  'dailyWithdrawPoints',
  'dailyVPPoints',
  'dailyLPPoints',
  'manualAwardPoints',
  'depositMultiplierBps',
  'borrowMultiplierBps',
  'vpMultiplierBps',
  'lpMultiplierBps',
  'depositPointsWithMultiplier',
  'borrowPointsWithMultiplier',
  'vpPointsWithMultiplier',
  'lpPointsWithMultiplier',
  'totalPoints',
  'totalPointsWithMultiplier',
  'totalMultiplierBps',
  'lastAppliedMultiplierBps',
  'testnetBonusBps',
  'duration',
  'startBlock',
  'endBlock',
]);

/**
 * `JSON.parse` reads a bare integer literal into a double, so every point total above 2^53
 * loses its low digits before `revive` can widen it. Quoting the literal first means the
 * BigInt is built from the exact decimal text on disk.
 *
 * Anchored on a known BigInt field name followed by a complete numeric value, so an address
 * or an id that merely contains digits is never rewritten.
 */
const BIGINT_LITERAL = new RegExp(
  `("(?:${[...BIGINT_FIELDS].join('|')})"\\s*:\\s*)(-?\\d+)(?=\\s*[,}])`,
  'g'
);

export function parseTideDocument(text: string): PrefilledTide {
  return JSON.parse(text.replace(BIGINT_LITERAL, '$1"$2"')) as PrefilledTide;
}

function revive(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    out[key] =
      BIGINT_FIELDS.has(key) && value !== null && value !== undefined
        ? BigInt(value as string)
        : value;
  }
  return out;
}

/**
 * Read a prefill artifact, gzipped or not.
 *
 * These files are delivered by `git pull` -- the deploy bind-mounts the repository into the
 * container -- so they have to live in git, and git history is append-only. Uncompressed they
 * are ~119 MB per regeneration and a tide closes monthly, which is ~1.4 GB/year that can never
 * be reclaimed. Gzipped the same set is ~17 MB. `.gz` is preferred and the plain file is still
 * accepted, so an existing deployment keeps working through the changeover.
 */
function readArtifact(file: string): string | null {
  for (const candidate of [`${file}.gz`, file]) {
    let buffer: Buffer;
    try {
      buffer = fs.readFileSync(candidate);
    } catch {
      continue;
    }
    return candidate.endsWith('.gz')
      ? zlib.gunzipSync(buffer).toString('utf8')
      : buffer.toString('utf8');
  }
  return null;
}

let cached: PrefilledTide[] | null = null;

/** Tide files present on disk, ascending. Read once and cached. */
export function loadPrefilledTides(): PrefilledTide[] {
  if (cached) return cached;
  const dir = prefillDataDir();
  assertFixtureDirUnderTest(dir);
  let names: string[] = [];
  try {
    names = fs.readdirSync(dir).filter(name => /^tide-\d+\.json(\.gz)?$/.test(name));
  } catch {
    // No data directory is a valid state: prefill simply has nothing to do.
    cached = [];
    return cached;
  }
  const tides = names
    .map(name => path.join(dir, name.replace(/\.gz$/, '')))
    // A directory holding both forms of the same tide yields the same document twice; the
    // dedupe below keeps one. readArtifact prefers .gz whenever both are present.
    .filter((file, index, all) => all.indexOf(file) === index)
    .map(file => {
      const text = readArtifact(file);
      if (text === null) throw new Error(`prefill: unreadable tide file ${file}`);
      return parseTideDocument(text);
    })
    .filter(doc => typeof doc.tide === 'number' && Array.isArray(doc.userEpochStats))
    .sort((left, right) => left.tide - right.tide);
  cached = tides;
  return cached;
}

/** Test seam: forget the cached read so a fixture directory can be picked up. */
export function resetPrefillCache(): void {
  cached = null;
  snapshotCache = undefined;
}

type SnapshotColumnType = 'bigint' | 'int' | 'float' | 'text' | 'bool' | 'textArray';

type SnapshotColumn = { readonly name: string; readonly type: SnapshotColumnType };

type SnapshotTable = {
  readonly columns: readonly SnapshotColumn[];
  readonly rowCount: number;
  readonly rows: readonly (readonly (string | number | boolean | string[] | null)[])[];
};

export type PrefillSnapshot = {
  readonly boundaryBlock: number;
  readonly exportedAt: string;
  readonly source: string;
  readonly tables: Readonly<Record<string, SnapshotTable>>;
};

/** `undefined` = not read yet, `null` = read and absent. Absent is a valid state. */
let snapshotCache: PrefillSnapshot | null | undefined;

/**
 * The boundary image written by `scripts/export-snapshot.ts`, if present.
 *
 * Plain `JSON.parse` is safe here, unlike the tide files: export-snapshot.ts writes every
 * `numeric` column as a QUOTED string, so the file contains no bare integer literal that could
 * lose its low digits above 2^53. Ints, floats and booleans are bare on purpose -- they are
 * exact as doubles, and Postgres emits shortest round-trip text for doubles.
 */
export function loadPrefillSnapshot(): PrefillSnapshot | null {
  if (snapshotCache !== undefined) return snapshotCache;
  const dir = prefillDataDir();
  // Same tripwire as loadPrefilledTides: the production snapshot is far larger than the tide
  // files, so a test that reached it would repeat the memory exhaustion those files caused.
  assertFixtureDirUnderTest(dir);
  const text = readArtifact(path.join(dir, 'prefill-snapshot.json'));
  if (text === null) {
    snapshotCache = null;
    return snapshotCache;
  }
  snapshotCache = JSON.parse(text) as PrefillSnapshot;
  return snapshotCache;
}

/**
 * Exclusive upper bound of the prefilled span: accrual is skipped for any timestamp
 * strictly below it. 0 when nothing is prefilled.
 *
 * Deliberately the START of the first tide that is NOT prefilled, not the END of the last
 * one that is. Tides do not abut -- Tide 8 ends 04:00 and Tide 9 opens 05:00 -- and gap
 * settlements landing in that hour are credited to the last CLOSED tide. Bounding at the
 * end of Tide 8 leaves that hour unguarded, and the gap accrual lands on top of the
 * prefilled figures. Measured: 1,415 of 2,673 Tide 8 rows inflated that way.
 *
 * A timestamp is the right key: every accrual site carries `event.block.timestamp`, and one
 * signal cannot disagree with itself the way a parallel block-number check could.
 */
export function prefilledBeforeTimestamp(): number {
  if (!isPrefillEnabled()) return 0;
  const tides = loadPrefilledTides();
  if (tides.length === 0) return 0;

  const lastTide = Math.max(...tides.map(doc => doc.tide));
  const nextStart = EPOCH_DATES_OVERRIDES[String(lastTide + 1)]?.startTime;
  if (nextStart) return nextStart;

  // No declared start for the next tide: fall back to just past the last known end, which
  // still covers every tide but leaves the trailing gap unguarded. Prefer an override.
  let end = 0;
  for (const doc of tides) {
    const value = Number(doc.epoch?.endTime ?? 0);
    if (value > end) end = value;
  }
  return end > 0 ? end + 1 : 0;
}

/** Whether `timestamp` falls inside the prefilled span, so no points may accrue for it. */
export function isPrefilledTimestamp(timestamp: number): boolean {
  const before = prefilledBeforeTimestamp();
  return before > 0 && timestamp < before;
}

/**
 * Whether `epochNumber` was written from disk, so its rows are settled and immutable.
 *
 * Keyed on the tide being credited rather than on the event timestamp, because settlement
 * walks backwards: an event in Tide 9 still credits time held during Tide 8, and a timestamp
 * gate never sees it. Measured before this guard, with the timestamp gate already in place:
 * 1,418 of 2,673 Tide 8 rows inflated by up to 92%, and 100 Tide 1 rows carrying their
 * manual award twice.
 */
export function isPrefilledEpoch(epochNumber: bigint | number): boolean {
  if (!isPrefillEnabled()) return false;
  const tide = Number(epochNumber);
  return loadPrefilledTides().some(doc => doc.tide === tide);
}

/**
 * The one write path for `UserEpochStats`. A write onto a prefilled tide is dropped rather
 * than layered on top of the figures from disk; `prefillHistoricEpochsIfNeeded` seeds those
 * rows directly and is the only writer that bypasses this.
 */
export function setUserEpochStats(context: handlerContext, stats: UserEpochStats): void {
  if (isPrefilledEpoch(stats.epochNumber)) return;
  context.UserEpochStats.set(stats);
}

/**
 * Writes the prefilled image once, AT THE BOUNDARY -- the first event at or after the start of
 * the first non-prefilled Tide.
 *
 * Applying it at the first indexed event instead does not survive: the image describes the state
 * the prefilled span ENDS in, and the ~67M blocks of that span are then processed on top of it.
 * Suppression keeps points out of the closed Tides, but it does not stop every writer -- voting
 * power refreshes, bootstrap seeding and multiplier snapshots all still run -- so the imported
 * rows drift as the span replays. Measured that way: LeaderboardTotals `global` came out holding
 * a Tide-1 timestamp (1767562070 / 2271 users) instead of the boundary's (1787889670 / 2033).
 *
 * Writing at the boundary makes the handoff exact by construction: whatever the span produced is
 * overwritten by the image a full recomputation would have arrived at, and the live span starts
 * from there.
 */
export async function prefillHistoricEpochsIfNeeded(
  context: handlerContext,
  timestamp: number
): Promise<void> {
  // Writes are no-ops during preload, but without this the loop below still revives every
  // row into throwaway objects on the preload pass as well as the ordered one. `isPreload`
  // is private to handlers/shared.ts, which already imports this module, so the check is
  // inlined to avoid a cycle -- identical to shared.ts:204-206, and to the same inline test
  // already used at leaderboardKeeper.ts:132 and lpGrowth.ts:407.
  if (context.isPreload === true) return;
  if (!isPrefillEnabled()) return;
  const tides = loadPrefilledTides();
  if (tides.length === 0) return;

  // Still inside the prefilled span: nothing to write yet.
  const boundary = prefilledBeforeTimestamp();
  if (boundary > 0 && timestamp < boundary) return;

  if (await isPrefillAlreadyApplied(context, tides)) return;

  for (const doc of tides) {
    context.LeaderboardEpoch.set(revive(doc.epoch) as never);
    for (const row of doc.userEpochStats) {
      context.UserEpochStats.set(revive(row) as never);
    }
  }

  writePrefillSnapshot(context);
}

/**
 * Has the image already been written?
 *
 * The marker has to be something the prefilled span provably cannot produce on its own, which
 * rules out the obvious candidates. A LeaderboardEpoch row is built from the chain as the span
 * replays -- by the boundary it already matches the file, so testing it (by existence OR by
 * value) reports "applied" and suppresses the write entirely.
 *
 * A UserEpochStats row for a prefilled Tide is the one thing that cannot appear by accident:
 * `setUserEpochStats` refuses every write whose epoch is prefilled, so throughout the span the
 * prefilled Tides stay empty. Its presence therefore means precisely "the image was written",
 * and it stays true afterwards because those rows are never rewritten. A restart mid-live-span
 * finds it and skips, instead of stamping boundary state over live progress.
 */
async function isPrefillAlreadyApplied(
  context: handlerContext,
  tides: readonly PrefilledTide[]
): Promise<boolean> {
  for (const doc of tides) {
    const first = doc.userEpochStats[0];
    if (!first) continue;
    const id = first.id as string;
    if (await context.UserEpochStats.get(id)) return true;
  }
  // Every prefilled tide is empty of stats -- nothing has been written yet.
  return false;
}

/**
 * Entities settlement writes that the per-tide files do not carry.
 *
 * The tide files hold the two entities a Tide is SCORED into. Settlement writes ten more --
 * lifetime totals, per-epoch bucket indices, score buckets, daily activity, per-epoch totals --
 * and prefill suppresses settlement across the prefilled span, so nothing else recreates them.
 * Without this a prefilled database silently diverges from a full sync: measured at UserIndex
 * -82%, UserPoints -64%, UserDailyActivity -47%, LeaderboardTotals absent for Tides 2-8.
 *
 * The snapshot is a boundary image, not a per-tide slice: the cumulative entities have no epoch
 * column to filter on, so they can only be captured as of the last block of the prefilled span.
 * `data/prefill-snapshot.json` is optional -- absent, prefill restores only the tide files, the
 * behaviour that shipped before this existed.
 */
function writePrefillSnapshot(context: handlerContext): void {
  const snapshot = loadPrefillSnapshot();
  if (!snapshot) return;

  for (const [table, dump] of Object.entries(snapshot.tables)) {
    const store = (
      context as unknown as Record<string, { set(entity: unknown): void } | undefined>
    )[table];
    // A table in the snapshot with no entity store means the export came from a different schema
    // version. Skipping it silently would reintroduce the drift this snapshot exists to remove.
    if (!store || typeof store.set !== 'function') {
      throw new Error(`prefill snapshot: no entity store for "${table}"`);
    }
    for (const row of dump.rows) {
      const entity: Record<string, unknown> = {};
      dump.columns.forEach((column, index) => {
        const value = row[index];
        // Envio reads an absent optional field as undefined; null would be written through.
        entity[column.name] =
          value === null ? undefined : column.type === 'bigint' ? BigInt(value as string) : value;
      });
      store.set(entity);
    }
  }
}
