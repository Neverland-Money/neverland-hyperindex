// Pins the operator settings (prefill off, fixture-only data dir) before any project
// module loads. See src/__tests__/test-env-preload.ts.
import './test-env-preload';

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import {
  EPOCH_FIELDS,
  STAT_FIELDS,
  buildCopySql,
  diffRows,
  encodeDocument,
  normalizeEpochRow,
  parseCsv,
  rowsToObjects,
} from '../../scripts/export-tides';
import { BIGINT_FIELDS, parseTideDocument } from '../helpers/prefill';

test('every column is cast to text so numeric never becomes a float', () => {
  const sql = buildCopySql('UserEpochStats', ['id', 'totalPoints'], '"epochNumber" = 8');
  assert.match(sql, /"id"::text,"totalPoints"::text/);
  assert.match(sql, /format csv/);
  assert.match(sql, /order by "id"/, 'stable ordering keeps exports byte-comparable');
});

test('csv reading handles quotes, embedded commas and doubled quotes', () => {
  const rows = parseCsv('a,b\n"x,y",2\n"he said ""hi""",3\n');
  assert.deepEqual(rows, [
    ['a', 'b'],
    ['x,y', '2'],
    ['he said "hi"', '3'],
  ]);
});

test('a trailing row without a newline is not dropped', () => {
  assert.deepEqual(parseCsv('1,2\n3,4'), [
    ['1', '2'],
    ['3', '4'],
  ]);
});

test('point totals above 2^53 are written as quoted strings', () => {
  const epoch: Record<string, string> = Object.fromEntries(EPOCH_FIELDS.map(f => [f, '0']));
  epoch.id = '8';
  epoch.isActive = 'f';
  const stat: Record<string, string> = Object.fromEntries(STAT_FIELDS.map(f => [f, '0']));
  stat.id = '0xabc:8';
  stat.user_id = '0xabc';
  stat.totalPoints = '172866260298447819119';
  stat.rank = '1';

  const text = encodeDocument(8, epoch, [stat], '2026-08-31T00:00:00Z');

  assert.match(text, /"totalPoints": "172866260298447819119"/, 'quoted, so nothing rounds it');
  assert.match(text, /"rank": 1/, 'Int fields stay JSON numbers');
  assert.match(text, /"isActive": false/, 'booleans stay booleans');

  // The decisive property: reading it back through JSON.parse must not lose a digit.
  const parsed = JSON.parse(text) as { userEpochStats: { totalPoints: string }[] };
  assert.equal(BigInt(parsed.userEpochStats[0].totalPoints), 172866260298447819119n);
});

test('an empty column becomes null rather than an empty string', () => {
  const epoch: Record<string, string> = Object.fromEntries(EPOCH_FIELDS.map(f => [f, '0']));
  epoch.endTime = '';
  const text = encodeDocument(3, epoch, [], '2026-08-31T00:00:00Z');
  assert.match(text, /"endTime": null/);
});

test('rowsToObjects pads a short row rather than shifting columns', () => {
  assert.deepEqual(rowsToObjects([['a']], ['x', 'y']), [{ x: 'a', y: '' }]);
});

test('diffing reports mismatches, absences and extras', () => {
  const base = Object.fromEntries(STAT_FIELDS.map(f => [f, '0']));
  const db = [
    { ...base, id: 'a', totalPoints: '1' },
    { ...base, id: 'b' },
  ];
  const file = [
    { ...base, id: 'a', totalPoints: '2' },
    { ...base, id: 'c' },
  ];
  const diffs = diffRows(db, file);
  assert.ok(diffs.some(d => d.includes('a.totalPoints: db=1 file=2')));
  assert.ok(diffs.some(d => d === 'b: missing from file'));
  assert.ok(diffs.some(d => d === 'c: only in file'));
});

test('the epoch record verifies field for field across the two representations', () => {
  // Database side: COPY text (booleans t/f, digit strings, NULL as empty). File side: what
  // parseTideDocument yields (booleans, numbers, quoted BigInts, null). Same record, no diff.
  const db = {
    duration: '2548800',
    endBlock: '99797722',
    endTime: '1787889600',
    epochNumber: '8',
    id: '8',
    isActive: 'f',
    scheduledEndTime: '1787889600',
    scheduledStartTime: '1785340800',
    startBlock: '91000000',
    startTime: '1785340800',
  };
  const file: Record<string, unknown> = {
    duration: 2548800,
    endBlock: '99797722',
    endTime: 1787889600,
    epochNumber: '8',
    id: '8',
    isActive: false,
    scheduledEndTime: 1787889600,
    scheduledStartTime: 1785340800,
    startBlock: '91000000',
    startTime: 1785340800,
  };
  assert.deepEqual(diffRows([normalizeEpochRow(db)], [normalizeEpochRow(file)], EPOCH_FIELDS), []);
  // A wrong endTime in the artifact is exactly what a stats-only verify let through.
  const drifted = normalizeEpochRow({ ...file, endTime: 1787889601, isActive: true });
  const diffs = diffRows([normalizeEpochRow(db)], [drifted], EPOCH_FIELDS);
  assert.ok(
    diffs.some(d => d === '8.endTime: db=1787889600 file=1787889601'),
    diffs.join('; ')
  );
  assert.ok(
    diffs.some(d => d === '8.isActive: db=false file=true'),
    diffs.join('; ')
  );
  // NULL in the database and null in the file are the same absence.
  assert.deepEqual(
    diffRows(
      [normalizeEpochRow({ ...db, duration: '' })],
      [normalizeEpochRow({ ...file, duration: null })],
      EPOCH_FIELDS
    ),
    []
  );
});

test('identical data diffs to nothing', () => {
  const base = Object.fromEntries(STAT_FIELDS.map(f => [f, '0']));
  const rows = [{ ...base, id: 'a', totalPoints: '172866260298447819119' }];
  assert.deepEqual(diffRows(rows, [...rows]), []);
});

test('the exporter and prefill agree on which fields are BigInt', () => {
  // Drift pin. If schema.graphql gains or loses a BigInt on either entity, the reader
  // (prefill.ts BIGINT_FIELDS) and this test disagree, and the export silently changes shape.
  const schema = fs.readFileSync(path.resolve(process.cwd(), 'schema.graphql'), 'utf8');
  const fromSchema = new Set<string>();
  for (const typeName of ['UserEpochStats', 'LeaderboardEpoch']) {
    const start = schema.indexOf(`type ${typeName} {`);
    assert.notEqual(start, -1, `${typeName} missing from schema.graphql`);
    const body = schema.slice(start, schema.indexOf('\n}', start));
    for (const line of body.split('\n')) {
      const m = line.match(/^\s*(\w+):\s*BigInt!?/);
      if (m) fromSchema.add(m[1]);
    }
  }
  const missing = [...fromSchema].filter(f => !BIGINT_FIELDS.has(f)).sort();
  assert.deepEqual(missing, [], 'schema has BigInt fields prefill.ts would read as numbers');
});

test('the verifier reads bare integer literals losslessly', () => {
  // Regression pin for a real defect: the verifier once used plain `JSON.parse`, which turns
  // a bare integer literal into a double and reports a spurious difference for every value
  // above 2^53. The committed data/tide-*.json files are in exactly that bare-literal shape,
  // so this is the form the verifier must survive. (Documents this exporter writes quote
  // their BigInts, so they are safe either way -- the hazard is the legacy shape on disk.)
  const legacy = [
    '{',
    '  "tide": 8,',
    '  "epoch": { "id": "8", "epochNumber": 8, "startTime": 0, "endTime": 1, "isActive": false },',
    '  "userEpochStats": [',
    '    { "id": "0xabc:8", "user_id": "0xabc", "epochNumber": 8,',
    '      "totalPoints": 3180359290417267449298,',
    '      "depositPoints": 172866260298447819119 }',
    '  ]',
    '}',
  ].join('\n');

  const exact = parseTideDocument(legacy) as unknown as {
    userEpochStats: Record<string, unknown>[];
  };
  assert.equal(BigInt(exact.userEpochStats[0].totalPoints as string), 3180359290417267449298n);
  assert.equal(BigInt(exact.userEpochStats[0].depositPoints as string), 172866260298447819119n);

  // A naive read of the same bytes loses digits -- which is what made the pin necessary.
  const lossy = JSON.parse(legacy) as { userEpochStats: Record<string, number>[] };
  assert.notEqual(String(lossy.userEpochStats[0].totalPoints), '3180359290417267449298');
});
