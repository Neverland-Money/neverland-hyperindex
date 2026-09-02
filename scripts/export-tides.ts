#!/usr/bin/env tsx
/**
 * Export settled Tides to `data/tide-<n>.json`, the files `PREFILL_HISTORIC_EPOCHS` reads.
 *
 * Precision is the whole point. A Tide's point totals exceed 2^53, so every step has to stay
 * exact: Postgres `numeric` is read as text via `COPY ... FORMAT csv`, and every BigInt field
 * is written as a QUOTED JSON string. A bare JSON number would be rounded the moment anything
 * parsed it with `JSON.parse` -- which is exactly the defect this exporter exists to avoid.
 *
 * Only export from a database synced with `PREFILL_HISTORIC_EPOCHS=false`. A prefilled
 * database's Tide rows are a copy of these files, so exporting from one is circular.
 *
 *   pnpm run export:tides -- --tides 1-8            # write data/tide-1..8.json
 *   pnpm run export:tides -- --tides 1-8 --verify   # compare, write nothing
 *   pnpm run export:tides -- --tides 9 --docker nvl-head-pg
 */
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';

import { BIGINT_FIELDS, parseTideDocument } from '../src/helpers/prefill';

/** UserEpochStats column order, mirroring schema.graphql. `user_id` is derived from `id`. */
export const STAT_FIELDS = [
  'id',
  'user_id',
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
  'lastSupplyPointsDay',
  'lastBorrowPointsDay',
  'lastRepayPointsDay',
  'lastWithdrawPointsDay',
  'lastVPPointsDay',
  'lastVPAccrualTimestamp',
  'totalPoints',
  'totalPointsWithMultiplier',
  'totalMultiplierBps',
  'lastAppliedMultiplierBps',
  'testnetBonusBps',
  'rank',
  'firstSeenAt',
  'lastUpdatedAt',
] as const;

export const EPOCH_FIELDS = [
  'duration',
  'endBlock',
  'endTime',
  'epochNumber',
  'id',
  'isActive',
  'scheduledEndTime',
  'scheduledStartTime',
  'startBlock',
  'startTime',
] as const;

/** Int fields that must stay JSON numbers; everything else in BIGINT_FIELDS is quoted. */
const NUMERIC_JSON_FIELDS = new Set([
  'lastSupplyPointsDay',
  'lastBorrowPointsDay',
  'lastRepayPointsDay',
  'lastWithdrawPointsDay',
  'lastVPPointsDay',
  'lastVPAccrualTimestamp',
  'rank',
  'firstSeenAt',
  'lastUpdatedAt',
  'endTime',
  'scheduledEndTime',
  'scheduledStartTime',
  'startTime',
]);

/** Every column is cast to text so `numeric` never round-trips through a float. */
export function buildCopySql(table: string, columns: readonly string[], where: string): string {
  const selected = columns.map(c => `"${c}"::text`).join(',');
  return `copy (select ${selected} from "${table}" where ${where} order by "id") to stdout with (format csv)`;
}

/** Minimal RFC4180 reader. psql quotes any field containing a comma, quote or newline. */
export function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = '';
  let quoted = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (quoted) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          quoted = false;
        }
      } else field += ch;
      continue;
    }
    if (ch === '"') quoted = true;
    else if (ch === ',') {
      row.push(field);
      field = '';
    } else if (ch === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
    } else if (ch !== '\r') field += ch;
  }
  if (field !== '' || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

export type Row = Record<string, string>;

export function rowsToObjects(rows: string[][], columns: readonly string[]): Row[] {
  return rows.map(cells => Object.fromEntries(columns.map((c, i) => [c, cells[i] ?? ''])));
}

/**
 * Serializes one tide. BigInt fields become quoted strings; Int fields stay numbers; `isActive`
 * stays a boolean. Nothing is parsed into a JS number on the way, so nothing can round.
 */
export function encodeDocument(tide: number, epoch: Row, stats: Row[], exportedAt: string): string {
  const value = (field: string, raw: string): string => {
    if (raw === '') return 'null';
    if (field === 'isActive') return raw === 't' || raw === 'true' ? 'true' : 'false';
    if (NUMERIC_JSON_FIELDS.has(field)) return raw;
    if (BIGINT_FIELDS.has(field)) return JSON.stringify(raw);
    return JSON.stringify(raw);
  };
  const obj = (fields: readonly string[], row: Row, indent: string): string =>
    `{\n${fields.map(f => `${indent}  ${JSON.stringify(f)}: ${value(f, row[f] ?? '')}`).join(',\n')}\n${indent}}`;

  return [
    '{',
    `  "tide": ${tide},`,
    `  "epoch": ${obj(EPOCH_FIELDS, epoch, '  ')},`,
    `  "fields": ${JSON.stringify(STAT_FIELDS)},`,
    `  "totalEntries": ${stats.length},`,
    '  "userEpochStats": [',
    stats.map(r => `    ${obj(STAT_FIELDS, r, '    ')}`).join(',\n'),
    '  ],',
    `  "exportedAt": ${JSON.stringify(exportedAt)},`,
    '  "source": "full sync, PREFILL_HISTORIC_EPOCHS=false"',
    '}',
    '',
  ].join('\n');
}

/** Field-level differences between a freshly read set of rows and a file already on disk. */
/**
 * One epoch record as comparable text. The database side arrives from `COPY ... csv` (booleans
 * as `t`/`f`, NULL as empty, numerics as digit strings); the file side arrives from
 * `parseTideDocument` (booleans, numbers, quoted BigInts, null). Both collapse to the same
 * strings here so `diffRows` can compare them field for field.
 */
export function normalizeEpochRow(row: Record<string, unknown>): Row {
  const flat: Row = {};
  for (const f of EPOCH_FIELDS) {
    const v = row[f];
    if (v === null || v === undefined || v === '') flat[f] = '';
    else if (f === 'isActive') flat[f] = v === true || v === 't' || v === 'true' ? 'true' : 'false';
    else flat[f] = String(v);
  }
  return flat;
}

export function diffRows(
  fresh: Row[],
  stored: Row[],
  fields: readonly string[] = STAT_FIELDS
): string[] {
  const byId = new Map(stored.map(r => [r.id, r]));
  const out: string[] = [];
  for (const row of fresh) {
    const other = byId.get(row.id);
    if (!other) {
      out.push(`${row.id}: missing from file`);
      continue;
    }
    for (const f of fields) {
      if (String(row[f]) !== String(other[f])) {
        out.push(`${row.id}.${f}: db=${row[f]} file=${other[f]}`);
      }
    }
  }
  for (const row of stored)
    if (!fresh.some(r => r.id === row.id)) out.push(`${row.id}: only in file`);
  return out;
}

/* c8 ignore start -- I/O and CLI wiring; the pure functions above carry the tests. */

function psql(sql: string, container?: string): string {
  const args = container
    ? [
        'exec',
        container,
        'psql',
        '-U',
        process.env.ENVIO_PG_USER ?? 'postgres',
        '-d',
        process.env.ENVIO_PG_DATABASE ?? 'envio',
        '-tAc',
        sql,
      ]
    : ['-tAc', sql];
  return execFileSync(container ? 'docker' : 'psql', args, {
    encoding: 'utf8',
    maxBuffer: 512 * 1024 * 1024,
    env: process.env,
  });
}

function parseTideRange(spec: string): number[] {
  const [a, b] = spec.split('-').map(Number);
  return Number.isFinite(b) ? Array.from({ length: b - a + 1 }, (_, i) => a + i) : [a];
}

function main(): void {
  const argv = process.argv.slice(2);
  const at = (flag: string): string | undefined => {
    const i = argv.indexOf(flag);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  const tides = parseTideRange(at('--tides') ?? '1-8');
  const container = at('--docker');
  const verify = argv.includes('--verify');
  const dir = at('--out') ?? path.resolve(process.cwd(), 'data');
  const stamp = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');

  let failures = 0;
  for (const tide of tides) {
    const epochRows = rowsToObjects(
      parseCsv(
        psql(buildCopySql('LeaderboardEpoch', EPOCH_FIELDS, `"epochNumber" = ${tide}`), container)
      ),
      EPOCH_FIELDS
    );
    if (!epochRows.length) throw new Error(`tide ${tide}: no LeaderboardEpoch row`);

    const statCols = STAT_FIELDS.filter(f => f !== 'user_id');
    const stats = rowsToObjects(
      parseCsv(
        psql(buildCopySql('UserEpochStats', statCols, `"epochNumber" = ${tide}`), container)
      ),
      statCols
    ).map(r => ({ ...r, user_id: r.id.split(':')[0] }));

    const file = path.join(dir, `tide-${tide}.json`);
    if (verify) {
      // Prefer the gzipped artifact, accept the plain one, exactly as prefill does.
      const gz = `${file}.gz`;
      const source = fs.existsSync(gz) ? gz : file;
      if (!fs.existsSync(source)) {
        console.error(`tide ${tide}: ${file}[.gz] missing`);
        failures++;
        continue;
      }
      const raw = fs.readFileSync(source);
      const text = source.endsWith('.gz')
        ? zlib.gunzipSync(raw).toString('utf8')
        : raw.toString('utf8');
      // MUST use the lossless reader. `JSON.parse` turns the file's bare integer literals
      // into doubles, which reports spurious differences for every value above 2^53 -- the
      // precise defect this exporter exists to prevent.
      const stored = parseTideDocument(text) as unknown as {
        epoch: Record<string, unknown>;
        userEpochStats: Row[];
      };
      // The epoch record is load-bearing (prefill writes it and reads its endTime as the
      // coverage bound), so verify covers it field for field, not only the stats rows.
      const diffs = [
        ...diffRows(
          [normalizeEpochRow(epochRows[0])],
          [normalizeEpochRow(stored.epoch)],
          EPOCH_FIELDS
        ).map(d => `epoch ${d}`),
        ...diffRows(
          stats,
          stored.userEpochStats.map(r => {
            const flat: Row = {};
            for (const f of STAT_FIELDS) flat[f] = String((r as Record<string, unknown>)[f] ?? '');
            return flat;
          })
        ),
      ];
      if (diffs.length) {
        failures++;
        console.error(`tide ${tide}: ${diffs.length} differences`);
        for (const d of diffs.slice(0, 10)) console.error(`  ${d}`);
      } else {
        console.log(`tide ${tide}: ${stats.length} rows match exactly`);
      }
    } else {
      // Gzipped for the same reason as the snapshot: these ship through git history.
      const json = encodeDocument(tide, epochRows[0], stats, stamp);
      fs.writeFileSync(`${file}.gz`, zlib.gzipSync(Buffer.from(json, 'utf8'), { level: 9 }));
      if (fs.existsSync(file)) fs.rmSync(file);
      console.log(`tide ${tide}: ${stats.length} rows -> ${file}.gz`);
    }
  }
  if (failures) process.exit(1);
}

if (process.argv[1] && process.argv[1].endsWith('export-tides.ts')) main();

/* c8 ignore stop */
