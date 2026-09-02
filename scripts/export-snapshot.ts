#!/usr/bin/env tsx
/**
 * Export the leaderboard state a full recomputation leaves behind at the prefill boundary.
 *
 * `export-tides.ts` writes the two entities a Tide is *scored* into -- LeaderboardEpoch and
 * UserEpochStats. Settlement writes ten more, and prefill suppresses settlement across the
 * prefilled span, so without this snapshot those ten are never rebuilt: a prefilled database
 * ends up missing lifetime totals, per-epoch bucket indices, daily activity and per-epoch
 * totals that a full sync would have. Measured against a full sync before this existed:
 * UserIndex -82%, UserPoints -64%, UserDailyActivity -47%, LeaderboardTotals absent for
 * Tides 2-8.
 *
 * The snapshot is taken AT THE BOUNDARY, not at head. Cumulative entities (UserPoints,
 * UserLeaderboardState, ...) have no epoch column to slice on, so a snapshot taken later
 * would carry live-Tide accrual into the prefilled state. Sync the source database with
 * `end_block` set to the last block of the prefilled span, so the database IS the handoff.
 *
 * Precision rules match export-tides.ts: every column is read as text through
 * `COPY ... FORMAT csv`, and `numeric` columns are written as QUOTED strings. A bare JSON
 * number loses its low digits above 2^53 the moment anything calls JSON.parse.
 *
 *   pnpm run export:snapshot -- --boundary-block 99809559
 *   pnpm run export:snapshot -- --boundary-block 99809559 --docker neverland-postgres
 */
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';

import { parseCsv } from './export-tides';

/**
 * Entities settlement writes that `export-tides.ts` does not cover. Every one of these is
 * produced by a code path prefill suppresses, so each must be restored from disk or a prefilled
 * database silently diverges from a full sync.
 */
export const SNAPSHOT_TABLES = [
  'UserIndex',
  'LeaderboardTotals',
  'ScoreBucket',
  'UserEpochFinalization',
  'UserDailyActivity',
  'UserPoints',
  'UserReservePoints',
  'UserLeaderboardState',
  'UserMultiplierSnapshot',
  'UserSpecialEditionAggregate',
  // Chain-derived entities that settlement ALSO writes. They are rebuilt from events during the
  // span, but settlement's contribution to them is suppressed, so they end the span in a
  // different state than a full recomputation. Verified by A/B diff: each of these was the only
  // remaining class of drift once the leaderboard entities above were restored.
  'User',
  'UserReserveList',
  'UserLPPosition',
  'UserLPEpochCursor',
  'UserLPStats',
  'LPPoolFeeStats',
  'PriceOracleAsset',
] as const;

export type ColumnType = 'bigint' | 'int' | 'float' | 'text' | 'bool' | 'textArray';

export interface Column {
  readonly name: string;
  readonly type: ColumnType;
}

/**
 * Postgres type -> encoding. Envio maps BigInt! to `numeric`, Int! to `integer`, Float to
 * `double precision`, String to `text`, Boolean to `boolean`. Typing the columns from the
 * catalog rather than from a hardcoded field-name list is what lets this scale past the two
 * tables export-tides.ts special-cases.
 */
export function classify(dataType: string, elementType: string | null): ColumnType {
  if (dataType === 'ARRAY') {
    if (elementType && elementType !== 'text') {
      throw new Error(`unsupported array element type: ${elementType}`);
    }
    return 'textArray';
  }
  if (dataType === 'numeric') return 'bigint';
  if (dataType === 'integer' || dataType === 'bigint' || dataType === 'smallint') return 'int';
  if (dataType === 'double precision' || dataType === 'real') return 'float';
  if (dataType === 'boolean') return 'bool';
  return 'text';
}

/** `\N` marks SQL NULL so an empty text value stays distinguishable from a missing one. */
export function buildSnapshotCopySql(table: string, columns: readonly Column[]): string {
  const selected = columns.map(c => `"${c.name}"::text`).join(',');
  return `copy (select ${selected} from "${table}" order by "id") to stdout with (format csv, null '\\N')`;
}

/** Postgres array text (`{a,b,"c,d"}`) -> string[]. */
export function parsePgArray(text: string): string[] {
  if (text === '{}') return [];
  const inner = text.slice(1, -1);
  const out: string[] = [];
  let field = '';
  let quoted = false;
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (quoted) {
      if (ch === '\\') {
        field += inner[++i] ?? '';
      } else if (ch === '"') {
        quoted = false;
      } else field += ch;
      continue;
    }
    if (ch === '"') quoted = true;
    else if (ch === ',') {
      out.push(field);
      field = '';
    } else field += ch;
  }
  out.push(field);
  return out;
}

/**
 * One cell, as the JSON text it should appear as. BigInts are quoted; ints and floats stay bare
 * numbers (Postgres emits shortest round-trip text for doubles under extra_float_digits=1);
 * NULL becomes `null`.
 */
export function encodeCell(value: string | null, type: ColumnType): string {
  if (value === null) return 'null';
  switch (type) {
    case 'bigint':
      return JSON.stringify(value);
    case 'int':
    case 'float':
      return value;
    case 'bool':
      return value === 't' || value === 'true' ? 'true' : 'false';
    case 'textArray':
      return JSON.stringify(parsePgArray(value));
    default:
      return JSON.stringify(value);
  }
}

export interface TableDump {
  readonly columns: readonly Column[];
  readonly rows: readonly (string | null)[][];
}

/**
 * Rows are arrays, not objects: at ~100k rows for UserDailyActivity and UserMultiplierSnapshot,
 * repeating the key names per row would multiply the file size for no gain. `columns` carries
 * the order and the types.
 */
export function encodeSnapshot(
  tables: Readonly<Record<string, TableDump>>,
  boundaryBlock: number,
  lastTide: number,
  exportedAt: string
): string {
  const parts: string[] = [];
  parts.push('{');
  parts.push(`  "boundaryBlock": ${boundaryBlock},`);
  // prefill refuses to apply the image unless this matches the last tide file present, so an
  // end-of-Tide-N snapshot can never be stamped after a different tide has been removed or added.
  parts.push(`  "lastTide": ${lastTide},`);
  parts.push(`  "exportedAt": ${JSON.stringify(exportedAt)},`);
  parts.push('  "source": "full resync with PREFILL_HISTORIC_EPOCHS=false, stopped at end_block",');
  parts.push('  "tables": {');
  const names = Object.keys(tables);
  names.forEach((name, ti) => {
    const dump = tables[name];
    parts.push(`    ${JSON.stringify(name)}: {`);
    parts.push(
      `      "columns": [${dump.columns.map(c => `{"name":${JSON.stringify(c.name)},"type":${JSON.stringify(c.type)}}`).join(',')}],`
    );
    parts.push(`      "rowCount": ${dump.rows.length},`);
    if (dump.rows.length === 0) {
      parts.push('      "rows": []');
    } else {
      parts.push('      "rows": [');
      dump.rows.forEach((row, ri) => {
        const cells = dump.columns.map((c, ci) => encodeCell(row[ci] ?? null, c.type));
        parts.push(`        [${cells.join(',')}]${ri === dump.rows.length - 1 ? '' : ','}`);
      });
      parts.push('      ]');
    }
    parts.push(`    }${ti === names.length - 1 ? '' : ','}`);
  });
  parts.push('  }');
  parts.push('}');
  return parts.join('\n') + '\n';
}

function psql(sql: string, container?: string): string {
  const args = container
    ? ['exec', '-i', container, 'psql', '-U', 'postgres', '-d', 'envio', '-tAc', sql]
    : ['-U', 'postgres', '-d', 'envio', '-tAc', sql];
  return execFileSync(container ? 'docker' : 'psql', args, {
    encoding: 'utf8',
    maxBuffer: 1024 * 1024 * 512,
  });
}

function describeTable(table: string, container?: string): Column[] {
  const sql = `select c.column_name || '|' || c.data_type || '|' || coalesce(e.data_type,'')
    from information_schema.columns c
    left join information_schema.element_types e
      on e.object_catalog = c.table_catalog and e.object_schema = c.table_schema
     and e.object_name = c.table_name and e.object_type = 'TABLE'
     and e.collection_type_identifier = c.dtd_identifier
    where c.table_schema = 'public' and c.table_name = '${table}'
    order by c.ordinal_position`;
  const lines = psql(sql, container)
    .split('\n')
    .map(l => l.trim())
    .filter(Boolean);
  if (!lines.length) throw new Error(`table ${table} not found`);
  return lines.map(line => {
    const [name, dataType, elementType] = line.split('|');
    return { name, type: classify(dataType, elementType || null) };
  });
}

function main(): void {
  const argv = process.argv.slice(2);
  const at = (flag: string) => {
    const i = argv.indexOf(flag);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  const boundaryRaw = at('--boundary-block');
  if (!boundaryRaw) throw new Error('--boundary-block is required');
  const boundaryBlock = Number(boundaryRaw);
  if (!Number.isInteger(boundaryBlock)) throw new Error('--boundary-block must be an integer');
  const container = at('--docker');
  const dir = at('--out') ?? path.resolve(process.cwd(), 'data');
  const stamp = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');

  // `--boundary-block` is only a label written into the file; the DATA comes from whatever the
  // database currently holds. Exporting from a database that has run past the boundary produces
  // a file that CLAIMS to be a boundary image while carrying live-Tide rows -- the cumulative
  // tables have no epoch column, so nothing downstream can detect it. Refuse outright unless the
  // indexer stopped exactly where the caller says it did.
  const progress = psql(
    `select coalesce((select "progressBlock"::text from "_meta" limit 1), '')`,
    container
  ).trim();
  if (progress === '') {
    throw new Error('cannot read _meta.progressBlock; is this an indexed database?');
  }
  if (Number(progress) !== boundaryBlock) {
    throw new Error(
      `database is at block ${progress} but --boundary-block says ${boundaryBlock}. ` +
        'Sync with PREFILL_HISTORIC_EPOCHS=false and end_block set to the boundary, then export.'
    );
  }

  const tables: Record<string, TableDump> = {};
  for (const table of SNAPSHOT_TABLES) {
    const columns = describeTable(table, container);
    const csv = psql(buildSnapshotCopySql(table, columns), container);
    const rows = parseCsv(csv).map(cells =>
      columns.map((_, i) => {
        const cell = cells[i];
        return cell === undefined || cell === '\\N' ? null : cell;
      })
    );
    tables[table] = { columns, rows };
    console.log(`${table}: ${rows.length} rows, ${columns.length} columns`);
  }

  // Gzipped: these artifacts ship through git (the deploy bind-mounts the repo), history is
  // append-only, and a tide closes monthly. Raw that is ~1.4 GB/year of unreclaimable history;
  // compressed it is ~200 MB/year. prefill reads .gz in preference to the plain file.
  const file = path.join(dir, 'prefill-snapshot.json.gz');
  // The image is an end-of-Tide-N state; record N so prefill can refuse a mismatched tide set.
  const lastTideRaw = psql(
    `select coalesce(max("epochNumber"), 0)::text from "LeaderboardEpoch" where not "isActive"`,
    container
  ).trim();
  const lastTide = Number(lastTideRaw);
  if (!Number.isInteger(lastTide) || lastTide < 1) {
    throw new Error(`cannot determine the last settled tide (got "${lastTideRaw}")`);
  }
  const json = encodeSnapshot(tables, boundaryBlock, lastTide, stamp);
  fs.writeFileSync(file, zlib.gzipSync(Buffer.from(json, 'utf8'), { level: 9 }));
  const bytes = fs.statSync(file).size;
  console.log(
    `wrote ${file} (${(bytes / 1024 / 1024).toFixed(1)} MB gz, ` +
      `${(Buffer.byteLength(json) / 1024 / 1024).toFixed(1)} MB raw, boundary block ${boundaryBlock}, last tide ${lastTide})`
  );
  // A stale uncompressed copy would still be read when the .gz is removed, so drop it.
  const plain = path.join(dir, 'prefill-snapshot.json');
  if (fs.existsSync(plain)) {
    fs.rmSync(plain);
    console.log(`removed stale ${plain}`);
  }
}

if (process.argv[1] && process.argv[1].endsWith('export-snapshot.ts')) main();
