#!/usr/bin/env tsx
/**
 * Snapshot + diff cumulative leaderboard points, to validate that the keeper
 * backfill gate (ENVIO_LEADERBOARD_LIVE_EPOCH) is a no-op on final points.
 *
 * Stored points are SETTLEMENT SNAPSHOTS, not continuous accrual: a user's
 * `UserPoints` row only changes when THAT user is settled by an event. So two
 * indexers stopped a few blocks apart still produce identical points for every
 * user who had no event in that gap -- i.e. nearly all of them. Exact block
 * alignment is NOT required; an `end_block` is optional, only for a perfectly
 * frozen A/B with zero gap-window drift.
 *
 * A/B usage (gate-on vs gate-off locally, or local vs a prod dump):
 *   1. Gate OFF (ground truth): comment ENVIO_LEADERBOARD_LIVE_EPOCH out of .env,
 *      `pnpm dev:restart`, wait for synced, then:
 *        npx tsx scripts/snapshot-leaderboard-points.ts dump /tmp/points-off.json
 *   2. Gate ON: set ENVIO_LEADERBOARD_LIVE_EPOCH=6 in .env, `pnpm dev:restart`,
 *      wait for synced, then:
 *        npx tsx scripts/snapshot-leaderboard-points.ts dump /tmp/points-on.json
 *   3. npx tsx scripts/snapshot-leaderboard-points.ts diff /tmp/points-off.json /tmp/points-on.json
 *      -> "gate is a verified no-op" means cumulative points match. A few users
 *         settled in the block gap between the runs are expected drift, not a gate
 *         effect -- the diff names them so you can tell (a keeper batch in the gap
 *         shows up as a cluster). The category split tells you WHAT moved:
 *         lifetimeDeposit/Borrow are reserve points that resample the point-in-time
 *         multiplier (the only ones the gate could move); the lifetimeDaily* fields
 *         are activity-gated and must never move.
 *   Optional: add `end_block: <N>` under `- id: 143` in config.yaml for both runs
 *   (a closed-epoch boundary is cleanest) to remove even the gap-window drift.
 *
 * `dump` reads GRAPHQL_URL (default http://localhost:8080/v1/graphql) and optional
 * ADMIN_SECRET; point it at a separate prod dump to compare local vs prod at head.
 *
 * Read-only. `diff` exits non-zero if any user/category diverges.
 */

import { readFileSync, writeFileSync } from 'node:fs';

const GRAPHQL_URL = process.env.GRAPHQL_URL || 'http://localhost:8080/v1/graphql';
const ADMIN_SECRET = process.env.ADMIN_SECRET || '';
const SAMPLE = 10; // example mismatches printed per category
const BLOCK_GAP_WARN = 100; // blocks; below this a head-vs-head gap is immaterial

// Cumulative (cross-epoch) point fields on UserPoints. lifetimeDeposit/Borrow are
// reserve points (point-in-time multiplier) -- the only ones the gate could move;
// the lifetimeDaily* fields are activity-gated and must stay identical.
const POINT_FIELDS = [
  'lifetimeTotalPoints',
  'lifetimeDepositPoints',
  'lifetimeBorrowPoints',
  'lifetimeDailySupplyPoints',
  'lifetimeDailyBorrowPoints',
  'lifetimeDailyRepayPoints',
  'lifetimeDailyWithdrawPoints',
  'lifetimeDailyVPPoints',
] as const;

type PointField = (typeof POINT_FIELDS)[number];
type Row = { id: string; user_id: string } & Record<PointField, string | number>;
interface Snapshot {
  source: string;
  processedBlock: number | null;
  count: number;
  rows: Row[];
}

async function gql<T>(query: string): Promise<T> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (ADMIN_SECRET) headers['x-hasura-admin-secret'] = ADMIN_SECRET;
  const res = await fetch(GRAPHQL_URL, {
    method: 'POST',
    headers,
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} (${GRAPHQL_URL})`);
  const json = (await res.json()) as { data?: T; errors?: unknown };
  if (json.errors) throw new Error(`GraphQL error: ${JSON.stringify(json.errors)}`);
  if (!json.data) throw new Error('no data');
  return json.data;
}

// Best-effort: Envio exposes chain_metadata; null if the role can't read it.
async function fetchProcessedBlock(): Promise<number | null> {
  try {
    const d = await gql<{ chain_metadata: { latest_processed_block: number | string | null }[] }>(
      `query { chain_metadata(order_by: { latest_processed_block: desc }, limit: 1) { latest_processed_block } }`
    );
    const v = d.chain_metadata?.[0]?.latest_processed_block;
    return v == null ? null : Number(v);
  } catch {
    return null;
  }
}

async function fetchAll(): Promise<Row[]> {
  const PAGE = 1000;
  const fields = `id user_id ${POINT_FIELDS.join(' ')}`;
  const out: Row[] = [];
  let cursor = '';
  for (;;) {
    const q = `query { UserPoints(where: { id: { _gt: ${JSON.stringify(cursor)} } }, order_by: { id: asc }, limit: ${PAGE}) { ${fields} } }`;
    const d = await gql<{ UserPoints: Row[] }>(q);
    const rows = d.UserPoints;
    out.push(...rows);
    if (rows.length < PAGE) break;
    cursor = rows[rows.length - 1].id;
  }
  return out;
}

// Exact when both sides serialize the same way (the A/B is local-vs-local, both
// exact strings); falls back to numeric only for a prod endpoint's lossy floats.
function samePoints(a: string | number, b: string | number): boolean {
  if (String(a) === String(b)) return true;
  return Number(a) === Number(b);
}

async function dump(file: string): Promise<void> {
  const [rows, processedBlock] = await Promise.all([fetchAll(), fetchProcessedBlock()]);
  rows.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const snap: Snapshot = { source: GRAPHQL_URL, processedBlock, count: rows.length, rows };
  writeFileSync(file, JSON.stringify(snap));
  const at = processedBlock == null ? '' : ` @ block ${processedBlock}`;
  console.log(`dumped ${rows.length} users from ${GRAPHQL_URL}${at} -> ${file}`);
}

function load(file: string): Snapshot {
  return JSON.parse(readFileSync(file, 'utf8')) as Snapshot;
}

function diff(fileA: string, fileB: string): void {
  const a = load(fileA);
  const b = load(fileB);
  const atA = a.processedBlock == null ? '' : ` @ block ${a.processedBlock}`;
  const atB = b.processedBlock == null ? '' : ` @ block ${b.processedBlock}`;
  console.log(`A ${fileA}: ${a.count} users${atA}  (${a.source})`);
  console.log(`B ${fileB}: ${b.count} users${atB}  (${b.source})`);
  const blockGap =
    a.processedBlock != null && b.processedBlock != null
      ? Math.abs(a.processedBlock - b.processedBlock)
      : null;
  if (blockGap != null && blockGap > BLOCK_GAP_WARN) {
    console.log(
      `\n  NOTE: snapshots are ${blockGap} blocks apart. Only users settled in that gap ` +
        `differ (a keeper batch in the gap shows up as a cluster); re-dump closer or add ` +
        `end_block to both runs to remove it. Small gaps are immaterial.`
    );
  }

  const mapA = new Map(a.rows.map(r => [r.user_id.toLowerCase(), r]));
  const mapB = new Map(b.rows.map(r => [r.user_id.toLowerCase(), r]));

  const onlyA = [...mapA.keys()].filter(u => !mapB.has(u));
  const onlyB = [...mapB.keys()].filter(u => !mapA.has(u));
  if (onlyA.length)
    console.log(`\n  ${onlyA.length} users only in A (e.g. ${onlyA.slice(0, SAMPLE).join(', ')})`);
  if (onlyB.length)
    console.log(`  ${onlyB.length} users only in B (e.g. ${onlyB.slice(0, SAMPLE).join(', ')})`);

  const perField: Record<PointField, string[]> = Object.fromEntries(
    POINT_FIELDS.map(f => [f, [] as string[]])
  ) as Record<PointField, string[]>;
  let shared = 0;
  let usersWithAnyDiff = 0;
  for (const [u, ra] of mapA) {
    const rb = mapB.get(u);
    if (!rb) continue;
    shared++;
    let any = false;
    for (const f of POINT_FIELDS) {
      if (!samePoints(ra[f], rb[f])) {
        perField[f].push(`${u}: ${ra[f]} -> ${rb[f]} (delta ${Number(rb[f]) - Number(ra[f])})`);
        any = true;
      }
    }
    if (any) usersWithAnyDiff++;
  }

  console.log(`\nshared users: ${shared}`);
  let totalFieldMismatches = 0;
  for (const f of POINT_FIELDS) {
    const m = perField[f];
    totalFieldMismatches += m.length;
    const tag =
      f === 'lifetimeDepositPoints' || f === 'lifetimeBorrowPoints'
        ? '  (reserve, multiplier-sensitive)'
        : '';
    console.log(`  ${f}: ${m.length} mismatches${tag}`);
    m.slice(0, SAMPLE).forEach(s => console.log(`      ${s}`));
  }

  const clean = totalFieldMismatches === 0 && onlyA.length === 0 && onlyB.length === 0;
  console.log(
    `\n${clean ? 'PASS' : 'FAIL'}: ${usersWithAnyDiff} users differ across ${totalFieldMismatches} field-values` +
      (clean ? ' -- gate is a verified no-op on cumulative points' : '')
  );
  process.exit(clean ? 0 : 1);
}

const [mode, ...rest] = process.argv.slice(2);
if (mode === 'dump' && rest[0]) {
  dump(rest[0]).catch((e: unknown) => {
    console.error(e instanceof Error ? e.message : e);
    process.exit(2);
  });
} else if (mode === 'diff' && rest[0] && rest[1]) {
  diff(rest[0], rest[1]);
} else {
  console.error('usage:');
  console.error('  GRAPHQL_URL=... npx tsx scripts/snapshot-leaderboard-points.ts dump <file>');
  console.error('  npx tsx scripts/snapshot-leaderboard-points.ts diff <fileA> <fileB>');
  process.exit(2);
}
