#!/usr/bin/env tsx
/**
 * Leaderboard parity check: localhost (this branch) vs production index.
 *
 * Confirms the new code reproduces prod's leaderboard data while reporting the
 * observed LP implementation drift across the V2 -> Balancer -> V2 eras:
 *   - Uniswap V2: block 56,436,798 until block 78,741,015.
 *   - Balancer AutoRange: block 78,741,015 until block 87,190,222.
 *   - Uniswap V2 resumed: block 87,190,222 onward.
 *   - PartnerNFT double-count fix: 4 static collections; latent in prod.
 *   - Special editions: NONE expected (no special-edition NFTs fed on-chain yet,
 *     so specialEditionMultiplier is neutral 10000 and contributes 0 points).
 *
 * Three checks, cleanest signal first:
 *   1. MULTIPLIER COMPONENTS (votingPower / vpMultiplier / vpTierIndex /
 *      nftCount / nftMultiplier / combinedMultiplier) must MATCH prod exactly.
 *      vp* mismatches are regressions (or a too-late start_block pin). nft*
 *      mismatches are likely the PartnerNFT fix (verify the user holds a static
 *      collection). combinedMultiplier must match because, with neutral special
 *      editions, (nft*10000*vp)/(10000*10000) === (nft*vp)/10000 exactly.
 *   2. PRE-BALANCER EPOCHS: EpochLeaderboardStats for epochs whose endBlock is
 *      below the Balancer start must match at the precision exposed by each endpoint.
 *   3. LP DRIFT REVIEW: compare lifetime drift with the aggregate raw lpPoints drift
 *      from UserEpochStats. Current pool-position counters are diagnostic only: they
 *      are cumulative and cannot attribute V2 points to its original vs resumed era.
 *
 * Read-only. Run AFTER localhost has synced to head. Exits non-zero if any
 * regression-class mismatch is found.
 *
 *   LOCAL_GRAPHQL_URL  (default http://localhost:8080/v1/graphql)
 *   PROD_GRAPHQL_URL   (default https://index.neverland.money/v1/graphql)
 *   LOCAL_ADMIN_SECRET / PROD_ADMIN_SECRET  (sent as x-hasura-admin-secret if set)
 */

import {
  LP_BALANCER_AUTORANGE_CUTOVER_BLOCK,
  LP_V2_CUTOVER_BLOCK,
  LP_V2_RESUME_CUTOVER_BLOCK,
  STATIC_NFT_COLLECTION_ADDRESSES,
} from '../src/helpers/constants';

type Endpoint = { name: string; url: string; secret: string };
type BigIntScalar = string | number;

const LOCAL: Endpoint = {
  name: 'local',
  url: process.env.LOCAL_GRAPHQL_URL || 'http://localhost:8080/v1/graphql',
  secret: process.env.LOCAL_ADMIN_SECRET || '',
};
const PROD: Endpoint = {
  name: 'prod',
  url: process.env.PROD_GRAPHQL_URL || 'https://index.neverland.money/v1/graphql',
  secret: process.env.PROD_ADMIN_SECRET || '', // prod allows anonymous select (same as the public app)
};

const SAMPLE = 10; // how many example mismatches to print per bucket

interface ULS {
  id: string;
  user_id: string;
  votingPower: BigIntScalar;
  vpTierIndex: BigIntScalar | null;
  vpMultiplier: BigIntScalar;
  nftCount: BigIntScalar;
  nftMultiplier: BigIntScalar;
  combinedMultiplier: BigIntScalar;
  lifetimePoints: BigIntScalar;
  lastUpdate: number;
}
interface UserEpochLP {
  id: string;
  user_id: string;
  lpPoints: BigIntScalar;
}
interface Epoch {
  id: string;
  epochNumber: BigIntScalar;
  endBlock: BigIntScalar | null;
}
interface EpochStats {
  id: string;
  epoch_id: string;
  totalUsers: BigIntScalar;
  totalPoints: BigIntScalar;
  topUserAddress: string | null;
  topUserPoints: BigIntScalar | null;
}

async function gql<T>(ep: Endpoint, query: string): Promise<T> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (ep.secret) headers['x-hasura-admin-secret'] = ep.secret;
  const res = await fetch(ep.url, { method: 'POST', headers, body: JSON.stringify({ query }) });
  if (!res.ok) throw new Error(`${ep.name} HTTP ${res.status} (${ep.url})`);
  const json = (await res.json()) as { data?: T; errors?: unknown };
  if (json.errors) throw new Error(`${ep.name} GraphQL error: ${JSON.stringify(json.errors)}`);
  if (!json.data) throw new Error(`${ep.name} returned no data`);
  return json.data;
}

// Paginate an entity by ascending id cursor (no _aggregate needed; works on both roles).
async function fetchAll<T extends { id: string }>(
  ep: Endpoint,
  entity: string,
  fields: string,
  extraWhere = ''
): Promise<T[]> {
  const PAGE = 1000;
  const out: T[] = [];
  let cursor = '';
  for (;;) {
    const filters = [`id: { _gt: ${JSON.stringify(cursor)} }`];
    if (extraWhere) filters.push(extraWhere);
    const q = `query { ${entity}(where: { ${filters.join(', ')} }, order_by: { id: asc }, limit: ${PAGE}) { ${fields} } }`;
    const data = await gql<Record<string, T[]>>(ep, q);
    const rows = data[entity];
    out.push(...rows);
    if (rows.length < PAGE) break;
    cursor = rows[rows.length - 1].id;
  }
  return out;
}

async function maxLastUpdate(ep: Endpoint): Promise<number> {
  const q = `query { UserLeaderboardState(order_by: { lastUpdate: desc }, limit: 1) { lastUpdate } }`;
  const d = await gql<{ UserLeaderboardState: { lastUpdate: number }[] }>(ep, q);
  return d.UserLeaderboardState[0]?.lastUpdate ?? 0;
}

function pct(n: number, total: number): string {
  return total === 0 ? '0%' : `${((100 * n) / total).toFixed(2)}%`;
}

// prod's Hasura serializes BigInt as JSON numbers (lossy beyond 2^53); local serializes
// them as strings (exact). Compare numerically so both collapse to the same value —
// exact for the small fields (multipliers, counts) and matching prod's own float
// precision for the large ones (votingPower). Treat null/undefined as equal only to each other.
function eqNum(
  a: string | number | null | undefined,
  b: string | number | null | undefined
): boolean {
  if (a == null || b == null) return (a == null) === (b == null);
  return Number(a) === Number(b);
}

function toBigInt(value: BigIntScalar): bigint {
  if (typeof value === 'string') return BigInt(value);
  if (!Number.isFinite(value) || !Number.isInteger(value)) {
    throw new Error(`Invalid BigInt scalar: ${String(value)}`);
  }
  // Public Hasura may serialize BigInt as a lossy JSON number. This conversion is
  // suitable for diagnostics, but its residual must not become a hard regression.
  return BigInt(value);
}

function sumEpochLpByUser(rows: UserEpochLP[]): Map<string, bigint> {
  const sums = new Map<string, bigint>();
  for (const row of rows) {
    const user = row.user_id.toLowerCase();
    sums.set(user, (sums.get(user) ?? 0n) + toBigInt(row.lpPoints));
  }
  return sums;
}

function absBigInt(value: bigint): bigint {
  return value < 0n ? -value : value;
}

async function main(): Promise<void> {
  const nftFix = new Set(STATIC_NFT_COLLECTION_ADDRESSES.map(a => a.toLowerCase()));
  void nftFix; // referenced in guidance below; per-user NFT-ownership join is left to manual drill-down
  let regressions = 0;

  console.log('Leaderboard parity: local vs prod');
  console.log(`  local: ${LOCAL.url}`);
  console.log(`  prod:  ${PROD.url}`);
  console.log('  LP points eras:');
  console.log(`    V2: [${LP_V2_CUTOVER_BLOCK}, ${LP_BALANCER_AUTORANGE_CUTOVER_BLOCK})`);
  console.log(
    `    Balancer: [${LP_BALANCER_AUTORANGE_CUTOVER_BLOCK}, ${LP_V2_RESUME_CUTOVER_BLOCK})`
  );
  console.log(`    V2 resumed: [${LP_V2_RESUME_CUTOVER_BLOCK}, head)\n`);
  console.log(
    '  NOTE: entity pagination uses live id-cursor reads, not a block-pinned snapshot.\n'
  );

  // ---- 0. Sync guard: comparison is only meaningful when local has caught up ----
  const [localTip, prodTip] = await Promise.all([maxLastUpdate(LOCAL), maxLastUpdate(PROD)]);
  const tipDeltaSec = localTip - prodTip;
  const lagSec = Math.abs(tipDeltaSec);
  const caughtUp = lagSec <= 600;
  console.log(
    `Activity-watermark guard: local ${localTip} vs prod ${prodTip} (delta ${tipDeltaSec}s)`
  );
  if (!caughtUp) {
    console.log(
      `  WARNING: endpoint activity watermarks differ by ${(lagSec / 60).toFixed(0)} min. Results below are INFORMATIONAL ONLY;\n` +
        `  mismatches are not asserted as regressions until localhost reaches head.\n`
    );
  } else {
    console.log(
      '  OK: activity watermarks are within 10 min. This does not prove every LP position is equally settled.\n'
    );
  }

  // ---- Load both leaderboards ----
  const fields =
    'id user_id votingPower vpTierIndex vpMultiplier nftCount nftMultiplier combinedMultiplier lifetimePoints lastUpdate';
  const [localRows, prodRows] = await Promise.all([
    fetchAll<ULS>(LOCAL, 'UserLeaderboardState', fields),
    fetchAll<ULS>(PROD, 'UserLeaderboardState', fields),
  ]);
  const localBy = new Map(localRows.map(r => [r.user_id.toLowerCase(), r]));
  const prodBy = new Map(prodRows.map(r => [r.user_id.toLowerCase(), r]));
  console.log(`Users: local ${localRows.length}, prod ${prodRows.length}`);
  const onlyLocal = [...localBy.keys()].filter(u => !prodBy.has(u));
  const onlyProd = [...prodBy.keys()].filter(u => !localBy.has(u));
  if (onlyLocal.length)
    console.log(
      `  local-only users: ${onlyLocal.length} (new; e.g. ${onlyLocal.slice(0, 3).join(', ')})`
    );
  if (onlyProd.length) {
    console.log(
      `  prod-only users: ${onlyProd.length}  <-- ${caughtUp ? 'REGRESSION: local dropped users present in prod' : '(expected while behind head)'}`
    );
    console.log(`    e.g. ${onlyProd.slice(0, SAMPLE).join(', ')}`);
    if (caughtUp) regressions += onlyProd.length;
  }
  console.log('');

  // ---- 1. Multiplier-component parity (clean regression signal) ----
  const vpMiss: string[] = [];
  const nftMiss: string[] = [];
  const combMiss: string[] = [];
  let shared = 0;
  for (const [user, l] of localBy) {
    const p = prodBy.get(user);
    if (!p) continue;
    shared++;
    if (
      !eqNum(l.votingPower, p.votingPower) ||
      !eqNum(l.vpMultiplier, p.vpMultiplier) ||
      !eqNum(l.vpTierIndex, p.vpTierIndex)
    )
      vpMiss.push(
        `${user} vp ${p.votingPower}->${l.votingPower} mult ${p.vpMultiplier}->${l.vpMultiplier}`
      );
    if (!eqNum(l.nftCount, p.nftCount) || !eqNum(l.nftMultiplier, p.nftMultiplier))
      nftMiss.push(
        `${user} nft ${p.nftCount}/${p.nftMultiplier} -> ${l.nftCount}/${l.nftMultiplier}`
      );
    if (!eqNum(l.combinedMultiplier, p.combinedMultiplier))
      combMiss.push(`${user} combined ${p.combinedMultiplier} -> ${l.combinedMultiplier}`);
  }
  console.log(`1) Multiplier components over ${shared} shared users:`);
  const report = (
    label: string,
    arr: string[],
    isRegression: boolean,
    denominator: number = shared
  ) => {
    const tag =
      arr.length === 0 ? 'OK' : !caughtUp ? 'info' : isRegression ? 'REGRESSION' : 'review';
    console.log(`   ${label}: ${arr.length} mismatches (${pct(arr.length, denominator)}) [${tag}]`);
    arr.slice(0, SAMPLE).forEach(s => console.log(`      ${s}`));
    if (isRegression && caughtUp) regressions += arr.length;
  };
  report('votingPower / vpMultiplier (must match)', vpMiss, true);
  report(
    'nftCount / nftMultiplier (PartnerNFT-fix expected on 4 static collections; rest = regression)',
    nftMiss,
    false
  );
  report('combinedMultiplier (must match while special editions unfed)', combMiss, true);
  console.log('');

  // ---- 2. Pre-Balancer epoch match (zero LP-source drift expected) ----
  const [lEpochs, pEpochs, lStats, pStats] = await Promise.all([
    fetchAll<Epoch>(LOCAL, 'LeaderboardEpoch', 'id epochNumber endBlock'),
    fetchAll<Epoch>(PROD, 'LeaderboardEpoch', 'id epochNumber endBlock'),
    fetchAll<EpochStats>(
      LOCAL,
      'EpochLeaderboardStats',
      'id epoch_id totalUsers totalPoints topUserAddress topUserPoints'
    ),
    fetchAll<EpochStats>(
      PROD,
      'EpochLeaderboardStats',
      'id epoch_id totalUsers totalPoints topUserAddress topUserPoints'
    ),
  ]);
  const preBalancer = (epochs: Epoch[]) =>
    new Set(
      epochs
        .filter(
          e =>
            e.endBlock != null && toBigInt(e.endBlock) < BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK)
        )
        .map(e => e.id)
    );
  const preL = preBalancer(lEpochs);
  const preP = preBalancer(pEpochs);
  const pStatsBy = new Map(pStats.map(s => [s.epoch_id, s]));
  const epochMiss: string[] = [];
  let preChecked = 0;
  for (const s of lStats) {
    if (!preL.has(s.epoch_id) || !preP.has(s.epoch_id)) continue;
    const p = pStatsBy.get(s.epoch_id);
    if (!p) continue;
    preChecked++;
    if (
      !eqNum(s.totalUsers, p.totalUsers) ||
      !eqNum(s.totalPoints, p.totalPoints) ||
      !eqNum(s.topUserPoints, p.topUserPoints)
    )
      epochMiss.push(
        `${s.epoch_id} users ${p.totalUsers}->${s.totalUsers} pts ${p.totalPoints}->${s.totalPoints}`
      );
  }
  console.log(
    `2) Pre-Balancer epochs (endBlock < ${LP_BALANCER_AUTORANGE_CUTOVER_BLOCK}): ${preChecked} compared`
  );
  report(
    '   EpochLeaderboardStats (must match at endpoint precision)',
    epochMiss,
    true,
    preChecked
  );
  console.log('');

  // ---- 3. Aggregate LP drift review ----
  // UserEpochStats.lpPoints is the authoritative aggregate LP counter exposed by the
  // schema. Current UserLPPosition counters are not used for attribution: they are
  // cumulative, and V2's position spans both of its active eras.
  const [localEpochLp, prodEpochLp] = await Promise.all([
    fetchAll<UserEpochLP>(LOCAL, 'UserEpochStats', 'id user_id lpPoints'),
    fetchAll<UserEpochLP>(PROD, 'UserEpochStats', 'id user_id lpPoints'),
  ]);
  const localLpByUser = sumEpochLpByUser(localEpochLp);
  const prodLpByUser = sumEpochLpByUser(prodEpochLp);
  const hasLossyScalars =
    localRows.some(row => typeof row.lifetimePoints === 'number') ||
    prodRows.some(row => typeof row.lifetimePoints === 'number') ||
    localEpochLp.some(row => typeof row.lpPoints === 'number') ||
    prodEpochLp.some(row => typeof row.lpPoints === 'number');
  const residuals: {
    user: string;
    lifetimeDrift: bigint;
    aggregateLpDrift: bigint;
    residual: bigint;
  }[] = [];
  for (const [user, l] of localBy) {
    const p = prodBy.get(user);
    if (!p) continue;
    const lifetimeDrift = toBigInt(l.lifetimePoints) - toBigInt(p.lifetimePoints);
    const aggregateLpDrift = (localLpByUser.get(user) ?? 0n) - (prodLpByUser.get(user) ?? 0n);
    const residual = lifetimeDrift - aggregateLpDrift;
    if (residual !== 0n) {
      residuals.push({ user, lifetimeDrift, aggregateLpDrift, residual });
    }
  }
  residuals.sort((a, b) => {
    const aAbs = absBigInt(a.residual);
    const bAbs = absBigInt(b.residual);
    return aAbs === bAbs ? 0 : aAbs > bAbs ? -1 : 1;
  });
  console.log('3) Aggregate LP drift review (source-agnostic UserEpochStats.lpPoints):');
  console.log(
    `   scalar precision: ${hasLossyScalars ? 'approximate — at least one endpoint returned JSON numbers' : 'exact strings'}`
  );
  console.log(
    '   current pool-position counters are diagnostic only and are not used for era attribution.'
  );
  console.log(
    `   users with non-zero lifetimeDrift - aggregateLpDrift: ${residuals.length} [${residuals.length ? 'REVIEW' : 'OK'}]`
  );
  residuals
    .slice(0, SAMPLE)
    .forEach(row =>
      console.log(
        `      ${row.user} lifetimeDrift=${row.lifetimeDrift} aggregateLpDrift=${row.aggregateLpDrift} residual=${row.residual}`
      )
    );
  console.log(
    '   LP residuals are review-only: endpoint serialization and live, unpinned reads prevent a reliable hard gate.'
  );
  console.log('');

  if (!caughtUp) {
    console.log(
      'INFORMATIONAL ONLY: localhost is behind head, so nothing is asserted. Re-run once synced.'
    );
    process.exit(0);
  }
  console.log(
    regressions === 0
      ? `NO HARD REGRESSIONS. LP review items: ${residuals.length}.`
      : `FOUND ${regressions} hard regression-class mismatch(es). Investigate above. LP review items: ${residuals.length}.`
  );
  process.exit(regressions === 0 ? 0 : 1);
}

main().catch(e => {
  console.error('parity check failed:', e instanceof Error ? e.message : e);
  process.exit(2);
});
