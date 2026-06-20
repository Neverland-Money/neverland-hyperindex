#!/usr/bin/env tsx
/**
 * Leaderboard parity check: localhost (this branch) vs production index.
 *
 * Confirms the new code reproduces prod's leaderboard data, EXCEPT for the
 * drift we know about:
 *   - Balancer AutoRange LP points: new LP source, live from block 78,741,015.
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
 *   2. PRE-CUTOVER EPOCHS: EpochLeaderboardStats for epochs whose endBlock is
 *      below the Balancer cutover must MATCH exactly -- zero expected drift, so
 *      any difference is a pure regression.
 *   3. POINTS RECONCILIATION: per user, (local.lifetimePoints - prod.lifetimePoints)
 *      should equal that user's Balancer LP contribution (sum of settledLpPoints
 *      over their Balancer-pool positions). A non-zero residual is unexplained
 *      drift -> a regression. (Only trustworthy once BOTH sides are settled at head.)
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
  BALANCER_AUTORANGE_V3_POOL_ADDRESS,
  STATIC_NFT_COLLECTION_ADDRESSES,
} from '../src/helpers/constants';

type Endpoint = { name: string; url: string; secret: string };

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
  votingPower: string;
  vpTierIndex: string | null;
  vpMultiplier: string;
  nftCount: string;
  nftMultiplier: string;
  combinedMultiplier: string;
  lifetimePoints: string;
  lastUpdate: number;
}
interface LPPos {
  id: string;
  user_id: string;
  pool: string;
  settledLpPoints: string;
}
interface Epoch {
  id: string;
  epochNumber: string;
  endBlock: string | null;
}
interface EpochStats {
  id: string;
  epoch_id: string;
  totalUsers: string;
  totalPoints: string;
  topUserAddress: string | null;
  topUserPoints: string | null;
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
function eqNum(a: string | number | null | undefined, b: string | number | null | undefined): boolean {
  if (a == null || b == null) return (a == null) === (b == null);
  return Number(a) === Number(b);
}

async function main(): Promise<void> {
  const balancerPool = BALANCER_AUTORANGE_V3_POOL_ADDRESS.toLowerCase();
  const nftFix = new Set(STATIC_NFT_COLLECTION_ADDRESSES.map((a) => a.toLowerCase()));
  void nftFix; // referenced in guidance below; per-user NFT-ownership join is left to manual drill-down
  let regressions = 0;

  console.log('Leaderboard parity: local vs prod');
  console.log(`  local: ${LOCAL.url}`);
  console.log(`  prod:  ${PROD.url}`);
  console.log(`  Balancer pool: ${balancerPool}  cutover block: ${LP_BALANCER_AUTORANGE_CUTOVER_BLOCK}\n`);

  // ---- 0. Sync guard: comparison is only meaningful when local has caught up ----
  const [localTip, prodTip] = await Promise.all([maxLastUpdate(LOCAL), maxLastUpdate(PROD)]);
  const lagSec = prodTip - localTip;
  const caughtUp = lagSec <= 600;
  console.log(`Sync guard: local tip ${localTip} vs prod tip ${prodTip} (lag ${lagSec}s)`);
  if (!caughtUp) {
    console.log(
      `  WARNING: local is ${(lagSec / 60).toFixed(0)} min behind prod. Results below are INFORMATIONAL ONLY;\n` +
        `  mismatches are not asserted as regressions until localhost reaches head.\n`
    );
  } else {
    console.log('  OK: tips within 10 min; mismatches are asserted as regressions.\n');
  }

  // ---- Load both leaderboards ----
  const fields =
    'id user_id votingPower vpTierIndex vpMultiplier nftCount nftMultiplier combinedMultiplier lifetimePoints lastUpdate';
  const [localRows, prodRows] = await Promise.all([
    fetchAll<ULS>(LOCAL, 'UserLeaderboardState', fields),
    fetchAll<ULS>(PROD, 'UserLeaderboardState', fields),
  ]);
  const localBy = new Map(localRows.map((r) => [r.user_id.toLowerCase(), r]));
  const prodBy = new Map(prodRows.map((r) => [r.user_id.toLowerCase(), r]));
  console.log(`Users: local ${localRows.length}, prod ${prodRows.length}`);
  const onlyLocal = [...localBy.keys()].filter((u) => !prodBy.has(u));
  const onlyProd = [...prodBy.keys()].filter((u) => !localBy.has(u));
  if (onlyLocal.length) console.log(`  local-only users: ${onlyLocal.length} (new; e.g. ${onlyLocal.slice(0, 3).join(', ')})`);
  if (onlyProd.length) {
    console.log(`  prod-only users: ${onlyProd.length}  <-- ${caughtUp ? 'REGRESSION: local dropped users present in prod' : '(expected while behind head)'}`);
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
    if (!eqNum(l.votingPower, p.votingPower) || !eqNum(l.vpMultiplier, p.vpMultiplier) || !eqNum(l.vpTierIndex, p.vpTierIndex))
      vpMiss.push(`${user} vp ${p.votingPower}->${l.votingPower} mult ${p.vpMultiplier}->${l.vpMultiplier}`);
    if (!eqNum(l.nftCount, p.nftCount) || !eqNum(l.nftMultiplier, p.nftMultiplier))
      nftMiss.push(`${user} nft ${p.nftCount}/${p.nftMultiplier} -> ${l.nftCount}/${l.nftMultiplier}`);
    if (!eqNum(l.combinedMultiplier, p.combinedMultiplier))
      combMiss.push(`${user} combined ${p.combinedMultiplier} -> ${l.combinedMultiplier}`);
  }
  console.log(`1) Multiplier components over ${shared} shared users:`);
  const report = (label: string, arr: string[], isRegression: boolean) => {
    const tag = arr.length === 0 ? 'OK' : !caughtUp ? 'info' : isRegression ? 'REGRESSION' : 'review';
    console.log(`   ${label}: ${arr.length} mismatches (${pct(arr.length, shared)}) [${tag}]`);
    arr.slice(0, SAMPLE).forEach((s) => console.log(`      ${s}`));
    if (isRegression && caughtUp) regressions += arr.length;
  };
  report('votingPower / vpMultiplier (must match)', vpMiss, true);
  report('nftCount / nftMultiplier (PartnerNFT-fix expected on 4 static collections; rest = regression)', nftMiss, false);
  report('combinedMultiplier (must match while special editions unfed)', combMiss, true);
  console.log('');

  // ---- 2. Pre-cutover epoch exact match (zero expected drift) ----
  const [lEpochs, pEpochs, lStats, pStats] = await Promise.all([
    fetchAll<Epoch>(LOCAL, 'LeaderboardEpoch', 'id epochNumber endBlock'),
    fetchAll<Epoch>(PROD, 'LeaderboardEpoch', 'id epochNumber endBlock'),
    fetchAll<EpochStats>(LOCAL, 'EpochLeaderboardStats', 'id epoch_id totalUsers totalPoints topUserAddress topUserPoints'),
    fetchAll<EpochStats>(PROD, 'EpochLeaderboardStats', 'id epoch_id totalUsers totalPoints topUserAddress topUserPoints'),
  ]);
  const preCutover = (epochs: Epoch[]) =>
    new Set(epochs.filter((e) => e.endBlock != null && BigInt(e.endBlock) < BigInt(LP_BALANCER_AUTORANGE_CUTOVER_BLOCK)).map((e) => e.id));
  const preL = preCutover(lEpochs);
  const preP = preCutover(pEpochs);
  const pStatsBy = new Map(pStats.map((s) => [s.epoch_id, s]));
  const epochMiss: string[] = [];
  let preChecked = 0;
  for (const s of lStats) {
    if (!preL.has(s.epoch_id) || !preP.has(s.epoch_id)) continue;
    const p = pStatsBy.get(s.epoch_id);
    if (!p) continue;
    preChecked++;
    if (s.totalUsers !== p.totalUsers || s.totalPoints !== p.totalPoints || (s.topUserPoints ?? '') !== (p.topUserPoints ?? ''))
      epochMiss.push(`${s.epoch_id} users ${p.totalUsers}->${s.totalUsers} pts ${p.totalPoints}->${s.totalPoints}`);
  }
  console.log(`2) Pre-cutover epochs (endBlock < ${LP_BALANCER_AUTORANGE_CUTOVER_BLOCK}): ${preChecked} compared`);
  report('   EpochLeaderboardStats (must match exactly)', epochMiss, true);
  console.log('');

  // ---- 3. Points reconciliation: drift must equal the Balancer LP contribution ----
  const balPos = await fetchAll<LPPos>(LOCAL, 'UserLPPosition', 'id user_id pool settledLpPoints', `pool: { _eq: ${JSON.stringify(balancerPool)} }`);
  const balByUser = new Map<string, bigint>();
  for (const pos of balPos) {
    const u = pos.user_id.toLowerCase();
    balByUser.set(u, (balByUser.get(u) ?? 0n) + BigInt(pos.settledLpPoints));
  }
  const residuals: { user: string; drift: bigint; balancer: bigint; residual: bigint }[] = [];
  for (const [user, l] of localBy) {
    const p = prodBy.get(user);
    if (!p) continue;
    const drift = BigInt(l.lifetimePoints) - BigInt(p.lifetimePoints);
    const bal = balByUser.get(user) ?? 0n;
    const residual = drift - bal;
    if (residual !== 0n) residuals.push({ user, drift, balancer: bal, residual });
  }
  residuals.sort((a, b) => (b.residual < 0n ? -b.residual : b.residual) > (a.residual < 0n ? -a.residual : a.residual) ? 1 : -1);
  console.log(`3) Points reconciliation (drift == Balancer LP contribution):`);
  console.log(`   Balancer LP positions: ${balPos.length} across ${balByUser.size} users`);
  console.log(`   users with non-zero residual: ${residuals.length} [${residuals.length ? 'review — unexplained drift' : 'OK'}]`);
  residuals.slice(0, SAMPLE).forEach((r) =>
    console.log(`      ${r.user} drift=${r.drift} balancer=${r.balancer} residual=${r.residual}`)
  );
  if (caughtUp) regressions += residuals.length; // only trust residuals when caught up + settled
  else console.log('   (residuals not counted as regressions while local is behind head)');
  console.log('');

  if (!caughtUp) {
    console.log('INFORMATIONAL ONLY: localhost is behind head, so nothing is asserted. Re-run once synced.');
    process.exit(0);
  }
  console.log(regressions === 0 ? 'PARITY OK: no regression-class mismatch.' : `FOUND ${regressions} regression-class mismatch(es). Investigate above.`);
  process.exit(regressions === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error('parity check failed:', e instanceof Error ? e.message : e);
  process.exit(2);
});
