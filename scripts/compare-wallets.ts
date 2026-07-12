#!/usr/bin/env tsx
/**
 * Per-wallet local-vs-prod cross-check across every dimension that feeds leaderboard
 * points: per-epoch points (the 6 epochs span the whole chain, so this IS the
 * "spread across blocks" view), lifetime points, veDUST (locks + synced voting power),
 * lending positions (UserReserve), and LP positions (UserLPPosition).
 *
 * Goal: diagnose local-vs-prod differences without assuming either endpoint's deployed
 * LP implementation. LP points moved V2 -> Balancer -> V2; per-epoch LP fields are
 * aggregate across pools, while position counters are cumulative rather than era-scoped.
 *
 *   LOCAL_GRAPHQL_URL (default http://localhost:8080/v1/graphql)
 *   PROD_GRAPHQL_URL  (default https://index.neverland.money/v1/graphql)
 *
 * Read-only.
 */

import {
  BALANCER_AUTORANGE_V3_POOL_ADDRESS,
  LP_BALANCER_AUTORANGE_CUTOVER_BLOCK,
  LP_V2_CUTOVER_BLOCK,
  LP_V2_RESUME_CUTOVER_BLOCK,
} from '../src/helpers/constants';

const LOCAL = process.env.LOCAL_GRAPHQL_URL || 'http://localhost:8080/v1/graphql';
const PROD = process.env.PROD_GRAPHQL_URL || 'https://index.neverland.money/v1/graphql';
const V2_LP_POOL_ADDRESS = '0x86dbf00485871c901c5129bd525348db96c2eb2d';

const WALLETS = [
  {
    addr: '0xcb69535ABBc95a042914507F963bDD74ad0025FF',
    note: 'historical LP activity — inspect aggregate epoch drift and cumulative pool counters',
  },
  {
    addr: '0x038201e05D140124952E34E31665420489288338',
    note: 'no known LP activity — useful as a non-LP comparison wallet',
  },
];

// Relative delta above which we call a field a real DIFFERENCE (below this is
// rounding/live-drift noise, e.g. the gate's sub-percent past-epoch rounding).
const NOISE_REL = 0.01; // 1%

interface EpochStat {
  epochNumber: string | number;
  totalPointsWithMultiplier: string | number;
  lpPoints: string | number;
  lpPointsWithMultiplier: string | number;
}
interface Points {
  lifetimeTotalPoints: string | number;
}
interface LbState {
  votingPower: string | number;
  combinedMultiplier: string | number;
  lifetimePoints: string | number;
}
interface Lock {
  id: string;
  lockedAmount: string | number;
  isPermanent: boolean;
}
interface Reserve {
  reserve_id: string;
  scaledATokenBalance: string | number;
  scaledVariableDebt: string | number;
}
interface LpPos {
  pool: string;
  settledLpPoints: string | number;
  valueUsd: string | number;
}
interface AggregatedLpPos {
  positions: number;
  settledLpPoints: number;
  valueUsd: number;
}
interface WalletData {
  epochs: EpochStat[];
  points: Points | null;
  state: LbState | null;
  locks: Lock[];
  reserves: Reserve[];
  lp: LpPos[];
}

async function gql<T>(url: string, query: string): Promise<T> {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`${url} HTTP ${res.status}`);
  const json = (await res.json()) as { data?: T; errors?: unknown };
  if (json.errors) throw new Error(`${url} GraphQL: ${JSON.stringify(json.errors)}`);
  if (!json.data) throw new Error(`${url} no data`);
  return json.data;
}

async function fetchWallet(url: string, addr: string): Promise<WalletData> {
  const a = addr.toLowerCase();
  const q = `query {
    UserEpochStats(where: { user_id: { _eq: "${a}" } }, order_by: { epochNumber: asc }) {
      epochNumber totalPointsWithMultiplier lpPoints lpPointsWithMultiplier
    }
    UserPoints(where: { id: { _eq: "${a}" } }) { lifetimeTotalPoints }
    UserLeaderboardState(where: { id: { _eq: "${a}" } }) { votingPower combinedMultiplier lifetimePoints }
    DustLockToken(where: { owner: { _eq: "${a}" } }) { id lockedAmount isPermanent }
    UserReserve(where: { user_id: { _eq: "${a}" } }) { reserve_id scaledATokenBalance scaledVariableDebt }
    UserLPPosition(where: { user_id: { _eq: "${a}" } }) { pool settledLpPoints valueUsd }
  }`;
  const d = await gql<{
    UserEpochStats: EpochStat[];
    UserPoints: Points[];
    UserLeaderboardState: LbState[];
    DustLockToken: Lock[];
    UserReserve: Reserve[];
    UserLPPosition: LpPos[];
  }>(url, q);
  return {
    epochs: d.UserEpochStats,
    points: d.UserPoints[0] ?? null,
    state: d.UserLeaderboardState[0] ?? null,
    locks: d.DustLockToken,
    reserves: d.UserReserve,
    lp: d.UserLPPosition,
  };
}

const n = (v: string | number | undefined | null): number => (v == null ? 0 : Number(v));
function rel(local: number, prod: number): number {
  if (local === prod) return 0;
  const base = Math.max(Math.abs(local), Math.abs(prod));
  return base === 0 ? 0 : Math.abs(local - prod) / base;
}
function tag(local: number, prod: number): string {
  const r = rel(local, prod);
  if (r === 0) return '~same (serialized precision)';
  if (r < NOISE_REL) return `~match (${(r * 100).toFixed(4)}%)`;
  return `DIFF ${local > prod ? '+' : '-'}${(r * 100).toFixed(2)}% (local ${local > prod ? '>' : '<'} prod)`;
}
const sum = (rows: { lockedAmount?: string | number }[], k: 'lockedAmount'): number =>
  rows.reduce((s, r) => s + n(r[k]), 0);

function aggregateLpByPool(rows: LpPos[]): Map<string, AggregatedLpPos> {
  const byPool = new Map<string, AggregatedLpPos>();
  for (const row of rows) {
    const pool = row.pool.toLowerCase();
    const current = byPool.get(pool) ?? { positions: 0, settledLpPoints: 0, valueUsd: 0 };
    byPool.set(pool, {
      positions: current.positions + 1,
      settledLpPoints: current.settledLpPoints + n(row.settledLpPoints),
      valueUsd: current.valueUsd + n(row.valueUsd),
    });
  }
  return byPool;
}

function reportWallet(addr: string, note: string, L: WalletData, P: WalletData): void {
  console.log(`\n${'='.repeat(78)}`);
  console.log(`WALLET ${addr}`);
  console.log(`  note: ${note}`);

  console.log(`\n  Per-epoch points (total multiplied | raw LP | multiplied LP):`);
  const epochs = Array.from(
    new Set([...L.epochs, ...P.epochs].map(e => String(e.epochNumber)))
  ).sort((x, y) => Number(x) - Number(y));
  for (const ep of epochs) {
    const le = L.epochs.find(e => String(e.epochNumber) === ep);
    const pe = P.epochs.find(e => String(e.epochNumber) === ep);
    const lt = n(le?.totalPointsWithMultiplier);
    const pt = n(pe?.totalPointsWithMultiplier);
    const llpRaw = n(le?.lpPoints);
    const plpRaw = n(pe?.lpPoints);
    const llpMultiplied = n(le?.lpPointsWithMultiplier);
    const plpMultiplied = n(pe?.lpPointsWithMultiplier);
    const lpNote =
      llpRaw || plpRaw || llpMultiplied || plpMultiplied
        ? `  rawLP=${tag(llpRaw, plpRaw)} multipliedLP=${tag(llpMultiplied, plpMultiplied)}`
        : '';
    console.log(`    epoch ${ep}: ${tag(lt, pt)}${lpNote}`);
  }

  const llife = n(L.points?.lifetimeTotalPoints ?? L.state?.lifetimePoints);
  const plife = n(P.points?.lifetimeTotalPoints ?? P.state?.lifetimePoints);
  console.log(
    `\n  Lifetime points: ${tag(llife, plife)}  (local ${llife.toExponential(4)} / prod ${plife.toExponential(4)})`
  );

  console.log(
    `\n  veDUST: locked ${tag(sum(L.locks, 'lockedAmount'), sum(P.locks, 'lockedAmount'))}` +
      ` (${L.locks.length}/${P.locks.length} locks) | votingPower ${tag(n(L.state?.votingPower), n(P.state?.votingPower))}`
  );

  console.log(
    `\n  Lending positions (${L.reserves.length} local / ${P.reserves.length} prod reserves):`
  );
  const reserves = Array.from(new Set([...L.reserves, ...P.reserves].map(r => r.reserve_id)));
  if (reserves.length === 0) console.log(`    (none)`);
  for (const rid of reserves) {
    const lr = L.reserves.find(r => r.reserve_id === rid);
    const pr = P.reserves.find(r => r.reserve_id === rid);
    console.log(
      `    ${rid.slice(0, 12)}..: supply ${tag(n(lr?.scaledATokenBalance), n(pr?.scaledATokenBalance))} | debt ${tag(n(lr?.scaledVariableDebt), n(pr?.scaledVariableDebt))}`
    );
  }

  console.log(`\n  LP positions (${L.lp.length} local / ${P.lp.length} prod):`);
  const localLpByPool = aggregateLpByPool(L.lp);
  const prodLpByPool = aggregateLpByPool(P.lp);
  const pools = Array.from(new Set([...localLpByPool.keys(), ...prodLpByPool.keys()]));
  if (pools.length === 0) console.log(`    (none)`);
  for (const pool of pools) {
    const lp = localLpByPool.get(pool);
    const pp = prodLpByPool.get(pool);
    console.log(
      `    ${pool.slice(0, 12)}..: positions ${lp?.positions ?? 0}/${pp?.positions ?? 0} | cumulative settledLpPoints ${tag(lp?.settledLpPoints ?? 0, pp?.settledLpPoints ?? 0)} | current valueUsd ${tag(lp?.valueUsd ?? 0, pp?.valueUsd ?? 0)}`
    );
  }
}

async function main(): Promise<void> {
  console.log(`Per-wallet local-vs-prod cross-check`);
  console.log(`  local: ${LOCAL}`);
  console.log(`  prod:  ${PROD}`);
  console.log(`  noise threshold (rounding/live-drift): <${NOISE_REL * 100}% = "~match"`);
  console.log(`  LP eras:`);
  console.log(
    `    V2 ${V2_LP_POOL_ADDRESS}: [${LP_V2_CUTOVER_BLOCK}, ${LP_BALANCER_AUTORANGE_CUTOVER_BLOCK})`
  );
  console.log(
    `    Balancer ${BALANCER_AUTORANGE_V3_POOL_ADDRESS}: [${LP_BALANCER_AUTORANGE_CUTOVER_BLOCK}, ${LP_V2_RESUME_CUTOVER_BLOCK})`
  );
  console.log(`    V2 resumed: [${LP_V2_RESUME_CUTOVER_BLOCK}, head)`);
  for (const w of WALLETS) {
    const [L, P] = await Promise.all([fetchWallet(LOCAL, w.addr), fetchWallet(PROD, w.addr)]);
    reportWallet(w.addr, w.note, L, P);
  }
  console.log(`\n${'='.repeat(78)}`);
  console.log(
    'Completed diagnostic report. Differences are review-only and do not change the exit status.'
  );
  console.log(
    'Raw per-epoch LP points aggregate all pools; current position counters are approximate, cumulative diagnostics and cannot split the original/resumed V2 eras.'
  );
}

main().catch((e: unknown) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
