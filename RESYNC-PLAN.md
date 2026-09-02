# Clean resync plan

Specification for the schema cleanup and full resync of this indexer. Written to be executed
cold. Read it top to bottom before touching anything: sections 1 and 3 are binding, section 4
describes invariants the code already holds, section 5 is the work.

Branch `feat/efficiency-settings-v3-upgrade`, HEAD `690cea9`. Everything described here is
uncommitted working-tree or staged work.

---

## 1. Hard constraints

These are not negotiable and they override any default behavior.

- **No git writes.** No `add`, `commit`, `stash`, `reset`, `restore`, `checkout`, `branch`,
  `push`. The repository owner stages files himself while reviewing them. Treat the working
  tree as authoritative and do not comment on the index.
- **No remote or production actions.** No deploys, no remote env mutation, no PR merges, no
  posting to external services.
- **Do not touch `neverland-app`.**
- **Do not stop, restart, modify or reuse any pre-existing service or database.** In
  particular never touch `neverland-governance_*`, `neverland-discord-bots_pgdata`, or
  `neverland-postgres-bots`.
- **Stop before starting any service and wait for explicit approval.**
- **Node 22.18.0**: put `/home/catalyst/.nvm/versions/node/v22.18.0/bin` first on `PATH`.
- **Keep `ENVIO_KEEPER_FINAL_ONLY_FROM_EPOCH` disabled** unless separately approved.
- **US English spelling** everywhere, with no renaming of existing identifiers.

### Test-execution constraints

A previous session twice exhausted host memory by running the full suite, disconnecting WSL.
The cause is understood and fixed (section 4), but the operating rules remain:

- **Never** run the full glob, `test:coverage`, or `c8` without showing the exact command and
  getting fresh approval.
- Prefer **one targeted test file at a time**, always inside a bounded scope:

```bash
systemd-run --user --scope --quiet \
  -p MemoryMax=2G -p MemorySwapMax=0 \
  --working-directory=/home/catalyst/projects/neverland/indexers/neverland-hyperindex \
  -- env ENVIO_TEST_HANDLER_WILDCARD=true \
     timeout --signal=INT 120 \
     node --import tsx --import ./src/__tests__/test-env-preload.ts \
          --test src/__tests__/<file>.test.ts
```

`NODE_OPTIONS=--max-old-space-size` is **not** an adequate boundary. The cgroup is.

---

## 2. What problem this solves

Two independent problems converged.

**Problem A — the indexer's request rate.** Production runs at roughly 200 requests/min
against HyperSync while the governance indexer sits near 20. The original hypothesis was
unbounded `UserVault` recording; that was removed and the rate did not move.

**Problem B — recomputed history no longer matches what was paid.** Tides 1–8 are settled and
distributed. Every correctness fix since they were scored moves the recomputed numbers away
from the published draws. Re-indexing from genesis therefore produces a leaderboard that
disagrees with what users were actually paid. `PREFILL_HISTORIC_EPOCHS` exists to write those
settled tides verbatim from `data/tide-<n>.json` instead of recomputing them.

### The acceptance criterion

> A full sync and a prefilled sync must be **indistinguishable in the indexer's end state for
> Tides 1–8**.

This is the bar. Note the asymmetry: the prefilled sync is the **normal** path (§ Step 5), and
the recompute arm exists only to prove the prefilled one is faithful. Unit tests prove the
mechanisms; only a real sync proves end-state equivalence, and that has **not yet been
demonstrated**.

---

## 3. Rules

### 3.1 `data/tide-*.json` is the source of truth — never regenerate it

The files hold exact integer digits, no exponential notation:

```
"depositPoints": 172866260298447819119
```

Any tool that reads them must preserve arbitrary precision. Node's `JSON.parse` does not: it
forces every number into an IEEE-754 double and silently rounds anything above 2^53. Use Python
(`json.loads` → `int`), or quote the literals before parsing, as `prefill.ts` now does.

Verify with Python or raw text, never through `node -e` / `require()`.

### 3.2 Never export tides from a database that was synced with prefill on

A prefilled database's Tide 1–8 rows are a copy of `data/`, not an independent computation.
Exporting from one is circular, and reproduces whatever defects the prefill path carried.

Only a database synced with `PREFILL_HISTORIC_EPOCHS=false` is a valid export source.

### 3.3 Schema cleanup does not reduce the request rate — do not claim it will

Measured after a 17% filter reduction: six one-minute windows at 175.2, 177.3, 201.5, 341.2,
144.2, 277.7 getLogs/min (mean 219.5) against a 169/min baseline. No reduction.

Ruled out by measurement, so do not re-investigate without new evidence: vaults (86 addresses
total, never registered), wildcards (zero), event density (0.39% of blocks carry data),
`field_selection`, reorg checking (defaults true in both v2.32 and v3.6.1), and partition count
(all three production processes show 1 partition with a 17× rate spread).

Steps 1–3 are justified by code cleanliness alone. The only untested lever is `block_lag`; it
must not be added to `config.yaml` while running tests, because the simulator has no advancing
head and `block_lag: 20` hangs the suite.

### 3.4 Stable-debt contracts are silent

All 18 reserves carry a non-zero `sToken`, so all 18 addresses reach the HyperSync filter via
`context.chain.StableDebtToken.add()` (there are no static addresses in `config.yaml`). But
`STokenBalanceHistoryItem` and `StableTokenDelegatedAllowance` hold **zero rows** — those
contracts have never emitted a stored event. Removing them cuts filter breadth, not log volume.

### 3.5 Schema changes force a full re-index

Envio has no in-place migration. `envio dev --restart` is documented as "clear the database and
re-index from scratch… **required when config/schema/ABI changes are incompatible with the
existing indexer state**", and `envio stop` deletes the database.

**Therefore steps 1–3 must complete before step 6.** Running them after the sync invalidates it.

---

## 4. Current state

### 4.1 Prefill guarantees

`src/helpers/prefill.ts` now holds three invariants. Do not weaken them.

- **Exact reads.** Tide files are parsed by `parseTideDocument`, which quotes BigInt-field
  literals (`BIGINT_LITERAL`) before `JSON.parse` so each BigInt is built from the exact decimal
  text. Pinned by `prefill.test.ts` and by a fixture with bare literals above 2^53.
- **Prefilled tides are immutable.** `isPrefilledEpoch()` keys on the tide being credited, not
  the event timestamp — settlement walks backwards, so a Tide-9 event credits time held in
  Tide 8 and a timestamp gate never sees it. All 10 production `UserEpochStats` writes route
  through the single path `setUserEpochStats()`: `shared.ts:2123, 2994, 3246, 3345, 3431, 3512,
3593`; `leaderboard.ts:470, 561`; `lp.ts:3934`. `prefillHistoricEpochsIfNeeded` is the only
  direct writer. Adding a new direct `context.UserEpochStats.set(...)` reintroduces the defect.
- **Preload does no work.** `prefillHistoricEpochsIfNeeded` returns on `context.isPreload ===
true`, matching the convention used 21 times in `shared.ts`. The check is inlined because
  `isPreload` is private to `shared.ts`, which already imports `prefill.ts`.

### 4.2 Test environment

`envio` bundles dotenv (`envio/src/Env.res.mjs:11`), so importing it loads the repo `.env` into
every test worker. Without the guards below, `PREFILL_HISTORIC_EPOCHS=true` reaches the suite and
`prefillDataDir()` resolves to the real `data/` — 31 MB, 28,403 rows — in each worker.

- `src/__tests__/test-env-preload.ts` is loaded via `--import` in all three test scripts. It
  **assigns** rather than deletes: dotenv skips keys already present but repopulates absent ones
  (`dotenv@16.4.5 lib/main.js:324-338`), so assignment is order-independent and deletion is not.
- A tripwire in `prefill.ts` throws before any filesystem read if `PREFILL_DATA_DIR` resolves to
  the production `data/` or is left at the sentinel. Armed only by `NEVERLAND_TEST_ENV=1`;
  production behavior is unchanged.
- The 13 test files that bypass `v3-test-helpers` import the preload directly.
- `--test-concurrency=2` on all three scripts. This bounds worker count, **not** memory — the
  cgroup is the boundary.
- `src/__tests__/test-env-guard.test.ts` pins all of the above with 8 assertions, including that
  every `*.test.ts` reaches a preload and every script carries both flags.

Known cost, deliberately not addressed: `v3-test-helpers` clones all stores per `.set()`
(`:119-121, 132, 137, 277`) and rebuilds a fresh simulator per event (`:253-257`), with the
simulator copying each entity on read and write (`TestIndexer.res.mjs:288-292, 329-353`). This is
O(store size) per operation. It is bounded only because the tripwire keeps stores fixture-scale.

### 4.3 Verification status

Green under the 2 GiB/zero-swap cgroup — 368 tests across 11 files: `prefill` (17),
`test-env-guard` (8), `shared-accrual` (3), `epoch-override-accrual.e2e` (1), `leaderboard.e2e`
(9), `shared-utils` (39), `shared-external` (8), `lp-coverage` (45), `lp-events` (72),
`shared-task0` (5), `leaderboard-keeper` (88), `leaderboard-handlers` (73). `tsc --noEmit`,
targeted ESLint and Prettier all clean.

**Full gate green after steps 1-5, 7 and 9:** 771 tests pass, coverage 100% on lines,
branches, functions and statements, and `format:check`, `eslint` and `tsc --noEmit` are clean.
The suite runs in ~55 s inside a 6 GB cgroup with swap disabled.

---

## 5. The plan

Steps are ordered by dependency. Status is marked per step. Steps 1-5, 7 and 9 are **done**;
the remaining steps need running services.

### Step 0 — Clean dead databases — PENDING (needs Docker)

Remove only these, and only after confirming nothing else uses them:
containers `nvl-ab-A-pg`, `nvl-ab-C-pg`, `nvl-ab-A-hasura`, `nvl-ab-C-hasura`, `envio-postgres`,
`envio-hasura`, `nvl-head-pg`, `nvl-head-hasura`; volumes `nvl-ab-A-pgdata`, `nvl-ab-C-pgdata`,
`nvl-head-pgdata`, `envio-postgres-data`.

**Never touch** `neverland-governance_postgres_data`, `neverland-governance_mysql_data`,
`neverland-governance_redis_data`, `neverland-discord-bots_pgdata`, `neverland-postgres-bots`.

Requires explicit approval — it starts/stops services.

### Step 1 — Remove all stable-debt tokens — DONE

Approved scope: **everything, fields included.** This is a breaking API change and that was
accepted explicitly.

Blast radius, measured: 13 schema fields (`UserReserve`: `principalStableDebt`,
`currentStableDebt`, `stableBorrowRate`, `oldStableBorrowRate`,
`stableBorrowLastUpdateTimestamp`; `Reserve`: `stableBorrowRateEnabled`, `stableRateSlope1`,
`stableRateSlope2`, `totalPrincipalStableDebt`, `averageStableRate`, `stableBorrowRate`,
`stableDebtLastUpdateTimestamp`, `lifetimePrincipalStableDebt`), plus `Borrow.stableTokenDebt`,
`ReserveConfigurationHistoryItem.stableBorrowRateEnabled`, four `ReserveParamsHistoryItem`
fields, entities `STokenBalanceHistoryItem` and `StableTokenDelegatedAllowance`, ~200 code
references, `currentTotalDebt` (28 refs, computed from stable + variable), 5 handlers in
`tokenization.ts`, registration + sToken `SubToken` in `config.ts`,
`abis/lending/StableDebtToken.json`, and 28 test references.

`INDEX_STABLE_DEBT_TOKENS` in `helpers/constants.ts` was the kill switch and its
registration site was `c8`-ignored; both went away with the real removal.

### Step 2 — Remove 4 dead Pool events — DONE

Exact deletions, **bottom-up so line numbers do not drift**:

- `config.yaml:325` MintUnbacked, `:326` BackUnbacked, `:328` SwapBorrowRateMode,
  `:329` RebalanceStableBorrowRate.
  **`config.yaml:327` (`IsolationModeTotalDebtUpdated`) sits between the two pairs — this is
  two separate cuts, not one 5-line slice.**
- `src/handlers/pool.ts:917-956`, `958-1006`, `1033-1074`, `1076-1111`.
- `schema.graphql:182-195`, `210-221`, `317-327`, `329-340`; Action enum members at `:9` and `:11`.
- `src/types/envio.ts:27, 29, 36, 37`.
- Tests: `pool-events.test.ts:1267-1322`, `1373-1405`, `1407-1451`; edit the `seedPool` signature
  at `:48-53`. **Keep** `:1348-1371` (isolation mode).

Keep the ABI entries (`abis/lending/Pool.json:45, 248, 286, 466` and the function entries).
`Reserve.unbacked`, `lifetimePortalLPFee`, `lifetimePortalProtocolFee` and
`Pool.bridgeProtocolFee` become write-only — decide whether to keep or drop.

Isolation mode and `DustLock.Split` are **kept** — explicitly decided.

### Step 3 — Remove 7 orphan entities — DONE

Premise confirmed by a three-stage proof: zero writers, list exactly complete.

**Methodology trap to respect:** a naive `context.<X>.set` grep reports 11 orphans, but 4 of
them do have writers (`lp.ts:1128-1130, 1148-1153, 1177-1179, 1299-1314, 775-972`). Use the
bare-identifier search instead.

Deletions, bottom-up: `schema.graphql:1929-1938` (EpochLeaderboardStats), `1735-1749`
(UserLPBaseline + SelfTransferredToken, contiguous), `778-792` (TopK + TopKEntry, contiguous),
`643-648` (UsdEthPriceHistoryItem), `586-593` (SwapHistory). Then `src/types/envio.ts:52, 57,
69, 70, 151, 152, 166`, and dead test references in `lp-coverage.test.ts`,
`shared-external.test.ts`, and `leaderboard-keeper.test.ts:2200-2201, 2221, 2246`.

**Three name-collision traps** at `schema.graphql:778/786`, `636/643`, `1727/1735` — a naive
sed will silently break them.

**TopK/TopKEntry are safe to delete.** Zero writers and zero readers in production code; the
only references are the aliases at `src/types/envio.ts:69-70` and inert mock stores at
`lp-coverage.test.ts:60,61,146,147`. Ranking is done via ORDER BY, so nothing consumes them.
API-breaking for external GraphQL consumers, the same class of change already accepted for
stable-debt removal.

**Note:** dead test imports left behind will not fail any gate. Clean them by hand.

### Step 4 — Fix tests so stable-debt is never needed — DONE

Follows step 1 mechanically.

### Step 5 — Prefill defaults ON — DONE

**Prefill is the normal operating mode, not the exception.** Settled tides are written from
`data/` instead of being recomputed, which is what makes a sync fast. When a tide closes it is
exported from the live database into `data/` (`pnpm run export:tides`) and joins the prefilled
span. `.env`, `.env.example`, `.env.production` and the compose default all say `true`.

Turning it off is a one-off: the recompute arm of the acceptance criterion, or a tide
deliberately re-derived from chain events.

Tests are insulated from this by `test-env-preload.ts`, which assigns `false` before `envio`
loads dotenv. Verified against `.env=true`: with the preload the value stays `false` and the
data dir is the sentinel; without it, `.env` yields `true` and an undefined data dir — the
original OOM condition. Full suite under the new default: 771 pass, 100% coverage, **772 MB
peak RSS**.

### Step 6 — Sync to HEAD — PENDING (needs Docker)

Long-running. Requires approval. **Steps 1–3 must be complete first** (§3.5): a later schema
change forces a re-index and discards this sync.

### Step 7 — Exporter into the repo — DONE

**Do not regenerate `data/`** (section 3.1). Bring `scripts/export-tides.ts` into the repo as a
reviewed, tested tool for _future_ tides, emitting BigInts as quoted strings with a `--verify`
round-trip mode. Field census: `schema.graphql:1769-1805` (UserEpochStats, 35 fields, 24
`BigInt!` → pg `numeric` → must be quoted) and `:1579-1590` (LeaderboardEpoch).

Delivered as `scripts/export-tides.ts` (`pnpm run export:tides` / `pnpm run verify:tides`),
reading Postgres through `COPY ... FORMAT csv` with every column cast to `::text` and writing
every BigInt as a quoted JSON string. Pure functions are unit-tested offline in
`src/__tests__/export-tides.test.ts`, including the drift pin that `BIGINT_FIELDS` in
`prefill.ts` covers every `BigInt` field in `schema.graphql`. No new dependency: `psql` is
invoked directly, with `--docker <container>` for a containerized database.

### Step 8 — Keep `data/` as-is; back up the synced database — PARTIAL

`data/tide-*.json` stays untouched — it is the source of truth (§3.1). After step 6, take one
database backup and treat it as _the_ reference. `pnpm run verify:tides` proves a synced
database matches these files field for field before anything is regenerated.

**Note:** the tide files are staged but not committed.

### Step 9 — Make container prefill mechanical — DONE

- `docker-compose.prod.yml:81` — `PREFILL_HISTORIC_EPOCHS` is **missing** from the indexer's
  environment list. Add it.
- `docker-compose.prod.yml:111-123` — put `cloudflared` behind a compose profile so it cannot
  start by accident.
- If adding `PREFILL_DATA_DIR`, beware: an **empty string silently disables the default** and
  is not nullish, so `prefillDataDir()` returns `''`.
- `data/` already reaches the container via the bind mount at `:83`. No Dockerfile exists.
- `:86-88` and `:22-46` — nothing publishes Hasura once cloudflared is skipped.
- `README.md:265-268, 275-283, 340-346` — self-hosting endpoints and reset instructions are
  wrong for this compose file.
- Add two `package.json` scripts so the local prod-path run is one command.

### Step 10 — Replicate production locally — PENDING (needs Docker)

```bash
docker compose -f docker-compose.prod.yml up -d postgres hasura indexer
```

Do **not** use `deploy.sh` / `prod:deploy` / `prod:up` / `prod:resync`: `deploy.sh:37-44`
hard-fails without a real `CLOUDFLARE_TUNNEL_TOKEN`.

**Hazard:** prod and staging share one compose project, so `--remove-orphans` is destructive
across them (`neverland-indexer.service:14`).

### Step 11 — Sync with prefill on and prove equivalence — PENDING (needs Docker)

Compare against the step-6 baseline and demonstrate the acceptance criterion for Tides 1–8.
This is the step that closes the project.

---

## 6. Open questions

1. **Write-only fields left by step 2** — `Reserve.unbacked`, `Reserve.lifetimePortalLPFee`,
   `Reserve.lifetimePortalProtocolFee`, `Pool.bridgeProtocolFee`: keep or drop.
2. **`block_lag` on v3.6.1** — untested. See §3.3 for the constraint on testing it.

## 7. Survey outputs

`~/.claude/jobs/a932d8a2/tmp/survey-*.txt` holds four detailed file-and-line surveys covering
the dead Pool events, the orphan entities, the exporter field census, and the local production
compose path. They are the source of most line references in section 5.

They predate sections 3 and 4, so ignore any claim that `data/` is corrupted, that the local
database and `data/` have mismatched provenance, or that an unstaged rename blocks the build.
Their finding that `revive()` rounds a JSON number is correct and is fixed.
