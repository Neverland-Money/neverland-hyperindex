# Deployment

How to deploy the Neverland HyperIndex indexer. Written for both humans and agents operating the
production host.

Runtime is Node **22.18.0** (`.nvmrc`), package manager **pnpm 10**, envio **2.32.12**, chain
**Monad mainnet (143)**.

---

## TL;DR

```bash
git pull
cp .env.production .env        # first time only, then fill in the real secrets
pnpm run prod:up               # or prod:up:tunnel to also expose the Cloudflare tunnel
pnpm run prod:progress         # verify it is advancing
```

The container itself runs `pnpm install --frozen-lockfile && pnpm run codegen && pnpm run start`.
**There is no manual patch step and no build step** — see below.

---

## 1. The envio patch — automatic, but the file must be committed

This repo patches `envio@2.32.12` to add `ENVIO_HEAD_DEBOUNCE_MS` (see
[docs/head-debounce-tiers.md](docs/head-debounce-tiers.md)). The patch is declared in
`pnpm-workspace.yaml`:

```yaml
patchedDependencies:
  envio@2.32.12: patches/envio@2.32.12.patch
```

**Applying it requires no action.** `pnpm install --frozen-lockfile` materializes the patched
package, and the `pnpm run codegen` that follows compiles it in. A normal deploy picks it up.

**But three files MUST be committed and present in the checkout:**

| file                          | if missing                                                |
| ----------------------------- | --------------------------------------------------------- |
| `patches/envio@2.32.12.patch` | `pnpm install --frozen-lockfile` **exits 254** (`ENOENT`) |
| `pnpm-lock.yaml`              | frozen-lockfile mismatch, install fails                   |
| `pnpm-workspace.yaml`         | patch silently not applied; indexer runs at ~170 RPM      |

The first case is a hard failure, verified: the container will crash-loop on start, not fall back
to unpatched envio. If you see `ENOENT ... patches/envio@2.32.12.patch` in `pnpm run
prod:logs:indexer`, the checkout is missing the patch file.

### Verifying the patch is live

```bash
docker exec neverland-indexer sh -c \
  'grep -c ENVIO_HEAD_DEBOUNCE_MS $(readlink -f /app/generated/node_modules/envio)/src/sources/SourceManager.res.js'
# expect: 1
```

`0` means the running indexer is stock envio and will burn ~170 RPM.

### If you ever modify the patch

It targets `src/sources/SourceManager.res` — the **ReScript source**, not the compiled
`.res.js`. `envio` is a `bs-dependency`, so `envio start` runs codegen and recompiles the source
over any `.res.js` edit, silently reverting it. Always `pnpm patch envio@2.32.12`, edit the
`.res`, `pnpm patch-commit`, then run `pnpm run codegen` once and confirm the marker survived
into the `.res.js`.

---

## 2. Prefill: with or without

`PREFILL_HISTORIC_EPOCHS` decides whether settled Tides 1-8 are **written verbatim from disk** or
**recomputed from chain events**.

### With prefill — the default, and what production runs

```bash
PREFILL_HISTORIC_EPOCHS=true
PREFILL_DATA_DIR=/app/data
```

Requires these committed artifacts, which reach the container through the `.:/app` bind mount:

```
data/tide-1.json.gz  ...  data/tide-8.json.gz     # per-Tide leaderboard scoring
data/prefill-snapshot.json.gz                     # settlement state at the boundary
```

Prefill exists for **fidelity, not speed**. Recomputing Tides 1-8 produces *more correct* numbers
that are **not the numbers that were actually distributed** to users. Prefill reproduces the
distributed values exactly. It saves only a slice of the handler budget; a backfill is ~87%
HyperSync fetch time, so it does not meaningfully shorten a resync.

Boundary: last prefilled block **99,809,559** (ts 1787893199). Tide 9 opens at block
**99,809,560**. From there the indexer computes normally.

`PREFILL_DATA_DIR` is pinned to an absolute path because `envio start` runs the indexer from the
`generated/` subproject, so the process cwd is `/app/generated`; a bare relative `data` would
resolve to `/app/generated/data` and silently prefill nothing. **Do not set it to an empty
string** — empty is not nullish and would override the default.

### Without prefill — full recompute

```bash
PREFILL_HISTORIC_EPOCHS=false
```

Recomputes every Tide from chain events. Use only when you intend to *replace* historic
leaderboard values, or to regenerate the artifacts.

> ⚠️ This changes historic user-facing points. Tides 1-8 will not match what was distributed.
> Do not do this on production without an explicit decision to restate history.

Before a no-prefill resync, check `src/handlers/lp.ts`: all four LP contracts must stay registered
in `config.yaml` (UniswapV3Pool, NonfungiblePositionManager, BalancerAutoRangePool, the V2 pair).
If any were commented out as dead, uncomment them first or LP history will be wrong.

### Regenerating the artifacts

```bash
# 1. sync with PREFILL_HISTORIC_EPOCHS=false and end_block set to the boundary
# 2. then, with the DB stopped exactly at that block:
pnpm run export:tides
pnpm run export:snapshot -- --boundary-block 99809559 --docker neverland-postgres
```

`export:snapshot` refuses to run unless `_meta.progressBlock` equals `--boundary-block`. That
guard exists because a snapshot taken at head silently mixes live-Tide rows into cumulative
tables that have no epoch column to slice on.

---

## 3. Environment

Copy `.env.production` to `.env` and fill in secrets. Only `environment:` entries in
`docker-compose.prod.yml` reach the container — **there is no `env_file`**, so a variable in
`.env` that is not listed in the compose `environment:` block does nothing.

| variable                      | value            | note                                     |
| ----------------------------- | ---------------- | ---------------------------------------- |
| `ENVIO_API_TOKEN`             | *(secret)*       | required; HyperSync access               |
| `CLOUDFLARE_TUNNEL_TOKEN`     | *(secret)*       | only for `prod:up:tunnel`                |
| `POSTGRES_PASSWORD`           | *(secret)*       |                                          |
| `HASURA_ADMIN_SECRET`         | *(secret)*       |                                          |
| `PREFILL_HISTORIC_EPOCHS`     | `true`           | default; see above                       |
| `PREFILL_DATA_DIR`            | `/app/data`      | absolute, never empty                    |
| `ENVIO_HEAD_DEBOUNCE_MS`      | `3000`           | **compose defaults to 0 — must be set**  |
| `ENVIO_ENABLE_EXTERNAL_CALLS` | `false`          | handlers are event-only; do not enable   |
| `ENVIO_ENABLE_ETH_CALLS`      | `false`          | Monad nodes cannot serve archive state   |
| `LOG_LEVEL`                   | `info`           |                                          |
| `METRICS_PORT`                | `9090`           | bound to 127.0.0.1 only                  |

### `ENVIO_HEAD_DEBOUNCE_MS` — read this

The compose fallback is `0`, which is **stock envio behaviour: ~170 RPM, i.e. 1.7x a 100 RPM
plan, permanently in overage.** An existing server `.env` predating this change has no such line
and will resolve to 0. Add it explicitly.

| tier         | value   | median RPM | p95 RPM | head lag |
| ------------ | ------- | ---------- | ------- | -------- |
| **best**     | `3000`  | ~46        | ~81     | ~1.5 s   |
| moderate     | `10000` | ~11        | ~15     | ~4.7 s   |
| light        | `20000` | ~9         | ~12     | ~9.2 s   |

Backfill is unaffected by this setting — it only applies once caught up to head. Full measurement
methodology in [docs/head-debounce-tiers.md](docs/head-debounce-tiers.md).

---

## 4. Deploy

### First time

```bash
git clone <repo> && cd neverland-hyperindex
cp .env.production .env && $EDITOR .env     # fill secrets, set ENVIO_HEAD_DEBOUNCE_MS=3000
pnpm run prod:up                            # postgres + hasura + indexer
pnpm run prod:up:tunnel                     # add the public Cloudflare tunnel
```

### Update to a new commit

```bash
git pull
pnpm run prod:restart:indexer   # re-runs install + codegen + start inside the container
```

Use `pnpm run prod:upgrade` (`zero-downtime-update.sh`) for the blue/green path when the schema
is unchanged.

### Full resync (destroys the database)

```bash
pnpm run prod:db:backup     # ALWAYS first
pnpm run prod:resync        # docker compose down -v && up -d
```

`down -v` deletes the Postgres volume. There is no undo. With prefill on, a resync reproduces the
distributed historic values; with prefill off, it restates them.

---

## 5. Verify a deploy

```bash
pnpm run prod:ps                 # all services Up / healthy
pnpm run prod:progress           # block height advancing
pnpm run prod:logs:errors        # should be quiet
pnpm run prod:health             # hasura reachable from the indexer
```

Checks that actually catch the failure modes seen in practice:

```bash
# 1. patch live? (0 = stock envio, ~170 RPM)
docker exec neverland-indexer sh -c \
  'grep -c ENVIO_HEAD_DEBOUNCE_MS $(readlink -f /app/generated/node_modules/envio)/src/sources/SourceManager.res.js'

# 2. debounce actually passed into the container?
docker exec neverland-indexer sh -c 'echo $ENVIO_HEAD_DEBOUNCE_MS'    # expect 3000

# 3. prefill applied? (0 rows for a prefilled tide means it did NOT run)
docker exec neverland-postgres psql -U postgres -d envio -tAc \
  'select count(*) from "UserEpochStats" where epoch <= 8'

# 4. lag from chain head, in blocks
docker exec neverland-postgres psql -U postgres -d envio -tAc 'select "progressBlock" from "_meta" limit 1'
curl -s https://monad.hypersync.xyz/height
```

---

## 6. Troubleshooting

| symptom | cause | fix |
| ------- | ----- | --- |
| indexer crash-loops, `ENOENT ... patches/envio@2.32.12.patch` | `patches/` not committed or not pulled | commit/pull the patch file |
| RPM ~170, plan in overage | patch not applied, or `ENVIO_HEAD_DEBOUNCE_MS` unset/0 | run verify check 1 and 2 above |
| historic Tides show wrong points | ran with `PREFILL_HISTORIC_EPOCHS=false` | restore from backup, or resync with prefill on |
| prefill silently did nothing | `PREFILL_DATA_DIR` relative or empty; `data/*.json.gz` missing | set `/app/data`, confirm artifacts present |
| `apk add` / `pnpm install` fails in container | host network/DNS/MTU issue on the docker network | check the bridge network can reach the internet |
| Hasura unhealthy after resync | metadata references dropped tables | restart hasura after the indexer has created the schema |

---

## 7. Notes for agents

- **Never** run `pnpm run prod:resync` or `prod:down` without an explicit instruction — `-v`
  destroys the database.
- Take `pnpm run prod:db:backup` before any destructive step.
- Do not enable `ENVIO_ENABLE_EXTERNAL_CALLS` / `ENVIO_ENABLE_ETH_CALLS`. Monad full nodes cannot
  serve archive-style historic state; handler paths are event-only by design.
- Do not edit files under `node_modules/` to change envio behaviour — codegen overwrites them.
  Use `pnpm patch`.
- `ENVIO_KEEPER_FINAL_ONLY_FROM_EPOCH` stays disabled unless an operator approves a floor.
- The indexer container mounts the repo at `/app`. Editing the checkout affects the next restart.
