#!/usr/bin/env bash
#
# Local full resync to chain head, in an isolated Postgres + Hasura pair.
#
# Touches nothing pre-existing: its own container names, ports and volume. Intended for
# measuring a candidate build against production without disturbing a running indexer.
#
#   pnpm run local:headsync           # start
#   pnpm run local:headsync:logs      # follow
#   pnpm run local:headsync:status    # progress + prefill state
#   pnpm run local:headsync:down      # stop and remove (volume included)
#
# Env passed through from .env, with the database pointed at the throwaway container.
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/local-headsync-lib.sh"
PGPASS="${HEADSYNC_PG_PASSWORD:-candidate}"

mkdir -p "$OUT"

# Postgres 18 mounts /var/lib/postgresql, NOT /var/lib/postgresql/data.
if ! docker ps -a --format '{{.Names}}' | grep -qx "$PG"; then
  docker volume create nvl-head-pgdata >/dev/null
  docker run -d --name "$PG" \
    -e POSTGRES_PASSWORD="$PGPASS" -e POSTGRES_DB=envio -e POSTGRES_USER=postgres \
    -p 127.0.0.1:$PGPORT:5432 -v nvl-head-pgdata:/var/lib/postgresql \
    postgres:18.3 >/dev/null
fi
# `docker ps -a` above also matches a stopped container (host or Docker Desktop restart);
# start it instead of waiting 120 s on a pg_isready that can never succeed.
docker ps --format '{{.Names}}' | grep -qx "$PG" || docker start "$PG" >/dev/null

echo "waiting for postgres..."
for _ in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 2; done
docker exec "$PG" pg_isready -U postgres

# STRINGIFY_NUMERIC_TYPES so BigInt comes back as exact strings for comparisons.
if ! docker ps -a --format '{{.Names}}' | grep -qx "$HASURA"; then
  docker run -d --name "$HASURA" --add-host=host.docker.internal:host-gateway \
    -e HASURA_GRAPHQL_DATABASE_URL="postgres://postgres:$PGPASS@host.docker.internal:$PGPORT/envio" \
    -e HASURA_GRAPHQL_ENABLE_CONSOLE=true -e HASURA_GRAPHQL_DEV_MODE=true \
    -e HASURA_GRAPHQL_STRINGIFY_NUMERIC_TYPES=true -e HASURA_GRAPHQL_UNAUTHORIZED_ROLE=public \
    -p 127.0.0.1:$HASURAPORT:8080 hasura/graphql-engine:v2.43.0 >/dev/null
fi
docker ps --format '{{.Names}}' | grep -qx "$HASURA" || docker start "$HASURA" >/dev/null

cd "$REPO"
set -a; . "$REPO/.env"; set +a
export ENVIO_PG_HOST=127.0.0.1 ENVIO_PG_PORT=$PGPORT ENVIO_PG_DATABASE=envio \
       ENVIO_PG_USER=postgres ENVIO_PG_PASSWORD="$PGPASS" ENVIO_PG_PUBLIC_SCHEMA=public \
       ENVIO_INDEXER_PORT=$INDEXERPORT ENVIO_HASURA=false TUI_OFF=true

# Everything else -- PREFILL_HISTORIC_EPOCHS included -- comes from .env, so what runs here
# is exactly what a deployment with the same .env would run.
echo "prefill: ${PREFILL_HISTORIC_EPOCHS:-false}"

# Refuse a second launch: both would bind ENVIO_INDEXER_PORT, the loser dies on EADDRINUSE and
# the pid file would then name a dead process while the first indexer keeps running. The
# identity check keeps a reused pid from an unrelated process from blocking a restart, and the
# lock makes the check, the launch and the pid write one step, so two launches started at the
# same moment cannot both pass the check. `down` takes the same lock.
exec 9>"$OUT/launch.lock"
flock -n 9 || { echo "another local:headsync launch or teardown is in progress" >&2; exit 1; }
if [ -s "$OUT/indexer.pid" ]; then
  prev=$(cat "$OUT/indexer.pid")
  if headsync_pid_alive "$prev"; then
    echo "local head sync already running (pid $prev); run 'pnpm run local:headsync:down' first" >&2
    exit 1
  fi
  rm -f "$OUT/indexer.pid"
fi

# fd 9 is closed for the indexer so the lock ends with this script, not with the sync.
nohup pnpm run start 9>&- > "$OUT/sync.log" 2>&1 &
echo $! > "$OUT/indexer.pid"
echo "started pid $(cat "$OUT/indexer.pid") -> $OUT/sync.log"
