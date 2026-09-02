#!/usr/bin/env bash
# Stop the local head sync and remove its containers and volume.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${HEADSYNC_OUT:-$REPO/.headsync}"
pid=$(cat "$OUT/indexer.pid" 2>/dev/null || echo)
if [ -n "$pid" ]; then kill -TERM "$pid" 2>/dev/null || true; fi
docker rm -f nvl-head-hasura nvl-head-pg >/dev/null 2>&1 || true
docker volume rm nvl-head-pgdata >/dev/null 2>&1 || true
echo "local head sync removed"
