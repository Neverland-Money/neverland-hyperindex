#!/usr/bin/env bash
# Stop the local head sync and remove its containers and volume.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/local-headsync-lib.sh"
pid=$(cat "$OUT/indexer.pid" 2>/dev/null || echo)
# Signal only the indexer: after a reboot the recorded pid can belong to any process.
if headsync_pid_alive "$pid"; then
  kill -TERM "$pid" 2>/dev/null || true
elif [ -n "$pid" ]; then
  echo "pid $pid is not the head-sync indexer (stale pid file); not signaling it" >&2
fi
rm -f "$OUT/indexer.pid"
docker rm -f nvl-head-hasura nvl-head-pg >/dev/null 2>&1 || true
docker volume rm nvl-head-pgdata >/dev/null 2>&1 || true
echo "local head sync removed"
