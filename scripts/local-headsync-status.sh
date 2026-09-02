#!/usr/bin/env bash
# Progress and prefill state of the local head sync.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/local-headsync-lib.sh"
# `|| ...` keeps a not-yet-ready Postgres (container up, queries failing) from aborting the
# script under `set -e` before the log summary below.
Q() { docker exec "$PG" psql -U postgres -d envio -tAc "$1" 2>/dev/null || echo "  (query failed: postgres not ready)"; }
pid=$(cat "$OUT/indexer.pid" 2>/dev/null || echo)
if headsync_pid_alive "$pid"; then
  echo "indexer pid $pid  up $(ps -o etime= -p "$pid" | tr -d ' ')"
else
  echo "indexer NOT running"
fi
# Status must report an absent database, not die on it: under `set -e` a failing `docker exec`
# would abort before the log line, which is the one still useful after `down`.
if docker ps --format '{{.Names}}' | grep -qx "$PG"; then
  Q "select '  block '||latest_processed_block||'  (height '||block_height||')' from chain_metadata;"
  Q "select '  tide '||\"epochNumber\"||'  users='||(select count(*) from \"UserEpochStats\" u where u.\"epochNumber\"=e.\"epochNumber\")||'  active='||\"isActive\" from \"LeaderboardEpoch\" e order by e.\"epochNumber\";"
  Q "select '  total UserEpochStats: '||count(*) from \"UserEpochStats\";"
else
  echo "  postgres $PG NOT running"
fi
echo "  errors in log: $(grep -ciE 'error|panic|fatal' "$OUT/sync.log" 2>/dev/null || echo 0)"
