#!/usr/bin/env bash
# Progress and prefill state of the local head sync.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${HEADSYNC_OUT:-$REPO/.headsync}"
Q() { docker exec nvl-head-pg psql -U postgres -d envio -tAc "$1" 2>/dev/null; }
pid=$(cat "$OUT/indexer.pid" 2>/dev/null || echo)
if [ -n "$pid" ] && ps -o args= -p "$pid" 2>/dev/null | grep -q 'pnpm run start'; then
  echo "indexer pid $pid  up $(ps -o etime= -p "$pid" | tr -d ' ')"
else
  echo "indexer NOT running"
fi
# Status must report an absent database, not die on it: under `set -e` a failing `docker exec`
# would abort before the log line, which is the one still useful after `down`.
if docker ps --format '{{.Names}}' | grep -qx nvl-head-pg; then
  Q "select '  block '||latest_processed_block||'  (height '||block_height||')' from chain_metadata;"
  Q "select '  tide '||\"epochNumber\"||'  users='||(select count(*) from \"UserEpochStats\" u where u.\"epochNumber\"=e.\"epochNumber\")||'  active='||\"isActive\" from \"LeaderboardEpoch\" e order by e.\"epochNumber\";"
  Q "select '  total UserEpochStats: '||count(*) from \"UserEpochStats\";"
else
  echo "  postgres nvl-head-pg NOT running"
fi
echo "  errors in log: $(grep -ciE 'error|panic|fatal' "$OUT/sync.log" 2>/dev/null || echo 0)"
