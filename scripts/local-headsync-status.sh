#!/usr/bin/env bash
# Progress and prefill state of the local head sync.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/local-headsync-lib.sh"
# A failing query must not abort the script under `set -e` before the log summary below, but
# it must not be mistaken for "not ready" either: the first line of psql's own error is shown
# (connection refused, missing relation, bad role...) and the script exits non-zero at the end.
rc=0
Q() {
  local out
  if out=$(docker exec "$PG" psql -U postgres -d envio -tAc "$1" 2>&1); then
    printf '%s\n' "$out"
  else
    echo "  (query failed: $(printf '%s\n' "$out" | head -n1))"
    rc=1
  fi
}
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
# `grep -c` prints the count and exits 1 when it is 0, so 1 is not a failure here. A log that is
# absent is normal before the first launch and is said so; one that cannot be read is a failure.
if [ ! -e "$OUT/sync.log" ]; then
  echo "  errors in log: (no sync.log yet)"
elif n=$(grep -ciE 'error|panic|fatal' "$OUT/sync.log" 2>/dev/null) || [ $? -eq 1 ]; then
  echo "  errors in log: $n"
else
  echo "  errors in log: (cannot read $OUT/sync.log)"
  rc=1
fi
exit "$rc"
