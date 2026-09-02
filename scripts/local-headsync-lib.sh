#!/usr/bin/env bash
# Shared by the local-headsync scripts. Sourced, never executed.
# shellcheck disable=SC2034  # every variable below is read by a sourcing script
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${HEADSYNC_OUT:-$REPO/.headsync}"
PG=nvl-head-pg
HASURA=nvl-head-hasura
PGPORT=15434
HASURAPORT=18082
INDEXERPORT=18083

# True only for THIS repository's head-sync indexer: it is launched from $REPO with the
# head-sync Postgres port exported, and no other process on the host has both. A bare
# `pnpm run start` cmdline match would also hit any other envio project after pid reuse.
headsync_pid_alive() {
  local pid="$1"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 1
  [ "$(readlink -f "/proc/$pid/cwd" 2>/dev/null)" = "$REPO" ] || return 1
  tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep -qx "ENVIO_PG_PORT=$PGPORT"
}
