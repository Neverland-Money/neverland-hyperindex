#!/usr/bin/env bash
# Live sync progress for the running indexer.
#
# `docker compose logs -f indexer` shows nothing after startup: envio draws progress as a TUI,
# and the container runs without a TTY, so there is no terminal to draw to. The Prometheus
# endpoint carries the same numbers and is the reliable source.
#
#   pnpm run prod:progress          # refresh every 5s
#   pnpm run prod:progress -- 30    # refresh every 30s
set -uo pipefail
URL="${ENVIO_METRICS_URL:-http://127.0.0.1:9090/metrics}"
INTERVAL="${1:-5}"

metric() { awk -v k="$1" '$0 ~ "^"k"[{ ]" {v=$NF} END{print (v==""?"":v)}' <<<"$2"; }

prev_logs=""; prev_push=""; prev_time=""; prev_block=""
printf '%-8s  %-12s  %-7s  %-11s  %-10s  %-10s  %-9s  %s\n' \
  TIME BLOCK PCT EVENTS getLogs/m push/m BLOCKS/s STATE
while true; do
  snap=$(curl -s --max-time 10 "$URL" 2>/dev/null) || snap=""
  if [ -z "$snap" ]; then
    printf '%-8s  %s\n' "$(date +%H:%M:%S)" "metrics endpoint unreachable at $URL"
    sleep "$INTERVAL"; continue
  fi

  block=$(metric envio_progress_block "$snap")
  events=$(metric envio_progress_events "$snap")
  ready=$(metric envio_progress_ready "$snap")
  # Anchor on the exact series. A loose /getLogs/ match also hits
  # envio_source_request_seconds_total and reports a delta of SECONDS as if it were requests.
  logs=$(awk '/^envio_source_request_total\{.*method="getLogs"\}/ {v=$NF} END{printf "%d", v+0}' <<<"$snap")
  push=$(awk '/^envio_source_request_total\{.*method="heightPush"\}/ {v=$NF} END{printf "%d", v+0}' <<<"$snap")
  now=$(date +%s)

  rpm="-"; push_pm="-"; bps="-"
  if [ -n "$prev_time" ] && [ "$now" -gt "$prev_time" ]; then
    dt=$((now - prev_time))
    rpm=$(awk -v a="$logs" -v b="$prev_logs" -v d="$dt" 'BEGIN{printf "%.1f",(a-b)*60/d}')
    push_pm=$(awk -v a="$push" -v b="$prev_push" -v d="$dt" 'BEGIN{printf "%.1f",(a-b)*60/d}')
    bps=$(awk -v a="$block" -v b="$prev_block" -v d="$dt" 'BEGIN{printf "%.0f",(a-b)/d}')
  fi
  prev_logs=$logs; prev_push=$push; prev_time=$now; prev_block=$block

  # Chain head as the source reports it; hyperindex_synced_to_head is the caught-up flag.
  head=$(metric envio_source_known_height "$snap")
  [ -z "${head:-}" ] && head=$(metric envio_indexing_known_height "$snap")
  pct="-"
  if [ -n "${head:-}" ] && [ "${head%.*}" -gt 0 ] 2>/dev/null; then
    pct=$(awk -v a="$block" -v b="$head" 'BEGIN{printf "%.2f%%",100*a/b}')
  fi

  synced=$(metric hyperindex_synced_to_head "$snap")
  if [ "${synced%.*}" = "1" ] || [ "${ready%.*}" = "1" ]; then state=SYNCED; else state=syncing; fi
  printf '%-8s  %-12s  %-7s  %-11s  %-10s  %-10s  %-9s  %s\n' \
    "$(date +%H:%M:%S)" "${block:--}" "$pct" "${events:--}" "$rpm" "$push_pm" "$bps" "$state"
  sleep "$INTERVAL"
done
