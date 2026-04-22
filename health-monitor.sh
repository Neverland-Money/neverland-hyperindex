#!/bin/bash
# Health monitoring script (optional; cron every 5 min).
#
# NOTE: In this deployment autoheal already restarts any unhealthy container,
# so this script is largely redundant. Kept for an app-layer GraphQL probe:
# it hits /v1/graphql end-to-end (through Hasura) and, if that fails, kicks
# the compose stack. Remove the cron entry if you'd rather trust autoheal alone.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="${NEVERLAND_HEALTH_LOG:-/var/log/neverland-health.log}"

# Pull admin secret from .env (no hardcoded fallbacks)
set -a
# shellcheck disable=SC1091
source "$REPO_DIR/.env"
set +a

if [ -z "${HASURA_ADMIN_SECRET:-}" ]; then
    echo "[$(date)] ERROR: HASURA_ADMIN_SECRET not set in $REPO_DIR/.env" >> "$LOG_FILE"
    exit 1
fi

# Run the probe from inside the docker network via the indexer container's curl.
# Avoids publishing Hasura on a host port.
response=$(docker exec neverland-indexer curl -s -o /dev/null -w "%{http_code}" \
    -X POST http://hasura:8080/v1/graphql \
    -H "x-hasura-admin-secret: $HASURA_ADMIN_SECRET" \
    -H "Content-Type: application/json" \
    -d '{"query": "{ __typename }"}' \
    --max-time 10 || echo "000")

if [ "$response" != "200" ]; then
    echo "[$(date)] ALERT: GraphQL probe returned $response — attempting restart" >> "$LOG_FILE"
    (cd "$REPO_DIR" && docker compose -f docker-compose.prod.yml up -d)
    echo "[$(date)] compose up -d executed" >> "$LOG_FILE"
fi
