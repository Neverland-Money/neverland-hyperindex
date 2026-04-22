#!/bin/bash
#
# Minimal-downtime upgrade / resync of the indexer stack.
#
# Flow:
#   1. git pull latest code
#   2. Bring up a parallel "staging" stack (docker-compose.staging.yml) with
#      its own postgres / hasura / indexer on isolated volumes + localhost ports.
#   3. Wait for staging indexer to finish syncing ("synced" in logs).
#   4. Ask for confirmation.
#   5. Stop prod → back up prod volume → copy staging volume into prod volume →
#      rename envio_staging → envio → start prod.
#   6. Verify prod via internal docker network (Hasura isn't published).
#
# Real downtime during the swap is ~10s (prod containers stop, volume copy,
# prod containers start). True zero-downtime would require flipping the
# Cloudflare Tunnel ingress route live, which is a bigger change.
#
# Run from this directory, as a user with docker group membership
# (catalyst or deploy). No sudo needed.

set -euo pipefail

cd "$(dirname "$0")"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# --- Config derived from the working directory (so this script is portable) ---
PROJECT="$(basename "$PWD")"                 # e.g. "indexer"
PROD_VOLUME="${PROJECT}_postgres_data"        # e.g. "indexer_postgres_data"
STAGING_VOLUME="${PROJECT}_postgres_data_staging"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP_VOLUME="${PROD_VOLUME}_backup_${TIMESTAMP}"

PROD_COMPOSE="docker-compose.prod.yml"
STAGING_COMPOSE="docker-compose.staging.yml"

# Load .env (for HASURA_ADMIN_SECRET, POSTGRES_*, etc.)
if [ ! -f .env ]; then
    echo -e "${RED}✗ .env not found in $PWD${NC}" >&2
    exit 1
fi
set -a
# shellcheck disable=SC1091
source .env
set +a

if [ -z "${HASURA_ADMIN_SECRET:-}" ]; then
    echo -e "${RED}✗ HASURA_ADMIN_SECRET not set in .env${NC}" >&2
    exit 1
fi

echo "========================================"
echo "Indexer minimal-downtime upgrade"
echo "  project:        $PROJECT"
echo "  prod volume:    $PROD_VOLUME"
echo "  staging volume: $STAGING_VOLUME"
echo "  backup volume:  $BACKUP_VOLUME"
echo "========================================"
echo ""

# --- Step 1: Pull latest code ---
echo -e "${BLUE}Step 1/8: Pulling latest code...${NC}"
git fetch && git pull
echo -e "${GREEN}✓ Code updated${NC}"
echo ""

# --- Step 2: Clean and start staging ---
echo -e "${BLUE}Step 2/8: Starting a fresh staging stack...${NC}"
docker compose -f "$STAGING_COMPOSE" down -v 2>/dev/null || true
docker compose -f "$STAGING_COMPOSE" up -d
echo -e "${GREEN}✓ Staging started${NC}"
echo ""

# --- Step 3: Wait for staging hasura healthy ---
echo -e "${BLUE}Step 3/8: Waiting for staging Hasura to become healthy...${NC}"
for _ in $(seq 1 60); do
    status=$(docker inspect -f '{{.State.Health.Status}}' neverland-hasura-staging 2>/dev/null || echo "starting")
    [ "$status" = "healthy" ] && break
    echo "  Hasura staging: $status"
    sleep 5
done
if [ "$(docker inspect -f '{{.State.Health.Status}}' neverland-hasura-staging 2>/dev/null)" != "healthy" ]; then
    echo -e "${RED}✗ Staging Hasura did not become healthy in time${NC}" >&2
    exit 1
fi
echo -e "${GREEN}✓ Staging Hasura healthy${NC}"
echo ""

# --- Step 4: Wait for staging indexer to sync ---
echo -e "${BLUE}Step 4/8: Waiting for staging indexer to finish syncing...${NC}"
echo "  Tail in another terminal:"
echo "    docker logs -f neverland-indexer-staging"
echo ""
while true; do
    if docker logs --tail 200 neverland-indexer-staging 2>&1 | grep -qi "synced"; then
        echo -e "${GREEN}✓ Staging indexer synced${NC}"
        break
    fi
    if [ "$(docker inspect -f '{{.State.Status}}' neverland-indexer-staging 2>/dev/null)" != "running" ]; then
        echo -e "${RED}✗ Staging indexer stopped unexpectedly${NC}" >&2
        echo "  docker logs neverland-indexer-staging" >&2
        exit 1
    fi
    echo "  Still syncing..."
    sleep 30
done
echo ""

# --- Step 5: Verify staging via internal docker network ---
echo -e "${BLUE}Step 5/8: Verifying staging GraphQL endpoint...${NC}"
staging_code=$(docker exec neverland-indexer-staging curl -s -o /dev/null -w "%{http_code}" \
    -X POST http://hasura-staging:8080/v1/graphql \
    -H "x-hasura-admin-secret: $HASURA_ADMIN_SECRET" \
    -H "Content-Type: application/json" \
    -d '{"query": "{ __typename }"}' \
    --max-time 10 || echo "000")

if [ "$staging_code" = "200" ]; then
    echo -e "${GREEN}✓ Staging GraphQL OK${NC}"
else
    echo -e "${RED}✗ Staging GraphQL returned HTTP $staging_code${NC}" >&2
    echo "  Verify manually then re-run or continue by hand." >&2
    exit 1
fi
echo ""

# --- Step 6: Confirm swap ---
echo -e "${YELLOW}Step 6/8: Ready to swap prod → staging volume.${NC}"
cat <<EOF

  This will:
    1. Stop prod containers  (~10s GraphQL outage starts)
    2. Back up current prod volume as:   $BACKUP_VOLUME
    3. Replace prod volume contents with staging volume contents
    4. Rename DB  envio_staging → envio  in the new prod volume
    5. Bring prod back up     (~10s outage ends)

EOF
read -r -p "Continue? (y/n) " REPLY
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled. Staging is still running (volumes intact)."
    exit 0
fi
echo ""

# --- Step 7: Swap ---
echo -e "${BLUE}Step 7/8: Performing swap...${NC}"
echo "  Stopping prod..."
docker compose -f "$PROD_COMPOSE" down

echo "  Creating backup volume: $BACKUP_VOLUME"
docker volume create "$BACKUP_VOLUME" >/dev/null
docker run --rm \
    -v "$PROD_VOLUME":/from:ro \
    -v "$BACKUP_VOLUME":/to \
    alpine sh -c 'cd /from && cp -a . /to'

echo "  Replacing prod volume from staging volume..."
docker volume rm "$PROD_VOLUME" >/dev/null
docker volume create "$PROD_VOLUME" >/dev/null
docker run --rm \
    -v "$STAGING_VOLUME":/from:ro \
    -v "$PROD_VOLUME":/to \
    alpine sh -c 'cd /from && cp -a . /to'

echo "  Starting prod postgres only..."
docker compose -f "$PROD_COMPOSE" up -d postgres

echo "  Waiting for prod postgres healthy..."
for _ in $(seq 1 30); do
    status=$(docker inspect -f '{{.State.Health.Status}}' neverland-postgres 2>/dev/null || echo "starting")
    [ "$status" = "healthy" ] && break
    sleep 2
done

echo "  Renaming envio_staging → envio (if needed)..."
docker exec -i neverland-postgres psql -U postgres -c \
    "ALTER DATABASE envio_staging RENAME TO envio;" 2>/dev/null || true

echo "  Starting remaining prod services..."
docker compose -f "$PROD_COMPOSE" up -d
echo -e "${GREEN}✓ Prod restarted with swapped database${NC}"
echo ""

# --- Step 8: Verify prod via internal network ---
echo -e "${BLUE}Step 8/8: Verifying prod GraphQL endpoint...${NC}"
sleep 10
prod_code=$(docker exec neverland-indexer curl -s -o /dev/null -w "%{http_code}" \
    -X POST http://hasura:8080/v1/graphql \
    -H "x-hasura-admin-secret: $HASURA_ADMIN_SECRET" \
    -H "Content-Type: application/json" \
    -d '{"query": "{ __typename }"}' \
    --max-time 10 || echo "000")
if [ "$prod_code" = "200" ]; then
    echo -e "${GREEN}✓ Prod GraphQL OK${NC}"
else
    echo -e "${RED}✗ Prod GraphQL returned HTTP $prod_code${NC}" >&2
    echo "  Check:  docker logs neverland-hasura" >&2
fi
echo ""

echo -e "${GREEN}========================================"
echo "✓ Upgrade complete!"
echo -e "========================================${NC}"
cat <<EOF

Cleanup when you're satisfied:
  docker compose -f $STAGING_COMPOSE down -v
  docker volume rm $BACKUP_VOLUME

Rollback (only if you need to revert):
  docker compose -f $PROD_COMPOSE down
  docker volume rm $PROD_VOLUME
  docker volume create $PROD_VOLUME
  docker run --rm -v $BACKUP_VOLUME:/from:ro -v $PROD_VOLUME:/to alpine sh -c 'cd /from && cp -a . /to'
  docker compose -f $PROD_COMPOSE up -d

EOF
