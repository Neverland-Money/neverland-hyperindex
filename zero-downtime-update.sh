#!/bin/bash

set -e

echo "========================================"
echo "Production Upgrade"
echo "========================================"
echo ""

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Step 1: Pull latest code
echo -e "${BLUE}Step 1/8: Pulling latest code from repository...${NC}"
git fetch && git pull
if [ $? -ne 0 ]; then
  echo -e "${RED}✗ Git pull failed${NC}"
  echo "Please resolve any git conflicts and try again."
  exit 1
fi
echo -e "${GREEN}✓ Code updated${NC}"
echo ""

# Step 2: Clean and start staging environment
echo -e "${BLUE}Step 2/8: Starting fresh staging environment...${NC}"
echo "Cleaning up any existing staging containers and volumes..."
docker-compose -f docker-compose.staging.yml down -v 2>/dev/null || true
echo ""
echo "Starting staging (this will compile the latest code)..."
docker-compose -f docker-compose.staging.yml up -d
echo -e "${GREEN}✓ Staging started${NC}"
echo ""

# Step 3: Wait for staging to be healthy
echo -e "${BLUE}Step 3/8: Waiting for staging services to be healthy...${NC}"
echo "This may take 30-60 seconds..."
sleep 30

# Wait for Hasura staging to be healthy
until [ "$(docker inspect -f {{.State.Health.Status}} neverland-hasura-staging 2>/dev/null)" == "healthy" ]; do
  echo "Waiting for Hasura staging to be healthy..."
  sleep 5
done
echo -e "${GREEN}✓ Staging services healthy${NC}"
echo ""

# Step 4: Monitor staging sync
echo -e "${BLUE}Step 4/8: Waiting for staging to sync...${NC}"
echo "You can monitor in another terminal with:"
echo "  docker-compose -f docker-compose.staging.yml logs -f indexer-staging"
echo ""
echo "Waiting for sync to complete (checking every 30 seconds)..."
echo "Press Ctrl+C if you want to check manually and continue when ready."
echo ""

# Wait for sync to complete (look for "synced" in logs)
while true; do
  if docker-compose -f docker-compose.staging.yml logs --tail=100 indexer-staging 2>/dev/null | grep -q "synced"; then
    echo -e "${GREEN}✓ Staging indexer is synced!${NC}"
    break
  fi
  
  # Check if indexer is still running
  if [ "$(docker inspect -f {{.State.Status}} neverland-indexer-staging 2>/dev/null)" != "running" ]; then
    echo -e "${RED}✗ Staging indexer stopped unexpectedly${NC}"
    echo "Check logs with: docker-compose -f docker-compose.staging.yml logs indexer-staging"
    exit 1
  fi
  
  echo "Still syncing... (check logs for progress)"
  sleep 30
done
echo ""

# Step 5: Verify staging data
echo -e "${BLUE}Step 5/8: Verifying staging data...${NC}"
response=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8081/v1/graphql \
  -H "x-hasura-admin-secret: ${HASURA_ADMIN_SECRET:-H9bN8Q9waXiS}" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ User(limit: 1) { id } }"}' \
  --max-time 10)

if [ "$response" == "200" ]; then
  echo -e "${GREEN}✓ Staging GraphQL endpoint is working${NC}"
else
  echo -e "${RED}✗ Staging GraphQL endpoint returned HTTP $response${NC}"
  echo "Please verify staging manually before continuing."
  exit 1
fi
echo ""

# Step 6: Confirm switch
echo -e "${YELLOW}Step 6/8: Ready to switch to new production${NC}"
echo "This will:"
echo "  1. Stop production services (~5 seconds downtime starts)"
echo "  2. Backup production database volume"
echo "  3. Replace production volume with staging volume"
echo "  4. Rename database from 'envio_staging' to 'envio'"
echo "  5. Start production with new database (~10 seconds total downtime)"
echo ""
read -p "Continue with the switch? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Cancelled. Staging is still running on ports 8081, 5433, 9091"
  exit 0
fi
echo ""

# Step 7: Stop production and swap volumes
echo -e "${BLUE}Step 7/8: Swapping production to staging database...${NC}"
echo "Stopping production..."
docker-compose -f docker-compose.prod.yml down

echo "Backing up production database volume..."
docker volume rm neverland-hyperindex_postgres_data_backup_${TIMESTAMP} 2>/dev/null || true
docker volume create neverland-hyperindex_postgres_data_backup_${TIMESTAMP}
docker run --rm \
  -v neverland-hyperindex_postgres_data:/from \
  -v neverland-hyperindex_postgres_data_backup_${TIMESTAMP}:/to \
  alpine sh -c "cd /from && cp -a . /to" 2>/dev/null || true

echo "Replacing production volume with staging volume..."
docker volume rm neverland-hyperindex_postgres_data
docker volume create neverland-hyperindex_postgres_data
docker run --rm \
  -v neverland-hyperindex_postgres_data_staging:/from \
  -v neverland-hyperindex_postgres_data:/to \
  alpine sh -c "cd /from && cp -a . /to"

echo "Starting production with new database..."
docker-compose -f docker-compose.prod.yml up -d postgres
sleep 5

echo "Renaming database from 'envio_staging' to 'envio'..."
docker exec -i neverland-postgres psql -U postgres -c "ALTER DATABASE envio_staging RENAME TO envio;" 2>/dev/null || echo "(Database may already be named 'envio')"

echo "Starting all production services..."
docker-compose -f docker-compose.prod.yml up -d

echo -e "${GREEN}✓ Production switched to new database${NC}"
echo ""

# Step 8: Verify production
echo -e "${BLUE}Step 8/8: Verifying production...${NC}"
sleep 10

response=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8080/v1/graphql \
  -H "x-hasura-admin-secret: ${HASURA_ADMIN_SECRET:-H9bN8Q9waXiS}" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ User(limit: 1) { id } }"}' \
  --max-time 10)

if [ "$response" == "200" ]; then
  echo -e "${GREEN}✓ Production GraphQL endpoint is working${NC}"
else
  echo -e "${RED}✗ Production GraphQL endpoint returned HTTP $response${NC}"
  echo "You may need to check production logs."
fi
echo ""

# Cleanup instructions
echo -e "${GREEN}========================================"
echo "✓ Production upgrade complete!"
echo -e "========================================${NC}"
echo ""
echo "Next steps:"
echo ""
echo "1. Verify production is working correctly"
echo "2. Clean up staging when satisfied:"
echo "   docker-compose -f docker-compose.staging.yml down -v"
echo ""
echo "3. Clean up old backups when satisfied:"
echo "   docker volume rm neverland-hyperindex_postgres_data_backup_${TIMESTAMP}"
echo ""
echo "Rollback instructions (if needed):"
echo "  docker-compose -f docker-compose.prod.yml down"
echo "  docker volume rm neverland-hyperindex_postgres_data"
echo "  docker volume create neverland-hyperindex_postgres_data"
echo "  docker run --rm -v neverland-hyperindex_postgres_data_backup_${TIMESTAMP}:/from -v neverland-hyperindex_postgres_data:/to alpine sh -c 'cd /from && cp -a . /to'"
echo "  docker exec -i neverland-postgres psql -U postgres -c 'ALTER DATABASE envio_staging RENAME TO envio;' || true"
echo "  docker-compose -f docker-compose.prod.yml up -d"
echo ""
