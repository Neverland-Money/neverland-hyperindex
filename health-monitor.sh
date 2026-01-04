#!/bin/bash
# Health monitoring script - run via cron every 5 minutes

HEALTH_ENDPOINT="http://localhost:8080/v1/graphql"
ADMIN_SECRET="H9bN8Q9waXiS"
LOG_FILE="/var/log/neverland-health.log"
COMPOSE_FILE="/home/catalyst/Documents/neverland/neverland-hyperindex/docker-compose.prod.yml"

# Test GraphQL endpoint
response=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$HEALTH_ENDPOINT" \
  -H "x-hasura-admin-secret: $ADMIN_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ __typename }"}' \
  --max-time 10)

if [ "$response" != "200" ]; then
  echo "[$(date)] ALERT: GraphQL endpoint returned $response - attempting restart" >> "$LOG_FILE"
  
  # Check which containers are down
  cd /home/catalyst/Documents/neverland/neverland-hyperindex
  down_containers=$(docker-compose -f "$COMPOSE_FILE" ps | grep -E "Exit|Restarting" || true)
  
  if [ ! -z "$down_containers" ]; then
    echo "[$(date)] Down containers: $down_containers" >> "$LOG_FILE"
    docker-compose -f "$COMPOSE_FILE" up -d
    echo "[$(date)] Restart command executed" >> "$LOG_FILE"
  else
    echo "[$(date)] All containers running but endpoint unresponsive" >> "$LOG_FILE"
  fi
else
  # Optionally log success (commented out to avoid log spam)
  # echo "[$(date)] OK: Health check passed" >> "$LOG_FILE"
  :
fi
