#!/bin/bash
# Tracks every public.* table in Hasura's GraphQL schema.
# Runs all HTTP calls from inside the docker network (no host port published).

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load admin secret from .env (no hardcoded fallbacks)
set -a
# shellcheck disable=SC1091
source "$REPO_DIR/.env"
set +a

if [ -z "${HASURA_ADMIN_SECRET:-}" ]; then
    echo "ERROR: HASURA_ADMIN_SECRET not set in $REPO_DIR/.env" >&2
    exit 1
fi

HASURA_URL="http://hasura:8080"

echo "Fetching all tables from database..."

TABLES=$(docker exec neverland-postgres psql -U postgres -d envio -t -c "
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name NOT LIKE 'pg_%'
    AND table_name NOT LIKE 'sql_%'
    AND table_name NOT IN ('spatial_ref_sys', 'geography_columns', 'geometry_columns', 'raster_columns', 'raster_overviews')
    ORDER BY table_name;
")

echo "Tracking tables in Hasura..."

for table in $TABLES; do
    table=$(echo "$table" | xargs)
    if [ -n "$table" ]; then
        echo "Tracking: $table"
        docker exec neverland-indexer curl -s -X POST "$HASURA_URL/v1/metadata" \
            -H "x-hasura-admin-secret: $HASURA_ADMIN_SECRET" \
            -H "Content-Type: application/json" \
            -d "{
                \"type\": \"pg_track_table\",
                \"args\": {
                    \"source\": \"default\",
                    \"table\": {
                        \"schema\": \"public\",
                        \"name\": \"$table\"
                    }
                }
            }" > /dev/null
    fi
done

echo ""
echo "✓ All tables tracked!"
echo ""
echo "Reloading metadata..."
docker exec neverland-indexer curl -s -X POST "$HASURA_URL/v1/metadata" \
    -H "x-hasura-admin-secret: $HASURA_ADMIN_SECRET" \
    -H "Content-Type: application/json" \
    -d '{"type": "reload_metadata", "args": {}}' > /dev/null

echo "✓ Done! Check your GraphQL endpoint."
