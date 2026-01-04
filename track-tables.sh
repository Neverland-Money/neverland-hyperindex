#!/bin/bash

# Track all Envio tables in Hasura

HASURA_URL="http://localhost:8080"
ADMIN_SECRET="neverland-hasura-2026-secure"

echo "Fetching all tables from database..."

# Get all table names
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

# Track each table
for table in $TABLES; do
  table=$(echo $table | xargs)  # Trim whitespace
  if [ ! -z "$table" ]; then
    echo "Tracking: $table"
    curl -s -X POST "$HASURA_URL/v1/metadata" \
      -H "x-hasura-admin-secret: $ADMIN_SECRET" \
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
curl -s -X POST "$HASURA_URL/v1/metadata" \
  -H "x-hasura-admin-secret: $ADMIN_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"type": "reload_metadata", "args": {}}' > /dev/null

echo "✓ Done! Check your GraphQL endpoint."
