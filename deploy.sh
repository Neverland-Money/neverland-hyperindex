#!/bin/bash

set -e

echo "=================================="
echo "Neverland HyperIndex Deployment"
echo "=================================="
echo ""

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

cd "$(dirname "$0")"

# Check if .env exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}⚠️  .env file not found${NC}"
    echo "Creating from template..."
    cp .env.production .env
    echo -e "${RED}❌ Please edit .env with your actual values before continuing${NC}"
    echo ""
    echo "Required variables:"
    echo "  - ENVIO_API_TOKEN (from https://envio.dev/app/api-keys)"
    echo "  - CLOUDFLARE_TUNNEL_TOKEN (from Cloudflare Zero Trust Dashboard)"
    echo "  - POSTGRES_PASSWORD (a strong password)"
    echo ""
    exit 1
fi

# Load .env for required-var check
set -a
source .env
set +a

for var in ENVIO_API_TOKEN CLOUDFLARE_TUNNEL_TOKEN POSTGRES_PASSWORD HASURA_ADMIN_SECRET; do
    placeholder_pattern="your_.*_here"
    value="${!var:-}"
    if [ -z "$value" ] || [[ "$value" =~ $placeholder_pattern ]]; then
        echo -e "${RED}❌ $var not set (or still a placeholder) in .env${NC}"
        exit 1
    fi
done
echo -e "${GREEN}✓ Environment configuration validated${NC}"
echo ""

# Check Docker + Compose v2 plugin
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker not found. Please install Docker first.${NC}"
    exit 1
fi
if ! docker compose version &> /dev/null; then
    echo -e "${RED}❌ Docker Compose v2 plugin not found. Install from https://docs.docker.com/compose/install/${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker + Compose v2 found${NC}"
echo ""

echo "Pulling Docker images..."
docker compose -f docker-compose.prod.yml pull

echo "Starting services..."
docker compose -f docker-compose.prod.yml up -d

echo ""
echo -e "${GREEN}=================================="
echo "✓ Deployment Complete!"
echo "==================================${NC}"
echo ""
echo "Services status:"
docker compose -f docker-compose.prod.yml ps
echo ""
echo "Tip: On this server, the stack is normally managed by systemd."
echo "  sudo systemctl status neverland-indexer"
echo "  docker logs -f neverland-indexer   # watch sync"
echo ""
