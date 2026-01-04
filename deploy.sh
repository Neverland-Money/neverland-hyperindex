#!/bin/bash

set -e

echo "=================================="
echo "Neverland HyperIndex Deployment"
echo "=================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

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

# Check required variables
source .env

if [ -z "$ENVIO_API_TOKEN" ] || [ "$ENVIO_API_TOKEN" = "your_envio_api_token_here" ]; then
    echo -e "${RED}❌ ENVIO_API_TOKEN not set in .env${NC}"
    exit 1
fi

if [ -z "$CLOUDFLARE_TUNNEL_TOKEN" ] || [ "$CLOUDFLARE_TUNNEL_TOKEN" = "your_cloudflare_tunnel_token_here" ]; then
    echo -e "${RED}❌ CLOUDFLARE_TUNNEL_TOKEN not set in .env${NC}"
    exit 1
fi

if [ -z "$POSTGRES_PASSWORD" ] || [ "$POSTGRES_PASSWORD" = "your_secure_postgres_password_here" ]; then
    echo -e "${RED}❌ POSTGRES_PASSWORD not set in .env${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Environment configuration validated${NC}"
echo ""

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker not found. Please install Docker first.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose not found. Please install Docker Compose first.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker and Docker Compose found${NC}"
echo ""

# Start deployment
echo "Starting deployment..."
echo ""

# Pull latest images
echo "Pulling Docker images..."
docker-compose -f docker-compose.prod.yml pull

# Start services
echo "Starting services..."
docker-compose -f docker-compose.prod.yml up -d

echo ""
echo -e "${GREEN}=================================="
echo "✓ Deployment Complete!"
echo "==================================${NC}"
echo ""
echo "Services status:"
docker-compose -f docker-compose.prod.yml ps
echo ""
echo "Next steps:"
echo "  1. Check logs: docker-compose -f docker-compose.prod.yml logs -f"
echo "  2. Wait for indexer to sync (may take several minutes)"
echo "  3. Access GraphQL endpoint via your Cloudflare Tunnel URL"
echo ""
echo "Useful commands:"
echo "  - View logs: docker-compose -f docker-compose.prod.yml logs -f indexer"
echo "  - Stop: docker-compose -f docker-compose.prod.yml down"
echo "  - Restart: docker-compose -f docker-compose.prod.yml restart"
echo ""
echo "For more information, see DEPLOYMENT.md"
