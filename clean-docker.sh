#!/bin/bash
###############################################################################
# Docker-Specific Cleanup Script
# Removes all Docker-related artifacts for Snowglobe
###############################################################################

set -e

echo "🐋 Cleaning Docker environment for Snowglobe..."

# Detect docker compose command
if command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    DOCKER_COMPOSE="docker compose"
fi

# Stop containers
echo "⏹️  Stopping containers..."
$DOCKER_COMPOSE down -v 2>/dev/null || true

# Remove containers by name
echo "🗑️  Removing Snowglobe containers..."
docker ps -a | grep -E "snowglobe" | awk '{print $1}' | xargs -r docker rm -f 2>/dev/null || true

# Remove images by name
echo "🗑️  Removing Snowglobe images..."
docker images | grep -E "snowglobe" | awk '{print $3}' | xargs -r docker rmi -f 2>/dev/null || true

# Clean build cache
echo "🗑️  Cleaning Docker build cache..."
docker builder prune -af 2>/dev/null || true

# Remove dangling images
echo "🗑️  Removing dangling images..."
docker image prune -f 2>/dev/null || true

# Remove unused volumes
echo "🗑️  Removing unused volumes..."
docker volume ls | grep -E "snowglobe" | awk '{print $2}' | xargs -r docker volume rm 2>/dev/null || true

# Remove unused networks
echo "🗑️  Removing unused networks..."
docker network prune -f 2>/dev/null || true

# Show remaining docker resources
echo ""
echo "✅ Docker cleanup complete!"
echo ""
echo "📊 Remaining Docker resources:"
echo ""
echo "Images:"
docker images | grep -E "snowglobe|REPOSITORY" || echo "  No Snowglobe images found"
echo ""
echo "Containers:"
docker ps -a | grep -E "snowglobe|CONTAINER" || echo "  No Snowglobe containers found"
echo ""
echo "Volumes:"
docker volume ls | grep -E "snowglobe|DRIVER" || echo "  No Snowglobe volumes found"
echo ""
