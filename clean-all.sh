#!/bin/bash
###############################################################################
# Snowglobe Complete Cleanup Script
# This script removes all build artifacts, caches, and temporary files
# to ensure a fresh build environment
###############################################################################

set -e  # Exit on error

echo "🧹 Starting Snowglobe Complete Cleanup..."
echo "=========================================="

# Detect docker compose command
if command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    DOCKER_COMPOSE="docker compose"
fi

# Function to safely remove directory/file
safe_remove() {
    if [ -e "$1" ]; then
        echo "  🗑️  Removing: $1"
        rm -rf "$1"
    fi
}

# 1. Stop and remove all Docker containers, volumes, and images
echo ""
echo "📦 Cleaning Docker environment..."
echo "  ⏹️  Stopping containers..."
$DOCKER_COMPOSE down -v 2>/dev/null || true

echo "  🗑️  Removing Snowglobe containers..."
docker ps -a | grep snowglobe | awk '{print $1}' | xargs -r docker rm -f 2>/dev/null || true

echo "  🗑️  Removing Snowglobe images..."
docker images | grep snowglobe | awk '{print $3}' | xargs -r docker rmi -f 2>/dev/null || true

echo "  🗑️  Removing dangling images..."
docker image prune -f 2>/dev/null || true

echo "  🗑️  Removing unused volumes..."
docker volume prune -f 2>/dev/null || true

# 2. Clean Python artifacts
echo ""
echo "🐍 Cleaning Python artifacts..."
safe_remove "__pycache__"
safe_remove ".pytest_cache"
safe_remove ".coverage"
safe_remove "htmlcov"
safe_remove ".tox"
safe_remove "dist"
safe_remove "build"
safe_remove "*.egg-info"
safe_remove ".eggs"

find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
find . -type f -name "*.pyo" -delete 2>/dev/null || true
find . -type f -name "*.coverage" -delete 2>/dev/null || true

# 3. Clean Node.js/Frontend artifacts
echo ""
echo "📦 Cleaning Frontend artifacts..."
if [ -d "frontend" ]; then
    cd frontend
    safe_remove "node_modules"
    safe_remove "dist"
    safe_remove ".vite"
    safe_remove "package-lock.json"
    safe_remove "yarn.lock"
    safe_remove "pnpm-lock.yaml"
    safe_remove ".turbo"
    safe_remove ".next"
    cd ..
fi

# 4. Clean data directories (with confirmation for data)
echo ""
echo "💾 Cleaning data directories..."
if [ -d "data" ]; then
    read -p "⚠️  Remove local data directory? This will delete all databases! (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        safe_remove "data"
        echo "  ✅ Data directory removed"
    else
        echo "  ⏭️  Skipping data directory"
    fi
fi

# 5. Clean logs
echo ""
echo "📝 Cleaning log files..."
safe_remove "logs"
find . -type f -name "*.log" -delete 2>/dev/null || true

# 6. Clean certificates (if you want to regenerate)
echo ""
read -p "🔐 Remove SSL certificates? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    safe_remove "certs"
    echo "  ✅ Certificates removed"
else
    echo "  ⏭️  Keeping certificates"
fi

# 7. Clean backup files
echo ""
echo "💾 Cleaning backup files..."
safe_remove "backups/*.tar.gz" 2>/dev/null || true

# 8. Clean system cache files
echo ""
echo "🗂️  Cleaning system cache files..."
safe_remove ".DS_Store"
find . -type f -name ".DS_Store" -delete 2>/dev/null || true
find . -type f -name "._*" -delete 2>/dev/null || true
find . -type f -name "Thumbs.db" -delete 2>/dev/null || true
find . -type f -name "*.swp" -delete 2>/dev/null || true
find . -type f -name "*.swo" -delete 2>/dev/null || true
find . -type f -name "*~" -delete 2>/dev/null || true

# 9. Clean build artifacts
echo ""
echo "🔨 Cleaning build artifacts..."
safe_remove ".build"
safe_remove "tmp"
safe_remove "temp"

# 10. Show disk space freed (if available)
echo ""
echo "✅ Cleanup Complete!"
echo "=========================================="
echo ""
echo "📊 Summary:"
echo "  ✓ Docker containers and images removed"
echo "  ✓ Python artifacts cleaned"
echo "  ✓ Frontend build files cleaned"
echo "  ✓ Cache and temporary files removed"
echo ""
echo "🚀 You can now run a fresh build with:"
echo "   make build"
echo "   make start"
echo ""
