#!/bin/bash
###############################################################################
# Frontend Cleanup Script
# Removes all frontend build artifacts and dependencies
###############################################################################

set -e

echo "🎨 Cleaning Frontend artifacts..."

if [ ! -d "frontend" ]; then
    echo "❌ Frontend directory not found!"
    exit 1
fi

cd frontend

echo "🗑️  Removing node_modules..."
rm -rf node_modules

echo "🗑️  Removing build artifacts..."
rm -rf dist
rm -rf .vite
rm -rf .turbo
rm -rf .next
rm -rf build

echo "🗑️  Removing lock files..."
rm -f package-lock.json
rm -f yarn.lock
rm -f pnpm-lock.yaml

echo "🗑️  Removing cache directories..."
rm -rf .cache
rm -rf .parcel-cache

echo "🗑️  Removing logs..."
rm -f npm-debug.log*
rm -f yarn-debug.log*
rm -f yarn-error.log*
rm -f lerna-debug.log*

cd ..

echo "🗑️  Removing deployed static files..."
rm -rf snowglobe_server/static

echo ""
echo "✅ Frontend cleanup complete!"
echo ""
echo "📦 To rebuild frontend, run:"
echo "   cd frontend && npm install && npm run build"
echo "   or: make frontend"
echo ""
