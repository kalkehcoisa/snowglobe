.PHONY: help start stop restart logs build clean clean-all clean-docker clean-frontend clean-python clean-cache rebuild certs test frontend

# Default target
help:
	@echo "❄️  Snowglobe - Local Snowflake Emulator"
	@echo "========================================"
	@echo ""
	@echo "Available commands:"
	@echo ""
	@echo "🚀 Starting & Running:"
	@echo "  make start          - Start Snowglobe (builds if needed)"
	@echo "  make stop           - Stop Snowglobe"
	@echo "  make restart        - Restart Snowglobe"
	@echo "  make logs           - View server logs"
	@echo ""
	@echo "🔨 Building:"
	@echo "  make build          - Build Docker image (with pre-checks)"
	@echo "  make build-fast     - Build Docker image (with cache, faster)"
	@echo "  make rebuild        - Clean and rebuild everything"
	@echo "  make frontend       - Build frontend"
	@echo ""
	@echo "🧹 Cleaning:"
	@echo "  make clean          - Stop and remove containers/volumes"
	@echo "  make clean-all      - Complete cleanup (Docker, Python, frontend, cache)"
	@echo "  make clean-docker   - Clean Docker artifacts only"
	@echo "  make clean-frontend - Clean frontend build artifacts"
	@echo "  make clean-python   - Clean Python artifacts (__pycache__, etc.)"
	@echo "  make clean-cache    - Clean all cache files"
	@echo ""
	@echo "🧪 Testing & Tools:"
	@echo "  make test           - Run tests"
	@echo "  make certs          - Generate SSL certificates"
	@echo "  make quickstart     - Quick start with all setup"
	@echo "  make status         - Show container status"
	@echo "  make health         - Check server health"
	@echo ""

# Detect docker compose command
DOCKER_COMPOSE := $(shell if command -v docker-compose >/dev/null 2>&1; then echo "docker-compose"; else echo "docker compose"; fi)

# Start Snowglobe
start:
	@echo "🚀 Starting Snowglobe..."
	@$(DOCKER_COMPOSE) up -d
	@echo "✅ Snowglobe is running!"
	@echo ""
	@echo "🌐 Access the dashboard:"
	@echo "   HTTPS: https://localhost:8443/dashboard"
	@echo "   HTTP:  http://localhost:8084/dashboard"

# Stop Snowglobe
stop:
	@echo "🛑 Stopping Snowglobe..."
	@$(DOCKER_COMPOSE) down
	@echo "✅ Snowglobe stopped"

# Restart Snowglobe
restart:
	@echo "🔄 Restarting Snowglobe..."
	@$(DOCKER_COMPOSE) restart
	@echo "✅ Snowglobe restarted"

# View logs
logs:
	@$(DOCKER_COMPOSE) logs -f

# Build Docker image
build:
	@echo "🔨 Building Snowglobe Docker image..."
	@./pre-build.sh
	@$(DOCKER_COMPOSE) build --no-cache
	@echo "✅ Build complete"

# Quick build (with cache)
build-fast:
	@echo "🔨 Building Snowglobe Docker image (with cache)..."
	@$(DOCKER_COMPOSE) build
	@echo "✅ Build complete"

# Clean Docker containers and volumes
clean:
	@echo "🧹 Cleaning up Snowglobe containers and volumes..."
	@$(DOCKER_COMPOSE) down -v
	@echo "✅ Cleanup complete"

# Complete cleanup - everything!
clean-all:
	@echo "🧹 Running complete cleanup..."
	@./clean-all.sh

# Clean Docker artifacts only
clean-docker:
	@echo "🧹 Cleaning Docker artifacts..."
	@./clean-docker.sh

# Clean frontend artifacts
clean-frontend:
	@echo "🧹 Cleaning frontend artifacts..."
	@./clean-frontend.sh

# Clean Python artifacts
clean-python:
	@echo "🧹 Cleaning Python artifacts..."
	@echo "  Removing __pycache__ directories..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "  Removing .pyc files..."
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "  Removing .pyo files..."
	@find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@echo "  Removing egg-info directories..."
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "  Removing pytest cache..."
	@rm -rf .pytest_cache
	@echo "  Removing coverage files..."
	@rm -rf htmlcov .coverage
	@echo "  Removing build directories..."
	@rm -rf build dist .eggs
	@echo "✅ Python artifacts cleaned"

# Clean cache files
clean-cache:
	@echo "🧹 Cleaning cache files..."
	@find . -name ".DS_Store" -delete 2>/dev/null || true
	@find . -name "._*" -delete 2>/dev/null || true
	@find . -name "Thumbs.db" -delete 2>/dev/null || true
	@find . -name "*.swp" -delete 2>/dev/null || true
	@find . -name "*.swo" -delete 2>/dev/null || true
	@find . -name "*~" -delete 2>/dev/null || true
	@echo "✅ Cache files cleaned"

# Rebuild everything from scratch
rebuild: clean-all
	@echo "🔨 Rebuilding everything from scratch..."
	@echo ""
	@echo "📦 Installing frontend dependencies..."
	@cd frontend && npm install
	@echo ""
	@echo "🎨 Building frontend..."
	@cd frontend && npm run build
	@echo ""
	@echo "📂 Copying frontend build to server..."
	@rm -rf snowglobe_server/static
	@cp -r frontend/dist snowglobe_server/static
	@echo ""
	@echo "🐋 Building Docker image..."
	@$(DOCKER_COMPOSE) build --no-cache
	@echo ""
	@echo "✅ Rebuild complete!"
	@echo ""
	@echo "🚀 Start with: make start"

# Generate SSL certificates
certs:
	@echo "🔐 Generating SSL certificates..."
	@./generate-certs.sh

# Run tests
test:
	@echo "🧪 Running tests..."
	@pytest tests/ -v

# Build frontend
frontend:
	@echo "🎨 Building frontend..."
	@cd frontend && npm install && npm run build
	@echo "📂 Copying frontend build to server..."
	@rm -rf snowglobe_server/static
	@cp -r frontend/dist snowglobe_server/static
	@echo "✅ Frontend built and deployed to snowglobe_server/static"

# Quick start
quickstart:
	@./quickstart.sh

# Show status
status:
	@$(DOCKER_COMPOSE) ps

# Shell into container
shell:
	@$(DOCKER_COMPOSE) exec snowglobe /bin/bash

# View health
health:
	@echo "🏥 Checking Snowglobe health..."
	@curl -k -s https://localhost:8443/health | python3 -m json.tool || curl -s http://localhost:8084/health | python3 -m json.tool

# Install Python dependencies
install:
	@echo "📦 Installing Python dependencies..."
	@pip install -r requirements-server.txt
	@echo "✅ Dependencies installed"

# Run locally (without Docker)
run-local:
	@echo "🏃 Running Snowglobe locally..."
	@python -m snowglobe_server.server

# Database backup
backup:
	@echo "💾 Creating backup..."
	@mkdir -p backups
	@$(DOCKER_COMPOSE) exec -T snowglobe tar -czf - /data > backups/snowglobe-backup-$$(date +%Y%m%d-%H%M%S).tar.gz
	@echo "✅ Backup created in backups/"

# Database restore
restore:
	@echo "📥 Restoring from backup..."
	@if [ -z "$(BACKUP)" ]; then \
		echo "❌ Please specify BACKUP file: make restore BACKUP=backups/file.tar.gz"; \
		exit 1; \
	fi
	@cat $(BACKUP) | $(DOCKER_COMPOSE) exec -T snowglobe tar -xzf - -C /
	@echo "✅ Backup restored"
