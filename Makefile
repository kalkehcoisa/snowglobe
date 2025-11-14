.PHONY: help start stop restart logs build clean certs test frontend

# Default target
help:
	@echo "❄️  Snowglobe - Local Snowflake Emulator"
	@echo "========================================"
	@echo ""
	@echo "Available commands:"
	@echo "  make start      - Start Snowglobe (builds if needed)"
	@echo "  make stop       - Stop Snowglobe"
	@echo "  make restart    - Restart Snowglobe"
	@echo "  make logs       - View server logs"
	@echo "  make build      - Build Docker image"
	@echo "  make clean      - Stop and remove containers/volumes"
	@echo "  make certs      - Generate SSL certificates"
	@echo "  make test       - Run tests"
	@echo "  make frontend   - Build frontend"
	@echo "  make quickstart - Quick start with all setup"
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
	@$(DOCKER_COMPOSE) build --no-cache
	@echo "✅ Build complete"

# Clean up everything
clean:
	@echo "🧹 Cleaning up Snowglobe..."
	@$(DOCKER_COMPOSE) down -v
	@echo "✅ Cleanup complete"

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
	@echo "✅ Frontend built"

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
