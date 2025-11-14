#!/bin/bash
# Snowglobe Quick Start Script

set -e

echo "🌐❄️ Snowglobe Quick Start"
echo "=========================="
echo ""

# Check if Docker is available
if command -v docker &> /dev/null; then
    echo "Docker detected! Starting Snowglobe with Docker..."
    
    # Build and start
    docker-compose up -d --build
    
    echo ""
    echo "Snowglobe server is starting..."
    echo "Waiting for server to be ready..."
    
    # Wait for health check
    for i in {1..30}; do
        if curl -s http://localhost:8084/health > /dev/null 2>&1; then
            echo "✅ Snowglobe is ready!"
            break
        fi
        sleep 1
    done
    
    echo ""
    echo "Server is running at: http://localhost:8084"
    echo ""
    echo "To stop: docker-compose down"
    echo "To view logs: docker-compose logs -f"
    
else
    echo "Docker not found. Starting Snowglobe locally..."
    
    # Install dependencies
    echo "Installing dependencies..."
    pip install -e .[server] --quiet
    
    # Create data directory
    mkdir -p ./data
    
    echo "Starting server..."
    echo ""
    
    # Set environment variables and start
    export SNOWGLOBE_PORT=8084
    export SNOWGLOBE_DATA_DIR=./data
    export SNOWGLOBE_LOG_LEVEL=INFO
    
    python -m snowglobe_server.server
fi
