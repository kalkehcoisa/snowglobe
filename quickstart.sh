#!/bin/bash

# Snowglobe Quick Start Script
# Automates the setup and launch of Snowglobe

set -e

echo "❄️  Snowglobe - Local Snowflake Emulator"
echo "========================================="
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed!"
    echo "Please install Docker from: https://www.docker.com/get-started"
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not installed!"
    echo "Please install Docker Compose from: https://docs.docker.com/compose/install/"
    exit 1
fi

# Use docker compose or docker-compose
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

echo "✅ Docker is installed"
echo "✅ Docker Compose is available"
echo ""

# Check if certificates exist
if [ ! -f "./certs/cert.pem" ] || [ ! -f "./certs/key.pem" ]; then
    echo "🔐 SSL certificates not found. Generating..."
    echo ""
    
    # Generate certificates non-interactively
    mkdir -p ./certs
    openssl req -x509 -newkey rsa:4096 -nodes \
        -out ./certs/cert.pem \
        -keyout ./certs/key.pem \
        -days 365 \
        -subj "/C=US/ST=CA/L=SanFrancisco/O=Snowglobe/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,DNS:snowglobe,IP:127.0.0.1" \
        2>/dev/null
    
    chmod 644 ./certs/cert.pem
    chmod 600 ./certs/key.pem
    
    echo "✅ SSL certificates generated"
    echo ""
else
    echo "✅ SSL certificates found"
    echo ""
fi

# Stop existing containers
echo "🛑 Stopping existing Snowglobe containers..."
$DOCKER_COMPOSE down 2>/dev/null || true
echo ""

# Build and start
echo "🔨 Building Snowglobe Docker image..."
$DOCKER_COMPOSE build --no-cache

echo ""
echo "🚀 Starting Snowglobe..."
$DOCKER_COMPOSE up -d

echo ""
echo "⏳ Waiting for Snowglobe to be ready..."
for i in {1..30}; do
    if curl -k -f https://localhost:8443/health &>/dev/null || curl -f http://localhost:8084/health &>/dev/null; then
        echo ""
        echo "✅ Snowglobe is ready!"
        break
    fi
    echo -n "."
    sleep 1
done

echo ""
echo ""
echo "╔════════════════════════════════════════════════════════╗"
echo "║                                                        ║"
echo "║   🎉 Snowglobe is running successfully!               ║"
echo "║                                                        ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
echo "📊 Dashboard URLs:"
echo "   🔒 HTTPS (recommended): https://localhost:8443/dashboard"
echo "   🔓 HTTP (fallback):     http://localhost:8084/dashboard"
echo ""
echo "🔌 Connection Details:"
echo "   HTTPS Port: 8443"
echo "   HTTP Port:  8084"
echo "   Protocol:   https"
echo "   Account:    localhost"
echo ""
echo "📝 Python Connection Example:"
echo ""
echo "   import snowflake.connector"
echo ""
echo "   conn = snowflake.connector.connect("
echo "       account='localhost',"
echo "       user='dev',"
echo "       password='dev',"
echo "       host='localhost',"
echo "       port=8443,"
echo "       protocol='https',"
echo "       insecure_mode=True"
echo "   )"
echo ""
echo "🎮 Management Commands:"
echo "   View logs:     $DOCKER_COMPOSE logs -f"
echo "   Stop:          $DOCKER_COMPOSE down"
echo "   Restart:       $DOCKER_COMPOSE restart"
echo "   Status:        $DOCKER_COMPOSE ps"
echo ""
echo "💡 Tip: Add '-k' flag to curl commands for self-signed certificates"
echo "    Example: curl -k https://localhost:8443/health"
echo ""
echo "📚 Documentation: See README.md for more details"
echo ""
echo "✨ Happy querying!"
