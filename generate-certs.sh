#!/bin/bash

# Snowglobe SSL Certificate Generator
# This script generates self-signed SSL certificates for local development

set -e

CERT_DIR="./certs"
CERT_FILE="$CERT_DIR/cert.pem"
KEY_FILE="$CERT_DIR/key.pem"
DAYS=365

echo "🔐 Snowglobe SSL Certificate Generator"
echo "======================================="
echo ""

# Create certs directory if it doesn't exist
if [ ! -d "$CERT_DIR" ]; then
    echo "📁 Creating certificates directory..."
    mkdir -p "$CERT_DIR"
fi

# Check if certificates already exist
if [ -f "$CERT_FILE" ] && [ -f "$KEY_FILE" ]; then
    echo "⚠️  Certificates already exist!"
    echo ""
    read -p "Do you want to regenerate them? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Cancelled. Existing certificates will be used."
        exit 0
    fi
fi

# Get certificate details
echo ""
echo "📝 Certificate Configuration"
echo "----------------------------"
read -p "Country Code (C) [US]: " COUNTRY
COUNTRY=${COUNTRY:-US}

read -p "State/Province (ST) [California]: " STATE
STATE=${STATE:-California}

read -p "City/Locality (L) [San Francisco]: " CITY
CITY=${CITY:-San Francisco}

read -p "Organization (O) [Snowglobe]: " ORG
ORG=${ORG:-Snowglobe}

read -p "Common Name (CN) [localhost]: " CN
CN=${CN:-localhost}

read -p "Validity in days [$DAYS]: " CERT_DAYS
CERT_DAYS=${CERT_DAYS:-$DAYS}

echo ""
echo "🔨 Generating SSL certificate..."
echo ""

# Generate certificate
openssl req -x509 -newkey rsa:4096 -nodes \
    -out "$CERT_FILE" \
    -keyout "$KEY_FILE" \
    -days "$CERT_DAYS" \
    -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/CN=$CN" \
    -addext "subjectAltName=DNS:localhost,DNS:snowglobe,DNS:$CN,IP:127.0.0.1,IP:0.0.0.0" \
    2>/dev/null

# Set permissions
chmod 644 "$CERT_FILE"
chmod 600 "$KEY_FILE"

echo "✅ SSL certificate generated successfully!"
echo ""
echo "📄 Certificate Details:"
echo "   Location: $CERT_FILE"
echo "   Key: $KEY_FILE"
echo "   Valid for: $CERT_DAYS days"
echo "   Common Name: $CN"
echo ""

# Display certificate info
echo "🔍 Certificate Information:"
echo "----------------------------"
openssl x509 -in "$CERT_FILE" -noout -subject -dates -fingerprint

echo ""
echo "📋 Next Steps:"
echo "1. Start Snowglobe with: docker-compose up -d"
echo "2. Access via HTTPS: https://localhost:8443/dashboard"
echo "3. (Optional) Trust the certificate to avoid browser warnings"
echo ""
echo "💡 To trust the certificate:"
echo ""
echo "macOS:"
echo "  sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain $CERT_FILE"
echo ""
echo "Linux:"
echo "  sudo cp $CERT_FILE /usr/local/share/ca-certificates/snowglobe.crt"
echo "  sudo update-ca-certificates"
echo ""
echo "Windows (PowerShell as Admin):"
echo "  certutil -addstore -f \"ROOT\" $CERT_FILE"
echo ""

echo "✨ Done!"
