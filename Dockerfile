# Snowglobe - Local Snowflake Emulator with SSL/TLS Support

# Stage 1: Build the Vue frontend
FROM node:18-slim AS frontend-builder

WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# Stage 2: Build the final image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    SNOWGLOBE_PORT=8084 \
    SNOWGLOBE_HTTPS_PORT=8443 \
    SNOWGLOBE_DATA_DIR=/data \
    SNOWGLOBE_LOG_LEVEL=INFO \
    SNOWGLOBE_ENABLE_HTTPS=true

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements-server.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-server.txt

# Copy server code
COPY snowglobe_server/ ./snowglobe_server/

# Copy built frontend from the builder stage
COPY --from=frontend-builder /frontend/dist ./snowglobe_server/static/

# Create directories
RUN mkdir -p /data /app/certs && chmod 777 /data /app/certs

# Generate self-signed SSL certificate (for local development)
RUN cat > /tmp/openssl.cnf << EOF && \
    openssl req -x509 -newkey rsa:4096 -nodes \
    -out /app/certs/cert.pem \
    -keyout /app/certs/key.pem \
    -days 365 \
    -config /tmp/openssl.cnf \
    -extensions v3_req && \
    rm /tmp/openssl.cnf
[req]
default_bits = 4096
prompt = no
default_md = sha256
distinguished_name = dn
x509_extensions = v3_req

[dn]
C=US
ST=CA
L=San Francisco
O=Snowglobe
OU=Development
CN=localhost

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names
extendedKeyUsage = serverAuth, clientAuth

[alt_names]
DNS.1 = localhost
DNS.2 = snowglobe
DNS.3 = *.localhost
IP.1 = 127.0.0.1
IP.2 = 0.0.0.0
IP.3 = ::1
EOF

# Create non-root user
RUN useradd -m -u 1000 snowglobe && \
    chown -R snowglobe:snowglobe /app /data

USER snowglobe

# Expose ports (HTTP and HTTPS)
EXPOSE 8084 8443

# Health check (uses HTTPS if enabled)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f -k https://localhost:8443/health || curl -f http://localhost:8084/health || exit 1

# Run server
CMD ["python", "-m", "snowglobe_server.server"]
