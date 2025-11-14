# Snowglobe - Local Snowflake Emulator with SSL/TLS Support
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

# Create directories
RUN mkdir -p /data /app/certs && chmod 777 /data /app/certs

# Generate self-signed SSL certificate (for local development)
RUN openssl req -x509 -newkey rsa:4096 -nodes \
    -out /app/certs/cert.pem \
    -keyout /app/certs/key.pem \
    -days 365 \
    -subj "/C=US/ST=CA/L=SanFrancisco/O=Snowglobe/OU=Dev/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,DNS:snowglobe,IP:127.0.0.1"

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
