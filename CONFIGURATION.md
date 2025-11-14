# Snowglobe Configuration Guide

This guide covers all configuration options for Snowglobe.

---

## Table of Contents

1. [Environment Variables](#environment-variables)
2. [SSL/TLS Configuration](#ssltls-configuration)
3. [Docker Configuration](#docker-configuration)
4. [Network Configuration](#network-configuration)
5. [Performance Tuning](#performance-tuning)
6. [Security Best Practices](#security-best-practices)
7. [Troubleshooting](#troubleshooting)

---

## Environment Variables

### Core Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SNOWGLOBE_PORT` | int | `8084` | HTTP port number |
| `SNOWGLOBE_HTTPS_PORT` | int | `8443` | HTTPS port number |
| `SNOWGLOBE_HOST` | string | `0.0.0.0` | Bind address |
| `SNOWGLOBE_DATA_DIR` | path | `/data` | Data storage directory |
| `SNOWGLOBE_LOG_LEVEL` | string | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

### SSL/TLS Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SNOWGLOBE_ENABLE_HTTPS` | bool | `true` | Enable HTTPS server |
| `SNOWGLOBE_CERT_PATH` | path | `/app/certs/cert.pem` | SSL certificate file path |
| `SNOWGLOBE_KEY_PATH` | path | `/app/certs/key.pem` | SSL private key file path |

### Example Configuration

```bash
# .env file
SNOWGLOBE_PORT=8084
SNOWGLOBE_HTTPS_PORT=8443
SNOWGLOBE_ENABLE_HTTPS=true
SNOWGLOBE_DATA_DIR=/data
SNOWGLOBE_LOG_LEVEL=INFO
SNOWGLOBE_CERT_PATH=/app/certs/cert.pem
SNOWGLOBE_KEY_PATH=/app/certs/key.pem
```

---

## SSL/TLS Configuration

### Quick Setup (Self-Signed Certificate)

The quickest way to get started with HTTPS:

```bash
# Generate certificates
./generate-certs.sh

# Or let Docker generate them automatically
docker-compose up -d
```

### Custom Certificate Setup

#### 1. Using Let's Encrypt (Production)

```bash
# Get Let's Encrypt certificates
certbot certonly --standalone -d your-domain.com

# Copy to Snowglobe
cp /etc/letsencrypt/live/your-domain.com/fullchain.pem certs/cert.pem
cp /etc/letsencrypt/live/your-domain.com/privkey.pem certs/key.pem

# Update permissions
chmod 644 certs/cert.pem
chmod 600 certs/key.pem
```

#### 2. Using Custom CA Certificate

```bash
# Generate CA private key
openssl genrsa -out ca-key.pem 4096

# Generate CA certificate
openssl req -new -x509 -days 3650 -key ca-key.pem -out ca-cert.pem

# Generate server private key
openssl genrsa -out certs/key.pem 4096

# Generate certificate signing request
openssl req -new -key certs/key.pem -out server.csr

# Sign with CA
openssl x509 -req -days 365 -in server.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out certs/cert.pem
```

#### 3. Using Existing Corporate Certificate

```bash
# Copy your certificates
cp /path/to/your/cert.pem certs/cert.pem
cp /path/to/your/key.pem certs/key.pem

# Ensure proper permissions
chmod 644 certs/cert.pem
chmod 600 certs/key.pem

# If you have a certificate chain
cat cert.pem intermediate.pem > certs/cert.pem
```

### Certificate Verification

```bash
# View certificate details
openssl x509 -in certs/cert.pem -noout -text

# Verify certificate and key match
openssl x509 -noout -modulus -in certs/cert.pem | openssl md5
openssl rsa -noout -modulus -in certs/key.pem | openssl md5
# (Both should produce the same hash)

# Test HTTPS connection
curl -k -v https://localhost:8443/health
```

---

## Docker Configuration

### Basic docker-compose.yml

```yaml
version: '3.8'

services:
  snowglobe:
    build: .
    container_name: snowglobe
    ports:
      - "8084:8084"
      - "8443:8443"
    volumes:
      - snowglobe-data:/data
    environment:
      - SNOWGLOBE_ENABLE_HTTPS=true
      - SNOWGLOBE_LOG_LEVEL=INFO
    restart: unless-stopped

volumes:
  snowglobe-data:
```

### Advanced docker-compose.yml

```yaml
version: '3.8'

services:
  snowglobe:
    build:
      context: .
      args:
        PYTHON_VERSION: "3.11"
    container_name: snowglobe
    hostname: snowglobe
    ports:
      - "8084:8084"
      - "8443:8443"
    volumes:
      - snowglobe-data:/data
      - ./certs:/app/certs:ro
      - ./logs:/var/log/snowglobe
    environment:
      - SNOWGLOBE_PORT=8084
      - SNOWGLOBE_HTTPS_PORT=8443
      - SNOWGLOBE_ENABLE_HTTPS=true
      - SNOWGLOBE_DATA_DIR=/data
      - SNOWGLOBE_LOG_LEVEL=INFO
      - SNOWGLOBE_CERT_PATH=/app/certs/cert.pem
      - SNOWGLOBE_KEY_PATH=/app/certs/key.pem
      - TZ=America/Los_Angeles
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-k", "-f", "https://localhost:8443/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
    networks:
      - snowglobe-network
    labels:
      - "com.snowglobe.description=Local Snowflake Emulator"
      - "com.snowglobe.version=0.2.0"

volumes:
  snowglobe-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /path/to/data

networks:
  snowglobe-network:
    driver: bridge
```

### Docker Build Arguments

```dockerfile
# Custom build
docker build \
  --build-arg PYTHON_VERSION=3.11 \
  --build-arg TIMEZONE=America/New_York \
  -t snowglobe:custom .
```

---

## Network Configuration

### Port Configuration

Default ports:
- **8084**: HTTP (unencrypted)
- **8443**: HTTPS (encrypted, recommended)

### Firewall Rules

```bash
# Allow HTTPS traffic
sudo ufw allow 8443/tcp

# Allow HTTP traffic (optional)
sudo ufw allow 8084/tcp
```

### Reverse Proxy Setup

#### Nginx

```nginx
server {
    listen 443 ssl http2;
    server_name snowflake.local;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass https://localhost:8443;
        proxy_ssl_verify off;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

#### Apache

```apache
<VirtualHost *:443>
    ServerName snowflake.local
    
    SSLEngine on
    SSLCertificateFile /path/to/cert.pem
    SSLCertificateKeyFile /path/to/key.pem
    
    ProxyPass / https://localhost:8443/
    ProxyPassReverse / https://localhost:8443/
    
    SSLProxyEngine on
    SSLProxyVerify none
</VirtualHost>
```

---

## Performance Tuning

### Resource Limits

```yaml
# docker-compose.yml
services:
  snowglobe:
    # ... other config ...
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
        reservations:
          cpus: '1.0'
          memory: 2G
```

### DuckDB Configuration

Currently managed internally, but you can optimize by:
1. Allocating more memory to Docker container
2. Using SSD storage for data directory
3. Adjusting query result chunk sizes

### Connection Pooling

For high-concurrency scenarios:

```python
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.pooling import PooledSnowflakeConnection

# Create connection pool
connection_pool = []
for _ in range(10):
    conn = snowflake.connector.connect(
        account='localhost',
        host='localhost',
        port=8443,
        protocol='https',
        user='dev',
        password='dev'
    )
    connection_pool.append(conn)
```

---

## Security Best Practices

### 1. Use HTTPS in Production

Always use HTTPS with valid SSL certificates:

```bash
# Enable HTTPS
export SNOWGLOBE_ENABLE_HTTPS=true

# Use valid certificates (not self-signed)
cp /path/to/valid/cert.pem certs/cert.pem
cp /path/to/valid/key.pem certs/key.pem
```

### 2. Secure Certificate Storage

```bash
# Restrict access to private key
chmod 600 certs/key.pem
chown root:root certs/key.pem

# Make certificate readable but not writable
chmod 644 certs/cert.pem
```

### 3. Network Isolation

```bash
# Bind to localhost only (not accessible from network)
export SNOWGLOBE_HOST=127.0.0.1

# Or use Docker networks for isolation
docker network create --internal snowglobe-internal
```

### 4. Regular Updates

```bash
# Update Snowglobe regularly
docker-compose pull
docker-compose up -d
```

### 5. Monitoring

```bash
# Monitor logs
docker-compose logs -f --tail=100

# Monitor resource usage
docker stats snowglobe
```

---

## Troubleshooting

### HTTPS Connection Issues

**Problem**: Cannot connect via HTTPS

```bash
# Check if HTTPS is enabled
docker-compose exec snowglobe env | grep HTTPS

# Verify certificates exist
docker-compose exec snowglobe ls -la /app/certs/

# Test HTTPS endpoint
curl -k https://localhost:8443/health
```

### Certificate Errors

**Problem**: SSL certificate errors in browser

```bash
# Option 1: Trust the certificate (for self-signed)
# macOS
sudo security add-trusted-cert -d -r trustRoot \
  -k /Library/Keychains/System.keychain certs/cert.pem

# Option 2: Use valid certificates (recommended)
# Get Let's Encrypt or corporate certs
```

### Connection Refused

**Problem**: Cannot connect to Snowglobe

```bash
# Check if container is running
docker-compose ps

# Check if ports are listening
netstat -tlnp | grep -E '8084|8443'

# Check container logs
docker-compose logs snowglobe

# Restart container
docker-compose restart
```

### Performance Issues

**Problem**: Slow query execution

```bash
# Increase memory allocation
docker-compose up -d --memory=4g

# Check resource usage
docker stats snowglobe

# Enable debug logging
export SNOWGLOBE_LOG_LEVEL=DEBUG
```

### Data Persistence Issues

**Problem**: Data not persisting between restarts

```bash
# Check volume mounts
docker-compose config

# Verify volume exists
docker volume ls | grep snowglobe

# Check data directory permissions
docker-compose exec snowglobe ls -la /data
```

---

## Getting Help

If you encounter issues not covered in this guide:

1. Check the [README](README.md) for basic setup
2. Review [CHANGELOG](CHANGELOG.md) for known issues
3. Search existing GitHub issues
4. Create a new issue with:
   - Snowglobe version
   - Docker version
   - Operating system
   - Error messages
   - Steps to reproduce

---

## Additional Resources

- **Snowflake Documentation**: https://docs.snowflake.com/
- **DuckDB Documentation**: https://duckdb.org/docs/
- **FastAPI Documentation**: https://fastapi.tiangolo.com/
- **Docker Documentation**: https://docs.docker.com/

---

*Last updated: 2024*
