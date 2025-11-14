# Snowglobe - Local Snowflake Emulator
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    SNOWGLOBE_PORT=8084 \
    SNOWGLOBE_DATA_DIR=/data \
    SNOWGLOBE_LOG_LEVEL=INFO

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements-server.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-server.txt

# Copy server code
COPY snowglobe_server/ ./snowglobe_server/

# Create data directory
RUN mkdir -p /data && chmod 777 /data

# Create non-root user
RUN useradd -m -u 1000 snowglobe && \
    chown -R snowglobe:snowglobe /app /data

USER snowglobe

# Expose port
EXPOSE 8084

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8084/health || exit 1

# Run server
CMD ["python", "-m", "snowglobe_server.server"]
