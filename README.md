# ❄️ Snowglobe - Enhanced Local Snowflake Emulator

<div align="center">

![Snowglobe](https://img.shields.io/badge/Snowglobe-v0.2.0-blue?logo=snowflake)
![Python](https://img.shields.io/badge/Python-3.11+-green?logo=python)
![Docker](https://img.shields.io/badge/Docker-Ready-blue?logo=docker)
![HTTPS](https://img.shields.io/badge/HTTPS-Enabled-success?logo=lock)
![License](https://img.shields.io/badge/License-MIT-yellow)

**A production-ready local Snowflake emulator with SSL/TLS support and a modern web interface**

[Features](#features) • [Quick Start](#quick-start) • [HTTPS Setup](#https-setup) • [Usage](#usage) • [Documentation](#documentation)

</div>

---

## 🎯 Overview

Snowglobe is a **local Snowflake emulator** designed for Python developers. This enhanced version includes:

- ✅ **SSL/TLS/HTTPS Support** - Full HTTPS encryption, matching Snowflake's security standards
- ✅ **Modern Web UI** - Side menu navigation with multiple views
- ✅ **SQL Worksheet** - Snowflake-like query interface with syntax highlighting
- ✅ **Query History** - Track all queries with execution details
- ✅ **Database Explorer** - Browse databases, schemas, and tables
- ✅ **Session Management** - Monitor active connections
- ✅ **Real-time Stats** - Performance metrics and monitoring
- ✅ **Docker Support** - Easy deployment with Docker Compose

---

## 🚀 Features

### 🔒 Security & HTTPS

- **SSL/TLS Encryption** - HTTPS enabled by default
- **Auto-generated Certificates** - Self-signed certificates for local development
- **Custom Certificates** - Support for custom SSL certificates
- **Dual Protocol** - HTTP (8084) and HTTPS (8443) support

### 📝 SQL Worksheet

- **Snowflake-Compatible** - Write and execute SQL queries
- **Syntax Highlighting** - Dark theme code editor
- **Query History** - Track all executed queries
- **Sample Queries** - Quick-start examples
- **Results Export** - Download results as CSV
- **Keyboard Shortcuts** - Ctrl/Cmd+Enter to execute

### 🗄️ Database Management

- **Multi-Database** - Create and manage multiple databases
- **Schema Support** - Full schema hierarchy
- **Table Operations** - CREATE, INSERT, SELECT, UPDATE, DELETE
- **Metadata Tracking** - Track all database objects

### 📊 Monitoring Dashboard

- **Side Navigation** - Easy access to all features
- **Real-time Stats** - Active sessions, query counts, performance
- **Query History View** - Filter and analyze past queries
- **Session Explorer** - Monitor active connections
- **Settings Panel** - Configure and view system settings

---

## 🏃 Quick Start

### Using Docker (Recommended)

```bash
# Clone or extract the project
cd enhanced_snowglobe

# Build and start with Docker Compose
docker-compose up -d

# Access the dashboard
# HTTPS (recommended): https://localhost:8443/dashboard
# HTTP (fallback): http://localhost:8084/dashboard
```

### Manual Setup

```bash
# Install dependencies
pip install -r requirements-server.txt

# Set environment variables
export SNOWGLOBE_ENABLE_HTTPS=true
export SNOWGLOBE_PORT=8084
export SNOWGLOBE_HTTPS_PORT=8443

# Run the server
python -m snowglobe_server.server
```

---

## 🔐 HTTPS Setup

### Default Configuration

Snowglobe automatically generates self-signed SSL certificates on first run:

```bash
# Certificates are created at:
/app/certs/cert.pem  # SSL Certificate
/app/certs/key.pem   # Private Key
```

### Custom Certificates

To use your own SSL certificates:

```bash
# 1. Create a certs directory
mkdir certs

# 2. Copy your certificates
cp your-cert.pem certs/cert.pem
cp your-key.pem certs/key.pem

# 3. Mount the directory in Docker Compose
volumes:
  - ./certs:/app/certs:ro
```

### Generate Custom Self-Signed Certificate

```bash
openssl req -x509 -newkey rsa:4096 -nodes \
  -out certs/cert.pem \
  -keyout certs/key.pem \
  -days 365 \
  -subj "/C=US/ST=CA/L=SF/O=MyOrg/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:snowglobe,IP:127.0.0.1"
```

### Trust the Certificate (Optional)

For local development without browser warnings:

**macOS:**
```bash
sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain certs/cert.pem
```

**Linux:**
```bash
sudo cp certs/cert.pem /usr/local/share/ca-certificates/snowglobe.crt
sudo update-ca-certificates
```

**Windows:**
```powershell
certutil -addstore -f "ROOT" certs\cert.pem
```

---

## 📖 Usage

### 1. Connect with Snowflake Python Connector

```python
import snowflake.connector

# HTTPS Connection (Recommended)
conn = snowflake.connector.connect(
    account='localhost',
    user='dev',
    password='dev',
    host='localhost',
    port=8443,
    protocol='https',
    insecure_mode=True,  # For self-signed certificates
    database='TEST_DB',
    schema='PUBLIC'
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_VERSION()")
print(cursor.fetchone())
```

### 2. Use the Web Interface

**Access the Dashboard:**
- HTTPS: `https://localhost:8443/dashboard`
- HTTP: `http://localhost:8084/dashboard`

**Main Features:**

1. **📝 Worksheet** - Write and execute SQL queries
   - Syntax-highlighted editor
   - Sample queries for quick start
   - Export results to CSV
   - Keyboard shortcuts

2. **📊 Overview** - System statistics and monitoring
   - Active sessions
   - Query performance
   - Server uptime

3. **🕒 Query History** - View past queries
   - Execution status
   - Duration and row counts
   - Error messages

4. **🔗 Sessions** - Monitor active connections
   - User information
   - Database context
   - Session duration

5. **🗄️ Databases** - Browse database objects
   - Database list
   - Schema hierarchy
   - Table details

6. **⚙️ Settings** - Configuration and information
   - Server status
   - Connection details
   - Performance metrics
   - Environment variables

### 3. Example Queries

```sql
-- Create a database and schema
CREATE DATABASE IF NOT EXISTS my_database;
USE DATABASE my_database;
CREATE SCHEMA IF NOT EXISTS my_schema;
USE SCHEMA my_schema;

-- Create a table
CREATE TABLE customers (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
);

-- Insert data
INSERT INTO customers VALUES
    (1, 'Alice Johnson', 'alice@example.com', CURRENT_TIMESTAMP),
    (2, 'Bob Smith', 'bob@example.com', CURRENT_TIMESTAMP),
    (3, 'Carol White', 'carol@example.com', CURRENT_TIMESTAMP);

-- Query data
SELECT * FROM customers;

-- Aggregation
SELECT COUNT(*) as total_customers FROM customers;

-- Show objects
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE my_database;
SHOW TABLES IN SCHEMA my_database.my_schema;
```

---

## 🐳 Docker Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SNOWGLOBE_PORT` | `8084` | HTTP port |
| `SNOWGLOBE_HTTPS_PORT` | `8443` | HTTPS port |
| `SNOWGLOBE_ENABLE_HTTPS` | `true` | Enable HTTPS |
| `SNOWGLOBE_DATA_DIR` | `/data` | Data directory |
| `SNOWGLOBE_LOG_LEVEL` | `INFO` | Logging level |
| `SNOWGLOBE_CERT_PATH` | `/app/certs/cert.pem` | SSL certificate path |
| `SNOWGLOBE_KEY_PATH` | `/app/certs/key.pem` | SSL key path |

### Docker Compose Example

```yaml
version: '3.8'

services:
  snowglobe:
    build: .
    container_name: snowglobe
    ports:
      - "8084:8084"   # HTTP
      - "8443:8443"   # HTTPS
    volumes:
      - snowglobe-data:/data
      - ./certs:/app/certs:ro  # Custom certificates
    environment:
      - SNOWGLOBE_ENABLE_HTTPS=true
      - SNOWGLOBE_LOG_LEVEL=INFO
    restart: unless-stopped

volumes:
  snowglobe-data:
```

---

## 🔧 Development

### Frontend Development

The Vue frontend is served by the backend at `/dashboard`. The frontend must be built and deployed to `snowglobe_server/static/` before it can be used.

```bash
# Build and deploy frontend (recommended)
make frontend

# Or manually:
cd frontend
npm install
npm run build
cd ..
cp -r frontend/dist snowglobe_server/static
```

For development with hot-reload:
```bash
cd frontend
npm install
npm run dev   # Runs on http://localhost:3000 with API proxy to backend
```

> **Note:** When building the Docker image, the frontend is automatically built and included. The `make frontend` command is only needed for local development without Docker.

### Backend Development

```bash
# Install in development mode
pip install -e .

# Run tests
pytest

# Run with auto-reload
uvicorn snowglobe_server.server:app --reload --port 8084
```

---

## 📚 API Documentation

### Snowflake-Compatible Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/session/v1/login-request` | POST | Authenticate and create session |
| `/queries/v1/query-request` | POST | Execute SQL query |
| `/session` | POST | Close session |

### Frontend API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/execute` | POST | Execute query from worksheet |
| `/api/sessions` | GET | List active sessions |
| `/api/queries` | GET | Get query history |
| `/api/databases` | GET | List databases |
| `/api/stats` | GET | Get server statistics |
| `/health` | GET | Health check |

---

## 🎨 Screenshots

### SQL Worksheet
```
┌─────────────────────────────────────────────────┐
│ 📝 SQL Worksheet                    [▶️ Run]    │
├─────────────────────────────────────────────────┤
│ SELECT * FROM customers;                        │
│                                                  │
│ ┌─────────────────────────────────────────┐    │
│ │ ✅ Query Results                         │    │
│ │ 📊 3 row(s) | ⏱️ 12.34ms                 │    │
│ ├─────────────────────────────────────────┤    │
│ │ ID │ NAME          │ EMAIL              │    │
│ │ 1  │ Alice Johnson │ alice@example.com  │    │
│ │ 2  │ Bob Smith     │ bob@example.com    │    │
│ │ 3  │ Carol White   │ carol@example.com  │    │
│ └─────────────────────────────────────────┘    │
└─────────────────────────────────────────────────┘
```

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 🙏 Acknowledgments

- Built with FastAPI and DuckDB
- Frontend powered by Vue.js
- Inspired by Snowflake's architecture

---

## 📧 Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Check the documentation
- Review sample queries in the Worksheet

---

<div align="center">

**Made with ❄️ by the Snowglobe Team**

[⬆ Back to Top](#-snowglobe---enhanced-local-snowflake-emulator)

</div>
