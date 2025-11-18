# Snowglobe Quick Reference

## 🚀 Quick Commands

```bash
# Start Snowglobe
make start

# Stop Snowglobe
make stop

# View logs
make logs

# Check status
make status

# Check health
make health
```

## 🧹 Cleanup Commands

```bash
# Complete cleanup (recommended)
make clean-all

# Docker only
make clean-docker

# Frontend only
make clean-frontend

# Python artifacts
make clean-python

# Cache files
make clean-cache
```

## 🔨 Building

```bash
# Full rebuild (clean + build)
make rebuild

# Build with pre-checks
make build

# Fast build (with cache)
make build-fast

# Build frontend
make frontend
```

## 🔍 Common Scenarios

### Frontend Changes Not Showing
```bash
make clean-frontend
cd frontend && npm install && npm run build
make build
make restart
```

### Backend Changes Not Showing
```bash
make clean-docker
make build
make restart
```

### Everything Broken / Fresh Start
```bash
make clean-all
make rebuild
make start
```

### After Git Pull
```bash
make rebuild
make start
```

### Low Disk Space
```bash
make clean-all
docker system prune -a
```

## 📝 Code Patterns

### Adding New Endpoint
```python
from snowglobe_server.decorators import (
    handle_exceptions,
    validate_json_body,
    create_success_response
)

@app.post("/api/my-endpoint")
@handle_exceptions
@validate_json_body(required_fields=["field1"])
async def my_endpoint(request: Request, body: dict):
    # Your code here
    return create_success_response(data)
```

### Loading Template
```python
from snowglobe_server.template_loader import load_template

@app.get("/my-page", response_class=HTMLResponse)
async def my_page():
    return load_template("my_page.html", variables={
        "title": "My Page"
    })
```

### Session Management
```python
# Add session
session_manager.add(token, {
    "user": "john",
    "database": "DB1"
})

# Get session
session = session_manager.get(token)

# Remove session
session_manager.remove(token)
```

### Query History
```python
# Add query to history
query_history_manager.add(
    query="SELECT * FROM users",
    session_id="sess-123",
    success=True,
    duration_ms=45.6,
    rows_affected=10
)

# Get recent queries
recent = query_history_manager.get_recent(limit=20)

# Get statistics
stats = query_history_manager.get_stats()
```

## 🔌 API Endpoints

### Authentication
```
POST /session/v1/login-request
POST /session/v1/login-request:renew
POST /session
```

### Query Execution
```
POST /queries/v1/query-request
POST /queries/v1/abort-request
```

### Health & Status
```
GET /health
GET /dashboard
```

### Frontend API
```
GET /api/sessions
GET /api/queries
GET /api/databases
GET /api/databases/{db}/schemas
GET /api/databases/{db}/schemas/{schema}/tables
GET /api/stats
POST /api/execute
DELETE /api/queries/history
```

## 🌐 Access URLs

```
# Dashboard (HTTPS)
https://localhost:8443/dashboard

# Dashboard (HTTP)
http://localhost:8084/dashboard

# Health Check (HTTPS)
https://localhost:8443/health

# Health Check (HTTP)
http://localhost:8084/health
```

## 🐍 Python Connection

```python
import snowflake.connector

conn = snowflake.connector.connect(
    user='test',
    password='test',
    account='localhost:8084',
    database='TESTDB',
    schema='PUBLIC'
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM my_table")
results = cursor.fetchall()
```

## 📁 Project Structure

```
snowglobe_2025-11-17/
├── snowglobe_server/
│   ├── server.py              # Main server
│   ├── decorators.py          # Helpers & decorators
│   ├── template_loader.py     # Template system
│   ├── templates/             # HTML templates
│   │   └── dashboard.html
│   ├── query_executor.py
│   ├── metadata.py
│   └── sql_translator.py
├── frontend/                   # Frontend app
├── tests/                      # Test suite
├── Makefile                    # Build commands
├── clean-*.sh                  # Cleanup scripts
└── *.md                        # Documentation
```

## 📚 Documentation Files

- `README.md` - Project overview
- `CLEANUP_GUIDE.md` - Detailed cleanup documentation
- `DEVELOPER_GUIDE.md` - Developer documentation
- `CHANGES.md` - Change log
- `ARCHITECTURE.md` - System architecture
- `TESTING.md` - Testing guide
- `QUICK_REFERENCE.md` - This file

## 🆘 Troubleshooting

| Problem | Solution |
|---------|----------|
| Port already in use | `make stop` then `make start` |
| Permission denied | `chmod +x *.sh` |
| Docker not found | Install Docker Desktop |
| Build fails | `make clean-all` then `make rebuild` |
| Frontend not updating | `make clean-frontend` then rebuild |
| Backend not updating | `make clean-docker` then rebuild |
| Out of space | `make clean-all` and `docker system prune -a` |

## 🔧 Environment Variables

```bash
# Server configuration
export SNOWGLOBE_PORT=8084
export SNOWGLOBE_HTTPS_PORT=8443
export SNOWGLOBE_ENABLE_HTTPS=true
export SNOWGLOBE_DATA_DIR=/data
export SNOWGLOBE_LOG_LEVEL=INFO

# Build configuration
export SNOWGLOBE_FORCE_CLEAN=1  # Skip confirmation
export SNOWGLOBE_KEEP_DATA=1    # Keep data in cleanup
```

## 💡 Tips

1. **Always use `make rebuild` after major changes**
2. **Use `make clean-frontend` if only frontend changed**
3. **Use `make clean-docker` if only backend changed**
4. **Check `make logs` if something goes wrong**
5. **Use `make health` to verify server is running**
6. **Dashboard updates every 5 seconds automatically**
7. **Use `make shell` to access container**
8. **Use `make backup` before major changes**

## 🎯 Best Practices

✅ **DO:**
- Use cleanup commands regularly
- Run `make rebuild` after git pull
- Check logs when debugging
- Use decorators for new endpoints
- Store HTML in templates
- Use session/history managers

❌ **DON'T:**
- Manually edit Docker volumes
- Skip cleanup when changes don't appear
- Embed HTML in Python
- Duplicate error handling code
- Access session dict directly
- Commit build artifacts

## 🚨 Emergency Commands

```bash
# Nuclear option - clean everything Docker
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)
docker rmi $(docker images -q)
docker system prune -a --volumes

# Then rebuild
make rebuild
make start
```

---

**Keep this file handy for quick reference!** 📌

For detailed information, see:
- `CLEANUP_GUIDE.md` - Detailed cleanup guide
- `DEVELOPER_GUIDE.md` - Complete developer docs

# Quick Testing Guide

This is a quick reference for running tests. For comprehensive documentation, see [TESTING.md](TESTING.md).

## Quick Commands

### Local Testing
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=snowglobe_server --cov-report=term-missing

# Run specific test file
pytest tests/test_server_endpoints.py

# Run fast tests only (skip slow tests)
pytest -m "not slow"
```

### Docker Testing
```bash
# Run tests in Docker
docker-compose run --rm test

# Run specific tests in Docker
docker-compose run --rm test pytest tests/test_ssl.py -v
```

### Using Test Runner Script
```bash
# Make script executable (first time only)
chmod +x run-tests.sh

# Run all tests
./run-tests.sh all

# Run with coverage
./run-tests.sh coverage

# Run API tests with verbose output
./run-tests.sh api verbose

# Run in Docker
./run-tests.sh docker all
```

## Test Categories

Run specific test categories using markers:

```bash
# API endpoint tests
pytest -m api

# SSL/TLS tests
pytest -m ssl

# Edge case tests
pytest -m edge_case

# Performance tests
pytest -m performance

# Fast tests only
pytest -m "not slow and not performance"
```

## Common Issues

### Import Errors
```bash
# Install package in development mode
pip install -e .
```

### Missing Dependencies
```bash
# Install test dependencies
pip install pytest pytest-cov pytest-asyncio httpx
```

### SSL Tests Failing
Ensure OpenSSL is installed:
```bash
# Check OpenSSL
openssl version

# Install on Ubuntu/Debian
sudo apt-get install openssl

# Install on macOS
brew install openssl
```

## See Also

- [TESTING.md](TESTING.md) - Comprehensive testing guide
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contributing guidelines
- [README.md](README.md) - Main documentation
