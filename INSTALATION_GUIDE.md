```markdown
# SNOWGLOBE - QUICK START

**Local Snowflake Emulator with SSL/TLS and Modern UI**

## 📦 WHAT'S INCLUDED

- ✅ SSL/TLS/HTTPS Support - 🔐 Production-grade security
- ✅ SQL Worksheet Interface - 📝 Snowflake-like query editor
- ✅ Side Menu Navigation - 🎨 Modern, intuitive UI
- ✅ 6 Main Screens - 📊 Complete dashboard
- ✅ Auto SSL Certificate Gen - 🔧 Zero-config HTTPS
- ✅ Sample Queries - 📚 Quick start examples
- ✅ Query History Tracking - 🕒 Full audit trail
- ✅ Settings Panel - ⚙️ Configuration view
- ✅ CSV Export - 💾 Data export support
- ✅ Docker Ready - 🐳 One-command deployment

## 🚀 QUICK START

### Run Quick Start
```
./quickstart.sh
```

### Access Dashboard
- 🔒 HTTPS: https://localhost:8443/dashboard
- 🔓 HTTP: http://localhost:8084/dashboard

## 📋 DETAILED INSTALLATION

### Prerequisites
- Docker (v20.10+)
- Docker Compose (v2.0+)
- 2GB free disk space
- Internet connection (for first build)

### Option A: Using Quick Start Script (Recommended)
```
cd enhanced_snowglobe
chmod +x quickstart.sh
./quickstart.sh
```
✓ Automatically generates SSL certificates  
✓ Builds Docker image  
✓ Starts containers  
✓ Runs health checks  

### Option B: Using Docker Compose
```
cd enhanced_snowglobe
docker-compose up -d

# View logs
docker-compose logs -f

# Check status
docker-compose ps
```

### Option C: Using Makefile
```
cd enhanced_snowglobe
make quickstart    # Full setup
# or
make start         # Just start
make logs          # View logs
make health        # Check health
make stop          # Stop service
```

## 🔐 SSL/TLS SETUP

### Auto-Generated Certificates (Default)
Certificates are automatically generated during Docker build.  
Location: `/app/certs/cert.pem` and `/app/certs/key.pem`

### Custom Certificates (Optional)
1. Create certs directory:
   ```
   mkdir certs
   ```

2. Generate custom certificate:
   ```
   ./generate-certs.sh
   ```

3. Or copy existing certificates:
   ```
   cp your-cert.pem certs/cert.pem
   cp your-key.pem certs/key.pem
   ```

4. Restart:
   ```
   docker-compose restart
   ```

### Trust Self-Signed Certificate (Optional)

**macOS:**
```
sudo security add-trusted-cert -d -r trustRoot \
  -k /Library/Keychains/System.keychain certs/cert.pem
```

**Linux:**
```
sudo cp certs/cert.pem /usr/local/share/ca-certificates/snowglobe.crt
sudo update-ca-certificates
```

**Windows (PowerShell as Admin):**
```
certutil -addstore -f "ROOT" certs\cert.pem
```

## 🐍 PYTHON CONNECTION

### HTTPS Connection (Recommended)
```python
import snowflake.connector

conn = snowflake.connector.connect(
    account='localhost',
    user='dev',
    password='dev',
    host='localhost',
    port=8443,
    protocol='https',
    insecure_mode=True,  # For self-signed certs
    database='TEST_DB',
    schema='PUBLIC'
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_VERSION()")
print(cursor.fetchone())
```

### HTTP Connection (Fallback)
```python
conn = snowflake.connector.connect(
    account='localhost',
    user='dev',
    password='dev',
    host='localhost',
    port=8084,
    protocol='http',
    database='TEST_DB',
    schema='PUBLIC'
)
```

## 🌐 WEB INTERFACE FEATURES

1. **📝 Worksheet**
   - SQL query editor with syntax highlighting
   - Sample queries for quick start
   - Execute with Ctrl+Enter (⌘+Enter on Mac)
   - Result tables with pagination
   - CSV export functionality

2. **📊 Overview**
   - Server statistics and uptime
   - Active sessions count
   - Query performance metrics
   - Success/failure rates

3. **🕒 Query History**
   - All executed queries
   - Execution times and row counts
   - Success/error status
   - Error messages

4. **🔗 Sessions**
   - Active connection list
   - User information
   - Database context
   - Session details

5. **🗄️ Databases**
   - Database browser
   - Schema explorer
   - Table list
   - Object metadata

6. **⚙️ Settings**
   - Server information
   - HTTPS status
   - Connection examples
   - Environment variables
   - Performance metrics

## 🛠️ MANAGEMENT COMMANDS

### Using Makefile:
```
make start         # Start Snowglobe
make stop          # Stop Snowglobe
make restart       # Restart Snowglobe
make logs          # View logs
make health        # Check health
make clean         # Clean everything
make certs         # Generate certificates
```

### Using Docker Compose:
```
docker-compose up -d           # Start
docker-compose down            # Stop
docker-compose restart         # Restart
docker-compose logs -f         # Logs
docker-compose ps              # Status
```

### Direct Docker:
```
docker ps                                    # List containers
docker logs snowglobe                        # View logs
docker exec -it snowglobe /bin/bash          # Shell access
docker restart snowglobe                     # Restart
```

## 📚 DOCUMENTATION

| File | Description |
|------|-------------|
| README.md | Complete user guide and features |
| CONFIGURATION.md | Detailed configuration options |
| ARCHITECTURE.md | System architecture and design |
| CHANGELOG.md | Version history and roadmap |
| SUMMARY.md | Quick feature overview |
| quickstart.sh | One-command setup script |
| generate-certs.sh | SSL certificate generator |
| Makefile | Common management commands |
| examples/ | Python example scripts |

## 🔍 TESTING THE INSTALLATION

1. Check if running:
   ```
   docker-compose ps
   ```
   Expected: Container 'snowglobe' should be 'Up'

2. Test HTTPS endpoint:
   ```
   curl -k https://localhost:8443/health
   ```
   Expected: `{"status":"healthy","version":"0.1.0",...}`

3. Test HTTP endpoint:
   ```
   curl http://localhost:8084/health
   ```
   Expected: `{"status":"healthy","version":"0.1.0",...}`

4. Open dashboard:
   Open browser: https://localhost:8443/dashboard
   Expected: Snowglobe dashboard loads

5. Run Python example:
   ```
   python examples/https_connection_example.py
   ```
   Expected: Successful connection and query execution

## ⚠️ TROUBLESHOOTING

### Problem: Port already in use
**Solution:** Change ports in docker-compose.yml
```yaml
ports:
  - "9084:8084"  # Changed from 8084
  - "9443:8443"  # Changed from 8443
```

### Problem: Certificate warnings in browser
**Solution:** This is normal for self-signed certificates
- Click "Advanced" and "Proceed to localhost"
- Or trust the certificate (see SSL setup section)

### Problem: Cannot connect via HTTPS
**Solution:** Check certificates and logs
```
docker-compose exec snowglobe ls -la /app/certs/
docker-compose logs snowglobe
```

### Problem: Python connection fails
**Solution:** Ensure insecure_mode=True for self-signed certs
```python
conn = snowflake.connector.connect(
    ...,
    insecure_mode=True  # Add this line
)
```

## 📊 ENVIRONMENT VARIABLES

| Variable | Default | Description |
|----------|---------|-------------|
| SNOWGLOBE_PORT | 8084 | HTTP port |
| SNOWGLOBE_HTTPS_PORT | 8443 | HTTPS port |
| SNOWGLOBE_ENABLE_HTTPS | true | Enable HTTPS |
| SNOWGLOBE_DATA_DIR | /data | Data directory |
| SNOWGLOBE_LOG_LEVEL | INFO | Log level |
| SNOWGLOBE_CERT_PATH | /app/certs/cert.pem | Certificate path |
| SNOWGLOBE_KEY_PATH | /app/certs/key.pem | Private key path |

## 💡 TIPS & BEST PRACTICES

- ✓ Use HTTPS for Snowflake compatibility
- ✓ Trust the certificate for seamless browser access
- ✓ Set insecure_mode=True in Python for self-signed certs
- ✓ Use the Worksheet for interactive query development
- ✓ Check Settings panel for connection examples
- ✓ Export query results to CSV for analysis
- ✓ Monitor Query History for performance insights
- ✓ Use make commands for easy management
- ✓ Keep data persistent with Docker volumes
- ✓ Review logs if something doesn't work

## 🎯 NEXT STEPS

1. ✅ Run quickstart.sh
2. ✅ Access https://localhost:8443/dashboard
3. ✅ Try sample queries in Worksheet
4. ✅ Connect with Python using examples/
5. ✅ Explore all 6 screens in the UI
6. ✅ Read README.md for detailed docs
7. ✅ Check CONFIGURATION.md for advanced setup

## 📞 SUPPORT

- **Documentation:** See README.md and CONFIGURATION.md
- **Architecture:** See ARCHITECTURE.md
- **Examples:** See examples/ directory
- **Issues:** Check logs with: `docker-compose logs snowglobe`

---

## 🎉 ENJOY SNOWGLOBE! ❄️

**A Local Snowflake Emulator for Python Developers**
```