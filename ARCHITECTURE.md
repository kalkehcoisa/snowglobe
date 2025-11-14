# Snowglobe Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          CLIENT APPLICATIONS                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   Python     │  │  Web Browser │  │     CLI      │              │
│  │ Snowflake    │  │   Dashboard  │  │    Tools     │              │
│  │  Connector   │  │              │  │              │              │
│  └───────┬──────┘  └───────┬──────┘  └───────┬──────┘              │
│          │                  │                  │                      │
└──────────┼──────────────────┼──────────────────┼──────────────────────┘
           │                  │                  │
           │ HTTPS/HTTP       │ HTTPS/HTTP       │ HTTPS/HTTP
           │ Port 8443/8084   │ Port 8443/8084   │ Port 8443/8084
           │                  │                  │
           ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        SNOWGLOBE SERVER                              │
│                       (Docker Container)                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    SSL/TLS Layer                             │   │
│  │  ┌───────────────┐  Auto-generated or Custom Certificates   │   │
│  │  │ /app/certs/   │  ✓ cert.pem                              │   │
│  │  │               │  ✓ key.pem                               │   │
│  │  └───────────────┘  ✓ TLS 1.2+                              │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                FastAPI Application                           │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │  Snowflake-Compatible Endpoints                      │   │   │
│  │  │  • /session/v1/login-request (POST)                  │   │   │
│  │  │  • /queries/v1/query-request (POST)                  │   │   │
│  │  │  • /session (POST - close)                           │   │   │
│  │  └──────────────────────────────────────────────────────┘   │   │
│  │                                                               │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │  Frontend API Endpoints                              │   │   │
│  │  │  • /api/execute (POST)      - Run queries            │   │   │
│  │  │  • /api/sessions (GET)      - List sessions          │   │   │
│  │  │  • /api/queries (GET)       - Query history          │   │   │
│  │  │  • /api/databases (GET)     - List databases         │   │   │
│  │  │  • /api/stats (GET)         - Server statistics      │   │   │
│  │  │  • /health (GET)            - Health check           │   │   │
│  │  │  • /dashboard (GET)         - Web interface          │   │   │
│  │  └──────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                  Query Executor                              │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │  SQL Translation Layer                               │   │   │
│  │  │  • Snowflake SQL → DuckDB SQL                        │   │   │
│  │  │  • SHOW commands → Metadata queries                  │   │   │
│  │  │  • DESCRIBE → Schema inspection                      │   │   │
│  │  │  • USE → Context switching                           │   │   │
│  │  └──────────────────────────────────────────────────────┘   │   │
│  │                                                               │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │  Session Management                                  │   │   │
│  │  │  • Token generation                                  │   │   │
│  │  │  • Context (database, schema, warehouse, role)       │   │   │
│  │  │  • Connection pooling                                │   │   │
│  │  └──────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                  Metadata Manager                            │   │
│  │  • Database registry                                         │   │
│  │  • Schema registry                                           │   │
│  │  • Table registry                                            │   │
│  │  • View registry                                             │   │
│  │  • Column metadata                                           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                     DuckDB Engine                            │   │
│  │  ┌──────────────────────────────────────────────────────┐   │   │
│  │  │  In-Memory & Persistent Storage                      │   │   │
│  │  │  • SQL execution engine                              │   │   │
│  │  │  • ACID transactions                                 │   │   │
│  │  │  • Columnar storage                                  │   │   │
│  │  │  • Query optimization                                │   │   │
│  │  └──────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                   Data Persistence                           │   │
│  │  /data/                                                      │   │
│  │  ├── snowglobe.db          - Main DuckDB database           │   │
│  │  ├── metadata.json         - Metadata registry              │   │
│  │  └── schemas/              - Schema definitions             │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Request Flow

### 1. Python Client Connection (HTTPS)

```
Python App
    │
    │ snowflake.connector.connect(
    │   protocol='https',
    │   port=8443,
    │   insecure_mode=True
    │ )
    │
    ▼
SSL/TLS Handshake
    │
    ▼
POST /session/v1/login-request
    │
    │ Body: {
    │   "data": {
    │     "LOGIN_NAME": "dev",
    │     "PASSWORD": "dev",
    │     "ACCOUNT_NAME": "localhost"
    │   }
    │ }
    │
    ▼
Session Manager
    │
    ├─► Generate session token
    ├─► Create QueryExecutor instance
    ├─► Set default context (DB/Schema/Warehouse/Role)
    └─► Store session in memory
    │
    ▼
Response
    {
      "data": {
        "token": "abc123...",
        "masterToken": "def456...",
        "sessionId": "uuid...",
        "parameters": [...],
        "sessionInfo": {
          "databaseName": "TEST_DB",
          "schemaName": "PUBLIC",
          "warehouseName": "COMPUTE_WH",
          "roleName": "SYSADMIN"
        }
      },
      "success": true
    }
```

### 2. Query Execution Flow

```
Python App
    │
    │ cursor.execute("SELECT * FROM table")
    │
    ▼
POST /queries/v1/query-request
    │
    │ Headers: {
    │   "Authorization": "Snowflake Token=\"abc123...\""
    │ }
    │
    │ Body: {
    │   "sqlText": "SELECT * FROM table",
    │   "sequenceId": 1
    │ }
    │
    ▼
Session Validation
    │
    ├─► Extract token from Authorization header
    ├─► Look up session
    └─► Get QueryExecutor instance
    │
    ▼
SQL Translation
    │
    ├─► Parse SQL statement
    ├─► Translate Snowflake → DuckDB syntax
    ├─► Handle context (USE DATABASE/SCHEMA)
    └─► Generate DuckDB-compatible SQL
    │
    ▼
Metadata Check
    │
    ├─► Verify database exists
    ├─► Verify schema exists
    ├─► Verify table exists
    └─► Validate permissions (future)
    │
    ▼
DuckDB Execution
    │
    ├─► Execute translated SQL
    ├─► Fetch results
    └─► Track execution time
    │
    ▼
Result Formatting
    │
    ├─► Convert to Snowflake format
    ├─► Add metadata (columns, types)
    └─► Package response
    │
    ▼
Query History
    │
    └─► Record query, duration, row count
    │
    ▼
Response
    {
      "data": {
        "rowtype": [
          {"name": "ID", "type": "text", ...},
          {"name": "NAME", "type": "text", ...}
        ],
        "rowset": [
          ["1", "Alice"],
          ["2", "Bob"]
        ],
        "total": 2,
        "returned": 2,
        "queryId": "uuid...",
        "statementTypeId": 4096
      },
      "success": true
    }
```

### 3. Web Dashboard Worksheet Flow

```
Browser
    │
    │ https://localhost:8443/dashboard
    │
    ▼
Load Vue.js App
    │
    ├─► Load App.vue (side menu + routing)
    ├─► Load WorksheetPanel.vue
    └─► Initialize state
    │
    ▼
User Enters SQL
    │
    │ "SELECT * FROM customers;"
    │
    ▼
Ctrl+Enter or Click "Run"
    │
    ▼
POST /api/execute
    │
    │ Body: {
    │   "sql": "SELECT * FROM customers;"
    │ }
    │
    ▼
Query Execution (same as above)
    │
    ▼
Display Results
    │
    ├─► Show result table
    ├─► Display row count
    ├─► Show execution time
    └─► Enable CSV export
    │
    ▼
Update Query History
    │
    └─► Add to history view
```

---

## Component Architecture

### Frontend (Vue.js)

```
src/
├── App.vue                          # Root component
│   ├── Side Menu Navigation
│   ├── Header (server status)
│   ├── Main Content Area
│   └── Footer (stats bar)
│
└── components/
    ├── WorksheetPanel.vue           # SQL query interface
    │   ├── SQL Editor (textarea)
    │   ├── Sample Queries
    │   ├── Result Table
    │   ├── Pagination
    │   └── CSV Export
    │
    ├── OverviewPanel.vue            # Dashboard stats
    │   ├── Stats Cards
    │   └── Quick Info
    │
    ├── QueryHistoryPanel.vue        # Query log
    │   ├── Query List
    │   ├── Filters
    │   └── Details
    │
    ├── SessionsPanel.vue            # Active sessions
    │   ├── Session List
    │   └── Session Details
    │
    ├── DatabaseExplorer.vue         # Object browser
    │   ├── Database Tree
    │   ├── Schema List
    │   └── Table List
    │
    └── SettingsPanel.vue            # Configuration
        ├── Server Info
        ├── Connection Details
        ├── Performance Metrics
        └── Security Status
```

### Backend (FastAPI)

```
snowglobe_server/
├── server.py                        # Main application
│   ├── FastAPI app setup
│   ├── CORS middleware
│   ├── SSL/TLS configuration
│   ├── Snowflake endpoints
│   ├── Frontend API endpoints
│   └── Dashboard HTML
│
├── query_executor.py                # Query execution engine
│   ├── QueryExecutor class
│   ├── execute() method
│   ├── Session context management
│   └── Result formatting
│
├── sql_translator.py                # SQL translation
│   ├── Snowflake → DuckDB mapping
│   ├── SHOW command handling
│   ├── DESCRIBE command handling
│   └── Context commands (USE)
│
└── metadata.py                      # Metadata management
    ├── Database registry
    ├── Schema registry
    ├── Table registry
    └── Metadata persistence
```

---

## Data Flow

### Database Hierarchy

```
Snowglobe Instance
    │
    ├── Database: TEST_DB
    │   │
    │   ├── Schema: PUBLIC
    │   │   │
    │   │   ├── Table: CUSTOMERS
    │   │   │   ├── Column: ID (INTEGER)
    │   │   │   ├── Column: NAME (VARCHAR)
    │   │   │   └── Column: EMAIL (VARCHAR)
    │   │   │
    │   │   └── Table: ORDERS
    │   │       ├── Column: ID (INTEGER)
    │   │       ├── Column: CUSTOMER_ID (INTEGER)
    │   │       └── Column: TOTAL (DECIMAL)
    │   │
    │   └── Schema: ANALYTICS
    │       └── ...
    │
    └── Database: PRODUCTION_DB
        └── ...
```

### Storage Layout

```
/data/
├── snowglobe.db                     # Main DuckDB file
│   └── Contains all table data in columnar format
│
├── metadata.json                    # Metadata registry
│   └── {
│         "databases": {
│           "TEST_DB": {
│             "created_at": "2024-01-01T00:00:00",
│             "schemas": {
│               "PUBLIC": {
│                 "tables": ["CUSTOMERS", "ORDERS"],
│                 "views": []
│               }
│             }
│           }
│         }
│       }
│
└── schemas/                         # Schema definitions
    └── TEST_DB/
        └── PUBLIC/
            ├── CUSTOMERS.json
            └── ORDERS.json
```

---

## Security Architecture

### SSL/TLS Layer

```
Client Request
    │
    ▼
┌─────────────────────────────────┐
│   TLS 1.2+ Handshake            │
│   ├─ Certificate validation     │
│   ├─ Key exchange               │
│   └─ Cipher negotiation         │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│   Encrypted Connection          │
│   ├─ AES-256 encryption         │
│   ├─ Perfect forward secrecy    │
│   └─ Integrity checks           │
└─────────────────────────────────┘
    │
    ▼
Application Layer
```

### Certificate Management

```
Docker Build
    │
    ▼
Check for certificates
    │
    ├─► Found? → Use existing
    │
    └─► Not found? → Generate self-signed
            │
            ├─ openssl req -x509 -newkey rsa:4096
            ├─- Save to /app/certs/cert.pem
            └─- Save to /app/certs/key.pem
```

---

## Scalability Considerations

### Current Architecture
- **Single instance**: One Docker container
- **In-memory sessions**: Session data in RAM
- **Single DuckDB connection**: One database file
- **Suitable for**: Local development, testing, CI/CD

### Future Enhancements
- **Multi-instance**: Load balancer + multiple containers
- **Persistent sessions**: Redis/database-backed sessions
- **Connection pooling**: Multiple DuckDB connections
- **Horizontal scaling**: Separate query execution from metadata

---

## Performance Characteristics

### Response Times (Typical)

| Operation | Time |
|-----------|------|
| Login | 10-50ms |
| Simple SELECT | 5-20ms |
| Complex JOIN | 20-100ms |
| INSERT (batch) | 10-50ms |
| SHOW commands | 5-15ms |
| Dashboard load | 100-300ms |

### Throughput

- **Queries per second**: 100-1000 (depending on complexity)
- **Concurrent sessions**: 10-100
- **Data volume**: Hundreds of MB to few GB

---

## Monitoring & Observability

### Available Metrics

```
GET /api/stats
{
  "uptime_seconds": 3600,
  "active_sessions": 5,
  "total_queries": 1234,
  "successful_queries": 1200,
  "failed_queries": 34,
  "average_query_duration_ms": 15.5
}
```

### Logging

```
[2024-01-01 12:00:00] INFO - Starting Snowglobe server
[2024-01-01 12:00:01] INFO - SSL certificates loaded
[2024-01-01 12:00:02] INFO - HTTPS server on 0.0.0.0:8443
[2024-01-01 12:00:03] INFO - HTTP server on 0.0.0.0:8084
[2024-01-01 12:01:00] INFO - Login request from user: dev
[2024-01-01 12:01:01] INFO - Session created: abc-123
[2024-01-01 12:01:05] DEBUG - Query: SELECT * FROM customers
[2024-01-01 12:01:06] INFO - Query successful, 100 rows
```

---

## Deployment Options

### Option 1: Docker Compose (Recommended)

```bash
docker-compose up -d
# Automatic SSL generation
# Easy management
# Persistent data volumes
```

### Option 2: Docker Run

```bash
docker run -d \
  -p 8084:8084 \
  -p 8443:8443 \
  -v snowglobe-data:/data \
  snowglobe:latest
```

### Option 3: Local Python

```bash
python -m snowglobe_server.server
# For development only
# Manual SSL setup required
```

---

## Integration Points

### Compatible With

- ✅ Snowflake Python Connector
- ✅ SQLAlchemy (with snowflake-sqlalchemy)
- ✅ Pandas (via snowflake-connector-python)
- ✅ dbt (via dbt-snowflake adapter)
- ✅ Any HTTP/HTTPS client

### Example Integration

```python
# Works with pandas
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
write_pandas(conn, df, 'MY_TABLE')
```

---

This architecture provides a solid foundation for Snowflake-compatible local development and testing!
