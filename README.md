# Snowglobe 🌐❄️

A local Snowflake emulator for Python developers. Work offline, test locally, and develop without a Snowflake subscription.

## Features

- **Local Snowflake Emulation**: Mimics Snowflake SQL behavior using DuckDB as the backend
- **Docker-Based**: Runs in an isolated container
- **Standard Connector Compatible**: Works with the official snowflake-connector-python (full protocol support)
- **SQL Compatibility**: Supports common Snowflake SQL syntax and functions
- **UNDROP Support**: Recover dropped databases, schemas, tables, and views
- **Debug Dashboard**: Built-in web dashboard for monitoring sessions, queries, and logs
- **Zero Cost**: No Snowflake subscription required
- **Offline Development**: Works without internet connection
- **Testing Ready**: Perfect for unit and functional testing with tox support

## Quick Start

### Using Docker Compose

```bash
# Start Snowglobe server
docker-compose up -d

# The server will be available at localhost:8084
```

### Using the Standard Snowflake Connector

```python
import snowflake.connector

# Connect to local Snowglobe server using standard connector
conn = snowflake.connector.connect(
    host='localhost',
    port=8084,
    user='test_user',
    password='test_password',
    database='test_db',
    schema='public',
    account='snowglobe',
    insecure_mode=True,        # no TSL for now
    protocol='http'            # only HTTP available for now
)

# Execute queries just like with Snowflake
cursor = conn.cursor()
cursor.execute("CREATE TABLE users (id INT, name VARCHAR, email VARCHAR)")
cursor.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
cursor.execute("SELECT * FROM users")

for row in cursor:
    print(row)

# UNDROP feature - recover dropped objects
cursor.execute("DROP TABLE users")
cursor.execute("UNDROP TABLE users")  # Restore the table!

cursor.close()
conn.close()
```

## Installation

### Server (Docker)

```bash
# Build the Docker image
docker build -t snowglobe:latest .

# Run the container
docker run -d -p 8084:8084 --name snowglobe snowglobe:latest
```

### Python Client

```bash
# Install the standard Snowflake connector
pip install snowflake-connector-python

# Or install Snowglobe with client extras
pip install snowglobe[client]
```

## Supported Features

### SQL Operations
- CREATE/DROP DATABASE
- CREATE/DROP SCHEMA
- CREATE/DROP TABLE
- UNDROP DATABASE/SCHEMA/TABLE/VIEW
- TRUNCATE TABLE
- ALTER TABLE RENAME
- CREATE TABLE CLONE
- INSERT, UPDATE, DELETE
- SELECT with JOINs, WHERE, GROUP BY, ORDER BY, LIMIT
- Common aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Window functions
- CTEs (Common Table Expressions)
- SHOW DATABASES/SCHEMAS/TABLES
- SHOW DROPPED DATABASES/SCHEMAS/TABLES

### Snowflake-Specific Functions
- CURRENT_TIMESTAMP(), CURRENT_DATE(), CURRENT_TIME()
- DATEADD(), DATEDIFF()
- TO_DATE(), TO_TIMESTAMP()
- NVL(), NVL2(), COALESCE()
- IFF(), DECODE()
- PARSE_JSON(), OBJECT_CONSTRUCT()
- ARRAY_AGG(), ARRAY_CONSTRUCT()
- SPLIT_PART(), REGEXP_LIKE()
- And many more...

### Data Types
- NUMBER/NUMERIC/DECIMAL/INT/INTEGER/BIGINT/SMALLINT/TINYINT/FLOAT/DOUBLE
- VARCHAR/CHAR/STRING/TEXT
- BOOLEAN
- DATE/TIME/TIMESTAMP/TIMESTAMP_NTZ/TIMESTAMP_LTZ/TIMESTAMP_TZ
- VARIANT/OBJECT/ARRAY
- BINARY/VARBINARY

## Debug Dashboard

Snowglobe includes a built-in web dashboard for monitoring and debugging:

```bash
# Access the dashboard at:
http://localhost:8084/dashboard
```

### Dashboard Features

- **Server Overview**: Uptime, active sessions, query statistics
- **Query History**: View all executed queries with status, duration, and error details
- **Session Monitoring**: Track active sessions and their context (database, schema, warehouse, role)
- **Auto-refresh**: Real-time updates (every 5 seconds)

### Frontend Development

For a more feature-rich dashboard experience, use the Vue.js frontend:

```bash
cd frontend
npm install
npm run dev
# Dashboard available at http://localhost:3000
```

## Configuration

### Environment Variables

- `SNOWGLOBE_PORT`: Server port (default: 8084)
- `SNOWGLOBE_DATA_DIR`: Data directory path (default: /data)
- `SNOWGLOBE_LOG_LEVEL`: Logging level (default: INFO)

### Docker Compose Configuration

```yaml
version: '3.8'
services:
  snowglobe:
    build: .
    ports:
      - "8084:8084"
    volumes:
      - snowglobe_data:/data
    environment:
      - SNOWGLOBE_LOG_LEVEL=DEBUG

volumes:
  snowglobe_data:
```

## Testing

Run the test suite:

```bash
# Install test dependencies
pip install -e .[test]

# Run tests with pytest
pytest tests/

# Run authentication tests specifically
pytest tests/test_authentication.py -v

# Run tests with tox (multiple Python versions)
tox

# Run specific test environment
tox -e py311

# Run linting
tox -e lint

# Run type checking
tox -e typecheck

# Run integration tests
tox -e integration

# Run tests in Docker
tox -e docker
```

### Test Fixtures

Tests use shared fixtures defined in `tests/conftest.py` for:
- Temporary directories
- Metadata store instances
- Query executor instances
- Sample data setups
- Dropped objects for UNDROP testing

### Authentication Tests

The authentication system has comprehensive tests covering:
- Snowflake protocol compatibility
- Session management
- Token generation and validation
- Gzip request compression handling
- Query execution through the authenticated channel
- Error handling and edge cases

## Architecture

Snowglobe consists of:

1. **Server**: A FastAPI-based HTTP server implementing the Snowflake REST API protocol
2. **Authentication**: Full Snowflake-compatible authentication with session tokens
3. **SQL Engine**: DuckDB-based query executor with Snowflake SQL translation
4. **Metadata Store**: Manages databases, schemas, and table metadata with UNDROP support
5. **Dashboard**: Built-in web dashboard for monitoring and debugging
6. **Standard Connector**: Uses official snowflake-connector-python

```
┌─────────────────┐     HTTP/JSON     ┌─────────────────┐
│   Snowflake     │ ◄──────────────► │  Snowglobe      │
│   Connector     │  (Snowflake API) │  Server         │
│   (standard)    │                   │  (FastAPI)      │
└─────────────────┘                   └────────┬────────┘
                                               │
                                      ┌────────▼────────┐
                                      │  Authentication │
                                      │  & Sessions     │
                                      └────────┬────────┘
                                               │
                                      ┌────────▼────────┐
                                      │  SQL Translator │
                                      │  & Executor     │
                                      └────────┬────────┘
                                               │
                                      ┌────────▼────────┐
                                      │    DuckDB       │
                                      │   (Backend)     │
                                      └─────────────────┘
```

### API Endpoints

**Snowflake Protocol Endpoints:**
- `POST /session/v1/login-request` - Authenticate and create session
- `POST /queries/v1/query-request` - Execute SQL queries
- `POST /session` - Close session

**Dashboard API Endpoints:**
- `GET /dashboard` - Embedded web dashboard
- `GET /api/stats` - Server statistics
- `GET /api/sessions` - List active sessions
- `GET /api/queries` - Query history
- `GET /api/databases` - List databases
- `DELETE /api/queries/history` - Clear query history

## Limitations

- Not all Snowflake functions are implemented (contributions welcome!)
- UNDROP restores metadata but data may not be fully preserved in all cases
- No support for Snowflake-specific features like:
  - Time Travel (partial - UNDROP supported)
  - Data Sharing
  - Streams and Tasks
  - Stored Procedures (limited support)
- Performance characteristics differ from actual Snowflake
- No multi-cluster warehouse simulation

## Use Cases

1. **Local Development**: Develop Snowflake-based applications without cloud costs
2. **Unit Testing**: Test SQL queries and data pipelines locally
3. **CI/CD Pipelines**: Run automated tests without Snowflake credentials
4. **Learning**: Study Snowflake SQL syntax without a subscription
5. **Prototyping**: Quickly prototype data models and queries

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- Inspired by [LocalStack](https://github.com/localstack/localstack)
- Built on [DuckDB](https://duckdb.org/) for SQL execution
- Uses [FastAPI](https://fastapi.tiangolo.com/) for the server

---

**Note**: Snowglobe is not affiliated with Snowflake Inc. This is an independent open-source project for local development and testing purposes.
