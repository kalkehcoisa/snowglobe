"""
Snowglobe Server - FastAPI-based HTTP server for Snowflake emulation
"""

import os
import uuid
import logging
import json
import gzip
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .query_executor import QueryExecutor

# Configure logging
logging.basicConfig(
    level=os.getenv("SNOWGLOBE_LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("snowglobe")

# Global state
sessions: Dict[str, Dict[str, Any]] = {}  # session_token -> session info
data_dir = os.getenv("SNOWGLOBE_DATA_DIR", "/data")

# Query history for the frontend
query_history: List[Dict[str, Any]] = []
MAX_QUERY_HISTORY = 1000


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Snowglobe server starting up...")
    os.makedirs(data_dir, exist_ok=True)
    yield
    logger.info("Snowglobe server shutting down...")
    # Clean up sessions
    for session_token, session_info in sessions.items():
        try:
            session_info["executor"].close()
        except Exception:
            pass
    sessions.clear()


# Create FastAPI app
app = FastAPI(
    title="Snowglobe",
    description="Local Snowflake Emulator for Python Developers",
    version="0.1.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Store server start time
server_start_time = datetime.utcnow()


def generate_token():
    """Generate a session token"""
    return str(uuid.uuid4()).replace("-", "") + str(uuid.uuid4()).replace("-", "")


def get_session_from_token(auth_header: str) -> Optional[Dict[str, Any]]:
    """Extract session from Authorization header"""
    if not auth_header:
        return None
    
    # Format: Snowflake Token="<token>"
    if auth_header.startswith('Snowflake Token="') and auth_header.endswith('"'):
        token = auth_header[17:-1]
        return sessions.get(token)
    
    return None


def add_query_to_history(query: str, session_id: str, success: bool, 
                          duration_ms: float, rows_affected: int, 
                          error: Optional[str] = None):
    """Add query to history for frontend viewing"""
    global query_history
    
    entry = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "session_id": session_id,
        "query": query,
        "success": success,
        "duration_ms": duration_ms,
        "rows_affected": rows_affected,
        "error": error
    }
    
    query_history.append(entry)
    
    # Keep only last N queries
    if len(query_history) > MAX_QUERY_HISTORY:
        query_history = query_history[-MAX_QUERY_HISTORY:]


# ========== Snowflake-Compatible API Endpoints ==========

async def get_request_body(request: Request) -> dict:
    """Get request body, handling gzip compression if present"""
    content_encoding = request.headers.get("Content-Encoding", "")
    raw_body = await request.body()
    
    if content_encoding == "gzip" or (raw_body and len(raw_body) >= 2 and raw_body[0:2] == b'\x1f\x8b'):
        # Decompress gzip data
        try:
            decompressed = gzip.decompress(raw_body)
            return json.loads(decompressed)
        except Exception as e:
            logger.error(f"Failed to decompress gzip body: {e}")
            raise
    else:
        return json.loads(raw_body)


@app.post("/session/v1/login-request")
async def login_request(request: Request):
    """
    Snowflake-compatible login endpoint.
    This is called by the official Snowflake Python connector.
    """
    try:
        body = await get_request_body(request)
        data = body.get("data", {})
        
        # Extract login information
        login_name = data.get("LOGIN_NAME", "")
        password = data.get("PASSWORD", "")
        account_name = data.get("ACCOUNT_NAME", "snowglobe")
        
        # Extract optional parameters from query string
        params = request.query_params
        database = params.get("databaseName")
        schema_name = params.get("schemaName")
        warehouse = params.get("warehouse")
        role = params.get("roleName")
        
        logger.info(f"Login request from user: {login_name}, account: {account_name}")
        
        # For Snowglobe, we accept any credentials (local development)
        # In production, you might want to validate credentials
        
        # Generate tokens
        session_token = generate_token()
        master_token = generate_token()
        session_id = str(uuid.uuid4())
        
        # Create query executor for this session
        executor = QueryExecutor(data_dir)
        
        # Set initial context
        if database:
            db_upper = database.upper()
            if not executor.metadata.database_exists(db_upper):
                executor.metadata.create_database(db_upper, if_not_exists=True)
            executor.current_database = db_upper
        
        if schema_name:
            schema_upper = schema_name.upper()
            if not executor.metadata.schema_exists(executor.current_database, schema_upper):
                executor.metadata.create_schema(executor.current_database, schema_upper, if_not_exists=True)
            executor.current_schema = schema_upper
        
        if warehouse:
            executor.current_warehouse = warehouse.upper()
        
        if role:
            executor.current_role = role.upper()
        
        # Ensure DuckDB schema exists
        executor._ensure_schema_exists(executor.current_database, executor.current_schema)
        
        # Store session
        sessions[session_token] = {
            "executor": executor,
            "session_id": session_id,
            "user": login_name,
            "account": account_name,
            "created_at": datetime.utcnow(),
            "master_token": master_token,
            "database": executor.current_database,
            "schema": executor.current_schema,
            "warehouse": executor.current_warehouse,
            "role": executor.current_role
        }
        
        logger.info(f"Session created: {session_id}, token: {session_token[:8]}...")
        
        # Return Snowflake-compatible response
        response = {
            "data": {
                "token": session_token,
                "masterToken": master_token,
                "validityInSeconds": 3600,
                "masterValidityInSeconds": 14400,
                "displayUserName": login_name.upper(),
                "serverVersion": "Snowglobe 0.1.0",
                "firstLogin": False,
                "remMeToken": None,
                "remMeValidityInSeconds": 0,
                "healthCheckInterval": 45,
                "newClientForUpgrade": None,
                "sessionId": session_id,
                "parameters": [
                    {"name": "TIMESTAMP_OUTPUT_FORMAT", "value": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"},
                    {"name": "CLIENT_PREFETCH_THREADS", "value": 4},
                    {"name": "TIMESTAMP_NTZ_OUTPUT_FORMAT", "value": "YYYY-MM-DD HH24:MI:SS.FF3"},
                    {"name": "CLIENT_RESULT_CHUNK_SIZE", "value": 160},
                    {"name": "CLIENT_SESSION_KEEP_ALIVE", "value": False},
                    {"name": "QUERY_RESULT_FORMAT", "value": "json"},
                    {"name": "TIMESTAMP_LTZ_OUTPUT_FORMAT", "value": ""},
                    {"name": "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX", "value": False},
                    {"name": "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ", "value": True},
                    {"name": "CLIENT_MEMORY_LIMIT", "value": 1536},
                    {"name": "CLIENT_TIMESTAMP_TYPE_MAPPING", "value": "TIMESTAMP_LTZ"},
                    {"name": "TIMEZONE", "value": "America/Los_Angeles"},
                    {"name": "CLIENT_RESULT_PREFETCH_SLOTS", "value": 2},
                    {"name": "CLIENT_RESULT_PREFETCH_THREADS", "value": 1},
                    {"name": "CLIENT_USE_V1_QUERY_API", "value": True},
                    {"name": "ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1", "value": False},
                ],
                "sessionInfo": {
                    "databaseName": executor.current_database,
                    "schemaName": executor.current_schema,
                    "warehouseName": executor.current_warehouse,
                    "roleName": executor.current_role
                },
                "idToken": None,
                "idTokenValidityInSeconds": 0,
                "responseData": None,
                "mfaToken": None,
                "mfaTokenValidityInSeconds": 0
            },
            "code": None,
            "message": None,
            "success": True
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Login error: {str(e)}", exc_info=True)
        return {
            "data": None,
            "code": "390100",
            "message": f"Authentication failed: {str(e)}",
            "success": False
        }


@app.post("/session/v1/login-request:renew")
async def renew_session(request: Request):
    """Renew session token"""
    auth_header = request.headers.get("Authorization", "")
    session = get_session_from_token(auth_header)
    
    if not session:
        return {
            "data": None,
            "code": "390104",
            "message": "Invalid session token",
            "success": False
        }
    
    # Generate new token
    new_token = generate_token()
    
    # Move session to new token
    for old_token, sess in list(sessions.items()):
        if sess["session_id"] == session["session_id"]:
            sessions[new_token] = sess
            del sessions[old_token]
            break
    
    return {
        "data": {
            "token": new_token,
            "validityInSeconds": 3600
        },
        "code": None,
        "message": None,
        "success": True
    }


@app.post("/session")
async def delete_session(request: Request):
    """Close/delete session"""
    auth_header = request.headers.get("Authorization", "")
    
    for token, session in list(sessions.items()):
        if auth_header.endswith(f'"{token}"'):
            try:
                session["executor"].close()
            except Exception:
                pass
            del sessions[token]
            logger.info(f"Session closed: {session['session_id']}")
            return {
                "data": None,
                "code": None,
                "message": None,
                "success": True
            }
    
    return {
        "data": None,
        "code": None,
        "message": None,
        "success": True
    }


@app.post("/queries/v1/query-request")
async def query_request(request: Request):
    """
    Snowflake-compatible query execution endpoint.
    This is called by the official Snowflake Python connector.
    """
    start_time = datetime.utcnow()
    
    auth_header = request.headers.get("Authorization", "")
    session = get_session_from_token(auth_header)
    
    if not session:
        return {
            "data": {
                "errorCode": "390104",
                "errorMessage": "Invalid session token"
            },
            "code": "390104",
            "message": "Invalid session token",
            "success": False
        }
    
    try:
        body = await get_request_body(request)
        sql_text = body.get("sqlText", "")
        sequence_id = body.get("sequenceId", 1)
        
        logger.debug(f"Query request: {sql_text[:100]}...")
        
        executor = session["executor"]
        
        # Execute the query
        result = executor.execute(sql_text)
        
        # Calculate duration
        end_time = datetime.utcnow()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        if result["success"]:
            # Map column types to Snowflake types
            row_type = []
            for col_name in result["columns"]:
                row_type.append({
                    "name": col_name.upper(),
                    "database": executor.current_database,
                    "schema": executor.current_schema,
                    "table": "",
                    "nullable": True,
                    "type": "text",  # Simplified type mapping
                    "length": None,
                    "scale": None,
                    "precision": None,
                    "byteLength": None,
                    "collation": None
                })
            
            # Convert data to Snowflake format
            rowset = []
            for row in result["data"]:
                # Convert each value to string (Snowflake JSON format)
                rowset.append([str(v) if v is not None else None for v in row])
            
            # Determine statement type
            sql_upper = sql_text.strip().upper()
            if sql_upper.startswith("SELECT") or sql_upper.startswith("SHOW") or sql_upper.startswith("DESCRIBE"):
                statement_type_id = 4096  # SELECT
            elif sql_upper.startswith("INSERT"):
                statement_type_id = 4608  # INSERT
            elif sql_upper.startswith("UPDATE"):
                statement_type_id = 4864  # UPDATE
            elif sql_upper.startswith("DELETE"):
                statement_type_id = 5120  # DELETE
            elif sql_upper.startswith("CREATE"):
                statement_type_id = 8192  # DDL
            elif sql_upper.startswith("DROP"):
                statement_type_id = 8192  # DDL
            elif sql_upper.startswith("ALTER"):
                statement_type_id = 8192  # DDL
            else:
                statement_type_id = 0  # Unknown
            
            # Add to query history
            add_query_to_history(
                sql_text,
                session["session_id"],
                True,
                duration_ms,
                result["rowcount"]
            )
            
            response = {
                "data": {
                    "parameters": [],
                    "rowtype": row_type,
                    "rowset": rowset,
                    "total": result["rowcount"],
                    "returned": len(rowset),
                    "queryId": str(uuid.uuid4()),
                    "databaseProvider": None,
                    "finalDatabaseName": executor.current_database,
                    "finalSchemaName": executor.current_schema,
                    "finalWarehouseName": executor.current_warehouse,
                    "finalRoleName": executor.current_role,
                    "numberOfBinds": 0,
                    "statementTypeId": statement_type_id,
                    "version": 1,
                    "sendResultTime": int(end_time.timestamp() * 1000),
                    "queryResultFormat": "json"
                },
                "code": None,
                "message": None,
                "success": True
            }
            
            logger.debug(f"Query successful, {result['rowcount']} rows")
            
            # Update session info if context changed
            session["database"] = executor.current_database
            session["schema"] = executor.current_schema
            session["warehouse"] = executor.current_warehouse
            session["role"] = executor.current_role
            
            return response
        else:
            # Query failed
            error_msg = result.get("error", "Unknown error")
            
            add_query_to_history(
                sql_text,
                session["session_id"],
                False,
                duration_ms,
                0,
                error_msg
            )
            
            return {
                "data": {
                    "errorCode": "100001",
                    "errorMessage": error_msg
                },
                "code": "100001",
                "message": error_msg,
                "success": False
            }
            
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}", exc_info=True)
        
        end_time = datetime.utcnow()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        add_query_to_history(
            sql_text if 'sql_text' in locals() else "Unknown",
            session["session_id"],
            False,
            duration_ms,
            0,
            str(e)
        )
        
        return {
            "data": {
                "errorCode": "100001",
                "errorMessage": str(e)
            },
            "code": "100001",
            "message": str(e),
            "success": False
        }


@app.post("/queries/v1/abort-request")
async def abort_request(request: Request):
    """Abort a running query"""
    # For now, just return success
    return {
        "data": None,
        "code": None,
        "message": None,
        "success": True
    }


# ========== Health Check and Info Endpoints ==========

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    uptime = datetime.utcnow() - server_start_time
    return {
        "status": "healthy",
        "version": "0.1.0",
        "uptime": str(uptime),
        "active_sessions": len(sessions),
        "queries_executed": len(query_history)
    }


# ========== Frontend API Endpoints ==========

@app.get("/api/sessions")
async def list_sessions():
    """List all active sessions (for frontend)"""
    session_list = []
    for token, session in sessions.items():
        session_list.append({
            "session_id": session["session_id"],
            "user": session["user"],
            "database": session["database"],
            "schema": session["schema"],
            "warehouse": session["warehouse"],
            "role": session["role"],
            "created_at": session["created_at"].isoformat(),
            "token_prefix": token[:8] + "..."
        })
    return {"sessions": session_list}


@app.get("/api/queries")
async def list_queries(limit: int = 100, offset: int = 0):
    """List query history (for frontend)"""
    # Return queries in reverse chronological order
    sorted_queries = list(reversed(query_history))
    return {
        "queries": sorted_queries[offset:offset + limit],
        "total": len(query_history)
    }


@app.get("/api/databases")
async def api_list_databases():
    """List all databases (for frontend)"""
    if not sessions:
        # Create a temporary executor for metadata access
        executor = QueryExecutor(data_dir)
        databases = executor.metadata.list_databases()
        executor.close()
    else:
        # Use first available session
        first_session = next(iter(sessions.values()))
        databases = first_session["executor"].metadata.list_databases()
    
    return {"databases": databases}


@app.get("/api/databases/{database}/schemas")
async def api_list_schemas(database: str):
    """List schemas in a database (for frontend)"""
    if not sessions:
        executor = QueryExecutor(data_dir)
        try:
            schemas = executor.metadata.list_schemas(database.upper())
        finally:
            executor.close()
    else:
        first_session = next(iter(sessions.values()))
        schemas = first_session["executor"].metadata.list_schemas(database.upper())
    
    return {"schemas": schemas}


@app.get("/api/databases/{database}/schemas/{schema_name}/tables")
async def api_list_tables(database: str, schema_name: str):
    """List tables in a schema (for frontend)"""
    if not sessions:
        executor = QueryExecutor(data_dir)
        try:
            tables = executor.metadata.list_tables(database.upper(), schema_name.upper())
        finally:
            executor.close()
    else:
        first_session = next(iter(sessions.values()))
        tables = first_session["executor"].metadata.list_tables(database.upper(), schema_name.upper())
    
    return {"tables": tables}


@app.get("/api/stats")
async def get_stats():
    """Get server statistics (for frontend)"""
    uptime = datetime.utcnow() - server_start_time
    
    # Calculate query statistics
    total_queries = len(query_history)
    successful_queries = sum(1 for q in query_history if q["success"])
    failed_queries = total_queries - successful_queries
    
    avg_duration = 0
    if total_queries > 0:
        avg_duration = sum(q["duration_ms"] for q in query_history) / total_queries
    
    return {
        "uptime_seconds": uptime.total_seconds(),
        "uptime_formatted": str(uptime),
        "active_sessions": len(sessions),
        "total_queries": total_queries,
        "successful_queries": successful_queries,
        "failed_queries": failed_queries,
        "average_query_duration_ms": round(avg_duration, 2),
        "server_start_time": server_start_time.isoformat()
    }


@app.delete("/api/queries/history")
async def clear_query_history():
    """Clear query history (for frontend)"""
    global query_history
    query_history = []
    return {"success": True, "message": "Query history cleared"}


@app.post("/api/execute")
async def execute_query(request: Request):
    """Execute a query from the frontend worksheet"""
    try:
        body = await request.json()
        sql = body.get("sql", "").strip()
        
        if not sql:
            return {"success": False, "error": "No SQL provided"}
        
        # Create a temporary session if none exists, or use first available
        if sessions:
            first_session = next(iter(sessions.values()))
            executor = first_session["executor"]
            session_id = first_session["session_id"]
        else:
            # Create temporary executor
            executor = QueryExecutor(data_dir)
            session_id = "frontend-temp"
        
        # Execute query
        start_time = datetime.utcnow()
        result = executor.execute(sql)
        end_time = datetime.utcnow()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        # Add to history
        add_query_to_history(
            sql,
            session_id,
            result["success"],
            duration_ms,
            result["rowcount"],
            result.get("error")
        )
        
        if result["success"]:
            return {
                "success": True,
                "columns": result["columns"],
                "data": result["data"],
                "rowcount": result["rowcount"],
                "duration_ms": duration_ms
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Unknown error"),
                "duration_ms": duration_ms
            }
    
    except Exception as e:
        logger.error(f"Frontend query execution error: {str(e)}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Serve embedded dashboard HTML"""
    return get_dashboard_html()


def get_dashboard_html():
    """Return the embedded dashboard HTML"""
    return '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Snowglobe Dashboard</title>
    <style>
        :root {
            --primary: #29b5e8;
            --success: #10b981;
            --error: #ef4444;
            --bg: #f8fafc;
            --card: #ffffff;
            --text: #1e293b;
            --muted: #64748b;
            --border: #e2e8f0;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: var(--bg); color: var(--text); }
        .header { background: linear-gradient(135deg, var(--primary), #1aa3d9); color: white; padding: 1rem 2rem; }
        .header h1 { font-size: 1.5rem; }
        .container { max-width: 1200px; margin: 0 auto; padding: 2rem; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
        .stat-card { background: var(--card); padding: 1.5rem; border-radius: 12px; border: 1px solid var(--border); }
        .stat-value { font-size: 2rem; font-weight: 700; }
        .stat-label { color: var(--muted); font-size: 0.9rem; }
        .section { background: var(--card); padding: 1.5rem; border-radius: 12px; border: 1px solid var(--border); margin-bottom: 1.5rem; }
        .section h2 { font-size: 1.1rem; margin-bottom: 1rem; }
        .query-item { background: var(--bg); padding: 1rem; border-radius: 8px; margin-bottom: 0.75rem; }
        .query-item.failed { border-left: 4px solid var(--error); }
        .query-header { display: flex; justify-content: space-between; margin-bottom: 0.5rem; font-size: 0.85rem; }
        .status { display: flex; align-items: center; gap: 0.5rem; }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; }
        .status-dot.success { background: var(--success); }
        .status-dot.error { background: var(--error); }
        .query-sql { font-family: monospace; font-size: 0.9rem; background: white; padding: 0.5rem; border-radius: 4px; word-break: break-all; }
        .query-meta { font-size: 0.8rem; color: var(--muted); margin-top: 0.5rem; }
        .query-error { background: #fee2e2; color: #991b1b; padding: 0.5rem; border-radius: 4px; margin-top: 0.5rem; font-size: 0.85rem; }
        .session-item { background: var(--bg); padding: 1rem; border-radius: 8px; margin-bottom: 0.5rem; }
        .session-user { font-weight: 600; margin-bottom: 0.5rem; }
        .session-details { font-size: 0.85rem; color: var(--muted); }
        .empty { text-align: center; padding: 2rem; color: var(--muted); }
        .actions { margin-bottom: 1rem; }
        button { padding: 0.5rem 1rem; border: 1px solid var(--border); border-radius: 8px; background: white; cursor: pointer; margin-right: 0.5rem; }
        button:hover { background: var(--bg); }
        .auto-refresh { font-size: 0.85rem; color: var(--muted); }
    </style>
</head>
<body>
    <div class="header">
        <h1>❄️ Snowglobe Dashboard</h1>
    </div>
    <div class="container">
        <div class="actions">
            <button onclick="refresh()">🔄 Refresh</button>
            <button onclick="clearHistory()">🗑️ Clear History</button>
            <span class="auto-refresh">Auto-refresh: <span id="autoStatus">ON</span> <button onclick="toggleAuto()">Toggle</button></span>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value" id="uptime">-</div>
                <div class="stat-label">Uptime</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="sessions">0</div>
                <div class="stat-label">Active Sessions</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="queries">0</div>
                <div class="stat-label">Total Queries</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="success">0</div>
                <div class="stat-label">Successful</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="failed">0</div>
                <div class="stat-label">Failed</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="avgTime">0ms</div>
                <div class="stat-label">Avg Query Time</div>
            </div>
        </div>

        <div class="section">
            <h2>📝 Recent Queries</h2>
            <div id="queryList"><div class="empty">No queries yet</div></div>
        </div>

        <div class="section">
            <h2>🔗 Active Sessions</h2>
            <div id="sessionList"><div class="empty">No active sessions</div></div>
        </div>
    </div>

    <script>
        let autoRefresh = true;
        let refreshInterval;

        async function fetchStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                document.getElementById('uptime').textContent = formatUptime(data.uptime_seconds);
                document.getElementById('sessions').textContent = data.active_sessions;
                document.getElementById('queries').textContent = data.total_queries;
                document.getElementById('success').textContent = data.successful_queries;
                document.getElementById('failed').textContent = data.failed_queries;
                document.getElementById('avgTime').textContent = Math.round(data.average_query_duration_ms) + 'ms';
            } catch (e) { console.error('Stats fetch error:', e); }
        }

        async function fetchQueries() {
            try {
                const res = await fetch('/api/queries?limit=20');
                const data = await res.json();
                const list = document.getElementById('queryList');
                if (data.queries.length === 0) {
                    list.innerHTML = '<div class="empty">No queries yet</div>';
                } else {
                    list.innerHTML = data.queries.map(q => `
                        <div class="query-item ${q.success ? '' : 'failed'}">
                            <div class="query-header">
                                <div class="status">
                                    <span class="status-dot ${q.success ? 'success' : 'error'}"></span>
                                    ${q.success ? 'Success' : 'Failed'}
                                </div>
                                <div>${new Date(q.timestamp).toLocaleString()}</div>
                            </div>
                            <div class="query-sql">${escapeHtml(q.query.substring(0, 200))}${q.query.length > 200 ? '...' : ''}</div>
                            <div class="query-meta">⏱️ ${Math.round(q.duration_ms)}ms | 📊 ${q.rows_affected} rows | 🔗 ${q.session_id.substring(0, 8)}...</div>
                            ${q.error ? `<div class="query-error">❌ ${escapeHtml(q.error)}</div>` : ''}
                        </div>
                    `).join('');
                }
            } catch (e) { console.error('Queries fetch error:', e); }
        }

        async function fetchSessions() {
            try {
                const res = await fetch('/api/sessions');
                const data = await res.json();
                const list = document.getElementById('sessionList');
                if (data.sessions.length === 0) {
                    list.innerHTML = '<div class="empty">No active sessions</div>';
                } else {
                    list.innerHTML = data.sessions.map(s => `
                        <div class="session-item">
                            <div class="session-user">👤 ${escapeHtml(s.user)}</div>
                            <div class="session-details">
                                🗄️ ${s.database} | 📁 ${s.schema} | 🏭 ${s.warehouse} | 👔 ${s.role}<br>
                                Session: ${s.session_id.substring(0, 16)}... | Created: ${new Date(s.created_at).toLocaleString()}
                            </div>
                        </div>
                    `).join('');
                }
            } catch (e) { console.error('Sessions fetch error:', e); }
        }

        function formatUptime(seconds) {
            if (!seconds) return '0s';
            const h = Math.floor(seconds / 3600);
            const m = Math.floor((seconds % 3600) / 60);
            const s = Math.floor(seconds % 60);
            if (h > 0) return `${h}h ${m}m`;
            if (m > 0) return `${m}m ${s}s`;
            return `${s}s`;
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.appendChild(document.createTextNode(text));
            return div.innerHTML;
        }

        function refresh() {
            fetchStats();
            fetchQueries();
            fetchSessions();
        }

        async function clearHistory() {
            if (confirm('Clear all query history?')) {
                await fetch('/api/queries/history', { method: 'DELETE' });
                refresh();
            }
        }

        function toggleAuto() {
            autoRefresh = !autoRefresh;
            document.getElementById('autoStatus').textContent = autoRefresh ? 'ON' : 'OFF';
        }

        refresh();
        refreshInterval = setInterval(() => { if (autoRefresh) refresh(); }, 5000);
    </script>
</body>
</html>'''


def main():
    """Run the server"""
    import uvicorn
    
    port = int(os.getenv("SNOWGLOBE_PORT", "8084"))
    https_port = int(os.getenv("SNOWGLOBE_HTTPS_PORT", "8443"))
    host = os.getenv("SNOWGLOBE_HOST", "0.0.0.0")
    enable_https = os.getenv("SNOWGLOBE_ENABLE_HTTPS", "false").lower() == "true"
    
    # SSL/TLS certificate paths
    cert_path = os.getenv("SNOWGLOBE_CERT_PATH", "/app/certs/cert.pem")
    key_path = os.getenv("SNOWGLOBE_KEY_PATH", "/app/certs/key.pem")
    
    if enable_https and os.path.exists(cert_path) and os.path.exists(key_path):
        logger.info(f"Starting Snowglobe server with HTTPS on {host}:{https_port}")
        logger.info(f"SSL Certificate: {cert_path}")
        logger.info(f"Also serving HTTP on {host}:{port}")
        
        # Start HTTPS server in main thread
        import threading
        
        # Start HTTP server in background thread for health checks and backward compatibility
        def run_http():
            uvicorn.run(
                "snowglobe_server.server:app",
                host=host,
                port=port,
                log_level="warning",
                reload=False
            )
        
        http_thread = threading.Thread(target=run_http, daemon=True)
        http_thread.start()
        
        # Run HTTPS server in main thread
        uvicorn.run(
            "snowglobe_server.server:app",
            host=host,
            port=https_port,
            log_level="info",
            reload=False,
            ssl_keyfile=key_path,
            ssl_certfile=cert_path,
            ssl_version=3,  # TLS 1.2+
            ssl_cert_reqs=0,  # Don't require client certificates
        )
    else:
        logger.info(f"Starting Snowglobe server on {host}:{port} (HTTP only)")
        if enable_https:
            logger.warning(f"HTTPS enabled but certificates not found at {cert_path} and {key_path}")
        
        uvicorn.run(
            "snowglobe_server.server:app",
            host=host,
            port=port,
            log_level="info",
            reload=False
        )


if __name__ == "__main__":
    main()
