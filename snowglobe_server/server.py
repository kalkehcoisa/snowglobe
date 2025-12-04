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
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pathlib import Path
import re

from .query_executor import QueryExecutor
from .decorators import (
    handle_exceptions,
    log_execution_time,
    create_success_response,
    create_error_response,
    get_session_from_token,
    calculate_duration_ms,
    get_statement_type_id,
    QueryHistoryManager,
    SessionManager
)
from .template_loader import load_template
from .dbt_adapter import (
    DbtAdapter,
    DbtModel,
    DbtSource,
    DbtSeed,
    DbtTest,
    DbtSnapshot,
    DbtProjectConfig,
    MaterializationType,
    generate_profiles_yml,
    generate_sample_project,
)
from .information_schema import InformationSchemaBuilder
from .data_import import DataImporter
from .workspace import WorkspaceManager

# Configure logging
logging.basicConfig(
    level=os.getenv("SNOWGLOBE_LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("snowglobe")

# Global state
data_dir = os.getenv("SNOWGLOBE_DATA_DIR", "/data")

# Use new managers for better organization
session_manager = SessionManager()
query_history_manager = QueryHistoryManager(max_size=1000)
workspace_manager = WorkspaceManager(data_dir)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Snowglobe server starting up...")
    os.makedirs(data_dir, exist_ok=True)
    yield
    logger.info("Snowglobe server shutting down...")
    # Clean up sessions using session manager
    session_manager.cleanup_all()


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

# Mount static files for Vue frontend assets
# This needs to be done after app creation
_static_dir = Path(__file__).parent / "static"
if _static_dir.exists() and (_static_dir / "assets").exists():
    app.mount("/dashboard/assets", StaticFiles(directory=_static_dir / "assets"), name="dashboard_assets")


# Store server start time
server_start_time = datetime.utcnow()


def generate_token():
    """Generate a session token"""
    return str(uuid.uuid4()).replace("-", "") + str(uuid.uuid4()).replace("-", "")


# Removed: get_session_from_token - now imported from decorators
# Removed: add_query_to_history - now using QueryHistoryManager


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
@handle_exceptions
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
        
        # Store session using session manager
        session_manager.add(session_token, {
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
        })
        
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
@handle_exceptions
async def renew_session(request: Request):
    """Renew session token"""
    auth_header = request.headers.get("Authorization", "")
    session = get_session_from_token(auth_header, session_manager.sessions)
    
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
    for old_token, sess in list(session_manager.sessions.items()):
        if sess["session_id"] == session["session_id"]:
            session_manager.add(new_token, sess)
            session_manager.remove(old_token)
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
@handle_exceptions
async def delete_session(request: Request):
    """Close/delete session"""
    auth_header = request.headers.get("Authorization", "")
    
    for token, session in list(session_manager.sessions.items()):
        if auth_header.endswith(f'"{token}"'):
            session_manager.remove(token)
            logger.info(f"Session closed: {session['session_id']}")
            return create_success_response()
    
    return create_success_response()


@app.post("/queries/v1/query-request")
@handle_exceptions
async def query_request(request: Request):
    """
    Snowflake-compatible query execution endpoint.
    This is called by the official Snowflake Python connector.
    """
    start_time = datetime.utcnow()
    
    auth_header = request.headers.get("Authorization", "")
    session = get_session_from_token(auth_header, session_manager.sessions)
    
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
        duration_ms = calculate_duration_ms(start_time, end_time)
        
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
            
            # Determine statement type using helper function
            statement_type_id = get_statement_type_id(sql_text)
            
            # Add to query history
            query_history_manager.add(
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
            
            query_history_manager.add(
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
        duration_ms = calculate_duration_ms(start_time, end_time)
        
        query_history_manager.add(
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
@handle_exceptions
async def abort_request(request: Request):
    """Abort a running query"""
    # For now, just return success
    return create_success_response()


# ========== Health Check and Info Endpoints ==========

@app.get("/health")
@handle_exceptions
async def health_check():
    """Health check endpoint"""
    uptime = datetime.utcnow() - server_start_time
    return {
        "status": "healthy",
        "version": "0.1.0",
        "uptime": str(uptime),
        "active_sessions": session_manager.count(),
        "queries_executed": len(query_history_manager.history)
    }


# ========== Frontend API Endpoints ==========

@app.get("/api/sessions")
@handle_exceptions
async def list_sessions():
    """List all active sessions (for frontend)"""
    return {"sessions": session_manager.list_all()}


@app.get("/api/queries")
@handle_exceptions
async def list_queries(limit: int = 100, offset: int = 0):
    """List query history (for frontend)"""
    return {
        "queries": query_history_manager.get_recent(limit, offset),
        "total": len(query_history_manager.history)
    }


@app.get("/api/databases")
@handle_exceptions
async def api_list_databases():
    """List all databases (for frontend)"""
    if session_manager.count() == 0:
        # Create a temporary executor for metadata access
        executor = QueryExecutor(data_dir)
        databases = executor.metadata.list_databases()
        executor.close()
    else:
        # Use first available session
        first_session = next(iter(session_manager.sessions.values()))
        databases = first_session["executor"].metadata.list_databases()
    
    return {"databases": databases}


@app.get("/api/databases/{database}/schemas")
@handle_exceptions
async def api_list_schemas(database: str):
    """List schemas in a database (for frontend)"""
    if session_manager.count() == 0:
        executor = QueryExecutor(data_dir)
        try:
            schemas = executor.metadata.list_schemas(database.upper())
        finally:
            executor.close()
    else:
        first_session = next(iter(session_manager.sessions.values()))
        schemas = first_session["executor"].metadata.list_schemas(database.upper())
    
    return {"schemas": schemas}


@app.get("/api/databases/{database}/schemas/{schema_name}/tables")
@handle_exceptions
async def api_list_tables(database: str, schema_name: str):
    """List tables in a schema (for frontend)"""
    if session_manager.count() == 0:
        executor = QueryExecutor(data_dir)
        try:
            tables = executor.metadata.list_tables(database.upper(), schema_name.upper())
        finally:
            executor.close()
    else:
        first_session = next(iter(session_manager.sessions.values()))
        tables = first_session["executor"].metadata.list_tables(database.upper(), schema_name.upper())
    
    return {"tables": tables}


@app.get("/api/stats")
@handle_exceptions
async def get_stats():
    """Get server statistics (for frontend)"""
    uptime = datetime.utcnow() - server_start_time
    stats = query_history_manager.get_stats()
    
    return {
        "uptime_seconds": uptime.total_seconds(),
        "uptime_formatted": str(uptime),
        "active_sessions": session_manager.count(),
        **stats,
        "server_start_time": server_start_time.isoformat()
    }


@app.delete("/api/queries/history")
@handle_exceptions
async def clear_query_history():
    """Clear query history (for frontend)"""
    query_history_manager.clear()
    return {"success": True, "message": "Query history cleared"}


@app.post("/api/execute")
@handle_exceptions
async def execute_query(request: Request):
    """Execute a query from the frontend worksheet"""
    try:
        body = await request.json()
        sql = body.get("sql", "").strip()
        
        if not sql:
            return {"success": False, "error": "No SQL provided"}
        
        # Create a temporary session if none exists, or use first available
        if session_manager.count() > 0:
            first_session = next(iter(session_manager.sessions.values()))
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
        duration_ms = calculate_duration_ms(start_time, end_time)
        
        # Add to history
        query_history_manager.add(
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


# ========== Worksheet Management Endpoints ==========

class WorksheetCreate(BaseModel):
    name: str
    sql: str = ""
    context: Optional[Dict[str, str]] = None
    folder_id: Optional[str] = None
    position: Optional[int] = None

class WorksheetUpdate(BaseModel):
    name: Optional[str] = None
    sql: Optional[str] = None
    context: Optional[Dict[str, str]] = None
    position: Optional[int] = None
    is_favorite: Optional[bool] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = None

class WorksheetReorder(BaseModel):
    worksheet_ids: List[str]


@app.get("/api/worksheets")
@handle_exceptions
async def list_worksheets(workspace_id: str = None, folder_id: str = None):
    """List all worksheets sorted by position (maintains order)"""
    worksheets = workspace_manager.list_worksheets(
        workspace_id=workspace_id,
        folder_id=folder_id,
        include_content=True
    )
    return {"worksheets": worksheets, "count": len(worksheets)}


@app.post("/api/worksheets")
@handle_exceptions
async def create_worksheet(worksheet: WorksheetCreate):
    """Create a new worksheet"""
    new_worksheet = workspace_manager.create_worksheet(
        name=worksheet.name or "New Worksheet",
        sql=worksheet.sql or "",
        folder_id=worksheet.folder_id,
        context=worksheet.context or {
            "database": "SNOWGLOBE",
            "schema": "PUBLIC",
            "warehouse": "COMPUTE_WH",
            "role": "ACCOUNTADMIN"
        },
        position=worksheet.position
    )
    
    logger.info(f"Created worksheet: {new_worksheet['id']}")
    return {"success": True, "worksheet": new_worksheet}


@app.get("/api/worksheets/{worksheet_id}")
@handle_exceptions
async def get_worksheet(worksheet_id: str):
    """Get a specific worksheet"""
    worksheet = workspace_manager.get_worksheet(worksheet_id)
    if not worksheet:
        raise HTTPException(status_code=404, detail="Worksheet not found")
    
    # Mark as recently accessed
    workspace_manager.add_to_recent(worksheet_id)
    
    return {"worksheet": worksheet}


@app.put("/api/worksheets/{worksheet_id}")
@handle_exceptions
async def update_worksheet(worksheet_id: str, worksheet: WorksheetUpdate):
    """Update a worksheet"""
    updates = {}
    if worksheet.name is not None:
        updates['name'] = worksheet.name
    if worksheet.sql is not None:
        updates['sql'] = worksheet.sql
    if worksheet.context is not None:
        updates['context'] = worksheet.context
    if worksheet.position is not None:
        updates['position'] = worksheet.position
    if worksheet.is_favorite is not None:
        updates['is_favorite'] = worksheet.is_favorite
    if worksheet.tags is not None:
        updates['tags'] = worksheet.tags
    if worksheet.description is not None:
        updates['description'] = worksheet.description
    
    updated = workspace_manager.update_worksheet(worksheet_id, updates)
    if not updated:
        raise HTTPException(status_code=404, detail="Worksheet not found")
    
    logger.info(f"Updated worksheet: {worksheet_id}")
    return {"success": True, "worksheet": updated}


@app.delete("/api/worksheets/{worksheet_id}")
@handle_exceptions
async def delete_worksheet(worksheet_id: str):
    """Delete a worksheet"""
    if not workspace_manager.delete_worksheet(worksheet_id):
        raise HTTPException(status_code=404, detail="Worksheet not found")
    
    logger.info(f"Deleted worksheet: {worksheet_id}")
    return {"success": True, "message": f"Worksheet {worksheet_id} deleted"}


@app.post("/api/worksheets/reorder")
@handle_exceptions
async def reorder_worksheets(reorder: WorksheetReorder):
    """Reorder worksheets by setting positions based on list order"""
    workspace_manager.reorder_worksheets(reorder.worksheet_ids)
    return {"success": True, "message": "Worksheets reordered"}


@app.post("/api/worksheets/{worksheet_id}/duplicate")
@handle_exceptions
async def duplicate_worksheet(worksheet_id: str, new_name: str = None):
    """Duplicate a worksheet"""
    new_worksheet = workspace_manager.duplicate_worksheet(worksheet_id, new_name)
    if not new_worksheet:
        raise HTTPException(status_code=404, detail="Worksheet not found")
    
    return {"success": True, "worksheet": new_worksheet}


@app.post("/api/worksheets/{worksheet_id}/move")
@handle_exceptions
async def move_worksheet(worksheet_id: str, request: Request):
    """Move a worksheet to a different folder"""
    body = await request.json()
    target_folder_id = body.get('folder_id')
    target_workspace_id = body.get('workspace_id')
    
    moved = workspace_manager.move_worksheet(worksheet_id, target_folder_id, target_workspace_id)
    if not moved:
        raise HTTPException(status_code=404, detail="Worksheet not found")
    
    return {"success": True, "worksheet": moved}


@app.post("/api/worksheets/{worksheet_id}/favorite")
@handle_exceptions
async def toggle_worksheet_favorite(worksheet_id: str):
    """Toggle favorite status for a worksheet"""
    is_favorite = workspace_manager.toggle_favorite(worksheet_id)
    return {"success": True, "is_favorite": is_favorite}


@app.post("/api/worksheets/{worksheet_id}/execute")
@handle_exceptions
async def execute_worksheet(worksheet_id: str, request: Request):
    """Execute the SQL in a worksheet and save results"""
    worksheet = workspace_manager.get_worksheet(worksheet_id)
    if not worksheet:
        raise HTTPException(status_code=404, detail="Worksheet not found")
    
    sql = worksheet.get("sql", "").strip()
    
    if not sql:
        return {"success": False, "error": "No SQL in worksheet"}
    
    # Get executor
    if session_manager.count() > 0:
        first_session = next(iter(session_manager.sessions.values()))
        executor = first_session["executor"]
        session_id = first_session["session_id"]
    else:
        executor = QueryExecutor(data_dir)
        session_id = "frontend-temp"
    
    # Execute query
    start_time = datetime.utcnow()
    result = executor.execute(sql)
    end_time = datetime.utcnow()
    duration_ms = calculate_duration_ms(start_time, end_time)
    
    # Add to history
    query_history_manager.add(
        sql,
        session_id,
        result["success"],
        duration_ms,
        result["rowcount"],
        result.get("error")
    )
    
    # Update worksheet execution info
    workspace_manager.update_worksheet(worksheet_id, {
        'last_executed': datetime.utcnow().isoformat(),
        'execution_count': worksheet.get('execution_count', 0) + 1
    })
    
    # Add to recent
    workspace_manager.add_to_recent(worksheet_id)
    
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


# ========== Workspace Management Endpoints ==========

class WorkspaceCreate(BaseModel):
    name: str
    description: str = ""
    icon: str = "📁"

class WorkspaceUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None
    settings: Optional[Dict[str, Any]] = None

class FolderCreate(BaseModel):
    name: str
    workspace_id: str = None
    parent_id: Optional[str] = None
    icon: str = "📂"

class FolderUpdate(BaseModel):
    name: Optional[str] = None
    icon: Optional[str] = None
    parent_id: Optional[str] = None


@app.get("/api/workspaces")
@handle_exceptions
async def list_workspaces():
    """List all workspaces"""
    workspaces = workspace_manager.list_workspaces()
    return {"workspaces": workspaces, "count": len(workspaces)}


@app.post("/api/workspaces")
@handle_exceptions
async def create_workspace(workspace: WorkspaceCreate):
    """Create a new workspace"""
    new_workspace = workspace_manager.create_workspace(
        name=workspace.name,
        description=workspace.description,
        icon=workspace.icon
    )
    return {"success": True, "workspace": new_workspace}


@app.get("/api/workspaces/{workspace_id}")
@handle_exceptions
async def get_workspace(workspace_id: str):
    """Get a specific workspace with its contents"""
    workspace = workspace_manager.get_workspace(workspace_id)
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")
    
    # Get associated folders and worksheets
    folders = workspace_manager.list_folders(workspace_id=workspace_id)
    worksheets = workspace_manager.list_worksheets(workspace_id=workspace_id)
    
    return {
        "workspace": workspace,
        "folders": folders,
        "worksheets": worksheets
    }


@app.put("/api/workspaces/{workspace_id}")
@handle_exceptions
async def update_workspace(workspace_id: str, workspace: WorkspaceUpdate):
    """Update a workspace"""
    updates = {}
    if workspace.name is not None:
        updates['name'] = workspace.name
    if workspace.description is not None:
        updates['description'] = workspace.description
    if workspace.icon is not None:
        updates['icon'] = workspace.icon
    if workspace.settings is not None:
        updates['settings'] = workspace.settings
    
    updated = workspace_manager.update_workspace(workspace_id, updates)
    if not updated:
        raise HTTPException(status_code=404, detail="Workspace not found")
    
    return {"success": True, "workspace": updated}


@app.delete("/api/workspaces/{workspace_id}")
@handle_exceptions
async def delete_workspace(workspace_id: str):
    """Delete a workspace"""
    if not workspace_manager.delete_workspace(workspace_id):
        raise HTTPException(status_code=400, detail="Cannot delete workspace (not found or is default)")
    return {"success": True, "message": f"Workspace {workspace_id} deleted"}


@app.get("/api/workspaces/{workspace_id}/export")
@handle_exceptions
async def export_workspace(workspace_id: str):
    """Export a workspace with all its contents"""
    data = workspace_manager.export_workspace(workspace_id)
    if not data:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return data


@app.post("/api/workspaces/import")
@handle_exceptions
async def import_workspace(request: Request):
    """Import a workspace from exported data"""
    body = await request.json()
    rename = body.pop('rename', None)
    
    workspace = workspace_manager.import_workspace(body, rename=rename)
    if not workspace:
        raise HTTPException(status_code=400, detail="Invalid workspace data")
    
    return {"success": True, "workspace": workspace}


# ========== Folder Management Endpoints ==========

@app.get("/api/folders")
@handle_exceptions
async def list_folders(workspace_id: str = None):
    """List all folders, optionally filtered by workspace"""
    folders = workspace_manager.list_folders(workspace_id=workspace_id)
    return {"folders": folders, "count": len(folders)}


@app.post("/api/folders")
@handle_exceptions
async def create_folder(folder: FolderCreate):
    """Create a new folder"""
    workspace_id = folder.workspace_id
    if not workspace_id:
        # Use default workspace
        settings = workspace_manager.get_settings()
        workspace_id = settings.get('default_workspace', 'default')
    
    new_folder = workspace_manager.create_folder(
        workspace_id=workspace_id,
        name=folder.name,
        parent_id=folder.parent_id,
        icon=folder.icon
    )
    
    if not new_folder:
        raise HTTPException(status_code=400, detail="Failed to create folder")
    
    return {"success": True, "folder": new_folder}


@app.get("/api/folders/{folder_id}")
@handle_exceptions
async def get_folder(folder_id: str):
    """Get a specific folder with its worksheets"""
    folder = workspace_manager.get_folder(folder_id)
    if not folder:
        raise HTTPException(status_code=404, detail="Folder not found")
    
    worksheets = workspace_manager.list_worksheets(folder_id=folder_id)
    
    return {
        "folder": folder,
        "worksheets": worksheets
    }


@app.put("/api/folders/{folder_id}")
@handle_exceptions
async def update_folder(folder_id: str, folder: FolderUpdate):
    """Update a folder"""
    updates = {}
    if folder.name is not None:
        updates['name'] = folder.name
    if folder.icon is not None:
        updates['icon'] = folder.icon
    if folder.parent_id is not None:
        updates['parent_id'] = folder.parent_id
    
    updated = workspace_manager.update_folder(folder_id, updates)
    if not updated:
        raise HTTPException(status_code=404, detail="Folder not found")
    
    return {"success": True, "folder": updated}


@app.delete("/api/folders/{folder_id}")
@handle_exceptions
async def delete_folder(folder_id: str, move_to: str = None):
    """Delete a folder, optionally moving worksheets to another folder"""
    if not workspace_manager.delete_folder(folder_id, move_to=move_to):
        raise HTTPException(status_code=400, detail="Cannot delete folder")
    return {"success": True, "message": f"Folder {folder_id} deleted"}


# ========== Recent & Favorites Endpoints ==========

@app.get("/api/recent")
@handle_exceptions
async def get_recent_worksheets(limit: int = 10):
    """Get recently accessed worksheets"""
    recent = workspace_manager.get_recent(limit=limit)
    return {"worksheets": recent, "count": len(recent)}


@app.get("/api/favorites")
@handle_exceptions
async def get_favorite_worksheets():
    """Get favorite worksheets"""
    favorites = workspace_manager.get_favorites()
    return {"worksheets": favorites, "count": len(favorites)}


# ========== Search Endpoint ==========

@app.get("/api/search")
@handle_exceptions
async def search_worksheets(q: str, workspace_id: str = None):
    """Search worksheets by name, SQL content, or tags"""
    results = workspace_manager.search_worksheets(q, workspace_id=workspace_id)
    return {"results": results, "count": len(results), "query": q}


# ========== Data Import Endpoints ==========

@app.post("/api/import/file")
@handle_exceptions
async def import_file(request: Request):
    """Import a data file (SQL, CSV, JSON, Parquet, notebook)"""
    # Get content type
    content_type = request.headers.get('content-type', '')
    
    if 'multipart/form-data' in content_type:
        # Handle file upload
        form = await request.form()
        file = form.get('file')
        if not file:
            return {"success": False, "error": "No file provided"}
        
        filename = file.filename
        content = await file.read()
        
        # Get optional options
        options = {}
        for key in ['table_name', 'database', 'schema', 'delimiter', 'encoding']:
            if key in form:
                options[key] = form[key]
        
        # Create importer
        if session_manager.count() > 0:
            first_session = next(iter(session_manager.sessions.values()))
            executor = first_session["executor"]
        else:
            executor = QueryExecutor(data_dir)
        
        importer = DataImporter(executor)
        result = importer.import_file_content(content, filename, options)
        
        return result
    else:
        # Handle JSON request with file path
        body = await request.json()
        file_path = body.get('file_path')
        options = body.get('options', {})
        
        if not file_path:
            return {"success": False, "error": "No file path provided"}
        
        # Create importer
        if session_manager.count() > 0:
            first_session = next(iter(session_manager.sessions.values()))
            executor = first_session["executor"]
        else:
            executor = QueryExecutor(data_dir)
        
        importer = DataImporter(executor)
        result = importer.import_file(file_path, options)
        
        return result


@app.post("/api/import/sql")
@handle_exceptions
async def import_sql(request: Request):
    """Import SQL content directly"""
    body = await request.json()
    sql_content = body.get('sql', '')
    options = body.get('options', {})
    
    if not sql_content:
        return {"success": False, "error": "No SQL content provided"}
    
    # Create importer
    if session_manager.count() > 0:
        first_session = next(iter(session_manager.sessions.values()))
        executor = first_session["executor"]
    else:
        executor = QueryExecutor(data_dir)
    
    importer = DataImporter(executor)
    
    # Save SQL to temp file and import
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(sql_content)
        temp_path = f.name
    
    try:
        result = importer.import_sql_file(temp_path, options)
        return result
    finally:
        os.unlink(temp_path)


@app.post("/api/import/csv")
@handle_exceptions
async def import_csv_data(request: Request):
    """Import CSV data directly"""
    body = await request.json()
    csv_content = body.get('csv', '')
    options = body.get('options', {})
    
    if not csv_content:
        return {"success": False, "error": "No CSV content provided"}
    
    # Create importer
    if session_manager.count() > 0:
        first_session = next(iter(session_manager.sessions.values()))
        executor = first_session["executor"]
    else:
        executor = QueryExecutor(data_dir)
    
    importer = DataImporter(executor)
    result = importer.import_file_content(
        csv_content.encode('utf-8'),
        options.get('filename', 'data.csv'),
        options
    )
    
    return result


# ========== Information Schema Endpoints ==========

@app.get("/api/information_schema/{view_name}")
@handle_exceptions
async def query_information_schema(view_name: str, database: str = None, 
                                   schema: str = None, table: str = None):
    """Query INFORMATION_SCHEMA views"""
    # Create executor and info schema builder
    if session_manager.count() > 0:
        first_session = next(iter(session_manager.sessions.values()))
        executor = first_session["executor"]
    else:
        executor = QueryExecutor(data_dir)
    
    info_schema = InformationSchemaBuilder(executor.metadata)
    result = info_schema.query_information_schema(view_name, database, schema, table)
    
    return result


@app.get("/api/information_schema")
@handle_exceptions
async def list_information_schema_views():
    """List available INFORMATION_SCHEMA views"""
    return {
        "views": InformationSchemaBuilder.SCHEMA_VIEWS,
        "count": len(InformationSchemaBuilder.SCHEMA_VIEWS)
    }


# ========== Stacked Filtering / Object Browser Endpoints ==========

@app.get("/api/browser/databases")
@handle_exceptions
async def browser_list_databases():
    """Get all databases for the object browser"""
    if session_manager.count() == 0:
        executor = QueryExecutor(data_dir)
        databases = executor.metadata.list_databases()
        executor.close()
    else:
        first_session = next(iter(session_manager.sessions.values()))
        databases = first_session["executor"].metadata.list_databases()
    
    return {
        "databases": databases,
        "count": len(databases)
    }


@app.get("/api/browser/databases/{database}/schemas")
@handle_exceptions
async def browser_list_schemas(database: str):
    """Get all schemas for a database (for stacked filtering)"""
    if session_manager.count() == 0:
        executor = QueryExecutor(data_dir)
        try:
            schemas = executor.metadata.list_schemas(database.upper())
        finally:
            executor.close()
    else:
        first_session = next(iter(session_manager.sessions.values()))
        schemas = first_session["executor"].metadata.list_schemas(database.upper())
    
    return {
        "database": database.upper(),
        "schemas": schemas,
        "count": len(schemas)
    }


@app.get("/api/browser/databases/{database}/schemas/{schema_name}/objects")
@handle_exceptions
async def browser_list_objects(database: str, schema_name: str, 
                               object_type: str = None):
    """Get all objects (tables, views) in a schema (for stacked filtering)"""
    if session_manager.count() == 0:
        executor = QueryExecutor(data_dir)
        metadata = executor.metadata
        close_executor = True
    else:
        first_session = next(iter(session_manager.sessions.values()))
        metadata = first_session["executor"].metadata
        close_executor = False
    
    try:
        db_upper = database.upper()
        schema_upper = schema_name.upper()
        
        result = {
            "database": db_upper,
            "schema": schema_upper,
            "objects": []
        }
        
        # Get tables
        if not object_type or object_type.upper() == 'TABLE':
            tables = metadata.list_tables(db_upper, schema_upper)
            for table in tables:
                result["objects"].append({
                    "name": table["name"],
                    "type": "TABLE",
                    "created_at": table["created_at"],
                    "row_count": table.get("row_count", 0),
                    "columns": table.get("columns", [])
                })
        
        # Get views
        if not object_type or object_type.upper() == 'VIEW':
            views = metadata.list_views(db_upper, schema_upper)
            for view in views:
                result["objects"].append({
                    "name": view["name"],
                    "type": "VIEW",
                    "created_at": view["created_at"],
                    "definition": view.get("definition", "")
                })
        
        result["count"] = len(result["objects"])
        return result
        
    finally:
        if close_executor:
            executor.close()


@app.get("/api/browser/databases/{database}/schemas/{schema_name}/tables/{table_name}")
@handle_exceptions
async def browser_get_table_details(database: str, schema_name: str, table_name: str):
    """Get detailed information about a specific table"""
    if session_manager.count() == 0:
        executor = QueryExecutor(data_dir)
        metadata = executor.metadata
        close_executor = True
    else:
        first_session = next(iter(session_manager.sessions.values()))
        metadata = first_session["executor"].metadata
        close_executor = False
    
    try:
        table_info = metadata.get_table_info(
            database.upper(), 
            schema_name.upper(), 
            table_name.upper()
        )
        
        if not table_info:
            raise HTTPException(status_code=404, detail="Table not found")
        
        return {
            "database": database.upper(),
            "schema": schema_name.upper(),
            "table": table_info
        }
        
    finally:
        if close_executor:
            executor.close()


@app.get("/api/browser/search")
@handle_exceptions
async def browser_search_objects(q: str, database: str = None, 
                                  schema: str = None, object_type: str = None):
    """Search for database objects by name (for quick filtering)"""
    if session_manager.count() == 0:
        executor = QueryExecutor(data_dir)
        metadata = executor.metadata
        close_executor = True
    else:
        first_session = next(iter(session_manager.sessions.values()))
        metadata = first_session["executor"].metadata
        close_executor = False
    
    try:
        query = q.upper()
        results = []
        
        databases = metadata.list_databases()
        if database:
            databases = [d for d in databases if d['name'] == database.upper()]
        
        for db in databases:
            # Search in database name
            if query in db['name']:
                results.append({
                    "name": db['name'],
                    "type": "DATABASE",
                    "full_name": db['name']
                })
            
            try:
                schemas = metadata.list_schemas(db['name'])
                if schema:
                    schemas = [s for s in schemas if s['name'] == schema.upper()]
                
                for sch in schemas:
                    # Search in schema name
                    if query in sch['name']:
                        results.append({
                            "name": sch['name'],
                            "type": "SCHEMA",
                            "database": db['name'],
                            "full_name": f"{db['name']}.{sch['name']}"
                        })
                    
                    # Search in tables
                    if not object_type or object_type.upper() == 'TABLE':
                        try:
                            tables = metadata.list_tables(db['name'], sch['name'])
                            for table in tables:
                                if query in table['name']:
                                    results.append({
                                        "name": table['name'],
                                        "type": "TABLE",
                                        "database": db['name'],
                                        "schema": sch['name'],
                                        "full_name": f"{db['name']}.{sch['name']}.{table['name']}"
                                    })
                        except ValueError:
                            pass
                    
                    # Search in views
                    if not object_type or object_type.upper() == 'VIEW':
                        try:
                            views = metadata.list_views(db['name'], sch['name'])
                            for view in views:
                                if query in view['name']:
                                    results.append({
                                        "name": view['name'],
                                        "type": "VIEW",
                                        "database": db['name'],
                                        "schema": sch['name'],
                                        "full_name": f"{db['name']}.{sch['name']}.{view['name']}"
                                    })
                        except ValueError:
                            pass
                            
            except ValueError:
                pass
        
        return {
            "results": results[:50],  # Limit results
            "count": len(results),
            "query": q
        }
        
    finally:
        if close_executor:
            executor.close()


# ========== Server Logs Endpoints ==========

# In-memory log buffer
_log_buffer: List[Dict[str, Any]] = []
_max_log_entries = 500

class LogCapture(logging.Handler):
    """Custom handler to capture logs for the frontend"""
    def emit(self, record):
        try:
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno
            }
            _log_buffer.append(log_entry)
            # Keep buffer size limited
            if len(_log_buffer) > _max_log_entries:
                _log_buffer.pop(0)
        except Exception:
            pass

# Add custom handler to capture logs
log_capture_handler = LogCapture()
log_capture_handler.setFormatter(logging.Formatter('%(message)s'))
logging.getLogger().addHandler(log_capture_handler)


@app.get("/api/logs")
@handle_exceptions
async def get_logs(limit: int = 100, level: Optional[str] = None):
    """Get server logs"""
    logs = _log_buffer.copy()
    
    # Filter by level if specified
    if level:
        level_upper = level.upper()
        logs = [log for log in logs if log["level"] == level_upper]
    
    # Return most recent logs first, limited
    logs = logs[-limit:]
    logs.reverse()
    
    return {
        "logs": logs,
        "count": len(logs),
        "total": len(_log_buffer)
    }


@app.delete("/api/logs")
@handle_exceptions
async def clear_logs():
    """Clear the log buffer"""
    _log_buffer.clear()
    logger.info("Log buffer cleared")
    return {"success": True, "message": "Logs cleared"}


# Static files directory for Vue frontend
STATIC_DIR = Path(__file__).parent / "static"

@app.get("/dashboard", response_class=HTMLResponse)
@handle_exceptions
async def dashboard():
    """Serve Vue frontend dashboard"""
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    # Fallback to old template if frontend not built
    return load_template("dashboard.html")

@app.get("/dashboard/{path:path}")
async def dashboard_static(path: str):
    """Serve static assets for Vue frontend"""
    file_path = STATIC_DIR / path
    if file_path.exists() and file_path.is_file():
        return FileResponse(file_path)
    # For SPA routing, return index.html
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    raise HTTPException(status_code=404, detail="Not found")


# Legacy function for backward compatibility
def get_dashboard_html():
    """Return the dashboard HTML (deprecated - use load_template instead)"""
    return load_template("dashboard.html")


# ========== dbt Support Endpoints ==========

# Global dbt adapter instance (created per-session or on-demand)
_dbt_adapters: Dict[str, DbtAdapter] = {}


def _get_dbt_adapter(session_id: str = None) -> DbtAdapter:
    """Get or create a dbt adapter for the given session"""
    if session_id and session_id in _dbt_adapters:
        return _dbt_adapters[session_id]
    
    # Get executor
    if session_manager.count() > 0:
        first_session = next(iter(session_manager.sessions.values()))
        executor = first_session["executor"]
        session_id = session_id or first_session["session_id"]
    else:
        executor = QueryExecutor(data_dir)
        session_id = session_id or "dbt-default"
    
    adapter = DbtAdapter(executor)
    _dbt_adapters[session_id] = adapter
    return adapter


class DbtCompileRequest(BaseModel):
    sql: str
    vars: Optional[Dict[str, Any]] = None


class DbtRunRequest(BaseModel):
    select: Optional[str] = None
    exclude: Optional[str] = None
    full_refresh: bool = False


class DbtSeedRequest(BaseModel):
    select: Optional[str] = None
    full_refresh: bool = False


class DbtTestRequest(BaseModel):
    select: Optional[str] = None


class DbtSnapshotRequest(BaseModel):
    select: Optional[str] = None


class DbtSourceRequest(BaseModel):
    name: str
    database: str
    schema_name: str  # 'schema' is reserved
    tables: List[Dict[str, Any]]
    description: str = ""


class DbtModelRequest(BaseModel):
    name: str
    sql: str
    materialization: str = "view"
    schema_name: Optional[str] = None
    alias: Optional[str] = None
    tags: List[str] = []
    description: str = ""


@app.get("/api/dbt/status")
@handle_exceptions
async def dbt_status():
    """Get dbt adapter status and capabilities"""
    return {
        "enabled": True,
        "version": "1.0.0",
        "adapter_type": "snowglobe",
        "features": {
            "models": True,
            "seeds": True,
            "tests": True,
            "snapshots": True,
            "sources": True,
            "documentation": True,
            "lineage": True,
            "compilation": True,
            "incremental": True,
        },
        "supported_materializations": [m.value for m in MaterializationType],
    }


@app.post("/api/dbt/compile")
@handle_exceptions
async def dbt_compile(request: DbtCompileRequest):
    """Compile dbt-style SQL with refs and sources"""
    adapter = _get_dbt_adapter()
    
    # Set vars if provided
    if request.vars:
        adapter.project_config.vars.update(request.vars)
    
    try:
        compiled_sql = adapter.compile_sql(request.sql)
        return {
            "success": True,
            "compiled_sql": compiled_sql,
            "original_sql": request.sql,
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "original_sql": request.sql,
        }


@app.post("/api/dbt/run")
@handle_exceptions
async def dbt_run(request: DbtRunRequest):
    """Run dbt models"""
    adapter = _get_dbt_adapter()
    
    try:
        results = adapter.run(
            select=request.select,
            exclude=request.exclude,
            full_refresh=request.full_refresh,
        )
        
        return {
            "success": True,
            "results": adapter.get_run_results(),
            "summary": {
                "total": len(results),
                "success": sum(1 for r in results if r.status == "success"),
                "error": sum(1 for r in results if r.status == "error"),
                "skipped": sum(1 for r in results if r.status == "skipped"),
            }
        }
    except Exception as e:
        logger.error(f"dbt run error: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
        }


@app.post("/api/dbt/seed")
@handle_exceptions
async def dbt_seed(request: DbtSeedRequest):
    """Load dbt seed data"""
    adapter = _get_dbt_adapter()
    
    try:
        results = adapter.seed(
            select=request.select,
            full_refresh=request.full_refresh,
        )
        
        return {
            "success": True,
            "results": [
                {
                    "status": r.status,
                    "message": r.message,
                    "node": r.node,
                    "execution_time": r.execution_time,
                }
                for r in results
            ],
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


@app.post("/api/dbt/test")
@handle_exceptions
async def dbt_test(request: DbtTestRequest):
    """Run dbt tests"""
    adapter = _get_dbt_adapter()
    
    try:
        results = adapter.test(select=request.select)
        
        return {
            "success": True,
            "results": [
                {
                    "status": r.status,
                    "message": r.message,
                    "node": r.node,
                    "failures": r.failures,
                    "execution_time": r.execution_time,
                }
                for r in results
            ],
            "summary": {
                "total": len(results),
                "pass": sum(1 for r in results if r.status == "success"),
                "fail": sum(1 for r in results if r.status == "error"),
                "warn": sum(1 for r in results if r.status == "warn"),
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


@app.post("/api/dbt/snapshot")
@handle_exceptions
async def dbt_snapshot(request: DbtSnapshotRequest):
    """Run dbt snapshots"""
    adapter = _get_dbt_adapter()
    
    try:
        results = adapter.snapshot(select=request.select)
        
        return {
            "success": True,
            "results": [
                {
                    "status": r.status,
                    "message": r.message,
                    "node": r.node,
                    "execution_time": r.execution_time,
                }
                for r in results
            ],
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


@app.get("/api/dbt/sources")
@handle_exceptions
async def dbt_list_sources():
    """List all dbt sources"""
    adapter = _get_dbt_adapter()
    
    return {
        "sources": [
            {
                "name": source.name,
                "database": source.database,
                "schema": source.schema,
                "tables": source.tables,
                "description": source.description,
            }
            for source in adapter.sources.values()
        ],
        "count": len(adapter.sources),
    }


@app.post("/api/dbt/sources")
@handle_exceptions
async def dbt_register_source(request: DbtSourceRequest):
    """Register a new dbt source"""
    adapter = _get_dbt_adapter()
    
    source = DbtSource(
        name=request.name,
        database=request.database.upper(),
        schema=request.schema_name.upper(),
        tables=request.tables,
        description=request.description,
    )
    
    adapter.sources[source.name] = source
    adapter.compiler.register_source(source)
    
    return {
        "success": True,
        "source": {
            "name": source.name,
            "database": source.database,
            "schema": source.schema,
            "tables": source.tables,
        }
    }


@app.get("/api/dbt/models")
@handle_exceptions
async def dbt_list_models():
    """List all dbt models"""
    adapter = _get_dbt_adapter()
    
    return {
        "models": [
            {
                "name": model.name,
                "database": model.database,
                "schema": model.schema,
                "alias": model.alias,
                "materialization": model.materialization.value,
                "status": model.status,
                "description": model.description,
                "tags": model.tags,
                "depends_on": model.depends_on,
                "last_run_at": model.last_run_at,
                "execution_time_ms": model.execution_time_ms,
            }
            for model in adapter.models.values()
        ],
        "count": len(adapter.models),
    }


@app.post("/api/dbt/models")
@handle_exceptions
async def dbt_register_model(request: DbtModelRequest):
    """Register a new dbt model"""
    adapter = _get_dbt_adapter()
    
    try:
        mat_type = MaterializationType(request.materialization)
    except ValueError:
        mat_type = MaterializationType.VIEW
    
    model = DbtModel(
        name=request.name,
        database=adapter.current_database,
        schema=(request.schema_name or adapter.current_schema).upper(),
        alias=request.alias,
        materialization=mat_type,
        sql=request.sql,
        unique_id=f"model.snowglobe.{request.name}",
        tags=request.tags,
        description=request.description,
        created_at=datetime.utcnow().isoformat(),
    )
    
    # Compile the SQL
    model.compiled_sql = adapter.compiler.compile(request.sql, model, {
        'vars': adapter.project_config.vars,
        'target': adapter.target,
    })
    
    # Extract dependencies
    refs = re.findall(r'\{\{\s*ref\s*\([\'"](\w+)[\'"]\)\s*\}\}', request.sql)
    model.depends_on = refs
    
    adapter.models[model.name] = model
    adapter.compiler.register_model(model)
    
    return {
        "success": True,
        "model": {
            "name": model.name,
            "unique_id": model.unique_id,
            "compiled_sql": model.compiled_sql,
            "depends_on": model.depends_on,
        }
    }


@app.get("/api/dbt/models/{model_name}")
@handle_exceptions
async def dbt_get_model(model_name: str):
    """Get details for a specific model"""
    adapter = _get_dbt_adapter()
    
    if model_name not in adapter.models:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")
    
    model = adapter.models[model_name]
    
    return {
        "model": {
            "name": model.name,
            "database": model.database,
            "schema": model.schema,
            "alias": model.alias,
            "materialization": model.materialization.value,
            "sql": model.sql,
            "compiled_sql": model.compiled_sql,
            "status": model.status,
            "description": model.description,
            "columns": model.columns,
            "tags": model.tags,
            "meta": model.meta,
            "depends_on": model.depends_on,
            "created_at": model.created_at,
            "last_run_at": model.last_run_at,
            "execution_time_ms": model.execution_time_ms,
            "rows_affected": model.rows_affected,
            "error": model.error,
        }
    }


@app.post("/api/dbt/models/{model_name}/run")
@handle_exceptions
async def dbt_run_model(model_name: str, full_refresh: bool = False):
    """Run a specific model"""
    adapter = _get_dbt_adapter()
    
    if model_name not in adapter.models:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")
    
    model = adapter.models[model_name]
    result = adapter._run_model(model, full_refresh)
    
    return {
        "success": result.status == "success",
        "result": {
            "status": result.status,
            "message": result.message,
            "execution_time": result.execution_time,
            "node": result.node,
        }
    }


@app.get("/api/dbt/models/{model_name}/lineage")
@handle_exceptions
async def dbt_model_lineage(model_name: str):
    """Get lineage for a model"""
    adapter = _get_dbt_adapter()
    
    lineage = adapter.get_model_lineage(model_name)
    
    if 'error' in lineage:
        raise HTTPException(status_code=404, detail=lineage['error'])
    
    return lineage


@app.get("/api/dbt/tests")
@handle_exceptions
async def dbt_list_tests():
    """List all dbt tests"""
    adapter = _get_dbt_adapter()
    
    return {
        "tests": [
            {
                "name": test.name,
                "unique_id": test.unique_id,
                "model": test.model,
                "column": test.column,
                "test_type": test.test_type,
                "severity": test.severity,
                "status": test.status,
            }
            for test in adapter.tests.values()
        ],
        "count": len(adapter.tests),
    }


@app.get("/api/dbt/seeds")
@handle_exceptions
async def dbt_list_seeds():
    """List all dbt seeds"""
    adapter = _get_dbt_adapter()
    
    return {
        "seeds": [
            {
                "name": seed.name,
                "database": seed.database,
                "schema": seed.schema,
                "file_path": seed.file_path,
                "rows_loaded": seed.rows_loaded,
                "loaded_at": seed.loaded_at,
            }
            for seed in adapter.seeds.values()
        ],
        "count": len(adapter.seeds),
    }


@app.get("/api/dbt/snapshots")
@handle_exceptions
async def dbt_list_snapshots():
    """List all dbt snapshots"""
    adapter = _get_dbt_adapter()
    
    return {
        "snapshots": [
            {
                "name": snapshot.name,
                "database": snapshot.database,
                "schema": snapshot.schema,
                "strategy": snapshot.strategy,
                "unique_key": snapshot.unique_key,
            }
            for snapshot in adapter.snapshots.values()
        ],
        "count": len(adapter.snapshots),
    }


@app.get("/api/dbt/source-freshness")
@handle_exceptions
async def dbt_source_freshness(select: Optional[str] = None):
    """Check source freshness"""
    adapter = _get_dbt_adapter()
    
    return adapter.source_freshness(select)


@app.get("/api/dbt/docs")
@handle_exceptions
async def dbt_generate_docs():
    """Generate dbt documentation"""
    adapter = _get_dbt_adapter()
    
    return adapter.generate_docs()


@app.get("/api/dbt/profiles")
@handle_exceptions
async def dbt_profiles():
    """Get profiles.yml configuration for connecting with dbt-snowflake"""
    return {
        "profiles_yml": generate_profiles_yml(
            host="localhost",
            port=int(os.getenv("SNOWGLOBE_HTTPS_PORT", "8443")),
        ),
        "instructions": [
            "Save the profiles_yml content to ~/.dbt/profiles.yml",
            "Or set DBT_PROFILES_DIR environment variable to point to your profiles directory",
            "Use 'insecure_mode: true' for self-signed certificates",
            "Any username/password will work with Snowglobe",
        ]
    }


@app.post("/api/dbt/project/load")
@handle_exceptions
async def dbt_load_project(request: Request):
    """Load a dbt project from directory"""
    body = await request.json()
    project_dir = body.get("project_dir", "")
    
    if not project_dir or not os.path.exists(project_dir):
        return {
            "success": False,
            "error": f"Project directory not found: {project_dir}",
        }
    
    adapter = _get_dbt_adapter()
    
    try:
        adapter.load_project(project_dir)
        
        return {
            "success": True,
            "project": {
                "name": adapter.project_config.name,
                "version": adapter.project_config.version,
                "models_count": len(adapter.models),
                "sources_count": len(adapter.sources),
                "seeds_count": len(adapter.seeds),
                "tests_count": len(adapter.tests),
                "snapshots_count": len(adapter.snapshots),
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


@app.post("/api/dbt/project/generate")
@handle_exceptions
async def dbt_generate_project(request: Request):
    """Generate a sample dbt project"""
    body = await request.json()
    project_dir = body.get("project_dir", "/tmp/dbt_sample_project")
    
    try:
        generate_sample_project(project_dir)
        
        return {
            "success": True,
            "project_dir": project_dir,
            "message": "Sample dbt project created successfully",
            "files_created": [
                "dbt_project.yml",
                "profiles.yml",
                "models/staging/stg_customers.sql",
                "models/staging/schema.yml",
                "models/marts/dim_customers.sql",
                "seeds/sample_data.csv",
            ]
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


@app.get("/api/dbt/vars")
@handle_exceptions
async def dbt_get_vars():
    """Get dbt project variables"""
    adapter = _get_dbt_adapter()
    
    return {
        "vars": adapter.project_config.vars,
    }


@app.post("/api/dbt/vars")
@handle_exceptions
async def dbt_set_vars(request: Request):
    """Set dbt project variables"""
    body = await request.json()
    vars_dict = body.get("vars", {})
    
    adapter = _get_dbt_adapter()
    adapter.project_config.vars.update(vars_dict)
    
    return {
        "success": True,
        "vars": adapter.project_config.vars,
    }


@app.get("/api/dbt/run-results")
@handle_exceptions
async def dbt_run_results():
    """Get results from the last dbt run"""
    adapter = _get_dbt_adapter()
    
    return {
        "results": adapter.get_run_results(),
        "count": len(adapter.run_results),
    }


@app.delete("/api/dbt/cache")
@handle_exceptions
async def dbt_clear_cache():
    """Clear dbt adapter cache"""
    global _dbt_adapters
    _dbt_adapters.clear()
    
    return {
        "success": True,
        "message": "dbt adapter cache cleared",
    }


# Old embedded HTML removed - now loaded from templates/dashboard.html
_LEGACY_DASHBOARD_HTML = '''<!DOCTYPE html>
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
    import ssl
    
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
        
        # Verify certificates are readable
        try:
            with open(cert_path, 'r') as f:
                cert_content = f.read()
                if not cert_content or 'BEGIN CERTIFICATE' not in cert_content:
                    raise ValueError("Invalid certificate file")
            with open(key_path, 'r') as f:
                key_content = f.read()
                if not key_content or 'BEGIN PRIVATE KEY' not in key_content:
                    raise ValueError("Invalid key file")
            logger.info("SSL certificates validated successfully")
        except Exception as e:
            logger.error(f"SSL certificate validation failed: {e}")
            logger.info(f"Falling back to HTTP only on {host}:{port}")
            uvicorn.run(
                "snowglobe_server.server:app",
                host=host,
                port=port,
                log_level="info",
                reload=False
            )
            return
        
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
        
        # Create SSL context with proper settings
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(cert_path, key_path)
        
        # Configure SSL context for better compatibility
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Enable TLS 1.2 and 1.3
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        ssl_context.maximum_version = ssl.TLSVersion.TLSv1_3
        
        # Set ciphers for better compatibility
        ssl_context.set_ciphers('ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS')
        
        logger.info("SSL context configured with TLS 1.2+ and secure ciphers")
        
        # Run HTTPS server in main thread with custom SSL context
        uvicorn.run(
            "snowglobe_server.server:app",
            host=host,
            port=https_port,
            log_level="info",
            reload=False,
            ssl_keyfile=key_path,
            ssl_certfile=cert_path,
            ssl_keyfile_password=None,
            ssl_version=ssl.PROTOCOL_TLS_SERVER,
            ssl_cert_reqs=ssl.CERT_NONE,
            ssl_ca_certs=None,
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
