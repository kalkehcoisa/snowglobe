"""
Decorators and helper functions for common server functionalities
"""

import functools
import logging
import time
from typing import Callable, Any, Dict, Optional
from datetime import datetime

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse

logger = logging.getLogger("snowglobe")


# ============================================================================
# Authentication Decorators
# ============================================================================

def requires_session(sessions_dict: Dict):
    """
    Decorator that validates session token from Authorization header
    
    Usage:
        @app.get("/api/endpoint")
        @requires_session(sessions)
        async def my_endpoint(request: Request, session: Dict):
            # session is automatically populated
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            auth_header = request.headers.get("Authorization", "")
            session = get_session_from_token(auth_header, sessions_dict)
            
            if not session:
                return create_error_response(
                    code="390104",
                    message="Invalid session token",
                    status_code=401
                )
            
            # Pass session to the wrapped function
            kwargs['session'] = session
            return await func(request, *args, **kwargs)
        
        return wrapper
    return decorator


# ============================================================================
# Error Handling Decorators
# ============================================================================

def handle_exceptions(func: Callable) -> Callable:
    """
    Decorator that catches exceptions and returns proper error responses
    
    Usage:
        @app.get("/api/endpoint")
        @handle_exceptions
        async def my_endpoint(request: Request):
            # Any exception will be caught and formatted
            pass
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException:
            # Re-raise FastAPI HTTPExceptions
            raise
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            return create_error_response(
                code="500000",
                message=f"Internal server error: {str(e)}",
                status_code=500
            )
    
    return wrapper


def log_execution_time(func: Callable) -> Callable:
    """
    Decorator that logs execution time of a function
    
    Usage:
        @log_execution_time
        async def my_slow_function():
            pass
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        duration = (time.time() - start_time) * 1000
        logger.debug(f"{func.__name__} executed in {duration:.2f}ms")
        return result
    
    return wrapper


# ============================================================================
# Validation Decorators
# ============================================================================

def validate_json_body(required_fields: Optional[list] = None):
    """
    Decorator that validates JSON body and required fields
    
    Usage:
        @app.post("/api/endpoint")
        @validate_json_body(required_fields=["name", "value"])
        async def my_endpoint(request: Request, body: dict):
            # body is automatically parsed and validated
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            try:
                body = await request.json()
            except Exception as e:
                return create_error_response(
                    code="400000",
                    message=f"Invalid JSON body: {str(e)}",
                    status_code=400
                )
            
            # Validate required fields
            if required_fields:
                missing_fields = [f for f in required_fields if f not in body]
                if missing_fields:
                    return create_error_response(
                        code="400001",
                        message=f"Missing required fields: {', '.join(missing_fields)}",
                        status_code=400
                    )
            
            kwargs['body'] = body
            return await func(request, *args, **kwargs)
        
        return wrapper
    return decorator


# ============================================================================
# Helper Functions
# ============================================================================

def create_success_response(data: Any = None, message: str = None) -> dict:
    """
    Create a standardized success response
    
    Args:
        data: Response data
        message: Optional success message
        
    Returns:
        Snowflake-compatible success response dictionary
    """
    return {
        "data": data,
        "code": None,
        "message": message,
        "success": True
    }


def create_error_response(
    code: str,
    message: str,
    data: Any = None,
    status_code: int = 400
) -> JSONResponse:
    """
    Create a standardized error response
    
    Args:
        code: Error code
        message: Error message
        data: Optional error data
        status_code: HTTP status code
        
    Returns:
        JSONResponse with error details
    """
    return JSONResponse(
        status_code=status_code,
        content={
            "data": data,
            "code": code,
            "message": message,
            "success": False
        }
    )


def get_session_from_token(auth_header: str, sessions: Dict) -> Optional[Dict[str, Any]]:
    """
    Extract session from Authorization header
    
    Args:
        auth_header: Authorization header value
        sessions: Dictionary of active sessions
        
    Returns:
        Session dictionary if valid, None otherwise
    """
    if not auth_header:
        return None
    
    # Format: Snowflake Token="<token>"
    if auth_header.startswith('Snowflake Token="') and auth_header.endswith('"'):
        token = auth_header[17:-1]
        return sessions.get(token)
    
    return None


def format_timestamp(dt: datetime) -> str:
    """
    Format datetime to ISO 8601 string
    
    Args:
        dt: datetime object
        
    Returns:
        ISO 8601 formatted string
    """
    return dt.isoformat() if dt else None


def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert value to int
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Integer value
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Safely convert value to float
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Float value
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def truncate_string(s: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate string to maximum length
    
    Args:
        s: String to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated string
    """
    if not s or len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix


def calculate_duration_ms(start_time: datetime, end_time: datetime = None) -> float:
    """
    Calculate duration in milliseconds between two timestamps
    
    Args:
        start_time: Start datetime
        end_time: End datetime (defaults to now)
        
    Returns:
        Duration in milliseconds
    """
    if end_time is None:
        end_time = datetime.utcnow()
    return (end_time - start_time).total_seconds() * 1000


def format_uptime(seconds: float) -> str:
    """
    Format uptime in human-readable format
    
    Args:
        seconds: Uptime in seconds
        
    Returns:
        Formatted uptime string (e.g., "2h 34m 12s")
    """
    if not seconds or seconds < 0:
        return "0s"
    
    days = int(seconds // 86400)
    hours = int((seconds % 86400) // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return " ".join(parts)


def sanitize_sql(sql: str, max_length: int = 1000) -> str:
    """
    Sanitize SQL for logging purposes
    
    Args:
        sql: SQL string
        max_length: Maximum length to log
        
    Returns:
        Sanitized SQL string
    """
    if not sql:
        return ""
    
    # Remove excessive whitespace
    sql = " ".join(sql.split())
    
    # Truncate if too long
    return truncate_string(sql, max_length)


def get_statement_type_id(sql: str) -> int:
    """
    Determine Snowflake statement type ID from SQL
    
    Args:
        sql: SQL statement
        
    Returns:
        Statement type ID
    """
    sql_upper = sql.strip().upper()
    
    if sql_upper.startswith(("SELECT", "SHOW", "DESCRIBE", "DESC")):
        return 4096  # SELECT
    elif sql_upper.startswith("INSERT"):
        return 4608  # INSERT
    elif sql_upper.startswith("UPDATE"):
        return 4864  # UPDATE
    elif sql_upper.startswith("DELETE"):
        return 5120  # DELETE
    elif sql_upper.startswith(("CREATE", "DROP", "ALTER", "TRUNCATE")):
        return 8192  # DDL
    elif sql_upper.startswith("USE"):
        return 16384  # USE
    elif sql_upper.startswith(("BEGIN", "COMMIT", "ROLLBACK")):
        return 32768  # TRANSACTION
    else:
        return 0  # Unknown


# ============================================================================
# Query History Management
# ============================================================================

class QueryHistoryManager:
    """
    Manager for query history with size limits
    """
    
    def __init__(self, max_size: int = 1000):
        self.history = []
        self.max_size = max_size
    
    def add(
        self,
        query: str,
        session_id: str,
        success: bool,
        duration_ms: float,
        rows_affected: int,
        error: Optional[str] = None
    ):
        """Add query to history"""
        import uuid
        
        entry = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "session_id": session_id,
            "query": sanitize_sql(query, 500),
            "success": success,
            "duration_ms": round(duration_ms, 2),
            "rows_affected": rows_affected,
            "error": error
        }
        
        self.history.append(entry)
        
        # Keep only last N queries
        if len(self.history) > self.max_size:
            self.history = self.history[-self.max_size:]
    
    def get_recent(self, limit: int = 100, offset: int = 0) -> list:
        """Get recent queries"""
        # Return in reverse chronological order
        sorted_history = list(reversed(self.history))
        return sorted_history[offset:offset + limit]
    
    def clear(self):
        """Clear all history"""
        self.history.clear()
    
    def get_stats(self) -> dict:
        """Get query statistics"""
        total = len(self.history)
        successful = sum(1 for q in self.history if q["success"])
        failed = total - successful
        
        avg_duration = 0
        if total > 0:
            avg_duration = sum(q["duration_ms"] for q in self.history) / total
        
        return {
            "total_queries": total,
            "successful_queries": successful,
            "failed_queries": failed,
            "average_query_duration_ms": round(avg_duration, 2)
        }


# ============================================================================
# Session Management
# ============================================================================

class SessionManager:
    """
    Manager for active sessions
    """
    
    def __init__(self):
        self.sessions = {}
    
    def add(self, token: str, session_data: dict):
        """Add a new session"""
        self.sessions[token] = session_data
    
    def get(self, token: str) -> Optional[dict]:
        """Get session by token"""
        return self.sessions.get(token)
    
    def remove(self, token: str):
        """Remove session and clean up resources"""
        if token in self.sessions:
            session = self.sessions[token]
            try:
                if "executor" in session:
                    session["executor"].close()
            except Exception as e:
                logger.error(f"Error closing session executor: {e}")
            del self.sessions[token]
    
    def get_by_session_id(self, session_id: str) -> Optional[tuple]:
        """Get session by session_id, returns (token, session)"""
        for token, session in self.sessions.items():
            if session.get("session_id") == session_id:
                return token, session
        return None
    
    def list_all(self) -> list:
        """List all active sessions"""
        session_list = []
        for token, session in self.sessions.items():
            session_list.append({
                "session_id": session["session_id"],
                "user": session["user"],
                "database": session.get("database"),
                "schema": session.get("schema"),
                "warehouse": session.get("warehouse"),
                "role": session.get("role"),
                "created_at": format_timestamp(session["created_at"]),
                "token_prefix": token[:8] + "..."
            })
        return session_list
    
    def cleanup_all(self):
        """Clean up all sessions"""
        for token in list(self.sessions.keys()):
            self.remove(token)
    
    def count(self) -> int:
        """Get count of active sessions"""
        return len(self.sessions)
