"""
Tests for Snowflake-compatible authentication system.
"""

import pytest
import json
import gzip
import uuid
import os
from fastapi.testclient import TestClient

# Set data directory before importing server
os.environ["SNOWGLOBE_DATA_DIR"] = "/tmp/snowglobe_test_data"

from snowglobe_server.server import app, sessions, query_history


@pytest.fixture
def client():
    """Create test client"""
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up sessions and history before each test"""
    sessions.clear()
    query_history.clear()
    yield
    sessions.clear()
    query_history.clear()


class TestLoginEndpoint:
    """Tests for /session/v1/login-request endpoint"""
    
    def test_basic_login_success(self, client):
        """Test basic login with minimal parameters"""
        request_body = {
            "data": {
                "LOGIN_NAME": "test_user",
                "PASSWORD": "test_password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response = client.post(
            "/session/v1/login-request",
            json=request_body
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        assert data["code"] is None
        assert data["message"] is None
        assert "data" in data
        
        response_data = data["data"]
        assert "token" in response_data
        assert "masterToken" in response_data
        assert "sessionId" in response_data
        assert response_data["displayUserName"] == "TEST_USER"
        assert response_data["validityInSeconds"] == 3600
        assert response_data["masterValidityInSeconds"] == 14400
    
    def test_login_with_database_and_schema(self, client):
        """Test login with database and schema parameters"""
        request_body = {
            "data": {
                "LOGIN_NAME": "test_user",
                "PASSWORD": "test_password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response = client.post(
            "/session/v1/login-request?databaseName=MY_DB&schemaName=MY_SCHEMA",
            json=request_body
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        session_info = data["data"]["sessionInfo"]
        assert session_info["databaseName"] == "MY_DB"
        assert session_info["schemaName"] == "MY_SCHEMA"
    
    def test_login_with_warehouse_and_role(self, client):
        """Test login with warehouse and role parameters"""
        request_body = {
            "data": {
                "LOGIN_NAME": "test_user",
                "PASSWORD": "test_password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response = client.post(
            "/session/v1/login-request?warehouse=MY_WH&roleName=MY_ROLE",
            json=request_body
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        session_info = data["data"]["sessionInfo"]
        assert session_info["warehouseName"] == "MY_WH"
        assert session_info["roleName"] == "MY_ROLE"
    
    def test_login_creates_session(self, client):
        """Test that login creates a session in memory"""
        initial_session_count = len(sessions)
        
        request_body = {
            "data": {
                "LOGIN_NAME": "session_test_user",
                "PASSWORD": "password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response = client.post(
            "/session/v1/login-request",
            json=request_body
        )
        
        assert response.status_code == 200
        assert len(sessions) == initial_session_count + 1
        
        # Verify session was stored with correct token
        token = response.json()["data"]["token"]
        assert token in sessions
        assert sessions[token]["user"] == "session_test_user"
    
    def test_login_with_gzip_body(self, client):
        """Test login with gzip-compressed request body"""
        request_body = {
            "data": {
                "LOGIN_NAME": "gzip_user",
                "PASSWORD": "gzip_password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        # Compress the body
        json_body = json.dumps(request_body).encode('utf-8')
        compressed_body = gzip.compress(json_body)
        
        response = client.post(
            "/session/v1/login-request",
            data=compressed_body,
            headers={"Content-Encoding": "gzip", "Content-Type": "application/json"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["displayUserName"] == "GZIP_USER"
    
    def test_login_response_contains_parameters(self, client):
        """Test that login response contains session parameters"""
        request_body = {
            "data": {
                "LOGIN_NAME": "test_user",
                "PASSWORD": "test_password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response = client.post(
            "/session/v1/login-request",
            json=request_body
        )
        
        assert response.status_code == 200
        parameters = response.json()["data"]["parameters"]
        
        # Check for essential parameters
        param_names = [p["name"] for p in parameters]
        assert "TIMESTAMP_OUTPUT_FORMAT" in param_names
        assert "CLIENT_PREFETCH_THREADS" in param_names
        assert "QUERY_RESULT_FORMAT" in param_names
        assert "TIMEZONE" in param_names
    
    def test_multiple_logins_create_separate_sessions(self, client):
        """Test that multiple logins create separate sessions"""
        request_body1 = {
            "data": {
                "LOGIN_NAME": "user1",
                "PASSWORD": "password1",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        request_body2 = {
            "data": {
                "LOGIN_NAME": "user2",
                "PASSWORD": "password2",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response1 = client.post("/session/v1/login-request", json=request_body1)
        response2 = client.post("/session/v1/login-request", json=request_body2)
        
        token1 = response1.json()["data"]["token"]
        token2 = response2.json()["data"]["token"]
        
        assert token1 != token2
        assert len(sessions) == 2
        assert sessions[token1]["user"] == "user1"
        assert sessions[token2]["user"] == "user2"
    
    def test_login_with_client_info(self, client):
        """Test login with client environment info (like real connector)"""
        request_body = {
            "data": {
                "CLIENT_APP_ID": "PythonConnector",
                "CLIENT_APP_VERSION": "2.7.6",
                "ACCOUNT_NAME": "snowglobe",
                "LOGIN_NAME": "demo_user",
                "PASSWORD": "demo_password",
                "CLIENT_ENVIRONMENT": {
                    "APPLICATION": "PythonConnector",
                    "OS": "Linux",
                    "PYTHON_VERSION": "3.9.7"
                },
                "SESSION_PARAMETERS": {
                    "CLIENT_PREFETCH_THREADS": 4
                }
            }
        }
        
        response = client.post(
            "/session/v1/login-request?request_id=" + str(uuid.uuid4()),
            json=request_body
        )
        
        assert response.status_code == 200
        assert response.json()["success"] is True


class TestSessionManagement:
    """Tests for session management"""
    
    def test_delete_session(self, client):
        """Test closing/deleting a session"""
        # First create a session
        request_body = {
            "data": {
                "LOGIN_NAME": "test_user",
                "PASSWORD": "test_password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        login_response = client.post(
            "/session/v1/login-request",
            json=request_body
        )
        token = login_response.json()["data"]["token"]
        
        assert token in sessions
        
        # Now delete the session
        response = client.post(
            "/session",
            headers={"Authorization": f'Snowflake Token="{token}"'}
        )
        
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert token not in sessions
    
    def test_delete_nonexistent_session(self, client):
        """Test deleting a session that doesn't exist"""
        response = client.post(
            "/session",
            headers={"Authorization": 'Snowflake Token="nonexistent_token"'}
        )
        
        assert response.status_code == 200
        assert response.json()["success"] is True


class TestQueryExecution:
    """Tests for query execution endpoint"""
    
    def create_session(self, client):
        """Helper to create a session and return token"""
        request_body = {
            "data": {
                "LOGIN_NAME": "test_user",
                "PASSWORD": "test_password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response = client.post(
            "/session/v1/login-request?databaseName=TEST_DB&schemaName=PUBLIC",
            json=request_body
        )
        return response.json()["data"]["token"]
    
    def test_query_without_auth(self, client):
        """Test that query fails without authentication"""
        query_body = {
            "sqlText": "SELECT 1",
            "sequenceId": 1
        }
        
        response = client.post(
            "/queries/v1/query-request",
            json=query_body
        )
        
        assert response.status_code == 200  # Snowflake returns 200 with error in body
        data = response.json()
        assert data["success"] is False
        assert "390104" in data["code"]
    
    def test_simple_select_query(self, client):
        """Test executing a simple SELECT query"""
        token = self.create_session(client)
        
        query_body = {
            "sqlText": "SELECT 1 as num, 'hello' as msg",
            "sequenceId": 1
        }
        
        response = client.post(
            "/queries/v1/query-request",
            json=query_body,
            headers={"Authorization": f'Snowflake Token="{token}"'}
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        assert "rowtype" in data["data"]
        assert "rowset" in data["data"]
        assert len(data["data"]["rowset"]) == 1
    
    def test_create_table_query(self, client):
        """Test CREATE TABLE query"""
        token = self.create_session(client)
        
        # Use unique table name
        table_name = f"test_table_{uuid.uuid4().hex[:8]}"
        
        query_body = {
            "sqlText": f"CREATE TABLE {table_name} (id INT, name VARCHAR)",
            "sequenceId": 1
        }
        
        response = client.post(
            "/queries/v1/query-request",
            json=query_body,
            headers={"Authorization": f'Snowflake Token="{token}"'}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
    
    def test_insert_and_select(self, client):
        """Test INSERT and SELECT queries"""
        token = self.create_session(client)
        auth_header = {"Authorization": f'Snowflake Token="{token}"'}
        
        # Use unique table name
        table_name = f"users_{uuid.uuid4().hex[:8]}"
        
        # Create table
        create_response = client.post(
            "/queries/v1/query-request",
            json={"sqlText": f"CREATE TABLE {table_name} (id INT, name VARCHAR)", "sequenceId": 1},
            headers=auth_header
        )
        assert create_response.json()["success"] is True
        
        # Insert data
        insert_response = client.post(
            "/queries/v1/query-request",
            json={"sqlText": f"INSERT INTO {table_name} VALUES (1, 'Alice')", "sequenceId": 2},
            headers=auth_header
        )
        assert insert_response.json()["success"] is True
        
        # Select data
        select_response = client.post(
            "/queries/v1/query-request",
            json={"sqlText": f"SELECT * FROM {table_name}", "sequenceId": 3},
            headers=auth_header
        )
        
        data = select_response.json()
        assert data["success"] is True
        assert len(data["data"]["rowset"]) == 1
        assert data["data"]["rowset"][0] == ["1", "Alice"]
    
    def test_query_response_format(self, client):
        """Test that query response matches Snowflake format"""
        token = self.create_session(client)
        
        query_body = {
            "sqlText": "SELECT 42 as answer",
            "sequenceId": 1
        }
        
        response = client.post(
            "/queries/v1/query-request",
            json=query_body,
            headers={"Authorization": f'Snowflake Token="{token}"'}
        )
        
        data = response.json()
        
        # Check all required fields are present
        assert "success" in data
        assert "code" in data
        assert "message" in data
        assert "data" in data
        
        response_data = data["data"]
        assert "rowtype" in response_data
        assert "rowset" in response_data
        assert "queryId" in response_data
        assert "finalDatabaseName" in response_data
        assert "finalSchemaName" in response_data
        assert "statementTypeId" in response_data
        assert "queryResultFormat" in response_data
    
    def test_query_with_gzip_body(self, client):
        """Test query with gzip-compressed request body"""
        token = self.create_session(client)
        
        query_body = {
            "sqlText": "SELECT 'compressed' as data",
            "sequenceId": 1
        }
        
        json_body = json.dumps(query_body).encode('utf-8')
        compressed_body = gzip.compress(json_body)
        
        response = client.post(
            "/queries/v1/query-request",
            data=compressed_body,
            headers={
                "Authorization": f'Snowflake Token="{token}"',
                "Content-Encoding": "gzip",
                "Content-Type": "application/json"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["rowset"][0][0] == "compressed"
    
    def test_query_adds_to_history(self, client):
        """Test that queries are added to history"""
        token = self.create_session(client)
        initial_count = len(query_history)
        
        client.post(
            "/queries/v1/query-request",
            json={"sqlText": "SELECT 1", "sequenceId": 1},
            headers={"Authorization": f'Snowflake Token="{token}"'}
        )
        
        assert len(query_history) == initial_count + 1
        assert query_history[-1]["query"] == "SELECT 1"
        assert query_history[-1]["success"] is True
    
    def test_failed_query_in_history(self, client):
        """Test that failed queries are also recorded in history"""
        token = self.create_session(client)
        
        client.post(
            "/queries/v1/query-request",
            json={"sqlText": "SELECT * FROM nonexistent_table", "sequenceId": 1},
            headers={"Authorization": f'Snowflake Token="{token}"'}
        )
        
        assert len(query_history) > 0
        last_query = query_history[-1]
        assert last_query["success"] is False
        assert last_query["error"] is not None


class TestFrontendAPI:
    """Tests for frontend API endpoints"""
    
    def test_list_sessions(self, client):
        """Test listing all sessions"""
        # Create a session
        request_body = {
            "data": {
                "LOGIN_NAME": "frontend_user",
                "PASSWORD": "password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        client.post("/session/v1/login-request", json=request_body)
        
        response = client.get("/api/sessions")
        
        assert response.status_code == 200
        data = response.json()
        assert "sessions" in data
        assert len(data["sessions"]) == 1
        assert data["sessions"][0]["user"] == "frontend_user"
    
    def test_list_queries(self, client):
        """Test listing query history"""
        response = client.get("/api/queries")
        
        assert response.status_code == 200
        data = response.json()
        assert "queries" in data
        assert "total" in data
    
    def test_get_stats(self, client):
        """Test getting server statistics"""
        response = client.get("/api/stats")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "uptime_seconds" in data
        assert "active_sessions" in data
        assert "total_queries" in data
        assert "successful_queries" in data
        assert "failed_queries" in data
        assert "average_query_duration_ms" in data
    
    def test_clear_query_history(self, client):
        """Test clearing query history"""
        # We need to import the module-level query_history
        from snowglobe_server import server as srv
        
        # Add some queries to history
        srv.query_history.append({
            "id": "test",
            "query": "SELECT 1",
            "success": True,
            "duration_ms": 10,
            "rows_affected": 1,
            "session_id": "test",
            "timestamp": "2024-01-01T00:00:00",
            "error": None
        })
        
        assert len(srv.query_history) > 0
        
        response = client.delete("/api/queries/history")
        
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert len(srv.query_history) == 0


class TestHealthCheck:
    """Tests for health check endpoint"""
    
    def test_health_check(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "healthy"
        assert "version" in data
        assert "uptime" in data
        assert "active_sessions" in data
        assert "queries_executed" in data


class TestEdgeCases:
    """Tests for edge cases and error handling"""
    
    def test_empty_login_name(self, client):
        """Test login with empty login name"""
        request_body = {
            "data": {
                "LOGIN_NAME": "",
                "PASSWORD": "password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response = client.post(
            "/session/v1/login-request",
            json=request_body
        )
        
        # Should still succeed (Snowglobe accepts any credentials)
        assert response.status_code == 200
        assert response.json()["success"] is True
    
    def test_special_characters_in_database_name(self, client):
        """Test database name with special characters"""
        request_body = {
            "data": {
                "LOGIN_NAME": "user",
                "PASSWORD": "password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response = client.post(
            "/session/v1/login-request?databaseName=MY_DB_123",
            json=request_body
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["sessionInfo"]["databaseName"] == "MY_DB_123"
    
    def test_case_insensitivity(self, client):
        """Test that database/schema names are uppercased"""
        request_body = {
            "data": {
                "LOGIN_NAME": "user",
                "PASSWORD": "password",
                "ACCOUNT_NAME": "snowglobe"
            }
        }
        
        response = client.post(
            "/session/v1/login-request?databaseName=lower_db&schemaName=lower_schema",
            json=request_body
        )
        
        assert response.status_code == 200
        session_info = response.json()["data"]["sessionInfo"]
        assert session_info["databaseName"] == "LOWER_DB"
        assert session_info["schemaName"] == "LOWER_SCHEMA"
    
    def test_invalid_auth_header_format(self, client):
        """Test query with invalid authorization header format"""
        query_body = {
            "sqlText": "SELECT 1",
            "sequenceId": 1
        }
        
        response = client.post(
            "/queries/v1/query-request",
            json=query_body,
            headers={"Authorization": "Bearer invalid_token"}
        )
        
        assert response.status_code == 200
        assert response.json()["success"] is False
