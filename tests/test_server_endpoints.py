"""
Tests for Snowglobe server HTTP endpoints
"""

import pytest
import json

pytestmark = pytest.mark.api


class TestHealthEndpoint:
    """Test health check endpoint"""
    
    def test_health_check(self, test_client):
        client = test_client
        """Test basic health check"""
        response = client.get("/health")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data
        assert "uptime" in data
        assert "active_sessions" in data
        assert "queries_executed" in data
    
    def test_health_check_response_format(self, test_client):
        client = test_client
        """Test health check response format"""
        response = client.get("/health")
        data = response.json()
        
        assert isinstance(data["active_sessions"], int)
        assert isinstance(data["queries_executed"], int)
        assert data["active_sessions"] >= 0
        assert data["queries_executed"] >= 0


class TestStatsEndpoint:
    """Test statistics endpoint"""
    
    def test_get_stats(self, test_client):
        client = test_client
        """Test stats endpoint"""
        response = client.get("/api/stats")
        assert response.status_code == 200
        
        data = response.json()
        assert "uptime_seconds" in data
        assert "active_sessions" in data
        assert "total_queries" in data
        assert "successful_queries" in data
        assert "failed_queries" in data
        assert "average_query_duration_ms" in data
    
    def test_stats_data_types(self, test_client):
        client = test_client
        """Test stats data types are correct"""
        response = client.get("/api/stats")
        data = response.json()
        
        assert isinstance(data["uptime_seconds"], (int, float))
        assert isinstance(data["active_sessions"], int)
        assert isinstance(data["total_queries"], int)
        assert isinstance(data["successful_queries"], int)
        assert isinstance(data["failed_queries"], int)
        assert isinstance(data["average_query_duration_ms"], (int, float))
    
    def test_stats_consistency(self, test_client):
        client = test_client
        """Test stats internal consistency"""
        response = client.get("/api/stats")
        data = response.json()
        
        # Total should equal successful + failed
        assert data["total_queries"] == data["successful_queries"] + data["failed_queries"]


class TestSessionsEndpoint:
    """Test sessions endpoint"""
    
    def test_list_sessions_empty(self, test_client):
        client = test_client
        """Test listing sessions when none exist"""
        response = client.get("/api/sessions")
        assert response.status_code == 200
        
        data = response.json()
        assert "sessions" in data
        assert isinstance(data["sessions"], list)
    
    def test_sessions_after_login(self, test_client):
        client = test_client
        """Test sessions list after creating a session"""
        # Create a session via login
        login_data = {
            "data": {
                "LOGIN_NAME": "testuser",
                "PASSWORD": "testpass",
                "ACCOUNT_NAME": "testaccount"
            }
        }
        
        login_response = client.post("/session/v1/login-request", json=login_data)
        assert login_response.status_code == 200
        assert login_response.json()["success"] is True
        
        # Now check sessions
        response = client.get("/api/sessions")
        data = response.json()
        
        assert len(data["sessions"]) == 1
        session = data["sessions"][0]
        assert session["user"] == "testuser"
        assert "session_id" in session
        assert "database" in session
        assert "schema" in session
        assert "warehouse" in session
        assert "role" in session


class TestQueriesEndpoint:
    """Test query history endpoint"""
    
    def test_list_queries_empty(self, test_client):
        client = test_client
        """Test listing queries when none exist"""
        response = client.get("/api/queries")
        assert response.status_code == 200
        
        data = response.json()
        assert "queries" in data
        assert "total" in data
        assert isinstance(data["queries"], list)
        assert isinstance(data["total"], int)
    
    def test_queries_with_limit(self, test_client):
        client = test_client
        """Test query listing with limit parameter"""
        response = client.get("/api/queries?limit=5")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data["queries"]) <= 5
    
    def test_queries_with_offset(self, test_client):
        client = test_client
        """Test query listing with offset parameter"""
        response = client.get("/api/queries?offset=10&limit=5")
        assert response.status_code == 200
        
        data = response.json()
        assert "queries" in data


class TestDatabasesEndpoint:
    """Test databases endpoint"""
    
    def test_list_databases(self, test_client):
        client = test_client
        """Test listing databases"""
        response = client.get("/api/databases")
        assert response.status_code == 200
        
        data = response.json()
        assert "databases" in data
        assert isinstance(data["databases"], list)
    
    def test_databases_structure(self, test_client):
        client = test_client
        """Test database structure"""
        # First create a session to ensure we have metadata
        login_data = {
            "data": {
                "LOGIN_NAME": "testuser",
                "PASSWORD": "testpass",
                "ACCOUNT_NAME": "testaccount"
            }
        }
        client.post("/session/v1/login-request", json=login_data)
        
        response = client.get("/api/databases")
        data = response.json()
        
        if len(data["databases"]) > 0:
            db = data["databases"][0]
            assert "name" in db
            assert "created_on" in db


class TestQueryExecution:
    """Test query execution from frontend"""
    
    def test_execute_simple_query(self, test_client):
        client = test_client
        """Test executing a simple SELECT query"""
        query_data = {
            "sql": "SELECT 1 as test_col"
        }
        
        response = client.post("/api/execute", json=query_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data["success"] is True
        assert "columns" in data
        assert "data" in data
        assert "rowcount" in data
        assert "duration_ms" in data
    
    def test_execute_empty_query(self, test_client):
        client = test_client
        """Test executing empty query"""
        query_data = {
            "sql": ""
        }
        
        response = client.post("/api/execute", json=query_data)
        data = response.json()
        
        assert data["success"] is False
        assert "error" in data
    
    def test_execute_invalid_query(self, test_client):
        client = test_client
        """Test executing invalid SQL"""
        query_data = {
            "sql": "SELECT * FROM nonexistent_table_xyz"
        }
        
        response = client.post("/api/execute", json=query_data)
        data = response.json()
        
        assert data["success"] is False
        assert "error" in data
    
    def test_execute_create_table(self, test_client):
        client = test_client
        """Test creating a table"""
        query_data = {
            "sql": "CREATE TABLE test_table (id INT, name VARCHAR)"
        }
        
        response = client.post("/api/execute", json=query_data)
        data = response.json()
        
        assert data["success"] is True
    
    def test_execute_insert_query(self, test_client):
        client = test_client
        """Test inserting data"""
        # First create table
        client.post("/api/execute", json={
            "sql": "CREATE TABLE insert_test (id INT, value VARCHAR)"
        })
        
        # Then insert
        query_data = {
            "sql": "INSERT INTO insert_test VALUES (1, 'test')"
        }
        
        response = client.post("/api/execute", json=query_data)
        data = response.json()
        
        assert data["success"] is True
        assert data["rowcount"] >= 0


class TestQueryHistoryManagement:
    """Test query history management"""
    
    def test_clear_query_history(self, test_client):
        client = test_client
        """Test clearing query history"""
        # Execute some queries first
        client.post("/api/execute", json={"sql": "SELECT 1"})
        client.post("/api/execute", json={"sql": "SELECT 2"})
        
        # Check history exists
        response = client.get("/api/queries")
        data = response.json()
        initial_count = data["total"]
        
        # Clear history
        clear_response = client.delete("/api/queries/history")
        assert clear_response.status_code == 200
        
        clear_data = clear_response.json()
        assert clear_data["success"] is True
        
        # Verify history is cleared
        response = client.get("/api/queries")
        data = response.json()
        assert data["total"] == 0


class TestLoginEndpoint:
    """Test Snowflake-compatible login endpoint"""
    
    def test_basic_login(self, test_client):
        client = test_client
        """Test basic login"""
        login_data = {
            "data": {
                "LOGIN_NAME": "testuser",
                "PASSWORD": "testpass",
                "ACCOUNT_NAME": "testaccount"
            }
        }
        
        response = client.post("/session/v1/login-request", json=login_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data["success"] is True
        assert "data" in data
        assert "token" in data["data"]
        assert "masterToken" in data["data"]
        assert "sessionId" in data["data"]
    
    def test_login_with_database(self, test_client):
        client = test_client
        """Test login with database specification"""
        login_data = {
            "data": {
                "LOGIN_NAME": "testuser",
                "PASSWORD": "testpass",
                "ACCOUNT_NAME": "testaccount"
            }
        }
        
        response = client.post(
            "/session/v1/login-request?databaseName=TEST_DB&schemaName=PUBLIC",
            json=login_data
        )
        assert response.status_code == 200
        
        data = response.json()
        assert data["success"] is True
        assert data["data"]["sessionInfo"]["databaseName"] == "TEST_DB"
        assert data["data"]["sessionInfo"]["schemaName"] == "PUBLIC"
    
    def test_login_token_format(self, test_client):
        client = test_client
        """Test that login returns properly formatted token"""
        login_data = {
            "data": {
                "LOGIN_NAME": "testuser",
                "PASSWORD": "testpass",
                "ACCOUNT_NAME": "testaccount"
            }
        }
        
        response = client.post("/session/v1/login-request", json=login_data)
        data = response.json()
        
        token = data["data"]["token"]
        assert isinstance(token, str)
        assert len(token) > 0


class TestQueryRequestEndpoint:
    """Test Snowflake-compatible query execution"""
    
    def test_query_without_session(self, test_client):
        client = test_client
        """Test query execution without valid session"""
        query_data = {
            "sqlText": "SELECT 1",
            "sequenceId": 1
        }
        
        response = client.post("/queries/v1/query-request", json=query_data)
        data = response.json()
        
        assert data["success"] is False
        assert "errorCode" in data["data"]
    
    def test_query_with_session(self, test_client):
        client = test_client
        """Test query execution with valid session"""
        # First login
        login_data = {
            "data": {
                "LOGIN_NAME": "testuser",
                "PASSWORD": "testpass",
                "ACCOUNT_NAME": "testaccount"
            }
        }
        
        login_response = client.post("/session/v1/login-request", json=login_data)
        token = login_response.json()["data"]["token"]
        
        # Execute query
        query_data = {
            "sqlText": "SELECT 1 as num",
            "sequenceId": 1
        }
        
        headers = {
            "Authorization": f'Snowflake Token="{token}"'
        }
        
        response = client.post(
            "/queries/v1/query-request",
            json=query_data,
            headers=headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "rowtype" in data["data"]
        assert "rowset" in data["data"]


class TestSessionManagement:
    """Test session lifecycle management"""
    
    def test_session_renewal(self, test_client):
        client = test_client
        """Test session token renewal"""
        # Create session
        login_data = {
            "data": {
                "LOGIN_NAME": "testuser",
                "PASSWORD": "testpass"
            }
        }
        
        login_response = client.post("/session/v1/login-request", json=login_data)
        old_token = login_response.json()["data"]["token"]
        
        # Renew session
        headers = {
            "Authorization": f'Snowflake Token="{old_token}"'
        }
        
        renew_response = client.post(
            "/session/v1/login-request:renew",
            headers=headers
        )
        
        assert renew_response.status_code == 200
        renew_data = renew_response.json()
        assert renew_data["success"] is True
        assert "token" in renew_data["data"]
        
        new_token = renew_data["data"]["token"]
        assert new_token != old_token
    
    def test_session_deletion(self, test_client):
        client = test_client
        """Test session deletion"""
        # Create session
        login_data = {
            "data": {
                "LOGIN_NAME": "testuser",
                "PASSWORD": "testpass"
            }
        }
        
        login_response = client.post("/session/v1/login-request", json=login_data)
        token = login_response.json()["data"]["token"]
        
        # Delete session
        headers = {
            "Authorization": f'Snowflake Token="{token}"'
        }
        
        delete_response = client.post("/session", headers=headers)
        assert delete_response.status_code == 200
        
        delete_data = delete_response.json()
        assert delete_data["success"] is True


class TestDashboard:
    """Test dashboard endpoint"""
    
    def test_dashboard_accessible(self, test_client):
        client = test_client
        """Test dashboard is accessible"""
        response = client.get("/dashboard")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
    
    def test_dashboard_contains_elements(self, test_client):
        client = test_client
        """Test dashboard contains expected elements"""
        response = client.get("/dashboard")
        content = response.text
        
        assert "Snowglobe" in content
        assert "Dashboard" in content
        assert "Queries" in content or "queries" in content
