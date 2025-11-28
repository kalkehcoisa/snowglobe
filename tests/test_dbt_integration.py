#!/usr/bin/env python3
"""
Test suite for dbt integration with Snowglobe
"""

import pytest
import sys
from pathlib import Path

# Add snowglobe_server to path
sys.path.insert(0, str(Path(__file__).parent / "snowglobe"))

from snowglobe_server.query_executor import QueryExecutor
from snowglobe_server.dbt_adapter import DbtAdapter
from snowglobe_server.dbt_profiles import (
    get_snowglobe_profile,
    get_dbt_project_yml,
    create_dbt_project
)


@pytest.fixture
def executor(tmp_path):
    """Create a QueryExecutor for testing"""
    data_dir = str(tmp_path / "data")
    return QueryExecutor(data_dir)


@pytest.fixture
def dbt_adapter(executor):
    """Create a DbtAdapter for testing"""
    return DbtAdapter(executor)


class TestDbtAdapter:
    """Test DbtAdapter functionality"""
    
    def test_create_adapter(self, dbt_adapter):
        """Test adapter creation"""
        assert dbt_adapter is not None
        assert dbt_adapter.executor is not None
        assert dbt_adapter.metadata is not None
    
    def test_get_catalog_empty(self, dbt_adapter):
        """Test catalog generation with empty database"""
        catalog = dbt_adapter.get_catalog()
        assert isinstance(catalog, list)
    
    def test_get_catalog_with_data(self, dbt_adapter, executor):
        """Test catalog generation with data"""
        # Create database, schema, and table
        executor.execute("CREATE DATABASE TEST_DB")
        executor.execute("USE DATABASE TEST_DB")
        executor.execute("CREATE SCHEMA TEST_SCHEMA")
        executor.execute("USE SCHEMA TEST_SCHEMA")
        executor.execute("""
            CREATE TABLE test_table (
                id INTEGER,
                name VARCHAR,
                created_at TIMESTAMP
            )
        """)
        
        # Get catalog
        catalog = dbt_adapter.get_catalog()
        
        # Verify catalog contains our table
        assert len(catalog) > 0
        
        table_entry = None
        for entry in catalog:
            if entry["metadata"]["name"] == "TEST_TABLE":
                table_entry = entry
                break
        
        assert table_entry is not None
        assert table_entry["metadata"]["database"] == "TEST_DB"
        assert table_entry["metadata"]["schema"] == "TEST_SCHEMA"
        assert "columns" in table_entry
        assert len(table_entry["columns"]) == 3
    
    def test_list_relations(self, dbt_adapter, executor):
        """Test listing relations"""
        # Create test table
        executor.execute("CREATE DATABASE TEST_DB")
        executor.execute("USE DATABASE TEST_DB")
        executor.execute("CREATE SCHEMA PUBLIC")
        executor.execute("USE SCHEMA PUBLIC")
        executor.execute("CREATE TABLE test_table (id INTEGER)")
        
        # List relations
        relations = dbt_adapter._macro_list_relations(
            database="TEST_DB",
            schema="PUBLIC"
        )
        
        assert len(relations) > 0
        assert any(r["identifier"] == "TEST_TABLE" for r in relations)
    
    def test_get_columns(self, dbt_adapter, executor):
        """Test getting column information"""
        # Create test table
        executor.execute("CREATE DATABASE TEST_DB")
        executor.execute("USE DATABASE TEST_DB")
        executor.execute("CREATE SCHEMA PUBLIC")
        executor.execute("""
            CREATE TABLE test_table (
                id INTEGER,
                name VARCHAR,
                amount DECIMAL(10, 2)
            )
        """)
        
        # Get columns
        relation = {
            "database": "TEST_DB",
            "schema": "PUBLIC",
            "identifier": "TEST_TABLE"
        }
        columns = dbt_adapter._macro_get_columns(relation)
        
        assert len(columns) == 3
        assert any(c["column"] == "ID" for c in columns)
        assert any(c["column"] == "NAME" for c in columns)
        assert any(c["column"] == "AMOUNT" for c in columns)
    
    def test_check_schema_exists(self, dbt_adapter, executor):
        """Test schema existence check"""
        executor.execute("CREATE DATABASE TEST_DB")
        executor.execute("CREATE SCHEMA TEST_DB.TEST_SCHEMA")
        
        # Check existing schema
        exists = dbt_adapter._macro_check_schema_exists(
            database="TEST_DB",
            schema="TEST_SCHEMA"
        )
        assert exists is True
        
        # Check non-existing schema
        exists = dbt_adapter._macro_check_schema_exists(
            database="TEST_DB",
            schema="NONEXISTENT"
        )
        assert exists is False
    
    def test_create_schema(self, dbt_adapter, executor):
        """Test schema creation"""
        executor.execute("CREATE DATABASE TEST_DB")
        
        success = dbt_adapter._macro_create_schema(
            database="TEST_DB",
            schema="NEW_SCHEMA"
        )
        
        assert success is True
        
        # Verify schema was created
        exists = dbt_adapter._macro_check_schema_exists(
            database="TEST_DB",
            schema="NEW_SCHEMA"
        )
        assert exists is True
    
    def test_drop_relation(self, dbt_adapter, executor):
        """Test relation dropping"""
        # Create test table
        executor.execute("CREATE DATABASE TEST_DB")
        executor.execute("CREATE SCHEMA TEST_DB.PUBLIC")
        executor.execute("CREATE TABLE TEST_DB.PUBLIC.test_table (id INTEGER)")
        
        # Drop relation
        relation = {
            "database": "TEST_DB",
            "schema": "PUBLIC",
            "identifier": "TEST_TABLE"
        }
        success = dbt_adapter._macro_drop_relation(relation)
        
        assert success is True
    
    def test_truncate_relation(self, dbt_adapter, executor):
        """Test relation truncation"""
        # Create and populate test table
        executor.execute("CREATE DATABASE TEST_DB")
        executor.execute("CREATE SCHEMA TEST_DB.PUBLIC")
        executor.execute("CREATE TABLE TEST_DB.PUBLIC.test_table (id INTEGER)")
        executor.execute("INSERT INTO TEST_DB.PUBLIC.test_table VALUES (1), (2), (3)")
        
        # Truncate relation
        relation = {
            "database": "TEST_DB",
            "schema": "PUBLIC",
            "identifier": "TEST_TABLE"
        }
        success = dbt_adapter._macro_truncate_relation(relation)
        
        assert success is True
        
        # Verify table is empty
        result = executor.execute("SELECT COUNT(*) FROM TEST_DB.PUBLIC.test_table")
        assert result["data"][0][0] == 0
    
    def test_create_table_as_sql(self, dbt_adapter):
        """Test CREATE TABLE AS SQL generation"""
        relation = {
            "database": "TEST_DB",
            "schema": "PUBLIC",
            "identifier": "new_table"
        }
        sql = "SELECT 1 as id, 'test' as name"
        
        result = dbt_adapter._macro_create_table_as(relation, sql)
        
        assert "CREATE" in result
        assert "TABLE" in result
        assert "new_table" in result
        assert "SELECT" in result
    
    def test_create_view_as_sql(self, dbt_adapter):
        """Test CREATE VIEW AS SQL generation"""
        relation = {
            "database": "TEST_DB",
            "schema": "PUBLIC",
            "identifier": "new_view"
        }
        sql = "SELECT 1 as id, 'test' as name"
        
        result = dbt_adapter._macro_create_view_as(relation, sql)
        
        assert "CREATE" in result
        assert "VIEW" in result
        assert "new_view" in result
        assert "SELECT" in result
    
    def test_information_schema_tables(self, dbt_adapter, executor):
        """Test information_schema.tables query"""
        # Create test data
        executor.execute("CREATE DATABASE TEST_DB")
        executor.execute("CREATE SCHEMA TEST_DB.PUBLIC")
        executor.execute("CREATE TABLE TEST_DB.PUBLIC.test_table (id INTEGER)")
        
        # Query information schema
        sql = """
            SELECT table_catalog, table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'PUBLIC'
        """
        
        result = dbt_adapter.handle_dbt_query(sql)
        
        assert result["success"] is True
        assert len(result["data"]) > 0
    
    def test_information_schema_columns(self, dbt_adapter, executor):
        """Test information_schema.columns query"""
        # Create test data
        executor.execute("CREATE DATABASE TEST_DB")
        executor.execute("CREATE SCHEMA TEST_DB.PUBLIC")
        executor.execute("""
            CREATE TABLE TEST_DB.PUBLIC.test_table (
                id INTEGER,
                name VARCHAR
            )
        """)
        
        # Query information schema
        sql = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'PUBLIC'
        """
        
        result = dbt_adapter.handle_dbt_query(sql)
        
        assert result["success"] is True
        assert len(result["data"]) >= 2


class TestDbtProfiles:
    """Test dbt profile generation"""
    
    def test_get_snowglobe_profile(self):
        """Test profile generation with defaults"""
        profile = get_snowglobe_profile()
        
        assert "snowglobe" in profile
        assert "target" in profile["snowglobe"]
        assert "outputs" in profile["snowglobe"]
        assert "dev" in profile["snowglobe"]["outputs"]
        
        dev_output = profile["snowglobe"]["outputs"]["dev"]
        assert dev_output["type"] == "snowflake"
        assert dev_output["account"] == "localhost"
        assert dev_output["host"] == "localhost"
        assert dev_output["port"] == 8443
        assert dev_output["protocol"] == "https"
        assert dev_output["insecure_mode"] is True
    
    def test_get_snowglobe_profile_custom(self):
        """Test profile generation with custom values"""
        profile = get_snowglobe_profile(
            profile_name="custom",
            target_name="prod",
            host="192.168.1.100",
            port=8080,
            protocol="http",
            database="PROD_DB",
            schema="MAIN"
        )
        
        assert "custom" in profile
        assert profile["custom"]["target"] == "prod"
        
        prod_output = profile["custom"]["outputs"]["prod"]
        assert prod_output["host"] == "192.168.1.100"
        assert prod_output["port"] == 8080
        assert prod_output["protocol"] == "http"
        assert prod_output["database"] == "PROD_DB"
        assert prod_output["schema"] == "MAIN"
    
    def test_get_dbt_project_yml(self):
        """Test dbt_project.yml generation"""
        yaml_content = get_dbt_project_yml("test_project")
        
        assert "name: test_project" in yaml_content
        assert "profile: snowglobe" in yaml_content
        assert "model-paths:" in yaml_content
        assert "config-version: 2" in yaml_content
    
    def test_create_dbt_project(self, tmp_path):
        """Test complete project creation"""
        project_dir = tmp_path / "test_project"
        
        result = create_dbt_project(str(project_dir), create_samples=True)
        
        assert Path(result).exists()
        assert (Path(result) / "dbt_project.yml").exists()
        assert (Path(result) / "README.md").exists()
        assert (Path(result) / ".gitignore").exists()
        assert (Path(result) / "models").exists()
        assert (Path(result) / "models" / "staging").exists()
        assert (Path(result) / "models" / "marts").exists()
        assert (Path(result) / "tests").exists()
        assert (Path(result) / "macros").exists()
        assert (Path(result) / "seeds").exists()
        
        # Check sample files
        assert (Path(result) / "models" / "staging" / "stg_sample.sql").exists()
        assert (Path(result) / "models" / "marts" / "mart_sample.sql").exists()
        assert (Path(result) / "seeds" / "sample_data.csv").exists()


class TestDbtQueries:
    """Test dbt-specific query handling"""
    
    def test_is_dbt_query(self, executor):
        """Test dbt query detection"""
        # Information schema queries
        assert executor._is_dbt_query("SELECT * FROM information_schema.tables")
        assert executor._is_dbt_query("SELECT * FROM information_schema.columns")
        assert executor._is_dbt_query("SELECT * FROM information_schema.schemata")
        
        # SHOW TERSE queries
        assert executor._is_dbt_query("SHOW TERSE SCHEMAS IN DATABASE test_db")
        assert executor._is_dbt_query("SHOW TERSE OBJECTS IN SCHEMA test_schema")
        
        # Regular queries should return False
        assert not executor._is_dbt_query("SELECT * FROM my_table")
        assert not executor._is_dbt_query("CREATE TABLE test (id INT)")
        assert not executor._is_dbt_query("INSERT INTO test VALUES (1)")
    
    def test_handle_dbt_query(self, executor):
        """Test dbt query execution"""
        # Create test data
        executor.execute("CREATE DATABASE TEST_DB")
        executor.execute("CREATE SCHEMA TEST_DB.PUBLIC")
        executor.execute("CREATE TABLE TEST_DB.PUBLIC.test_table (id INTEGER)")
        
        # Execute dbt query
        result = executor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'PUBLIC'
        """)
        
        assert result["success"] is True
        assert len(result["data"]) > 0


class TestDbtIntegration:
    """Integration tests for dbt functionality"""
    
    def test_full_dbt_workflow(self, executor):
        """Test complete dbt workflow simulation"""
        # 1. Create database
        result = executor.execute("CREATE DATABASE IF NOT EXISTS DBT_DB")
        assert result["success"] is True
        
        # 2. Create schema
        result = executor.execute("CREATE SCHEMA IF NOT EXISTS DBT_DB.PUBLIC")
        assert result["success"] is True
        
        # 3. Create source table
        result = executor.execute("""
            CREATE TABLE IF NOT EXISTS DBT_DB.PUBLIC.raw_customers (
                customer_id INTEGER,
                customer_name VARCHAR,
                customer_email VARCHAR,
                created_at TIMESTAMP
            )
        """)
        assert result["success"] is True
        
        # 4. Insert source data
        result = executor.execute("""
            INSERT INTO DBT_DB.PUBLIC.raw_customers VALUES
            (1, 'Alice', 'alice@example.com', CURRENT_TIMESTAMP),
            (2, 'Bob', 'bob@example.com', CURRENT_TIMESTAMP)
        """)
        assert result["success"] is True
        
        # 5. Create staging view (simulating dbt model)
        result = executor.execute("""
            CREATE VIEW DBT_DB.PUBLIC.stg_customers AS
            SELECT
                customer_id,
                UPPER(customer_name) as customer_name,
                LOWER(customer_email) as customer_email,
                created_at
            FROM DBT_DB.PUBLIC.raw_customers
        """)
        assert result["success"] is True
        
        # 6. Create mart table (simulating dbt model)
        result = executor.execute("""
            CREATE TABLE DBT_DB.PUBLIC.dim_customers AS
            SELECT
                customer_id,
                customer_name,
                customer_email,
                created_at,
                CURRENT_TIMESTAMP as dbt_updated_at
            FROM DBT_DB.PUBLIC.stg_customers
        """)
        assert result["success"] is True
        
        # 7. Query mart table
        result = executor.execute("SELECT * FROM DBT_DB.PUBLIC.dim_customers")
        assert result["success"] is True
        assert len(result["data"]) == 2
        
        # 8. Test data quality (simulating dbt test)
        result = executor.execute("""
            SELECT customer_id, COUNT(*)
            FROM DBT_DB.PUBLIC.dim_customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        """)
        assert result["success"] is True
        assert len(result["data"]) == 0  # No duplicates


def run_tests():
    """Run all tests"""
    pytest.main([__file__, "-v", "--tb=short"])


if __name__ == "__main__":
    run_tests()
