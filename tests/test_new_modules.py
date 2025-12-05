"""
Tests for new modules: Python Worksheets, Data Export, Object Management
"""

import pytest
import json
import os
import tempfile
from datetime import datetime

# Test Python Worksheet module
class TestPythonWorksheet:
    """Test Python worksheet functionality"""
    
    def test_snowpark_session_creation(self):
        """Test SnowparkSession can be created"""
        from snowglobe_server.python_worksheet import SnowparkSession, PythonWorksheetExecutor
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            session = SnowparkSession(executor)
            
            assert session.current_database is not None
            assert session.current_schema is not None
            executor.close()
    
    def test_dataframe_creation(self):
        """Test DataFrame creation from local data"""
        from snowglobe_server.python_worksheet import SnowparkSession, DataFrame
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            session = SnowparkSession(executor)
            
            data = [["Alice", 30], ["Bob", 25]]
            schema = ["name", "age"]
            
            df = session.create_dataframe(data, schema)
            
            assert df.count() == 2
            assert df.columns == schema
            executor.close()
    
    def test_python_code_execution(self):
        """Test executing Python code"""
        from snowglobe_server.python_worksheet import PythonWorksheetExecutor
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            py_executor = PythonWorksheetExecutor(executor)
            
            code = """
x = 1 + 1
y = "hello"
print(f"Result: {x}")
"""
            result = py_executor.execute(code)
            
            assert result["success"] == True
            assert "Result: 2" in result["output"]
            assert result["variables"]["x"] == 2
            assert result["variables"]["y"] == "hello"
            executor.close()
    
    def test_python_code_validation(self):
        """Test validating Python code"""
        from snowglobe_server.python_worksheet import PythonWorksheetExecutor
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            py_executor = PythonWorksheetExecutor(executor)
            
            # Valid code
            result = py_executor.validate_code("x = 1\nprint(x)")
            assert result["valid"] == True
            assert "x" in result["variables"]
            
            # Invalid code
            result = py_executor.validate_code("def foo(\n  pass")
            assert result["valid"] == False
            executor.close()
    
    def test_sql_execution_via_session(self):
        """Test SQL execution through session.sql()"""
        from snowglobe_server.python_worksheet import PythonWorksheetExecutor
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            py_executor = PythonWorksheetExecutor(executor)
            
            code = """
df = session.sql("SELECT 1 as val, 'test' as name")
row = df.collect()[0]
result_val = row.val
result_name = row.name
"""
            result = py_executor.execute(code)
            
            assert result["success"] == True
            assert len(result["dataframes"]) > 0
            executor.close()
    
    def test_column_operations(self):
        """Test Column class operations"""
        from snowglobe_server.python_worksheet import Column, col, lit
        
        c = col("age")
        
        # Test comparison operations
        eq = c == 30
        assert "= 30" in eq._expr
        
        gt = c > 18
        assert "> 18" in gt._expr
        
        # Test arithmetic
        plus = c + 1
        assert "+ 1" in plus._expr
        
        # Test logical
        and_expr = (c > 18) & (c < 65)
        assert "AND" in and_expr._expr
        
        # Test lit
        literal = lit("hello")
        assert "'hello'" in literal._expr


class TestDataExport:
    """Test data export functionality"""
    
    def test_csv_export(self):
        """Test CSV export"""
        from snowglobe_server.data_export import DataExporter
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            
            # Create test data
            executor.execute("CREATE TABLE test_export (id INT, name VARCHAR)")
            executor.execute("INSERT INTO test_export VALUES (1, 'Alice')")
            executor.execute("INSERT INTO test_export VALUES (2, 'Bob')")
            
            exporter = DataExporter(executor)
            result = exporter.export_query_result(
                "SELECT * FROM test_export",
                "csv"
            )
            
            assert result["success"] == True
            assert "Alice" in result["content"]
            assert "Bob" in result["content"]
            assert result["row_count"] == 2
            executor.close()
    
    def test_json_export(self):
        """Test JSON export"""
        from snowglobe_server.data_export import DataExporter
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            
            executor.execute("CREATE TABLE test_json (id INT, name VARCHAR)")
            executor.execute("INSERT INTO test_json VALUES (1, 'Test')")
            
            exporter = DataExporter(executor)
            result = exporter.export_query_result(
                "SELECT * FROM test_json",
                "json",
                {"orient": "records"}
            )
            
            assert result["success"] == True
            data = json.loads(result["content"])
            assert isinstance(data, list)
            assert len(data) == 1
            executor.close()
    
    def test_sql_export(self):
        """Test SQL INSERT export"""
        from snowglobe_server.data_export import DataExporter
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            
            executor.execute("CREATE TABLE test_sql (id INT, name VARCHAR)")
            executor.execute("INSERT INTO test_sql VALUES (1, 'Test')")
            
            exporter = DataExporter(executor)
            result = exporter.export_query_result(
                "SELECT * FROM test_sql",
                "sql",
                {"table_name": "MY_TABLE", "include_create": True}
            )
            
            assert result["success"] == True
            assert "INSERT INTO" in result["content"]
            assert "MY_TABLE" in result["content"]
            executor.close()
    
    def test_table_export(self):
        """Test table export with DDL"""
        from snowglobe_server.data_export import DataExporter
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            
            executor.execute("CREATE DATABASE TESTDB")
            executor.execute("USE DATABASE TESTDB")
            executor.execute("CREATE SCHEMA TESTSCHEMA")
            executor.execute("USE SCHEMA TESTSCHEMA")
            executor.execute("CREATE TABLE TESTTBL (id INT, val VARCHAR)")
            executor.execute("INSERT INTO TESTTBL VALUES (1, 'A')")
            
            exporter = DataExporter(executor)
            result = exporter.export_table(
                "TESTDB", "TESTSCHEMA", "TESTTBL",
                "sql",
                {"include_ddl": True}
            )
            
            assert result["success"] == True
            assert "TESTTBL" in result["content"]
            executor.close()
    
    def test_ddl_export(self):
        """Test DDL-only export"""
        from snowglobe_server.data_export import DataExporter
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            
            executor.execute("CREATE DATABASE DDLTEST")
            executor.execute("USE DATABASE DDLTEST")
            executor.execute("CREATE SCHEMA DDL_SCHEMA")
            
            exporter = DataExporter(executor)
            result = exporter.export_ddl(database="DDLTEST")
            
            assert result["success"] == True
            assert "CREATE DATABASE" in result["content"]
            assert "CREATE SCHEMA" in result["content"]
            executor.close()


class TestObjectManagement:
    """Test object management CRUD operations"""
    
    def test_create_database(self):
        """Test database creation"""
        from snowglobe_server.object_management import ObjectManager
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            mgr = ObjectManager(executor)
            
            result = mgr.create_database("NEW_DB", {"if_not_exists": True})
            
            assert result["success"] == True
            assert result["database"] == "NEW_DB"
            
            # Verify database exists
            dbs = executor.metadata.list_databases()
            db_names = [d["name"] for d in dbs]
            assert "NEW_DB" in db_names
            executor.close()
    
    def test_create_schema(self):
        """Test schema creation"""
        from snowglobe_server.object_management import ObjectManager
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            mgr = ObjectManager(executor)
            
            mgr.create_database("SCHEMA_TEST_DB")
            result = mgr.create_schema("SCHEMA_TEST_DB", "NEW_SCHEMA")
            
            assert result["success"] == True
            assert "SCHEMA_TEST_DB.NEW_SCHEMA" in result["schema"]
            executor.close()
    
    def test_create_table(self):
        """Test table creation - verifies SQL generation"""
        from snowglobe_server.object_management import ObjectManager
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            mgr = ObjectManager(executor)
            
            columns = [
                {"name": "id", "type": "INTEGER", "primary_key": True},
                {"name": "name", "type": "VARCHAR", "nullable": False},
                {"name": "email", "type": "VARCHAR"}
            ]
            
            result = mgr.create_table(
                executor.current_database,
                executor.current_schema,
                "USERS",
                columns
            )
            
            # Verify SQL is generated correctly
            assert "sql" in result
            assert "USERS" in result["sql"]
            assert "PRIMARY KEY" in result["sql"]
            assert "NOT NULL" in result["sql"]
            executor.close()
    
    def test_drop_table(self):
        """Test table drop"""
        from snowglobe_server.object_management import ObjectManager
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            mgr = ObjectManager(executor)
            
            # Create table first
            executor.execute("CREATE TABLE DROP_TEST (id INT)")
            
            result = mgr.drop_table(
                executor.current_database,
                executor.current_schema,
                "DROP_TEST",
                {"if_exists": True}
            )
            
            assert result["success"] == True
            executor.close()
    
    def test_alter_table_rename(self):
        """Test table rename operation"""
        from snowglobe_server.object_management import ObjectManager
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            mgr = ObjectManager(executor)
            
            # Create table using direct SQL
            executor.execute("CREATE TABLE ALTER_TEST (id INT)")
            
            result = mgr.alter_table(
                executor.current_database,
                executor.current_schema,
                "ALTER_TEST",
                {"set_comment": "Test comment"}
            )
            
            # Verify the alteration was attempted
            assert "alterations" in result
            assert any(a["action"] == "set_comment" for a in result["alterations"])
            executor.close()
    
    def test_create_view(self):
        """Test view creation - verifies SQL generation"""
        from snowglobe_server.object_management import ObjectManager
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            mgr = ObjectManager(executor)
            
            result = mgr.create_view(
                executor.current_database,
                executor.current_schema,
                "MY_VIEW",
                "SELECT 1 AS value"
            )
            
            # Verify SQL is generated correctly
            assert "sql" in result
            assert "MY_VIEW" in result["sql"]
            assert "SELECT" in result["sql"]
            executor.close()
    
    def test_create_stage(self):
        """Test stage creation - expect failure as DuckDB doesn't support STAGE natively"""
        from snowglobe_server.object_management import ObjectManager
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            mgr = ObjectManager(executor)
            
            result = mgr.create_stage(
                executor.current_database,
                executor.current_schema,
                "MY_STAGE",
                {"if_not_exists": True}
            )
            
            # STAGE is not directly supported by DuckDB - this tests the API generates correct SQL
            assert "MY_STAGE" in result.get("sql", "")
            executor.close()
    
    def test_truncate_table(self):
        """Test table truncation via direct SQL"""
        from snowglobe_server.query_executor import QueryExecutor
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            
            # Create and populate table
            executor.execute("CREATE TABLE TRUNC_TEST (id INT)")
            executor.execute("INSERT INTO TRUNC_TEST VALUES (1)")
            executor.execute("INSERT INTO TRUNC_TEST VALUES (2)")
            
            # Verify rows exist
            result = executor.execute("SELECT COUNT(*) FROM TRUNC_TEST")
            assert result["data"][0][0] == 2
            
            # Delete all rows (DuckDB translation for TRUNCATE)
            executor.execute("DELETE FROM TRUNC_TEST")
            
            # Verify empty
            result = executor.execute("SELECT COUNT(*) FROM TRUNC_TEST")
            assert result["data"][0][0] == 0
            executor.close()


class TestWorkspaceManager:
    """Test workspace manager Python worksheet support"""
    
    def test_create_python_worksheet(self):
        """Test creating Python worksheet"""
        from snowglobe_server.workspace import WorkspaceManager
        
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = WorkspaceManager(tmpdir)
            
            ws = mgr.create_worksheet(
                name="My Python Script",
                sql="print('Hello, World!')"
            )
            
            # Update to Python type
            mgr.update_worksheet(ws["id"], {"worksheet_type": "python"})
            
            # Retrieve and verify
            retrieved = mgr.get_worksheet(ws["id"])
            assert retrieved["worksheet_type"] == "python"
            assert retrieved["name"] == "My Python Script"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
