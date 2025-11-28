"""
Tests for Hybrid Tables functionality
"""

import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from snowglobe_server.hybrid_tables import HybridTableManager
from snowglobe_server.metadata import MetadataStore
import duckdb


@pytest.fixture
def setup_hybrid_manager(tmp_path):
    """Setup hybrid table manager for testing"""
    conn = duckdb.connect(':memory:')
    metadata = MetadataStore(str(tmp_path))
    manager = HybridTableManager(conn, metadata)
    
    # Create test database schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS testdb_public")
    
    yield manager, conn
    conn.close()


def test_create_hybrid_table(setup_hybrid_manager):
    """Test creating a hybrid table"""
    manager, conn = setup_hybrid_manager
    
    result = manager.create_hybrid_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="orders",
        columns=[
            {"name": "order_id", "type": "INTEGER", "not_null": True},
            {"name": "customer_id", "type": "INTEGER"},
            {"name": "amount", "type": "DECIMAL(10,2)"},
            {"name": "status", "type": "VARCHAR"}
        ],
        primary_key=["order_id"],
        indexes=[{"column": "customer_id"}]
    )
    
    assert result["success"] == True
    assert result["table_type"] == "HYBRID"
    assert result["supports_transactions"] == True
    assert result["supports_analytics"] == True
    
    # Verify table exists
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'orders'").fetchall()
    assert len(tables) == 1


def test_upsert_row(setup_hybrid_manager):
    """Test UPSERT operation on hybrid table"""
    manager, conn = setup_hybrid_manager
    
    # Create table
    manager.create_hybrid_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="orders",
        columns=[
            {"name": "order_id", "type": "INTEGER", "not_null": True},
            {"name": "amount", "type": "DECIMAL(10,2)"}
        ],
        primary_key=["order_id"]
    )
    
    # Insert
    result = manager.upsert_row(
        database="TESTDB",
        schema="PUBLIC",
        table_name="orders",
        data={"order_id": 1, "amount": 99.99},
        primary_key_cols=["order_id"]
    )
    
    assert result["success"] == True
    assert result["operation"] == "UPSERT"
    assert result["rows_affected"] == 1
    
    # Update same row
    result = manager.upsert_row(
        database="TESTDB",
        schema="PUBLIC",
        table_name="orders",
        data={"order_id": 1, "amount": 149.99},
        primary_key_cols=["order_id"]
    )
    
    assert result["success"] == True
    
    # Verify data
    rows = conn.execute("SELECT amount FROM testdb_public.orders WHERE order_id = 1").fetchall()
    assert len(rows) == 1
    assert float(rows[0][0]) == 149.99


def test_delete_row(setup_hybrid_manager):
    """Test DELETE operation on hybrid table"""
    manager, conn = setup_hybrid_manager
    
    # Create and populate table
    manager.create_hybrid_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="orders",
        columns=[
            {"name": "order_id", "type": "INTEGER", "not_null": True}
        ],
        primary_key=["order_id"]
    )
    
    manager.upsert_row(
        database="TESTDB",
        schema="PUBLIC",
        table_name="orders",
        data={"order_id": 1},
        primary_key_cols=["order_id"]
    )
    
    # Delete
    result = manager.delete_row(
        database="TESTDB",
        schema="PUBLIC",
        table_name="orders",
        where_clause="order_id = ?",
        params=[1]
    )
    
    assert result["success"] == True
    assert result["operation"] == "DELETE"
    
    # Verify deletion
    rows = conn.execute("SELECT * FROM testdb_public.orders WHERE order_id = 1").fetchall()
    assert len(rows) == 0


def test_get_row_by_pk(setup_hybrid_manager):
    """Test fast primary key lookup"""
    manager, conn = setup_hybrid_manager
    
    # Create and populate table
    manager.create_hybrid_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="customers",
        columns=[
            {"name": "customer_id", "type": "INTEGER", "not_null": True},
            {"name": "name", "type": "VARCHAR"}
        ],
        primary_key=["customer_id"]
    )
    
    manager.upsert_row(
        database="TESTDB",
        schema="PUBLIC",
        table_name="customers",
        data={"customer_id": 100, "name": "Alice"},
        primary_key_cols=["customer_id"]
    )
    
    # Get by PK
    result = manager.get_row_by_pk(
        database="TESTDB",
        schema="PUBLIC",
        table_name="customers",
        pk_values={"customer_id": 100}
    )
    
    assert result["success"] == True
    assert result["found"] == True
    assert result["data"]["customer_id"] == 100
    assert result["data"]["name"] == "Alice"
    
    # Get non-existent row
    result = manager.get_row_by_pk(
        database="TESTDB",
        schema="PUBLIC",
        table_name="customers",
        pk_values={"customer_id": 999}
    )
    
    assert result["success"] == True
    assert result["found"] == False
    assert result["data"] is None


def test_list_hybrid_tables(setup_hybrid_manager):
    """Test listing hybrid tables"""
    manager, conn = setup_hybrid_manager
    
    # Create multiple hybrid tables
    for i in range(3):
        manager.create_hybrid_table(
            database="TESTDB",
            schema="PUBLIC",
            table_name=f"table_{i}",
            columns=[{"name": "id", "type": "INTEGER"}],
            primary_key=["id"]
        )
    
    tables = manager.list_hybrid_tables(database="TESTDB", schema="PUBLIC")
    
    assert len(tables) == 3
    assert all(t["type"] == "HYBRID" for t in tables)
    assert all(t["database"] == "TESTDB" for t in tables)


def test_is_hybrid_table(setup_hybrid_manager):
    """Test checking if table is hybrid"""
    manager, conn = setup_hybrid_manager
    
    manager.create_hybrid_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="hybrid_table",
        columns=[{"name": "id", "type": "INTEGER"}],
        primary_key=["id"]
    )
    
    assert manager.is_hybrid_table("TESTDB", "PUBLIC", "hybrid_table") == True
    assert manager.is_hybrid_table("TESTDB", "PUBLIC", "non_existent") == False


def test_hybrid_table_with_multiple_pk_columns(setup_hybrid_manager):
    """Test hybrid table with composite primary key"""
    manager, conn = setup_hybrid_manager
    
    result = manager.create_hybrid_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="composite_pk",
        columns=[
            {"name": "dept_id", "type": "INTEGER", "not_null": True},
            {"name": "emp_id", "type": "INTEGER", "not_null": True},
            {"name": "name", "type": "VARCHAR"}
        ],
        primary_key=["dept_id", "emp_id"]
    )
    
    assert result["success"] == True
    
    # Upsert with composite PK
    manager.upsert_row(
        database="TESTDB",
        schema="PUBLIC",
        table_name="composite_pk",
        data={"dept_id": 10, "emp_id": 1, "name": "Bob"},
        primary_key_cols=["dept_id", "emp_id"]
    )
    
    # Get by composite PK
    result = manager.get_row_by_pk(
        database="TESTDB",
        schema="PUBLIC",
        table_name="composite_pk",
        pk_values={"dept_id": 10, "emp_id": 1}
    )
    
    assert result["success"] == True
    assert result["found"] == True
    assert result["data"]["name"] == "Bob"
