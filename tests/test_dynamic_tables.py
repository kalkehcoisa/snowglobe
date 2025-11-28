"""
Tests for Dynamic Tables functionality
"""

import pytest
import sys
import os
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from snowglobe_server.dynamic_tables import DynamicTableManager
from snowglobe_server.metadata import MetadataStore
import duckdb


@pytest.fixture
def setup_dynamic_manager(tmp_path):
    """Setup dynamic table manager for testing"""
    conn = duckdb.connect(':memory:')
    metadata = MetadataStore(str(tmp_path))
    manager = DynamicTableManager(conn, metadata)
    
    # Create test schema and source table
    conn.execute("CREATE SCHEMA IF NOT EXISTS testdb_public")
    conn.execute("""
        CREATE TABLE testdb_public.source_data (
            id INTEGER,
            category VARCHAR,
            amount DECIMAL(10,2)
        )
    """)
    conn.execute("""
        INSERT INTO testdb_public.source_data VALUES
        (1, 'A', 100.00),
        (2, 'B', 200.00),
        (3, 'A', 150.00)
    """)
    
    yield manager, conn
    
    manager.shutdown()
    conn.close()


def test_create_dynamic_table(setup_dynamic_manager):
    """Test creating a dynamic table"""
    manager, conn = setup_dynamic_manager
    
    result = manager.create_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="summary",
        target_lag="5 minutes",
        refresh_mode="AUTO",
        query="""
            SELECT category, 
                   COUNT(*) as count,
                   SUM(amount) as total
            FROM testdb_public.source_data
            GROUP BY category
        """
    )
    
    assert result["success"] == True
    assert result["table_type"] == "DYNAMIC"
    assert result["target_lag"] == "5 minutes"
    assert result["refresh_mode"] == "AUTO"
    assert "next_refresh" in result
    
    # Verify table was created
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'summary'").fetchall()
    assert len(tables) == 1
    
    # Verify data
    rows = conn.execute("SELECT * FROM testdb_public.summary ORDER BY category").fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 'A'  # category
    assert rows[0][1] == 2     # count
    assert float(rows[0][2]) == 250.00  # total


def test_refresh_dynamic_table(setup_dynamic_manager):
    """Test manual refresh of dynamic table"""
    manager, conn = setup_dynamic_manager
    
    # Create dynamic table
    manager.create_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="summary",
        target_lag="1 hour",
        refresh_mode="FULL",
        query="SELECT category, COUNT(*) as count FROM testdb_public.source_data GROUP BY category"
    )
    
    # Add more data to source
    conn.execute("INSERT INTO testdb_public.source_data VALUES (4, 'C', 300.00)")
    
    # Manually refresh
    result = manager.refresh_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="summary"
    )
    
    assert result["success"] == True
    assert result["refresh_mode"] == "FULL"
    assert "duration_seconds" in result
    assert result["refresh_count"] == 1
    
    # Verify refreshed data
    rows = conn.execute("SELECT * FROM testdb_public.summary WHERE category = 'C'").fetchall()
    assert len(rows) == 1


def test_suspend_resume_dynamic_table(setup_dynamic_manager):
    """Test suspending and resuming dynamic table"""
    manager, conn = setup_dynamic_manager
    
    # Create dynamic table
    manager.create_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="summary",
        target_lag="10 seconds",
        refresh_mode="AUTO",
        query="SELECT category, COUNT(*) as count FROM testdb_public.source_data GROUP BY category"
    )
    
    # Suspend
    result = manager.suspend_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="summary"
    )
    
    assert result["success"] == True
    assert "suspended" in result["message"]
    
    # Resume
    result = manager.resume_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="summary"
    )
    
    assert result["success"] == True
    assert "resumed" in result["message"]


def test_alter_dynamic_table(setup_dynamic_manager):
    """Test altering dynamic table configuration"""
    manager, conn = setup_dynamic_manager
    
    # Create dynamic table
    manager.create_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="summary",
        target_lag="5 minutes",
        refresh_mode="AUTO",
        query="SELECT category FROM testdb_public.source_data"
    )
    
    # Alter target lag and mode
    result = manager.alter_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="summary",
        target_lag="10 minutes",
        refresh_mode="FULL"
    )
    
    assert result["success"] == True
    assert result["target_lag"] == "10 minutes"
    assert result["refresh_mode"] == "FULL"


def test_list_dynamic_tables(setup_dynamic_manager):
    """Test listing dynamic tables"""
    manager, conn = setup_dynamic_manager
    
    # Create multiple dynamic tables
    for i in range(3):
        manager.create_dynamic_table(
            database="TESTDB",
            schema="PUBLIC",
            table_name=f"summary_{i}",
            target_lag="5 minutes",
            refresh_mode="AUTO",
            query=f"SELECT category FROM testdb_public.source_data"
        )
    
    tables = manager.list_dynamic_tables(database="TESTDB", schema="PUBLIC")
    
    assert len(tables) == 3
    assert all(t["type"] == "DYNAMIC" for t in tables)
    assert all(t["target_lag"] == "5 minutes" for t in tables)


def test_parse_target_lag(setup_dynamic_manager):
    """Test parsing various target lag formats"""
    manager, conn = setup_dynamic_manager
    
    assert manager._parse_target_lag("30 seconds") == 30
    assert manager._parse_target_lag("5 minutes") == 300
    assert manager._parse_target_lag("2 hours") == 7200
    assert manager._parse_target_lag("1 day") == 86400
    assert manager._parse_target_lag("DOWNSTREAM") == 300  # Default


def test_dynamic_table_refresh_modes(setup_dynamic_manager):
    """Test different refresh modes"""
    manager, conn = setup_dynamic_manager
    
    # Test FULL mode
    manager.create_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="full_refresh",
        target_lag="1 hour",
        refresh_mode="FULL",
        query="SELECT * FROM testdb_public.source_data"
    )
    
    # Test INCREMENTAL mode (same as AUTO for now)
    manager.create_dynamic_table(
        database="TESTDB",
        schema="PUBLIC",
        table_name="incremental_refresh",
        target_lag="1 hour",
        refresh_mode="INCREMENTAL",
        query="SELECT * FROM testdb_public.source_data"
    )
    
    tables = manager.list_dynamic_tables()
    assert len(tables) == 2
    
    full_table = next(t for t in tables if t["table"] == "full_refresh")
    incr_table = next(t for t in tables if t["table"] == "incremental_refresh")
    
    assert full_table["refresh_mode"] == "FULL"
    assert incr_table["refresh_mode"] == "INCREMENTAL"
