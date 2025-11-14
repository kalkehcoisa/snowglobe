"""
Shared test fixtures for Snowglobe tests
"""

import pytest
import tempfile
import shutil
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snowglobe_server.metadata import MetadataStore
from snowglobe_server.query_executor import QueryExecutor
from snowglobe_server.sql_translator import SnowflakeToDuckDBTranslator


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test data"""
    temp = tempfile.mkdtemp()
    yield temp
    shutil.rmtree(temp, ignore_errors=True)


@pytest.fixture
def metadata_store(temp_dir):
    """Create a MetadataStore instance with temporary directory"""
    return MetadataStore(temp_dir)


@pytest.fixture
def query_executor(temp_dir):
    """Create a QueryExecutor instance with temporary directory"""
    executor = QueryExecutor(temp_dir)
    yield executor
    executor.close()


@pytest.fixture
def sql_translator():
    """Create a SQL translator instance"""
    return SnowflakeToDuckDBTranslator()


@pytest.fixture
def sample_database(metadata_store):
    """Create a sample database with schema"""
    metadata_store.create_database("TEST_DB")
    metadata_store.create_schema("TEST_DB", "ANALYTICS")
    return metadata_store


@pytest.fixture
def sample_table(metadata_store):
    """Create a sample table in metadata"""
    columns = [
        {"name": "ID", "type": "INTEGER", "nullable": "N"},
        {"name": "NAME", "type": "VARCHAR", "nullable": "Y"},
        {"name": "EMAIL", "type": "VARCHAR", "nullable": "Y"},
        {"name": "AGE", "type": "INTEGER", "nullable": "Y"}
    ]
    metadata_store.register_table("SNOWGLOBE", "PUBLIC", "USERS", columns)
    return metadata_store


@pytest.fixture
def sample_view(metadata_store):
    """Create a sample view in metadata"""
    metadata_store.register_view(
        "SNOWGLOBE", "PUBLIC", "USER_VIEW", 
        "SELECT ID, NAME FROM USERS WHERE AGE > 18"
    )
    return metadata_store


@pytest.fixture
def executor_with_data(query_executor):
    """Create executor with sample data"""
    # Create tables
    query_executor.execute("""
        CREATE TABLE products (
            id INT,
            name VARCHAR,
            price DECIMAL(10,2),
            category VARCHAR
        )
    """)
    
    query_executor.execute("""
        CREATE TABLE orders (
            id INT,
            product_id INT,
            quantity INT,
            order_date DATE
        )
    """)
    
    # Insert sample products
    products = [
        (1, 'Widget A', 10.00, 'Widgets'),
        (2, 'Widget B', 15.00, 'Widgets'),
        (3, 'Gadget X', 25.00, 'Gadgets'),
        (4, 'Gadget Y', 30.00, 'Gadgets'),
    ]
    
    for p in products:
        query_executor.execute(
            f"INSERT INTO products VALUES ({p[0]}, '{p[1]}', {p[2]}, '{p[3]}')"
        )
    
    # Insert sample orders
    orders = [
        (1, 1, 5, '2023-01-01'),
        (2, 2, 3, '2023-01-02'),
        (3, 3, 2, '2023-01-03'),
        (4, 1, 4, '2023-01-04'),
    ]
    
    for o in orders:
        query_executor.execute(
            f"INSERT INTO orders VALUES ({o[0]}, {o[1]}, {o[2]}, '{o[3]}')"
        )
    
    return query_executor


@pytest.fixture
def executor_with_aggregates(query_executor):
    """Create executor with data for aggregate tests"""
    query_executor.execute("CREATE TABLE agg_test (value INT)")
    query_executor.execute("INSERT INTO agg_test VALUES (1)")
    query_executor.execute("INSERT INTO agg_test VALUES (2)")
    query_executor.execute("INSERT INTO agg_test VALUES (3)")
    query_executor.execute("INSERT INTO agg_test VALUES (4)")
    query_executor.execute("INSERT INTO agg_test VALUES (5)")
    return query_executor


@pytest.fixture
def executor_with_groups(query_executor):
    """Create executor with data for GROUP BY tests"""
    query_executor.execute("CREATE TABLE groups (category VARCHAR, amount INT)")
    query_executor.execute("INSERT INTO groups VALUES ('A', 10)")
    query_executor.execute("INSERT INTO groups VALUES ('A', 20)")
    query_executor.execute("INSERT INTO groups VALUES ('B', 30)")
    query_executor.execute("INSERT INTO groups VALUES ('B', 40)")
    query_executor.execute("INSERT INTO groups VALUES ('C', 50)")
    return query_executor


@pytest.fixture
def multiple_schemas(metadata_store):
    """Create multiple schemas for testing"""
    metadata_store.create_database("PROD_DB")
    metadata_store.create_schema("PROD_DB", "RAW")
    metadata_store.create_schema("PROD_DB", "STAGING")
    metadata_store.create_schema("PROD_DB", "ANALYTICS")
    return metadata_store


@pytest.fixture
def dropped_objects(metadata_store):
    """Create and drop objects for UNDROP testing"""
    # Create and drop database
    metadata_store.create_database("DROPPED_DB")
    metadata_store.drop_database("DROPPED_DB")
    
    # Create and drop schema
    metadata_store.create_schema("SNOWGLOBE", "DROPPED_SCHEMA")
    metadata_store.drop_schema("SNOWGLOBE", "DROPPED_SCHEMA")
    
    # Create and drop table
    columns = [{"name": "ID", "type": "INTEGER", "nullable": "N"}]
    metadata_store.register_table("SNOWGLOBE", "PUBLIC", "DROPPED_TABLE", columns)
    metadata_store.drop_table("SNOWGLOBE", "PUBLIC", "DROPPED_TABLE")
    
    # Create and drop view
    metadata_store.register_view("SNOWGLOBE", "PUBLIC", "DROPPED_VIEW", "SELECT 1")
    metadata_store.drop_view("SNOWGLOBE", "PUBLIC", "DROPPED_VIEW")
    
    return metadata_store
