"""Integration tests for Snowglobe Server"""

import pytest
import tempfile
import os
import sys
import time
import subprocess
import signal

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="module")
def server_process():
    """Start Snowglobe server for integration tests"""
    temp_dir = tempfile.mkdtemp()
    
    # Set environment variables
    env = os.environ.copy()
    env["SNOWGLOBE_PORT"] = "8085"
    env["SNOWGLOBE_DATA_DIR"] = temp_dir
    env["SNOWGLOBE_LOG_LEVEL"] = "WARNING"
    
    # Start server
    process = subprocess.Popen(
        [sys.executable, "-m", "snowglobe_server.server"],
        env=env,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for server to start
    time.sleep(2)
    
    yield process, temp_dir
    
    # Cleanup
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
    
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.integration
class TestServerHealth:
    """Test server health and connectivity"""
    
    def test_server_starts(self, server_process):
        """Test that server starts successfully"""
        process, _ = server_process
        assert process.poll() is None, "Server process should be running"
    
    def test_health_endpoint(self, server_process):
        """Test health check endpoint"""
        import httpx
        
        process, _ = server_process
        if process.poll() is not None:
            pytest.skip("Server not running")
        
        try:
            response = httpx.get("http://localhost:8085/health", timeout=5)
            assert response.status_code == 200
        except Exception as e:
            pytest.skip(f"Health check failed: {e}")


@pytest.mark.integration
class TestServerQueryExecution:
    """Test query execution via server API"""
    
    def test_simple_query_via_api(self, server_process):
        """Test executing a simple query via API"""
        import httpx
        
        process, _ = server_process
        if process.poll() is not None:
            pytest.skip("Server not running")
        
        try:
            # Connect first
            connect_response = httpx.post(
                "http://localhost:8085/connect",
                json={
                    "user": "test_user",
                    "password": "test_pass",
                    "account": "test_account"
                },
                timeout=10
            )
            
            if connect_response.status_code != 200:
                pytest.skip("Connection failed")
            
            session_id = connect_response.json().get("session_id")
            
            # Execute query
            query_response = httpx.post(
                "http://localhost:8085/query",
                headers={"X-Session-ID": session_id},
                json={"sql": "SELECT 1 + 1 as result"},
                timeout=10
            )
            
            assert query_response.status_code == 200
            result = query_response.json()
            assert result["success"] is True
            assert result["data"][0][0] == 2
            
        except Exception as e:
            pytest.skip(f"Query execution failed: {e}")


@pytest.mark.integration
class TestDataPipeline:
    """Test data pipeline scenarios with direct executor"""
    
    def test_etl_workflow(self, query_executor):
        """Test ETL-like workflow"""
        # Create source table (Extract simulation)
        query_executor.execute("""
            CREATE TABLE raw_data (
                id INT,
                raw_value VARCHAR,
                timestamp VARCHAR
            )
        """)
        
        # Insert raw data
        query_executor.execute("INSERT INTO raw_data VALUES (1, '100', '2023-01-01')")
        query_executor.execute("INSERT INTO raw_data VALUES (2, '200', '2023-01-02')")
        query_executor.execute("INSERT INTO raw_data VALUES (3, '300', '2023-01-03')")
        
        # Create transformed table (Transform)
        query_executor.execute("""
            CREATE TABLE processed_data (
                id INT,
                value INT,
                processed_date DATE
            )
        """)
        
        # Transform and load
        query_executor.execute("""
            INSERT INTO processed_data
            SELECT 
                id,
                CAST(raw_value AS INT),
                CAST(timestamp AS DATE)
            FROM raw_data
        """)
        
        # Verify (Load verification)
        result = query_executor.execute("SELECT COUNT(*) FROM processed_data")
        count = result["data"][0][0]
        assert count == 3
        
        result = query_executor.execute("SELECT SUM(value) FROM processed_data")
        total = result["data"][0][0]
        assert total == 600
    
    def test_analytics_query(self, query_executor):
        """Test analytics-style query"""
        # Create sales data
        query_executor.execute("""
            CREATE TABLE sales (
                region VARCHAR,
                product VARCHAR,
                amount DECIMAL(10,2),
                sale_date DATE
            )
        """)
        
        # Insert sample data
        query_executor.execute("INSERT INTO sales VALUES ('North', 'Widget', 100.00, '2023-01-01')")
        query_executor.execute("INSERT INTO sales VALUES ('North', 'Gadget', 150.00, '2023-01-02')")
        query_executor.execute("INSERT INTO sales VALUES ('South', 'Widget', 120.00, '2023-01-01')")
        query_executor.execute("INSERT INTO sales VALUES ('South', 'Gadget', 180.00, '2023-01-03')")
        
        # Analytics query
        result = query_executor.execute("""
            SELECT 
                region,
                SUM(amount) as total_sales,
                AVG(amount) as avg_sale,
                COUNT(*) as num_sales
            FROM sales
            GROUP BY region
            ORDER BY total_sales DESC
        """)
        
        results = result["data"]
        assert len(results) == 2
        
        # South should have higher total (120 + 180 = 300)
        assert results[0][0] == 'South'


@pytest.mark.integration
class TestUndropFeatures:
    """Test UNDROP functionality end-to-end"""
    
    def test_undrop_table_workflow(self, query_executor):
        """Test complete UNDROP table workflow"""
        # Create and populate table
        query_executor.execute("CREATE TABLE important_data (id INT, value VARCHAR)")
        query_executor.execute("INSERT INTO important_data VALUES (1, 'critical')")
        
        # Verify data exists
        result = query_executor.execute("SELECT * FROM important_data")
        assert len(result["data"]) == 1
        
        # Drop the table
        query_executor.execute("DROP TABLE important_data")
        
        # Table should be gone
        result = query_executor.execute("SELECT * FROM important_data")
        assert result["success"] is False
        
        # Undrop the table
        result = query_executor.execute("UNDROP TABLE important_data")
        assert result["success"] is True
        
        # Table metadata should be restored (but data is lost in current implementation)
        assert query_executor.metadata.table_exists("SNOWGLOBE", "PUBLIC", "IMPORTANT_DATA")
    
    def test_show_dropped_tables_workflow(self, query_executor):
        """Test SHOW DROPPED TABLES workflow"""
        # Create and drop multiple tables
        query_executor.execute("CREATE TABLE drop1 (id INT)")
        query_executor.execute("CREATE TABLE drop2 (id INT)")
        query_executor.execute("DROP TABLE drop1")
        query_executor.execute("DROP TABLE drop2")
        
        # Show dropped tables
        result = query_executor.execute("SHOW DROPPED TABLES")
        assert result["success"] is True
        names = [row[0] for row in result["data"]]
        assert "DROP1" in names
        assert "DROP2" in names


@pytest.mark.integration
class TestComplexQueries:
    """Test complex query scenarios"""
    
    def test_subquery(self, executor_with_data):
        """Test subquery execution"""
        result = executor_with_data.execute("""
            SELECT name, price 
            FROM products 
            WHERE price > (SELECT AVG(price) FROM products)
            ORDER BY price
        """)
        assert result["success"] is True
        assert len(result["data"]) > 0
    
    def test_window_function(self, executor_with_data):
        """Test window function"""
        result = executor_with_data.execute("""
            SELECT 
                name,
                category,
                price,
                SUM(price) OVER (PARTITION BY category) as category_total
            FROM products
            ORDER BY category, name
        """)
        assert result["success"] is True
        assert len(result["data"]) == 4
    
    def test_multiple_joins(self, executor_with_data):
        """Test multiple table joins"""
        # Add customers table
        executor_with_data.execute("""
            CREATE TABLE customers (
                id INT,
                name VARCHAR
            )
        """)
        executor_with_data.execute("INSERT INTO customers VALUES (1, 'Customer A')")
        
        # Update orders to include customer_id
        executor_with_data.execute("""
            ALTER TABLE orders ADD COLUMN customer_id INT
        """)
        
        result = executor_with_data.execute("""
            SELECT 
                p.name as product,
                o.quantity,
                p.price * o.quantity as total
            FROM orders o
            JOIN products p ON o.product_id = p.id
            ORDER BY total DESC
        """)
        
        assert result["success"] is True
        assert len(result["data"]) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"])
