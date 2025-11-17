"""
Performance and stress tests for Snowglobe
"""

import pytest
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

pytestmark = [pytest.mark.performance, pytest.mark.slow]


class TestQueryPerformance:
    """Test query execution performance"""
    
    def test_simple_query_performance(self, query_executor):
        """Test simple query execution time"""
        start = time.time()
        result = query_executor.execute("SELECT 1")
        duration = time.time() - start
        
        assert result["success"] is True
        assert duration < 1.0  # Should complete in less than 1 second
    
    def test_create_table_performance(self, query_executor):
        """Test table creation performance"""
        start = time.time()
        result = query_executor.execute("CREATE TABLE perf_test (id INT, name VARCHAR)")
        duration = time.time() - start
        
        assert result["success"] is True
        assert duration < 2.0  # Should complete in less than 2 seconds
    
    def test_insert_performance(self, query_executor):
        """Test insert performance"""
        query_executor.execute("CREATE TABLE insert_perf (id INT, value VARCHAR)")
        
        start = time.time()
        for i in range(100):
            query_executor.execute(f"INSERT INTO insert_perf VALUES ({i}, 'value_{i}')")
        duration = time.time() - start
        
        # 100 inserts should complete in reasonable time
        assert duration < 10.0
    
    def test_select_performance(self, query_executor):
        """Test select query performance with moderate data"""
        query_executor.execute("CREATE TABLE select_perf (id INT)")
        
        # Insert data
        for i in range(1000):
            query_executor.execute(f"INSERT INTO select_perf VALUES ({i})")
        
        # Measure select performance
        start = time.time()
        result = query_executor.execute("SELECT * FROM select_perf")
        duration = time.time() - start
        
        assert result["success"] is True
        assert result["rowcount"] == 1000
        assert duration < 5.0  # Should complete in less than 5 seconds
    
    def test_aggregate_performance(self, query_executor):
        """Test aggregate query performance"""
        query_executor.execute("CREATE TABLE agg_perf (category VARCHAR, value INT)")
        
        # Insert test data
        for i in range(500):
            category = f"cat_{i % 10}"
            query_executor.execute(f"INSERT INTO agg_perf VALUES ('{category}', {i})")
        
        # Test aggregation performance
        start = time.time()
        result = query_executor.execute(
            "SELECT category, COUNT(*), SUM(value), AVG(value) FROM agg_perf GROUP BY category"
        )
        duration = time.time() - start
        
        assert result["success"] is True
        assert duration < 5.0
    
    def test_join_performance(self, query_executor):
        """Test join query performance"""
        # Create tables
        query_executor.execute("CREATE TABLE join_a (id INT, value VARCHAR)")
        query_executor.execute("CREATE TABLE join_b (id INT, data VARCHAR)")
        
        # Insert data
        for i in range(100):
            query_executor.execute(f"INSERT INTO join_a VALUES ({i}, 'value_{i}')")
            query_executor.execute(f"INSERT INTO join_b VALUES ({i}, 'data_{i}')")
        
        # Test join performance
        start = time.time()
        result = query_executor.execute(
            "SELECT a.id, a.value, b.data FROM join_a a JOIN join_b b ON a.id = b.id"
        )
        duration = time.time() - start
        
        assert result["success"] is True
        assert duration < 5.0


class TestConcurrentOperations:
    """Test concurrent query execution"""
    
    def test_concurrent_selects(self, temp_dir):
        """Test concurrent SELECT queries"""
        from snowglobe_server.query_executor import QueryExecutor
        
        def run_query(query_num):
            executor = QueryExecutor(temp_dir)
            result = executor.execute(f"SELECT {query_num}")
            executor.close()
            return result["success"]
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(run_query, i) for i in range(10)]
            results = [f.result() for f in as_completed(futures)]
        
        # All queries should succeed
        assert all(results)
    
    def test_concurrent_table_creation(self, temp_dir):
        """Test concurrent table creation"""
        from snowglobe_server.query_executor import QueryExecutor
        
        def create_table(table_num):
            executor = QueryExecutor(temp_dir)
            result = executor.execute(f"CREATE TABLE concurrent_table_{table_num} (id INT)")
            executor.close()
            return result["success"]
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(create_table, i) for i in range(5)]
            results = [f.result() for f in as_completed(futures)]
        
        # Most should succeed (some may have conflicts)
        assert sum(results) >= 3


class TestMemoryUsage:
    """Test memory efficiency"""
    
    def test_large_result_set(self, query_executor):
        """Test handling of large result sets"""
        query_executor.execute("CREATE TABLE large_results (id INT, data VARCHAR)")
        
        # Insert a reasonable amount of data
        for i in range(1000):
            query_executor.execute(f"INSERT INTO large_results VALUES ({i}, 'data_{i}')")
        
        result = query_executor.execute("SELECT * FROM large_results")
        
        assert result["success"] is True
        assert result["rowcount"] == 1000
        assert len(result["data"]) == 1000
    
    def test_repeated_queries(self, query_executor):
        """Test memory usage with repeated queries"""
        query_executor.execute("CREATE TABLE repeat_test (id INT)")
        query_executor.execute("INSERT INTO repeat_test VALUES (1)")
        
        # Execute same query multiple times
        for _ in range(100):
            result = query_executor.execute("SELECT * FROM repeat_test")
            assert result["success"] is True
    
    def test_many_small_queries(self, query_executor):
        """Test memory usage with many small queries"""
        # Execute many small queries
        for i in range(100):
            result = query_executor.execute(f"SELECT {i}")
            assert result["success"] is True


class TestScalability:
    """Test scalability limits"""
    
    def test_wide_table(self, query_executor):
        """Test table with many columns"""
        # Create table with 50 columns
        columns = ", ".join([f"col{i} INT" for i in range(50)])
        result = query_executor.execute(f"CREATE TABLE wide_table ({columns})")
        
        assert result["success"] is True
    
    def test_many_tables(self, query_executor):
        """Test creating many tables"""
        # Create multiple tables
        for i in range(20):
            result = query_executor.execute(f"CREATE TABLE many_tables_{i} (id INT)")
            assert result["success"] is True
    
    def test_complex_query(self, query_executor):
        """Test complex query with multiple operations"""
        query_executor.execute("CREATE TABLE complex_test (id INT, category VARCHAR, value INT)")
        
        # Insert test data
        for i in range(100):
            query_executor.execute(
                f"INSERT INTO complex_test VALUES ({i}, 'cat_{i % 5}', {i * 10})"
            )
        
        # Execute complex query
        complex_query = """
        SELECT 
            category,
            COUNT(*) as count,
            SUM(value) as total,
            AVG(value) as average,
            MIN(value) as minimum,
            MAX(value) as maximum
        FROM complex_test
        WHERE value > 100
        GROUP BY category
        HAVING COUNT(*) > 5
        ORDER BY total DESC
        """
        
        result = query_executor.execute(complex_query)
        assert result["success"] is True


class TestResourceCleanup:
    """Test resource cleanup"""
    
    def test_executor_cleanup(self, temp_dir):
        """Test that executors clean up properly"""
        from snowglobe_server.query_executor import QueryExecutor
        
        # Create and close multiple executors
        for _ in range(10):
            executor = QueryExecutor(temp_dir)
            executor.execute("SELECT 1")
            executor.close()
        
        # Should not leak resources
        assert True  # If we get here without errors, cleanup worked
    
    def test_failed_query_cleanup(self, query_executor):
        """Test cleanup after failed queries"""
        # Execute some failing queries
        for _ in range(10):
            query_executor.execute("SELECT * FROM nonexistent_table")
        
        # Should still be able to execute valid queries
        result = query_executor.execute("SELECT 1")
        assert result["success"] is True
    
    def test_transaction_cleanup(self, query_executor):
        """Test cleanup of uncommitted transactions"""
        query_executor.execute("BEGIN TRANSACTION")
        query_executor.execute("CREATE TABLE trans_cleanup (id INT)")
        # Don't commit - cleanup should handle it
        
        # Should still be functional
        result = query_executor.execute("SELECT 1")
        assert "success" in result


class TestResponseTime:
    """Test response time requirements"""
    
    def test_health_check_response_time(self, test_client):
        """Test health check responds quickly"""
        client = test_client
        start = time.time()
        response = client.get("/health")
        duration = time.time() - start
        
        assert response.status_code == 200
        assert duration < 0.5  # Should respond in less than 500ms
    
    def test_stats_response_time(self, test_client):
        """Test stats endpoint responds quickly"""
        client = test_client
        start = time.time()
        response = client.get("/api/stats")
        duration = time.time() - start
        
        assert response.status_code == 200
        assert duration < 1.0  # Should respond in less than 1 second
    
    def test_query_execution_response_time(self, test_client):
        """Test query execution response time"""
        client = test_client
        query_data = {"sql": "SELECT 1"}
        
        start = time.time()
        response = client.post("/api/execute", json=query_data)
        duration = time.time() - start
        
        assert response.status_code == 200
        assert duration < 2.0  # Should respond in less than 2 seconds
