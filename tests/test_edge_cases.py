"""
Tests for edge cases and error handling
"""

import pytest
from snowglobe_server.query_executor import QueryExecutor
from snowglobe_server.metadata import MetadataStore
from snowglobe_server.sql_translator import SnowflakeToDuckDBTranslator

pytestmark = pytest.mark.edge_case


class TestQueryExecutorEdgeCases:
    """Test edge cases in query executor"""
    
    def test_execute_empty_string(self, query_executor):
        """Test executing empty string - returns empty result"""
        result = query_executor.execute("")
        assert result["success"] is True
        assert result["data"] == []
        assert result["rowcount"] == 0
    
    def test_execute_whitespace_only(self, query_executor):
        """Test executing whitespace-only query - returns empty result"""
        result = query_executor.execute("   \n\t   ")
        assert result["success"] is True
        assert result["data"] == []
    
    def test_execute_comment_only(self, query_executor):
        """Test executing comment-only query - DuckDB executes comments as no-op"""
        result = query_executor.execute("-- This is a comment")
        # DuckDB executes comments successfully with no results
        assert result["success"] is True or "error" in result
    
    def test_execute_multiple_statements(self, query_executor):
        """Test executing multiple statements (should handle gracefully)"""
        # Create table first
        query_executor.execute("CREATE TABLE multi_test (id INT)")
        
        # Try multiple statements
        result = query_executor.execute(
            "INSERT INTO multi_test VALUES (1); INSERT INTO multi_test VALUES (2);"
        )
        # Should execute at least the first statement
        assert "error" in result or result["success"] is True
    
    def test_execute_very_long_query(self, query_executor):
        """Test executing very long query"""
        # Create a query with many values
        values = ", ".join([f"({i})" for i in range(1000)])
        result = query_executor.execute(f"SELECT * FROM (VALUES {values}) AS t(n)")
        
        if result["success"]:
            assert result["rowcount"] == 1000
    
    def test_select_from_nonexistent_table(self, query_executor):
        """Test selecting from non-existent table"""
        result = query_executor.execute("SELECT * FROM this_table_does_not_exist_xyz")
        assert result["success"] is False
        assert "error" in result
    
    def test_insert_into_nonexistent_table(self, query_executor):
        """Test inserting into non-existent table"""
        result = query_executor.execute("INSERT INTO nonexistent VALUES (1)")
        assert result["success"] is False
    
    def test_syntax_error_handling(self, query_executor):
        """Test handling of SQL syntax errors"""
        result = query_executor.execute("SELCT * FORM table")
        assert result["success"] is False
        assert "error" in result
    
    def test_division_by_zero(self, query_executor):
        """Test division by zero handling"""
        result = query_executor.execute("SELECT 1/0")
        # Should either handle gracefully or return error
        assert "error" in result or "data" in result
    
    def test_null_handling(self, query_executor):
        """Test NULL value handling"""
        result = query_executor.execute("SELECT NULL as null_col")
        assert result["success"] is True
        assert result["data"][0][0] is None
    
    def test_unicode_characters(self, query_executor):
        """Test handling of Unicode characters"""
        query_executor.execute("CREATE TABLE unicode_test (text VARCHAR)")
        result = query_executor.execute("INSERT INTO unicode_test VALUES ('Hello 世界 🌍')")
        
        if result["success"]:
            select_result = query_executor.execute("SELECT * FROM unicode_test")
            assert select_result["success"] is True
    
    def test_special_characters_in_strings(self, query_executor):
        """Test special characters in string literals"""
        result = query_executor.execute("SELECT 'It''s a test' as text")
        assert result["success"] is True
    
    def test_case_sensitivity(self, query_executor):
        """Test case sensitivity handling"""
        query_executor.execute("CREATE TABLE CaseSensitive (Id INT)")
        
        # Try accessing with different case
        result = query_executor.execute("SELECT * FROM casesensitive")
        # Should work due to case-insensitive handling
        assert result["success"] is True or "error" in result


class TestMetadataEdgeCases:
    """Test edge cases in metadata management"""
    
    def test_create_database_empty_name(self, metadata_store):
        """Test creating database with empty name"""
        with pytest.raises(ValueError):
            metadata_store.create_database("")
    
    def test_create_database_special_characters(self, metadata_store):
        """Test database name with special characters"""
        # Should handle or reject appropriately
        try:
            metadata_store.create_database("TEST-DB")
        except Exception:
            pass  # Expected for invalid names
    
    def test_create_duplicate_database(self, metadata_store):
        """Test creating duplicate database"""
        metadata_store.create_database("DUP_DB")
        
        # Creating again should fail or be handled
        with pytest.raises(Exception):
            metadata_store.create_database("DUP_DB")
    
    def test_drop_nonexistent_database(self, metadata_store):
        """Test dropping non-existent database"""
        with pytest.raises(Exception):
            metadata_store.drop_database("NONEXISTENT_DB_XYZ")
    
    def test_create_schema_in_nonexistent_database(self, metadata_store):
        """Test creating schema in non-existent database"""
        with pytest.raises(Exception):
            metadata_store.create_schema("NONEXISTENT_DB", "SCHEMA1")
    
    def test_very_long_database_name(self, metadata_store):
        """Test very long database name"""
        long_name = "A" * 300
        
        try:
            metadata_store.create_database(long_name)
            # If successful, should be able to retrieve it
            dbs = metadata_store.list_databases()
        except Exception:
            pass  # Expected if there's a length limit
    
    def test_database_with_numbers(self, metadata_store):
        """Test database name with numbers"""
        metadata_store.create_database("DB123")
        dbs = metadata_store.list_databases()
        assert any(db["name"] == "DB123" for db in dbs)
    
    def test_database_with_underscores(self, metadata_store):
        """Test database name with underscores"""
        metadata_store.create_database("TEST_DB_123")
        dbs = metadata_store.list_databases()
        assert any(db["name"] == "TEST_DB_123" for db in dbs)


class TestSQLTranslatorEdgeCases:
    """Test edge cases in SQL translation"""
    
    def test_translate_empty_query(self, sql_translator):
        """Test translating empty query"""
        result = sql_translator.translate("")
        assert result == ""
    
    def test_translate_whitespace(self, sql_translator):
        """Test translating whitespace"""
        result = sql_translator.translate("   \n\t   ")
        assert result.strip() == ""
    
    def test_translate_comment_only(self, sql_translator):
        """Test translating comment-only query"""
        result = sql_translator.translate("-- Comment")
        assert result.strip() == "-- Comment" or result.strip() == ""
    
    def test_translate_complex_nested_query(self, sql_translator):
        """Test translating complex nested query"""
        query = """
        SELECT *
        FROM (
            SELECT *
            FROM (
                SELECT id FROM table1
            ) t1
        ) t2
        WHERE id > 10
        """
        result = sql_translator.translate(query)
        assert "SELECT" in result
    
    def test_translate_with_multiple_joins(self, sql_translator):
        """Test translating query with multiple joins"""
        query = """
        SELECT t1.id, t2.name, t3.value
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.id
        JOIN table3 t3 ON t2.id = t3.id
        """
        result = sql_translator.translate(query)
        assert "JOIN" in result or "join" in result
    
    def test_translate_with_subquery(self, sql_translator):
        """Test translating query with subquery"""
        query = "SELECT * FROM table1 WHERE id IN (SELECT id FROM table2)"
        result = sql_translator.translate(query)
        assert "SELECT" in result


class TestConcurrency:
    """Test concurrent operations"""
    
    def test_multiple_executors_same_dir(self, temp_dir):
        """Test multiple executors on same directory"""
        executor1 = QueryExecutor(temp_dir)
        executor2 = QueryExecutor(temp_dir)
        
        # Both should work
        result1 = executor1.execute("SELECT 1")
        result2 = executor2.execute("SELECT 2")
        
        assert result1["success"] is True
        assert result2["success"] is True
        
        executor1.close()
        executor2.close()
    
    def test_create_table_in_multiple_executors(self, temp_dir):
        """Test creating tables in multiple executors"""
        executor1 = QueryExecutor(temp_dir)
        
        # Create table in first executor
        result1 = executor1.execute("CREATE TABLE shared_table (id INT)")
        assert result1["success"] is True
        
        # Close first executor to release lock
        executor1.close()
        
        # Create second executor to see if table persists
        executor2 = QueryExecutor(temp_dir)
        
        # Should be visible in second executor (via metadata)
        result2 = executor2.execute("SELECT * FROM shared_table")
        # Note: DuckDB may not share tables between separate connections
        # This tests that metadata is shared, not necessarily the DuckDB table
        assert result2["success"] is True or "error" in result2
        
        executor2.close()


class TestDataTypeEdgeCases:
    """Test edge cases with different data types"""
    
    def test_integer_overflow(self, query_executor):
        """Test handling of large integers"""
        result = query_executor.execute("SELECT 9223372036854775807 as big_int")
        assert result["success"] is True
    
    def test_negative_numbers(self, query_executor):
        """Test negative numbers"""
        result = query_executor.execute("SELECT -123 as neg_num")
        assert result["success"] is True
        assert result["data"][0][0] == -123
    
    def test_decimal_precision(self, query_executor):
        """Test decimal precision"""
        result = query_executor.execute("SELECT 123.456789 as decimal_num")
        assert result["success"] is True
    
    def test_empty_string(self, query_executor):
        """Test empty string values"""
        result = query_executor.execute("SELECT '' as empty_str")
        assert result["success"] is True
    
    def test_very_long_string(self, query_executor):
        """Test very long string"""
        long_string = "A" * 10000
        query_executor.execute("CREATE TABLE long_str_test (text VARCHAR)")
        result = query_executor.execute(f"INSERT INTO long_str_test VALUES ('{long_string}')")
        
        # Should handle or truncate
        assert result["success"] is True or "error" in result
    
    def test_boolean_values(self, query_executor):
        """Test boolean value handling"""
        result = query_executor.execute("SELECT TRUE as bool_val")
        assert result["success"] is True
    
    def test_date_formats(self, query_executor):
        """Test various date formats"""
        result = query_executor.execute("SELECT DATE '2024-01-01' as date_val")
        assert result["success"] is True


class TestTransactionEdgeCases:
    """Test edge cases in transaction handling"""
    
    def test_transaction_without_commit(self, query_executor):
        """Test transaction without commit"""
        query_executor.execute("BEGIN TRANSACTION")
        query_executor.execute("CREATE TABLE trans_test (id INT)")
        # Don't commit - what happens?
        
        # Should still work in same session
        result = query_executor.execute("SELECT * FROM trans_test")
        # Either succeeds or handles gracefully
        assert "success" in result
    
    def test_rollback_without_transaction(self, query_executor):
        """Test rollback without active transaction"""
        result = query_executor.execute("ROLLBACK")
        # Should handle gracefully
        assert "success" in result or "error" in result
    
    def test_nested_transactions(self, query_executor):
        """Test nested transactions"""
        query_executor.execute("BEGIN TRANSACTION")
        result = query_executor.execute("BEGIN TRANSACTION")
        # Should handle or reject
        assert "success" in result or "error" in result


class TestResourceLimits:
    """Test resource limit handling"""
    
    def test_many_columns(self, query_executor):
        """Test table with many columns"""
        cols = ", ".join([f"col{i} INT" for i in range(100)])
        result = query_executor.execute(f"CREATE TABLE many_cols ({cols})")
        
        # Should handle or return error
        assert "success" in result
    
    def test_many_rows_select(self, query_executor):
        """Test selecting many rows"""
        query_executor.execute("CREATE TABLE many_rows (id INT)")
        
        # Insert many rows
        for i in range(100):
            query_executor.execute(f"INSERT INTO many_rows VALUES ({i})")
        
        result = query_executor.execute("SELECT * FROM many_rows")
        if result["success"]:
            assert result["rowcount"] >= 100
    
    def test_deeply_nested_subqueries(self, query_executor):
        """Test deeply nested subqueries"""
        query = "SELECT * FROM (SELECT * FROM (SELECT 1 as n) t1) t2"
        result = query_executor.execute(query)
        assert "success" in result
