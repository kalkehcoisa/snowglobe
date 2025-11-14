"""Tests for Query Executor"""

import pytest


class TestBasicQueries:
    """Test basic query execution"""
    
    def test_simple_select(self, query_executor):
        """Test simple SELECT query"""
        result = query_executor.execute("SELECT 1 as num")
        assert result["success"] is True
        assert result["data"] == [[1]]
        assert result["columns"] == ["num"]
    
    def test_select_arithmetic(self, query_executor):
        """Test SELECT with arithmetic"""
        result = query_executor.execute("SELECT 2 + 3 as sum")
        assert result["success"] is True
        assert result["data"][0][0] == 5
    
    def test_select_multiple_values(self, query_executor):
        """Test SELECT with multiple values"""
        result = query_executor.execute("SELECT 1, 2, 3")
        assert result["success"] is True
        assert result["data"][0] == [1, 2, 3]
    
    def test_empty_query(self, query_executor):
        """Test empty query"""
        result = query_executor.execute("")
        assert result["success"] is True
    
    def test_semicolon_handling(self, query_executor):
        """Test that semicolons are handled properly"""
        result = query_executor.execute("SELECT 1;")
        assert result["success"] is True
        assert result["data"] == [[1]]


class TestTableOperations:
    """Test table DDL and DML operations"""
    
    def test_create_table(self, query_executor):
        """Test CREATE TABLE"""
        result = query_executor.execute("""
            CREATE TABLE users (
                id INT,
                name VARCHAR,
                email VARCHAR
            )
        """)
        assert result["success"] is True
        assert query_executor.metadata.table_exists("SNOWGLOBE", "PUBLIC", "USERS")
    
    def test_insert_and_select(self, query_executor):
        """Test INSERT and SELECT"""
        query_executor.execute("CREATE TABLE test (id INT, value VARCHAR)")
        query_executor.execute("INSERT INTO test VALUES (1, 'one')")
        result = query_executor.execute("SELECT * FROM test")
        assert result["success"] is True
        assert len(result["data"]) == 1
        assert result["data"][0] == [1, 'one']
    
    def test_multiple_inserts(self, query_executor):
        """Test multiple INSERT operations"""
        query_executor.execute("CREATE TABLE numbers (n INT)")
        query_executor.execute("INSERT INTO numbers VALUES (1)")
        query_executor.execute("INSERT INTO numbers VALUES (2)")
        query_executor.execute("INSERT INTO numbers VALUES (3)")
        result = query_executor.execute("SELECT * FROM numbers ORDER BY n")
        assert len(result["data"]) == 3
        assert result["data"][0][0] == 1
        assert result["data"][2][0] == 3
    
    def test_update(self, query_executor):
        """Test UPDATE operation"""
        query_executor.execute("CREATE TABLE items (id INT, name VARCHAR)")
        query_executor.execute("INSERT INTO items VALUES (1, 'old')")
        query_executor.execute("UPDATE items SET name = 'new' WHERE id = 1")
        result = query_executor.execute("SELECT name FROM items WHERE id = 1")
        assert result["data"][0][0] == 'new'
    
    def test_delete(self, query_executor):
        """Test DELETE operation"""
        query_executor.execute("CREATE TABLE temp (id INT)")
        query_executor.execute("INSERT INTO temp VALUES (1)")
        query_executor.execute("INSERT INTO temp VALUES (2)")
        query_executor.execute("DELETE FROM temp WHERE id = 1")
        result = query_executor.execute("SELECT * FROM temp")
        assert len(result["data"]) == 1
        assert result["data"][0][0] == 2
    
    def test_drop_table(self, query_executor):
        """Test DROP TABLE"""
        query_executor.execute("CREATE TABLE to_drop (id INT)")
        result = query_executor.execute("DROP TABLE to_drop")
        assert result["success"] is True
        assert not query_executor.metadata.table_exists("SNOWGLOBE", "PUBLIC", "TO_DROP")
    
    def test_truncate_table(self, query_executor):
        """Test TRUNCATE TABLE"""
        query_executor.execute("CREATE TABLE to_truncate (id INT)")
        query_executor.execute("INSERT INTO to_truncate VALUES (1)")
        query_executor.execute("INSERT INTO to_truncate VALUES (2)")
        result = query_executor.execute("TRUNCATE TABLE to_truncate")
        assert result["success"] is True
        count_result = query_executor.execute("SELECT COUNT(*) FROM to_truncate")
        assert count_result["data"][0][0] == 0
    
    def test_alter_table_rename(self, query_executor):
        """Test ALTER TABLE RENAME"""
        query_executor.execute("CREATE TABLE old_name (id INT)")
        result = query_executor.execute("ALTER TABLE old_name RENAME TO new_name")
        assert result["success"] is True
        assert not query_executor.metadata.table_exists("SNOWGLOBE", "PUBLIC", "OLD_NAME")
        assert query_executor.metadata.table_exists("SNOWGLOBE", "PUBLIC", "NEW_NAME")
    
    def test_clone_table(self, query_executor):
        """Test CREATE TABLE CLONE"""
        query_executor.execute("CREATE TABLE source (id INT, value VARCHAR)")
        query_executor.execute("INSERT INTO source VALUES (1, 'test')")
        result = query_executor.execute("CREATE TABLE target CLONE source")
        assert result["success"] is True
        assert query_executor.metadata.table_exists("SNOWGLOBE", "PUBLIC", "TARGET")
        count_result = query_executor.execute("SELECT COUNT(*) FROM target")
        assert count_result["data"][0][0] == 1


class TestDatabaseSchemaOperations:
    """Test database and schema operations"""
    
    def test_use_database(self, query_executor):
        """Test USE DATABASE command"""
        query_executor.execute("CREATE DATABASE test_db")
        result = query_executor.execute("USE DATABASE test_db")
        assert result["success"] is True
        assert query_executor.current_database == "TEST_DB"
    
    def test_use_schema(self, query_executor):
        """Test USE SCHEMA command"""
        query_executor.execute("CREATE SCHEMA analytics")
        result = query_executor.execute("USE SCHEMA analytics")
        assert result["success"] is True
        assert query_executor.current_schema == "ANALYTICS"
    
    def test_create_database(self, query_executor):
        """Test CREATE DATABASE"""
        result = query_executor.execute("CREATE DATABASE new_db")
        assert result["success"] is True
        assert query_executor.metadata.database_exists("NEW_DB")
    
    def test_create_schema_command(self, query_executor):
        """Test CREATE SCHEMA command"""
        result = query_executor.execute("CREATE SCHEMA new_schema")
        assert result["success"] is True
        assert query_executor.metadata.schema_exists("SNOWGLOBE", "NEW_SCHEMA")
    
    def test_use_warehouse(self, query_executor):
        """Test USE WAREHOUSE command"""
        result = query_executor.execute("USE WAREHOUSE my_wh")
        assert result["success"] is True
        assert query_executor.current_warehouse == "MY_WH"
    
    def test_use_role(self, query_executor):
        """Test USE ROLE command"""
        result = query_executor.execute("USE ROLE analyst")
        assert result["success"] is True
        assert query_executor.current_role == "ANALYST"


class TestShowCommands:
    """Test SHOW commands"""
    
    def test_show_databases(self, query_executor):
        """Test SHOW DATABASES command"""
        query_executor.execute("CREATE DATABASE db1")
        result = query_executor.execute("SHOW DATABASES")
        assert result["success"] is True
        assert len(result["data"]) >= 2
        names = [row[0] for row in result["data"]]
        assert "SNOWGLOBE" in names
        assert "DB1" in names
    
    def test_show_schemas(self, query_executor):
        """Test SHOW SCHEMAS command"""
        query_executor.execute("CREATE SCHEMA test_schema")
        result = query_executor.execute("SHOW SCHEMAS")
        assert result["success"] is True
        names = [row[0] for row in result["data"]]
        assert "PUBLIC" in names
        assert "TEST_SCHEMA" in names
    
    def test_show_tables(self, query_executor):
        """Test SHOW TABLES command"""
        query_executor.execute("CREATE TABLE t1 (id INT)")
        query_executor.execute("CREATE TABLE t2 (id INT)")
        result = query_executor.execute("SHOW TABLES")
        assert result["success"] is True
        names = [row[0] for row in result["data"]]
        assert "T1" in names
        assert "T2" in names
    
    def test_describe_table(self, query_executor):
        """Test DESCRIBE TABLE command"""
        query_executor.execute("CREATE TABLE desc_test (id INT, name VARCHAR, active BOOLEAN)")
        result = query_executor.execute("DESCRIBE TABLE desc_test")
        assert result["success"] is True
        assert len(result["data"]) == 3
        col_names = [row[0] for row in result["data"]]
        assert "ID" in col_names
        assert "NAME" in col_names
        assert "ACTIVE" in col_names
    
    def test_show_dropped_tables(self, query_executor):
        """Test SHOW DROPPED TABLES command"""
        query_executor.execute("CREATE TABLE to_drop (id INT)")
        query_executor.execute("DROP TABLE to_drop")
        result = query_executor.execute("SHOW DROPPED TABLES")
        assert result["success"] is True
        names = [row[0] for row in result["data"]]
        assert "TO_DROP" in names
    
    def test_show_dropped_schemas(self, query_executor):
        """Test SHOW DROPPED SCHEMAS command"""
        query_executor.execute("CREATE SCHEMA to_drop")
        query_executor.execute("DROP SCHEMA to_drop")
        result = query_executor.execute("SHOW DROPPED SCHEMAS")
        assert result["success"] is True
        names = [row[0] for row in result["data"]]
        assert "TO_DROP" in names
    
    def test_show_dropped_databases(self, query_executor):
        """Test SHOW DROPPED DATABASES command"""
        query_executor.execute("CREATE DATABASE to_drop_db")
        query_executor.execute("DROP DATABASE to_drop_db")
        result = query_executor.execute("SHOW DROPPED DATABASES")
        assert result["success"] is True
        names = [row[0] for row in result["data"]]
        assert "TO_DROP_DB" in names


class TestContextFunctions:
    """Test context functions"""
    
    def test_current_database_function(self, query_executor):
        """Test CURRENT_DATABASE() function"""
        result = query_executor.execute("SELECT CURRENT_DATABASE()")
        assert result["success"] is True
        assert result["data"][0][0] == "SNOWGLOBE"
    
    def test_current_schema_function(self, query_executor):
        """Test CURRENT_SCHEMA() function"""
        result = query_executor.execute("SELECT CURRENT_SCHEMA()")
        assert result["success"] is True
        assert result["data"][0][0] == "PUBLIC"
    
    def test_current_warehouse_function(self, query_executor):
        """Test CURRENT_WAREHOUSE() function"""
        result = query_executor.execute("SELECT CURRENT_WAREHOUSE()")
        assert result["success"] is True
        assert result["data"][0][0] == "COMPUTE_WH"
    
    def test_current_role_function(self, query_executor):
        """Test CURRENT_ROLE() function"""
        result = query_executor.execute("SELECT CURRENT_ROLE()")
        assert result["success"] is True
        assert result["data"][0][0] == "ACCOUNTADMIN"
    
    def test_set_variable(self, query_executor):
        """Test SET variable command"""
        result = query_executor.execute("SET my_var = 'test_value'")
        assert result["success"] is True
        assert query_executor.session_vars["MY_VAR"] == "'test_value'"
    
    def test_get_context(self, query_executor):
        """Test getting session context"""
        context = query_executor.get_context()
        assert "database" in context
        assert "schema" in context
        assert "warehouse" in context
        assert "role" in context
    
    def test_set_context(self, query_executor):
        """Test setting session context"""
        query_executor.set_context(
            database="TEST", 
            schema="ANALYTICS", 
            warehouse="LARGE_WH", 
            role="DEVELOPER"
        )
        context = query_executor.get_context()
        assert context["database"] == "TEST"
        assert context["schema"] == "ANALYTICS"
        assert context["warehouse"] == "LARGE_WH"
        assert context["role"] == "DEVELOPER"


class TestAggregates:
    """Test aggregate functions"""
    
    def test_count(self, executor_with_aggregates):
        """Test COUNT function"""
        result = executor_with_aggregates.execute("SELECT COUNT(*) FROM agg_test")
        assert result["data"][0][0] == 5
    
    def test_sum(self, executor_with_aggregates):
        """Test SUM function"""
        result = executor_with_aggregates.execute("SELECT SUM(value) FROM agg_test")
        assert result["data"][0][0] == 15
    
    def test_avg(self, executor_with_aggregates):
        """Test AVG function"""
        result = executor_with_aggregates.execute("SELECT AVG(value) FROM agg_test")
        assert result["data"][0][0] == 3.0
    
    def test_min(self, executor_with_aggregates):
        """Test MIN function"""
        result = executor_with_aggregates.execute("SELECT MIN(value) FROM agg_test")
        assert result["data"][0][0] == 1
    
    def test_max(self, executor_with_aggregates):
        """Test MAX function"""
        result = executor_with_aggregates.execute("SELECT MAX(value) FROM agg_test")
        assert result["data"][0][0] == 5


class TestGroupBy:
    """Test GROUP BY operations"""
    
    def test_group_by_sum(self, executor_with_groups):
        """Test GROUP BY with SUM"""
        result = executor_with_groups.execute("""
            SELECT category, SUM(amount) as total
            FROM groups
            GROUP BY category
            ORDER BY category
        """)
        assert result["data"][0] == ['A', 30]
        assert result["data"][1] == ['B', 70]
        assert result["data"][2] == ['C', 50]
    
    def test_group_by_count(self, executor_with_groups):
        """Test GROUP BY with COUNT"""
        result = executor_with_groups.execute("""
            SELECT category, COUNT(*) as cnt
            FROM groups
            GROUP BY category
            ORDER BY category
        """)
        assert result["data"][0] == ['A', 2]
        assert result["data"][1] == ['B', 2]
        assert result["data"][2] == ['C', 1]


class TestOrderAndLimit:
    """Test ORDER BY and LIMIT"""
    
    def test_order_by(self, query_executor):
        """Test ORDER BY clause"""
        query_executor.execute("CREATE TABLE ordered (id INT, name VARCHAR)")
        query_executor.execute("INSERT INTO ordered VALUES (3, 'C')")
        query_executor.execute("INSERT INTO ordered VALUES (1, 'A')")
        query_executor.execute("INSERT INTO ordered VALUES (2, 'B')")
        
        result = query_executor.execute("SELECT * FROM ordered ORDER BY id")
        assert result["data"][0][0] == 1
        assert result["data"][2][0] == 3
    
    def test_limit(self, query_executor):
        """Test LIMIT clause"""
        query_executor.execute("CREATE TABLE limited (id INT)")
        for i in range(10):
            query_executor.execute(f"INSERT INTO limited VALUES ({i})")
        
        result = query_executor.execute("SELECT * FROM limited LIMIT 3")
        assert len(result["data"]) == 3


class TestJoins:
    """Test JOIN operations"""
    
    def test_inner_join(self, executor_with_data):
        """Test INNER JOIN"""
        result = executor_with_data.execute("""
            SELECT p.name, o.quantity
            FROM products p
            JOIN orders o ON p.id = o.product_id
            ORDER BY p.name
        """)
        assert len(result["data"]) == 4
    
    def test_join_with_aggregation(self, executor_with_data):
        """Test JOIN with aggregation"""
        result = executor_with_data.execute("""
            SELECT p.name, SUM(o.quantity * p.price) as revenue
            FROM products p
            JOIN orders o ON p.id = o.product_id
            GROUP BY p.name
            ORDER BY revenue DESC
        """)
        assert result["data"][0][0] == 'Widget A'
        assert float(result["data"][0][1]) == 90.0


class TestCTE:
    """Test Common Table Expressions"""
    
    def test_simple_cte(self, query_executor):
        """Test simple CTE"""
        query_executor.execute("CREATE TABLE cte_test (value INT)")
        query_executor.execute("INSERT INTO cte_test VALUES (1)")
        query_executor.execute("INSERT INTO cte_test VALUES (2)")
        
        result = query_executor.execute("""
            WITH doubled AS (
                SELECT value * 2 as val FROM cte_test
            )
            SELECT * FROM doubled ORDER BY val
        """)
        assert result["data"][0][0] == 2
        assert result["data"][1][0] == 4


class TestUndropCommands:
    """Test UNDROP commands"""
    
    def test_undrop_table(self, query_executor):
        """Test UNDROP TABLE command"""
        query_executor.execute("CREATE TABLE undrop_test (id INT)")
        query_executor.execute("DROP TABLE undrop_test")
        
        assert not query_executor.metadata.table_exists("SNOWGLOBE", "PUBLIC", "UNDROP_TEST")
        
        result = query_executor.execute("UNDROP TABLE undrop_test")
        assert result["success"] is True
        assert query_executor.metadata.table_exists("SNOWGLOBE", "PUBLIC", "UNDROP_TEST")
    
    def test_undrop_schema(self, query_executor):
        """Test UNDROP SCHEMA command"""
        query_executor.execute("CREATE SCHEMA undrop_schema")
        query_executor.execute("DROP SCHEMA undrop_schema")
        
        result = query_executor.execute("UNDROP SCHEMA undrop_schema")
        assert result["success"] is True
        assert query_executor.metadata.schema_exists("SNOWGLOBE", "UNDROP_SCHEMA")
    
    def test_undrop_database(self, query_executor):
        """Test UNDROP DATABASE command"""
        query_executor.execute("CREATE DATABASE undrop_db")
        query_executor.execute("DROP DATABASE undrop_db")
        
        result = query_executor.execute("UNDROP DATABASE undrop_db")
        assert result["success"] is True
        assert query_executor.metadata.database_exists("UNDROP_DB")


class TestViewOperations:
    """Test VIEW operations"""
    
    def test_create_view(self, query_executor):
        """Test CREATE VIEW"""
        query_executor.execute("CREATE TABLE view_source (id INT, val VARCHAR)")
        query_executor.execute("INSERT INTO view_source VALUES (1, 'test')")
        result = query_executor.execute("CREATE VIEW my_view AS SELECT * FROM view_source")
        assert result["success"] is True


class TestErrorHandling:
    """Test error handling"""
    
    def test_error_handling(self, query_executor):
        """Test error handling for invalid SQL"""
        result = query_executor.execute("SELECT * FROM nonexistent_table")
        assert result["success"] is False
        assert "error" in result


class TestSnowflakeFunctions:
    """Test Snowflake-specific SQL functions"""
    
    def test_iff_translation(self, query_executor):
        """Test IFF function translation"""
        result = query_executor.execute("SELECT IFF(1 > 0, 'yes', 'no')")
        assert result["success"] is True
        assert result["data"][0][0] == 'yes'
    
    def test_div0_translation(self, query_executor):
        """Test DIV0 function translation"""
        result = query_executor.execute("SELECT DIV0(10, 0)")
        assert result["success"] is True
        assert result["data"][0][0] == 0
    
    def test_zeroifnull_translation(self, query_executor):
        """Test ZEROIFNULL function translation"""
        result = query_executor.execute("SELECT ZEROIFNULL(NULL)")
        assert result["success"] is True
        assert result["data"][0][0] == 0
    
    def test_case_when(self, query_executor):
        """Test CASE WHEN expression"""
        result = query_executor.execute("""
            SELECT CASE WHEN 1 > 0 THEN 'positive' ELSE 'non-positive' END
        """)
        assert result["data"][0][0] == 'positive'
