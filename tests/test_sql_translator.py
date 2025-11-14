"""Tests for SQL Translator"""

import pytest
from snowglobe_server.sql_translator import translate_snowflake_to_duckdb


class TestDateFunctions:
    """Test date function translations"""
    
    def test_dateadd_year(self, sql_translator):
        """Test DATEADD with year"""
        sql = "SELECT DATEADD(YEAR, 1, '2023-01-01')"
        result = sql_translator.translate(sql)
        assert "INTERVAL" in result
        assert "YEAR" in result
    
    def test_dateadd_month(self, sql_translator):
        """Test DATEADD with month"""
        sql = "SELECT DATEADD(MONTH, 3, current_date)"
        result = sql_translator.translate(sql)
        assert "INTERVAL" in result
        assert "MONTH" in result
    
    def test_dateadd_day(self, sql_translator):
        """Test DATEADD with day"""
        sql = "SELECT DATEADD(DAY, 7, '2023-01-01')"
        result = sql_translator.translate(sql)
        assert "INTERVAL" in result
        assert "DAY" in result
    
    def test_datediff(self, sql_translator):
        """Test DATEDIFF function"""
        sql = "SELECT DATEDIFF(DAY, '2023-01-01', '2023-01-10')"
        result = sql_translator.translate(sql)
        assert "DATE_DIFF" in result
        assert "'day'" in result
    
    def test_to_date_simple(self, sql_translator):
        """Test TO_DATE without format"""
        sql = "SELECT TO_DATE('2023-01-01')"
        result = sql_translator.translate(sql)
        assert "CAST" in result
        assert "AS DATE" in result
    
    def test_to_timestamp(self, sql_translator):
        """Test TO_TIMESTAMP function"""
        sql = "SELECT TO_TIMESTAMP('2023-01-01 12:00:00')"
        result = sql_translator.translate(sql)
        assert "CAST" in result
        assert "AS TIMESTAMP" in result
    
    def test_getdate_function(self, sql_translator):
        """Test GETDATE function translation"""
        sql = "SELECT GETDATE()"
        result = sql_translator.translate(sql)
        assert "CURRENT_TIMESTAMP" in result
    
    def test_current_timestamp_function(self, sql_translator):
        """Test CURRENT_TIMESTAMP function"""
        sql = "SELECT CURRENT_TIMESTAMP"
        result = sql_translator.translate(sql)
        assert "CURRENT_TIMESTAMP" in result


class TestConditionalFunctions:
    """Test conditional function translations"""
    
    def test_iff_function(self, sql_translator):
        """Test IFF function translation"""
        sql = "SELECT IFF(x > 10, 'high', 'low')"
        result = sql_translator.translate(sql)
        assert "CASE WHEN" in result
        assert "THEN" in result
        assert "ELSE" in result
        assert "END" in result
    
    def test_nvl_function(self, sql_translator):
        """Test NVL function translation"""
        sql = "SELECT NVL(col1, 'default')"
        result = sql_translator.translate(sql)
        assert "COALESCE" in result
    
    def test_nvl2_function(self, sql_translator):
        """Test NVL2 function translation"""
        sql = "SELECT NVL2(col1, 'not null', 'null')"
        result = sql_translator.translate(sql)
        assert "CASE WHEN" in result
        assert "IS NOT NULL" in result
    
    def test_decode_function(self, sql_translator):
        """Test DECODE function translation"""
        sql = "SELECT DECODE(status, 1, 'Active', 2, 'Inactive', 'Unknown')"
        result = sql_translator.translate(sql)
        assert "CASE" in result
        assert "WHEN" in result
        assert "ELSE" in result


class TestNullHandlingFunctions:
    """Test NULL handling function translations"""
    
    def test_zeroifnull_function(self, sql_translator):
        """Test ZEROIFNULL function translation"""
        sql = "SELECT ZEROIFNULL(amount)"
        result = sql_translator.translate(sql)
        assert "COALESCE" in result
        assert ", 0)" in result
    
    def test_nullifzero_function(self, sql_translator):
        """Test NULLIFZERO function translation"""
        sql = "SELECT NULLIFZERO(value)"
        result = sql_translator.translate(sql)
        assert "CASE WHEN" in result
        assert "= 0 THEN NULL" in result
    
    def test_div0_function(self, sql_translator):
        """Test DIV0 function translation"""
        sql = "SELECT DIV0(a, b)"
        result = sql_translator.translate(sql)
        assert "CASE WHEN" in result
        assert "= 0 THEN 0" in result
    
    def test_equal_null_function(self, sql_translator):
        """Test EQUAL_NULL function translation"""
        sql = "SELECT EQUAL_NULL(a, b)"
        result = sql_translator.translate(sql)
        assert "IS NULL" in result
        assert "OR" in result


class TestStringFunctions:
    """Test string function translations"""
    
    def test_len_function(self, sql_translator):
        """Test LEN function translation"""
        sql = "SELECT LEN(name)"
        result = sql_translator.translate(sql)
        assert "LENGTH" in result
    
    def test_listagg(self, sql_translator):
        """Test LISTAGG function translation"""
        sql = "SELECT LISTAGG(name, ', ')"
        result = sql_translator.translate(sql)
        assert "STRING_AGG" in result
    
    def test_regexp_like(self, sql_translator):
        """Test REGEXP_LIKE function translation"""
        sql = "SELECT REGEXP_LIKE(col, '^test')"
        result = sql_translator.translate(sql)
        assert "REGEXP_MATCHES" in result


class TestArrayJsonFunctions:
    """Test array and JSON function translations"""
    
    def test_parse_json(self, sql_translator):
        """Test PARSE_JSON function"""
        sql = "SELECT PARSE_JSON('{\"key\": \"value\"}')"
        result = sql_translator.translate(sql)
        assert "::JSON" in result
    
    def test_array_construct(self, sql_translator):
        """Test ARRAY_CONSTRUCT function"""
        sql = "SELECT ARRAY_CONSTRUCT(1, 2, 3)"
        result = sql_translator.translate(sql)
        assert "LIST_VALUE(1, 2, 3)" in result
    
    def test_flatten(self, sql_translator):
        """Test FLATTEN function translation"""
        sql = "SELECT * FROM FLATTEN(array_col)"
        result = sql_translator.translate(sql)
        assert "UNNEST" in result


class TestDataTypeTranslations:
    """Test data type translations"""
    
    def test_data_type_number(self, sql_translator):
        """Test NUMBER data type translation"""
        sql = "CREATE TABLE t (col NUMBER(10,2))"
        result = sql_translator.translate(sql)
        assert "DECIMAL(10,2)" in result
    
    def test_data_type_varchar(self, sql_translator):
        """Test VARCHAR data type translation"""
        sql = "CREATE TABLE t (col VARCHAR(100))"
        result = sql_translator.translate(sql)
        assert "VARCHAR(100)" in result
    
    def test_data_type_string(self, sql_translator):
        """Test STRING data type translation"""
        sql = "CREATE TABLE t (col STRING)"
        result = sql_translator.translate(sql)
        assert "VARCHAR" in result
    
    def test_data_type_timestamp_ntz(self, sql_translator):
        """Test TIMESTAMP_NTZ data type translation"""
        sql = "CREATE TABLE t (col TIMESTAMP_NTZ)"
        result = sql_translator.translate(sql)
        assert "TIMESTAMP" in result
    
    def test_data_type_variant(self, sql_translator):
        """Test VARIANT data type translation"""
        sql = "CREATE TABLE t (col VARIANT)"
        result = sql_translator.translate(sql)
        assert "JSON" in result


class TestSampleClause:
    """Test SAMPLE clause translations"""
    
    def test_sample_rows(self, sql_translator):
        """Test SAMPLE clause translation"""
        sql = "SELECT * FROM table SAMPLE (100 ROWS)"
        result = sql_translator.translate(sql)
        assert "USING SAMPLE 100 ROWS" in result


class TestComplexTranslations:
    """Test complex SQL translation scenarios"""
    
    def test_complex_query_with_multiple_functions(self, sql_translator):
        """Test query with multiple Snowflake functions"""
        sql = """
        SELECT 
            NVL(name, 'Unknown'),
            IFF(status = 1, 'Active', 'Inactive'),
            DATEADD(DAY, 30, created_date),
            ZEROIFNULL(amount)
        FROM users
        """
        result = sql_translator.translate(sql)
        assert "COALESCE" in result
        assert "CASE WHEN" in result
        assert "INTERVAL" in result
    
    def test_create_table_with_snowflake_types(self, sql_translator):
        """Test CREATE TABLE with Snowflake-specific types"""
        sql = """
        CREATE TABLE analytics (
            id NUMBER(10),
            name STRING,
            data VARIANT,
            created_at TIMESTAMP_NTZ,
            is_active BOOLEAN
        )
        """
        result = sql_translator.translate(sql)
        assert "DECIMAL" in result or "INTEGER" in result or "DOUBLE" in result
        assert "VARCHAR" in result
        assert "JSON" in result
        assert "TIMESTAMP" in result
        assert "BOOLEAN" in result
    
    def test_nested_functions(self, sql_translator):
        """Test nested function calls"""
        sql = "SELECT NVL(IFF(x > 0, x, NULL), 0)"
        result = sql_translator.translate(sql)
        assert "COALESCE" in result
        assert "CASE WHEN" in result
    
    def test_simple_select(self, sql_translator):
        """Test simple SELECT statement"""
        sql = "SELECT * FROM users"
        result = sql_translator.translate(sql)
        assert "SELECT * FROM users" in result


class TestConvenienceFunction:
    """Test the convenience translation function"""
    
    def test_convenience_function(self):
        """Test convenience function"""
        sql = "SELECT NVL(col, 0)"
        result = translate_snowflake_to_duckdb(sql)
        assert "COALESCE" in result
