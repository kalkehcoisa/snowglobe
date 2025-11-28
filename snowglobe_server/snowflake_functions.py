"""
Snowflake SQL Functions - System-defined scalar and table functions
Comprehensive coverage of Snowflake's built-in functions
"""

import re
from typing import Dict, Any, List
from datetime import datetime, timedelta
import json
import hashlib
import uuid


class SnowflakeFunctionsRegistry:
    """Registry of Snowflake SQL functions with DuckDB translations"""
    
    def __init__(self):
        self.functions = {}
        self._register_all_functions()
    
    def _register_all_functions(self):
        """Register all supported Snowflake functions"""
        
        # String Functions
        self.functions.update({
            'CONCAT': 'CONCAT',
            'SUBSTRING': 'SUBSTRING',
            'LENGTH': 'LENGTH',
            'UPPER': 'UPPER',
            'LOWER': 'LOWER',
            'TRIM': 'TRIM',
            'LTRIM': 'LTRIM',
            'RTRIM': 'RTRIM',
            'REPLACE': 'REPLACE',
            'SPLIT': 'STRING_SPLIT',
            'SPLIT_PART': 'SPLIT_PART',
            'LEFT': 'LEFT',
            'RIGHT': 'RIGHT',
            'REVERSE': 'REVERSE',
            'REPEAT': 'REPEAT',
            'LPAD': 'LPAD',
            'RPAD': 'RPAD',
            'POSITION': 'POSITION',
            'CHARINDEX': 'POSITION',
            'CONTAINS': lambda x, y: f"POSITION({y} IN {x}) > 0",
            'STARTSWITH': lambda x, y: f"STARTS_WITH({x}, {y})",
            'ENDSWITH': lambda x, y: f"ENDS_WITH({x}, {y})",
        })
        
        # Date/Time Functions
        self.functions.update({
            'CURRENT_DATE': 'CURRENT_DATE',
            'CURRENT_TIME': 'CURRENT_TIME',
            'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP',
            'DATEADD': 'DATE_ADD',
            'DATEDIFF': 'DATE_DIFF',
            'DATE_TRUNC': 'DATE_TRUNC',
            'YEAR': 'YEAR',
            'MONTH': 'MONTH',
            'DAY': 'DAY',
            'HOUR': 'HOUR',
            'MINUTE': 'MINUTE',
            'SECOND': 'SECOND',
            'DAYOFWEEK': 'DAYOFWEEK',
            'DAYOFYEAR': 'DAYOFYEAR',
            'WEEK': 'WEEK',
            'QUARTER': 'QUARTER',
            'LAST_DAY': 'LAST_DAY',
            'TO_DATE': 'TO_DATE',
            'TO_TIMESTAMP': 'TO_TIMESTAMP',
            'DATE_FROM_PARTS': lambda y, m, d: f"MAKE_DATE({y}, {m}, {d})",
            'TIME_FROM_PARTS': lambda h, m, s: f"MAKE_TIME({h}, {m}, {s})",
            'TIMESTAMP_FROM_PARTS': lambda *args: f"MAKE_TIMESTAMP({', '.join(args)})",
        })
        
        # Numeric Functions
        self.functions.update({
            'ABS': 'ABS',
            'CEIL': 'CEIL',
            'FLOOR': 'FLOOR',
            'ROUND': 'ROUND',
            'TRUNCATE': 'TRUNC',
            'MOD': 'MOD',
            'POWER': 'POWER',
            'SQRT': 'SQRT',
            'EXP': 'EXP',
            'LN': 'LN',
            'LOG': 'LOG',
            'LOG10': 'LOG10',
            'SIGN': 'SIGN',
            'GREATEST': 'GREATEST',
            'LEAST': 'LEAST',
            'RANDOM': 'RANDOM',
            'UNIFORM': lambda min_val, max_val: f"({min_val} + RANDOM() * ({max_val} - {min_val}))",
            'DIV0': lambda x, y: f"CASE WHEN {y} = 0 THEN 0 ELSE {x} / {y} END",
            'DIV0NULL': lambda x, y: f"CASE WHEN {y} = 0 THEN NULL ELSE {x} / {y} END",
        })
        
        # Conditional Functions
        self.functions.update({
            'COALESCE': 'COALESCE',
            'NULLIF': 'NULLIF',
            'NVL': 'COALESCE',
            'NVL2': lambda expr, val1, val2: f"CASE WHEN {expr} IS NOT NULL THEN {val1} ELSE {val2} END",
            'IFF': lambda cond, true_val, false_val: f"CASE WHEN {cond} THEN {true_val} ELSE {false_val} END",
            'IFNULL': 'COALESCE',
            'ZEROIFNULL': lambda x: f"COALESCE({x}, 0)",
            'NULLIFZERO': lambda x: f"NULLIF({x}, 0)",
        })
        
        # Aggregate Functions
        self.functions.update({
            'COUNT': 'COUNT',
            'SUM': 'SUM',
            'AVG': 'AVG',
            'MIN': 'MIN',
            'MAX': 'MAX',
            'STDDEV': 'STDDEV',
            'VARIANCE': 'VARIANCE',
            'MEDIAN': 'MEDIAN',
            'MODE': 'MODE',
            'LISTAGG': 'STRING_AGG',
            'ARRAY_AGG': 'ARRAY_AGG',
            'OBJECT_AGG': lambda k, v: f"MAP_FROM_ENTRIES(ARRAY_AGG(STRUCT_PACK({k}, {v})))",
        })
        
        # Window Functions
        self.functions.update({
            'ROW_NUMBER': 'ROW_NUMBER',
            'RANK': 'RANK',
            'DENSE_RANK': 'DENSE_RANK',
            'PERCENT_RANK': 'PERCENT_RANK',
            'CUME_DIST': 'CUME_DIST',
            'NTILE': 'NTILE',
            'LAG': 'LAG',
            'LEAD': 'LEAD',
            'FIRST_VALUE': 'FIRST_VALUE',
            'LAST_VALUE': 'LAST_VALUE',
            'NTH_VALUE': 'NTH_VALUE',
        })
        
        # Conversion Functions
        self.functions.update({
            'CAST': 'CAST',
            'TRY_CAST': 'TRY_CAST',
            'TO_CHAR': 'TO_CHAR',
            'TO_NUMBER': 'TO_DECIMAL',
            'TO_NUMERIC': 'TO_DECIMAL',
            'TO_DECIMAL': 'TO_DECIMAL',
            'TO_DOUBLE': 'TO_DOUBLE',
            'TO_VARCHAR': 'TO_VARCHAR',
            'TO_BINARY': 'TO_BINARY',
            'TO_BOOLEAN': lambda x: f"CAST({x} AS BOOLEAN)",
            'TRY_TO_NUMBER': lambda x: f"TRY_CAST({x} AS DECIMAL)",
            'TRY_TO_DATE': lambda x: f"TRY_CAST({x} AS DATE)",
            'TRY_TO_TIMESTAMP': lambda x: f"TRY_CAST({x} AS TIMESTAMP)",
        })
        
        # JSON Functions
        self.functions.update({
            'PARSE_JSON': lambda x: f"CAST({x} AS JSON)",
            'TO_JSON': lambda x: f"TO_JSON({x})",
            'JSON_EXTRACT_PATH_TEXT': lambda json, path: f"JSON_EXTRACT_STRING({json}, {path})",
            'GET': lambda json, key: f"JSON_EXTRACT({json}, {key})",
            'GET_PATH': lambda json, path: f"JSON_EXTRACT({json}, {path})",
            'ARRAY_SIZE': 'JSON_ARRAY_LENGTH',
            'ARRAY_CONTAINS': lambda arr, val: f"LIST_CONTAINS({arr}, {val})",
            'OBJECT_KEYS': 'JSON_KEYS',
            'FLATTEN': 'UNNEST',
        })
        
        # Array/Object Functions
        self.functions.update({
            'ARRAY_CONSTRUCT': 'LIST_VALUE',
            'ARRAY_APPEND': 'LIST_APPEND',
            'ARRAY_CAT': 'LIST_CONCAT',
            'ARRAY_COMPACT': lambda arr: f"LIST_FILTER({arr}, x -> x IS NOT NULL)",
            'ARRAY_DISTINCT': lambda arr: f"LIST_DISTINCT({arr})",
            'ARRAY_INTERSECTION': 'LIST_INTERSECT',
            'ARRAY_POSITION': 'LIST_POSITION',
            'ARRAY_SIZE': 'LEN',
            'ARRAY_SLICE': 'LIST_SLICE',
            'ARRAY_TO_STRING': lambda arr, delim: f"LIST_AGGREGATE({arr}, 'STRING_AGG', {delim})",
            'OBJECT_CONSTRUCT': lambda *args: f"STRUCT_PACK({', '.join(args)})",
        })
        
        # Hash Functions
        self.functions.update({
            'MD5': 'MD5',
            'SHA1': 'SHA1',
            'SHA2': 'SHA256',
            'HASH': 'HASH',
            'UUID_STRING': lambda: "UUID()",
        })
        
        # System Functions
        self.functions.update({
            'CURRENT_USER': lambda: "'ACCOUNTADMIN'",
            'CURRENT_ROLE': lambda: "'ACCOUNTADMIN'",
            'CURRENT_DATABASE': lambda: "CURRENT_DATABASE()",
            'CURRENT_SCHEMA': lambda: "CURRENT_SCHEMA()",
            'CURRENT_WAREHOUSE': lambda: "'COMPUTE_WH'",
            'CURRENT_VERSION': lambda: "'8.0.0-snowglobe'",
            'CURRENT_ACCOUNT': lambda: "'SNOWGLOBE'",
            'CURRENT_REGION': lambda: "'LOCAL'",
            'SYSTEM$TYPEOF': 'TYPEOF',
        })
        
        # Metadata Functions
        self.functions.update({
            'METADATA$ACTION': lambda: "'INSERT'",
            'METADATA$ISUPDATE': lambda: "FALSE",
            'METADATA$ROW_ID': 'ROWID',
        })
    
    def get_function_mapping(self, snowflake_func: str) -> Any:
        """Get DuckDB mapping for a Snowflake function"""
        return self.functions.get(snowflake_func.upper())
    
    def is_supported(self, snowflake_func: str) -> bool:
        """Check if a Snowflake function is supported"""
        return snowflake_func.upper() in self.functions
    
    def list_functions(self, category: Optional[str] = None) -> List[str]:
        """List all supported functions"""
        return sorted(self.functions.keys())
    
    def get_function_signature(self, func_name: str) -> Optional[str]:
        """Get function signature/documentation"""
        signatures = {
            'CONCAT': 'CONCAT(expr1, expr2, ...) - Concatenates strings',
            'DATEADD': 'DATEADD(unit, value, date) - Adds interval to date',
            'DATEDIFF': 'DATEDIFF(unit, date1, date2) - Returns difference between dates',
            'IFF': 'IFF(condition, true_value, false_value) - Conditional expression',
            'PARSE_JSON': 'PARSE_JSON(string) - Parses JSON string',
            'ARRAY_CONSTRUCT': 'ARRAY_CONSTRUCT(expr1, expr2, ...) - Creates array',
            'OBJECT_CONSTRUCT': 'OBJECT_CONSTRUCT(key1, val1, ...) - Creates object',
        }
        return signatures.get(func_name.upper())


class TableFunctions:
    """Snowflake table functions (functions that return tables)"""
    
    @staticmethod
    def flatten(input_expr: str, path: Optional[str] = None,
               outer: bool = False, recursive: bool = False,
               mode: str = 'BOTH') -> str:
        """
        FLATTEN table function - Explodes arrays/objects into rows
        """
        return f"UNNEST({input_expr})"
    
    @staticmethod
    def generator(rowcount: int) -> str:
        """
        GENERATOR table function - Generates rows
        """
        return f"GENERATE_SERIES(1, {rowcount})"
    
    @staticmethod
    def result_scan(query_id: str) -> str:
        """
        RESULT_SCAN table function - Retrieves results of previous query
        """
        # Would need to implement query result caching
        return f"-- RESULT_SCAN not yet implemented"
    
    @staticmethod
    def split_to_table(string: str, delimiter: str) -> str:
        """
        SPLIT_TO_TABLE table function - Splits string into rows
        """
        return f"UNNEST(STRING_SPLIT({string}, {delimiter}))"


class UDFManager:
    """User-Defined Functions (UDF) manager"""
    
    def __init__(self):
        self.udfs = {}
    
    def create_udf(self, name: str, args: List[str], return_type: str,
                  language: str, body: str) -> Dict[str, Any]:
        """
        Create a User-Defined Function
        
        Args:
            name: Function name
            args: List of argument definitions
            return_type: Return data type
            language: SQL, PYTHON, JAVASCRIPT
            body: Function body
        """
        self.udfs[name.upper()] = {
            "name": name,
            "args": args,
            "return_type": return_type,
            "language": language,
            "body": body,
            "created_at": datetime.now().isoformat()
        }
        
        return {
            "success": True,
            "message": f"UDF {name} created successfully",
            "language": language
        }
    
    def call_udf(self, name: str, args: List[Any]) -> Any:
        """Call a user-defined function"""
        if name.upper() not in self.udfs:
            raise ValueError(f"UDF {name} not found")
        
        udf = self.udfs[name.upper()]
        
        # For SQL UDFs, would execute the SQL body
        # For Python/JS UDFs, would execute in appropriate runtime
        
        return None  # Placeholder
    
    def list_udfs(self) -> List[Dict[str, Any]]:
        """List all UDFs"""
        return list(self.udfs.values())
    
    def drop_udf(self, name: str) -> Dict[str, Any]:
        """Drop a UDF"""
        if name.upper() in self.udfs:
            del self.udfs[name.upper()]
            return {"success": True, "message": f"UDF {name} dropped"}
        return {"success": False, "error": f"UDF {name} not found"}
