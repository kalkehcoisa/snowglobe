"""
Query Executor - Executes SQL queries using DuckDB backend
"""

import duckdb
import re
import os
from typing import List, Dict, Any, Optional, Tuple
from .sql_translator import SnowflakeToDuckDBTranslator
from .metadata import MetadataStore
from .information_schema import InformationSchemaBuilder


class QueryExecutor:
    """Executes SQL queries with Snowflake compatibility"""
    
    def __init__(self, data_dir: str = "/data"):
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "snowglobe.duckdb")
        os.makedirs(data_dir, exist_ok=True)
        
        self.conn = duckdb.connect(self.db_path)
        self.translator = SnowflakeToDuckDBTranslator()
        self.metadata = MetadataStore(data_dir)
        self.information_schema = InformationSchemaBuilder(self.metadata)
        
        # Current context
        self.current_database = "SNOWGLOBE"
        self.current_schema = "PUBLIC"
        self.current_warehouse = "COMPUTE_WH"
        self.current_role = "ACCOUNTADMIN"
        
        # Session variables
        self.session_vars = {}
        
        # Initialize internal schema
        self._init_internal_schema()
    
    def _init_internal_schema(self):
        """Initialize internal DuckDB schema to mirror Snowflake structure"""
        # Create a schema for each database.schema combination
        try:
            self.conn.execute("CREATE SCHEMA IF NOT EXISTS snowglobe_public")
        except Exception:
            pass
    
    def _get_duckdb_schema(self, database: str, schema: str) -> str:
        """Convert Snowflake database.schema to DuckDB schema name"""
        return f"{database.lower()}_{schema.lower()}"
    
    def _ensure_schema_exists(self, database: str, schema: str):
        """Ensure DuckDB schema exists for the Snowflake database.schema"""
        duck_schema = self._get_duckdb_schema(database, schema)
        try:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {duck_schema}")
        except Exception:
            pass
    
    def execute(self, sql: str, params: Optional[List] = None) -> Dict[str, Any]:
        """Execute a SQL statement"""
        sql = sql.strip()
        if not sql:
            return {"success": True, "data": [], "columns": [], "rowcount": 0}
        
        # Remove trailing semicolon
        if sql.endswith(';'):
            sql = sql[:-1].strip()
        
        # Check for special Snowflake commands
        result = self._handle_special_commands(sql)
        if result is not None:
            return result
        
        # Parse and handle DDL
        result = self._handle_ddl(sql)
        if result is not None:
            return result
        
        # Translate and execute
        try:
            translated_sql = self._prepare_sql(sql)
            
            if params:
                result = self.conn.execute(translated_sql, params)
            else:
                result = self.conn.execute(translated_sql)
            
            # Fetch results if it's a SELECT query
            if self._is_select_query(sql):
                columns = [desc[0] for desc in result.description] if result.description else []
                data = result.fetchall()
                return {
                    "success": True,
                    "data": [list(row) for row in data],
                    "columns": columns,
                    "rowcount": len(data)
                }
            else:
                rowcount = 0
                if result.description:
                    row = result.fetchone()
                    if row is not None and len(row) > 0:
                        rowcount = row[0] if isinstance(row[0], int) else 0
                return {
                    "success": True,
                    "data": [],
                    "columns": [],
                    "rowcount": rowcount
                }
        
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "columns": [],
                "rowcount": 0
            }
    
    def _prepare_sql(self, sql: str) -> str:
        """Prepare SQL for execution"""
        # Translate Snowflake SQL to DuckDB
        translated = self.translator.translate(sql)
        
        # Replace unqualified table names with schema-qualified names
        translated = self._qualify_table_names(translated)
        
        return translated
    
    def _qualify_table_names(self, sql: str) -> str:
        """Add schema qualification to table names"""
        # This is a simplified version - a full implementation would use SQL parsing
        duck_schema = self._get_duckdb_schema(self.current_database, self.current_schema)
        
        # Handle CREATE TABLE
        pattern = r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|TEMP\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
        
        def replace_create_table(match):
            table_name = match.group(1)
            if '.' not in table_name:
                full_match = match.group(0)
                return full_match.replace(table_name, f"{duck_schema}.{table_name}", 1)
            return match.group(0)
        
        sql = re.sub(pattern, replace_create_table, sql, flags=re.IGNORECASE)
        
        # Handle DROP TABLE
        pattern = r'\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\b'
        
        def replace_drop_table(match):
            table_name = match.group(1)
            if '.' not in table_name:
                full_match = match.group(0)
                return full_match.replace(table_name, f"{duck_schema}.{table_name}", 1)
            return match.group(0)
        
        sql = re.sub(pattern, replace_drop_table, sql, flags=re.IGNORECASE)
        
        # Handle INSERT INTO
        pattern = r'\bINSERT\s+(?:INTO\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\(|VALUES|SELECT)'
        
        def replace_insert(match):
            table_name = match.group(1)
            if '.' not in table_name and table_name.upper() not in ['VALUES', 'SELECT']:
                full_match = match.group(0)
                return full_match.replace(table_name, f"{duck_schema}.{table_name}", 1)
            return match.group(0)
        
        sql = re.sub(pattern, replace_insert, sql, flags=re.IGNORECASE)
        
        # Handle UPDATE
        pattern = r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+SET\b'
        
        def replace_update(match):
            table_name = match.group(1)
            if '.' not in table_name:
                full_match = match.group(0)
                return full_match.replace(table_name, f"{duck_schema}.{table_name}", 1)
            return match.group(0)
        
        sql = re.sub(pattern, replace_update, sql, flags=re.IGNORECASE)
        
        # Handle DELETE FROM
        pattern = r'\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b'
        
        def replace_delete(match):
            table_name = match.group(1)
            if '.' not in table_name:
                full_match = match.group(0)
                return full_match.replace(table_name, f"{duck_schema}.{table_name}", 1)
            return match.group(0)
        
        sql = re.sub(pattern, replace_delete, sql, flags=re.IGNORECASE)
        
        # Handle FROM clause
        pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b(?!\s*\()'
        
        def replace_from(match):
            table_name = match.group(1)
            # Skip SQL keywords
            keywords = ['SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'EXCEPT', 'INTERSECT']
            if '.' not in table_name and table_name.upper() not in keywords:
                full_match = match.group(0)
                return full_match.replace(table_name, f"{duck_schema}.{table_name}", 1)
            return match.group(0)
        
        sql = re.sub(pattern, replace_from, sql, flags=re.IGNORECASE)
        
        # Handle JOIN
        pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\b(?!\s*\()'
        
        def replace_join(match):
            table_name = match.group(1)
            if '.' not in table_name:
                full_match = match.group(0)
                return full_match.replace(table_name, f"{duck_schema}.{table_name}", 1)
            return match.group(0)
        
        sql = re.sub(pattern, replace_join, sql, flags=re.IGNORECASE)
        
        return sql
    
    def _is_select_query(self, sql: str) -> bool:
        """Check if SQL is a SELECT query"""
        sql_upper = sql.strip().upper()
        return (sql_upper.startswith('SELECT') or 
                sql_upper.startswith('WITH') or
                sql_upper.startswith('SHOW') or
                sql_upper.startswith('DESCRIBE') or
                sql_upper.startswith('DESC '))
    
    def _handle_special_commands(self, sql: str) -> Optional[Dict[str, Any]]:
        """Handle Snowflake-specific commands"""
        sql_upper = sql.upper().strip()
        
        # USE DATABASE
        match = re.match(r'USE\s+(?:DATABASE\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match and 'SCHEMA' not in sql_upper and 'WAREHOUSE' not in sql_upper and 'ROLE' not in sql_upper:
            db_name = match.group(1).upper()
            if self.metadata.database_exists(db_name):
                self.current_database = db_name
                self._ensure_schema_exists(db_name, self.current_schema)
                return {"success": True, "data": [], "columns": [], "rowcount": 0, 
                        "message": f"Database changed to {db_name}"}
            else:
                return {"success": False, "error": f"Database '{db_name}' does not exist",
                        "data": [], "columns": [], "rowcount": 0}
        
        # USE SCHEMA
        match = re.match(r'USE\s+SCHEMA\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            schema_name = match.group(1).upper()
            if self.metadata.schema_exists(self.current_database, schema_name):
                self.current_schema = schema_name
                self._ensure_schema_exists(self.current_database, schema_name)
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Schema changed to {schema_name}"}
            else:
                return {"success": False, "error": f"Schema '{schema_name}' does not exist",
                        "data": [], "columns": [], "rowcount": 0}
        
        # USE WAREHOUSE
        match = re.match(r'USE\s+WAREHOUSE\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            wh_name = match.group(1).upper()
            self.current_warehouse = wh_name
            return {"success": True, "data": [], "columns": [], "rowcount": 0,
                    "message": f"Warehouse changed to {wh_name}"}
        
        # USE ROLE
        match = re.match(r'USE\s+ROLE\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            role_name = match.group(1).upper()
            self.current_role = role_name
            return {"success": True, "data": [], "columns": [], "rowcount": 0,
                    "message": f"Role changed to {role_name}"}
        
        # SHOW DATABASES
        if re.match(r'SHOW\s+DATABASES', sql, re.IGNORECASE):
            databases = self.metadata.list_databases()
            data = [[db["name"], db["created_at"]] for db in databases]
            return {"success": True, "data": data, "columns": ["name", "created_on"], "rowcount": len(data)}
        
        # SHOW SCHEMAS
        match = re.match(r'SHOW\s+SCHEMAS(?:\s+IN\s+(?:DATABASE\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?', sql, re.IGNORECASE)
        if match:
            db_name = match.group(1).upper() if match.group(1) else self.current_database
            try:
                schemas = self.metadata.list_schemas(db_name)
                data = [[s["name"], s["created_at"]] for s in schemas]
                return {"success": True, "data": data, "columns": ["name", "created_on"], "rowcount": len(data)}
            except ValueError as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # SHOW TABLES
        match = re.match(r'SHOW\s+TABLES(?:\s+IN\s+(?:SCHEMA\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?', sql, re.IGNORECASE)
        if match:
            schema_name = match.group(1).upper() if match.group(1) else self.current_schema
            try:
                tables = self.metadata.list_tables(self.current_database, schema_name)
                data = [[t["name"], t["created_at"], t.get("row_count", 0)] for t in tables]
                return {"success": True, "data": data, "columns": ["name", "created_on", "rows"], "rowcount": len(data)}
            except ValueError as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # DESCRIBE TABLE
        match = re.match(r'(?:DESCRIBE|DESC)\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            table_name = match.group(1).upper()
            table_info = self.metadata.get_table_info(self.current_database, self.current_schema, table_name)
            if table_info:
                data = [[col["name"], col["type"], col.get("nullable", "Y")] for col in table_info["columns"]]
                return {"success": True, "data": data, "columns": ["name", "type", "nullable"], "rowcount": len(data)}
            else:
                return {"success": False, "error": f"Table '{table_name}' does not exist",
                        "data": [], "columns": [], "rowcount": 0}
        
        # SET variable
        match = re.match(r'SET\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(.+)', sql, re.IGNORECASE)
        if match:
            var_name = match.group(1).upper()
            var_value = match.group(2).strip()
            self.session_vars[var_name] = var_value
            return {"success": True, "data": [], "columns": [], "rowcount": 0,
                    "message": f"Variable {var_name} set"}
        
        # SELECT CURRENT_DATABASE()
        if re.match(r'SELECT\s+CURRENT_DATABASE\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [[self.current_database]], 
                    "columns": ["CURRENT_DATABASE()"], "rowcount": 1}
        
        # SELECT CURRENT_SCHEMA()
        if re.match(r'SELECT\s+CURRENT_SCHEMA\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [[self.current_schema]], 
                    "columns": ["CURRENT_SCHEMA()"], "rowcount": 1}
        
        # SELECT CURRENT_WAREHOUSE()
        if re.match(r'SELECT\s+CURRENT_WAREHOUSE\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [[self.current_warehouse]], 
                    "columns": ["CURRENT_WAREHOUSE()"], "rowcount": 1}
        
        # SELECT CURRENT_ROLE()
        if re.match(r'SELECT\s+CURRENT_ROLE\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [[self.current_role]], 
                    "columns": ["CURRENT_ROLE()"], "rowcount": 1}
        
        # SELECT CURRENT_USER()
        if re.match(r'SELECT\s+CURRENT_USER\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [["SNOWGLOBE_USER"]], 
                    "columns": ["CURRENT_USER()"], "rowcount": 1}
        
        # SELECT CURRENT_ACCOUNT()
        if re.match(r'SELECT\s+CURRENT_ACCOUNT\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [["SNOWGLOBE_ACCOUNT"]], 
                    "columns": ["CURRENT_ACCOUNT()"], "rowcount": 1}
        
        # SHOW DROPPED TABLES
        if re.match(r'SHOW\s+DROPPED\s+TABLES', sql, re.IGNORECASE):
            tables = self.metadata.list_dropped_tables(self.current_database, self.current_schema)
            data = [[t["name"], t["database"], t["schema"], t["dropped_at"]] for t in tables]
            return {"success": True, "data": data, 
                    "columns": ["name", "database", "schema", "dropped_on"], "rowcount": len(data)}
        
        # SHOW DROPPED SCHEMAS
        if re.match(r'SHOW\s+DROPPED\s+SCHEMAS', sql, re.IGNORECASE):
            schemas = self.metadata.list_dropped_schemas(self.current_database)
            data = [[s["name"], s["database"], s["dropped_at"]] for s in schemas]
            return {"success": True, "data": data, 
                    "columns": ["name", "database", "dropped_on"], "rowcount": len(data)}
        
        # SHOW DROPPED DATABASES
        if re.match(r'SHOW\s+DROPPED\s+DATABASES', sql, re.IGNORECASE):
            databases = self.metadata.list_dropped_databases()
            data = [[db["name"], db["dropped_at"]] for db in databases]
            return {"success": True, "data": data, 
                    "columns": ["name", "dropped_on"], "rowcount": len(data)}
        
        # SHOW VIEWS
        match = re.match(r'SHOW\s+VIEWS(?:\s+IN\s+(?:SCHEMA\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?', sql, re.IGNORECASE)
        if match:
            schema_name = match.group(1).upper() if match.group(1) else self.current_schema
            try:
                views = self.metadata.list_views(self.current_database, schema_name)
                data = [[v["name"], v["created_at"]] for v in views]
                return {"success": True, "data": data, "columns": ["name", "created_on"], "rowcount": len(data)}
            except ValueError as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # SHOW WAREHOUSES
        if re.match(r'SHOW\s+WAREHOUSES', sql, re.IGNORECASE):
            data = [[self.current_warehouse, "STARTED", "STANDARD", "X-SMALL"]]
            return {"success": True, "data": data, 
                    "columns": ["name", "state", "type", "size"], "rowcount": len(data)}
        
        # SHOW ROLES
        if re.match(r'SHOW\s+ROLES', sql, re.IGNORECASE):
            data = [["ACCOUNTADMIN"], ["SYSADMIN"], ["USERADMIN"], ["PUBLIC"], [self.current_role]]
            return {"success": True, "data": data, "columns": ["name"], "rowcount": len(data)}
        
        # SHOW USERS
        if re.match(r'SHOW\s+USERS', sql, re.IGNORECASE):
            data = [["SNOWGLOBE_USER", "SNOWGLOBE_USER@LOCAL", "ACTIVE"]]
            return {"success": True, "data": data, 
                    "columns": ["name", "email", "status"], "rowcount": len(data)}
        
        # SHOW GRANTS
        if re.match(r'SHOW\s+GRANTS', sql, re.IGNORECASE):
            data = [["USAGE", "DATABASE", self.current_database, self.current_role]]
            return {"success": True, "data": data, 
                    "columns": ["privilege", "object_type", "name", "grantee"], "rowcount": len(data)}
        
        # SHOW PARAMETERS
        if re.match(r'SHOW\s+PARAMETERS', sql, re.IGNORECASE):
            data = [
                ["TIMEZONE", "America/Los_Angeles", "SESSION"],
                ["QUERY_TAG", "", "SESSION"],
                ["DATE_OUTPUT_FORMAT", "YYYY-MM-DD", "SESSION"],
            ]
            return {"success": True, "data": data, 
                    "columns": ["key", "value", "level"], "rowcount": len(data)}
        
        # SHOW COLUMNS
        match = re.match(r'SHOW\s+COLUMNS\s+IN\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            table_name = match.group(1).upper()
            table_info = self.metadata.get_table_info(self.current_database, self.current_schema, table_name)
            if table_info:
                data = [[col["name"], col["type"], col.get("nullable", "Y")] for col in table_info["columns"]]
                return {"success": True, "data": data, "columns": ["column_name", "data_type", "is_nullable"], "rowcount": len(data)}
            else:
                return {"success": False, "error": f"Table '{table_name}' does not exist",
                        "data": [], "columns": [], "rowcount": 0}
        
        # SELECT CURRENT_SESSION()
        if re.match(r'SELECT\s+CURRENT_SESSION\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [["snowglobe_session_001"]], 
                    "columns": ["CURRENT_SESSION()"], "rowcount": 1}
        
        # SELECT CURRENT_REGION()
        if re.match(r'SELECT\s+CURRENT_REGION\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [["LOCAL"]], 
                    "columns": ["CURRENT_REGION()"], "rowcount": 1}
        
        # SELECT CURRENT_VERSION()
        if re.match(r'SELECT\s+CURRENT_VERSION\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [["0.1.0"]], 
                    "columns": ["CURRENT_VERSION()"], "rowcount": 1}
        
        # SELECT CURRENT_CLIENT()
        if re.match(r'SELECT\s+CURRENT_CLIENT\s*\(\s*\)', sql, re.IGNORECASE):
            return {"success": True, "data": [["Snowglobe"]], 
                    "columns": ["CURRENT_CLIENT()"], "rowcount": 1}
        
        # SELECT $variable
        match = re.match(r'SELECT\s+\$([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            var_name = match.group(1).upper()
            if var_name in self.session_vars:
                return {"success": True, "data": [[self.session_vars[var_name]]], 
                        "columns": [f"${var_name}"], "rowcount": 1}
            else:
                return {"success": False, "error": f"Variable '{var_name}' not found",
                        "data": [], "columns": [], "rowcount": 0}
        
        # UNSET variable
        match = re.match(r'UNSET\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            var_name = match.group(1).upper()
            if var_name in self.session_vars:
                del self.session_vars[var_name]
            return {"success": True, "data": [], "columns": [], "rowcount": 0,
                    "message": f"Variable {var_name} unset"}
        
        # SELECT FROM INFORMATION_SCHEMA
        match = re.match(
            r'SELECT\s+(.+?)\s+FROM\s+(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?INFORMATION_SCHEMA\.([a-zA-Z_][a-zA-Z0-9_]*)',
            sql, re.IGNORECASE | re.DOTALL
        )
        if match:
            columns_clause = match.group(1)
            database = match.group(2) or self.current_database
            view_name = match.group(3).upper()
            
            # Parse WHERE clause for filters
            where_match = re.search(r'WHERE\s+(.+?)(?:ORDER|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
            filters = {}
            if where_match:
                where_clause = where_match.group(1)
                # Simple filter parsing for equality conditions
                for filter_match in re.finditer(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*'([^']*)'", where_clause):
                    filters[filter_match.group(1).upper()] = filter_match.group(2)
            
            result = self.information_schema.query_information_schema(
                view_name, 
                database=database,
                schema=filters.get('TABLE_SCHEMA') or filters.get('SCHEMA_NAME'),
                table=filters.get('TABLE_NAME'),
                filters=filters
            )
            
            if result['success'] and result['data']:
                # Handle SELECT * vs specific columns
                if columns_clause.strip() == '*':
                    return result
                else:
                    # Filter to requested columns
                    requested_cols = [c.strip().upper() for c in columns_clause.split(',')]
                    col_indices = []
                    new_columns = []
                    for req_col in requested_cols:
                        if req_col in result['columns']:
                            idx = result['columns'].index(req_col)
                            col_indices.append(idx)
                            new_columns.append(req_col)
                    
                    if col_indices:
                        new_data = [[row[i] for i in col_indices] for row in result['data']]
                        return {
                            'success': True,
                            'data': new_data,
                            'columns': new_columns,
                            'rowcount': len(new_data)
                        }
            
            return result
        
        # LIST @stage (stage operations)
        match = re.match(r'LIST\s+@([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            stage_name = match.group(1).upper()
            # Return empty list for now as stage storage is not fully implemented
            return {"success": True, "data": [], 
                    "columns": ["name", "size", "md5", "last_modified"], "rowcount": 0}
        
        # REMOVE @stage/file
        match = re.match(r'REMOVE\s+@([a-zA-Z_][a-zA-Z0-9_/]*)', sql, re.IGNORECASE)
        if match:
            return {"success": True, "data": [], "columns": [], "rowcount": 0,
                    "message": "Files removed"}
        
        return None
    
    def _handle_ddl(self, sql: str) -> Optional[Dict[str, Any]]:
        """Handle DDL statements"""
        sql_upper = sql.upper().strip()
        
        # CREATE DATABASE
        match = re.match(r'CREATE\s+(?:OR\s+REPLACE\s+)?DATABASE\s+(IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', 
                         sql, re.IGNORECASE)
        if match:
            if_not_exists = bool(match.group(1))
            db_name = match.group(2).upper()
            try:
                self.metadata.create_database(db_name, if_not_exists)
                self._ensure_schema_exists(db_name, "PUBLIC")
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Database {db_name} created"}
            except ValueError as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # DROP DATABASE
        match = re.match(r'DROP\s+DATABASE\s+(IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            if_exists = bool(match.group(1))
            db_name = match.group(2).upper()
            try:
                self.metadata.drop_database(db_name, if_exists)
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Database {db_name} dropped"}
            except ValueError as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # CREATE SCHEMA
        match = re.match(r'CREATE\s+(?:OR\s+REPLACE\s+)?SCHEMA\s+(IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', 
                         sql, re.IGNORECASE)
        if match:
            if_not_exists = bool(match.group(1))
            schema_name = match.group(2).upper()
            try:
                self.metadata.create_schema(self.current_database, schema_name, if_not_exists)
                self._ensure_schema_exists(self.current_database, schema_name)
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Schema {schema_name} created"}
            except ValueError as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # DROP SCHEMA
        match = re.match(r'DROP\s+SCHEMA\s+(IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*(CASCADE)?', 
                         sql, re.IGNORECASE)
        if match:
            if_exists = bool(match.group(1))
            schema_name = match.group(2).upper()
            cascade = bool(match.group(3))
            try:
                self.metadata.drop_schema(self.current_database, schema_name, if_exists, cascade)
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Schema {schema_name} dropped"}
            except ValueError as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # CREATE TABLE
        match = re.match(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|TEMP\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.+)\)', 
                         sql, re.IGNORECASE | re.DOTALL)
        if match:
            if_not_exists = bool(match.group(1))
            table_name = match.group(2).upper()
            columns_def = match.group(3)
            
            # Parse columns
            columns = self._parse_column_definitions(columns_def)
            
            try:
                # Register in metadata
                self.metadata.register_table(self.current_database, self.current_schema, 
                                            table_name, columns, if_not_exists)
                
                # Create actual table in DuckDB
                self._ensure_schema_exists(self.current_database, self.current_schema)
                translated_sql = self._prepare_sql(sql)
                self.conn.execute(translated_sql)
                
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Table {table_name} created"}
            except Exception as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # DROP TABLE
        match = re.match(r'DROP\s+TABLE\s+(IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            if_exists = bool(match.group(1))
            table_name = match.group(2).upper()
            
            try:
                # Remove from metadata
                self.metadata.drop_table(self.current_database, self.current_schema, 
                                         table_name, if_exists)
                
                # Drop from DuckDB
                translated_sql = self._prepare_sql(sql)
                self.conn.execute(translated_sql)
                
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Table {table_name} dropped"}
            except Exception as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # CREATE VIEW
        match = re.match(r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s+(.+)', 
                         sql, re.IGNORECASE | re.DOTALL)
        if match:
            if_not_exists = bool(match.group(1))
            view_name = match.group(2).upper()
            definition = match.group(3)
            
            try:
                self.metadata.register_view(self.current_database, self.current_schema,
                                           view_name, definition, if_not_exists)
                
                # Create in DuckDB
                translated_sql = self._prepare_sql(sql)
                self.conn.execute(translated_sql)
                
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"View {view_name} created"}
            except Exception as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # DROP VIEW
        match = re.match(r'DROP\s+VIEW\s+(IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            if_exists = bool(match.group(1))
            view_name = match.group(2).upper()
            
            try:
                self.metadata.drop_view(self.current_database, self.current_schema,
                                       view_name, if_exists)
                
                translated_sql = self._prepare_sql(sql)
                self.conn.execute(translated_sql)
                
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"View {view_name} dropped"}
            except Exception as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # UNDROP DATABASE
        match = re.match(r'UNDROP\s+DATABASE\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            db_name = match.group(1).upper()
            try:
                self.metadata.undrop_database(db_name)
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Database {db_name} restored"}
            except ValueError as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # UNDROP SCHEMA
        match = re.match(r'UNDROP\s+SCHEMA\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            schema_name = match.group(1).upper()
            try:
                self.metadata.undrop_schema(self.current_database, schema_name)
                self._ensure_schema_exists(self.current_database, schema_name)
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Schema {schema_name} restored"}
            except ValueError as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # UNDROP TABLE
        match = re.match(r'UNDROP\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            table_name = match.group(1).upper()
            try:
                # Get table info before undrop to recreate it
                dropped_key = f"{self.current_database}.{self.current_schema}.{table_name}"
                if dropped_key in self.metadata._metadata["dropped"]["tables"]:
                    table_info = self.metadata._metadata["dropped"]["tables"][dropped_key]
                    
                    # Recreate table in DuckDB
                    columns_sql = ", ".join([
                        f"{col['name']} {col['type']}"
                        for col in table_info["columns"]
                    ])
                    duck_schema = self._get_duckdb_schema(self.current_database, self.current_schema)
                    create_sql = f"CREATE TABLE IF NOT EXISTS {duck_schema}.{table_name} ({columns_sql})"
                    self.conn.execute(create_sql)
                
                self.metadata.undrop_table(self.current_database, self.current_schema, table_name)
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Table {table_name} restored"}
            except Exception as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # UNDROP VIEW
        match = re.match(r'UNDROP\s+VIEW\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            view_name = match.group(1).upper()
            try:
                # Get view definition before undrop
                dropped_key = f"{self.current_database}.{self.current_schema}.{view_name}"
                if dropped_key in self.metadata._metadata["dropped"]["views"]:
                    view_info = self.metadata._metadata["dropped"]["views"][dropped_key]
                    
                    # Recreate view in DuckDB
                    duck_schema = self._get_duckdb_schema(self.current_database, self.current_schema)
                    create_sql = f"CREATE VIEW {duck_schema}.{view_name} AS {view_info['definition']}"
                    self.conn.execute(create_sql)
                
                self.metadata.undrop_view(self.current_database, self.current_schema, view_name)
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"View {view_name} restored"}
            except Exception as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # TRUNCATE TABLE (similar to DELETE but more efficient)
        match = re.match(r'TRUNCATE\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            table_name = match.group(1).upper()
            try:
                if not self.metadata.table_exists(self.current_database, self.current_schema, table_name):
                    return {"success": False, "error": f"Table '{table_name}' does not exist",
                            "data": [], "columns": [], "rowcount": 0}
                
                duck_schema = self._get_duckdb_schema(self.current_database, self.current_schema)
                self.conn.execute(f"DELETE FROM {duck_schema}.{table_name}")
                
                # Update table stats
                self.metadata.update_table_stats(self.current_database, self.current_schema, 
                                                  table_name, row_count=0)
                
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Table {table_name} truncated"}
            except Exception as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # ALTER TABLE RENAME
        match = re.match(r'ALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+RENAME\s+TO\s+([a-zA-Z_][a-zA-Z0-9_]*)', 
                         sql, re.IGNORECASE)
        if match:
            old_name = match.group(1).upper()
            new_name = match.group(2).upper()
            try:
                if not self.metadata.table_exists(self.current_database, self.current_schema, old_name):
                    return {"success": False, "error": f"Table '{old_name}' does not exist",
                            "data": [], "columns": [], "rowcount": 0}
                
                if self.metadata.table_exists(self.current_database, self.current_schema, new_name):
                    return {"success": False, "error": f"Table '{new_name}' already exists",
                            "data": [], "columns": [], "rowcount": 0}
                
                # Rename in DuckDB
                duck_schema = self._get_duckdb_schema(self.current_database, self.current_schema)
                self.conn.execute(f"ALTER TABLE {duck_schema}.{old_name} RENAME TO {new_name}")
                
                # Update metadata
                tables = self.metadata._metadata["databases"][self.current_database]["schemas"][self.current_schema]["tables"]
                tables[new_name] = tables[old_name]
                tables[new_name]["name"] = new_name
                del tables[old_name]
                self.metadata._save_metadata()
                
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Table {old_name} renamed to {new_name}"}
            except Exception as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        # CLONE TABLE (Snowflake feature)
        match = re.match(r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+CLONE\s+([a-zA-Z_][a-zA-Z0-9_]*)', 
                         sql, re.IGNORECASE)
        if match:
            new_table = match.group(1).upper()
            source_table = match.group(2).upper()
            try:
                if not self.metadata.table_exists(self.current_database, self.current_schema, source_table):
                    return {"success": False, "error": f"Source table '{source_table}' does not exist",
                            "data": [], "columns": [], "rowcount": 0}
                
                # Get source table info
                source_info = self.metadata.get_table_info(self.current_database, self.current_schema, source_table)
                
                # Register new table with same columns
                self.metadata.register_table(self.current_database, self.current_schema, 
                                            new_table, source_info["columns"], if_not_exists=False)
                
                # Clone in DuckDB
                duck_schema = self._get_duckdb_schema(self.current_database, self.current_schema)
                self.conn.execute(f"CREATE TABLE {duck_schema}.{new_table} AS SELECT * FROM {duck_schema}.{source_table}")
                
                return {"success": True, "data": [], "columns": [], "rowcount": 0,
                        "message": f"Table {new_table} cloned from {source_table}"}
            except Exception as e:
                return {"success": False, "error": str(e), "data": [], "columns": [], "rowcount": 0}
        
        return None
    
    def _parse_column_definitions(self, columns_str: str) -> List[Dict]:
        """Parse column definitions from CREATE TABLE"""
        columns = []
        
        # Simple parsing - split by comma (not inside parentheses)
        parts = []
        current = ''
        depth = 0
        
        for char in columns_str:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                parts.append(current.strip())
                current = ''
            else:
                current += char
        
        if current.strip():
            parts.append(current.strip())
        
        for part in parts:
            # Skip constraints
            part_upper = part.upper().strip()
            if (part_upper.startswith('PRIMARY') or part_upper.startswith('FOREIGN') or
                part_upper.startswith('UNIQUE') or part_upper.startswith('CHECK') or
                part_upper.startswith('CONSTRAINT')):
                continue
            
            # Parse column: name type [constraints]
            tokens = part.split()
            if len(tokens) >= 2:
                col_name = tokens[0].upper()
                col_type = tokens[1].upper()
                
                # Check for precision/scale
                if '(' in part:
                    type_match = re.search(r'(\w+)\s*(\([^)]+\))', part)
                    if type_match:
                        col_type = type_match.group(1).upper() + type_match.group(2)
                
                nullable = 'NOT NULL' not in part.upper()
                
                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "nullable": "Y" if nullable else "N"
                })
        
        return columns
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
    
    def get_context(self) -> Dict[str, str]:
        """Get current session context"""
        return {
            "database": self.current_database,
            "schema": self.current_schema,
            "warehouse": self.current_warehouse,
            "role": self.current_role
        }
    
    def set_context(self, database: str = None, schema: str = None, 
                    warehouse: str = None, role: str = None):
        """Set session context"""
        if database:
            self.current_database = database.upper()
        if schema:
            self.current_schema = schema.upper()
        if warehouse:
            self.current_warehouse = warehouse.upper()
        if role:
            self.current_role = role.upper()
        
        self._ensure_schema_exists(self.current_database, self.current_schema)
