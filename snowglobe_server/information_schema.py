"""
Information Schema - Snowflake-compatible INFORMATION_SCHEMA views
"""

from typing import Dict, List, Any, Optional
from datetime import datetime


class InformationSchemaBuilder:
    """
    Builds and manages Snowflake-compatible INFORMATION_SCHEMA views.
    
    Snowflake's INFORMATION_SCHEMA contains views that provide metadata about
    database objects. This module creates virtual views that query our metadata.
    """
    
    # Standard INFORMATION_SCHEMA tables/views in Snowflake
    SCHEMA_VIEWS = [
        'APPLICABLE_ROLES',
        'COLUMNS',
        'DATABASES',
        'ENABLED_ROLES',
        'EXTERNAL_TABLES',
        'FILE_FORMATS',
        'FUNCTIONS',
        'INFORMATION_SCHEMA_CATALOG_NAME',
        'LOAD_HISTORY',
        'OBJECT_PRIVILEGES',
        'PACKAGES',
        'PIPES',
        'PROCEDURES',
        'REFERENTIAL_CONSTRAINTS',
        'REPLICATION_DATABASES',
        'REPLICATION_GROUPS',
        'SCHEMATA',
        'SEQUENCES',
        'STAGES',
        'TABLE_CONSTRAINTS',
        'TABLE_PRIVILEGES',
        'TABLE_STORAGE_METRICS',
        'TABLES',
        'USAGE_PRIVILEGES',
        'VIEWS',
    ]
    
    def __init__(self, metadata_store):
        """
        Initialize with a MetadataStore instance.
        
        Args:
            metadata_store: MetadataStore instance for accessing database metadata
        """
        self.metadata = metadata_store
    
    def get_databases(self) -> List[Dict[str, Any]]:
        """
        Get INFORMATION_SCHEMA.DATABASES view data.
        
        Returns:
            List of database records with Snowflake-compatible columns
        """
        databases = self.metadata.list_databases()
        result = []
        
        for db in databases:
            result.append({
                'DATABASE_NAME': db['name'],
                'DATABASE_OWNER': 'ACCOUNTADMIN',
                'IS_TRANSIENT': 'NO',
                'COMMENT': None,
                'CREATED': db['created_at'],
                'LAST_ALTERED': db['created_at'],
                'RETENTION_TIME': 1,
                'TYPE': 'STANDARD',
                'IS_DEFAULT': 'YES' if db['name'] == 'SNOWGLOBE' else 'NO',
                'OPTIONS': None,
            })
        
        return result
    
    def get_schemata(self, database: str = None) -> List[Dict[str, Any]]:
        """
        Get INFORMATION_SCHEMA.SCHEMATA view data.
        
        Args:
            database: Optional database filter
            
        Returns:
            List of schema records with Snowflake-compatible columns
        """
        result = []
        
        if database:
            databases = [{'name': database.upper()}]
        else:
            databases = self.metadata.list_databases()
        
        for db in databases:
            try:
                schemas = self.metadata.list_schemas(db['name'])
                for schema in schemas:
                    result.append({
                        'CATALOG_NAME': db['name'],
                        'SCHEMA_NAME': schema['name'],
                        'SCHEMA_OWNER': 'ACCOUNTADMIN',
                        'IS_TRANSIENT': 'NO',
                        'IS_MANAGED_ACCESS': 'NO',
                        'RETENTION_TIME': 1,
                        'DEFAULT_CHARACTER_SET_CATALOG': None,
                        'DEFAULT_CHARACTER_SET_SCHEMA': None,
                        'DEFAULT_CHARACTER_SET_NAME': None,
                        'SQL_PATH': None,
                        'CREATED': schema['created_at'],
                        'LAST_ALTERED': schema['created_at'],
                        'COMMENT': None,
                        'OPTIONS': None,
                    })
            except ValueError:
                continue
        
        return result
    
    def get_tables(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """
        Get INFORMATION_SCHEMA.TABLES view data.
        
        Args:
            database: Optional database filter
            schema: Optional schema filter
            
        Returns:
            List of table records with Snowflake-compatible columns
        """
        result = []
        
        if database:
            databases = [{'name': database.upper()}]
        else:
            databases = self.metadata.list_databases()
        
        for db in databases:
            try:
                schemas = self.metadata.list_schemas(db['name'])
                for sch in schemas:
                    if schema and sch['name'].upper() != schema.upper():
                        continue
                    
                    # Skip INFORMATION_SCHEMA itself (it's virtual)
                    if sch['name'] == 'INFORMATION_SCHEMA':
                        # Add virtual INFORMATION_SCHEMA tables
                        for view_name in self.SCHEMA_VIEWS:
                            result.append({
                                'TABLE_CATALOG': db['name'],
                                'TABLE_SCHEMA': 'INFORMATION_SCHEMA',
                                'TABLE_NAME': view_name,
                                'TABLE_OWNER': 'ACCOUNTADMIN',
                                'TABLE_TYPE': 'VIEW',
                                'IS_TRANSIENT': 'NO',
                                'CLUSTERING_KEY': None,
                                'ROW_COUNT': 0,
                                'BYTES': 0,
                                'RETENTION_TIME': 1,
                                'SELF_REFERENCING_COLUMN_NAME': None,
                                'REFERENCE_GENERATION': None,
                                'USER_DEFINED_TYPE_CATALOG': None,
                                'USER_DEFINED_TYPE_SCHEMA': None,
                                'USER_DEFINED_TYPE_NAME': None,
                                'IS_INSERTABLE_INTO': 'NO',
                                'IS_TYPED': 'NO',
                                'COMMIT_ACTION': None,
                                'CREATED': db.get('created_at', datetime.utcnow().isoformat()),
                                'LAST_ALTERED': db.get('created_at', datetime.utcnow().isoformat()),
                                'AUTO_CLUSTERING_ON': 'NO',
                                'COMMENT': f'Snowflake Information Schema {view_name} view',
                            })
                        continue
                    
                    try:
                        tables = self.metadata.list_tables(db['name'], sch['name'])
                        for table in tables:
                            result.append({
                                'TABLE_CATALOG': db['name'],
                                'TABLE_SCHEMA': sch['name'],
                                'TABLE_NAME': table['name'],
                                'TABLE_OWNER': 'ACCOUNTADMIN',
                                'TABLE_TYPE': 'BASE TABLE',
                                'IS_TRANSIENT': 'NO',
                                'CLUSTERING_KEY': None,
                                'ROW_COUNT': table.get('row_count', 0),
                                'BYTES': table.get('bytes', 0),
                                'RETENTION_TIME': 1,
                                'SELF_REFERENCING_COLUMN_NAME': None,
                                'REFERENCE_GENERATION': None,
                                'USER_DEFINED_TYPE_CATALOG': None,
                                'USER_DEFINED_TYPE_SCHEMA': None,
                                'USER_DEFINED_TYPE_NAME': None,
                                'IS_INSERTABLE_INTO': 'YES',
                                'IS_TYPED': 'NO',
                                'COMMIT_ACTION': None,
                                'CREATED': table['created_at'],
                                'LAST_ALTERED': table['created_at'],
                                'AUTO_CLUSTERING_ON': 'NO',
                                'COMMENT': None,
                            })
                        
                        # Also include views
                        views = self.metadata.list_views(db['name'], sch['name'])
                        for view in views:
                            result.append({
                                'TABLE_CATALOG': db['name'],
                                'TABLE_SCHEMA': sch['name'],
                                'TABLE_NAME': view['name'],
                                'TABLE_OWNER': 'ACCOUNTADMIN',
                                'TABLE_TYPE': 'VIEW',
                                'IS_TRANSIENT': 'NO',
                                'CLUSTERING_KEY': None,
                                'ROW_COUNT': None,
                                'BYTES': None,
                                'RETENTION_TIME': 1,
                                'SELF_REFERENCING_COLUMN_NAME': None,
                                'REFERENCE_GENERATION': None,
                                'USER_DEFINED_TYPE_CATALOG': None,
                                'USER_DEFINED_TYPE_SCHEMA': None,
                                'USER_DEFINED_TYPE_NAME': None,
                                'IS_INSERTABLE_INTO': 'NO',
                                'IS_TYPED': 'NO',
                                'COMMIT_ACTION': None,
                                'CREATED': view['created_at'],
                                'LAST_ALTERED': view['created_at'],
                                'AUTO_CLUSTERING_ON': 'NO',
                                'COMMENT': None,
                            })
                    except ValueError:
                        continue
            except ValueError:
                continue
        
        return result
    
    def get_columns(self, database: str = None, schema: str = None, 
                    table: str = None) -> List[Dict[str, Any]]:
        """
        Get INFORMATION_SCHEMA.COLUMNS view data.
        
        Args:
            database: Optional database filter
            schema: Optional schema filter
            table: Optional table filter
            
        Returns:
            List of column records with Snowflake-compatible columns
        """
        result = []
        
        if database:
            databases = [{'name': database.upper()}]
        else:
            databases = self.metadata.list_databases()
        
        for db in databases:
            try:
                schemas = self.metadata.list_schemas(db['name'])
                for sch in schemas:
                    if schema and sch['name'].upper() != schema.upper():
                        continue
                    
                    # Skip INFORMATION_SCHEMA (virtual tables)
                    if sch['name'] == 'INFORMATION_SCHEMA':
                        continue
                    
                    try:
                        tables = self.metadata.list_tables(db['name'], sch['name'])
                        for tbl in tables:
                            if table and tbl['name'].upper() != table.upper():
                                continue
                            
                            for idx, col in enumerate(tbl.get('columns', []), 1):
                                data_type = col['type'].upper()
                                numeric_precision = None
                                numeric_scale = None
                                character_max_length = None
                                
                                # Parse type for precision/scale
                                if 'NUMBER' in data_type or 'DECIMAL' in data_type or 'NUMERIC' in data_type:
                                    import re
                                    match = re.search(r'\((\d+)(?:,\s*(\d+))?\)', data_type)
                                    if match:
                                        numeric_precision = int(match.group(1))
                                        numeric_scale = int(match.group(2)) if match.group(2) else 0
                                    else:
                                        numeric_precision = 38
                                        numeric_scale = 0
                                elif 'VARCHAR' in data_type or 'STRING' in data_type or 'TEXT' in data_type:
                                    import re
                                    match = re.search(r'\((\d+)\)', data_type)
                                    if match:
                                        character_max_length = int(match.group(1))
                                    else:
                                        character_max_length = 16777216  # Snowflake max
                                elif 'INTEGER' in data_type or 'INT' in data_type or 'BIGINT' in data_type:
                                    numeric_precision = 38
                                    numeric_scale = 0
                                elif 'FLOAT' in data_type or 'DOUBLE' in data_type or 'REAL' in data_type:
                                    numeric_precision = None
                                    numeric_scale = None
                                
                                result.append({
                                    'TABLE_CATALOG': db['name'],
                                    'TABLE_SCHEMA': sch['name'],
                                    'TABLE_NAME': tbl['name'],
                                    'COLUMN_NAME': col['name'],
                                    'ORDINAL_POSITION': idx,
                                    'COLUMN_DEFAULT': col.get('default'),
                                    'IS_NULLABLE': 'YES' if col.get('nullable', 'Y') == 'Y' else 'NO',
                                    'DATA_TYPE': data_type.split('(')[0],  # Base type without params
                                    'CHARACTER_MAXIMUM_LENGTH': character_max_length,
                                    'CHARACTER_OCTET_LENGTH': character_max_length * 4 if character_max_length else None,
                                    'NUMERIC_PRECISION': numeric_precision,
                                    'NUMERIC_PRECISION_RADIX': 10 if numeric_precision else None,
                                    'NUMERIC_SCALE': numeric_scale,
                                    'DATETIME_PRECISION': 9 if 'TIMESTAMP' in data_type or 'TIME' in data_type else None,
                                    'INTERVAL_TYPE': None,
                                    'INTERVAL_PRECISION': None,
                                    'CHARACTER_SET_CATALOG': None,
                                    'CHARACTER_SET_SCHEMA': None,
                                    'CHARACTER_SET_NAME': 'UTF8' if character_max_length else None,
                                    'COLLATION_CATALOG': None,
                                    'COLLATION_SCHEMA': None,
                                    'COLLATION_NAME': None,
                                    'DOMAIN_CATALOG': None,
                                    'DOMAIN_SCHEMA': None,
                                    'DOMAIN_NAME': None,
                                    'UDT_CATALOG': None,
                                    'UDT_SCHEMA': None,
                                    'UDT_NAME': None,
                                    'SCOPE_CATALOG': None,
                                    'SCOPE_SCHEMA': None,
                                    'SCOPE_NAME': None,
                                    'MAXIMUM_CARDINALITY': None,
                                    'IS_SELF_REFERENCING': 'NO',
                                    'IS_IDENTITY': col.get('is_identity', 'NO'),
                                    'IDENTITY_GENERATION': None,
                                    'IDENTITY_START': None,
                                    'IDENTITY_INCREMENT': None,
                                    'IDENTITY_MAXIMUM': None,
                                    'IDENTITY_MINIMUM': None,
                                    'IDENTITY_CYCLE': 'NO',
                                    'COMMENT': col.get('comment'),
                                })
                    except ValueError:
                        continue
            except ValueError:
                continue
        
        return result
    
    def get_views(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """
        Get INFORMATION_SCHEMA.VIEWS view data.
        
        Args:
            database: Optional database filter
            schema: Optional schema filter
            
        Returns:
            List of view records with Snowflake-compatible columns
        """
        result = []
        
        if database:
            databases = [{'name': database.upper()}]
        else:
            databases = self.metadata.list_databases()
        
        for db in databases:
            try:
                schemas = self.metadata.list_schemas(db['name'])
                for sch in schemas:
                    if schema and sch['name'].upper() != schema.upper():
                        continue
                    
                    if sch['name'] == 'INFORMATION_SCHEMA':
                        continue
                    
                    try:
                        views = self.metadata.list_views(db['name'], sch['name'])
                        for view in views:
                            result.append({
                                'TABLE_CATALOG': db['name'],
                                'TABLE_SCHEMA': sch['name'],
                                'TABLE_NAME': view['name'],
                                'TABLE_OWNER': 'ACCOUNTADMIN',
                                'VIEW_DEFINITION': view.get('definition', ''),
                                'CHECK_OPTION': 'NONE',
                                'IS_UPDATABLE': 'NO',
                                'INSERTABLE_INTO': 'NO',
                                'IS_SECURE': 'NO',
                                'CREATED': view['created_at'],
                                'LAST_ALTERED': view['created_at'],
                                'COMMENT': None,
                            })
                    except ValueError:
                        continue
            except ValueError:
                continue
        
        return result
    
    def get_applicable_roles(self) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.APPLICABLE_ROLES view data."""
        return [
            {'GRANTEE': 'SNOWGLOBE_USER', 'ROLE_NAME': 'ACCOUNTADMIN', 'ROLE_OWNER': 'ACCOUNTADMIN', 'IS_GRANTABLE': 'YES'},
            {'GRANTEE': 'SNOWGLOBE_USER', 'ROLE_NAME': 'SYSADMIN', 'ROLE_OWNER': 'ACCOUNTADMIN', 'IS_GRANTABLE': 'YES'},
            {'GRANTEE': 'SNOWGLOBE_USER', 'ROLE_NAME': 'USERADMIN', 'ROLE_OWNER': 'ACCOUNTADMIN', 'IS_GRANTABLE': 'YES'},
            {'GRANTEE': 'SNOWGLOBE_USER', 'ROLE_NAME': 'SECURITYADMIN', 'ROLE_OWNER': 'ACCOUNTADMIN', 'IS_GRANTABLE': 'YES'},
            {'GRANTEE': 'SNOWGLOBE_USER', 'ROLE_NAME': 'PUBLIC', 'ROLE_OWNER': 'ACCOUNTADMIN', 'IS_GRANTABLE': 'YES'},
        ]
    
    def get_enabled_roles(self) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.ENABLED_ROLES view data."""
        return [
            {'ROLE_NAME': 'ACCOUNTADMIN', 'ROLE_OWNER': 'ACCOUNTADMIN'},
        ]
    
    def get_sequences(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.SEQUENCES view data."""
        # For now, return empty - sequences support can be added later
        return []
    
    def get_stages(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.STAGES view data."""
        # Return a default internal stage
        return [
            {
                'STAGE_CATALOG': database or 'SNOWGLOBE',
                'STAGE_SCHEMA': schema or 'PUBLIC',
                'STAGE_NAME': '~',
                'STAGE_URL': 'file:///',
                'STAGE_REGION': None,
                'STAGE_TYPE': 'USER',
                'STAGE_OWNER': 'ACCOUNTADMIN',
                'COMMENT': 'User stage',
                'CREATED': datetime.utcnow().isoformat(),
                'LAST_ALTERED': datetime.utcnow().isoformat(),
            }
        ]
    
    def get_file_formats(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.FILE_FORMATS view data."""
        return [
            {
                'FILE_FORMAT_CATALOG': database or 'SNOWGLOBE',
                'FILE_FORMAT_SCHEMA': schema or 'PUBLIC',
                'FILE_FORMAT_NAME': 'CSV_DEFAULT',
                'FILE_FORMAT_OWNER': 'ACCOUNTADMIN',
                'FILE_FORMAT_TYPE': 'CSV',
                'RECORD_DELIMITER': '\\n',
                'FIELD_DELIMITER': ',',
                'SKIP_HEADER': 0,
                'DATE_FORMAT': 'AUTO',
                'TIME_FORMAT': 'AUTO',
                'TIMESTAMP_FORMAT': 'AUTO',
                'BINARY_FORMAT': 'HEX',
                'ESCAPE': 'NONE',
                'ESCAPE_UNENCLOSED_FIELD': '\\\\',
                'TRIM_SPACE': False,
                'FIELD_OPTIONALLY_ENCLOSED_BY': '"',
                'NULL_IF': [''],
                'COMPRESSION': 'AUTO',
                'ERROR_ON_COLUMN_COUNT_MISMATCH': True,
                'CREATED': datetime.utcnow().isoformat(),
                'LAST_ALTERED': datetime.utcnow().isoformat(),
                'COMMENT': 'Default CSV format',
            }
        ]
    
    def get_table_constraints(self, database: str = None, schema: str = None, 
                              table: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.TABLE_CONSTRAINTS view data."""
        # Basic implementation - can be expanded with actual constraint tracking
        return []
    
    def get_referential_constraints(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS view data."""
        return []
    
    def get_functions(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.FUNCTIONS view data."""
        # Return some built-in functions
        builtin_functions = [
            'CURRENT_DATABASE', 'CURRENT_SCHEMA', 'CURRENT_USER', 'CURRENT_ROLE',
            'CURRENT_WAREHOUSE', 'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME',
            'GETDATE', 'SYSDATE', 'NOW', 'DATEADD', 'DATEDIFF', 'DATE_TRUNC',
            'TO_DATE', 'TO_TIMESTAMP', 'TO_CHAR', 'TO_NUMBER', 'TO_VARCHAR',
            'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'LENGTH', 'SUBSTR',
            'CONCAT', 'REPLACE', 'SPLIT', 'SPLIT_PART', 'REGEXP_REPLACE',
            'SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'COUNT_IF',
            'COALESCE', 'NVL', 'NVL2', 'NULLIF', 'ZEROIFNULL', 'IFNULL',
            'IFF', 'CASE', 'DECODE',
            'ARRAY_AGG', 'ARRAY_SIZE', 'ARRAY_TO_STRING', 'FLATTEN',
            'OBJECT_CONSTRUCT', 'PARSE_JSON', 'TRY_PARSE_JSON', 'TO_JSON',
            'MD5', 'SHA1', 'SHA2', 'HASH',
            'RANDOM', 'UNIFORM', 'SEQ1', 'SEQ2', 'SEQ4', 'SEQ8',
        ]
        
        result = []
        for func in builtin_functions:
            result.append({
                'FUNCTION_CATALOG': database or 'SNOWGLOBE',
                'FUNCTION_SCHEMA': 'PUBLIC',
                'FUNCTION_NAME': func,
                'FUNCTION_OWNER': 'ACCOUNTADMIN',
                'ARGUMENT_SIGNATURE': '()',
                'DATA_TYPE': 'VARIANT',
                'CHARACTER_MAXIMUM_LENGTH': None,
                'CHARACTER_OCTET_LENGTH': None,
                'NUMERIC_PRECISION': None,
                'NUMERIC_PRECISION_RADIX': None,
                'NUMERIC_SCALE': None,
                'FUNCTION_LANGUAGE': 'SQL',
                'FUNCTION_DEFINITION': None,
                'VOLATILITY': 'VOLATILE',
                'IS_NULL_CALL': 'NO',
                'CREATED': datetime.utcnow().isoformat(),
                'LAST_ALTERED': datetime.utcnow().isoformat(),
                'COMMENT': f'Built-in {func} function',
                'IS_SECURE': 'NO',
                'IS_EXTERNAL': 'NO',
            })
        
        return result
    
    def get_procedures(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.PROCEDURES view data."""
        return []
    
    def get_object_privileges(self, database: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.OBJECT_PRIVILEGES view data."""
        result = []
        
        databases = self.metadata.list_databases()
        for db in databases:
            result.append({
                'GRANTOR': 'ACCOUNTADMIN',
                'GRANTEE': 'ACCOUNTADMIN',
                'OBJECT_CATALOG': db['name'],
                'OBJECT_SCHEMA': None,
                'OBJECT_NAME': db['name'],
                'OBJECT_TYPE': 'DATABASE',
                'PRIVILEGE_TYPE': 'OWNERSHIP',
                'IS_GRANTABLE': 'YES',
                'CREATED': db['created_at'],
            })
        
        return result
    
    def get_table_storage_metrics(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.TABLE_STORAGE_METRICS view data."""
        result = []
        tables_data = self.get_tables(database, schema)
        
        for table in tables_data:
            if table['TABLE_TYPE'] != 'BASE TABLE':
                continue
            
            result.append({
                'TABLE_CATALOG': table['TABLE_CATALOG'],
                'TABLE_SCHEMA': table['TABLE_SCHEMA'],
                'TABLE_NAME': table['TABLE_NAME'],
                'ID': hash(f"{table['TABLE_CATALOG']}.{table['TABLE_SCHEMA']}.{table['TABLE_NAME']}") % (10 ** 18),
                'CLONE_GROUP_ID': None,
                'IS_TRANSIENT': 'NO',
                'ACTIVE_BYTES': table.get('BYTES', 0) or 0,
                'TIME_TRAVEL_BYTES': 0,
                'FAILSAFE_BYTES': 0,
                'RETAINED_FOR_CLONE_BYTES': 0,
                'DELETED': 'NO',
                'TABLE_CREATED': table['CREATED'],
                'TABLE_DROPPED': None,
                'TABLE_ENTERED_FAILSAFE': None,
                'SCHEMA_CREATED': None,
                'SCHEMA_DROPPED': None,
                'CATALOG_CREATED': None,
                'CATALOG_DROPPED': None,
                'COMMENT': None,
            })
        
        return result
    
    def get_load_history(self, database: str = None, schema: str = None) -> List[Dict[str, Any]]:
        """Get INFORMATION_SCHEMA.LOAD_HISTORY view data."""
        # Return empty - can be populated when COPY INTO is implemented
        return []
    
    def query_information_schema(self, view_name: str, database: str = None,
                                  schema: str = None, table: str = None,
                                  filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Query an INFORMATION_SCHEMA view.
        
        Args:
            view_name: Name of the INFORMATION_SCHEMA view
            database: Optional database filter
            schema: Optional schema filter
            table: Optional table filter
            filters: Additional column filters
            
        Returns:
            Query result dict with success, data, columns, rowcount
        """
        view_name = view_name.upper()
        
        # Map view names to getter methods
        view_methods = {
            'DATABASES': lambda: self.get_databases(),
            'SCHEMATA': lambda: self.get_schemata(database),
            'TABLES': lambda: self.get_tables(database, schema),
            'COLUMNS': lambda: self.get_columns(database, schema, table),
            'VIEWS': lambda: self.get_views(database, schema),
            'APPLICABLE_ROLES': lambda: self.get_applicable_roles(),
            'ENABLED_ROLES': lambda: self.get_enabled_roles(),
            'SEQUENCES': lambda: self.get_sequences(database, schema),
            'STAGES': lambda: self.get_stages(database, schema),
            'FILE_FORMATS': lambda: self.get_file_formats(database, schema),
            'TABLE_CONSTRAINTS': lambda: self.get_table_constraints(database, schema, table),
            'REFERENTIAL_CONSTRAINTS': lambda: self.get_referential_constraints(database, schema),
            'FUNCTIONS': lambda: self.get_functions(database, schema),
            'PROCEDURES': lambda: self.get_procedures(database, schema),
            'OBJECT_PRIVILEGES': lambda: self.get_object_privileges(database),
            'TABLE_STORAGE_METRICS': lambda: self.get_table_storage_metrics(database, schema),
            'LOAD_HISTORY': lambda: self.get_load_history(database, schema),
        }
        
        if view_name not in view_methods:
            return {
                'success': False,
                'error': f"Unknown INFORMATION_SCHEMA view: {view_name}",
                'data': [],
                'columns': [],
                'rowcount': 0
            }
        
        try:
            data = view_methods[view_name]()
            
            # Apply additional filters
            if filters:
                filtered_data = []
                for row in data:
                    matches = True
                    for key, value in filters.items():
                        key_upper = key.upper()
                        if key_upper in row and row[key_upper] != value:
                            matches = False
                            break
                    if matches:
                        filtered_data.append(row)
                data = filtered_data
            
            if not data:
                return {
                    'success': True,
                    'data': [],
                    'columns': [],
                    'rowcount': 0
                }
            
            columns = list(data[0].keys())
            rows = [[row.get(col) for col in columns] for row in data]
            
            return {
                'success': True,
                'data': rows,
                'columns': columns,
                'rowcount': len(rows)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'data': [],
                'columns': [],
                'rowcount': 0
            }
