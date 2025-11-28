"""
Hybrid Tables Support - Unistore hybrid tables for concurrent analytical and transactional workloads
Enables fast single-row operations with ACID compliance
"""

import duckdb
from typing import Dict, Any, List, Optional
from datetime import datetime


class HybridTableManager:
    """Manages Snowflake Hybrid Tables (Unistore)"""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection, metadata_store):
        self.conn = conn
        self.metadata = metadata_store
        self.hybrid_tables = {}  # Track hybrid table configurations
        
    def create_hybrid_table(self, database: str, schema: str, table_name: str, 
                           columns: List[Dict[str, str]], primary_key: Optional[List[str]] = None,
                           indexes: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Create a hybrid table with ACID properties for transactional operations
        
        Args:
            database: Database name
            schema: Schema name
            table_name: Table name
            columns: List of column definitions
            primary_key: List of primary key column names
            indexes: List of index definitions for fast lookups
        """
        full_name = f"{database}.{schema}.{table_name}"
        
        # Build column definitions
        col_defs = []
        for col in columns:
            col_def = f"{col['name']} {col['type']}"
            if col.get('not_null'):
                col_def += " NOT NULL"
            if col.get('unique'):
                col_def += " UNIQUE"
            col_defs.append(col_def)
        
        # Add primary key constraint
        if primary_key:
            pk_cols = ", ".join(primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")
        
        # Create the table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {database.lower()}_{schema.lower()}.{table_name} (
            {", ".join(col_defs)},
            _hybrid_metadata_row_version INTEGER DEFAULT 0,
            _hybrid_metadata_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _hybrid_metadata_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        self.conn.execute(create_sql)
        
        # Create indexes for fast single-row operations
        if indexes:
            for idx in indexes:
                idx_name = idx.get('name', f"idx_{table_name}_{idx['column']}")
                idx_col = idx['column']
                idx_sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {database.lower()}_{schema.lower()}.{table_name} ({idx_col})"
                self.conn.execute(idx_sql)
        
        # Track as hybrid table
        self.hybrid_tables[full_name] = {
            "database": database,
            "schema": schema,
            "table": table_name,
            "primary_key": primary_key,
            "indexes": indexes or [],
            "created_at": datetime.now().isoformat(),
            "type": "HYBRID"
        }
        
        # Register in metadata
        self.metadata.track_table(database, schema, table_name, "HYBRID")
        
        return {
            "success": True,
            "message": f"Hybrid table {full_name} created successfully",
            "table_type": "HYBRID",
            "supports_transactions": True,
            "supports_analytics": True
        }
    
    def upsert_row(self, database: str, schema: str, table_name: str, 
                   data: Dict[str, Any], primary_key_cols: List[str]) -> Dict[str, Any]:
        """
        Perform fast single-row UPSERT operation (transactional)
        """
        full_name = f"{database}.{schema}.{table_name}"
        
        if full_name not in self.hybrid_tables:
            return {"success": False, "error": f"{full_name} is not a hybrid table"}
        
        table_ref = f"{database.lower()}_{schema.lower()}.{table_name}"
        
        # Build UPSERT using INSERT ... ON CONFLICT
        columns = list(data.keys())
        values = list(data.values())
        
        placeholders = ", ".join(["?" for _ in values])
        col_list = ", ".join(columns)
        
        # Update clause for ON CONFLICT
        update_sets = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in primary_key_cols])
        update_sets += ", _hybrid_metadata_row_version = _hybrid_metadata_row_version + 1"
        update_sets += ", _hybrid_metadata_updated_at = CURRENT_TIMESTAMP"
        
        pk_constraint = ", ".join(primary_key_cols)
        
        upsert_sql = f"""
        INSERT INTO {table_ref} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({pk_constraint}) DO UPDATE SET {update_sets}
        """
        
        self.conn.execute(upsert_sql, values)
        
        return {
            "success": True,
            "operation": "UPSERT",
            "rows_affected": 1,
            "table_type": "HYBRID"
        }
    
    def delete_row(self, database: str, schema: str, table_name: str, 
                   where_clause: str, params: Optional[List] = None) -> Dict[str, Any]:
        """
        Perform fast single-row DELETE operation (transactional)
        """
        full_name = f"{database}.{schema}.{table_name}"
        
        if full_name not in self.hybrid_tables:
            return {"success": False, "error": f"{full_name} is not a hybrid table"}
        
        table_ref = f"{database.lower()}_{schema.lower()}.{table_name}"
        delete_sql = f"DELETE FROM {table_ref} WHERE {where_clause}"
        
        result = self.conn.execute(delete_sql, params or [])
        
        return {
            "success": True,
            "operation": "DELETE",
            "rows_affected": result.rowcount if hasattr(result, 'rowcount') else 1,
            "table_type": "HYBRID"
        }
    
    def get_row_by_pk(self, database: str, schema: str, table_name: str, 
                      pk_values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fast single-row lookup by primary key (optimized with indexes)
        """
        full_name = f"{database}.{schema}.{table_name}"
        
        if full_name not in self.hybrid_tables:
            return {"success": False, "error": f"{full_name} is not a hybrid table"}
        
        table_ref = f"{database.lower()}_{schema.lower()}.{table_name}"
        
        # Build WHERE clause from PK values
        where_parts = [f"{k} = ?" for k in pk_values.keys()]
        where_clause = " AND ".join(where_parts)
        
        select_sql = f"SELECT * FROM {table_ref} WHERE {where_clause}"
        result = self.conn.execute(select_sql, list(pk_values.values()))
        
        row = result.fetchone()
        columns = [desc[0] for desc in result.description] if result.description else []
        
        return {
            "success": True,
            "data": dict(zip(columns, row)) if row else None,
            "found": row is not None,
            "table_type": "HYBRID"
        }
    
    def is_hybrid_table(self, database: str, schema: str, table_name: str) -> bool:
        """Check if a table is a hybrid table"""
        full_name = f"{database}.{schema}.{table_name}"
        return full_name in self.hybrid_tables
    
    def list_hybrid_tables(self, database: Optional[str] = None, 
                          schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all hybrid tables, optionally filtered by database/schema"""
        tables = []
        for full_name, config in self.hybrid_tables.items():
            if database and config['database'] != database:
                continue
            if schema and config['schema'] != schema:
                continue
            tables.append({
                "name": full_name,
                "database": config['database'],
                "schema": config['schema'],
                "table": config['table'],
                "type": "HYBRID",
                "primary_key": config['primary_key'],
                "indexes": config['indexes'],
                "created_at": config['created_at']
            })
        return tables
