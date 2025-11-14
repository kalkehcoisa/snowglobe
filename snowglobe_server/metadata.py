"""
Metadata Store - Manages database, schema, and table metadata
"""

import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
import threading


class MetadataStore:
    """Manages Snowflake-like metadata for databases, schemas, and tables"""
    
    def __init__(self, data_dir: str = "/data"):
        self.data_dir = data_dir
        self.metadata_file = os.path.join(data_dir, "metadata.json")
        self._lock = threading.RLock()
        self._metadata = self._load_metadata()
        
        # Initialize default database if not exists
        if "databases" not in self._metadata:
            self._metadata["databases"] = {}
        
        # Initialize dropped objects storage for UNDROP support
        if "dropped" not in self._metadata:
            self._metadata["dropped"] = {
                "databases": {},
                "schemas": {},
                "tables": {},
                "views": {}
            }
        
        # Create default database
        if "SNOWGLOBE" not in self._metadata["databases"]:
            self.create_database("SNOWGLOBE")
    
    def _load_metadata(self) -> Dict:
        """Load metadata from disk"""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    data = json.load(f)
                    # Ensure dropped section exists for backward compatibility
                    if "dropped" not in data:
                        data["dropped"] = {
                            "databases": {},
                            "schemas": {},
                            "tables": {},
                            "views": {}
                        }
                    return data
            except Exception:
                pass
        return {
            "databases": {},
            "dropped": {
                "databases": {},
                "schemas": {},
                "tables": {},
                "views": {}
            }
        }
    
    def _save_metadata(self):
        """Save metadata to disk"""
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.metadata_file, 'w') as f:
            json.dump(self._metadata, f, indent=2, default=str)
    
    def create_database(self, name: str, if_not_exists: bool = False) -> bool:
        """Create a new database"""
        with self._lock:
            name = name.upper()
            if name in self._metadata["databases"]:
                if if_not_exists:
                    return True
                raise ValueError(f"Database '{name}' already exists")
            
            self._metadata["databases"][name] = {
                "name": name,
                "created_at": datetime.utcnow().isoformat(),
                "schemas": {
                    "PUBLIC": {
                        "name": "PUBLIC",
                        "created_at": datetime.utcnow().isoformat(),
                        "tables": {},
                        "views": {},
                        "sequences": {}
                    },
                    "INFORMATION_SCHEMA": {
                        "name": "INFORMATION_SCHEMA",
                        "created_at": datetime.utcnow().isoformat(),
                        "tables": {},
                        "views": {},
                        "sequences": {}
                    }
                }
            }
            self._save_metadata()
            return True
    
    def drop_database(self, name: str, if_exists: bool = False) -> bool:
        """Drop a database"""
        with self._lock:
            name = name.upper()
            if name not in self._metadata["databases"]:
                if if_exists:
                    return True
                raise ValueError(f"Database '{name}' does not exist")
            
            # Store in dropped for UNDROP support
            dropped_db = self._metadata["databases"][name].copy()
            dropped_db["dropped_at"] = datetime.utcnow().isoformat()
            self._metadata["dropped"]["databases"][name] = dropped_db
            
            del self._metadata["databases"][name]
            self._save_metadata()
            return True
    
    def undrop_database(self, name: str) -> bool:
        """Restore a dropped database"""
        with self._lock:
            name = name.upper()
            if name not in self._metadata["dropped"]["databases"]:
                raise ValueError(f"Database '{name}' not found in dropped databases")
            
            if name in self._metadata["databases"]:
                raise ValueError(f"Database '{name}' already exists")
            
            # Restore from dropped
            restored_db = self._metadata["dropped"]["databases"][name].copy()
            del restored_db["dropped_at"]
            self._metadata["databases"][name] = restored_db
            
            del self._metadata["dropped"]["databases"][name]
            self._save_metadata()
            return True
    
    def list_databases(self) -> List[Dict]:
        """List all databases"""
        with self._lock:
            return [
                {"name": name, "created_at": db["created_at"]}
                for name, db in self._metadata["databases"].items()
            ]
    
    def database_exists(self, name: str) -> bool:
        """Check if database exists"""
        with self._lock:
            return name.upper() in self._metadata["databases"]
    
    def create_schema(self, database: str, schema: str, if_not_exists: bool = False) -> bool:
        """Create a new schema"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema in self._metadata["databases"][database]["schemas"]:
                if if_not_exists:
                    return True
                raise ValueError(f"Schema '{schema}' already exists in database '{database}'")
            
            self._metadata["databases"][database]["schemas"][schema] = {
                "name": schema,
                "created_at": datetime.utcnow().isoformat(),
                "tables": {},
                "views": {},
                "sequences": {}
            }
            self._save_metadata()
            return True
    
    def drop_schema(self, database: str, schema: str, if_exists: bool = False, cascade: bool = False) -> bool:
        """Drop a schema"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema not in self._metadata["databases"][database]["schemas"]:
                if if_exists:
                    return True
                raise ValueError(f"Schema '{schema}' does not exist in database '{database}'")
            
            schema_data = self._metadata["databases"][database]["schemas"][schema]
            if not cascade and (schema_data["tables"] or schema_data["views"]):
                raise ValueError(f"Schema '{schema}' is not empty. Use CASCADE to drop.")
            
            # Store in dropped for UNDROP support
            dropped_key = f"{database}.{schema}"
            dropped_schema = schema_data.copy()
            dropped_schema["dropped_at"] = datetime.utcnow().isoformat()
            dropped_schema["database"] = database
            self._metadata["dropped"]["schemas"][dropped_key] = dropped_schema
            
            del self._metadata["databases"][database]["schemas"][schema]
            self._save_metadata()
            return True
    
    def undrop_schema(self, database: str, schema: str) -> bool:
        """Restore a dropped schema"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            dropped_key = f"{database}.{schema}"
            
            if dropped_key not in self._metadata["dropped"]["schemas"]:
                raise ValueError(f"Schema '{schema}' not found in dropped schemas for database '{database}'")
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema in self._metadata["databases"][database]["schemas"]:
                raise ValueError(f"Schema '{schema}' already exists in database '{database}'")
            
            # Restore from dropped
            restored_schema = self._metadata["dropped"]["schemas"][dropped_key].copy()
            del restored_schema["dropped_at"]
            del restored_schema["database"]
            self._metadata["databases"][database]["schemas"][schema] = restored_schema
            
            del self._metadata["dropped"]["schemas"][dropped_key]
            self._save_metadata()
            return True
    
    def list_schemas(self, database: str) -> List[Dict]:
        """List all schemas in a database"""
        with self._lock:
            database = database.upper()
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            return [
                {"name": name, "created_at": schema["created_at"]}
                for name, schema in self._metadata["databases"][database]["schemas"].items()
            ]
    
    def schema_exists(self, database: str, schema: str) -> bool:
        """Check if schema exists"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            if database not in self._metadata["databases"]:
                return False
            return schema in self._metadata["databases"][database]["schemas"]
    
    def register_table(self, database: str, schema: str, table: str, 
                       columns: List[Dict], if_not_exists: bool = False) -> bool:
        """Register a table in metadata"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            table = table.upper()
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema not in self._metadata["databases"][database]["schemas"]:
                raise ValueError(f"Schema '{schema}' does not exist in database '{database}'")
            
            tables = self._metadata["databases"][database]["schemas"][schema]["tables"]
            if table in tables:
                if if_not_exists:
                    return True
                raise ValueError(f"Table '{table}' already exists in schema '{schema}'")
            
            tables[table] = {
                "name": table,
                "created_at": datetime.utcnow().isoformat(),
                "columns": columns,
                "row_count": 0,
                "bytes": 0
            }
            self._save_metadata()
            return True
    
    def drop_table(self, database: str, schema: str, table: str, if_exists: bool = False) -> bool:
        """Drop a table from metadata"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            table = table.upper()
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema not in self._metadata["databases"][database]["schemas"]:
                raise ValueError(f"Schema '{schema}' does not exist")
            
            tables = self._metadata["databases"][database]["schemas"][schema]["tables"]
            if table not in tables:
                if if_exists:
                    return True
                raise ValueError(f"Table '{table}' does not exist in schema '{schema}'")
            
            # Store in dropped for UNDROP support
            dropped_key = f"{database}.{schema}.{table}"
            dropped_table = tables[table].copy()
            dropped_table["dropped_at"] = datetime.utcnow().isoformat()
            dropped_table["database"] = database
            dropped_table["schema"] = schema
            self._metadata["dropped"]["tables"][dropped_key] = dropped_table
            
            del tables[table]
            self._save_metadata()
            return True
    
    def undrop_table(self, database: str, schema: str, table: str) -> bool:
        """Restore a dropped table"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            table = table.upper()
            dropped_key = f"{database}.{schema}.{table}"
            
            if dropped_key not in self._metadata["dropped"]["tables"]:
                raise ValueError(f"Table '{table}' not found in dropped tables")
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema not in self._metadata["databases"][database]["schemas"]:
                raise ValueError(f"Schema '{schema}' does not exist")
            
            tables = self._metadata["databases"][database]["schemas"][schema]["tables"]
            if table in tables:
                raise ValueError(f"Table '{table}' already exists in schema '{schema}'")
            
            # Restore from dropped
            restored_table = self._metadata["dropped"]["tables"][dropped_key].copy()
            del restored_table["dropped_at"]
            del restored_table["database"]
            del restored_table["schema"]
            tables[table] = restored_table
            
            del self._metadata["dropped"]["tables"][dropped_key]
            self._save_metadata()
            return True
    
    def list_tables(self, database: str, schema: str) -> List[Dict]:
        """List all tables in a schema"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema not in self._metadata["databases"][database]["schemas"]:
                raise ValueError(f"Schema '{schema}' does not exist")
            
            return [
                {
                    "name": name,
                    "created_at": table["created_at"],
                    "columns": table["columns"],
                    "row_count": table.get("row_count", 0)
                }
                for name, table in self._metadata["databases"][database]["schemas"][schema]["tables"].items()
            ]
    
    def table_exists(self, database: str, schema: str, table: str) -> bool:
        """Check if table exists"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            table = table.upper()
            
            if database not in self._metadata["databases"]:
                return False
            if schema not in self._metadata["databases"][database]["schemas"]:
                return False
            return table in self._metadata["databases"][database]["schemas"][schema]["tables"]
    
    def get_table_info(self, database: str, schema: str, table: str) -> Optional[Dict]:
        """Get table information"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            table = table.upper()
            
            if not self.table_exists(database, schema, table):
                return None
            
            return self._metadata["databases"][database]["schemas"][schema]["tables"][table]
    
    def update_table_stats(self, database: str, schema: str, table: str, 
                           row_count: Optional[int] = None, bytes_size: Optional[int] = None):
        """Update table statistics"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            table = table.upper()
            
            if not self.table_exists(database, schema, table):
                return
            
            table_info = self._metadata["databases"][database]["schemas"][schema]["tables"][table]
            if row_count is not None:
                table_info["row_count"] = row_count
            if bytes_size is not None:
                table_info["bytes"] = bytes_size
            
            self._save_metadata()
    
    def register_view(self, database: str, schema: str, view: str, 
                      definition: str, if_not_exists: bool = False) -> bool:
        """Register a view in metadata"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            view = view.upper()
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema not in self._metadata["databases"][database]["schemas"]:
                raise ValueError(f"Schema '{schema}' does not exist in database '{database}'")
            
            views = self._metadata["databases"][database]["schemas"][schema]["views"]
            if view in views:
                if if_not_exists:
                    return True
                raise ValueError(f"View '{view}' already exists in schema '{schema}'")
            
            views[view] = {
                "name": view,
                "created_at": datetime.utcnow().isoformat(),
                "definition": definition
            }
            self._save_metadata()
            return True
    
    def drop_view(self, database: str, schema: str, view: str, if_exists: bool = False) -> bool:
        """Drop a view from metadata"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            view = view.upper()
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema not in self._metadata["databases"][database]["schemas"]:
                raise ValueError(f"Schema '{schema}' does not exist")
            
            views = self._metadata["databases"][database]["schemas"][schema]["views"]
            if view not in views:
                if if_exists:
                    return True
                raise ValueError(f"View '{view}' does not exist in schema '{schema}'")
            
            # Store in dropped for UNDROP support
            dropped_key = f"{database}.{schema}.{view}"
            dropped_view = views[view].copy()
            dropped_view["dropped_at"] = datetime.utcnow().isoformat()
            dropped_view["database"] = database
            dropped_view["schema"] = schema
            self._metadata["dropped"]["views"][dropped_key] = dropped_view
            
            del views[view]
            self._save_metadata()
            return True
    
    def undrop_view(self, database: str, schema: str, view: str) -> bool:
        """Restore a dropped view"""
        with self._lock:
            database = database.upper()
            schema = schema.upper()
            view = view.upper()
            dropped_key = f"{database}.{schema}.{view}"
            
            if dropped_key not in self._metadata["dropped"]["views"]:
                raise ValueError(f"View '{view}' not found in dropped views")
            
            if database not in self._metadata["databases"]:
                raise ValueError(f"Database '{database}' does not exist")
            
            if schema not in self._metadata["databases"][database]["schemas"]:
                raise ValueError(f"Schema '{schema}' does not exist")
            
            views = self._metadata["databases"][database]["schemas"][schema]["views"]
            if view in views:
                raise ValueError(f"View '{view}' already exists in schema '{schema}'")
            
            # Restore from dropped
            restored_view = self._metadata["dropped"]["views"][dropped_key].copy()
            del restored_view["dropped_at"]
            del restored_view["database"]
            del restored_view["schema"]
            views[view] = restored_view
            
            del self._metadata["dropped"]["views"][dropped_key]
            self._save_metadata()
            return True
    
    def list_dropped_tables(self, database: str = None, schema: str = None) -> List[Dict]:
        """List all dropped tables, optionally filtered by database and schema"""
        with self._lock:
            result = []
            for key, table_data in self._metadata["dropped"]["tables"].items():
                if database and table_data["database"] != database.upper():
                    continue
                if schema and table_data["schema"] != schema.upper():
                    continue
                result.append({
                    "name": table_data["name"],
                    "database": table_data["database"],
                    "schema": table_data["schema"],
                    "dropped_at": table_data["dropped_at"]
                })
            return result
    
    def list_dropped_schemas(self, database: str = None) -> List[Dict]:
        """List all dropped schemas, optionally filtered by database"""
        with self._lock:
            result = []
            for key, schema_data in self._metadata["dropped"]["schemas"].items():
                if database and schema_data["database"] != database.upper():
                    continue
                result.append({
                    "name": schema_data["name"],
                    "database": schema_data["database"],
                    "dropped_at": schema_data["dropped_at"]
                })
            return result
    
    def list_dropped_databases(self) -> List[Dict]:
        """List all dropped databases"""
        with self._lock:
            return [
                {"name": name, "dropped_at": db["dropped_at"]}
                for name, db in self._metadata["dropped"]["databases"].items()
            ]
    
    def get_full_table_name(self, database: str, schema: str, table: str) -> str:
        """Get fully qualified table name"""
        return f"{database.upper()}.{schema.upper()}.{table.upper()}"
