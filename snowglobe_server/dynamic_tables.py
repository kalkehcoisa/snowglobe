"""
Dynamic Tables Support - Background continuous data loading with scheduled refreshes
Automatically keeps data current without separate targets or transformation code
"""

import duckdb
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import threading
import time


class DynamicTableManager:
    """Manages Snowflake Dynamic Tables with automatic refresh"""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection, metadata_store):
        self.conn = conn
        self.metadata = metadata_store
        self.dynamic_tables = {}  # Track dynamic table configurations
        self.refresh_threads = {}  # Background refresh threads
        self.running = True
        
    def create_dynamic_table(self, database: str, schema: str, table_name: str,
                            target_lag: str, refresh_mode: str, query: str,
                            warehouse: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a dynamic table with automatic refresh
        
        Args:
            database: Database name
            schema: Schema name
            table_name: Table name
            target_lag: Target freshness (e.g., '5 minutes', '1 hour', 'DOWNSTREAM')
            refresh_mode: 'AUTO', 'FULL', or 'INCREMENTAL'
            query: SQL query to populate the table
            warehouse: Warehouse to use for refresh operations
        """
        full_name = f"{database}.{schema}.{table_name}"
        
        # Parse target lag
        lag_seconds = self._parse_target_lag(target_lag)
        
        # Create the underlying table
        table_ref = f"{database.lower()}_{schema.lower()}.{table_name}"
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_ref} AS {query}"
        
        try:
            self.conn.execute(create_sql)
        except Exception as e:
            return {"success": False, "error": f"Failed to create dynamic table: {str(e)}"}
        
        # Track dynamic table configuration
        config = {
            "database": database,
            "schema": schema,
            "table": table_name,
            "target_lag": target_lag,
            "lag_seconds": lag_seconds,
            "refresh_mode": refresh_mode,
            "query": query,
            "warehouse": warehouse or "COMPUTE_WH",
            "created_at": datetime.now().isoformat(),
            "last_refresh": datetime.now().isoformat(),
            "next_refresh": (datetime.now() + timedelta(seconds=lag_seconds)).isoformat(),
            "refresh_count": 0,
            "type": "DYNAMIC"
        }
        
        self.dynamic_tables[full_name] = config
        
        # Register in metadata
        self.metadata.track_table(database, schema, table_name, "DYNAMIC")
        
        # Start background refresh thread
        self._start_refresh_thread(full_name, config)
        
        return {
            "success": True,
            "message": f"Dynamic table {full_name} created successfully",
            "table_type": "DYNAMIC",
            "target_lag": target_lag,
            "refresh_mode": refresh_mode,
            "next_refresh": config["next_refresh"]
        }
    
    def alter_dynamic_table(self, database: str, schema: str, table_name: str,
                           target_lag: Optional[str] = None,
                           refresh_mode: Optional[str] = None,
                           warehouse: Optional[str] = None) -> Dict[str, Any]:
        """Alter dynamic table configuration"""
        full_name = f"{database}.{schema}.{table_name}"
        
        if full_name not in self.dynamic_tables:
            return {"success": False, "error": f"{full_name} is not a dynamic table"}
        
        config = self.dynamic_tables[full_name]
        
        # Update configuration
        if target_lag:
            config["target_lag"] = target_lag
            config["lag_seconds"] = self._parse_target_lag(target_lag)
        
        if refresh_mode:
            config["refresh_mode"] = refresh_mode
        
        if warehouse:
            config["warehouse"] = warehouse
        
        # Restart refresh thread with new configuration
        self._stop_refresh_thread(full_name)
        self._start_refresh_thread(full_name, config)
        
        return {
            "success": True,
            "message": f"Dynamic table {full_name} altered successfully",
            "target_lag": config["target_lag"],
            "refresh_mode": config["refresh_mode"]
        }
    
    def refresh_dynamic_table(self, database: str, schema: str, table_name: str,
                             force: bool = False) -> Dict[str, Any]:
        """Manually trigger a refresh of a dynamic table"""
        full_name = f"{database}.{schema}.{table_name}"
        
        if full_name not in self.dynamic_tables:
            return {"success": False, "error": f"{full_name} is not a dynamic table"}
        
        config = self.dynamic_tables[full_name]
        
        # Perform refresh
        start_time = datetime.now()
        
        try:
            table_ref = f"{database.lower()}_{schema.lower()}.{table_name}"
            
            if config["refresh_mode"] == "FULL":
                # Full refresh: truncate and reload
                self.conn.execute(f"DELETE FROM {table_ref}")
                insert_sql = f"INSERT INTO {table_ref} {config['query']}"
                self.conn.execute(insert_sql)
            else:
                # Incremental or AUTO: replace entire table
                self.conn.execute(f"DROP TABLE IF EXISTS {table_ref}")
                create_sql = f"CREATE TABLE {table_ref} AS {config['query']}"
                self.conn.execute(create_sql)
            
            # Update metadata
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            config["last_refresh"] = end_time.isoformat()
            config["next_refresh"] = (end_time + timedelta(seconds=config["lag_seconds"])).isoformat()
            config["refresh_count"] += 1
            
            return {
                "success": True,
                "message": f"Dynamic table {full_name} refreshed successfully",
                "refresh_mode": config["refresh_mode"],
                "duration_seconds": duration,
                "last_refresh": config["last_refresh"],
                "next_refresh": config["next_refresh"],
                "refresh_count": config["refresh_count"]
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to refresh dynamic table: {str(e)}"
            }
    
    def suspend_dynamic_table(self, database: str, schema: str, table_name: str) -> Dict[str, Any]:
        """Suspend automatic refresh for a dynamic table"""
        full_name = f"{database}.{schema}.{table_name}"
        
        if full_name not in self.dynamic_tables:
            return {"success": False, "error": f"{full_name} is not a dynamic table"}
        
        self._stop_refresh_thread(full_name)
        self.dynamic_tables[full_name]["suspended"] = True
        
        return {
            "success": True,
            "message": f"Dynamic table {full_name} suspended"
        }
    
    def resume_dynamic_table(self, database: str, schema: str, table_name: str) -> Dict[str, Any]:
        """Resume automatic refresh for a dynamic table"""
        full_name = f"{database}.{schema}.{table_name}"
        
        if full_name not in self.dynamic_tables:
            return {"success": False, "error": f"{full_name} is not a dynamic table"}
        
        config = self.dynamic_tables[full_name]
        config["suspended"] = False
        self._start_refresh_thread(full_name, config)
        
        return {
            "success": True,
            "message": f"Dynamic table {full_name} resumed"
        }
    
    def list_dynamic_tables(self, database: Optional[str] = None,
                           schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all dynamic tables"""
        tables = []
        for full_name, config in self.dynamic_tables.items():
            if database and config['database'] != database:
                continue
            if schema and config['schema'] != schema:
                continue
            tables.append({
                "name": full_name,
                "database": config['database'],
                "schema": config['schema'],
                "table": config['table'],
                "type": "DYNAMIC",
                "target_lag": config['target_lag'],
                "refresh_mode": config['refresh_mode'],
                "last_refresh": config['last_refresh'],
                "next_refresh": config.get('next_refresh'),
                "refresh_count": config['refresh_count'],
                "suspended": config.get('suspended', False)
            })
        return tables
    
    def _parse_target_lag(self, target_lag: str) -> int:
        """Parse target lag string to seconds"""
        if target_lag.upper() == "DOWNSTREAM":
            return 300  # Default 5 minutes for downstream
        
        # Parse time expressions like "5 minutes", "1 hour", "30 seconds"
        parts = target_lag.lower().split()
        if len(parts) != 2:
            return 300  # Default to 5 minutes
        
        value = int(parts[0])
        unit = parts[1].rstrip('s')  # Remove plural 's'
        
        multipliers = {
            'second': 1,
            'minute': 60,
            'hour': 3600,
            'day': 86400
        }
        
        return value * multipliers.get(unit, 60)
    
    def _start_refresh_thread(self, full_name: str, config: Dict[str, Any]):
        """Start background refresh thread for a dynamic table"""
        if full_name in self.refresh_threads:
            return  # Already running
        
        def refresh_loop():
            while self.running and full_name in self.dynamic_tables:
                config = self.dynamic_tables.get(full_name)
                if not config or config.get("suspended"):
                    break
                
                # Wait until next refresh time
                next_refresh = datetime.fromisoformat(config["next_refresh"])
                wait_seconds = (next_refresh - datetime.now()).total_seconds()
                
                if wait_seconds > 0:
                    time.sleep(min(wait_seconds, 60))  # Check every minute
                    continue
                
                # Perform refresh
                parts = full_name.split('.')
                self.refresh_dynamic_table(parts[0], parts[1], parts[2])
        
        thread = threading.Thread(target=refresh_loop, daemon=True)
        thread.start()
        self.refresh_threads[full_name] = thread
    
    def _stop_refresh_thread(self, full_name: str):
        """Stop background refresh thread"""
        if full_name in self.refresh_threads:
            del self.refresh_threads[full_name]
    
    def shutdown(self):
        """Shutdown all refresh threads"""
        self.running = False
        self.refresh_threads.clear()
