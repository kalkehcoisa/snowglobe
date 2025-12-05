"""
Object Management Module - CRUD operations for Snowflake objects

Provides seamless creation, alteration, and deletion of:
- Databases
- Schemas
- Tables
- Views
- Stages
- File Formats
- Sequences
- Streams
- Tasks
- Pipes
"""

import os
import logging
import json
import re
from datetime import datetime
from typing import Dict, List, Any, Optional

logger = logging.getLogger("snowglobe.object_management")


class ObjectManager:
    """
    Manages Snowflake-compatible database objects.
    Provides CRUD operations with validation and proper error handling.
    """
    
    def __init__(self, query_executor):
        """
        Initialize the object manager.
        
        Args:
            query_executor: QueryExecutor instance for database operations
        """
        self.executor = query_executor
        self.metadata = query_executor.metadata
    
    # ==================== Database Operations ====================
    
    def create_database(self, name: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a new database.
        
        Args:
            name: Database name
            options: Creation options
                - if_not_exists: bool
                - comment: str
                - data_retention_time_in_days: int
                - transient: bool
                - clone_from: str
                
        Returns:
            Result dictionary
        """
        options = options or {}
        name = name.upper()
        
        # Build CREATE statement
        parts = ["CREATE"]
        if options.get('or_replace'):
            parts.append("OR REPLACE")
        if options.get('transient'):
            parts.append("TRANSIENT")
        parts.append("DATABASE")
        if options.get('if_not_exists'):
            parts.append("IF NOT EXISTS")
        parts.append(name)
        
        # Clone from existing database
        if options.get('clone_from'):
            parts.append(f"CLONE {options['clone_from'].upper()}")
        
        # Additional options
        extra_opts = []
        if options.get('comment'):
            extra_opts.append(f"COMMENT = '{options['comment']}'")
        if options.get('data_retention_time_in_days') is not None:
            extra_opts.append(f"DATA_RETENTION_TIME_IN_DAYS = {options['data_retention_time_in_days']}")
        
        if extra_opts:
            parts.extend(extra_opts)
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Created database: {name}")
            return {
                "success": True,
                "message": f"Database {name} created successfully",
                "database": name,
                "sql": sql
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to create database"),
                "sql": sql
            }
    
    def alter_database(self, name: str, alterations: Dict[str, Any]) -> Dict[str, Any]:
        """
        Alter a database.
        
        Args:
            name: Database name
            alterations: Changes to make
                - rename_to: str
                - set_comment: str
                - set_data_retention_time_in_days: int
                
        Returns:
            Result dictionary
        """
        name = name.upper()
        results = []
        
        if alterations.get('rename_to'):
            new_name = alterations['rename_to'].upper()
            sql = f"ALTER DATABASE {name} RENAME TO {new_name}"
            result = self.executor.execute(sql)
            results.append({
                "action": "rename",
                "success": result["success"],
                "error": result.get("error")
            })
            if result["success"]:
                name = new_name
        
        if alterations.get('set_comment'):
            sql = f"ALTER DATABASE {name} SET COMMENT = '{alterations['set_comment']}'"
            result = self.executor.execute(sql)
            results.append({
                "action": "set_comment",
                "success": result["success"],
                "error": result.get("error")
            })
        
        if 'set_data_retention_time_in_days' in alterations:
            sql = f"ALTER DATABASE {name} SET DATA_RETENTION_TIME_IN_DAYS = {alterations['set_data_retention_time_in_days']}"
            result = self.executor.execute(sql)
            results.append({
                "action": "set_data_retention",
                "success": result["success"],
                "error": result.get("error")
            })
        
        all_success = all(r["success"] for r in results)
        
        return {
            "success": all_success,
            "database": name,
            "alterations": results
        }
    
    def drop_database(self, name: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Drop a database.
        
        Args:
            name: Database name
            options:
                - if_exists: bool
                - cascade: bool
                - restrict: bool
                
        Returns:
            Result dictionary
        """
        options = options or {}
        name = name.upper()
        
        parts = ["DROP DATABASE"]
        if options.get('if_exists'):
            parts.append("IF EXISTS")
        parts.append(name)
        
        if options.get('cascade'):
            parts.append("CASCADE")
        elif options.get('restrict'):
            parts.append("RESTRICT")
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Dropped database: {name}")
            return {
                "success": True,
                "message": f"Database {name} dropped successfully",
                "database": name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to drop database")
            }
    
    def undrop_database(self, name: str) -> Dict[str, Any]:
        """
        Undrop (restore) a dropped database.
        
        Args:
            name: Database name
            
        Returns:
            Result dictionary
        """
        name = name.upper()
        sql = f"UNDROP DATABASE {name}"
        result = self.executor.execute(sql)
        
        if result["success"]:
            return {
                "success": True,
                "message": f"Database {name} restored successfully",
                "database": name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to restore database")
            }
    
    # ==================== Schema Operations ====================
    
    def create_schema(self, database: str, name: str, 
                      options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a new schema.
        
        Args:
            database: Database name
            name: Schema name
            options: Creation options
                - if_not_exists: bool
                - comment: str
                - transient: bool
                - managed_access: bool
                - clone_from: str
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        name = name.upper()
        full_name = f"{database}.{name}"
        
        parts = ["CREATE"]
        if options.get('or_replace'):
            parts.append("OR REPLACE")
        if options.get('transient'):
            parts.append("TRANSIENT")
        parts.append("SCHEMA")
        if options.get('if_not_exists'):
            parts.append("IF NOT EXISTS")
        parts.append(full_name)
        
        if options.get('clone_from'):
            parts.append(f"CLONE {options['clone_from'].upper()}")
        
        if options.get('managed_access'):
            parts.append("WITH MANAGED ACCESS")
        
        extra_opts = []
        if options.get('comment'):
            extra_opts.append(f"COMMENT = '{options['comment']}'")
        if options.get('data_retention_time_in_days') is not None:
            extra_opts.append(f"DATA_RETENTION_TIME_IN_DAYS = {options['data_retention_time_in_days']}")
        
        if extra_opts:
            parts.extend(extra_opts)
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Created schema: {full_name}")
            return {
                "success": True,
                "message": f"Schema {full_name} created successfully",
                "schema": full_name,
                "sql": sql
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to create schema"),
                "sql": sql
            }
    
    def alter_schema(self, database: str, name: str, 
                     alterations: Dict[str, Any]) -> Dict[str, Any]:
        """
        Alter a schema.
        
        Args:
            database: Database name
            name: Schema name
            alterations: Changes to make
                - rename_to: str
                - set_comment: str
                - enable_managed_access: bool
                - disable_managed_access: bool
                
        Returns:
            Result dictionary
        """
        database = database.upper()
        name = name.upper()
        full_name = f"{database}.{name}"
        results = []
        
        if alterations.get('rename_to'):
            new_name = alterations['rename_to'].upper()
            sql = f"ALTER SCHEMA {full_name} RENAME TO {database}.{new_name}"
            result = self.executor.execute(sql)
            results.append({
                "action": "rename",
                "success": result["success"],
                "error": result.get("error")
            })
            if result["success"]:
                name = new_name
                full_name = f"{database}.{name}"
        
        if alterations.get('set_comment'):
            sql = f"ALTER SCHEMA {full_name} SET COMMENT = '{alterations['set_comment']}'"
            result = self.executor.execute(sql)
            results.append({
                "action": "set_comment",
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('enable_managed_access'):
            sql = f"ALTER SCHEMA {full_name} ENABLE MANAGED ACCESS"
            result = self.executor.execute(sql)
            results.append({
                "action": "enable_managed_access",
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('disable_managed_access'):
            sql = f"ALTER SCHEMA {full_name} DISABLE MANAGED ACCESS"
            result = self.executor.execute(sql)
            results.append({
                "action": "disable_managed_access",
                "success": result["success"],
                "error": result.get("error")
            })
        
        all_success = all(r["success"] for r in results)
        
        return {
            "success": all_success,
            "schema": full_name,
            "alterations": results
        }
    
    def drop_schema(self, database: str, name: str, 
                    options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Drop a schema.
        
        Args:
            database: Database name
            name: Schema name
            options:
                - if_exists: bool
                - cascade: bool
                - restrict: bool
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        name = name.upper()
        full_name = f"{database}.{name}"
        
        parts = ["DROP SCHEMA"]
        if options.get('if_exists'):
            parts.append("IF EXISTS")
        parts.append(full_name)
        
        if options.get('cascade'):
            parts.append("CASCADE")
        elif options.get('restrict'):
            parts.append("RESTRICT")
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Dropped schema: {full_name}")
            return {
                "success": True,
                "message": f"Schema {full_name} dropped successfully",
                "schema": full_name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to drop schema")
            }
    
    def undrop_schema(self, database: str, name: str) -> Dict[str, Any]:
        """
        Undrop (restore) a dropped schema.
        
        Args:
            database: Database name
            name: Schema name
            
        Returns:
            Result dictionary
        """
        database = database.upper()
        name = name.upper()
        full_name = f"{database}.{name}"
        
        sql = f"UNDROP SCHEMA {full_name}"
        result = self.executor.execute(sql)
        
        if result["success"]:
            return {
                "success": True,
                "message": f"Schema {full_name} restored successfully",
                "schema": full_name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to restore schema")
            }
    
    # ==================== Table Operations ====================
    
    def create_table(self, database: str, schema: str, name: str,
                     columns: List[Dict[str, Any]], 
                     options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a new table.
        
        Args:
            database: Database name
            schema: Schema name
            name: Table name
            columns: List of column definitions
                - name: str
                - type: str
                - nullable: bool
                - default: any
                - primary_key: bool
                - unique: bool
                - comment: str
            options: Creation options
                - if_not_exists: bool
                - or_replace: bool
                - transient: bool
                - temporary: bool
                - cluster_by: list
                - comment: str
                - copy_grants: bool
                - like: str (table to copy structure from)
                - clone_from: str
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        parts = ["CREATE"]
        if options.get('or_replace'):
            parts.append("OR REPLACE")
        if options.get('transient'):
            parts.append("TRANSIENT")
        if options.get('temporary'):
            parts.append("TEMPORARY")
        parts.append("TABLE")
        if options.get('if_not_exists'):
            parts.append("IF NOT EXISTS")
        parts.append(full_name)
        
        # Clone from existing table
        if options.get('clone_from'):
            parts.append(f"CLONE {options['clone_from'].upper()}")
        # Like existing table
        elif options.get('like'):
            parts.append(f"LIKE {options['like'].upper()}")
        # Define columns
        elif columns:
            col_defs = []
            primary_keys = []
            
            for col in columns:
                col_name = col['name'].upper()
                col_type = col.get('type', 'VARCHAR').upper()
                
                col_def = f"{col_name} {col_type}"
                
                if not col.get('nullable', True):
                    col_def += " NOT NULL"
                
                if 'default' in col:
                    default_val = col['default']
                    if default_val is None:
                        col_def += " DEFAULT NULL"
                    elif isinstance(default_val, str):
                        col_def += f" DEFAULT '{default_val}'"
                    else:
                        col_def += f" DEFAULT {default_val}"
                
                if col.get('unique'):
                    col_def += " UNIQUE"
                
                if col.get('comment'):
                    col_def += f" COMMENT '{col['comment']}'"
                
                col_defs.append(col_def)
                
                if col.get('primary_key'):
                    primary_keys.append(col_name)
            
            # Add primary key constraint
            if primary_keys:
                col_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
            
            parts.append(f"({', '.join(col_defs)})")
        
        # Clustering
        if options.get('cluster_by'):
            cluster_cols = ", ".join(c.upper() for c in options['cluster_by'])
            parts.append(f"CLUSTER BY ({cluster_cols})")
        
        # Copy grants
        if options.get('copy_grants'):
            parts.append("COPY GRANTS")
        
        # Comment
        if options.get('comment'):
            parts.append(f"COMMENT = '{options['comment']}'")
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Created table: {full_name}")
            return {
                "success": True,
                "message": f"Table {full_name} created successfully",
                "table": full_name,
                "sql": sql
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to create table"),
                "sql": sql
            }
    
    def alter_table(self, database: str, schema: str, name: str,
                    alterations: Dict[str, Any]) -> Dict[str, Any]:
        """
        Alter a table.
        
        Args:
            database: Database name
            schema: Schema name
            name: Table name
            alterations: Changes to make
                - rename_to: str
                - set_comment: str
                - add_column: dict (name, type, nullable, default)
                - drop_column: str
                - rename_column: dict (old_name, new_name)
                - alter_column_type: dict (name, new_type)
                - alter_column_nullable: dict (name, nullable)
                - add_constraint: dict
                - drop_constraint: str
                - cluster_by: list
                - suspend_recluster: bool
                - resume_recluster: bool
                
        Returns:
            Result dictionary
        """
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        results = []
        
        if alterations.get('rename_to'):
            new_name = alterations['rename_to'].upper()
            sql = f"ALTER TABLE {full_name} RENAME TO {database}.{schema}.{new_name}"
            result = self.executor.execute(sql)
            results.append({
                "action": "rename",
                "success": result["success"],
                "error": result.get("error")
            })
            if result["success"]:
                name = new_name
                full_name = f"{database}.{schema}.{name}"
        
        if alterations.get('set_comment'):
            sql = f"ALTER TABLE {full_name} SET COMMENT = '{alterations['set_comment']}'"
            result = self.executor.execute(sql)
            results.append({
                "action": "set_comment",
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('add_column'):
            col = alterations['add_column']
            col_def = f"{col['name'].upper()} {col.get('type', 'VARCHAR').upper()}"
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in col:
                default_val = col['default']
                if default_val is None:
                    col_def += " DEFAULT NULL"
                elif isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            
            sql = f"ALTER TABLE {full_name} ADD COLUMN {col_def}"
            result = self.executor.execute(sql)
            results.append({
                "action": "add_column",
                "column": col['name'],
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('drop_column'):
            col_name = alterations['drop_column'].upper()
            sql = f"ALTER TABLE {full_name} DROP COLUMN {col_name}"
            result = self.executor.execute(sql)
            results.append({
                "action": "drop_column",
                "column": col_name,
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('rename_column'):
            rename = alterations['rename_column']
            old_name = rename['old_name'].upper()
            new_name = rename['new_name'].upper()
            sql = f"ALTER TABLE {full_name} RENAME COLUMN {old_name} TO {new_name}"
            result = self.executor.execute(sql)
            results.append({
                "action": "rename_column",
                "old_name": old_name,
                "new_name": new_name,
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('alter_column_type'):
            col_change = alterations['alter_column_type']
            col_name = col_change['name'].upper()
            new_type = col_change['new_type'].upper()
            sql = f"ALTER TABLE {full_name} ALTER COLUMN {col_name} SET DATA TYPE {new_type}"
            result = self.executor.execute(sql)
            results.append({
                "action": "alter_column_type",
                "column": col_name,
                "new_type": new_type,
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('cluster_by'):
            cluster_cols = ", ".join(c.upper() for c in alterations['cluster_by'])
            sql = f"ALTER TABLE {full_name} CLUSTER BY ({cluster_cols})"
            result = self.executor.execute(sql)
            results.append({
                "action": "cluster_by",
                "success": result["success"],
                "error": result.get("error")
            })
        
        all_success = all(r["success"] for r in results)
        
        return {
            "success": all_success,
            "table": full_name,
            "alterations": results
        }
    
    def drop_table(self, database: str, schema: str, name: str,
                   options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Drop a table.
        
        Args:
            database: Database name
            schema: Schema name
            name: Table name
            options:
                - if_exists: bool
                - cascade: bool
                - restrict: bool
                - purge: bool
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        parts = ["DROP TABLE"]
        if options.get('if_exists'):
            parts.append("IF EXISTS")
        parts.append(full_name)
        
        if options.get('cascade'):
            parts.append("CASCADE")
        elif options.get('restrict'):
            parts.append("RESTRICT")
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"] and options.get('purge'):
            # Purge from time travel
            purge_sql = f"DROP TABLE IF EXISTS {full_name} /* purge */"
            self.executor.execute(purge_sql)
        
        if result["success"]:
            logger.info(f"Dropped table: {full_name}")
            return {
                "success": True,
                "message": f"Table {full_name} dropped successfully",
                "table": full_name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to drop table")
            }
    
    def undrop_table(self, database: str, schema: str, name: str) -> Dict[str, Any]:
        """
        Undrop (restore) a dropped table.
        
        Args:
            database: Database name
            schema: Schema name
            name: Table name
            
        Returns:
            Result dictionary
        """
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        sql = f"UNDROP TABLE {full_name}"
        result = self.executor.execute(sql)
        
        if result["success"]:
            return {
                "success": True,
                "message": f"Table {full_name} restored successfully",
                "table": full_name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to restore table")
            }
    
    def truncate_table(self, database: str, schema: str, name: str) -> Dict[str, Any]:
        """
        Truncate a table (remove all rows).
        
        Args:
            database: Database name
            schema: Schema name
            name: Table name
            
        Returns:
            Result dictionary
        """
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        sql = f"TRUNCATE TABLE {full_name}"
        result = self.executor.execute(sql)
        
        if result["success"]:
            return {
                "success": True,
                "message": f"Table {full_name} truncated successfully",
                "table": full_name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to truncate table")
            }
    
    # ==================== View Operations ====================
    
    def create_view(self, database: str, schema: str, name: str,
                    definition: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a view.
        
        Args:
            database: Database name
            schema: Schema name
            name: View name
            definition: SELECT statement for the view
            options:
                - if_not_exists: bool
                - or_replace: bool
                - secure: bool
                - comment: str
                - copy_grants: bool
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        parts = ["CREATE"]
        if options.get('or_replace'):
            parts.append("OR REPLACE")
        if options.get('secure'):
            parts.append("SECURE")
        parts.append("VIEW")
        if options.get('if_not_exists'):
            parts.append("IF NOT EXISTS")
        parts.append(full_name)
        
        if options.get('copy_grants'):
            parts.append("COPY GRANTS")
        
        if options.get('comment'):
            parts.append(f"COMMENT = '{options['comment']}'")
        
        parts.append("AS")
        parts.append(definition)
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Created view: {full_name}")
            return {
                "success": True,
                "message": f"View {full_name} created successfully",
                "view": full_name,
                "sql": sql
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to create view"),
                "sql": sql
            }
    
    def alter_view(self, database: str, schema: str, name: str,
                   alterations: Dict[str, Any]) -> Dict[str, Any]:
        """
        Alter a view.
        
        Args:
            database: Database name
            schema: Schema name
            name: View name
            alterations:
                - rename_to: str
                - set_comment: str
                - set_secure: bool
                - unset_secure: bool
                
        Returns:
            Result dictionary
        """
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        results = []
        
        if alterations.get('rename_to'):
            new_name = alterations['rename_to'].upper()
            sql = f"ALTER VIEW {full_name} RENAME TO {database}.{schema}.{new_name}"
            result = self.executor.execute(sql)
            results.append({
                "action": "rename",
                "success": result["success"],
                "error": result.get("error")
            })
            if result["success"]:
                name = new_name
                full_name = f"{database}.{schema}.{name}"
        
        if alterations.get('set_comment'):
            sql = f"ALTER VIEW {full_name} SET COMMENT = '{alterations['set_comment']}'"
            result = self.executor.execute(sql)
            results.append({
                "action": "set_comment",
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('set_secure'):
            sql = f"ALTER VIEW {full_name} SET SECURE"
            result = self.executor.execute(sql)
            results.append({
                "action": "set_secure",
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('unset_secure'):
            sql = f"ALTER VIEW {full_name} UNSET SECURE"
            result = self.executor.execute(sql)
            results.append({
                "action": "unset_secure",
                "success": result["success"],
                "error": result.get("error")
            })
        
        all_success = all(r["success"] for r in results)
        
        return {
            "success": all_success,
            "view": full_name,
            "alterations": results
        }
    
    def drop_view(self, database: str, schema: str, name: str,
                  options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Drop a view.
        
        Args:
            database: Database name
            schema: Schema name
            name: View name
            options:
                - if_exists: bool
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        parts = ["DROP VIEW"]
        if options.get('if_exists'):
            parts.append("IF EXISTS")
        parts.append(full_name)
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Dropped view: {full_name}")
            return {
                "success": True,
                "message": f"View {full_name} dropped successfully",
                "view": full_name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to drop view")
            }
    
    # ==================== Stage Operations ====================
    
    def create_stage(self, database: str, schema: str, name: str,
                     options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a stage for data loading/unloading.
        
        Args:
            database: Database name
            schema: Schema name
            name: Stage name
            options:
                - if_not_exists: bool
                - or_replace: bool
                - temporary: bool
                - url: str (external stage URL)
                - storage_integration: str
                - credentials: dict (aws_key_id, aws_secret_key, etc.)
                - encryption: dict (type, master_key)
                - file_format: str or dict
                - copy_options: dict
                - comment: str
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        parts = ["CREATE"]
        if options.get('or_replace'):
            parts.append("OR REPLACE")
        if options.get('temporary'):
            parts.append("TEMPORARY")
        parts.append("STAGE")
        if options.get('if_not_exists'):
            parts.append("IF NOT EXISTS")
        parts.append(full_name)
        
        # External stage URL
        if options.get('url'):
            parts.append(f"URL = '{options['url']}'")
        
        # Storage integration
        if options.get('storage_integration'):
            parts.append(f"STORAGE_INTEGRATION = {options['storage_integration']}")
        
        # Credentials (for external stages)
        if options.get('credentials'):
            creds = options['credentials']
            cred_parts = []
            if 'aws_key_id' in creds:
                cred_parts.append(f"AWS_KEY_ID = '{creds['aws_key_id']}'")
            if 'aws_secret_key' in creds:
                cred_parts.append(f"AWS_SECRET_KEY = '{creds['aws_secret_key']}'")
            if 'aws_token' in creds:
                cred_parts.append(f"AWS_TOKEN = '{creds['aws_token']}'")
            if cred_parts:
                parts.append(f"CREDENTIALS = ({' '.join(cred_parts)})")
        
        # Encryption
        if options.get('encryption'):
            enc = options['encryption']
            enc_parts = []
            if 'type' in enc:
                enc_parts.append(f"TYPE = '{enc['type']}'")
            if 'master_key' in enc:
                enc_parts.append(f"MASTER_KEY = '{enc['master_key']}'")
            if enc_parts:
                parts.append(f"ENCRYPTION = ({' '.join(enc_parts)})")
        
        # File format
        if options.get('file_format'):
            ff = options['file_format']
            if isinstance(ff, str):
                parts.append(f"FILE_FORMAT = {ff}")
            elif isinstance(ff, dict):
                ff_parts = []
                for k, v in ff.items():
                    if isinstance(v, str):
                        ff_parts.append(f"{k.upper()} = '{v}'")
                    else:
                        ff_parts.append(f"{k.upper()} = {v}")
                parts.append(f"FILE_FORMAT = ({' '.join(ff_parts)})")
        
        # Comment
        if options.get('comment'):
            parts.append(f"COMMENT = '{options['comment']}'")
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Created stage: {full_name}")
            return {
                "success": True,
                "message": f"Stage {full_name} created successfully",
                "stage": full_name,
                "sql": sql
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to create stage"),
                "sql": sql
            }
    
    def alter_stage(self, database: str, schema: str, name: str,
                    alterations: Dict[str, Any]) -> Dict[str, Any]:
        """
        Alter a stage.
        
        Args:
            database: Database name
            schema: Schema name
            name: Stage name
            alterations:
                - rename_to: str
                - set_url: str
                - set_credentials: dict
                - set_file_format: str or dict
                - set_comment: str
                
        Returns:
            Result dictionary
        """
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        results = []
        
        if alterations.get('rename_to'):
            new_name = alterations['rename_to'].upper()
            sql = f"ALTER STAGE {full_name} RENAME TO {database}.{schema}.{new_name}"
            result = self.executor.execute(sql)
            results.append({
                "action": "rename",
                "success": result["success"],
                "error": result.get("error")
            })
            if result["success"]:
                name = new_name
                full_name = f"{database}.{schema}.{name}"
        
        if alterations.get('set_url'):
            sql = f"ALTER STAGE {full_name} SET URL = '{alterations['set_url']}'"
            result = self.executor.execute(sql)
            results.append({
                "action": "set_url",
                "success": result["success"],
                "error": result.get("error")
            })
        
        if alterations.get('set_comment'):
            sql = f"ALTER STAGE {full_name} SET COMMENT = '{alterations['set_comment']}'"
            result = self.executor.execute(sql)
            results.append({
                "action": "set_comment",
                "success": result["success"],
                "error": result.get("error")
            })
        
        all_success = all(r["success"] for r in results)
        
        return {
            "success": all_success,
            "stage": full_name,
            "alterations": results
        }
    
    def drop_stage(self, database: str, schema: str, name: str,
                   options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Drop a stage.
        
        Args:
            database: Database name
            schema: Schema name
            name: Stage name
            options:
                - if_exists: bool
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        parts = ["DROP STAGE"]
        if options.get('if_exists'):
            parts.append("IF EXISTS")
        parts.append(full_name)
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Dropped stage: {full_name}")
            return {
                "success": True,
                "message": f"Stage {full_name} dropped successfully",
                "stage": full_name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to drop stage")
            }
    
    # ==================== File Format Operations ====================
    
    def create_file_format(self, database: str, schema: str, name: str,
                           format_type: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a file format.
        
        Args:
            database: Database name
            schema: Schema name
            name: File format name
            format_type: Type (CSV, JSON, AVRO, ORC, PARQUET, XML)
            options: Format-specific options
                - Common: if_not_exists, or_replace, comment
                - CSV: compression, record_delimiter, field_delimiter, 
                       skip_header, field_optionally_enclosed_by, etc.
                - JSON: compression, strip_outer_array, strip_null_values, etc.
                - PARQUET: compression, snappy_compression, etc.
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        format_type = format_type.upper()
        
        parts = ["CREATE"]
        if options.get('or_replace'):
            parts.append("OR REPLACE")
        parts.append("FILE FORMAT")
        if options.get('if_not_exists'):
            parts.append("IF NOT EXISTS")
        parts.append(full_name)
        
        # Format type and options
        format_opts = [f"TYPE = {format_type}"]
        
        # Common options
        if 'compression' in options:
            format_opts.append(f"COMPRESSION = {options['compression'].upper()}")
        
        # CSV-specific options
        if format_type == 'CSV':
            if 'record_delimiter' in options:
                format_opts.append(f"RECORD_DELIMITER = '{options['record_delimiter']}'")
            if 'field_delimiter' in options:
                format_opts.append(f"FIELD_DELIMITER = '{options['field_delimiter']}'")
            if 'skip_header' in options:
                format_opts.append(f"SKIP_HEADER = {options['skip_header']}")
            if 'field_optionally_enclosed_by' in options:
                format_opts.append(f"FIELD_OPTIONALLY_ENCLOSED_BY = '{options['field_optionally_enclosed_by']}'")
            if 'null_if' in options:
                null_values = ", ".join(f"'{v}'" for v in options['null_if'])
                format_opts.append(f"NULL_IF = ({null_values})")
            if 'trim_space' in options:
                format_opts.append(f"TRIM_SPACE = {str(options['trim_space']).upper()}")
            if 'error_on_column_count_mismatch' in options:
                format_opts.append(f"ERROR_ON_COLUMN_COUNT_MISMATCH = {str(options['error_on_column_count_mismatch']).upper()}")
        
        # JSON-specific options
        elif format_type == 'JSON':
            if 'strip_outer_array' in options:
                format_opts.append(f"STRIP_OUTER_ARRAY = {str(options['strip_outer_array']).upper()}")
            if 'strip_null_values' in options:
                format_opts.append(f"STRIP_NULL_VALUES = {str(options['strip_null_values']).upper()}")
            if 'ignore_utf8_errors' in options:
                format_opts.append(f"IGNORE_UTF8_ERRORS = {str(options['ignore_utf8_errors']).upper()}")
        
        # Parquet-specific options
        elif format_type == 'PARQUET':
            if 'snappy_compression' in options:
                format_opts.append(f"SNAPPY_COMPRESSION = {str(options['snappy_compression']).upper()}")
        
        # Comment
        if options.get('comment'):
            format_opts.append(f"COMMENT = '{options['comment']}'")
        
        parts.append(f"({' '.join(format_opts)})")
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Created file format: {full_name}")
            return {
                "success": True,
                "message": f"File format {full_name} created successfully",
                "file_format": full_name,
                "sql": sql
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to create file format"),
                "sql": sql
            }
    
    def drop_file_format(self, database: str, schema: str, name: str,
                         options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Drop a file format.
        
        Args:
            database: Database name
            schema: Schema name
            name: File format name
            options:
                - if_exists: bool
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        parts = ["DROP FILE FORMAT"]
        if options.get('if_exists'):
            parts.append("IF EXISTS")
        parts.append(full_name)
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Dropped file format: {full_name}")
            return {
                "success": True,
                "message": f"File format {full_name} dropped successfully",
                "file_format": full_name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to drop file format")
            }
    
    # ==================== Sequence Operations ====================
    
    def create_sequence(self, database: str, schema: str, name: str,
                        options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a sequence.
        
        Args:
            database: Database name
            schema: Schema name
            name: Sequence name
            options:
                - if_not_exists: bool
                - or_replace: bool
                - start: int
                - increment: int
                - comment: str
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        parts = ["CREATE"]
        if options.get('or_replace'):
            parts.append("OR REPLACE")
        parts.append("SEQUENCE")
        if options.get('if_not_exists'):
            parts.append("IF NOT EXISTS")
        parts.append(full_name)
        
        if options.get('start') is not None:
            parts.append(f"START = {options['start']}")
        
        if options.get('increment') is not None:
            parts.append(f"INCREMENT = {options['increment']}")
        
        if options.get('comment'):
            parts.append(f"COMMENT = '{options['comment']}'")
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Created sequence: {full_name}")
            return {
                "success": True,
                "message": f"Sequence {full_name} created successfully",
                "sequence": full_name,
                "sql": sql
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to create sequence"),
                "sql": sql
            }
    
    def drop_sequence(self, database: str, schema: str, name: str,
                      options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Drop a sequence.
        
        Args:
            database: Database name
            schema: Schema name
            name: Sequence name
            options:
                - if_exists: bool
                
        Returns:
            Result dictionary
        """
        options = options or {}
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        parts = ["DROP SEQUENCE"]
        if options.get('if_exists'):
            parts.append("IF EXISTS")
        parts.append(full_name)
        
        sql = " ".join(parts)
        result = self.executor.execute(sql)
        
        if result["success"]:
            logger.info(f"Dropped sequence: {full_name}")
            return {
                "success": True,
                "message": f"Sequence {full_name} dropped successfully",
                "sequence": full_name
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to drop sequence")
            }
    
    # ==================== Utility Methods ====================
    
    def get_create_statement(self, object_type: str, database: str, 
                             schema: str, name: str) -> Dict[str, Any]:
        """
        Get the CREATE statement for an object.
        
        Args:
            object_type: Type of object (TABLE, VIEW, STAGE, etc.)
            database: Database name
            schema: Schema name
            name: Object name
            
        Returns:
            Dictionary with the DDL statement
        """
        object_type = object_type.upper()
        database = database.upper()
        schema = schema.upper()
        name = name.upper()
        full_name = f"{database}.{schema}.{name}"
        
        sql = f"SELECT GET_DDL('{object_type}', '{full_name}')"
        result = self.executor.execute(sql)
        
        if result["success"] and result["data"]:
            return {
                "success": True,
                "object_type": object_type,
                "object_name": full_name,
                "ddl": result["data"][0][0]
            }
        else:
            # Try to reconstruct DDL from metadata
            if object_type == 'TABLE':
                table_info = self.metadata.get_table_info(database, schema, name)
                if table_info:
                    col_defs = []
                    for col in table_info.get('columns', []):
                        col_def = f"{col['name']} {col['type']}"
                        if not col.get('nullable', True):
                            col_def += " NOT NULL"
                        col_defs.append(col_def)
                    
                    ddl = f"CREATE TABLE {full_name} (\n    " + ",\n    ".join(col_defs) + "\n);"
                    return {
                        "success": True,
                        "object_type": object_type,
                        "object_name": full_name,
                        "ddl": ddl
                    }
            
            return {
                "success": False,
                "error": "Failed to get DDL for object"
            }
    
    def list_object_types(self) -> List[Dict[str, Any]]:
        """
        List supported object types.
        
        Returns:
            List of object type information
        """
        return [
            {"type": "DATABASE", "supports_schema": False, "supports_clone": True},
            {"type": "SCHEMA", "supports_schema": True, "supports_clone": True},
            {"type": "TABLE", "supports_schema": True, "supports_clone": True},
            {"type": "VIEW", "supports_schema": True, "supports_clone": False},
            {"type": "STAGE", "supports_schema": True, "supports_clone": False},
            {"type": "FILE_FORMAT", "supports_schema": True, "supports_clone": False},
            {"type": "SEQUENCE", "supports_schema": True, "supports_clone": False},
            {"type": "STREAM", "supports_schema": True, "supports_clone": False},
            {"type": "TASK", "supports_schema": True, "supports_clone": False},
            {"type": "PIPE", "supports_schema": True, "supports_clone": False},
        ]
