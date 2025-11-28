"""
File Operations - Support for COPY, PUT, GET, and REMOVE commands
Enables data loading/unloading from local and cloud storage
"""

import os
import shutil
import csv
import json
from typing import Dict, Any, List, Optional
from pathlib import Path
import duckdb


class FileOperationsManager:
    """Manages Snowflake file operations (PUT, GET, COPY, REMOVE)"""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection, data_dir: str):
        self.conn = conn
        self.data_dir = data_dir
        self.stage_dir = os.path.join(data_dir, "stages")
        os.makedirs(self.stage_dir, exist_ok=True)
        
        # Track internal and external stages
        self.stages = {}
        self._create_default_stages()
    
    def _create_default_stages(self):
        """Create default internal stages"""
        for stage_name in ['@~', '@%', '@temp']:
            stage_path = os.path.join(self.stage_dir, stage_name.replace('@', ''))
            os.makedirs(stage_path, exist_ok=True)
            self.stages[stage_name] = {
                "type": "INTERNAL",
                "path": stage_path,
                "encryption": "SNOWFLAKE_SSE"
            }
    
    def create_stage(self, stage_name: str, url: Optional[str] = None,
                    credentials: Optional[Dict[str, str]] = None,
                    file_format: Optional[str] = None,
                    encryption: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a named stage (internal or external)
        
        Args:
            stage_name: Name of the stage (e.g., '@my_stage')
            url: S3/Azure/GCS URL for external stage
            credentials: Cloud credentials
            file_format: Default file format
            encryption: Encryption type
        """
        if not stage_name.startswith('@'):
            stage_name = f'@{stage_name}'
        
        if url:
            # External stage
            self.stages[stage_name] = {
                "type": "EXTERNAL",
                "url": url,
                "credentials": credentials or {},
                "file_format": file_format,
                "encryption": encryption or "NONE"
            }
        else:
            # Internal stage
            stage_path = os.path.join(self.stage_dir, stage_name.replace('@', ''))
            os.makedirs(stage_path, exist_ok=True)
            self.stages[stage_name] = {
                "type": "INTERNAL",
                "path": stage_path,
                "file_format": file_format,
                "encryption": encryption or "SNOWFLAKE_SSE"
            }
        
        return {
            "success": True,
            "message": f"Stage {stage_name} created successfully",
            "type": self.stages[stage_name]["type"]
        }
    
    def put_file(self, local_path: str, stage_location: str,
                auto_compress: bool = True, overwrite: bool = False) -> Dict[str, Any]:
        """
        PUT - Upload local file to stage
        
        Args:
            local_path: Local file path or pattern
            stage_location: Stage location (e.g., '@my_stage/path/')
            auto_compress: Automatically compress files
            overwrite: Overwrite existing files
        """
        # Parse stage location
        stage_name, stage_path = self._parse_stage_location(stage_location)
        
        if stage_name not in self.stages:
            return {"success": False, "error": f"Stage {stage_name} does not exist"}
        
        stage_config = self.stages[stage_name]
        
        if stage_config["type"] != "INTERNAL":
            return {"success": False, "error": "PUT only supports internal stages"}
        
        target_dir = os.path.join(stage_config["path"], stage_path)
        os.makedirs(target_dir, exist_ok=True)
        
        # Handle file patterns
        from glob import glob
        files = glob(local_path)
        
        if not files:
            return {"success": False, "error": f"No files found matching {local_path}"}
        
        uploaded_files = []
        for file_path in files:
            filename = os.path.basename(file_path)
            target_path = os.path.join(target_dir, filename)
            
            if os.path.exists(target_path) and not overwrite:
                continue
            
            shutil.copy2(file_path, target_path)
            file_size = os.path.getsize(target_path)
            
            uploaded_files.append({
                "source": file_path,
                "target": f"{stage_location}/{filename}",
                "size": file_size,
                "status": "UPLOADED"
            })
        
        return {
            "success": True,
            "message": f"Uploaded {len(uploaded_files)} file(s) to {stage_location}",
            "files": uploaded_files
        }
    
    def get_file(self, stage_location: str, local_path: str) -> Dict[str, Any]:
        """
        GET - Download file from stage to local
        
        Args:
            stage_location: Stage location with file pattern
            local_path: Local directory path
        """
        # Parse stage location
        stage_name, stage_path = self._parse_stage_location(stage_location)
        
        if stage_name not in self.stages:
            return {"success": False, "error": f"Stage {stage_name} does not exist"}
        
        stage_config = self.stages[stage_name]
        
        if stage_config["type"] != "INTERNAL":
            return {"success": False, "error": "GET only supports internal stages"}
        
        source_path = os.path.join(stage_config["path"], stage_path)
        
        # Handle patterns
        from glob import glob
        files = glob(source_path)
        
        if not files:
            return {"success": False, "error": f"No files found at {stage_location}"}
        
        os.makedirs(local_path, exist_ok=True)
        
        downloaded_files = []
        for file_path in files:
            filename = os.path.basename(file_path)
            target_path = os.path.join(local_path, filename)
            shutil.copy2(file_path, target_path)
            
            downloaded_files.append({
                "source": f"{stage_location}/{filename}",
                "target": target_path,
                "size": os.path.getsize(target_path),
                "status": "DOWNLOADED"
            })
        
        return {
            "success": True,
            "message": f"Downloaded {len(downloaded_files)} file(s) from {stage_location}",
            "files": downloaded_files
        }
    
    def copy_into_table(self, database: str, schema: str, table_name: str,
                       stage_location: str, file_format: Optional[Dict[str, Any]] = None,
                       pattern: Optional[str] = None, validation_mode: Optional[str] = None) -> Dict[str, Any]:
        """
        COPY INTO - Load data from stage into table
        
        Args:
            database: Target database
            schema: Target schema
            table_name: Target table
            stage_location: Source stage location
            file_format: File format options (CSV, JSON, PARQUET, etc.)
            pattern: File pattern to match
            validation_mode: RETURN_ERRORS, RETURN_n_ROWS, etc.
        """
        # Parse stage location
        stage_name, stage_path = self._parse_stage_location(stage_location)
        
        if stage_name not in self.stages:
            return {"success": False, "error": f"Stage {stage_name} does not exist"}
        
        stage_config = self.stages[stage_name]
        
        # Get file path
        if stage_config["type"] == "INTERNAL":
            file_path = os.path.join(stage_config["path"], stage_path)
        else:
            # External stage - would integrate with S3/cloud storage
            return {"success": False, "error": "External stage COPY not yet implemented"}
        
        # Determine file format
        format_type = (file_format or {}).get("type", "CSV").upper()
        
        table_ref = f"{database.lower()}_{schema.lower()}.{table_name}"
        
        try:
            if format_type == "CSV":
                # Use DuckDB's CSV reader
                delimiter = (file_format or {}).get("field_delimiter", ",")
                skip_header = (file_format or {}).get("skip_header", 1)
                
                copy_sql = f"""
                COPY {table_ref} FROM '{file_path}'
                (DELIMITER '{delimiter}', HEADER {skip_header > 0})
                """
                result = self.conn.execute(copy_sql)
                
            elif format_type == "JSON":
                copy_sql = f"COPY {table_ref} FROM '{file_path}' (FORMAT JSON)"
                result = self.conn.execute(copy_sql)
                
            elif format_type == "PARQUET":
                copy_sql = f"COPY {table_ref} FROM '{file_path}' (FORMAT PARQUET)"
                result = self.conn.execute(copy_sql)
            
            else:
                return {"success": False, "error": f"Unsupported file format: {format_type}"}
            
            return {
                "success": True,
                "message": f"Data loaded into {database}.{schema}.{table_name}",
                "file": stage_location,
                "status": "LOADED"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to load data: {str(e)}"
            }
    
    def copy_into_location(self, query_or_table: str, stage_location: str,
                          file_format: Optional[Dict[str, Any]] = None,
                          single_file: bool = False, max_file_size: Optional[int] = None) -> Dict[str, Any]:
        """
        COPY INTO - Unload data from table/query into stage
        
        Args:
            query_or_table: SELECT query or table name
            stage_location: Target stage location
            file_format: Output file format
            single_file: Generate single output file
            max_file_size: Maximum file size in bytes
        """
        # Parse stage location
        stage_name, stage_path = self._parse_stage_location(stage_location)
        
        if stage_name not in self.stages:
            return {"success": False, "error": f"Stage {stage_name} does not exist"}
        
        stage_config = self.stages[stage_name]
        
        if stage_config["type"] != "INTERNAL":
            return {"success": False, "error": "COPY INTO location only supports internal stages"}
        
        target_dir = os.path.join(stage_config["path"], stage_path)
        os.makedirs(target_dir, exist_ok=True)
        
        # Execute query
        result = self.conn.execute(query_or_table)
        columns = [desc[0] for desc in result.description]
        data = result.fetchall()
        
        # Determine format
        format_type = (file_format or {}).get("type", "CSV").upper()
        
        # Generate output filename
        filename = f"data_0_0_0.{format_type.lower()}"
        output_path = os.path.join(target_dir, filename)
        
        try:
            if format_type == "CSV":
                with open(output_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(data)
            
            elif format_type == "JSON":
                with open(output_path, 'w') as f:
                    records = [dict(zip(columns, row)) for row in data]
                    json.dump(records, f, indent=2, default=str)
            
            else:
                return {"success": False, "error": f"Unsupported format: {format_type}"}
            
            file_size = os.path.getsize(output_path)
            
            return {
                "success": True,
                "message": f"Data unloaded to {stage_location}",
                "files": [{
                    "name": filename,
                    "size": file_size,
                    "rows": len(data)
                }]
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to unload data: {str(e)}"
            }
    
    def remove_files(self, stage_location: str, pattern: Optional[str] = None) -> Dict[str, Any]:
        """
        REMOVE - Delete files from stage
        
        Args:
            stage_location: Stage location
            pattern: File pattern to match
        """
        # Parse stage location
        stage_name, stage_path = self._parse_stage_location(stage_location)
        
        if stage_name not in self.stages:
            return {"success": False, "error": f"Stage {stage_name} does not exist"}
        
        stage_config = self.stages[stage_name]
        
        if stage_config["type"] != "INTERNAL":
            return {"success": False, "error": "REMOVE only supports internal stages"}
        
        target_path = os.path.join(stage_config["path"], stage_path)
        
        # Handle patterns
        from glob import glob
        files = glob(target_path)
        
        removed_files = []
        for file_path in files:
            if os.path.isfile(file_path):
                os.remove(file_path)
                removed_files.append(os.path.basename(file_path))
        
        return {
            "success": True,
            "message": f"Removed {len(removed_files)} file(s)",
            "files": removed_files
        }
    
    def list_stage_files(self, stage_location: str, pattern: Optional[str] = None) -> Dict[str, Any]:
        """List files in a stage"""
        # Parse stage location
        stage_name, stage_path = self._parse_stage_location(stage_location)
        
        if stage_name not in self.stages:
            return {"success": False, "error": f"Stage {stage_name} does not exist"}
        
        stage_config = self.stages[stage_name]
        
        if stage_config["type"] != "INTERNAL":
            return {"success": False, "error": "LS only supports internal stages"}
        
        search_path = os.path.join(stage_config["path"], stage_path or "")
        
        files = []
        if os.path.exists(search_path):
            for item in os.listdir(search_path):
                item_path = os.path.join(search_path, item)
                if os.path.isfile(item_path):
                    files.append({
                        "name": item,
                        "size": os.path.getsize(item_path),
                        "last_modified": os.path.getmtime(item_path)
                    })
        
        return {
            "success": True,
            "stage": stage_name,
            "files": files
        }
    
    def _parse_stage_location(self, location: str) -> tuple:
        """Parse stage location into stage name and path"""
        parts = location.split('/', 1)
        stage_name = parts[0]
        stage_path = parts[1] if len(parts) > 1 else ""
        return stage_name, stage_path
