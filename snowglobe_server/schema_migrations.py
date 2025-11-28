"""
Schema Migrations - Flyway integration for managing database migrations
Tracks and applies schema changes in order
"""

import os
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
import hashlib
import duckdb


class FlywayMigrationManager:
    """Manages database schema migrations with Flyway-compatible versioning"""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection, data_dir: str):
        self.conn = conn
        self.data_dir = data_dir
        self.migrations_dir = os.path.join(data_dir, "migrations")
        os.makedirs(self.migrations_dir, exist_ok=True)
        
        self._init_migration_table()
    
    def _init_migration_table(self):
        """Create migration tracking table"""
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS snowglobe_schema_history (
                    installed_rank INTEGER PRIMARY KEY,
                    version VARCHAR,
                    description VARCHAR,
                    type VARCHAR,
                    script VARCHAR,
                    checksum VARCHAR,
                    installed_by VARCHAR,
                    installed_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    execution_time INTEGER,
                    success BOOLEAN
                )
            """)
        except Exception:
            pass
    
    def add_migration(self, version: str, description: str, sql: str,
                     migration_type: str = "SQL") -> Dict[str, Any]:
        """
        Add a new migration script
        
        Args:
            version: Version number (e.g., "1.0", "2.1", "001")
            description: Migration description
            sql: SQL script content
            migration_type: SQL or UNDO
        """
        # Generate filename: V{version}__{description}.sql
        filename = f"V{version}__{description.replace(' ', '_')}.sql"
        filepath = os.path.join(self.migrations_dir, filename)
        
        # Save migration file
        with open(filepath, 'w') as f:
            f.write(sql)
        
        return {
            "success": True,
            "message": f"Migration {version} added successfully",
            "filename": filename,
            "path": filepath
        }
    
    def migrate(self, target_version: Optional[str] = None) -> Dict[str, Any]:
        """
        Apply pending migrations up to target version
        
        Args:
            target_version: Target version to migrate to (None = latest)
        """
        # Get pending migrations
        pending = self._get_pending_migrations()
        
        if not pending:
            return {
                "success": True,
                "message": "No pending migrations",
                "applied": []
            }
        
        applied_migrations = []
        errors = []
        
        for migration in pending:
            if target_version and migration["version"] > target_version:
                break
            
            result = self._apply_migration(migration)
            
            if result["success"]:
                applied_migrations.append(migration["version"])
            else:
                errors.append({
                    "version": migration["version"],
                    "error": result["error"]
                })
                break  # Stop on first error
        
        return {
            "success": len(errors) == 0,
            "message": f"Applied {len(applied_migrations)} migration(s)",
            "applied": applied_migrations,
            "errors": errors
        }
    
    def _apply_migration(self, migration: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a single migration"""
        start_time = datetime.now()
        
        try:
            # Read SQL script
            with open(migration["filepath"], 'r') as f:
                sql = f.read()
            
            # Split into statements and execute
            statements = self._split_sql_statements(sql)
            
            for stmt in statements:
                if stmt.strip():
                    self.conn.execute(stmt)
            
            end_time = datetime.now()
            execution_time = int((end_time - start_time).total_seconds() * 1000)
            
            # Record in history
            checksum = self._calculate_checksum(sql)
            installed_rank = self._get_next_rank()
            
            self.conn.execute("""
                INSERT INTO snowglobe_schema_history
                (installed_rank, version, description, type, script, checksum, 
                 installed_by, execution_time, success)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                installed_rank,
                migration["version"],
                migration["description"],
                migration["type"],
                migration["filename"],
                checksum,
                "SNOWGLOBE",
                execution_time,
                True
            ])
            
            return {
                "success": True,
                "version": migration["version"],
                "execution_time_ms": execution_time
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def rollback(self, target_version: str) -> Dict[str, Any]:
        """
        Rollback to a specific version (requires UNDO migrations)
        
        Args:
            target_version: Version to roll back to
        """
        # Get applied migrations after target version
        query = """
            SELECT version, description, script
            FROM snowglobe_schema_history
            WHERE version > ? AND success = true
            ORDER BY version DESC
        """
        
        result = self.conn.execute(query, [target_version])
        migrations_to_undo = result.fetchall()
        
        if not migrations_to_undo:
            return {
                "success": True,
                "message": "Already at target version",
                "rolled_back": []
            }
        
        rolled_back = []
        
        for row in migrations_to_undo:
            version, description, script = row
            
            # Look for corresponding UNDO script
            undo_script = script.replace("V", "U", 1)
            undo_path = os.path.join(self.migrations_dir, undo_script)
            
            if os.path.exists(undo_path):
                with open(undo_path, 'r') as f:
                    undo_sql = f.read()
                
                try:
                    statements = self._split_sql_statements(undo_sql)
                    for stmt in statements:
                        if stmt.strip():
                            self.conn.execute(stmt)
                    
                    rolled_back.append(version)
                    
                except Exception as e:
                    return {
                        "success": False,
                        "error": f"Failed to rollback {version}: {str(e)}",
                        "rolled_back": rolled_back
                    }
        
        return {
            "success": True,
            "message": f"Rolled back {len(rolled_back)} migration(s)",
            "rolled_back": rolled_back
        }
    
    def info(self) -> Dict[str, Any]:
        """Get migration status information"""
        # Get applied migrations
        query = """
            SELECT version, description, installed_on, execution_time, success
            FROM snowglobe_schema_history
            ORDER BY installed_rank
        """
        
        result = self.conn.execute(query)
        applied = []
        
        for row in result.fetchall():
            applied.append({
                "version": row[0],
                "description": row[1],
                "installed_on": row[2].isoformat() if row[2] else None,
                "execution_time_ms": row[3],
                "success": row[4]
            })
        
        # Get pending migrations
        pending = self._get_pending_migrations()
        
        current_version = applied[-1]["version"] if applied else None
        
        return {
            "current_version": current_version,
            "applied_migrations": len(applied),
            "pending_migrations": len(pending),
            "applied": applied,
            "pending": [{"version": m["version"], "description": m["description"]} for m in pending]
        }
    
    def validate(self) -> Dict[str, Any]:
        """Validate applied migrations against migration files"""
        query = """
            SELECT version, checksum, script
            FROM snowglobe_schema_history
            WHERE success = true
            ORDER BY installed_rank
        """
        
        result = self.conn.execute(query)
        validation_errors = []
        
        for row in result.fetchall():
            version, stored_checksum, script = row
            filepath = os.path.join(self.migrations_dir, script)
            
            if not os.path.exists(filepath):
                validation_errors.append({
                    "version": version,
                    "error": "Migration file not found"
                })
                continue
            
            with open(filepath, 'r') as f:
                sql = f.read()
            
            current_checksum = self._calculate_checksum(sql)
            
            if current_checksum != stored_checksum:
                validation_errors.append({
                    "version": version,
                    "error": "Checksum mismatch - migration file has been modified"
                })
        
        return {
            "valid": len(validation_errors) == 0,
            "errors": validation_errors
        }
    
    def _get_pending_migrations(self) -> List[Dict[str, Any]]:
        """Get list of pending migrations"""
        # Get applied versions
        query = "SELECT version FROM snowglobe_schema_history WHERE success = true"
        result = self.conn.execute(query)
        applied_versions = set(row[0] for row in result.fetchall())
        
        # Scan migration directory
        pending = []
        
        if os.path.exists(self.migrations_dir):
            for filename in sorted(os.listdir(self.migrations_dir)):
                if filename.startswith('V') and filename.endswith('.sql'):
                    # Parse version from filename: V1.0__Description.sql
                    match = re.match(r'V(.+?)__(.+)\.sql', filename)
                    if match:
                        version = match.group(1)
                        description = match.group(2).replace('_', ' ')
                        
                        if version not in applied_versions:
                            pending.append({
                                "version": version,
                                "description": description,
                                "filename": filename,
                                "filepath": os.path.join(self.migrations_dir, filename),
                                "type": "SQL"
                            })
        
        return pending
    
    def _get_next_rank(self) -> int:
        """Get next migration rank"""
        query = "SELECT COALESCE(MAX(installed_rank), 0) + 1 FROM snowglobe_schema_history"
        result = self.conn.execute(query)
        return result.fetchone()[0]
    
    def _calculate_checksum(self, content: str) -> str:
        """Calculate MD5 checksum of migration content"""
        return hashlib.md5(content.encode()).hexdigest()
    
    def _split_sql_statements(self, sql: str) -> List[str]:
        """Split SQL script into individual statements"""
        # Simple split by semicolon (should handle more complex cases)
        statements = []
        current = []
        
        for line in sql.split('\n'):
            # Skip comments
            if line.strip().startswith('--'):
                continue
            
            current.append(line)
            
            if line.strip().endswith(';'):
                statements.append('\n'.join(current))
                current = []
        
        if current:
            statements.append('\n'.join(current))
        
        return statements
    
    def clean(self, confirm: bool = False) -> Dict[str, Any]:
        """Drop all objects and clear migration history (destructive!)"""
        if not confirm:
            return {
                "success": False,
                "error": "Must confirm clean operation (set confirm=True)"
            }
        
        try:
            # Truncate history table
            self.conn.execute("DELETE FROM snowglobe_schema_history")
            
            return {
                "success": True,
                "message": "Migration history cleaned"
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
