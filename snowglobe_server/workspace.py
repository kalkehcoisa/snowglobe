"""
Workspace Management - Snowsight-like workspace functionality for Snowglobe

Workspaces allow organizing:
- Worksheets with specific SQL and context
- Folders for grouping worksheets
- Dashboard configurations
- Shared resources between team members (in future)
"""

import os
import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
import threading
import logging


logger = logging.getLogger("snowglobe.workspace")


class WorkspaceManager:
    """
    Manages Snowsight-like workspaces.
    
    A workspace contains:
    - Folders for organization
    - Worksheets with SQL content and execution context
    - Recent items
    - Favorites
    """
    
    def __init__(self, data_dir: str = "/data"):
        """
        Initialize the workspace manager.
        
        Args:
            data_dir: Directory for persisting workspace data
        """
        self.data_dir = data_dir
        self.workspace_file = os.path.join(data_dir, "workspaces.json")
        self._lock = threading.RLock()
        self._data = self._load_data()
        
        # Initialize default workspace if needed
        if not self._data.get('workspaces'):
            self._create_default_workspace()
    
    def _load_data(self) -> Dict:
        """Load workspace data from disk."""
        if os.path.exists(self.workspace_file):
            try:
                with open(self.workspace_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load workspace data: {e}")
        
        return {
            'workspaces': {},
            'folders': {},
            'worksheets': {},
            'recent': [],
            'favorites': [],
            'settings': {
                'default_workspace': None,
                'auto_save': True,
                'auto_save_interval': 30,
            }
        }
    
    def _save_data(self):
        """Save workspace data to disk."""
        os.makedirs(self.data_dir, exist_ok=True)
        try:
            with open(self.workspace_file, 'w') as f:
                json.dump(self._data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save workspace data: {e}")
    
    def _create_default_workspace(self):
        """Create the default workspace."""
        with self._lock:
            workspace_id = 'default'
            now = datetime.utcnow().isoformat()
            
            self._data['workspaces'][workspace_id] = {
                'id': workspace_id,
                'name': 'My Worksheets',
                'description': 'Default workspace for SQL worksheets',
                'icon': '📁',
                'created_at': now,
                'updated_at': now,
                'owner': 'SNOWGLOBE_USER',
                'is_default': True,
                'folders': ['recent', 'favorites'],
                'worksheets': [],
                'settings': {
                    'theme': 'default',
                    'auto_complete': True,
                    'line_numbers': True,
                }
            }
            
            # Create default folders
            self._data['folders']['recent'] = {
                'id': 'recent',
                'name': 'Recent',
                'icon': '🕒',
                'workspace_id': workspace_id,
                'parent_id': None,
                'created_at': now,
                'is_system': True,
                'worksheets': [],
            }
            
            self._data['folders']['favorites'] = {
                'id': 'favorites',
                'name': 'Favorites',
                'icon': '⭐',
                'workspace_id': workspace_id,
                'parent_id': None,
                'created_at': now,
                'is_system': True,
                'worksheets': [],
            }
            
            self._data['settings']['default_workspace'] = workspace_id
            self._save_data()
    
    # ==================== Workspace Operations ====================
    
    def create_workspace(self, name: str, description: str = '', 
                         icon: str = '📁') -> Dict[str, Any]:
        """
        Create a new workspace.
        
        Args:
            name: Workspace name
            description: Optional description
            icon: Emoji icon for the workspace
            
        Returns:
            Created workspace data
        """
        with self._lock:
            workspace_id = f"ws_{uuid.uuid4().hex[:8]}"
            now = datetime.utcnow().isoformat()
            
            workspace = {
                'id': workspace_id,
                'name': name,
                'description': description,
                'icon': icon,
                'created_at': now,
                'updated_at': now,
                'owner': 'SNOWGLOBE_USER',
                'is_default': False,
                'folders': [],
                'worksheets': [],
                'settings': {
                    'theme': 'default',
                    'auto_complete': True,
                    'line_numbers': True,
                }
            }
            
            self._data['workspaces'][workspace_id] = workspace
            self._save_data()
            
            logger.info(f"Created workspace: {workspace_id} - {name}")
            return workspace
    
    def get_workspace(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        """Get a workspace by ID."""
        with self._lock:
            return self._data['workspaces'].get(workspace_id)
    
    def list_workspaces(self) -> List[Dict[str, Any]]:
        """List all workspaces."""
        with self._lock:
            return list(self._data['workspaces'].values())
    
    def update_workspace(self, workspace_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Update a workspace.
        
        Args:
            workspace_id: Workspace ID
            updates: Fields to update
            
        Returns:
            Updated workspace or None if not found
        """
        with self._lock:
            if workspace_id not in self._data['workspaces']:
                return None
            
            workspace = self._data['workspaces'][workspace_id]
            
            # Update allowed fields
            for key in ['name', 'description', 'icon', 'settings']:
                if key in updates:
                    workspace[key] = updates[key]
            
            workspace['updated_at'] = datetime.utcnow().isoformat()
            self._save_data()
            
            return workspace
    
    def delete_workspace(self, workspace_id: str) -> bool:
        """
        Delete a workspace.
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            True if deleted, False if not found or is default
        """
        with self._lock:
            if workspace_id not in self._data['workspaces']:
                return False
            
            workspace = self._data['workspaces'][workspace_id]
            if workspace.get('is_default'):
                logger.warning("Cannot delete default workspace")
                return False
            
            # Delete associated folders and worksheets
            for folder_id in workspace.get('folders', []):
                if folder_id in self._data['folders']:
                    del self._data['folders'][folder_id]
            
            for worksheet_id in workspace.get('worksheets', []):
                if worksheet_id in self._data['worksheets']:
                    del self._data['worksheets'][worksheet_id]
            
            del self._data['workspaces'][workspace_id]
            self._save_data()
            
            logger.info(f"Deleted workspace: {workspace_id}")
            return True
    
    # ==================== Folder Operations ====================
    
    def create_folder(self, workspace_id: str, name: str, parent_id: str = None,
                      icon: str = '📂') -> Optional[Dict[str, Any]]:
        """
        Create a folder in a workspace.
        
        Args:
            workspace_id: Parent workspace ID
            name: Folder name
            parent_id: Optional parent folder ID for nesting
            icon: Emoji icon
            
        Returns:
            Created folder data or None if workspace not found
        """
        with self._lock:
            if workspace_id not in self._data['workspaces']:
                return None
            
            folder_id = f"folder_{uuid.uuid4().hex[:8]}"
            now = datetime.utcnow().isoformat()
            
            folder = {
                'id': folder_id,
                'name': name,
                'icon': icon,
                'workspace_id': workspace_id,
                'parent_id': parent_id,
                'created_at': now,
                'updated_at': now,
                'is_system': False,
                'worksheets': [],
                'subfolders': [],
            }
            
            self._data['folders'][folder_id] = folder
            self._data['workspaces'][workspace_id]['folders'].append(folder_id)
            
            # Add to parent folder if specified
            if parent_id and parent_id in self._data['folders']:
                self._data['folders'][parent_id]['subfolders'].append(folder_id)
            
            self._save_data()
            
            logger.info(f"Created folder: {folder_id} - {name}")
            return folder
    
    def get_folder(self, folder_id: str) -> Optional[Dict[str, Any]]:
        """Get a folder by ID."""
        with self._lock:
            return self._data['folders'].get(folder_id)
    
    def list_folders(self, workspace_id: str = None) -> List[Dict[str, Any]]:
        """List folders, optionally filtered by workspace."""
        with self._lock:
            folders = list(self._data['folders'].values())
            if workspace_id:
                folders = [f for f in folders if f['workspace_id'] == workspace_id]
            return folders
    
    def update_folder(self, folder_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update a folder."""
        with self._lock:
            if folder_id not in self._data['folders']:
                return None
            
            folder = self._data['folders'][folder_id]
            
            if folder.get('is_system'):
                # Only allow certain updates to system folders
                if 'worksheets' in updates:
                    folder['worksheets'] = updates['worksheets']
            else:
                for key in ['name', 'icon', 'parent_id']:
                    if key in updates:
                        folder[key] = updates[key]
            
            folder['updated_at'] = datetime.utcnow().isoformat()
            self._save_data()
            
            return folder
    
    def delete_folder(self, folder_id: str, move_to: str = None) -> bool:
        """
        Delete a folder.
        
        Args:
            folder_id: Folder ID to delete
            move_to: Optional folder ID to move worksheets to
            
        Returns:
            True if deleted
        """
        with self._lock:
            if folder_id not in self._data['folders']:
                return False
            
            folder = self._data['folders'][folder_id]
            
            if folder.get('is_system'):
                logger.warning("Cannot delete system folder")
                return False
            
            # Move or delete worksheets
            for ws_id in folder.get('worksheets', []):
                if move_to and move_to in self._data['folders']:
                    self._data['folders'][move_to]['worksheets'].append(ws_id)
                    if ws_id in self._data['worksheets']:
                        self._data['worksheets'][ws_id]['folder_id'] = move_to
                else:
                    if ws_id in self._data['worksheets']:
                        del self._data['worksheets'][ws_id]
            
            # Remove from workspace
            workspace_id = folder['workspace_id']
            if workspace_id in self._data['workspaces']:
                ws = self._data['workspaces'][workspace_id]
                if folder_id in ws['folders']:
                    ws['folders'].remove(folder_id)
            
            # Remove from parent folder
            if folder['parent_id'] and folder['parent_id'] in self._data['folders']:
                parent = self._data['folders'][folder['parent_id']]
                if folder_id in parent.get('subfolders', []):
                    parent['subfolders'].remove(folder_id)
            
            del self._data['folders'][folder_id]
            self._save_data()
            
            return True
    
    # ==================== Worksheet Operations ====================
    
    def create_worksheet(self, name: str, sql: str = '', 
                         workspace_id: str = None, folder_id: str = None,
                         context: Dict[str, str] = None,
                         position: int = None) -> Dict[str, Any]:
        """
        Create a new worksheet.
        
        Args:
            name: Worksheet name
            sql: Initial SQL content
            workspace_id: Parent workspace ID (default workspace if not specified)
            folder_id: Optional folder ID
            context: Execution context (database, schema, warehouse, role)
            position: Position in worksheet order (append if None)
            
        Returns:
            Created worksheet data
        """
        with self._lock:
            # If folder_id is specified, get workspace from folder
            if folder_id and folder_id in self._data['folders']:
                folder_workspace_id = self._data['folders'][folder_id].get('workspace_id')
                if folder_workspace_id:
                    workspace_id = folder_workspace_id
            
            # Use default workspace if not specified
            if not workspace_id:
                workspace_id = self._data['settings'].get('default_workspace', 'default')
            
            worksheet_id = f"ws_{uuid.uuid4().hex[:12]}"
            now = datetime.utcnow().isoformat()
            
            # Determine position
            if position is None:
                # Get max position in workspace
                existing = [w for w in self._data['worksheets'].values() 
                           if w.get('workspace_id') == workspace_id]
                position = max([w.get('position', 0) for w in existing], default=-1) + 1
            
            worksheet = {
                'id': worksheet_id,
                'name': name,
                'sql': sql,
                'workspace_id': workspace_id,
                'folder_id': folder_id,
                'context': context or {
                    'database': 'SNOWGLOBE',
                    'schema': 'PUBLIC',
                    'warehouse': 'COMPUTE_WH',
                    'role': 'ACCOUNTADMIN'
                },
                'position': position,
                'created_at': now,
                'updated_at': now,
                'last_executed': None,
                'execution_count': 0,
                'is_favorite': False,
                'tags': [],
                'description': '',
                'version': 1,
                'versions': [],  # Version history
            }
            
            self._data['worksheets'][worksheet_id] = worksheet
            
            # Add to workspace
            if workspace_id in self._data['workspaces']:
                self._data['workspaces'][workspace_id]['worksheets'].append(worksheet_id)
            
            # Add to folder if specified
            if folder_id and folder_id in self._data['folders']:
                self._data['folders'][folder_id]['worksheets'].append(worksheet_id)
            
            self._save_data()
            
            logger.info(f"Created worksheet: {worksheet_id} - {name}")
            return worksheet
    
    def get_worksheet(self, worksheet_id: str) -> Optional[Dict[str, Any]]:
        """Get a worksheet by ID."""
        with self._lock:
            return self._data['worksheets'].get(worksheet_id)
    
    def list_worksheets(self, workspace_id: str = None, folder_id: str = None,
                        include_content: bool = True) -> List[Dict[str, Any]]:
        """
        List worksheets with optional filtering.
        
        Args:
            workspace_id: Filter by workspace
            folder_id: Filter by folder
            include_content: Include SQL content in results
            
        Returns:
            List of worksheets sorted by position
        """
        with self._lock:
            worksheets = list(self._data['worksheets'].values())
            
            if workspace_id:
                worksheets = [w for w in worksheets if w['workspace_id'] == workspace_id]
            
            if folder_id:
                worksheets = [w for w in worksheets if w.get('folder_id') == folder_id]
            
            # Sort by position
            worksheets.sort(key=lambda x: x.get('position', 0))
            
            if not include_content:
                # Return without SQL content for listing
                return [{k: v for k, v in w.items() if k != 'sql'} for w in worksheets]
            
            return worksheets
    
    def update_worksheet(self, worksheet_id: str, updates: Dict[str, Any],
                         save_version: bool = False) -> Optional[Dict[str, Any]]:
        """
        Update a worksheet.
        
        Args:
            worksheet_id: Worksheet ID
            updates: Fields to update
            save_version: Save current state to version history
            
        Returns:
            Updated worksheet or None if not found
        """
        with self._lock:
            if worksheet_id not in self._data['worksheets']:
                return None
            
            worksheet = self._data['worksheets'][worksheet_id]
            
            # Save version if requested
            if save_version and worksheet.get('sql'):
                version_entry = {
                    'version': worksheet.get('version', 1),
                    'sql': worksheet['sql'],
                    'saved_at': datetime.utcnow().isoformat(),
                }
                worksheet['versions'].append(version_entry)
                # Keep last 10 versions
                worksheet['versions'] = worksheet['versions'][-10:]
                worksheet['version'] = worksheet.get('version', 1) + 1
            
            # Update allowed fields
            allowed_fields = ['name', 'sql', 'context', 'folder_id', 'position',
                            'is_favorite', 'tags', 'description', 'last_executed',
                            'execution_count']
            for key in allowed_fields:
                if key in updates:
                    worksheet[key] = updates[key]
            
            worksheet['updated_at'] = datetime.utcnow().isoformat()
            self._save_data()
            
            return worksheet
    
    def delete_worksheet(self, worksheet_id: str) -> bool:
        """Delete a worksheet."""
        with self._lock:
            if worksheet_id not in self._data['worksheets']:
                return False
            
            worksheet = self._data['worksheets'][worksheet_id]
            
            # Remove from workspace
            workspace_id = worksheet.get('workspace_id')
            if workspace_id and workspace_id in self._data['workspaces']:
                ws = self._data['workspaces'][workspace_id]
                if worksheet_id in ws['worksheets']:
                    ws['worksheets'].remove(worksheet_id)
            
            # Remove from folder
            folder_id = worksheet.get('folder_id')
            if folder_id and folder_id in self._data['folders']:
                folder = self._data['folders'][folder_id]
                if worksheet_id in folder['worksheets']:
                    folder['worksheets'].remove(worksheet_id)
            
            # Remove from favorites and recent
            if worksheet_id in self._data['favorites']:
                self._data['favorites'].remove(worksheet_id)
            self._data['recent'] = [r for r in self._data['recent'] if r.get('id') != worksheet_id]
            
            del self._data['worksheets'][worksheet_id]
            self._save_data()
            
            logger.info(f"Deleted worksheet: {worksheet_id}")
            return True
    
    def reorder_worksheets(self, worksheet_ids: List[str]) -> bool:
        """
        Reorder worksheets by setting positions based on list order.
        
        Args:
            worksheet_ids: List of worksheet IDs in desired order
            
        Returns:
            True if successful
        """
        with self._lock:
            for position, ws_id in enumerate(worksheet_ids):
                if ws_id in self._data['worksheets']:
                    self._data['worksheets'][ws_id]['position'] = position
            
            self._save_data()
            return True
    
    def move_worksheet(self, worksheet_id: str, target_folder_id: str = None,
                       target_workspace_id: str = None) -> Optional[Dict[str, Any]]:
        """
        Move a worksheet to a different folder or workspace.
        
        Args:
            worksheet_id: Worksheet ID
            target_folder_id: Target folder ID (None for root)
            target_workspace_id: Target workspace ID
            
        Returns:
            Updated worksheet or None if not found
        """
        with self._lock:
            if worksheet_id not in self._data['worksheets']:
                return None
            
            worksheet = self._data['worksheets'][worksheet_id]
            old_folder_id = worksheet.get('folder_id')
            old_workspace_id = worksheet.get('workspace_id')
            
            # Remove from old folder
            if old_folder_id and old_folder_id in self._data['folders']:
                folder = self._data['folders'][old_folder_id]
                if worksheet_id in folder['worksheets']:
                    folder['worksheets'].remove(worksheet_id)
            
            # Remove from old workspace
            if old_workspace_id and old_workspace_id in self._data['workspaces']:
                ws = self._data['workspaces'][old_workspace_id]
                if worksheet_id in ws['worksheets']:
                    ws['worksheets'].remove(worksheet_id)
            
            # Add to new folder
            if target_folder_id and target_folder_id in self._data['folders']:
                self._data['folders'][target_folder_id]['worksheets'].append(worksheet_id)
                worksheet['folder_id'] = target_folder_id
                # Get workspace from folder
                target_workspace_id = self._data['folders'][target_folder_id].get('workspace_id')
            else:
                worksheet['folder_id'] = None
            
            # Add to new workspace
            if target_workspace_id and target_workspace_id in self._data['workspaces']:
                self._data['workspaces'][target_workspace_id]['worksheets'].append(worksheet_id)
                worksheet['workspace_id'] = target_workspace_id
            
            worksheet['updated_at'] = datetime.utcnow().isoformat()
            self._save_data()
            
            return worksheet
    
    def duplicate_worksheet(self, worksheet_id: str, new_name: str = None) -> Optional[Dict[str, Any]]:
        """
        Duplicate a worksheet.
        
        Args:
            worksheet_id: Source worksheet ID
            new_name: Name for the copy (default: "Copy of <original>")
            
        Returns:
            New worksheet or None if source not found
        """
        with self._lock:
            if worksheet_id not in self._data['worksheets']:
                return None
            
            source = self._data['worksheets'][worksheet_id]
            name = new_name or f"Copy of {source['name']}"
            
            return self.create_worksheet(
                name=name,
                sql=source['sql'],
                workspace_id=source['workspace_id'],
                folder_id=source.get('folder_id'),
                context=source['context'].copy()
            )
    
    # ==================== Recent & Favorites ====================
    
    def add_to_recent(self, worksheet_id: str):
        """Add a worksheet to recent items."""
        with self._lock:
            if worksheet_id not in self._data['worksheets']:
                return
            
            # Remove if already in recent
            self._data['recent'] = [r for r in self._data['recent'] if r.get('id') != worksheet_id]
            
            # Add to beginning
            self._data['recent'].insert(0, {
                'id': worksheet_id,
                'accessed_at': datetime.utcnow().isoformat()
            })
            
            # Keep last 20 recent items
            self._data['recent'] = self._data['recent'][:20]
            
            # Update recent folder
            if 'recent' in self._data['folders']:
                recent_ids = [r['id'] for r in self._data['recent']]
                self._data['folders']['recent']['worksheets'] = recent_ids
            
            self._save_data()
    
    def get_recent(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent worksheets."""
        with self._lock:
            recent = self._data['recent'][:limit]
            result = []
            for item in recent:
                ws = self._data['worksheets'].get(item['id'])
                if ws:
                    result.append({
                        **ws,
                        'accessed_at': item['accessed_at']
                    })
            return result
    
    def toggle_favorite(self, worksheet_id: str) -> bool:
        """Toggle favorite status for a worksheet."""
        with self._lock:
            if worksheet_id not in self._data['worksheets']:
                return False
            
            worksheet = self._data['worksheets'][worksheet_id]
            is_favorite = not worksheet.get('is_favorite', False)
            worksheet['is_favorite'] = is_favorite
            
            if is_favorite:
                if worksheet_id not in self._data['favorites']:
                    self._data['favorites'].append(worksheet_id)
            else:
                if worksheet_id in self._data['favorites']:
                    self._data['favorites'].remove(worksheet_id)
            
            # Update favorites folder
            if 'favorites' in self._data['folders']:
                self._data['folders']['favorites']['worksheets'] = self._data['favorites']
            
            self._save_data()
            return is_favorite
    
    def get_favorites(self) -> List[Dict[str, Any]]:
        """Get favorite worksheets."""
        with self._lock:
            return [
                self._data['worksheets'][ws_id]
                for ws_id in self._data['favorites']
                if ws_id in self._data['worksheets']
            ]
    
    # ==================== Search ====================
    
    def search_worksheets(self, query: str, workspace_id: str = None) -> List[Dict[str, Any]]:
        """
        Search worksheets by name, SQL content, or tags.
        
        Args:
            query: Search query
            workspace_id: Optional workspace filter
            
        Returns:
            Matching worksheets
        """
        with self._lock:
            query_lower = query.lower()
            results = []
            
            for ws in self._data['worksheets'].values():
                if workspace_id and ws.get('workspace_id') != workspace_id:
                    continue
                
                # Search in name
                if query_lower in ws['name'].lower():
                    results.append(ws)
                    continue
                
                # Search in SQL
                if query_lower in ws.get('sql', '').lower():
                    results.append(ws)
                    continue
                
                # Search in tags
                if any(query_lower in tag.lower() for tag in ws.get('tags', [])):
                    results.append(ws)
                    continue
                
                # Search in description
                if query_lower in ws.get('description', '').lower():
                    results.append(ws)
                    continue
            
            return results
    
    # ==================== Export/Import ====================
    
    def export_workspace(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        """
        Export a workspace with all its contents.
        
        Args:
            workspace_id: Workspace ID
            
        Returns:
            Workspace data for export
        """
        with self._lock:
            if workspace_id not in self._data['workspaces']:
                return None
            
            workspace = self._data['workspaces'][workspace_id]
            
            # Gather all associated folders
            folders = [self._data['folders'][f_id] 
                      for f_id in workspace.get('folders', [])
                      if f_id in self._data['folders']]
            
            # Gather all worksheets that belong to this workspace
            # (either directly in workspace or via workspace_id field)
            worksheets = []
            for ws_id, ws_data in self._data['worksheets'].items():
                if ws_data.get('workspace_id') == workspace_id:
                    worksheets.append(ws_data)
            
            # Also include worksheets from the workspace's list that might not have workspace_id set
            for ws_id in workspace.get('worksheets', []):
                if ws_id in self._data['worksheets']:
                    ws_data = self._data['worksheets'][ws_id]
                    if ws_data not in worksheets:
                        worksheets.append(ws_data)
            
            return {
                'workspace': workspace,
                'folders': folders,
                'worksheets': worksheets,
                'exported_at': datetime.utcnow().isoformat(),
                'version': '1.0'
            }
    
    def import_workspace(self, data: Dict[str, Any], rename: str = None) -> Optional[Dict[str, Any]]:
        """
        Import a workspace from exported data.
        
        Args:
            data: Exported workspace data
            rename: Optional new name for the workspace
            
        Returns:
            Imported workspace
        """
        with self._lock:
            if 'workspace' not in data:
                return None
            
            workspace_data = data['workspace']
            name = rename or f"{workspace_data['name']} (Imported)"
            
            # Create new workspace
            new_workspace = self.create_workspace(
                name=name,
                description=workspace_data.get('description', ''),
                icon=workspace_data.get('icon', '📁')
            )
            
            # Map old IDs to new IDs
            folder_id_map = {}
            worksheet_id_map = {}
            
            # Import folders
            for folder in data.get('folders', []):
                if folder.get('is_system'):
                    continue
                
                new_folder = self.create_folder(
                    workspace_id=new_workspace['id'],
                    name=folder['name'],
                    icon=folder.get('icon', '📂')
                )
                if new_folder:
                    folder_id_map[folder['id']] = new_folder['id']
            
            # Import worksheets
            for ws in data.get('worksheets', []):
                folder_id = folder_id_map.get(ws.get('folder_id'))
                
                new_ws = self.create_worksheet(
                    name=ws['name'],
                    sql=ws.get('sql', ''),
                    workspace_id=new_workspace['id'],
                    folder_id=folder_id,
                    context=ws.get('context')
                )
                if new_ws:
                    worksheet_id_map[ws['id']] = new_ws['id']
            
            return new_workspace
    
    # ==================== Settings ====================
    
    def get_settings(self) -> Dict[str, Any]:
        """Get workspace manager settings."""
        with self._lock:
            return self._data.get('settings', {})
    
    def update_settings(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update workspace manager settings."""
        with self._lock:
            self._data['settings'].update(updates)
            self._save_data()
            return self._data['settings']
