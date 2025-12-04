"""
Tests for new Snowglobe features:
- Worksheet ordering
- Data import
- Information Schema
- Workspace management
- Stacked filtering
"""

import pytest
import os
import tempfile
import json
from datetime import datetime


class TestWorksheetOrdering:
    """Tests for worksheet ordering functionality."""
    
    @pytest.fixture
    def workspace_manager(self):
        from snowglobe_server.workspace import WorkspaceManager
        with tempfile.TemporaryDirectory() as tmpdir:
            yield WorkspaceManager(tmpdir)
    
    def test_worksheet_position_on_create(self, workspace_manager):
        """Worksheets should get sequential positions when created."""
        ws1 = workspace_manager.create_worksheet("Worksheet 1")
        ws2 = workspace_manager.create_worksheet("Worksheet 2")
        ws3 = workspace_manager.create_worksheet("Worksheet 3")
        
        assert ws1['position'] == 0
        assert ws2['position'] == 1
        assert ws3['position'] == 2
    
    def test_worksheet_ordering_preserved(self, workspace_manager):
        """Worksheets should be returned in position order."""
        workspace_manager.create_worksheet("Worksheet A")
        workspace_manager.create_worksheet("Worksheet B")
        workspace_manager.create_worksheet("Worksheet C")
        
        worksheets = workspace_manager.list_worksheets()
        names = [w['name'] for w in worksheets]
        
        assert names == ["Worksheet A", "Worksheet B", "Worksheet C"]
    
    def test_reorder_worksheets(self, workspace_manager):
        """Reordering should change the position order."""
        ws1 = workspace_manager.create_worksheet("First")
        ws2 = workspace_manager.create_worksheet("Second")
        ws3 = workspace_manager.create_worksheet("Third")
        
        # Reorder to: Third, First, Second
        workspace_manager.reorder_worksheets([ws3['id'], ws1['id'], ws2['id']])
        
        worksheets = workspace_manager.list_worksheets()
        names = [w['name'] for w in worksheets]
        
        assert names == ["Third", "First", "Second"]
    
    def test_custom_position_on_create(self, workspace_manager):
        """Worksheet can be created at a specific position."""
        ws1 = workspace_manager.create_worksheet("First", position=0)
        ws2 = workspace_manager.create_worksheet("Second", position=5)  # Custom position
        ws3 = workspace_manager.create_worksheet("Third")  # Should auto-assign
        
        assert ws1['position'] == 0
        assert ws2['position'] == 5
        # Third should get position 6 (max + 1)
        assert ws3['position'] == 6


class TestInformationSchema:
    """Tests for INFORMATION_SCHEMA functionality."""
    
    @pytest.fixture
    def info_schema(self):
        from snowglobe_server.metadata import MetadataStore
        from snowglobe_server.information_schema import InformationSchemaBuilder
        
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata = MetadataStore(tmpdir)
            yield InformationSchemaBuilder(metadata)
    
    def test_get_databases(self, info_schema):
        """Should return database information."""
        databases = info_schema.get_databases()
        
        assert len(databases) >= 1
        db = databases[0]
        assert 'DATABASE_NAME' in db
        assert 'DATABASE_OWNER' in db
        assert 'CREATED' in db
    
    def test_get_schemata(self, info_schema):
        """Should return schema information."""
        schemata = info_schema.get_schemata()
        
        assert len(schemata) >= 1
        schema = schemata[0]
        assert 'CATALOG_NAME' in schema
        assert 'SCHEMA_NAME' in schema
        assert 'SCHEMA_OWNER' in schema
    
    def test_get_tables(self, info_schema):
        """Should return table information including INFORMATION_SCHEMA views."""
        tables = info_schema.get_tables()
        
        # Should include INFORMATION_SCHEMA virtual tables
        info_tables = [t for t in tables if t['TABLE_SCHEMA'] == 'INFORMATION_SCHEMA']
        assert len(info_tables) > 0
        
        # Check structure
        if tables:
            t = tables[0]
            assert 'TABLE_CATALOG' in t
            assert 'TABLE_SCHEMA' in t
            assert 'TABLE_NAME' in t
            assert 'TABLE_TYPE' in t
    
    def test_get_columns(self, info_schema):
        """Should return column information."""
        # Columns require actual tables to exist
        columns = info_schema.get_columns()
        
        # Just verify it returns a list without error
        assert isinstance(columns, list)
    
    def test_get_functions(self, info_schema):
        """Should return built-in function information."""
        functions = info_schema.get_functions()
        
        assert len(functions) > 0
        function_names = [f['FUNCTION_NAME'] for f in functions]
        
        # Check some expected built-in functions
        assert 'CURRENT_DATABASE' in function_names
        assert 'CURRENT_USER' in function_names
        assert 'SUM' in function_names
    
    def test_query_information_schema(self, info_schema):
        """Should be able to query INFORMATION_SCHEMA views."""
        result = info_schema.query_information_schema('DATABASES')
        
        assert result['success'] is True
        assert result['rowcount'] >= 1
        assert 'DATABASE_NAME' in result['columns']
    
    def test_query_with_filter(self, info_schema):
        """Should support filtering queries."""
        result = info_schema.query_information_schema(
            'SCHEMATA',
            database='SNOWGLOBE',
            filters={'SCHEMA_NAME': 'PUBLIC'}
        )
        
        assert result['success'] is True
        if result['rowcount'] > 0:
            # All results should be PUBLIC schema
            schema_idx = result['columns'].index('SCHEMA_NAME')
            for row in result['data']:
                assert row[schema_idx] == 'PUBLIC'


class TestDataImport:
    """Tests for data import functionality."""
    
    @pytest.fixture
    def importer(self):
        from snowglobe_server.query_executor import QueryExecutor
        from snowglobe_server.data_import import DataImporter
        
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = QueryExecutor(tmpdir)
            yield DataImporter(executor)
            executor.close()
    
    def test_import_sql_file(self, importer):
        """Should import SQL file and execute statements."""
        sql_content = """
        CREATE TABLE test_import (id INTEGER, name VARCHAR);
        INSERT INTO test_import VALUES (1, 'Alice');
        INSERT INTO test_import VALUES (2, 'Bob');
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            temp_path = f.name
        
        try:
            result = importer.import_sql_file(temp_path)
            
            assert result['success'] is True
            assert result['statements_total'] == 3
            assert result['statements_success'] == 3
        finally:
            os.unlink(temp_path)
    
    def test_import_csv_file(self, importer):
        """Should import CSV file into a table."""
        csv_content = """id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            result = importer.import_csv_file(temp_path, {
                'table_name': 'CSV_TEST',
                'has_header': True,
                'create_table': True
            })
            
            assert result['success'] is True
            assert result['rows_inserted'] == 3
            assert 'ID' in result['columns']
            assert 'NAME' in result['columns']
        finally:
            os.unlink(temp_path)
    
    def test_import_json_file(self, importer):
        """Should import JSON file into a table."""
        json_content = json.dumps([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ])
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(json_content)
            temp_path = f.name
        
        try:
            result = importer.import_json_file(temp_path, {
                'table_name': 'JSON_TEST'
            })
            
            assert result['success'] is True
            assert result['rows_inserted'] == 2
        finally:
            os.unlink(temp_path)
    
    def test_parse_sql_statements(self, importer):
        """Should correctly parse SQL statements."""
        sql = """
        SELECT * FROM table1;
        INSERT INTO table2 VALUES ('value with; semicolon');
        -- Comment with; semicolon
        CREATE TABLE t3 (id INT);
        """
        
        statements = importer._parse_sql_statements(sql)
        
        assert len(statements) == 3
        assert 'SELECT' in statements[0]
        assert 'INSERT' in statements[1]
        assert 'CREATE TABLE' in statements[2]
    
    def test_infer_column_types(self, importer):
        """Should infer column types from data."""
        data = [
            ['1', 'Alice', '100.5', 'true', '2024-01-01'],
            ['2', 'Bob', '200.0', 'false', '2024-01-02'],
        ]
        columns = ['id', 'name', 'amount', 'active', 'created']
        
        types = importer._infer_column_types(data, columns)
        
        assert types[0] == 'INTEGER'  # id
        assert types[1] == 'VARCHAR'  # name
        assert types[2] == 'FLOAT'    # amount
        assert types[3] == 'BOOLEAN'  # active


class TestWorkspaceManagement:
    """Tests for workspace management functionality."""
    
    @pytest.fixture
    def workspace_manager(self):
        from snowglobe_server.workspace import WorkspaceManager
        with tempfile.TemporaryDirectory() as tmpdir:
            yield WorkspaceManager(tmpdir)
    
    def test_default_workspace_created(self, workspace_manager):
        """Default workspace should be created automatically."""
        workspaces = workspace_manager.list_workspaces()
        
        assert len(workspaces) >= 1
        default = next((w for w in workspaces if w.get('is_default')), None)
        assert default is not None
    
    def test_create_workspace(self, workspace_manager):
        """Should create a new workspace."""
        workspace = workspace_manager.create_workspace(
            "My Project",
            description="Project workspace",
            icon="🚀"
        )
        
        assert workspace['name'] == "My Project"
        assert workspace['description'] == "Project workspace"
        assert workspace['icon'] == "🚀"
    
    def test_create_folder(self, workspace_manager):
        """Should create folders within workspaces."""
        workspaces = workspace_manager.list_workspaces()
        ws_id = workspaces[0]['id']
        
        folder = workspace_manager.create_folder(
            workspace_id=ws_id,
            name="Reports",
            icon="📊"
        )
        
        assert folder['name'] == "Reports"
        assert folder['workspace_id'] == ws_id
    
    def test_worksheet_in_folder(self, workspace_manager):
        """Should create worksheets within folders."""
        workspaces = workspace_manager.list_workspaces()
        ws_id = workspaces[0]['id']
        
        folder = workspace_manager.create_folder(ws_id, "Analysis")
        worksheet = workspace_manager.create_worksheet(
            "Monthly Report",
            sql="SELECT * FROM sales",
            folder_id=folder['id']
        )
        
        assert worksheet['folder_id'] == folder['id']
        
        # Should be in folder's worksheet list
        folder_data = workspace_manager.get_folder(folder['id'])
        assert worksheet['id'] in folder_data['worksheets']
    
    def test_favorites(self, workspace_manager):
        """Should manage favorite worksheets."""
        ws = workspace_manager.create_worksheet("Test")
        
        # Toggle favorite on
        is_fav = workspace_manager.toggle_favorite(ws['id'])
        assert is_fav is True
        
        favorites = workspace_manager.get_favorites()
        assert any(f['id'] == ws['id'] for f in favorites)
        
        # Toggle favorite off
        is_fav = workspace_manager.toggle_favorite(ws['id'])
        assert is_fav is False
        
        favorites = workspace_manager.get_favorites()
        assert not any(f['id'] == ws['id'] for f in favorites)
    
    def test_recent_worksheets(self, workspace_manager):
        """Should track recently accessed worksheets."""
        ws1 = workspace_manager.create_worksheet("First")
        ws2 = workspace_manager.create_worksheet("Second")
        
        workspace_manager.add_to_recent(ws1['id'])
        workspace_manager.add_to_recent(ws2['id'])
        
        recent = workspace_manager.get_recent()
        
        # Most recent should be first
        assert recent[0]['id'] == ws2['id']
        assert recent[1]['id'] == ws1['id']
    
    def test_search_worksheets(self, workspace_manager):
        """Should search worksheets by name and content."""
        workspace_manager.create_worksheet("Sales Report", sql="SELECT * FROM sales")
        workspace_manager.create_worksheet("User Analysis", sql="SELECT * FROM users")
        
        # Search by name
        results = workspace_manager.search_worksheets("Sales")
        assert len(results) == 1
        assert results[0]['name'] == "Sales Report"
        
        # Search by SQL content
        results = workspace_manager.search_worksheets("users")
        assert len(results) == 1
        assert results[0]['name'] == "User Analysis"
    
    def test_move_worksheet(self, workspace_manager):
        """Should move worksheets between folders."""
        workspaces = workspace_manager.list_workspaces()
        ws_id = workspaces[0]['id']
        
        folder1 = workspace_manager.create_folder(ws_id, "Folder 1")
        folder2 = workspace_manager.create_folder(ws_id, "Folder 2")
        
        worksheet = workspace_manager.create_worksheet(
            "Moving WS",
            folder_id=folder1['id']
        )
        
        # Move to folder2
        moved = workspace_manager.move_worksheet(worksheet['id'], folder2['id'])
        
        assert moved['folder_id'] == folder2['id']
        
        # Should be removed from folder1
        f1 = workspace_manager.get_folder(folder1['id'])
        assert worksheet['id'] not in f1['worksheets']
        
        # Should be in folder2
        f2 = workspace_manager.get_folder(folder2['id'])
        assert worksheet['id'] in f2['worksheets']
    
    def test_duplicate_worksheet(self, workspace_manager):
        """Should duplicate worksheets."""
        original = workspace_manager.create_worksheet(
            "Original",
            sql="SELECT * FROM table1",
            context={'database': 'TEST_DB'}
        )
        
        copy = workspace_manager.duplicate_worksheet(original['id'], "Copy of Original")
        
        assert copy['id'] != original['id']
        assert copy['name'] == "Copy of Original"
        assert copy['sql'] == original['sql']
        assert copy['context'] == original['context']
    
    def test_export_import_workspace(self, workspace_manager):
        """Should export and import workspaces."""
        # Create workspace with content
        ws = workspace_manager.create_workspace("Export Test")
        folder = workspace_manager.create_folder(ws['id'], "Data")
        workspace_manager.create_worksheet("Query 1", sql="SELECT 1", folder_id=folder['id'])
        
        # Export
        exported = workspace_manager.export_workspace(ws['id'])
        
        assert exported is not None
        assert exported['workspace']['name'] == "Export Test"
        assert len(exported['folders']) >= 1
        assert len(exported['worksheets']) >= 1
        
        # Import
        imported = workspace_manager.import_workspace(exported, rename="Imported Test")
        
        assert imported['name'] == "Imported Test"


class TestStackedFiltering:
    """Tests for stacked database filtering functionality."""
    
    @pytest.fixture
    def metadata(self):
        from snowglobe_server.metadata import MetadataStore
        
        with tempfile.TemporaryDirectory() as tmpdir:
            store = MetadataStore(tmpdir)
            
            # Create test data
            store.create_database("TEST_DB", if_not_exists=True)
            store.create_schema("TEST_DB", "ANALYTICS", if_not_exists=True)
            store.register_table(
                "TEST_DB", "PUBLIC", "USERS",
                [{"name": "ID", "type": "INTEGER"}, {"name": "NAME", "type": "VARCHAR"}],
                if_not_exists=True
            )
            store.register_table(
                "TEST_DB", "ANALYTICS", "METRICS",
                [{"name": "ID", "type": "INTEGER"}, {"name": "VALUE", "type": "FLOAT"}],
                if_not_exists=True
            )
            
            yield store
    
    def test_list_databases(self, metadata):
        """Should list all databases."""
        databases = metadata.list_databases()
        db_names = [d['name'] for d in databases]
        
        assert 'SNOWGLOBE' in db_names
        assert 'TEST_DB' in db_names
    
    def test_list_schemas_for_database(self, metadata):
        """Should list schemas for a specific database."""
        schemas = metadata.list_schemas("TEST_DB")
        schema_names = [s['name'] for s in schemas]
        
        assert 'PUBLIC' in schema_names
        assert 'ANALYTICS' in schema_names
    
    def test_list_tables_for_schema(self, metadata):
        """Should list tables for a specific database.schema."""
        tables = metadata.list_tables("TEST_DB", "PUBLIC")
        table_names = [t['name'] for t in tables]
        
        assert 'USERS' in table_names
        assert 'METRICS' not in table_names
        
        tables = metadata.list_tables("TEST_DB", "ANALYTICS")
        table_names = [t['name'] for t in tables]
        
        assert 'METRICS' in table_names
        assert 'USERS' not in table_names
    
    def test_get_table_info(self, metadata):
        """Should get detailed table information."""
        info = metadata.get_table_info("TEST_DB", "PUBLIC", "USERS")
        
        assert info is not None
        assert info['name'] == 'USERS'
        assert len(info['columns']) == 2
        
        col_names = [c['name'] for c in info['columns']]
        assert 'ID' in col_names
        assert 'NAME' in col_names


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
