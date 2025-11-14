"""Tests for Metadata Store"""

import pytest


class TestDatabaseOperations:
    """Test database-level operations"""
    
    def test_default_database_created(self, metadata_store):
        """Test that default SNOWGLOBE database is created"""
        assert metadata_store.database_exists("SNOWGLOBE")
    
    def test_create_database(self, metadata_store):
        """Test creating a new database"""
        result = metadata_store.create_database("TEST_DB")
        assert result is True
        assert metadata_store.database_exists("TEST_DB")
    
    def test_create_database_if_not_exists(self, metadata_store):
        """Test creating database with IF NOT EXISTS"""
        metadata_store.create_database("TEST_DB")
        result = metadata_store.create_database("TEST_DB", if_not_exists=True)
        assert result is True
    
    def test_create_database_already_exists(self, metadata_store):
        """Test creating database that already exists"""
        metadata_store.create_database("TEST_DB")
        with pytest.raises(ValueError, match="already exists"):
            metadata_store.create_database("TEST_DB")
    
    def test_drop_database(self, metadata_store):
        """Test dropping a database"""
        metadata_store.create_database("TEST_DB")
        result = metadata_store.drop_database("TEST_DB")
        assert result is True
        assert not metadata_store.database_exists("TEST_DB")
    
    def test_drop_database_if_exists(self, metadata_store):
        """Test dropping database with IF EXISTS"""
        result = metadata_store.drop_database("NONEXISTENT", if_exists=True)
        assert result is True
    
    def test_drop_database_not_exists(self, metadata_store):
        """Test dropping database that doesn't exist"""
        with pytest.raises(ValueError, match="does not exist"):
            metadata_store.drop_database("NONEXISTENT")
    
    def test_list_databases(self, metadata_store):
        """Test listing all databases"""
        metadata_store.create_database("DB1")
        metadata_store.create_database("DB2")
        databases = metadata_store.list_databases()
        names = [db["name"] for db in databases]
        assert "SNOWGLOBE" in names
        assert "DB1" in names
        assert "DB2" in names


class TestSchemaOperations:
    """Test schema-level operations"""
    
    def test_create_schema(self, sample_database):
        """Test creating a schema"""
        result = sample_database.create_schema("TEST_DB", "RAW")
        assert result is True
        assert sample_database.schema_exists("TEST_DB", "RAW")
    
    def test_create_schema_if_not_exists(self, sample_database):
        """Test creating schema with IF NOT EXISTS"""
        result = sample_database.create_schema("TEST_DB", "ANALYTICS", if_not_exists=True)
        assert result is True
    
    def test_create_schema_already_exists(self, sample_database):
        """Test creating schema that already exists"""
        with pytest.raises(ValueError, match="already exists"):
            sample_database.create_schema("TEST_DB", "ANALYTICS")
    
    def test_drop_schema(self, sample_database):
        """Test dropping a schema"""
        result = sample_database.drop_schema("TEST_DB", "ANALYTICS")
        assert result is True
        assert not sample_database.schema_exists("TEST_DB", "ANALYTICS")
    
    def test_list_schemas(self, sample_database):
        """Test listing schemas in a database"""
        sample_database.create_schema("TEST_DB", "RAW")
        schemas = sample_database.list_schemas("TEST_DB")
        names = [s["name"] for s in schemas]
        assert "PUBLIC" in names
        assert "ANALYTICS" in names
        assert "RAW" in names


class TestTableOperations:
    """Test table-level operations"""
    
    def test_register_table(self, metadata_store):
        """Test registering a table"""
        columns = [
            {"name": "ID", "type": "INTEGER", "nullable": "N"},
            {"name": "NAME", "type": "VARCHAR", "nullable": "Y"}
        ]
        result = metadata_store.register_table("SNOWGLOBE", "PUBLIC", "USERS", columns)
        assert result is True
        assert metadata_store.table_exists("SNOWGLOBE", "PUBLIC", "USERS")
    
    def test_register_table_if_not_exists(self, sample_table):
        """Test registering table with IF NOT EXISTS"""
        columns = [{"name": "ID", "type": "INTEGER", "nullable": "N"}]
        result = sample_table.register_table("SNOWGLOBE", "PUBLIC", "USERS", columns, if_not_exists=True)
        assert result is True
    
    def test_register_table_already_exists(self, sample_table):
        """Test registering table that already exists"""
        columns = [{"name": "ID", "type": "INTEGER", "nullable": "N"}]
        with pytest.raises(ValueError, match="already exists"):
            sample_table.register_table("SNOWGLOBE", "PUBLIC", "USERS", columns)
    
    def test_drop_table(self, sample_table):
        """Test dropping a table"""
        result = sample_table.drop_table("SNOWGLOBE", "PUBLIC", "USERS")
        assert result is True
        assert not sample_table.table_exists("SNOWGLOBE", "PUBLIC", "USERS")
    
    def test_list_tables(self, sample_table):
        """Test listing tables in a schema"""
        columns = [{"name": "ID", "type": "INTEGER", "nullable": "N"}]
        sample_table.register_table("SNOWGLOBE", "PUBLIC", "ORDERS", columns)
        tables = sample_table.list_tables("SNOWGLOBE", "PUBLIC")
        names = [t["name"] for t in tables]
        assert "USERS" in names
        assert "ORDERS" in names
    
    def test_get_table_info(self, sample_table):
        """Test getting table information"""
        info = sample_table.get_table_info("SNOWGLOBE", "PUBLIC", "USERS")
        assert info is not None
        assert info["name"] == "USERS"
        assert len(info["columns"]) == 4
        assert info["columns"][0]["name"] == "ID"
    
    def test_update_table_stats(self, sample_table):
        """Test updating table statistics"""
        sample_table.update_table_stats("SNOWGLOBE", "PUBLIC", "USERS", row_count=100, bytes_size=4096)
        info = sample_table.get_table_info("SNOWGLOBE", "PUBLIC", "USERS")
        assert info["row_count"] == 100
        assert info["bytes"] == 4096


class TestViewOperations:
    """Test view-level operations"""
    
    def test_register_view(self, metadata_store):
        """Test registering a view"""
        result = metadata_store.register_view("SNOWGLOBE", "PUBLIC", "USER_VIEW", "SELECT * FROM USERS")
        assert result is True
    
    def test_drop_view(self, sample_view):
        """Test dropping a view"""
        result = sample_view.drop_view("SNOWGLOBE", "PUBLIC", "USER_VIEW")
        assert result is True


class TestUndropOperations:
    """Test UNDROP functionality"""
    
    def test_undrop_database(self, dropped_objects):
        """Test restoring a dropped database"""
        result = dropped_objects.undrop_database("DROPPED_DB")
        assert result is True
        assert dropped_objects.database_exists("DROPPED_DB")
    
    def test_undrop_database_not_found(self, metadata_store):
        """Test undrop when database not in dropped list"""
        with pytest.raises(ValueError, match="not found"):
            metadata_store.undrop_database("NEVER_EXISTED")
    
    def test_undrop_schema(self, dropped_objects):
        """Test restoring a dropped schema"""
        result = dropped_objects.undrop_schema("SNOWGLOBE", "DROPPED_SCHEMA")
        assert result is True
        assert dropped_objects.schema_exists("SNOWGLOBE", "DROPPED_SCHEMA")
    
    def test_undrop_table(self, dropped_objects):
        """Test restoring a dropped table"""
        result = dropped_objects.undrop_table("SNOWGLOBE", "PUBLIC", "DROPPED_TABLE")
        assert result is True
        assert dropped_objects.table_exists("SNOWGLOBE", "PUBLIC", "DROPPED_TABLE")
    
    def test_undrop_view(self, dropped_objects):
        """Test restoring a dropped view"""
        result = dropped_objects.undrop_view("SNOWGLOBE", "PUBLIC", "DROPPED_VIEW")
        assert result is True
    
    def test_list_dropped_databases(self, dropped_objects):
        """Test listing dropped databases"""
        databases = dropped_objects.list_dropped_databases()
        names = [db["name"] for db in databases]
        assert "DROPPED_DB" in names
    
    def test_list_dropped_schemas(self, dropped_objects):
        """Test listing dropped schemas"""
        schemas = dropped_objects.list_dropped_schemas("SNOWGLOBE")
        names = [s["name"] for s in schemas]
        assert "DROPPED_SCHEMA" in names
    
    def test_list_dropped_tables(self, dropped_objects):
        """Test listing dropped tables"""
        tables = dropped_objects.list_dropped_tables("SNOWGLOBE", "PUBLIC")
        names = [t["name"] for t in tables]
        assert "DROPPED_TABLE" in names


class TestCaseInsensitivity:
    """Test case insensitivity of names"""
    
    def test_database_case_insensitive(self, metadata_store):
        """Test that database names are case-insensitive"""
        metadata_store.create_database("test_db")
        assert metadata_store.database_exists("TEST_DB")
        assert metadata_store.database_exists("test_db")
        assert metadata_store.database_exists("Test_Db")
    
    def test_schema_case_insensitive(self, sample_database):
        """Test that schema names are case-insensitive"""
        sample_database.create_schema("test_db", "raw_data")
        assert sample_database.schema_exists("TEST_DB", "RAW_DATA")
        assert sample_database.schema_exists("test_db", "raw_data")
    
    def test_full_table_name(self, metadata_store):
        """Test getting fully qualified table name"""
        name = metadata_store.get_full_table_name("db", "schema", "table")
        assert name == "DB.SCHEMA.TABLE"


class TestPersistence:
    """Test metadata persistence"""
    
    def test_persistence_to_disk(self, temp_dir):
        """Test that metadata is persisted to disk"""
        from snowglobe_server.metadata import MetadataStore as MS
        store1 = MS(temp_dir)
        store1.create_database("PERSISTENT_DB")
        
        # Create a new store instance with the same directory
        store2 = MS(temp_dir)
        assert store2.database_exists("PERSISTENT_DB")
