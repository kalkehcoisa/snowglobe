"""
Tests for dbt adapter functionality
"""

import pytest
import os
import tempfile
import shutil
from datetime import datetime

from snowglobe_server.dbt_adapter import (
    DbtAdapter,
    DbtModel,
    DbtSource,
    DbtSeed,
    DbtTest,
    DbtSnapshot,
    DbtSqlCompiler,
    DbtProjectConfig,
    MaterializationType,
    DbtRunResult,
    generate_profiles_yml,
    generate_sample_project,
)
from snowglobe_server.query_executor import QueryExecutor
from snowglobe_server.sql_translator import DbtSqlTranslator, translate_dbt_sql


@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def executor(temp_data_dir):
    """Create a QueryExecutor instance"""
    executor = QueryExecutor(temp_data_dir)
    yield executor
    executor.close()


@pytest.fixture
def dbt_adapter(executor):
    """Create a DbtAdapter instance"""
    adapter = DbtAdapter(executor)
    return adapter


class TestDbtSqlCompiler:
    """Test cases for DbtSqlCompiler"""
    
    def test_compile_ref(self, dbt_adapter):
        """Test compiling {{ ref('model_name') }}"""
        # Register a model first
        model = DbtModel(
            name="customers",
            database="SNOWGLOBE",
            schema="PUBLIC",
        )
        dbt_adapter.models["customers"] = model
        dbt_adapter.compiler.register_model(model)
        
        sql = "SELECT * FROM {{ ref('customers') }}"
        compiled = dbt_adapter.compile_sql(sql)
        
        # DuckDB uses combined schema: snowglobe_public.CUSTOMERS
        assert "CUSTOMERS" in compiled.upper()
        assert "snowglobe_public" in compiled.lower() or "SNOWGLOBE" in compiled.upper()
        assert "{{" not in compiled
    
    def test_compile_source(self, dbt_adapter):
        """Test compiling {{ source('source_name', 'table_name') }}"""
        # Register a source
        source = DbtSource(
            name="raw",
            database="SNOWGLOBE",
            schema="RAW",
            tables=[{"name": "customers", "identifier": "raw_customers"}]
        )
        dbt_adapter.sources["raw"] = source
        dbt_adapter.compiler.register_source(source)
        
        sql = "SELECT * FROM {{ source('raw', 'customers') }}"
        compiled = dbt_adapter.compile_sql(sql)
        
        # DuckDB uses combined schema: snowglobe_raw.RAW_CUSTOMERS
        assert "RAW_CUSTOMERS" in compiled.upper() or "CUSTOMERS" in compiled.upper()
        assert "{{" not in compiled
    
    def test_compile_config(self, dbt_adapter):
        """Test extracting {{ config(...) }} blocks"""
        model = DbtModel(
            name="test_model",
            database="SNOWGLOBE",
            schema="PUBLIC",
        )
        
        sql = """
        {{ config(materialized='table', schema='staging') }}
        SELECT * FROM other_table
        """
        
        compiled = dbt_adapter.compiler.compile(sql, model)
        
        # Config should be removed from SQL
        assert "config" not in compiled.lower()
        assert "SELECT" in compiled.upper()
        
        # Model should be updated
        assert model.materialization == MaterializationType.TABLE
        assert model.schema == "STAGING"
    
    def test_compile_var(self, dbt_adapter):
        """Test compiling {{ var('var_name') }}"""
        dbt_adapter.project_config.vars = {"my_var": "test_value"}
        
        sql = "SELECT '{{ var('my_var') }}' AS value"
        compiled = dbt_adapter.compile_sql(sql)
        
        assert "test_value" in compiled
    
    def test_compile_var_with_default(self, dbt_adapter):
        """Test compiling {{ var('var_name', default) }}"""
        sql = "SELECT '{{ var('missing_var', 'default_value') }}' AS value"
        compiled = dbt_adapter.compile_sql(sql)
        
        assert "default_value" in compiled
    
    def test_compile_this(self, dbt_adapter):
        """Test compiling {{ this }}"""
        model = DbtModel(
            name="my_model",
            database="SNOWGLOBE",
            schema="ANALYTICS",
        )
        
        sql = "DELETE FROM {{ this }} WHERE 1=1"
        compiled = dbt_adapter.compiler.compile(sql, model)
        
        # DuckDB uses combined schema format
        assert "MY_MODEL" in compiled.upper()
        assert "{{" not in compiled
    
    def test_compile_target(self, dbt_adapter):
        """Test compiling {{ target.xxx }}"""
        dbt_adapter.target = {
            'name': 'dev',
            'schema': 'DEV_SCHEMA',
            'database': 'DEV_DB',
        }
        
        sql = "SELECT '{{ target.name }}' AS env, '{{ target.schema }}' AS schema"
        compiled = dbt_adapter.compile_sql(sql)
        
        assert "dev" in compiled
        assert "DEV_SCHEMA" in compiled
    
    def test_compile_is_incremental(self, dbt_adapter):
        """Test compiling {{ is_incremental() }}"""
        # Non-incremental model
        model = DbtModel(
            name="test",
            database="SNOWGLOBE",
            schema="PUBLIC",
            materialization=MaterializationType.TABLE,
        )
        
        sql = "SELECT * FROM source {% if is_incremental() %} WHERE date > max_date {% endif %}"
        # Note: Control structures are simplified in compiler
        compiled = dbt_adapter.compiler.compile(sql, model)
        
        # is_incremental() should be replaced with false for non-incremental
        assert "is_incremental" not in compiled.lower() or "false" in compiled.lower()
    
    def test_compile_env_var(self, dbt_adapter):
        """Test compiling {{ env_var('VAR_NAME') }}"""
        os.environ["TEST_DBT_VAR"] = "env_value"
        
        sql = "SELECT {{ env_var('TEST_DBT_VAR') }} AS value"
        compiled = dbt_adapter.compile_sql(sql)
        
        assert "env_value" in compiled
        
        del os.environ["TEST_DBT_VAR"]


class TestDbtAdapter:
    """Test cases for DbtAdapter"""
    
    def test_register_model(self, dbt_adapter):
        """Test registering a model"""
        model = DbtModel(
            name="test_model",
            database="SNOWGLOBE",
            schema="PUBLIC",
            sql="SELECT 1 AS id",
            materialization=MaterializationType.VIEW,
        )
        
        dbt_adapter.models["test_model"] = model
        dbt_adapter.compiler.register_model(model)
        
        assert "test_model" in dbt_adapter.models
    
    def test_register_source(self, dbt_adapter):
        """Test registering a source"""
        source = DbtSource(
            name="raw_data",
            database="SNOWGLOBE",
            schema="RAW",
            tables=[{"name": "orders"}, {"name": "customers"}],
        )
        
        dbt_adapter.sources["raw_data"] = source
        dbt_adapter.compiler.register_source(source)
        
        assert "raw_data" in dbt_adapter.sources
        assert len(source.tables) == 2
    
    def test_model_dependencies(self, dbt_adapter):
        """Test extracting model dependencies"""
        model1 = DbtModel(
            name="stg_customers",
            database="SNOWGLOBE",
            schema="STAGING",
            sql="SELECT * FROM {{ source('raw', 'customers') }}",
        )
        
        model2 = DbtModel(
            name="dim_customers",
            database="SNOWGLOBE",
            schema="ANALYTICS",
            sql="SELECT * FROM {{ ref('stg_customers') }}",
        )
        model2.depends_on = ["stg_customers"]
        
        dbt_adapter.models["stg_customers"] = model1
        dbt_adapter.models["dim_customers"] = model2
        
        # Test upstream
        upstream = dbt_adapter._get_upstream("dim_customers")
        assert "stg_customers" in upstream
        
        # Test downstream
        downstream = dbt_adapter._get_downstream("stg_customers")
        assert "dim_customers" in downstream
    
    def test_topological_sort(self, dbt_adapter):
        """Test topological sorting of models"""
        # Create a chain: A -> B -> C
        model_a = DbtModel(name="model_a", database="DB", schema="S", depends_on=[])
        model_b = DbtModel(name="model_b", database="DB", schema="S", depends_on=["model_a"])
        model_c = DbtModel(name="model_c", database="DB", schema="S", depends_on=["model_b"])
        
        dbt_adapter.models = {
            "model_a": model_a,
            "model_b": model_b,
            "model_c": model_c,
        }
        
        sorted_models = dbt_adapter._topological_sort(["model_a", "model_b", "model_c"])
        
        # model_a should come before model_b, model_b before model_c
        assert sorted_models.index("model_a") < sorted_models.index("model_b")
        assert sorted_models.index("model_b") < sorted_models.index("model_c")
    
    def test_run_view_model(self, dbt_adapter, executor):
        """Test running a view model"""
        # First ensure the schema exists
        executor._ensure_schema_exists("SNOWGLOBE", "PUBLIC")
        
        model = DbtModel(
            name="test_view",
            database="SNOWGLOBE",
            schema="PUBLIC",
            sql="SELECT 1 AS id, 'test' AS name",
            materialization=MaterializationType.VIEW,
            unique_id="model.test.test_view",
        )
        
        dbt_adapter.models["test_view"] = model
        dbt_adapter.compiler.register_model(model)
        
        result = dbt_adapter._run_model(model)
        
        assert result.status == "success"
        assert model.status == "success"
    
    def test_run_table_model(self, dbt_adapter, executor):
        """Test running a table model"""
        # First ensure the schema exists
        executor._ensure_schema_exists("SNOWGLOBE", "PUBLIC")
        
        model = DbtModel(
            name="test_table",
            database="SNOWGLOBE",
            schema="PUBLIC",
            sql="SELECT 1 AS id, 'test' AS name",
            materialization=MaterializationType.TABLE,
            unique_id="model.test.test_table",
        )
        
        dbt_adapter.models["test_table"] = model
        dbt_adapter.compiler.register_model(model)
        
        result = dbt_adapter._run_model(model)
        
        assert result.status == "success"
    
    def test_model_selection(self, dbt_adapter):
        """Test model selection syntax"""
        model1 = DbtModel(name="m1", database="DB", schema="S", tags=["important"])
        model2 = DbtModel(name="m2", database="DB", schema="S", tags=["unimportant"])
        model3 = DbtModel(name="m3", database="DB", schema="S", depends_on=["m1"])
        
        dbt_adapter.models = {"m1": model1, "m2": model2, "m3": model3}
        
        # Select by tag
        selected = dbt_adapter._select_models("tag:important")
        assert "m1" in selected
        assert "m2" not in selected
        
        # Select with upstream
        selected = dbt_adapter._select_models("+m3")
        assert "m3" in selected
        assert "m1" in selected
    
    def test_get_lineage(self, dbt_adapter):
        """Test getting model lineage"""
        model1 = DbtModel(name="source_model", database="DB", schema="S", depends_on=[])
        model2 = DbtModel(name="middle_model", database="DB", schema="S", depends_on=["source_model"])
        model3 = DbtModel(name="target_model", database="DB", schema="S", depends_on=["middle_model"])
        
        dbt_adapter.models = {
            "source_model": model1,
            "middle_model": model2,
            "target_model": model3,
        }
        
        lineage = dbt_adapter.get_model_lineage("middle_model")
        
        assert "source_model" in lineage["upstream"]
        assert "target_model" in lineage["downstream"]
    
    def test_generate_docs(self, dbt_adapter):
        """Test documentation generation"""
        model = DbtModel(
            name="documented_model",
            database="SNOWGLOBE",
            schema="PUBLIC",
            description="A well-documented model",
            unique_id="model.test.documented_model",
            columns=[{"name": "id", "description": "Primary key"}],
        )
        
        dbt_adapter.models["documented_model"] = model
        
        docs = dbt_adapter.generate_docs()
        
        assert "nodes" in docs
        assert "model.test.documented_model" in docs["nodes"]
        assert docs["nodes"]["model.test.documented_model"]["description"] == "A well-documented model"


class TestDbtTests:
    """Test cases for dbt test functionality"""
    
    def test_unique_test(self, dbt_adapter, executor):
        """Test unique column test"""
        # Create a table with duplicate values using proper DuckDB schema
        executor._ensure_schema_exists("TEST_DB", "TEST_SCHEMA")
        duck_schema = executor._get_duckdb_schema("TEST_DB", "TEST_SCHEMA")
        
        executor.execute(f"CREATE TABLE IF NOT EXISTS {duck_schema}.test_unique (id INTEGER, name VARCHAR)")
        executor.execute(f"DELETE FROM {duck_schema}.test_unique")  # Clear any existing data
        executor.execute(f"INSERT INTO {duck_schema}.test_unique VALUES (1, 'a'), (1, 'b')")  # Duplicate id
        
        # Register model
        model = DbtModel(
            name="test_unique",
            database="TEST_DB",
            schema="TEST_SCHEMA",
        )
        dbt_adapter.models["test_unique"] = model
        dbt_adapter.compiler.register_model(model)
        
        # Create test with direct schema reference
        test = DbtTest(
            name="unique_id",
            unique_id="test.unique_id",
            model="test_unique",
            column="id",
            test_type="schema",
            sql=f"""
                SELECT id
                FROM {duck_schema}.test_unique
                GROUP BY id
                HAVING COUNT(*) > 1
            """,
        )
        dbt_adapter.tests["unique_id"] = test
        
        result = dbt_adapter._run_test(test)
        
        # Test should fail because there are duplicates
        assert result.status == "error"
        assert result.failures > 0
    
    def test_not_null_test(self, dbt_adapter, executor):
        """Test not_null column test"""
        executor._ensure_schema_exists("TEST_DB", "TEST_SCHEMA")
        duck_schema = executor._get_duckdb_schema("TEST_DB", "TEST_SCHEMA")
        
        executor.execute(f"CREATE TABLE IF NOT EXISTS {duck_schema}.test_null (id INTEGER, name VARCHAR)")
        executor.execute(f"DELETE FROM {duck_schema}.test_null")  # Clear any existing data
        executor.execute(f"INSERT INTO {duck_schema}.test_null VALUES (1, 'a'), (2, NULL)")  # NULL value
        
        model = DbtModel(
            name="test_null",
            database="TEST_DB",
            schema="TEST_SCHEMA",
        )
        dbt_adapter.models["test_null"] = model
        dbt_adapter.compiler.register_model(model)
        
        test = DbtTest(
            name="not_null_name",
            unique_id="test.not_null_name",
            model="test_null",
            column="name",
            test_type="schema",
            sql=f"""
                SELECT *
                FROM {duck_schema}.test_null
                WHERE name IS NULL
            """,
        )
        dbt_adapter.tests["not_null_name"] = test
        
        result = dbt_adapter._run_test(test)
        
        # Test should fail because there's a NULL
        assert result.status == "error"
        assert result.failures > 0


class TestDbtSeeds:
    """Test cases for dbt seed functionality"""
    
    def test_load_seed(self, dbt_adapter, temp_data_dir, executor):
        """Test loading a seed CSV file"""
        # Ensure schema exists
        executor._ensure_schema_exists("SNOWGLOBE", "PUBLIC")
        
        # Create a CSV file
        csv_content = "id,name,value\n1,Alice,100\n2,Bob,200\n3,Carol,300\n"
        csv_path = os.path.join(temp_data_dir, "test_seed.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)
        
        seed = DbtSeed(
            name="test_seed",
            database=dbt_adapter.current_database,
            schema=dbt_adapter.current_schema,
            file_path=csv_path,
        )
        
        result = dbt_adapter._load_seed(seed, full_refresh=True)
        
        assert result.status == "success"
        assert seed.rows_loaded == 3
        
        # Verify data was loaded using DuckDB schema format
        duck_schema = executor._get_duckdb_schema(seed.database, seed.schema)
        query_result = dbt_adapter.executor.execute(
            f"SELECT COUNT(*) FROM {duck_schema}.{seed.name}"
        )
        assert query_result["data"][0][0] == 3
    
    def test_infer_csv_types(self, dbt_adapter):
        """Test CSV type inference"""
        rows = [
            {"int_col": "1", "float_col": "1.5", "str_col": "abc", "bool_col": "true"},
            {"int_col": "2", "float_col": "2.5", "str_col": "def", "bool_col": "false"},
        ]
        columns = ["int_col", "float_col", "str_col", "bool_col"]
        
        types = dbt_adapter._infer_csv_types(rows, columns)
        
        assert types["int_col"] == "INTEGER"
        assert types["float_col"] == "DOUBLE"
        assert types["str_col"] == "VARCHAR"
        assert types["bool_col"] == "BOOLEAN"


class TestDbtSqlTranslator:
    """Test cases for dbt SQL translator"""
    
    def test_translate_copy_grants(self):
        """Test removing COPY GRANTS"""
        sql = "CREATE TABLE test COPY GRANTS AS SELECT 1"
        translated = translate_dbt_sql(sql)
        
        assert "COPY GRANTS" not in translated
        assert "SELECT 1" in translated
    
    def test_translate_transient(self):
        """Test removing TRANSIENT"""
        sql = "CREATE TRANSIENT TABLE test AS SELECT 1"
        translated = translate_dbt_sql(sql)
        
        assert "TRANSIENT" not in translated
        assert "CREATE" in translated
        assert "TABLE" in translated
    
    def test_translate_cluster_by(self):
        """Test removing CLUSTER BY"""
        sql = "CREATE TABLE test CLUSTER BY (date_col) AS SELECT 1"
        translated = translate_dbt_sql(sql)
        
        assert "CLUSTER BY" not in translated
    
    def test_translate_surrogate_key(self):
        """Test dbt_utils.surrogate_key translation"""
        translator = DbtSqlTranslator()
        sql = "SELECT {{ dbt_utils.surrogate_key(['col1', 'col2']) }} AS sk"
        translated = translator._translate_dbt_utils(sql)
        
        assert "MD5" in translated
        assert "col1" in translated
        assert "col2" in translated


class TestProfilesGeneration:
    """Test cases for profiles.yml generation"""
    
    def test_generate_profiles_yml(self):
        """Test profiles.yml generation"""
        profiles = generate_profiles_yml(
            host="localhost",
            port=8443,
            database="TEST_DB",
            schema="TEST_SCHEMA",
        )
        
        assert "snowglobe" in profiles
        assert "localhost" in profiles
        assert "8443" in profiles
        assert "TEST_DB" in profiles
        assert "TEST_SCHEMA" in profiles
        assert "insecure_mode" in profiles


class TestSampleProjectGeneration:
    """Test cases for sample project generation"""
    
    def test_generate_sample_project(self):
        """Test generating a sample dbt project"""
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = os.path.join(temp_dir, "sample_project")
            
            generate_sample_project(project_dir)
            
            # Check files exist
            assert os.path.exists(os.path.join(project_dir, "dbt_project.yml"))
            assert os.path.exists(os.path.join(project_dir, "profiles.yml"))
            assert os.path.exists(os.path.join(project_dir, "models", "staging", "stg_customers.sql"))
            assert os.path.exists(os.path.join(project_dir, "models", "marts", "dim_customers.sql"))
            assert os.path.exists(os.path.join(project_dir, "seeds", "sample_data.csv"))


class TestDbtProjectConfig:
    """Test cases for DbtProjectConfig"""
    
    def test_default_config(self):
        """Test default configuration"""
        config = DbtProjectConfig()
        
        assert config.name == "snowglobe_project"
        assert config.config_version == 2
        assert "models" in config.model_paths
        assert "seeds" in config.seed_paths
    
    def test_load_config(self):
        """Test loading configuration from file"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a dbt_project.yml
            project_yml = """
name: my_project
version: '2.0.0'
config-version: 2
profile: my_profile
model-paths: ['my_models']
vars:
  my_var: my_value
"""
            yml_path = os.path.join(temp_dir, "dbt_project.yml")
            with open(yml_path, "w") as f:
                f.write(project_yml)
            
            config = DbtProjectConfig(temp_dir)
            config.load()
            
            assert config.name == "my_project"
            assert config.version == "2.0.0"
            assert config.profile == "my_profile"
            assert "my_models" in config.model_paths
            assert config.vars["my_var"] == "my_value"


class TestDbtSnapshots:
    """Test cases for dbt snapshot functionality"""
    
    def test_parse_snapshot_sql(self, dbt_adapter):
        """Test parsing snapshot SQL"""
        snapshot_sql = """
{% snapshot orders_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
) }}

SELECT * FROM {{ source('raw', 'orders') }}

{% endsnapshot %}
"""
        dbt_adapter._parse_snapshot_sql(snapshot_sql)
        
        assert "orders_snapshot" in dbt_adapter.snapshots
        snapshot = dbt_adapter.snapshots["orders_snapshot"]
        assert snapshot.strategy == "timestamp"
        assert snapshot.unique_key == "id"


class TestMaterializations:
    """Test cases for different materializations"""
    
    def test_view_materialization(self, dbt_adapter):
        """Test view materialization DDL generation"""
        model = DbtModel(
            name="test_view",
            database="DB",
            schema="SCH",
            materialization=MaterializationType.VIEW,
            compiled_sql="SELECT 1 AS col",
        )
        
        ddl = dbt_adapter._generate_model_ddl(model)
        
        assert "CREATE OR REPLACE VIEW" in ddl.upper()
    
    def test_table_materialization(self, dbt_adapter):
        """Test table materialization DDL generation"""
        model = DbtModel(
            name="test_table",
            database="DB",
            schema="SCH",
            materialization=MaterializationType.TABLE,
            compiled_sql="SELECT 1 AS col",
        )
        
        ddl = dbt_adapter._generate_model_ddl(model)
        
        # DuckDB uses DROP + CREATE instead of CREATE OR REPLACE TABLE ... AS
        assert "DROP TABLE IF EXISTS" in ddl.upper()
        assert "CREATE TABLE" in ddl.upper()
    
    def test_incremental_initial(self, dbt_adapter):
        """Test incremental materialization initial run"""
        model = DbtModel(
            name="test_incremental",
            database="SNOWGLOBE",
            schema="PUBLIC",
            materialization=MaterializationType.INCREMENTAL,
            compiled_sql="SELECT 1 AS id",
            meta={},
        )
        
        ddl = dbt_adapter._generate_model_ddl(model, full_refresh=True)
        
        # DuckDB uses DROP + CREATE for full refresh
        assert "DROP TABLE IF EXISTS" in ddl.upper()
        assert "CREATE TABLE" in ddl.upper()
    
    def test_ephemeral_materialization(self, dbt_adapter):
        """Test ephemeral materialization"""
        model = DbtModel(
            name="test_ephemeral",
            database="DB",
            schema="SCH",
            materialization=MaterializationType.EPHEMERAL,
            compiled_sql="SELECT 1 AS col",
        )
        
        ddl = dbt_adapter._generate_model_ddl(model)
        
        # Ephemeral models shouldn't create anything
        assert "Ephemeral" in ddl or "CTE" in ddl
