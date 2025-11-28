"""
dbt Adapter - Full dbt support for Snowglobe

This module provides comprehensive dbt (data build tool) support for Snowglobe,
enabling users to run dbt projects against the local Snowflake emulator.

Features:
- dbt-snowflake compatible connection interface
- Model materialization support (table, view, incremental, ephemeral)
- Seed file loading
- Source freshness checking
- Test execution
- Snapshot support
- Macro compatibility
- Jinja template processing for refs and sources
"""

import re
import os
import json
import yaml
import hashlib
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging

logger = logging.getLogger("snowglobe.dbt")


class MaterializationType(Enum):
    """dbt materialization types"""
    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"
    SNAPSHOT = "snapshot"
    SEED = "seed"
    TEST = "test"


@dataclass
class DbtModel:
    """Represents a dbt model"""
    name: str
    database: str
    schema: str
    alias: Optional[str] = None
    materialization: MaterializationType = MaterializationType.VIEW
    sql: str = ""
    compiled_sql: str = ""
    unique_id: str = ""
    depends_on: List[str] = field(default_factory=list)
    columns: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    description: str = ""
    created_at: Optional[str] = None
    last_run_at: Optional[str] = None
    status: str = "pending"
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None
    rows_affected: int = 0


@dataclass
class DbtSource:
    """Represents a dbt source"""
    name: str
    database: str
    schema: str
    tables: List[Dict[str, Any]] = field(default_factory=list)
    description: str = ""
    loader: str = ""
    freshness: Optional[Dict[str, Any]] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class DbtSeed:
    """Represents a dbt seed"""
    name: str
    database: str
    schema: str
    file_path: str = ""
    columns: List[Dict[str, Any]] = field(default_factory=list)
    rows_loaded: int = 0
    loaded_at: Optional[str] = None


@dataclass
class DbtTest:
    """Represents a dbt test"""
    name: str
    unique_id: str
    model: Optional[str] = None
    column: Optional[str] = None
    test_type: str = "generic"  # generic, singular, schema
    sql: str = ""
    severity: str = "error"  # error, warn
    status: str = "pending"  # pending, pass, fail, warn, error
    error_message: Optional[str] = None
    failures: int = 0
    execution_time_ms: Optional[float] = None


@dataclass
class DbtSnapshot:
    """Represents a dbt snapshot"""
    name: str
    database: str
    schema: str
    strategy: str = "timestamp"  # timestamp, check
    unique_key: str = ""
    updated_at: Optional[str] = None  # For timestamp strategy
    check_cols: List[str] = field(default_factory=list)  # For check strategy
    sql: str = ""
    compiled_sql: str = ""
    invalidate_hard_deletes: bool = False


@dataclass
class DbtRunResult:
    """Result of a dbt run"""
    status: str  # success, error, skipped
    timing: Dict[str, Any] = field(default_factory=dict)
    message: str = ""
    failures: int = 0
    node: Optional[str] = None
    execution_time: float = 0.0


class DbtProjectConfig:
    """dbt project configuration"""
    
    def __init__(self, project_dir: str = None):
        self.project_dir = project_dir or os.getcwd()
        self.name = "snowglobe_project"
        self.version = "1.0.0"
        self.config_version = 2
        self.profile = "snowglobe"
        self.model_paths = ["models"]
        self.seed_paths = ["seeds"]
        self.test_paths = ["tests"]
        self.snapshot_paths = ["snapshots"]
        self.macro_paths = ["macros"]
        self.target_path = "target"
        self.clean_targets = ["target", "dbt_packages"]
        self.vars = {}
        self.models = {}
        self.seeds = {}
        self.snapshots = {}
        self.sources = {}
        
    def load(self, project_file: str = None):
        """Load dbt_project.yml configuration"""
        if project_file is None:
            project_file = os.path.join(self.project_dir, "dbt_project.yml")
        
        if os.path.exists(project_file):
            with open(project_file, 'r') as f:
                config = yaml.safe_load(f)
                
            self.name = config.get("name", self.name)
            self.version = config.get("version", self.version)
            self.config_version = config.get("config-version", self.config_version)
            self.profile = config.get("profile", self.profile)
            self.model_paths = config.get("model-paths", self.model_paths)
            self.seed_paths = config.get("seed-paths", self.seed_paths)
            self.test_paths = config.get("test-paths", self.test_paths)
            self.snapshot_paths = config.get("snapshot-paths", self.snapshot_paths)
            self.macro_paths = config.get("macro-paths", self.macro_paths)
            self.target_path = config.get("target-path", self.target_path)
            self.clean_targets = config.get("clean-targets", self.clean_targets)
            self.vars = config.get("vars", self.vars)
            self.models = config.get("models", self.models)
            self.seeds = config.get("seeds", self.seeds)
            self.snapshots = config.get("snapshots", self.snapshots)
            self.sources = config.get("sources", self.sources)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "version": self.version,
            "config-version": self.config_version,
            "profile": self.profile,
            "model-paths": self.model_paths,
            "seed-paths": self.seed_paths,
            "test-paths": self.test_paths,
            "snapshot-paths": self.snapshot_paths,
            "macro-paths": self.macro_paths,
            "target-path": self.target_path,
            "clean-targets": self.clean_targets,
            "vars": self.vars,
            "models": self.models,
            "seeds": self.seeds,
            "snapshots": self.snapshots,
        }


class DbtSqlCompiler:
    """
    Compiles dbt-style SQL by resolving refs, sources, and config blocks.
    This is a simplified Jinja-like processor for dbt SQL.
    """
    
    def __init__(self, adapter: 'DbtAdapter'):
        self.adapter = adapter
        self.models_registry: Dict[str, DbtModel] = {}
        self.sources_registry: Dict[str, DbtSource] = {}
        
    def register_model(self, model: DbtModel):
        """Register a model for ref resolution"""
        self.models_registry[model.name] = model
        
    def register_source(self, source: DbtSource):
        """Register a source for source resolution"""
        self.sources_registry[source.name] = source
    
    def compile(self, sql: str, model: DbtModel = None, context: Dict = None) -> str:
        """
        Compile dbt SQL by processing:
        - {{ ref('model_name') }} -> fully qualified table name
        - {{ source('source_name', 'table_name') }} -> fully qualified source table
        - {{ config(...) }} -> extract and remove config blocks
        - {{ var('var_name') }} -> variable substitution
        - {{ this }} -> current model's fully qualified name
        - {{ target }} -> target configuration
        - {{ env_var('VAR_NAME') }} -> environment variables
        - Jinja control structures (if/for) - basic support
        """
        context = context or {}
        compiled = sql
        
        # Extract and process config blocks
        compiled, config = self._process_config(compiled)
        if model and config:
            self._apply_config_to_model(model, config)
        
        # Process {{ this }}
        if model:
            this_ref = self._get_fully_qualified_name(model.database, model.schema, 
                                                       model.alias or model.name)
            compiled = re.sub(r'\{\{\s*this\s*\}\}', this_ref, compiled)
        
        # Process {{ ref('model_name') }} and {{ ref('project', 'model_name') }}
        compiled = self._process_refs(compiled)
        
        # Process {{ source('source_name', 'table_name') }}
        compiled = self._process_sources(compiled)
        
        # Process {{ var('var_name') }} and {{ var('var_name', default) }}
        compiled = self._process_vars(compiled, context.get('vars', {}))
        
        # Process {{ target.name }}, {{ target.schema }}, etc.
        compiled = self._process_target(compiled, context.get('target', {}))
        
        # Process {{ env_var('VAR_NAME') }} and {{ env_var('VAR_NAME', 'default') }}
        compiled = self._process_env_vars(compiled)
        
        # Process basic Jinja control structures
        compiled = self._process_control_structures(compiled, context)
        
        # Process dbt built-in macros
        compiled = self._process_builtin_macros(compiled, model)
        
        # Clean up any remaining simple Jinja expressions
        compiled = self._clean_remaining_jinja(compiled)
        
        return compiled.strip()
    
    def _process_config(self, sql: str) -> Tuple[str, Dict]:
        """Extract and remove config blocks, return SQL and config dict"""
        config = {}
        
        # Match {{ config(...) }} blocks
        pattern = r'\{\{\s*config\s*\((.*?)\)\s*\}\}'
        matches = re.findall(pattern, sql, re.DOTALL)
        
        for match in matches:
            # Parse config arguments
            config.update(self._parse_config_args(match))
        
        # Remove config blocks from SQL
        sql = re.sub(pattern, '', sql, flags=re.DOTALL)
        
        return sql, config
    
    def _parse_config_args(self, args_str: str) -> Dict:
        """Parse config arguments like materialized='table', schema='staging'"""
        config = {}
        
        # Handle key=value pairs
        # Match: key='value', key="value", key=value, key=True/False
        pattern = r"(\w+)\s*=\s*(?:'([^']*)'|\"([^\"]*)\"|(\w+))"
        
        for match in re.finditer(pattern, args_str):
            key = match.group(1)
            # Get the value from whichever group matched
            value = match.group(2) or match.group(3) or match.group(4)
            
            # Convert boolean strings
            if value.lower() == 'true':
                value = True
            elif value.lower() == 'false':
                value = False
            
            config[key] = value
        
        return config
    
    def _apply_config_to_model(self, model: DbtModel, config: Dict):
        """Apply parsed config to model"""
        if 'materialized' in config:
            mat_type = config['materialized']
            try:
                model.materialization = MaterializationType(mat_type)
            except ValueError:
                logger.warning(f"Unknown materialization type: {mat_type}")
        
        if 'schema' in config:
            model.schema = config['schema'].upper()
        
        if 'alias' in config:
            model.alias = config['alias']
        
        if 'tags' in config:
            tags = config['tags']
            if isinstance(tags, str):
                model.tags = [tags]
            elif isinstance(tags, list):
                model.tags = tags
        
        # Store all config in meta
        model.meta.update(config)
    
    def _process_refs(self, sql: str) -> str:
        """Process {{ ref('model_name') }} expressions"""
        
        def replace_ref(match):
            args = match.group(1)
            # Parse arguments - could be 'model' or 'project', 'model'
            parts = [p.strip().strip("'\"") for p in args.split(',')]
            
            if len(parts) == 1:
                model_name = parts[0]
            else:
                # project, model format
                model_name = parts[1]
            
            # Look up the model
            if model_name in self.models_registry:
                model = self.models_registry[model_name]
                return self._get_fully_qualified_name(
                    model.database, model.schema, model.alias or model.name
                )
            else:
                # Model not found, use default schema
                return self._get_fully_qualified_name(
                    self.adapter.current_database,
                    self.adapter.current_schema,
                    model_name.upper()
                )
        
        pattern = r'\{\{\s*ref\s*\((.*?)\)\s*\}\}'
        return re.sub(pattern, replace_ref, sql)
    
    def _process_sources(self, sql: str) -> str:
        """Process {{ source('source_name', 'table_name') }} expressions"""
        
        def replace_source(match):
            args = match.group(1)
            parts = [p.strip().strip("'\"") for p in args.split(',')]
            
            if len(parts) != 2:
                return match.group(0)  # Invalid source reference
            
            source_name, table_name = parts
            
            # Look up the source
            if source_name in self.sources_registry:
                source = self.sources_registry[source_name]
                # Find the table in the source
                for table in source.tables:
                    if table.get('name', '').lower() == table_name.lower():
                        identifier = table.get('identifier', table_name)
                        return self._get_fully_qualified_name(
                            source.database, source.schema, identifier.upper()
                        )
                # Table not found in source, use source schema
                return self._get_fully_qualified_name(
                    source.database, source.schema, table_name.upper()
                )
            else:
                # Source not found, use default
                return self._get_fully_qualified_name(
                    self.adapter.current_database,
                    "RAW",  # Common convention for source data
                    table_name.upper()
                )
        
        pattern = r'\{\{\s*source\s*\((.*?)\)\s*\}\}'
        return re.sub(pattern, replace_source, sql)
    
    def _process_vars(self, sql: str, vars_dict: Dict) -> str:
        """Process {{ var('var_name') }} and {{ var('var_name', default) }}"""
        
        def replace_var(match):
            args = match.group(1)
            parts = [p.strip() for p in args.split(',', 1)]
            
            var_name = parts[0].strip("'\"")
            default = parts[1].strip("'\"") if len(parts) > 1 else None
            
            value = vars_dict.get(var_name, default)
            if value is None:
                logger.warning(f"Variable '{var_name}' not found and no default provided")
                return f"NULL /* missing var: {var_name} */"
            
            # Quote strings, leave numbers as-is
            if isinstance(value, str) and not value.isdigit():
                return f"'{value}'"
            return str(value)
        
        pattern = r'\{\{\s*var\s*\((.*?)\)\s*\}\}'
        return re.sub(pattern, replace_var, sql)
    
    def _process_target(self, sql: str, target: Dict) -> str:
        """Process {{ target.xxx }} expressions"""
        target_defaults = {
            'name': 'snowglobe',
            'schema': self.adapter.current_schema,
            'database': self.adapter.current_database,
            'type': 'snowflake',
            'account': 'snowglobe_account',
            'user': 'snowglobe_user',
            'warehouse': self.adapter.current_warehouse,
            'role': self.adapter.current_role,
        }
        target_defaults.update(target)
        
        def replace_target(match):
            prop = match.group(1)
            return str(target_defaults.get(prop, ''))
        
        pattern = r'\{\{\s*target\.(\w+)\s*\}\}'
        return re.sub(pattern, replace_target, sql)
    
    def _process_env_vars(self, sql: str) -> str:
        """Process {{ env_var('VAR_NAME') }} and {{ env_var('VAR_NAME', 'default') }}"""
        
        def replace_env_var(match):
            args = match.group(1)
            parts = [p.strip().strip("'\"") for p in args.split(',', 1)]
            
            var_name = parts[0]
            default = parts[1] if len(parts) > 1 else None
            
            value = os.environ.get(var_name, default)
            if value is None:
                return f"NULL /* missing env_var: {var_name} */"
            return f"'{value}'"
        
        pattern = r'\{\{\s*env_var\s*\((.*?)\)\s*\}\}'
        return re.sub(pattern, replace_env_var, sql)
    
    def _process_control_structures(self, sql: str, context: Dict) -> str:
        """Process basic Jinja control structures - simplified support"""
        # Process {% if %} ... {% endif %}
        # This is a simplified implementation - for complex Jinja, use a full engine
        
        # Handle simple {% if condition %} ... {% endif %}
        pattern = r'\{%\s*if\s+(.*?)\s*%\}(.*?)\{%\s*endif\s*%\}'
        
        def replace_if(match):
            condition = match.group(1)
            body = match.group(2)
            
            # Try to evaluate simple conditions
            try:
                # Replace target/var references in condition
                cond_eval = condition
                for key, value in context.get('vars', {}).items():
                    cond_eval = cond_eval.replace(f"var('{key}')", repr(value))
                
                # Very basic evaluation - only for simple boolean checks
                if eval(cond_eval, {"__builtins__": {}}, context):
                    return body
                return ""
            except:
                # If evaluation fails, include the content
                return body
        
        sql = re.sub(pattern, replace_if, sql, flags=re.DOTALL)
        
        # Handle {% else %} within if blocks (simplified)
        pattern = r'\{%\s*if\s+(.*?)\s*%\}(.*?)\{%\s*else\s*%\}(.*?)\{%\s*endif\s*%\}'
        
        def replace_if_else(match):
            condition = match.group(1)
            if_body = match.group(2)
            else_body = match.group(3)
            
            try:
                cond_eval = condition
                for key, value in context.get('vars', {}).items():
                    cond_eval = cond_eval.replace(f"var('{key}')", repr(value))
                
                if eval(cond_eval, {"__builtins__": {}}, context):
                    return if_body
                return else_body
            except:
                return if_body
        
        sql = re.sub(pattern, replace_if_else, sql, flags=re.DOTALL)
        
        return sql
    
    def _process_builtin_macros(self, sql: str, model: DbtModel = None) -> str:
        """Process dbt built-in macros"""
        
        # {{ dbt_utils.star(from=ref('table')) }} - simplified
        def replace_star(match):
            return "*"
        sql = re.sub(r'\{\{\s*dbt_utils\.star\s*\(.*?\)\s*\}\}', replace_star, sql)
        
        # {{ adapter.dispatch('macro_name')() }} - just remove
        sql = re.sub(r'\{\{\s*adapter\.dispatch\s*\(.*?\)\s*\(\s*\)\s*\}\}', '', sql)
        
        # {{ run_started_at }} 
        sql = re.sub(r'\{\{\s*run_started_at\s*\}\}', 
                     f"'{datetime.utcnow().isoformat()}'", sql)
        
        # {{ invocation_id }}
        sql = re.sub(r'\{\{\s*invocation_id\s*\}\}', 
                     f"'{hashlib.md5(str(datetime.utcnow()).encode()).hexdigest()}'", sql)
        
        # {{ log(message, info=True) }} - just remove
        sql = re.sub(r'\{\{\s*log\s*\(.*?\)\s*\}\}', '', sql)
        
        # {{ return(value) }} - just extract value
        sql = re.sub(r'\{\{\s*return\s*\((.*?)\)\s*\}\}', r'\1', sql)
        
        # {{ is_incremental() }}
        if model and model.materialization == MaterializationType.INCREMENTAL:
            sql = re.sub(r'\{\{\s*is_incremental\s*\(\s*\)\s*\}\}', 'true', sql, flags=re.IGNORECASE)
        else:
            sql = re.sub(r'\{\{\s*is_incremental\s*\(\s*\)\s*\}\}', 'false', sql, flags=re.IGNORECASE)
        
        return sql
    
    def _clean_remaining_jinja(self, sql: str) -> str:
        """Clean up any remaining Jinja expressions"""
        # Remove any remaining {{ ... }} that weren't processed
        # But log a warning
        remaining = re.findall(r'\{\{.*?\}\}', sql)
        for expr in remaining:
            logger.warning(f"Unprocessed Jinja expression: {expr}")
        
        # Remove Jinja comments {# ... #}
        sql = re.sub(r'\{#.*?#\}', '', sql, flags=re.DOTALL)
        
        # Remove empty Jinja blocks
        sql = re.sub(r'\{%.*?%\}', '', sql, flags=re.DOTALL)
        
        return sql
    
    def _get_fully_qualified_name(self, database: str, schema: str, name: str) -> str:
        """Get fully qualified table/view name using DuckDB schema format"""
        # DuckDB uses a single-level schema, so we combine database and schema
        # into a DuckDB schema name (e.g., snowglobe_public)
        duck_schema = f"{database.lower()}_{schema.lower()}"
        return f"{duck_schema}.{name.upper()}"


class DbtAdapter:
    """
    Main dbt adapter class for Snowglobe.
    Provides dbt-compatible interface for running models, tests, seeds, and snapshots.
    """
    
    def __init__(self, executor):
        """
        Initialize dbt adapter with a QueryExecutor instance.
        
        Args:
            executor: QueryExecutor instance for running SQL
        """
        self.executor = executor
        self.compiler = DbtSqlCompiler(self)
        
        # Project configuration
        self.project_config = DbtProjectConfig()
        
        # Registries
        self.models: Dict[str, DbtModel] = {}
        self.sources: Dict[str, DbtSource] = {}
        self.seeds: Dict[str, DbtSeed] = {}
        self.tests: Dict[str, DbtTest] = {}
        self.snapshots: Dict[str, DbtSnapshot] = {}
        
        # Run state
        self.run_results: List[DbtRunResult] = []
        self.manifest: Dict[str, Any] = {}
        
        # Target configuration
        self.target = {
            'name': 'dev',
            'type': 'snowflake',
            'account': 'snowglobe_local',
            'user': 'dbt_user',
            'database': self.current_database,
            'schema': self.current_schema,
            'warehouse': self.current_warehouse,
            'role': self.current_role,
        }
    
    @property
    def current_database(self) -> str:
        return self.executor.current_database
    
    @property
    def current_schema(self) -> str:
        return self.executor.current_schema
    
    @property
    def current_warehouse(self) -> str:
        return self.executor.current_warehouse
    
    @property
    def current_role(self) -> str:
        return self.executor.current_role
    
    def load_project(self, project_dir: str):
        """Load a dbt project from directory"""
        self.project_config.project_dir = project_dir
        self.project_config.load()
        
        # Load sources from schema.yml files
        self._load_sources(project_dir)
        
        # Load models
        self._load_models(project_dir)
        
        # Load seeds
        self._load_seeds(project_dir)
        
        # Load tests
        self._load_tests(project_dir)
        
        # Load snapshots
        self._load_snapshots(project_dir)
        
        logger.info(f"Loaded dbt project: {self.project_config.name}")
        logger.info(f"  Models: {len(self.models)}")
        logger.info(f"  Sources: {len(self.sources)}")
        logger.info(f"  Seeds: {len(self.seeds)}")
        logger.info(f"  Tests: {len(self.tests)}")
        logger.info(f"  Snapshots: {len(self.snapshots)}")
    
    def _load_sources(self, project_dir: str):
        """Load source definitions from schema.yml files"""
        for model_path in self.project_config.model_paths:
            models_dir = os.path.join(project_dir, model_path)
            if not os.path.exists(models_dir):
                continue
            
            for root, dirs, files in os.walk(models_dir):
                for file in files:
                    if file.endswith('.yml') or file.endswith('.yaml'):
                        filepath = os.path.join(root, file)
                        self._parse_schema_yml(filepath)
    
    def _parse_schema_yml(self, filepath: str):
        """Parse a schema.yml file for sources and model docs"""
        try:
            with open(filepath, 'r') as f:
                content = yaml.safe_load(f)
            
            if not content:
                return
            
            # Parse sources
            for source_def in content.get('sources', []):
                source = DbtSource(
                    name=source_def.get('name', ''),
                    database=source_def.get('database', self.current_database).upper(),
                    schema=source_def.get('schema', 'RAW').upper(),
                    tables=source_def.get('tables', []),
                    description=source_def.get('description', ''),
                    loader=source_def.get('loader', ''),
                    freshness=source_def.get('freshness'),
                    meta=source_def.get('meta', {}),
                )
                self.sources[source.name] = source
                self.compiler.register_source(source)
            
            # Parse model documentation
            for model_def in content.get('models', []):
                model_name = model_def.get('name', '')
                if model_name in self.models:
                    model = self.models[model_name]
                    model.description = model_def.get('description', '')
                    model.columns = model_def.get('columns', [])
                    model.meta.update(model_def.get('meta', {}))
                    
                    # Extract tests from column definitions
                    for col in model_def.get('columns', []):
                        col_name = col.get('name', '')
                        for test in col.get('tests', []):
                            self._create_column_test(model_name, col_name, test)
        
        except Exception as e:
            logger.warning(f"Failed to parse schema file {filepath}: {e}")
    
    def _create_column_test(self, model_name: str, column_name: str, test_def):
        """Create a test from column definition"""
        if isinstance(test_def, str):
            test_type = test_def
            test_config = {}
        elif isinstance(test_def, dict):
            test_type = list(test_def.keys())[0]
            test_config = test_def[test_type] if isinstance(test_def[test_type], dict) else {}
        else:
            return
        
        test_id = f"test_{model_name}_{column_name}_{test_type}"
        
        # Generate test SQL based on test type
        if test_type == 'unique':
            sql = f"""
                SELECT {column_name}
                FROM {{{{ ref('{model_name}') }}}}
                GROUP BY {column_name}
                HAVING COUNT(*) > 1
            """
        elif test_type == 'not_null':
            sql = f"""
                SELECT *
                FROM {{{{ ref('{model_name}') }}}}
                WHERE {column_name} IS NULL
            """
        elif test_type == 'accepted_values':
            values = test_config.get('values', [])
            values_str = ', '.join([f"'{v}'" for v in values])
            sql = f"""
                SELECT *
                FROM {{{{ ref('{model_name}') }}}}
                WHERE {column_name} NOT IN ({values_str})
            """
        elif test_type == 'relationships':
            to_model = test_config.get('to', '')
            to_field = test_config.get('field', '')
            sql = f"""
                SELECT a.{column_name}
                FROM {{{{ ref('{model_name}') }}}} a
                LEFT JOIN {{{{ ref('{to_model}') }}}} b ON a.{column_name} = b.{to_field}
                WHERE b.{to_field} IS NULL AND a.{column_name} IS NOT NULL
            """
        else:
            # Generic test placeholder
            sql = f"SELECT 1 WHERE 1=0 /* Unknown test: {test_type} */"
        
        test = DbtTest(
            name=test_id,
            unique_id=test_id,
            model=model_name,
            column=column_name,
            test_type='schema',
            sql=sql,
            severity=test_config.get('severity', 'error'),
        )
        self.tests[test_id] = test
    
    def _load_models(self, project_dir: str):
        """Load model SQL files"""
        for model_path in self.project_config.model_paths:
            models_dir = os.path.join(project_dir, model_path)
            if not os.path.exists(models_dir):
                continue
            
            for root, dirs, files in os.walk(models_dir):
                for file in files:
                    if file.endswith('.sql'):
                        filepath = os.path.join(root, file)
                        model_name = os.path.splitext(file)[0]
                        
                        with open(filepath, 'r') as f:
                            sql = f.read()
                        
                        # Determine schema from path
                        rel_path = os.path.relpath(root, models_dir)
                        schema = self.current_schema
                        if rel_path != '.':
                            # Use subdirectory as schema prefix
                            schema = f"{self.current_schema}_{rel_path.replace(os.sep, '_')}".upper()
                        
                        model = DbtModel(
                            name=model_name,
                            database=self.current_database,
                            schema=schema,
                            sql=sql,
                            unique_id=f"model.{self.project_config.name}.{model_name}",
                            created_at=datetime.utcnow().isoformat(),
                        )
                        
                        # Parse SQL to extract refs and config
                        compiled_sql = self.compiler.compile(sql, model, {
                            'vars': self.project_config.vars,
                            'target': self.target,
                        })
                        model.compiled_sql = compiled_sql
                        
                        # Extract dependencies from refs
                        refs = re.findall(r'\{\{\s*ref\s*\([\'"](\w+)[\'"]\)\s*\}\}', sql)
                        model.depends_on = refs
                        
                        self.models[model_name] = model
                        self.compiler.register_model(model)
    
    def _load_seeds(self, project_dir: str):
        """Load seed CSV files"""
        for seed_path in self.project_config.seed_paths:
            seeds_dir = os.path.join(project_dir, seed_path)
            if not os.path.exists(seeds_dir):
                continue
            
            for root, dirs, files in os.walk(seeds_dir):
                for file in files:
                    if file.endswith('.csv'):
                        filepath = os.path.join(root, file)
                        seed_name = os.path.splitext(file)[0]
                        
                        seed = DbtSeed(
                            name=seed_name,
                            database=self.current_database,
                            schema=self.current_schema,
                            file_path=filepath,
                        )
                        self.seeds[seed_name] = seed
    
    def _load_tests(self, project_dir: str):
        """Load singular test SQL files"""
        for test_path in self.project_config.test_paths:
            tests_dir = os.path.join(project_dir, test_path)
            if not os.path.exists(tests_dir):
                continue
            
            for root, dirs, files in os.walk(tests_dir):
                for file in files:
                    if file.endswith('.sql'):
                        filepath = os.path.join(root, file)
                        test_name = os.path.splitext(file)[0]
                        
                        with open(filepath, 'r') as f:
                            sql = f.read()
                        
                        test = DbtTest(
                            name=test_name,
                            unique_id=f"test.{self.project_config.name}.{test_name}",
                            test_type='singular',
                            sql=sql,
                        )
                        self.tests[test_name] = test
    
    def _load_snapshots(self, project_dir: str):
        """Load snapshot SQL files"""
        for snapshot_path in self.project_config.snapshot_paths:
            snapshots_dir = os.path.join(project_dir, snapshot_path)
            if not os.path.exists(snapshots_dir):
                continue
            
            for root, dirs, files in os.walk(snapshots_dir):
                for file in files:
                    if file.endswith('.sql'):
                        filepath = os.path.join(root, file)
                        
                        with open(filepath, 'r') as f:
                            sql = f.read()
                        
                        # Parse snapshot config from SQL
                        self._parse_snapshot_sql(sql)
    
    def _parse_snapshot_sql(self, sql: str):
        """Parse snapshot SQL to extract configuration"""
        # Match {% snapshot snapshot_name %}...{% endsnapshot %}
        pattern = r'\{%\s*snapshot\s+(\w+)\s*%\}(.*?)\{%\s*endsnapshot\s*%\}'
        matches = re.findall(pattern, sql, re.DOTALL)
        
        for snapshot_name, snapshot_sql in matches:
            # Extract config
            config_pattern = r'\{\{\s*config\s*\((.*?)\)\s*\}\}'
            config_match = re.search(config_pattern, snapshot_sql, re.DOTALL)
            config = {}
            if config_match:
                config = self.compiler._parse_config_args(config_match.group(1))
            
            snapshot = DbtSnapshot(
                name=snapshot_name,
                database=self.current_database,
                schema=config.get('target_schema', self.current_schema).upper(),
                strategy=config.get('strategy', 'timestamp'),
                unique_key=config.get('unique_key', 'id'),
                updated_at=config.get('updated_at'),
                check_cols=config.get('check_cols', []),
                sql=snapshot_sql,
                invalidate_hard_deletes=config.get('invalidate_hard_deletes', False),
            )
            self.snapshots[snapshot_name] = snapshot
    
    def run(self, select: str = None, exclude: str = None, 
            full_refresh: bool = False) -> List[DbtRunResult]:
        """
        Run dbt models.
        
        Args:
            select: Model selection syntax (e.g., "+model_name", "tag:important")
            exclude: Models to exclude
            full_refresh: Whether to full refresh incremental models
        
        Returns:
            List of DbtRunResult
        """
        self.run_results = []
        models_to_run = self._select_models(select, exclude)
        
        # Sort models by dependencies
        sorted_models = self._topological_sort(models_to_run)
        
        logger.info(f"Running {len(sorted_models)} models")
        
        for model_name in sorted_models:
            model = self.models[model_name]
            result = self._run_model(model, full_refresh)
            self.run_results.append(result)
            
            if result.status == 'error':
                logger.error(f"Model {model_name} failed: {result.message}")
                # Continue running other models unless they depend on this one
        
        return self.run_results
    
    def _select_models(self, select: str = None, exclude: str = None) -> List[str]:
        """Select models based on selection syntax"""
        if select is None:
            selected = set(self.models.keys())
        else:
            selected = set()
            for selector in select.split():
                if selector.startswith('tag:'):
                    tag = selector[4:]
                    for name, model in self.models.items():
                        if tag in model.tags:
                            selected.add(name)
                elif selector.startswith('+'):
                    # Include upstream dependencies
                    model_name = selector[1:]
                    selected.add(model_name)
                    selected.update(self._get_upstream(model_name))
                elif selector.endswith('+'):
                    # Include downstream dependencies
                    model_name = selector[:-1]
                    selected.add(model_name)
                    selected.update(self._get_downstream(model_name))
                else:
                    selected.add(selector)
        
        if exclude:
            for exc in exclude.split():
                selected.discard(exc)
        
        return list(selected)
    
    def _get_upstream(self, model_name: str) -> set:
        """Get all upstream dependencies of a model"""
        upstream = set()
        if model_name not in self.models:
            return upstream
        
        model = self.models[model_name]
        for dep in model.depends_on:
            upstream.add(dep)
            upstream.update(self._get_upstream(dep))
        
        return upstream
    
    def _get_downstream(self, model_name: str) -> set:
        """Get all downstream dependencies of a model"""
        downstream = set()
        for name, model in self.models.items():
            if model_name in model.depends_on:
                downstream.add(name)
                downstream.update(self._get_downstream(name))
        return downstream
    
    def _topological_sort(self, model_names: List[str]) -> List[str]:
        """Sort models by dependencies"""
        # Build dependency graph for selected models
        in_degree = {name: 0 for name in model_names}
        graph = {name: [] for name in model_names}
        
        for name in model_names:
            model = self.models[name]
            for dep in model.depends_on:
                if dep in model_names:
                    graph[dep].append(name)
                    in_degree[name] += 1
        
        # Kahn's algorithm
        queue = [name for name, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            node = queue.pop(0)
            result.append(node)
            
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(result) != len(model_names):
            logger.warning("Circular dependency detected in models")
            # Return what we have plus remaining
            remaining = set(model_names) - set(result)
            result.extend(remaining)
        
        return result
    
    def _run_model(self, model: DbtModel, full_refresh: bool = False) -> DbtRunResult:
        """Run a single model"""
        start_time = datetime.utcnow()
        
        try:
            # Recompile SQL with current context
            compiled_sql = self.compiler.compile(model.sql, model, {
                'vars': self.project_config.vars,
                'target': self.target,
            })
            model.compiled_sql = compiled_sql
            
            # Generate DDL based on materialization
            ddl_sql = self._generate_model_ddl(model, full_refresh)
            
            # Execute - handle multiple statements separated by semicolons
            statements = [s.strip() for s in ddl_sql.split(';') if s.strip()]
            result = {'success': True}
            for stmt in statements:
                result = self.executor.execute(stmt)
                if not result.get('success', False):
                    break
            
            end_time = datetime.utcnow()
            execution_time = (end_time - start_time).total_seconds()
            
            if result['success']:
                model.status = 'success'
                model.last_run_at = end_time.isoformat()
                model.rows_affected = result.get('rowcount', 0)
                model.execution_time_ms = execution_time * 1000
                
                return DbtRunResult(
                    status='success',
                    node=model.unique_id,
                    execution_time=execution_time,
                    message=f"Created {model.materialization.value} {model.name}",
                    timing={
                        'started_at': start_time.isoformat(),
                        'completed_at': end_time.isoformat(),
                    }
                )
            else:
                model.status = 'error'
                model.error = result.get('error', 'Unknown error')
                
                return DbtRunResult(
                    status='error',
                    node=model.unique_id,
                    execution_time=execution_time,
                    message=model.error,
                    timing={
                        'started_at': start_time.isoformat(),
                        'completed_at': end_time.isoformat(),
                    }
                )
        
        except Exception as e:
            end_time = datetime.utcnow()
            model.status = 'error'
            model.error = str(e)
            
            return DbtRunResult(
                status='error',
                node=model.unique_id,
                execution_time=(end_time - start_time).total_seconds(),
                message=str(e),
            )
    
    def _generate_model_ddl(self, model: DbtModel, full_refresh: bool = False) -> str:
        """Generate DDL SQL for model based on materialization type"""
        # Use DuckDB schema naming convention
        duck_schema = self.executor._get_duckdb_schema(model.database, model.schema)
        target_table = f"{duck_schema}.{model.alias or model.name}"
        
        # Ensure schema exists
        self.executor._ensure_schema_exists(model.database, model.schema)
        
        if model.materialization == MaterializationType.VIEW:
            return f"CREATE OR REPLACE VIEW {target_table} AS\n{model.compiled_sql}"
        
        elif model.materialization == MaterializationType.TABLE:
            # DuckDB doesn't support CREATE OR REPLACE TABLE ... AS SELECT
            # Use DROP + CREATE instead
            return f"DROP TABLE IF EXISTS {target_table};\nCREATE TABLE {target_table} AS\n{model.compiled_sql}"
        
        elif model.materialization == MaterializationType.INCREMENTAL:
            if full_refresh or not self._table_exists(model.database, model.schema, 
                                                       model.alias or model.name):
                return f"DROP TABLE IF EXISTS {target_table};\nCREATE TABLE {target_table} AS\n{model.compiled_sql}"
            else:
                # For incremental, the model SQL should already have is_incremental() logic
                # We need to do a merge or insert
                unique_key = model.meta.get('unique_key', None)
                if unique_key:
                    # Use MERGE for upsert
                    return self._generate_merge_sql(model, target_table, unique_key)
                else:
                    # Append mode
                    return f"INSERT INTO {target_table}\n{model.compiled_sql}"
        
        elif model.materialization == MaterializationType.EPHEMERAL:
            # Ephemeral models are CTEs, nothing to execute directly
            return "SELECT 1 /* Ephemeral model - used as CTE */"
        
        else:
            return f"CREATE OR REPLACE TABLE {target_table} AS\n{model.compiled_sql}"
    
    def _generate_merge_sql(self, model: DbtModel, target_table: str, 
                           unique_key: str) -> str:
        """Generate MERGE SQL for incremental model"""
        # DuckDB doesn't support MERGE, so we use a delete + insert approach
        return f"""
            DELETE FROM {target_table}
            WHERE {unique_key} IN (
                SELECT {unique_key} FROM ({model.compiled_sql}) src
            );
            INSERT INTO {target_table}
            {model.compiled_sql};
        """
    
    def _table_exists(self, database: str, schema: str, table: str) -> bool:
        """Check if a table exists"""
        return self.executor.metadata.table_exists(database, schema, table)
    
    def seed(self, select: str = None, full_refresh: bool = False) -> List[DbtRunResult]:
        """
        Load seed data from CSV files.
        
        Args:
            select: Seed selection
            full_refresh: Whether to drop and recreate
        
        Returns:
            List of DbtRunResult
        """
        results = []
        
        seeds_to_load = list(self.seeds.keys())
        if select:
            seeds_to_load = [s for s in seeds_to_load if s in select.split()]
        
        for seed_name in seeds_to_load:
            seed = self.seeds[seed_name]
            result = self._load_seed(seed, full_refresh)
            results.append(result)
        
        return results
    
    def _load_seed(self, seed: DbtSeed, full_refresh: bool = False) -> DbtRunResult:
        """Load a single seed file"""
        start_time = datetime.utcnow()
        
        try:
            # Read CSV file
            with open(seed.file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                columns = reader.fieldnames or []
            
            if not rows:
                return DbtRunResult(
                    status='success',
                    message=f"Seed {seed.name} is empty",
                    node=seed.name,
                )
            
            # Infer column types from data
            col_types = self._infer_csv_types(rows, columns)
            seed.columns = [{'name': col, 'type': col_types[col]} for col in columns]
            
            # Use DuckDB schema naming
            duck_schema = self.executor._get_duckdb_schema(seed.database, seed.schema)
            target_table = f"{duck_schema}.{seed.name}"
            
            # Ensure schema exists
            self.executor._ensure_schema_exists(seed.database, seed.schema)
            
            # Create table
            columns_ddl = ', '.join([f"{col} {col_types[col]}" for col in columns])
            
            if full_refresh or not self._table_exists(seed.database, seed.schema, seed.name):
                self.executor.execute(f"DROP TABLE IF EXISTS {target_table}")
                self.executor.execute(f"CREATE TABLE {target_table} ({columns_ddl})")
            
            # Insert data
            for row in rows:
                values = []
                for col in columns:
                    val = row.get(col, '')
                    if val is None or val == '':
                        values.append('NULL')
                    elif col_types[col] == 'VARCHAR':
                        values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                    else:
                        values.append(str(val))
                
                values_str = ', '.join(values)
                self.executor.execute(f"INSERT INTO {target_table} VALUES ({values_str})")
            
            seed.rows_loaded = len(rows)
            seed.loaded_at = datetime.utcnow().isoformat()
            
            end_time = datetime.utcnow()
            
            return DbtRunResult(
                status='success',
                message=f"Loaded {len(rows)} rows into {seed.name}",
                node=seed.name,
                execution_time=(end_time - start_time).total_seconds(),
            )
        
        except Exception as e:
            return DbtRunResult(
                status='error',
                message=str(e),
                node=seed.name,
            )
    
    def _infer_csv_types(self, rows: List[Dict], columns: List[str]) -> Dict[str, str]:
        """Infer SQL types from CSV data"""
        types = {}
        
        for col in columns:
            sample_values = [row.get(col, '') for row in rows[:100] if row.get(col, '')]
            
            if not sample_values:
                types[col] = 'VARCHAR'
                continue
            
            # Try to infer type
            is_int = all(v.lstrip('-').isdigit() for v in sample_values if v)
            is_float = all(self._is_float(v) for v in sample_values if v)
            is_bool = all(v.lower() in ('true', 'false', '0', '1') for v in sample_values if v)
            is_date = all(self._is_date(v) for v in sample_values if v)
            
            if is_int:
                types[col] = 'INTEGER'
            elif is_float:
                types[col] = 'DOUBLE'
            elif is_bool:
                types[col] = 'BOOLEAN'
            elif is_date:
                types[col] = 'TIMESTAMP'
            else:
                types[col] = 'VARCHAR'
        
        return types
    
    def _is_float(self, s: str) -> bool:
        try:
            float(s)
            return True
        except ValueError:
            return False
    
    def _is_date(self, s: str) -> bool:
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            r'^\d{2}/\d{2}/\d{4}$',
        ]
        return any(re.match(p, s) for p in date_patterns)
    
    def test(self, select: str = None) -> List[DbtRunResult]:
        """
        Run dbt tests.
        
        Args:
            select: Test selection
        
        Returns:
            List of DbtRunResult
        """
        results = []
        
        tests_to_run = list(self.tests.keys())
        if select:
            tests_to_run = [t for t in tests_to_run if select in t]
        
        for test_name in tests_to_run:
            test = self.tests[test_name]
            result = self._run_test(test)
            results.append(result)
        
        return results
    
    def _run_test(self, test: DbtTest) -> DbtRunResult:
        """Run a single test"""
        start_time = datetime.utcnow()
        
        try:
            # Compile test SQL
            compiled_sql = self.compiler.compile(test.sql, None, {
                'vars': self.project_config.vars,
                'target': self.target,
            })
            
            # Execute test - tests should return 0 rows to pass
            result = self.executor.execute(compiled_sql)
            
            end_time = datetime.utcnow()
            execution_time = (end_time - start_time).total_seconds()
            
            if result['success']:
                row_count = result.get('rowcount', 0)
                
                if row_count == 0:
                    test.status = 'pass'
                    return DbtRunResult(
                        status='success',
                        message=f"Test {test.name} passed",
                        node=test.unique_id,
                        execution_time=execution_time,
                        failures=0,
                    )
                else:
                    test.status = 'fail' if test.severity == 'error' else 'warn'
                    test.failures = row_count
                    
                    return DbtRunResult(
                        status='error' if test.severity == 'error' else 'warn',
                        message=f"Test {test.name} failed with {row_count} failures",
                        node=test.unique_id,
                        execution_time=execution_time,
                        failures=row_count,
                    )
            else:
                test.status = 'error'
                test.error_message = result.get('error', 'Unknown error')
                
                return DbtRunResult(
                    status='error',
                    message=test.error_message,
                    node=test.unique_id,
                    execution_time=execution_time,
                )
        
        except Exception as e:
            test.status = 'error'
            test.error_message = str(e)
            
            return DbtRunResult(
                status='error',
                message=str(e),
                node=test.unique_id,
            )
    
    def snapshot(self, select: str = None) -> List[DbtRunResult]:
        """
        Run dbt snapshots.
        
        Args:
            select: Snapshot selection
        
        Returns:
            List of DbtRunResult
        """
        results = []
        
        snapshots_to_run = list(self.snapshots.keys())
        if select:
            snapshots_to_run = [s for s in snapshots_to_run if s in select.split()]
        
        for snapshot_name in snapshots_to_run:
            snapshot = self.snapshots[snapshot_name]
            result = self._run_snapshot(snapshot)
            results.append(result)
        
        return results
    
    def _run_snapshot(self, snapshot: DbtSnapshot) -> DbtRunResult:
        """Run a single snapshot"""
        start_time = datetime.utcnow()
        
        try:
            # Compile snapshot SQL
            compiled_sql = self.compiler.compile(snapshot.sql, None, {
                'vars': self.project_config.vars,
                'target': self.target,
            })
            snapshot.compiled_sql = compiled_sql
            
            target_table = f"{snapshot.database}.{snapshot.schema}.{snapshot.name}"
            
            # Ensure schema exists
            self.executor._ensure_schema_exists(snapshot.database, snapshot.schema)
            
            # Check if snapshot table exists
            table_exists = self._table_exists(snapshot.database, snapshot.schema, snapshot.name)
            
            if not table_exists:
                # Create initial snapshot
                sql = f"""
                    CREATE TABLE {target_table} AS
                    SELECT 
                        *,
                        MD5(CAST({snapshot.unique_key} AS VARCHAR)) AS dbt_scd_id,
                        CURRENT_TIMESTAMP AS dbt_updated_at,
                        CURRENT_TIMESTAMP AS dbt_valid_from,
                        CAST(NULL AS TIMESTAMP) AS dbt_valid_to
                    FROM ({compiled_sql}) src
                """
            else:
                # Update snapshot - simplified SCD Type 2 logic
                if snapshot.strategy == 'timestamp':
                    sql = self._generate_timestamp_snapshot_sql(snapshot, target_table, compiled_sql)
                else:
                    sql = self._generate_check_snapshot_sql(snapshot, target_table, compiled_sql)
            
            result = self.executor.execute(sql)
            
            end_time = datetime.utcnow()
            
            if result['success']:
                return DbtRunResult(
                    status='success',
                    message=f"Snapshot {snapshot.name} completed",
                    node=snapshot.name,
                    execution_time=(end_time - start_time).total_seconds(),
                )
            else:
                return DbtRunResult(
                    status='error',
                    message=result.get('error', 'Unknown error'),
                    node=snapshot.name,
                )
        
        except Exception as e:
            return DbtRunResult(
                status='error',
                message=str(e),
                node=snapshot.name,
            )
    
    def _generate_timestamp_snapshot_sql(self, snapshot: DbtSnapshot, 
                                         target_table: str, source_sql: str) -> str:
        """Generate SQL for timestamp-based snapshot"""
        # This is a simplified version - full implementation would be more complex
        return f"""
            -- Close old records that have been updated
            UPDATE {target_table}
            SET dbt_valid_to = CURRENT_TIMESTAMP
            WHERE dbt_valid_to IS NULL
            AND {snapshot.unique_key} IN (
                SELECT src.{snapshot.unique_key}
                FROM ({source_sql}) src
                INNER JOIN {target_table} tgt
                ON src.{snapshot.unique_key} = tgt.{snapshot.unique_key}
                WHERE tgt.dbt_valid_to IS NULL
                AND src.{snapshot.updated_at} > tgt.dbt_updated_at
            );
            
            -- Insert new/updated records
            INSERT INTO {target_table}
            SELECT 
                src.*,
                MD5(CAST(src.{snapshot.unique_key} AS VARCHAR) || '-' || CAST(CURRENT_TIMESTAMP AS VARCHAR)) AS dbt_scd_id,
                CURRENT_TIMESTAMP AS dbt_updated_at,
                CURRENT_TIMESTAMP AS dbt_valid_from,
                CAST(NULL AS TIMESTAMP) AS dbt_valid_to
            FROM ({source_sql}) src
            LEFT JOIN {target_table} tgt
            ON src.{snapshot.unique_key} = tgt.{snapshot.unique_key}
            AND tgt.dbt_valid_to IS NULL
            WHERE tgt.{snapshot.unique_key} IS NULL
            OR src.{snapshot.updated_at} > tgt.dbt_updated_at;
        """
    
    def _generate_check_snapshot_sql(self, snapshot: DbtSnapshot,
                                     target_table: str, source_sql: str) -> str:
        """Generate SQL for check-based snapshot"""
        check_cols = snapshot.check_cols or ['*']
        check_condition = ' OR '.join([
            f"src.{col} != tgt.{col}" for col in check_cols if col != '*'
        ]) or '1=1'
        
        return f"""
            -- Close old records that have changed
            UPDATE {target_table}
            SET dbt_valid_to = CURRENT_TIMESTAMP
            WHERE dbt_valid_to IS NULL
            AND {snapshot.unique_key} IN (
                SELECT src.{snapshot.unique_key}
                FROM ({source_sql}) src
                INNER JOIN {target_table} tgt
                ON src.{snapshot.unique_key} = tgt.{snapshot.unique_key}
                WHERE tgt.dbt_valid_to IS NULL
                AND ({check_condition})
            );
            
            -- Insert new/changed records
            INSERT INTO {target_table}
            SELECT 
                src.*,
                MD5(CAST(src.{snapshot.unique_key} AS VARCHAR) || '-' || CAST(CURRENT_TIMESTAMP AS VARCHAR)) AS dbt_scd_id,
                CURRENT_TIMESTAMP AS dbt_updated_at,
                CURRENT_TIMESTAMP AS dbt_valid_from,
                CAST(NULL AS TIMESTAMP) AS dbt_valid_to
            FROM ({source_sql}) src
            LEFT JOIN {target_table} tgt
            ON src.{snapshot.unique_key} = tgt.{snapshot.unique_key}
            AND tgt.dbt_valid_to IS NULL
            WHERE tgt.{snapshot.unique_key} IS NULL
            OR ({check_condition});
        """
    
    def source_freshness(self, select: str = None) -> Dict[str, Any]:
        """
        Check freshness of sources.
        
        Args:
            select: Source selection
        
        Returns:
            Dict with freshness results
        """
        results = {'sources': []}
        
        for source_name, source in self.sources.items():
            if select and source_name not in select.split():
                continue
            
            if not source.freshness:
                continue
            
            for table in source.tables:
                table_name = table.get('name', '')
                freshness_config = table.get('freshness', source.freshness)
                
                if not freshness_config:
                    continue
                
                result = self._check_source_freshness(source, table_name, freshness_config)
                results['sources'].append(result)
        
        return results
    
    def _check_source_freshness(self, source: DbtSource, table_name: str,
                                freshness_config: Dict) -> Dict:
        """Check freshness of a single source table"""
        loaded_at_field = freshness_config.get('loaded_at_field', 'updated_at')
        warn_after = freshness_config.get('warn_after', {})
        error_after = freshness_config.get('error_after', {})
        
        full_table = f"{source.database}.{source.schema}.{table_name}"
        
        try:
            # Get max loaded_at
            result = self.executor.execute(f"SELECT MAX({loaded_at_field}) FROM {full_table}")
            
            if result['success'] and result['data']:
                max_loaded_at = result['data'][0][0]
                if max_loaded_at:
                    age = datetime.utcnow() - datetime.fromisoformat(str(max_loaded_at))
                    age_seconds = age.total_seconds()
                    
                    warn_seconds = self._get_seconds_from_interval(warn_after)
                    error_seconds = self._get_seconds_from_interval(error_after)
                    
                    if error_seconds and age_seconds > error_seconds:
                        status = 'error'
                    elif warn_seconds and age_seconds > warn_seconds:
                        status = 'warn'
                    else:
                        status = 'pass'
                    
                    return {
                        'source': source.name,
                        'table': table_name,
                        'status': status,
                        'max_loaded_at': str(max_loaded_at),
                        'age_seconds': age_seconds,
                    }
            
            return {
                'source': source.name,
                'table': table_name,
                'status': 'error',
                'message': 'Could not determine freshness',
            }
        
        except Exception as e:
            return {
                'source': source.name,
                'table': table_name,
                'status': 'error',
                'message': str(e),
            }
    
    def _get_seconds_from_interval(self, interval: Dict) -> int:
        """Convert interval dict to seconds"""
        if not interval:
            return 0
        
        count = interval.get('count', 0)
        period = interval.get('period', 'hour')
        
        multipliers = {
            'minute': 60,
            'hour': 3600,
            'day': 86400,
        }
        
        return count * multipliers.get(period, 3600)
    
    def generate_docs(self) -> Dict[str, Any]:
        """
        Generate dbt documentation manifest.
        
        Returns:
            Dict containing documentation data
        """
        docs = {
            'metadata': {
                'project_id': self.project_config.name,
                'generated_at': datetime.utcnow().isoformat(),
                'dbt_version': '1.7.0',  # Compatibility version
                'adapter': 'snowglobe',
            },
            'nodes': {},
            'sources': {},
            'metrics': {},
            'exposures': {},
        }
        
        # Add models
        for name, model in self.models.items():
            docs['nodes'][model.unique_id] = {
                'unique_id': model.unique_id,
                'name': model.name,
                'database': model.database,
                'schema': model.schema,
                'alias': model.alias,
                'description': model.description,
                'columns': {col.get('name', ''): col for col in model.columns},
                'meta': model.meta,
                'tags': model.tags,
                'depends_on': model.depends_on,
                'compiled_sql': model.compiled_sql,
                'resource_type': 'model',
                'materialization': model.materialization.value,
            }
        
        # Add sources
        for name, source in self.sources.items():
            for table in source.tables:
                source_id = f"source.{self.project_config.name}.{name}.{table.get('name', '')}"
                docs['sources'][source_id] = {
                    'unique_id': source_id,
                    'source_name': name,
                    'name': table.get('name', ''),
                    'database': source.database,
                    'schema': source.schema,
                    'description': table.get('description', source.description),
                    'columns': {col.get('name', ''): col for col in table.get('columns', [])},
                }
        
        self.manifest = docs
        return docs
    
    def compile_sql(self, sql: str) -> str:
        """
        Compile arbitrary SQL with dbt context.
        
        Args:
            sql: Raw SQL with Jinja/dbt expressions
        
        Returns:
            Compiled SQL
        """
        return self.compiler.compile(sql, None, {
            'vars': self.project_config.vars,
            'target': self.target,
        })
    
    def get_model_lineage(self, model_name: str) -> Dict[str, Any]:
        """
        Get lineage information for a model.
        
        Args:
            model_name: Name of the model
        
        Returns:
            Dict with upstream and downstream models
        """
        if model_name not in self.models:
            return {'error': f'Model {model_name} not found'}
        
        return {
            'model': model_name,
            'upstream': list(self._get_upstream(model_name)),
            'downstream': list(self._get_downstream(model_name)),
        }
    
    def get_run_results(self) -> List[Dict]:
        """Get results from the last run"""
        return [asdict(r) for r in self.run_results]


def generate_profiles_yml(host: str = 'localhost', port: int = 8443,
                          database: str = 'SNOWGLOBE', schema: str = 'PUBLIC',
                          warehouse: str = 'COMPUTE_WH', role: str = 'ACCOUNTADMIN') -> str:
    """
    Generate a profiles.yml configuration for connecting to Snowglobe.
    
    This can be used with the standard dbt-snowflake adapter.
    
    Args:
        host: Snowglobe host
        port: Snowglobe HTTPS port
        database: Default database
        schema: Default schema
        warehouse: Default warehouse
        role: Default role
    
    Returns:
        YAML string for profiles.yml
    """
    profiles = {
        'snowglobe': {
            'target': 'dev',
            'outputs': {
                'dev': {
                    'type': 'snowflake',
                    'account': host,
                    'user': 'dbt_user',
                    'password': 'dbt_password',  # Any password works with Snowglobe
                    'role': role,
                    'database': database,
                    'warehouse': warehouse,
                    'schema': schema,
                    'threads': 4,
                    'client_session_keep_alive': False,
                    # Snowglobe-specific settings
                    'host': host,
                    'port': port,
                    'protocol': 'https',
                    'insecure_mode': True,  # For self-signed certificates
                }
            }
        }
    }
    
    return yaml.dump(profiles, default_flow_style=False, sort_keys=False)


def generate_sample_project(project_dir: str):
    """
    Generate a sample dbt project for Snowglobe.
    
    Args:
        project_dir: Directory to create project in
    """
    os.makedirs(project_dir, exist_ok=True)
    
    # Create dbt_project.yml
    project_yml = {
        'name': 'snowglobe_sample',
        'version': '1.0.0',
        'config-version': 2,
        'profile': 'snowglobe',
        'model-paths': ['models'],
        'seed-paths': ['seeds'],
        'test-paths': ['tests'],
        'snapshot-paths': ['snapshots'],
        'macro-paths': ['macros'],
        'target-path': 'target',
        'clean-targets': ['target', 'dbt_packages'],
    }
    
    with open(os.path.join(project_dir, 'dbt_project.yml'), 'w') as f:
        yaml.dump(project_yml, f, default_flow_style=False, sort_keys=False)
    
    # Create directories
    for dir_name in ['models', 'seeds', 'tests', 'snapshots', 'macros']:
        os.makedirs(os.path.join(project_dir, dir_name), exist_ok=True)
    
    # Create sample staging model
    os.makedirs(os.path.join(project_dir, 'models', 'staging'), exist_ok=True)
    with open(os.path.join(project_dir, 'models', 'staging', 'stg_customers.sql'), 'w') as f:
        f.write("""{{ config(materialized='view') }}

SELECT
    id AS customer_id,
    name AS customer_name,
    email,
    created_at
FROM {{ source('raw', 'customers') }}
""")
    
    # Create sample mart model
    os.makedirs(os.path.join(project_dir, 'models', 'marts'), exist_ok=True)
    with open(os.path.join(project_dir, 'models', 'marts', 'dim_customers.sql'), 'w') as f:
        f.write("""{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_name,
    email,
    created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_customers') }}
""")
    
    # Create schema.yml with sources
    schema_yml = {
        'version': 2,
        'sources': [
            {
                'name': 'raw',
                'database': 'SNOWGLOBE',
                'schema': 'RAW',
                'tables': [
                    {
                        'name': 'customers',
                        'description': 'Raw customer data',
                        'columns': [
                            {'name': 'id', 'description': 'Primary key'},
                            {'name': 'name', 'description': 'Customer name'},
                            {'name': 'email', 'description': 'Customer email'},
                            {'name': 'created_at', 'description': 'When customer was created'},
                        ]
                    }
                ]
            }
        ],
        'models': [
            {
                'name': 'stg_customers',
                'description': 'Staged customer data',
                'columns': [
                    {'name': 'customer_id', 'tests': ['unique', 'not_null']},
                    {'name': 'customer_name', 'tests': ['not_null']},
                    {'name': 'email', 'tests': ['unique']},
                ]
            }
        ]
    }
    
    with open(os.path.join(project_dir, 'models', 'staging', 'schema.yml'), 'w') as f:
        yaml.dump(schema_yml, f, default_flow_style=False, sort_keys=False)
    
    # Create sample seed
    with open(os.path.join(project_dir, 'seeds', 'sample_data.csv'), 'w') as f:
        f.write("id,name,email,created_at\n")
        f.write("1,Alice Johnson,alice@example.com,2024-01-01 00:00:00\n")
        f.write("2,Bob Smith,bob@example.com,2024-01-02 00:00:00\n")
        f.write("3,Carol White,carol@example.com,2024-01-03 00:00:00\n")
    
    # Create profiles.yml
    with open(os.path.join(project_dir, 'profiles.yml'), 'w') as f:
        f.write(generate_profiles_yml())
    
    logger.info(f"Sample dbt project created at {project_dir}")
