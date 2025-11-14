# Changelog

## [Unreleased]

### Removed
- **snowglobe_client package** - Removed custom client library in favor of using the standard `snowflake-connector-python`

### Added
- **UNDROP operations** - Full support for recovering dropped objects:
  - `UNDROP DATABASE <name>` - Restore dropped database
  - `UNDROP SCHEMA <name>` - Restore dropped schema
  - `UNDROP TABLE <name>` - Restore dropped table
  - `UNDROP VIEW <name>` - Restore dropped view
  - `SHOW DROPPED DATABASES` - List all dropped databases
  - `SHOW DROPPED SCHEMAS` - List all dropped schemas
  - `SHOW DROPPED TABLES` - List all dropped tables

- **Additional Snowflake operations**:
  - `TRUNCATE TABLE <name>` - Clear all data from table
  - `ALTER TABLE <old> RENAME TO <new>` - Rename table
  - `CREATE TABLE <new> CLONE <source>` - Clone table with data

- **Tox configuration** - Added `tox.ini` for testing across multiple Python versions and environments:
  - `py38`, `py39`, `py310`, `py311`, `py312` - Test on different Python versions
  - `lint` - Code style checking with black, flake8, isort
  - `typecheck` - Type checking with mypy
  - `format` - Auto-format code
  - `integration` - Run integration tests
  - `docker` - Run tests in Docker container

- **Shared test fixtures** - Created `tests/conftest.py` with reusable fixtures:
  - `temp_dir` - Temporary directory for test data
  - `metadata_store` - MetadataStore instance
  - `query_executor` - QueryExecutor instance
  - `sql_translator` - SQL translator instance
  - `sample_database` - Database with sample schema
  - `sample_table` - Table with sample columns
  - `sample_view` - View with sample definition
  - `executor_with_data` - Executor with products and orders data
  - `executor_with_aggregates` - Executor with numeric data for aggregate tests
  - `executor_with_groups` - Executor with grouped data
  - `multiple_schemas` - Multiple schemas for testing
  - `dropped_objects` - Pre-dropped objects for UNDROP testing

- **Comprehensive UNDROP tests** - New test file `tests/test_undrop.py`:
  - Tests for UNDROP DATABASE operations
  - Tests for UNDROP SCHEMA operations
  - Tests for UNDROP TABLE operations
  - Tests for UNDROP VIEW operations
  - Workflow tests for disaster recovery scenarios
  - Metadata tracking tests

### Changed
- **Dependencies updated**:
  - Moved FastAPI, uvicorn, pydantic, duckdb from optional to required dependencies
  - Added `snowflake-connector-python` as optional client dependency
  - Added `tox>=4.0.0` to test dependencies

- **Examples updated** - All examples now use standard `snowflake.connector` instead of custom client:
  - `examples/basic_usage.py`
  - `examples/etl_pipeline.py`
  - `examples/testing_example.py`

- **Tests refactored** - All test files now use fixtures from conftest.py:
  - Removed `setup_method` and `teardown_method` in favor of fixtures
  - Tests are more focused and cleaner
  - Better test isolation

- **Metadata Store enhanced**:
  - Added tracking for dropped objects
  - Added methods for listing dropped objects with filtering
  - Persistence includes dropped objects

- **Query Executor enhanced**:
  - Added handlers for all new UNDROP operations
  - Added handlers for TRUNCATE, ALTER TABLE RENAME, and CLONE operations
  - Improved error messages

### Fixed
- Backward compatibility for metadata files without dropped section

## Summary of Key Changes

1. **No more custom client** - Use the standard `snowflake-connector-python` library
2. **UNDROP support** - Recover accidentally dropped databases, schemas, tables, and views
3. **Tox for testing** - Multi-environment testing support with Docker integration
4. **Better test structure** - Shared fixtures in conftest.py for cleaner tests
5. **More Snowflake operations** - TRUNCATE, RENAME, CLONE support
