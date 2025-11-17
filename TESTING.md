# Testing Guide for Snowglobe

This document provides comprehensive instructions for running tests in Snowglobe across different environments.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Running Tests Locally](#running-tests-locally)
- [Running Tests in Docker](#running-tests-in-docker)
- [Using Tox](#using-tox)
- [Test Coverage](#test-coverage)
- [Continuous Integration](#continuous-integration)
- [Writing Tests](#writing-tests)

## Overview

Snowglobe uses pytest as its testing framework. The test suite includes:

- **Unit tests**: Test individual components in isolation
- **Integration tests**: Test interactions between components
- **End-to-end tests**: Test the complete system
- **Edge case tests**: Test boundary conditions and error handling
- **SSL/TLS tests**: Test certificate generation and SSL functionality
- **API tests**: Test HTTP endpoints and Snowflake compatibility

## Quick Start

### Prerequisites

- Python 3.8 or higher
- pip package manager
- Docker (optional, for containerized testing)
- OpenSSL (for SSL tests)

### Install Test Dependencies

```bash
# Install package with test dependencies
pip install -e ".[test]"

# Or install from requirements
pip install -r requirements.txt pytest pytest-cov pytest-asyncio httpx
```

## Running Tests Locally

### Run All Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run with detailed output
pytest -vv
```

### Run Specific Test Files

```bash
# Run authentication tests
pytest tests/test_authentication.py

# Run server endpoint tests
pytest tests/test_server_endpoints.py

# Run SSL tests
pytest tests/test_ssl.py
```

### Run Specific Test Classes or Methods

```bash
# Run specific test class
pytest tests/test_server_endpoints.py::TestHealthEndpoint

# Run specific test method
pytest tests/test_server_endpoints.py::TestHealthEndpoint::test_health_check

# Run tests matching a pattern
pytest -k "test_login"
```

### Run Tests with Coverage

```bash
# Run tests with coverage report
pytest --cov=snowglobe_server --cov-report=term-missing

# Generate HTML coverage report
pytest --cov=snowglobe_server --cov-report=html

# View HTML report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

### Run Tests in Parallel

```bash
# Install pytest-xdist
pip install pytest-xdist

# Run tests in parallel (auto-detect CPU count)
pytest -n auto

# Run tests with 4 workers
pytest -n 4
```

## Running Tests in Docker

### Using Docker Compose

```bash
# Build and run tests
docker-compose run --rm test

# Run specific test file
docker-compose run --rm test pytest tests/test_authentication.py

# Run with coverage
docker-compose run --rm test pytest --cov=snowglobe_server --cov-report=term-missing
```

### Using Docker Directly

```bash
# Build test image
docker build -t snowglobe-test .

# Run all tests
docker run --rm snowglobe-test python -m pytest tests/ -v

# Run with coverage
docker run --rm snowglobe-test python -m pytest tests/ --cov=snowglobe_server --cov-report=term-missing

# Run specific test file
docker run --rm snowglobe-test python -m pytest tests/test_server_endpoints.py -v

# Run with shell access (for debugging)
docker run --rm -it snowglobe-test /bin/bash
# Inside container:
pytest tests/ -v
```

### Interactive Docker Testing

```bash
# Start container with shell
docker run --rm -it -v $(pwd):/app snowglobe-test /bin/bash

# Inside container, run tests
cd /app
pytest tests/ -v

# Run specific tests
pytest tests/test_ssl.py -v

# Run with coverage
pytest --cov=snowglobe_server --cov-report=term-missing
```

## Using Tox

Tox is used for testing across multiple Python versions and environments.

### Basic Tox Usage

```bash
# Install tox
pip install tox

# Run tests for all environments
tox

# List available environments
tox -l

# Run specific environment
tox -e py311

# Run multiple specific environments
tox -e py310,py311
```

### Available Tox Environments

- **py38, py39, py310, py311, py312**: Run tests on different Python versions
- **lint**: Run code linting (black, isort, flake8)
- **typecheck**: Run type checking with mypy
- **format**: Auto-format code with black and isort
- **integration**: Run integration tests
- **docker**: Run tests in Docker container

### Tox Examples

```bash
# Run tests on Python 3.11
tox -e py311

# Run linting
tox -e lint

# Run type checking
tox -e typecheck

# Auto-format code
tox -e format

# Run integration tests
tox -e integration

# Run Docker tests
tox -e docker
```

### Tox in Docker

While tox is primarily for local testing across Python versions, you can also run it in Docker:

```bash
# Build image with tox
docker run --rm -v $(pwd):/app -w /app python:3.11-slim bash -c "pip install tox && tox -e py311"

# Or create a dedicated tox Dockerfile
cat > Dockerfile.tox << 'EOF'
FROM python:3.11-slim
RUN pip install tox
WORKDIR /app
COPY . .
CMD ["tox"]
EOF

docker build -f Dockerfile.tox -t snowglobe-tox .
docker run --rm snowglobe-tox
```

## Test Coverage

### Generate Coverage Reports

```bash
# Terminal report with missing lines
pytest --cov=snowglobe_server --cov-report=term-missing

# HTML report
pytest --cov=snowglobe_server --cov-report=html
open htmlcov/index.html

# XML report (for CI tools)
pytest --cov=snowglobe_server --cov-report=xml

# Multiple report formats
pytest --cov=snowglobe_server --cov-report=term-missing --cov-report=html --cov-report=xml
```

### Coverage Thresholds

```bash
# Fail if coverage is below 80%
pytest --cov=snowglobe_server --cov-fail-under=80
```

## Test Organization

### Test Structure

```
tests/
├── __init__.py
├── conftest.py                    # Shared fixtures
├── test_authentication.py         # Authentication tests
├── test_integration.py            # Integration tests
├── test_metadata.py               # Metadata management tests
├── test_query_executor.py         # Query execution tests
├── test_sql_translator.py         # SQL translation tests
├── test_undrop.py                 # UNDROP functionality tests
├── test_server_endpoints.py       # HTTP API endpoint tests
├── test_ssl.py                    # SSL/TLS tests
└── test_edge_cases.py             # Edge cases and error handling
```

### Test Categories

**Unit Tests**: Test individual components
```bash
pytest tests/test_sql_translator.py tests/test_metadata.py
```

**Integration Tests**: Test component interactions
```bash
pytest tests/test_integration.py tests/test_authentication.py
```

**API Tests**: Test HTTP endpoints
```bash
pytest tests/test_server_endpoints.py
```

**Security Tests**: Test SSL/TLS functionality
```bash
pytest tests/test_ssl.py
```

**Edge Cases**: Test error handling
```bash
pytest tests/test_edge_cases.py
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ['3.8', '3.9', '3.10', '3.11', '3.12']
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        pip install -e .
        pip install pytest pytest-cov pytest-asyncio httpx
    
    - name: Run tests
      run: |
        pytest --cov=snowglobe_server --cov-report=xml --cov-report=term-missing
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
```

### GitLab CI Example

```yaml
test:
  image: python:3.11
  script:
    - pip install -e .
    - pip install pytest pytest-cov
    - pytest --cov=snowglobe_server --cov-report=term-missing
  coverage: '/TOTAL.*\s+(\d+%)$/'
```

## Writing Tests

### Basic Test Structure

```python
def test_example(query_executor):
    """Test description"""
    # Arrange
    query_executor.execute("CREATE TABLE test (id INT)")
    
    # Act
    result = query_executor.execute("SELECT * FROM test")
    
    # Assert
    assert result["success"] is True
    assert result["rowcount"] == 0
```

### Using Fixtures

```python
@pytest.fixture
def sample_data(query_executor):
    """Create sample data for testing"""
    query_executor.execute("CREATE TABLE users (id INT, name VARCHAR)")
    query_executor.execute("INSERT INTO users VALUES (1, 'Alice')")
    query_executor.execute("INSERT INTO users VALUES (2, 'Bob')")
    return query_executor

def test_with_fixture(sample_data):
    """Test using the fixture"""
    result = sample_data.execute("SELECT * FROM users")
    assert result["rowcount"] == 2
```

### Parameterized Tests

```python
@pytest.mark.parametrize("input_sql,expected_success", [
    ("SELECT 1", True),
    ("SELECT * FROM nonexistent", False),
    ("INVALID SQL", False),
])
def test_query_execution(query_executor, input_sql, expected_success):
    """Test multiple query scenarios"""
    result = query_executor.execute(input_sql)
    assert result["success"] == expected_success
```

### Async Tests

```python
import pytest

@pytest.mark.asyncio
async def test_async_endpoint(client):
    """Test async endpoint"""
    response = await client.get("/health")
    assert response.status_code == 200
```

## Debugging Tests

### Run Tests with Debug Output

```bash
# Show print statements
pytest -s

# Show local variables on failure
pytest -l

# Drop to debugger on failure
pytest --pdb

# Drop to debugger on first failure
pytest -x --pdb
```

### Use pytest-watch for Development

```bash
# Install pytest-watch
pip install pytest-watch

# Auto-run tests on file changes
ptw

# Run specific tests on changes
ptw tests/test_server_endpoints.py
```

## Performance Testing

### Measure Test Execution Time

```bash
# Show slowest tests
pytest --durations=10

# Show all test durations
pytest --durations=0
```

### Profile Tests

```bash
# Install pytest-profiling
pip install pytest-profiling

# Profile tests
pytest --profile

# Profile with SVG output
pytest --profile-svg
```

## Troubleshooting

### Common Issues

**Import Errors**
```bash
# Make sure package is installed
pip install -e .

# Or add to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

**Database Lock Errors**
```bash
# Use separate test directories
pytest --basetemp=/tmp/pytest
```

**SSL Certificate Errors**
```bash
# Ensure OpenSSL is installed
openssl version

# Generate test certificates
./generate-certs.sh
```

**Permission Errors in Docker**
```bash
# Run as current user
docker run --rm --user $(id -u):$(id -g) snowglobe-test pytest
```

## Best Practices

1. **Isolate Tests**: Each test should be independent
2. **Use Fixtures**: Reuse setup code with fixtures
3. **Clean Up**: Ensure tests clean up resources
4. **Test Edge Cases**: Test boundary conditions and error paths
5. **Meaningful Names**: Use descriptive test names
6. **Documentation**: Add docstrings to tests
7. **Fast Tests**: Keep tests fast for quick feedback
8. **Coverage**: Aim for high code coverage
9. **CI Integration**: Run tests automatically on commits
10. **Regular Updates**: Keep test dependencies updated

## Getting Help

- **Documentation**: See README.md and ARCHITECTURE.md
- **Issues**: Report bugs on GitHub Issues
- **Contributing**: See CONTRIBUTING.md for contribution guidelines

## Summary

```bash
# Quick reference for common commands

# Local testing
pytest                                    # Run all tests
pytest -v                                 # Verbose output
pytest -k "test_name"                    # Run specific tests
pytest --cov=snowglobe_server            # With coverage

# Docker testing  
docker-compose run --rm test             # Run all tests
docker run --rm snowglobe-test pytest    # Direct docker run

# Tox (multi-version testing)
tox                                      # Run all environments
tox -e py311                            # Specific Python version
tox -e lint                             # Code linting

# Coverage
pytest --cov=snowglobe_server --cov-report=html
open htmlcov/index.html

# Debugging
pytest -s                               # Show print output
pytest --pdb                            # Debug on failure
pytest -x                               # Stop on first failure
```
