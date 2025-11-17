# Quick Testing Guide

This is a quick reference for running tests. For comprehensive documentation, see [TESTING.md](TESTING.md).

## Quick Commands

### Local Testing
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=snowglobe_server --cov-report=term-missing

# Run specific test file
pytest tests/test_server_endpoints.py

# Run fast tests only (skip slow tests)
pytest -m "not slow"
```

### Docker Testing
```bash
# Run tests in Docker
docker-compose run --rm test

# Run specific tests in Docker
docker-compose run --rm test pytest tests/test_ssl.py -v
```

### Using Test Runner Script
```bash
# Make script executable (first time only)
chmod +x run-tests.sh

# Run all tests
./run-tests.sh all

# Run with coverage
./run-tests.sh coverage

# Run API tests with verbose output
./run-tests.sh api verbose

# Run in Docker
./run-tests.sh docker all
```

## Test Categories

Run specific test categories using markers:

```bash
# API endpoint tests
pytest -m api

# SSL/TLS tests
pytest -m ssl

# Edge case tests
pytest -m edge_case

# Performance tests
pytest -m performance

# Fast tests only
pytest -m "not slow and not performance"
```

## Common Issues

### Import Errors
```bash
# Install package in development mode
pip install -e .
```

### Missing Dependencies
```bash
# Install test dependencies
pip install pytest pytest-cov pytest-asyncio httpx
```

### SSL Tests Failing
Ensure OpenSSL is installed:
```bash
# Check OpenSSL
openssl version

# Install on Ubuntu/Debian
sudo apt-get install openssl

# Install on macOS
brew install openssl
```

## See Also

- [TESTING.md](TESTING.md) - Comprehensive testing guide
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contributing guidelines
- [README.md](README.md) - Main documentation
