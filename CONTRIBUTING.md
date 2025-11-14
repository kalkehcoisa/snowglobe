# Contributing to Snowglobe

Thank you for your interest in contributing to Snowglobe! This document provides guidelines and information for contributors.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/your-username/snowglobe.git
   cd snowglobe
   ```

3. **Set up the development environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e .[dev,test,server]
   ```

4. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Workflow

### Running the Server Locally

```bash
# Set environment variables
export SNOWGLOBE_PORT=8084
export SNOWGLOBE_DATA_DIR=./data
export SNOWGLOBE_LOG_LEVEL=DEBUG

# Run the server
python -m snowglobe_server.server
```

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_sql_translator.py -v

# Run with coverage
pytest tests/ --cov=snowglobe_client --cov=snowglobe_server --cov-report=html
```

### Code Style

We follow PEP 8 style guidelines. Please ensure your code:

- Uses 4 spaces for indentation
- Has a maximum line length of 100 characters
- Includes docstrings for all public modules, classes, and functions
- Uses type hints where appropriate

```bash
# Format code with black
black snowglobe_server/ snowglobe_client/ tests/

# Sort imports with isort
isort snowglobe_server/ snowglobe_client/ tests/

# Check style with flake8
flake8 snowglobe_server/ snowglobe_client/ tests/

# Type checking with mypy
mypy snowglobe_server/ snowglobe_client/
```

## Areas for Contribution

### High Priority

1. **SQL Function Support**
   - Add more Snowflake-specific functions
   - Improve SQL translation accuracy
   - Handle edge cases in SQL parsing

2. **Data Type Support**
   - Better VARIANT/OBJECT/ARRAY handling
   - Improved type casting
   - Semi-structured data operations

3. **Performance Optimization**
   - Connection pooling
   - Query caching
   - Batch operations

### Medium Priority

4. **Documentation**
   - API documentation
   - More examples
   - Tutorial content

5. **Testing**
   - Increase test coverage
   - Add integration tests
   - Performance benchmarks

6. **Error Handling**
   - Better error messages
   - Error code compatibility with Snowflake
   - Logging improvements

### Future Features

7. **Advanced Features**
   - Stored procedures (limited support)
   - User-defined functions
   - Streams and tasks (simulation)

8. **Tooling**
   - CLI tools
   - Configuration management
   - Health monitoring

## Adding New SQL Functions

To add support for a new Snowflake SQL function:

1. **Identify the function** in Snowflake documentation
2. **Add translation logic** in `snowglobe_server/sql_translator.py`:

```python
def _translate_new_function(self, sql: str) -> str:
    """Translate NEW_FUNCTION to DuckDB equivalent"""
    pattern = r'\bNEW_FUNCTION\s*\(\s*([^)]+)\s*\)'
    
    def replace_func(match):
        args = match.group(1)
        # Transform to DuckDB equivalent
        return f"DUCKDB_EQUIVALENT({args})"
    
    return re.sub(pattern, replace_func, sql, flags=re.IGNORECASE)
```

3. **Call the translation** in the `translate` method:
```python
def translate(self, sql: str) -> str:
    # ... existing translations ...
    translated = self._translate_new_function(translated)
    return translated
```

4. **Add tests** in `tests/test_sql_translator.py`:
```python
def test_new_function(self):
    sql = "SELECT NEW_FUNCTION(arg1, arg2)"
    result = self.translator.translate(sql)
    assert "DUCKDB_EQUIVALENT" in result
```

5. **Update documentation** in README.md

## Pull Request Process

1. **Update documentation** for any new features
2. **Add tests** for new functionality
3. **Ensure all tests pass** locally
4. **Update CHANGELOG.md** with your changes
5. **Submit pull request** with:
   - Clear description of changes
   - Reference to any related issues
   - Screenshots/examples if applicable

### PR Checklist

- [ ] Code follows project style guidelines
- [ ] Tests added and passing
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] No merge conflicts
- [ ] Reviewers assigned

## Reporting Issues

When reporting issues, please include:

1. **Description** of the problem
2. **Steps to reproduce**
3. **Expected behavior**
4. **Actual behavior**
5. **Environment details**:
   - Python version
   - OS and version
   - Snowglobe version
   - Docker version (if applicable)

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Follow the project's coding standards

## Questions?

- Open a GitHub issue for questions
- Tag issues appropriately
- Check existing issues before creating new ones

Thank you for contributing to Snowglobe! 🌐❄️
