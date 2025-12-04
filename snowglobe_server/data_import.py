"""
Data Import - Import SQL, CSV, and other data files into Snowglobe
"""

import os
import re
import csv
import json
import logging
from typing import Dict, List, Any, Optional, Tuple, BinaryIO
from datetime import datetime
from io import StringIO, BytesIO
import tempfile


logger = logging.getLogger("snowglobe.data_import")


class DataImporter:
    """
    Handles importing various data file formats into Snowglobe.
    
    Supported formats:
    - SQL files (.sql) - DDL and DML statements
    - CSV files (.csv) - Tabular data
    - JSON files (.json) - JSON arrays or objects
    - Parquet files (.parquet) - Columnar data (if pyarrow available)
    - Jupyter notebooks (.ipynb) - Extract SQL from code cells
    """
    
    def __init__(self, query_executor):
        """
        Initialize with a QueryExecutor instance.
        
        Args:
            query_executor: QueryExecutor instance for executing SQL
        """
        self.executor = query_executor
        self.import_results = []
    
    def import_file(self, file_path: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Import a file based on its extension.
        
        Args:
            file_path: Path to the file to import
            options: Import options (varies by file type)
            
        Returns:
            Import result with success status and details
        """
        if not os.path.exists(file_path):
            return {
                'success': False,
                'error': f"File not found: {file_path}",
                'file': file_path
            }
        
        ext = os.path.splitext(file_path)[1].lower()
        options = options or {}
        
        importers = {
            '.sql': self.import_sql_file,
            '.csv': self.import_csv_file,
            '.json': self.import_json_file,
            '.parquet': self.import_parquet_file,
            '.ipynb': self.import_notebook_file,
            '.tsv': lambda f, o: self.import_csv_file(f, {**o, 'delimiter': '\t'}),
            '.txt': self.import_sql_file,  # Assume SQL for txt files
        }
        
        if ext not in importers:
            return {
                'success': False,
                'error': f"Unsupported file type: {ext}",
                'file': file_path,
                'supported_types': list(importers.keys())
            }
        
        try:
            result = importers[ext](file_path, options)
            self.import_results.append(result)
            return result
        except Exception as e:
            logger.error(f"Import failed for {file_path}: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'file': file_path
            }
    
    def import_file_content(self, content: bytes, filename: str, 
                            options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Import file content directly (for uploaded files).
        
        Args:
            content: File content as bytes
            filename: Original filename (for extension detection)
            options: Import options
            
        Returns:
            Import result
        """
        # Save to temp file and import
        ext = os.path.splitext(filename)[1].lower()
        
        with tempfile.NamedTemporaryFile(mode='wb', suffix=ext, delete=False) as f:
            f.write(content)
            temp_path = f.name
        
        try:
            result = self.import_file(temp_path, options)
            result['original_filename'] = filename
            return result
        finally:
            os.unlink(temp_path)
    
    def import_sql_file(self, file_path: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Import a SQL file containing DDL/DML statements.
        
        Args:
            file_path: Path to SQL file
            options: Options including:
                - encoding: File encoding (default: utf-8)
                - stop_on_error: Stop on first error (default: False)
                - skip_comments: Skip comment-only blocks (default: True)
                
        Returns:
            Import result with statement execution details
        """
        options = options or {}
        encoding = options.get('encoding', 'utf-8')
        stop_on_error = options.get('stop_on_error', False)
        
        with open(file_path, 'r', encoding=encoding) as f:
            sql_content = f.read()
        
        # Parse SQL into individual statements
        statements = self._parse_sql_statements(sql_content)
        
        results = []
        success_count = 0
        error_count = 0
        
        for idx, stmt in enumerate(statements, 1):
            stmt = stmt.strip()
            if not stmt:
                continue
            
            result = self.executor.execute(stmt)
            
            stmt_result = {
                'statement_number': idx,
                'sql': stmt[:100] + ('...' if len(stmt) > 100 else ''),
                'success': result['success'],
                'rowcount': result.get('rowcount', 0),
                'error': result.get('error')
            }
            results.append(stmt_result)
            
            if result['success']:
                success_count += 1
            else:
                error_count += 1
                if stop_on_error:
                    break
        
        return {
            'success': error_count == 0,
            'file': file_path,
            'file_type': 'sql',
            'statements_total': len(statements),
            'statements_success': success_count,
            'statements_failed': error_count,
            'details': results,
            'imported_at': datetime.utcnow().isoformat()
        }
    
    def import_csv_file(self, file_path: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Import a CSV file into a table.
        
        Args:
            file_path: Path to CSV file
            options: Options including:
                - table_name: Target table name (default: derived from filename)
                - database: Target database
                - schema: Target schema
                - delimiter: Field delimiter (default: ,)
                - has_header: First row is header (default: True)
                - skip_rows: Number of rows to skip (default: 0)
                - null_values: List of values to treat as NULL
                - create_table: Create table if not exists (default: True)
                - truncate: Truncate table before import (default: False)
                - encoding: File encoding (default: utf-8)
                
        Returns:
            Import result with row counts
        """
        options = options or {}
        
        # Derive table name from filename if not specified
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        table_name = options.get('table_name', self._sanitize_name(base_name))
        
        database = options.get('database', self.executor.current_database)
        schema = options.get('schema', self.executor.current_schema)
        delimiter = options.get('delimiter', ',')
        has_header = options.get('has_header', True)
        skip_rows = options.get('skip_rows', 0)
        null_values = options.get('null_values', ['', 'NULL', 'null', 'None', 'N/A', 'n/a'])
        create_table = options.get('create_table', True)
        truncate = options.get('truncate', False)
        encoding = options.get('encoding', 'utf-8')
        
        with open(file_path, 'r', encoding=encoding, newline='') as f:
            # Skip specified rows
            for _ in range(skip_rows):
                next(f, None)
            
            reader = csv.reader(f, delimiter=delimiter)
            rows = list(reader)
        
        if not rows:
            return {
                'success': False,
                'error': 'CSV file is empty',
                'file': file_path
            }
        
        # Extract header and data
        if has_header:
            headers = [self._sanitize_name(h) for h in rows[0]]
            data_rows = rows[1:]
        else:
            # Generate column names
            headers = [f'COL_{i}' for i in range(len(rows[0]))]
            data_rows = rows
        
        if not headers:
            return {
                'success': False,
                'error': 'No columns found in CSV',
                'file': file_path
            }
        
        # Infer column types from data
        column_types = self._infer_column_types(data_rows, headers)
        
        # Create table if needed
        if create_table:
            columns_def = ', '.join([
                f'{col} {col_type}' 
                for col, col_type in zip(headers, column_types)
            ])
            create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})'
            result = self.executor.execute(create_sql)
            if not result['success']:
                return {
                    'success': False,
                    'error': f"Failed to create table: {result.get('error')}",
                    'file': file_path
                }
        
        # Truncate if requested
        if truncate:
            self.executor.execute(f'TRUNCATE TABLE {table_name}')
        
        # Insert data
        rows_inserted = 0
        errors = []
        
        for row_idx, row in enumerate(data_rows, 1):
            # Pad or truncate row to match columns
            row = list(row)
            while len(row) < len(headers):
                row.append(None)
            row = row[:len(headers)]
            
            # Process values
            values = []
            for val in row:
                if val in null_values:
                    values.append('NULL')
                elif isinstance(val, str):
                    # Escape single quotes
                    escaped = val.replace("'", "''")
                    values.append(f"'{escaped}'")
                else:
                    values.append(str(val) if val is not None else 'NULL')
            
            insert_sql = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({', '.join(values)})"
            
            result = self.executor.execute(insert_sql)
            if result['success']:
                rows_inserted += 1
            else:
                errors.append({
                    'row': row_idx,
                    'error': result.get('error')
                })
                # Continue on error for CSV imports
        
        return {
            'success': len(errors) == 0,
            'file': file_path,
            'file_type': 'csv',
            'table_name': table_name,
            'database': database,
            'schema': schema,
            'columns': headers,
            'column_types': column_types,
            'rows_total': len(data_rows),
            'rows_inserted': rows_inserted,
            'rows_failed': len(errors),
            'errors': errors[:10],  # Limit error details
            'imported_at': datetime.utcnow().isoformat()
        }
    
    def import_json_file(self, file_path: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Import a JSON file into a table.
        
        Args:
            file_path: Path to JSON file
            options: Options including:
                - table_name: Target table name
                - flatten: Flatten nested objects (default: False)
                - json_path: JSON path to extract array from
                
        Returns:
            Import result
        """
        options = options or {}
        
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        table_name = options.get('table_name', self._sanitize_name(base_name))
        flatten = options.get('flatten', False)
        json_path = options.get('json_path')
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract data from JSON path if specified
        if json_path:
            for key in json_path.split('.'):
                if isinstance(data, dict) and key in data:
                    data = data[key]
                else:
                    return {
                        'success': False,
                        'error': f"JSON path '{json_path}' not found",
                        'file': file_path
                    }
        
        # Ensure data is a list
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            return {
                'success': False,
                'error': 'JSON must be an array or object',
                'file': file_path
            }
        
        if not data:
            return {
                'success': False,
                'error': 'No data found in JSON',
                'file': file_path
            }
        
        # Flatten nested objects if requested
        if flatten:
            data = [self._flatten_dict(row) for row in data]
        
        # Get all possible columns from all rows
        all_columns = set()
        for row in data:
            if isinstance(row, dict):
                all_columns.update(row.keys())
        
        columns = [self._sanitize_name(c) for c in sorted(all_columns)]
        
        if not columns:
            return {
                'success': False,
                'error': 'No columns found in JSON data',
                'file': file_path
            }
        
        # Create table with VARIANT columns for JSON data
        columns_def = ', '.join([f'{col} VARCHAR' for col in columns])
        create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})'
        result = self.executor.execute(create_sql)
        
        if not result['success']:
            return {
                'success': False,
                'error': f"Failed to create table: {result.get('error')}",
                'file': file_path
            }
        
        # Insert data
        rows_inserted = 0
        for row in data:
            if not isinstance(row, dict):
                continue
            
            values = []
            for col in columns:
                orig_col = col  # Column might have been sanitized
                # Find original key
                for k in row.keys():
                    if self._sanitize_name(k) == col:
                        orig_col = k
                        break
                
                val = row.get(orig_col)
                if val is None:
                    values.append('NULL')
                elif isinstance(val, (dict, list)):
                    # Store as JSON string
                    json_str = json.dumps(val).replace("'", "''")
                    values.append(f"'{json_str}'")
                elif isinstance(val, str):
                    values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                elif isinstance(val, bool):
                    values.append('TRUE' if val else 'FALSE')
                else:
                    values.append(str(val))
            
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"
            result = self.executor.execute(insert_sql)
            if result['success']:
                rows_inserted += 1
        
        return {
            'success': True,
            'file': file_path,
            'file_type': 'json',
            'table_name': table_name,
            'columns': columns,
            'rows_total': len(data),
            'rows_inserted': rows_inserted,
            'imported_at': datetime.utcnow().isoformat()
        }
    
    def import_parquet_file(self, file_path: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Import a Parquet file into a table.
        
        Args:
            file_path: Path to Parquet file
            options: Import options
            
        Returns:
            Import result
        """
        options = options or {}
        
        try:
            import pyarrow.parquet as pq
        except ImportError:
            return {
                'success': False,
                'error': 'PyArrow not installed. Install with: pip install pyarrow',
                'file': file_path
            }
        
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        table_name = options.get('table_name', self._sanitize_name(base_name))
        
        try:
            # Read parquet file
            table = pq.read_table(file_path)
            df_dict = table.to_pydict()
            
            columns = list(df_dict.keys())
            num_rows = len(df_dict[columns[0]]) if columns else 0
            
            # Infer types from Arrow schema
            column_types = []
            for field in table.schema:
                arrow_type = str(field.type)
                if 'int' in arrow_type:
                    column_types.append('INTEGER')
                elif 'float' in arrow_type or 'double' in arrow_type:
                    column_types.append('FLOAT')
                elif 'bool' in arrow_type:
                    column_types.append('BOOLEAN')
                elif 'timestamp' in arrow_type:
                    column_types.append('TIMESTAMP')
                elif 'date' in arrow_type:
                    column_types.append('DATE')
                else:
                    column_types.append('VARCHAR')
            
            # Create table
            sanitized_columns = [self._sanitize_name(c) for c in columns]
            columns_def = ', '.join([
                f'{col} {col_type}' 
                for col, col_type in zip(sanitized_columns, column_types)
            ])
            create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})'
            result = self.executor.execute(create_sql)
            
            if not result['success']:
                return {
                    'success': False,
                    'error': f"Failed to create table: {result.get('error')}",
                    'file': file_path
                }
            
            # Insert data row by row
            rows_inserted = 0
            for i in range(num_rows):
                values = []
                for col in columns:
                    val = df_dict[col][i]
                    if val is None:
                        values.append('NULL')
                    elif isinstance(val, str):
                        values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                    elif isinstance(val, bool):
                        values.append('TRUE' if val else 'FALSE')
                    else:
                        values.append(str(val))
                
                insert_sql = f"INSERT INTO {table_name} ({', '.join(sanitized_columns)}) VALUES ({', '.join(values)})"
                result = self.executor.execute(insert_sql)
                if result['success']:
                    rows_inserted += 1
            
            return {
                'success': True,
                'file': file_path,
                'file_type': 'parquet',
                'table_name': table_name,
                'columns': sanitized_columns,
                'column_types': column_types,
                'rows_total': num_rows,
                'rows_inserted': rows_inserted,
                'imported_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'file': file_path
            }
    
    def import_notebook_file(self, file_path: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Import SQL from a Jupyter notebook.
        
        Extracts SQL from:
        - Code cells with SQL magic (%%sql, %sql)
        - Code cells with SQL strings
        - Markdown cells with SQL code blocks
        
        Args:
            file_path: Path to .ipynb file
            options: Import options
            
        Returns:
            Import result
        """
        options = options or {}
        extract_only = options.get('extract_only', False)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            notebook = json.load(f)
        
        sql_statements = []
        
        cells = notebook.get('cells', [])
        for cell in cells:
            cell_type = cell.get('cell_type', '')
            source = ''.join(cell.get('source', []))
            
            if cell_type == 'code':
                # Check for SQL magic
                if source.strip().startswith('%%sql') or source.strip().startswith('%sql'):
                    # Remove magic command and get SQL
                    lines = source.strip().split('\n')
                    sql = '\n'.join(lines[1:])
                    if sql.strip():
                        sql_statements.append(sql.strip())
                else:
                    # Look for SQL strings in Python code
                    sql_strings = self._extract_sql_from_python(source)
                    sql_statements.extend(sql_strings)
            
            elif cell_type == 'markdown':
                # Extract SQL from code blocks
                sql_blocks = re.findall(r'```sql\s*(.*?)\s*```', source, re.DOTALL | re.IGNORECASE)
                sql_statements.extend([s.strip() for s in sql_blocks if s.strip()])
        
        if extract_only:
            return {
                'success': True,
                'file': file_path,
                'file_type': 'notebook',
                'sql_statements': sql_statements,
                'statement_count': len(sql_statements),
                'extracted_only': True
            }
        
        # Execute SQL statements
        results = []
        success_count = 0
        
        for idx, sql in enumerate(sql_statements, 1):
            result = self.executor.execute(sql)
            results.append({
                'statement_number': idx,
                'sql': sql[:100] + ('...' if len(sql) > 100 else ''),
                'success': result['success'],
                'error': result.get('error')
            })
            if result['success']:
                success_count += 1
        
        return {
            'success': success_count == len(sql_statements),
            'file': file_path,
            'file_type': 'notebook',
            'statements_total': len(sql_statements),
            'statements_success': success_count,
            'statements_failed': len(sql_statements) - success_count,
            'details': results,
            'imported_at': datetime.utcnow().isoformat()
        }
    
    def _parse_sql_statements(self, sql_content: str) -> List[str]:
        """
        Parse SQL content into individual statements.
        
        Handles:
        - Semicolon-separated statements
        - Multi-line statements
        - Comments (single and multi-line)
        - String literals containing semicolons
        """
        statements = []
        current = []
        in_string = False
        string_char = None
        in_comment = False
        comment_type = None
        i = 0
        
        while i < len(sql_content):
            char = sql_content[i]
            next_char = sql_content[i + 1] if i + 1 < len(sql_content) else ''
            
            # Handle comments
            if not in_string:
                if char == '-' and next_char == '-' and not in_comment:
                    in_comment = True
                    comment_type = 'single'
                    current.append(char)
                    i += 1
                    continue
                elif char == '/' and next_char == '*' and not in_comment:
                    in_comment = True
                    comment_type = 'multi'
                    current.append(char)
                    i += 1
                    continue
            
            if in_comment:
                current.append(char)
                if comment_type == 'single' and char == '\n':
                    in_comment = False
                elif comment_type == 'multi' and char == '*' and next_char == '/':
                    current.append(next_char)
                    in_comment = False
                    i += 1
                i += 1
                continue
            
            # Handle strings
            if char in ("'", '"') and not in_string:
                in_string = True
                string_char = char
                current.append(char)
                i += 1
                continue
            elif in_string and char == string_char:
                # Check for escaped quote
                if next_char == string_char:
                    current.append(char)
                    current.append(next_char)
                    i += 2
                    continue
                else:
                    in_string = False
                    current.append(char)
                    i += 1
                    continue
            
            # Handle statement terminator
            if char == ';' and not in_string:
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(char)
            
            i += 1
        
        # Add final statement if any
        stmt = ''.join(current).strip()
        if stmt:
            statements.append(stmt)
        
        return statements
    
    def _extract_sql_from_python(self, code: str) -> List[str]:
        """Extract SQL strings from Python code."""
        sql_statements = []
        
        # Match triple-quoted strings that look like SQL
        patterns = [
            r'"""((?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|WITH).*?)"""',
            r"'''((?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|WITH).*?)'''",
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, code, re.DOTALL | re.IGNORECASE)
            sql_statements.extend([m.strip() for m in matches if m.strip()])
        
        return sql_statements
    
    def _sanitize_name(self, name: str) -> str:
        """Sanitize a name for use as a table/column name."""
        # Remove invalid characters
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Ensure starts with letter or underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = '_' + sanitized
        # Uppercase for Snowflake compatibility
        return sanitized.upper()
    
    def _infer_column_types(self, data: List[List], columns: List[str]) -> List[str]:
        """Infer column types from sample data."""
        types = ['VARCHAR'] * len(columns)
        
        for col_idx in range(len(columns)):
            sample_values = []
            for row in data[:100]:  # Sample first 100 rows
                if col_idx < len(row) and row[col_idx]:
                    sample_values.append(row[col_idx])
            
            if not sample_values:
                continue
            
            # Try to determine type
            all_int = True
            all_float = True
            all_bool = True
            all_date = True
            
            for val in sample_values:
                val_str = str(val).strip()
                
                # Check integer
                try:
                    int(val_str)
                except ValueError:
                    all_int = False
                
                # Check float
                try:
                    float(val_str)
                except ValueError:
                    all_float = False
                
                # Check boolean
                if val_str.lower() not in ('true', 'false', '1', '0', 'yes', 'no', 't', 'f'):
                    all_bool = False
                
                # Check date patterns
                date_patterns = [
                    r'^\d{4}-\d{2}-\d{2}$',
                    r'^\d{2}/\d{2}/\d{4}$',
                    r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',
                ]
                if not any(re.match(p, val_str) for p in date_patterns):
                    all_date = False
            
            if all_bool:
                types[col_idx] = 'BOOLEAN'
            elif all_int:
                types[col_idx] = 'INTEGER'
            elif all_float:
                types[col_idx] = 'FLOAT'
            elif all_date:
                types[col_idx] = 'TIMESTAMP'
            # Default is VARCHAR
        
        return types
    
    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Flatten a nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    def get_import_history(self) -> List[Dict[str, Any]]:
        """Get history of imports performed in this session."""
        return self.import_results
    
    def clear_import_history(self):
        """Clear import history."""
        self.import_results = []
