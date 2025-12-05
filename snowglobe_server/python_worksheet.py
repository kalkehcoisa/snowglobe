"""
Python Worksheet Support - Snowpark-compatible Python execution for Snowglobe

This module provides support for Python worksheets that can execute Python code
with access to Snowpark-like APIs for interacting with the local database.
"""

import os
import sys
import io
import ast
import json
import traceback
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import redirect_stdout, redirect_stderr
import threading

logger = logging.getLogger("snowglobe.python_worksheet")


class SnowparkSession:
    """
    A Snowpark-compatible session for local execution.
    Provides DataFrame API and SQL execution capabilities.
    """
    
    def __init__(self, executor):
        """
        Initialize the Snowpark session.
        
        Args:
            executor: QueryExecutor instance for database operations
        """
        self.executor = executor
        self._current_database = executor.current_database
        self._current_schema = executor.current_schema
        self._current_warehouse = executor.current_warehouse
        self._current_role = executor.current_role
    
    @property
    def current_database(self) -> str:
        return self._current_database
    
    @property
    def current_schema(self) -> str:
        return self._current_schema
    
    @property
    def current_warehouse(self) -> str:
        return self._current_warehouse
    
    @property
    def current_role(self) -> str:
        return self._current_role
    
    def use_database(self, database: str):
        """Switch to a different database."""
        self._current_database = database.upper()
        self.executor.execute(f"USE DATABASE {database}")
    
    def use_schema(self, schema: str):
        """Switch to a different schema."""
        self._current_schema = schema.upper()
        self.executor.execute(f"USE SCHEMA {schema}")
    
    def use_warehouse(self, warehouse: str):
        """Switch to a different warehouse."""
        self._current_warehouse = warehouse.upper()
        self.executor.execute(f"USE WAREHOUSE {warehouse}")
    
    def use_role(self, role: str):
        """Switch to a different role."""
        self._current_role = role.upper()
        self.executor.execute(f"USE ROLE {role}")
    
    def sql(self, query: str) -> 'DataFrame':
        """
        Execute a SQL query and return results as a DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame with query results
        """
        result = self.executor.execute(query)
        if result["success"]:
            return DataFrame(self, result["columns"], result["data"], query)
        else:
            raise Exception(f"SQL execution failed: {result.get('error', 'Unknown error')}")
    
    def table(self, name: str) -> 'DataFrame':
        """
        Get a DataFrame representing a table.
        
        Args:
            name: Table name (can include database and schema)
            
        Returns:
            DataFrame for the table
        """
        # Parse the table name
        parts = name.upper().split(".")
        if len(parts) == 3:
            full_name = name.upper()
        elif len(parts) == 2:
            full_name = f"{self._current_database}.{name.upper()}"
        else:
            full_name = f"{self._current_database}.{self._current_schema}.{name.upper()}"
        
        return DataFrame(self, query=f"SELECT * FROM {full_name}")
    
    def create_dataframe(self, data: List[List], schema: List[str] = None) -> 'DataFrame':
        """
        Create a DataFrame from local data.
        
        Args:
            data: List of rows (each row is a list of values)
            schema: Optional list of column names
            
        Returns:
            DataFrame with the local data
        """
        if schema is None:
            schema = [f"COL{i}" for i in range(len(data[0]) if data else 0)]
        
        return DataFrame(self, schema, data)
    
    def get_current_database(self) -> str:
        """Get the current database name."""
        return self._current_database
    
    def get_current_schema(self) -> str:
        """Get the current schema name."""
        return self._current_schema
    
    def get_fully_qualified_current_schema(self) -> str:
        """Get the fully qualified current schema name."""
        return f"{self._current_database}.{self._current_schema}"
    
    def call(self, sproc_name: str, *args) -> Any:
        """
        Call a stored procedure.
        
        Args:
            sproc_name: Name of the stored procedure
            *args: Arguments to pass to the procedure
            
        Returns:
            Result of the procedure call
        """
        # Format args as SQL literals
        formatted_args = []
        for arg in args:
            if arg is None:
                formatted_args.append("NULL")
            elif isinstance(arg, str):
                formatted_args.append(f"'{arg}'")
            elif isinstance(arg, bool):
                formatted_args.append("TRUE" if arg else "FALSE")
            else:
                formatted_args.append(str(arg))
        
        query = f"CALL {sproc_name}({', '.join(formatted_args)})"
        result = self.executor.execute(query)
        
        if result["success"]:
            if result["data"]:
                return result["data"][0][0] if result["data"][0] else None
            return None
        else:
            raise Exception(f"Procedure call failed: {result.get('error')}")
    
    def close(self):
        """Close the session (no-op for local execution)."""
        pass


class DataFrame:
    """
    A Snowpark-compatible DataFrame for local execution.
    Supports common transformations and actions.
    """
    
    def __init__(self, session: SnowparkSession, columns: List[str] = None, 
                 data: List[List] = None, query: str = None):
        """
        Initialize a DataFrame.
        
        Args:
            session: Parent Snowpark session
            columns: Column names
            data: Row data
            query: SQL query that produces this DataFrame
        """
        self._session = session
        self._columns = columns or []
        self._data = data
        self._query = query
        self._collected = False
        
        # If we have a query but no data, we're lazy
        if query and data is None:
            self._lazy = True
        else:
            self._lazy = False
    
    def _ensure_collected(self):
        """Ensure the data has been collected from the database."""
        if self._lazy and self._data is None:
            result = self._session.executor.execute(self._query)
            if result["success"]:
                self._columns = result["columns"]
                self._data = result["data"]
            else:
                raise Exception(f"Query execution failed: {result.get('error')}")
    
    @property
    def columns(self) -> List[str]:
        """Get column names."""
        self._ensure_collected()
        return self._columns
    
    @property
    def schema(self) -> 'StructType':
        """Get the DataFrame schema."""
        self._ensure_collected()
        return StructType([StructField(col, StringType()) for col in self._columns])
    
    def collect(self) -> List['Row']:
        """
        Collect all rows as a list of Row objects.
        
        Returns:
            List of Row objects
        """
        self._ensure_collected()
        return [Row(**dict(zip(self._columns, row))) for row in self._data]
    
    def to_local_iterator(self):
        """Return an iterator over the rows."""
        self._ensure_collected()
        for row in self._data:
            yield Row(**dict(zip(self._columns, row)))
    
    def to_pandas(self):
        """
        Convert to a pandas DataFrame.
        
        Returns:
            pandas DataFrame
        """
        try:
            import pandas as pd
            self._ensure_collected()
            return pd.DataFrame(self._data, columns=self._columns)
        except ImportError:
            raise ImportError("pandas is required for to_pandas()")
    
    def count(self) -> int:
        """Return the number of rows."""
        self._ensure_collected()
        return len(self._data) if self._data else 0
    
    def show(self, n: int = 10, max_width: int = 50) -> str:
        """
        Return a string representation of the first n rows.
        
        Args:
            n: Number of rows to show
            max_width: Maximum width for each column
            
        Returns:
            Formatted string representation
        """
        self._ensure_collected()
        
        if not self._data:
            return "(Empty DataFrame)"
        
        # Calculate column widths
        widths = []
        for i, col in enumerate(self._columns):
            col_width = len(col)
            for row in self._data[:n]:
                if i < len(row):
                    val_width = len(str(row[i]) if row[i] is not None else "NULL")
                    col_width = max(col_width, min(val_width, max_width))
            widths.append(col_width)
        
        # Build output
        lines = []
        
        # Header
        header = " | ".join(col.ljust(widths[i]) for i, col in enumerate(self._columns))
        lines.append(header)
        lines.append("-" * len(header))
        
        # Data rows
        for row in self._data[:n]:
            row_str = " | ".join(
                str(row[i] if row[i] is not None else "NULL")[:max_width].ljust(widths[i])
                if i < len(row) else "".ljust(widths[i])
                for i in range(len(self._columns))
            )
            lines.append(row_str)
        
        if len(self._data) > n:
            lines.append(f"... ({len(self._data) - n} more rows)")
        
        return "\n".join(lines)
    
    def select(self, *cols) -> 'DataFrame':
        """
        Select specific columns.
        
        Args:
            *cols: Column names or expressions
            
        Returns:
            New DataFrame with selected columns
        """
        col_strs = []
        for col in cols:
            if isinstance(col, Column):
                col_strs.append(col._expr)
            else:
                col_strs.append(str(col))
        
        if self._query:
            new_query = f"SELECT {', '.join(col_strs)} FROM ({self._query}) AS _subquery"
        else:
            # If we have local data, select columns
            self._ensure_collected()
            indices = [self._columns.index(c.upper()) for c in cols if c.upper() in [col.upper() for col in self._columns]]
            new_data = [[row[i] for i in indices] for row in self._data]
            return DataFrame(self._session, [cols[i] for i in range(len(indices))], new_data)
        
        return DataFrame(self._session, query=new_query)
    
    def filter(self, condition) -> 'DataFrame':
        """
        Filter rows based on a condition.
        
        Args:
            condition: Column expression representing the filter condition
            
        Returns:
            New filtered DataFrame
        """
        if isinstance(condition, Column):
            cond_str = condition._expr
        else:
            cond_str = str(condition)
        
        if self._query:
            new_query = f"SELECT * FROM ({self._query}) AS _subquery WHERE {cond_str}"
        else:
            new_query = f"SELECT * FROM _local_data WHERE {cond_str}"
        
        return DataFrame(self._session, query=new_query)
    
    def where(self, condition) -> 'DataFrame':
        """Alias for filter."""
        return self.filter(condition)
    
    def limit(self, n: int) -> 'DataFrame':
        """
        Limit the number of rows.
        
        Args:
            n: Maximum number of rows
            
        Returns:
            New limited DataFrame
        """
        if self._query:
            new_query = f"SELECT * FROM ({self._query}) AS _subquery LIMIT {n}"
            return DataFrame(self._session, query=new_query)
        else:
            self._ensure_collected()
            return DataFrame(self._session, self._columns, self._data[:n])
    
    def order_by(self, *cols) -> 'DataFrame':
        """
        Order by columns.
        
        Args:
            *cols: Column names or expressions
            
        Returns:
            New ordered DataFrame
        """
        col_strs = []
        for col in cols:
            if isinstance(col, Column):
                col_strs.append(col._expr)
            else:
                col_strs.append(str(col))
        
        if self._query:
            new_query = f"SELECT * FROM ({self._query}) AS _subquery ORDER BY {', '.join(col_strs)}"
            return DataFrame(self._session, query=new_query)
        else:
            # Can't order local data easily without pandas
            return self
    
    def sort(self, *cols) -> 'DataFrame':
        """Alias for order_by."""
        return self.order_by(*cols)
    
    def group_by(self, *cols) -> 'GroupedDataFrame':
        """
        Group by columns.
        
        Args:
            *cols: Column names
            
        Returns:
            GroupedDataFrame for aggregation
        """
        return GroupedDataFrame(self, cols)
    
    def join(self, other: 'DataFrame', on: str = None, how: str = "inner") -> 'DataFrame':
        """
        Join with another DataFrame.
        
        Args:
            other: Other DataFrame to join with
            on: Join condition
            how: Join type (inner, left, right, full)
            
        Returns:
            New joined DataFrame
        """
        if not self._query or not other._query:
            raise ValueError("Both DataFrames must be query-based for join")
        
        join_type = how.upper()
        new_query = f"""
            SELECT * FROM ({self._query}) AS _left
            {join_type} JOIN ({other._query}) AS _right
            ON {on}
        """
        return DataFrame(self._session, query=new_query)
    
    def union(self, other: 'DataFrame') -> 'DataFrame':
        """
        Union with another DataFrame.
        
        Args:
            other: Other DataFrame
            
        Returns:
            New DataFrame with unioned rows
        """
        if self._query and other._query:
            new_query = f"({self._query}) UNION ({other._query})"
            return DataFrame(self._session, query=new_query)
        else:
            self._ensure_collected()
            other._ensure_collected()
            return DataFrame(self._session, self._columns, self._data + other._data)
    
    def union_all(self, other: 'DataFrame') -> 'DataFrame':
        """
        Union all with another DataFrame (keeps duplicates).
        
        Args:
            other: Other DataFrame
            
        Returns:
            New DataFrame with all rows
        """
        if self._query and other._query:
            new_query = f"({self._query}) UNION ALL ({other._query})"
            return DataFrame(self._session, query=new_query)
        else:
            self._ensure_collected()
            other._ensure_collected()
            return DataFrame(self._session, self._columns, self._data + other._data)
    
    def distinct(self) -> 'DataFrame':
        """
        Remove duplicate rows.
        
        Returns:
            New DataFrame with distinct rows
        """
        if self._query:
            new_query = f"SELECT DISTINCT * FROM ({self._query}) AS _subquery"
            return DataFrame(self._session, query=new_query)
        else:
            self._ensure_collected()
            seen = set()
            new_data = []
            for row in self._data:
                row_tuple = tuple(row)
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    new_data.append(row)
            return DataFrame(self._session, self._columns, new_data)
    
    def drop(self, *cols) -> 'DataFrame':
        """
        Drop columns.
        
        Args:
            *cols: Column names to drop
            
        Returns:
            New DataFrame without dropped columns
        """
        self._ensure_collected()
        drop_cols = set(c.upper() for c in cols)
        keep_indices = [i for i, c in enumerate(self._columns) if c.upper() not in drop_cols]
        new_columns = [self._columns[i] for i in keep_indices]
        new_data = [[row[i] for i in keep_indices] for row in self._data]
        return DataFrame(self._session, new_columns, new_data)
    
    def with_column(self, col_name: str, col_expr) -> 'DataFrame':
        """
        Add or replace a column.
        
        Args:
            col_name: Name for the new/replaced column
            col_expr: Expression for the column value
            
        Returns:
            New DataFrame with the column
        """
        if isinstance(col_expr, Column):
            expr_str = col_expr._expr
        else:
            expr_str = str(col_expr)
        
        if self._query:
            new_query = f"SELECT *, {expr_str} AS {col_name} FROM ({self._query}) AS _subquery"
            return DataFrame(self._session, query=new_query)
        else:
            raise ValueError("with_column requires a query-based DataFrame")
    
    def rename(self, col_name: str, new_name: str) -> 'DataFrame':
        """
        Rename a column.
        
        Args:
            col_name: Current column name
            new_name: New column name
            
        Returns:
            New DataFrame with renamed column
        """
        self._ensure_collected()
        new_columns = [new_name if c.upper() == col_name.upper() else c for c in self._columns]
        return DataFrame(self._session, new_columns, self._data)
    
    def write_table(self, table_name: str, mode: str = "errorifexists"):
        """
        Write the DataFrame to a table.
        
        Args:
            table_name: Target table name
            mode: Write mode (errorifexists, overwrite, append)
        """
        self._ensure_collected()
        
        # Parse table name
        parts = table_name.upper().split(".")
        if len(parts) == 3:
            full_name = table_name.upper()
        elif len(parts) == 2:
            full_name = f"{self._session._current_database}.{table_name.upper()}"
        else:
            full_name = f"{self._session._current_database}.{self._session._current_schema}.{table_name.upper()}"
        
        if mode == "overwrite":
            self._session.executor.execute(f"DROP TABLE IF EXISTS {full_name}")
            create_mode = "CREATE"
        elif mode == "append":
            create_mode = "CREATE OR REPLACE"
        else:
            create_mode = "CREATE"
        
        if mode != "append":
            # Create table
            col_defs = ", ".join(f"{col} VARCHAR" for col in self._columns)
            self._session.executor.execute(f"{create_mode} TABLE {full_name} ({col_defs})")
        
        # Insert data
        if self._data:
            for row in self._data:
                values = ", ".join(
                    "NULL" if v is None else f"'{str(v)}'" 
                    for v in row
                )
                self._session.executor.execute(f"INSERT INTO {full_name} VALUES ({values})")
    
    def to_dict(self) -> Dict[str, List]:
        """Convert to a dictionary of lists."""
        self._ensure_collected()
        return {col: [row[i] for row in self._data] for i, col in enumerate(self._columns)}
    
    def __repr__(self):
        return f"DataFrame[{', '.join(self._columns) if self._columns else '...'}]"


class GroupedDataFrame:
    """Represents a grouped DataFrame for aggregations."""
    
    def __init__(self, df: DataFrame, group_cols: tuple):
        self._df = df
        self._group_cols = group_cols
    
    def agg(self, *agg_exprs) -> DataFrame:
        """
        Aggregate the grouped data.
        
        Args:
            *agg_exprs: Aggregation expressions
            
        Returns:
            Aggregated DataFrame
        """
        col_strs = list(self._group_cols)
        for expr in agg_exprs:
            if isinstance(expr, Column):
                col_strs.append(expr._expr)
            elif isinstance(expr, tuple) and len(expr) == 2:
                col, func = expr
                col_strs.append(f"{func.upper()}({col}) AS {col}_{func}")
            else:
                col_strs.append(str(expr))
        
        group_str = ", ".join(str(c) for c in self._group_cols)
        
        if self._df._query:
            new_query = f"""
                SELECT {', '.join(col_strs)}
                FROM ({self._df._query}) AS _subquery
                GROUP BY {group_str}
            """
            return DataFrame(self._df._session, query=new_query)
        else:
            raise ValueError("Aggregation requires a query-based DataFrame")
    
    def count(self) -> DataFrame:
        """Count rows in each group."""
        return self.agg(Column("COUNT(*)").alias("COUNT"))
    
    def sum(self, col: str) -> DataFrame:
        """Sum a column."""
        return self.agg(Column(f"SUM({col})").alias(f"SUM_{col}"))
    
    def avg(self, col: str) -> DataFrame:
        """Average a column."""
        return self.agg(Column(f"AVG({col})").alias(f"AVG_{col}"))
    
    def min(self, col: str) -> DataFrame:
        """Minimum of a column."""
        return self.agg(Column(f"MIN({col})").alias(f"MIN_{col}"))
    
    def max(self, col: str) -> DataFrame:
        """Maximum of a column."""
        return self.agg(Column(f"MAX({col})").alias(f"MAX_{col}"))


class Column:
    """Represents a column expression."""
    
    def __init__(self, expr: str):
        self._expr = expr
    
    def alias(self, name: str) -> 'Column':
        """Create an alias for this column."""
        return Column(f"{self._expr} AS {name}")
    
    def asc(self) -> 'Column':
        """Sort ascending."""
        return Column(f"{self._expr} ASC")
    
    def desc(self) -> 'Column':
        """Sort descending."""
        return Column(f"{self._expr} DESC")
    
    def __eq__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} = {other._expr}")
        elif other is None:
            return Column(f"{self._expr} IS NULL")
        elif isinstance(other, str):
            return Column(f"{self._expr} = '{other}'")
        else:
            return Column(f"{self._expr} = {other}")
    
    def __ne__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} != {other._expr}")
        elif other is None:
            return Column(f"{self._expr} IS NOT NULL")
        elif isinstance(other, str):
            return Column(f"{self._expr} != '{other}'")
        else:
            return Column(f"{self._expr} != {other}")
    
    def __lt__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} < {other._expr}")
        elif isinstance(other, str):
            return Column(f"{self._expr} < '{other}'")
        else:
            return Column(f"{self._expr} < {other}")
    
    def __le__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} <= {other._expr}")
        elif isinstance(other, str):
            return Column(f"{self._expr} <= '{other}'")
        else:
            return Column(f"{self._expr} <= {other}")
    
    def __gt__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} > {other._expr}")
        elif isinstance(other, str):
            return Column(f"{self._expr} > '{other}'")
        else:
            return Column(f"{self._expr} > {other}")
    
    def __ge__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} >= {other._expr}")
        elif isinstance(other, str):
            return Column(f"{self._expr} >= '{other}'")
        else:
            return Column(f"{self._expr} >= {other}")
    
    def __and__(self, other):
        return Column(f"({self._expr}) AND ({other._expr})")
    
    def __or__(self, other):
        return Column(f"({self._expr}) OR ({other._expr})")
    
    def __invert__(self):
        return Column(f"NOT ({self._expr})")
    
    def __add__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} + {other._expr}")
        return Column(f"{self._expr} + {other}")
    
    def __sub__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} - {other._expr}")
        return Column(f"{self._expr} - {other}")
    
    def __mul__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} * {other._expr}")
        return Column(f"{self._expr} * {other}")
    
    def __truediv__(self, other):
        if isinstance(other, Column):
            return Column(f"{self._expr} / {other._expr}")
        return Column(f"{self._expr} / {other}")
    
    def is_null(self) -> 'Column':
        return Column(f"{self._expr} IS NULL")
    
    def is_not_null(self) -> 'Column':
        return Column(f"{self._expr} IS NOT NULL")
    
    def like(self, pattern: str) -> 'Column':
        return Column(f"{self._expr} LIKE '{pattern}'")
    
    def ilike(self, pattern: str) -> 'Column':
        return Column(f"{self._expr} ILIKE '{pattern}'")
    
    def in_(self, *values) -> 'Column':
        formatted_values = []
        for v in values:
            if isinstance(v, str):
                formatted_values.append(f"'{v}'")
            else:
                formatted_values.append(str(v))
        return Column(f"{self._expr} IN ({', '.join(formatted_values)})")
    
    def between(self, lower, upper) -> 'Column':
        return Column(f"{self._expr} BETWEEN {lower} AND {upper}")
    
    def cast(self, data_type: str) -> 'Column':
        return Column(f"CAST({self._expr} AS {data_type})")
    
    def __repr__(self):
        return f"Column({self._expr})"


class Row:
    """Represents a row of data."""
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        self._fields = list(kwargs.keys())
    
    def __getitem__(self, item):
        if isinstance(item, int):
            return getattr(self, self._fields[item])
        return getattr(self, item)
    
    def __repr__(self):
        return f"Row({', '.join(f'{k}={getattr(self, k)!r}' for k in self._fields)})"
    
    def as_dict(self) -> Dict:
        return {k: getattr(self, k) for k in self._fields}


# Type classes for schema
class DataType:
    def __repr__(self):
        return self.__class__.__name__


class StringType(DataType):
    pass


class IntegerType(DataType):
    pass


class LongType(DataType):
    pass


class FloatType(DataType):
    pass


class DoubleType(DataType):
    pass


class BooleanType(DataType):
    pass


class DateType(DataType):
    pass


class TimestampType(DataType):
    pass


class DecimalType(DataType):
    def __init__(self, precision: int = 38, scale: int = 0):
        self.precision = precision
        self.scale = scale


class ArrayType(DataType):
    def __init__(self, element_type: DataType):
        self.element_type = element_type


class MapType(DataType):
    def __init__(self, key_type: DataType, value_type: DataType):
        self.key_type = key_type
        self.value_type = value_type


class StructField:
    def __init__(self, name: str, datatype: DataType, nullable: bool = True):
        self.name = name
        self.datatype = datatype
        self.nullable = nullable


class StructType:
    def __init__(self, fields: List[StructField] = None):
        self.fields = fields or []
    
    def add(self, name: str, datatype: DataType, nullable: bool = True):
        self.fields.append(StructField(name, datatype, nullable))
        return self


# Helper functions to create columns
def col(name: str) -> Column:
    """Create a Column from a name."""
    return Column(name)


def lit(value) -> Column:
    """Create a literal value column."""
    if value is None:
        return Column("NULL")
    elif isinstance(value, str):
        return Column(f"'{value}'")
    elif isinstance(value, bool):
        return Column("TRUE" if value else "FALSE")
    else:
        return Column(str(value))


# Aggregation functions
def count(col_name: str = "*") -> Column:
    return Column(f"COUNT({col_name})")


def sum_(col_name: str) -> Column:
    return Column(f"SUM({col_name})")


def avg(col_name: str) -> Column:
    return Column(f"AVG({col_name})")


def min_(col_name: str) -> Column:
    return Column(f"MIN({col_name})")


def max_(col_name: str) -> Column:
    return Column(f"MAX({col_name})")


def coalesce(*cols) -> Column:
    exprs = []
    for c in cols:
        if isinstance(c, Column):
            exprs.append(c._expr)
        else:
            exprs.append(str(c))
    return Column(f"COALESCE({', '.join(exprs)})")


class PythonWorksheetExecutor:
    """
    Executes Python worksheets with Snowpark-like capabilities.
    """
    
    def __init__(self, query_executor):
        """
        Initialize the Python worksheet executor.
        
        Args:
            query_executor: QueryExecutor instance for database operations
        """
        self.query_executor = query_executor
        self._execution_count = 0
    
    def execute(self, code: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute Python code with Snowpark-like APIs available.
        
        Args:
            code: Python code to execute
            context: Optional execution context (variables to inject)
            
        Returns:
            Dictionary with execution results
        """
        self._execution_count += 1
        start_time = datetime.utcnow()
        
        # Create session and prepare namespace
        session = SnowparkSession(self.query_executor)
        
        # Prepare the namespace with Snowpark-like APIs
        namespace = {
            # Session and DataFrame
            'session': session,
            'Session': SnowparkSession,
            'DataFrame': DataFrame,
            'Column': Column,
            'Row': Row,
            
            # Helper functions
            'col': col,
            'lit': lit,
            
            # Aggregation functions
            'count': count,
            'sum': sum_,
            'avg': avg,
            'min': min_,
            'max': max_,
            'coalesce': coalesce,
            
            # Type classes
            'StringType': StringType,
            'IntegerType': IntegerType,
            'LongType': LongType,
            'FloatType': FloatType,
            'DoubleType': DoubleType,
            'BooleanType': BooleanType,
            'DateType': DateType,
            'TimestampType': TimestampType,
            'DecimalType': DecimalType,
            'ArrayType': ArrayType,
            'MapType': MapType,
            'StructField': StructField,
            'StructType': StructType,
            
            # Standard library
            'print': print,
            'len': len,
            'range': range,
            'list': list,
            'dict': dict,
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'set': set,
            'tuple': tuple,
            'type': type,
            'isinstance': isinstance,
            'enumerate': enumerate,
            'zip': zip,
            'map': map,
            'filter': filter,
            'sorted': sorted,
            'reversed': reversed,
            'sum': sum,  # Override for Python's sum
            'min': min,  # Override for Python's min  
            'max': max,  # Override for Python's max
            'abs': abs,
            'round': round,
            'json': json,
            'datetime': datetime,
        }
        
        # Add any additional context variables
        if context:
            namespace.update(context)
        
        # Try to import optional libraries
        try:
            import pandas as pd
            namespace['pd'] = pd
            namespace['pandas'] = pd
        except ImportError:
            pass
        
        try:
            import numpy as np
            namespace['np'] = np
            namespace['numpy'] = np
        except ImportError:
            pass
        
        # Capture stdout and stderr
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()
        
        result = {
            "success": False,
            "output": "",
            "error": None,
            "variables": {},
            "dataframes": [],
            "duration_ms": 0,
            "execution_id": self._execution_count
        }
        
        try:
            # Validate the code
            ast.parse(code)
            
            # Execute with captured output
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                exec(code, namespace)
            
            result["success"] = True
            result["output"] = stdout_capture.getvalue()
            
            # Collect output variables (skip internal ones)
            internal_names = {
                'session', 'Session', 'DataFrame', 'Column', 'Row',
                'col', 'lit', 'count', 'sum', 'avg', 'min', 'max', 'coalesce',
                'StringType', 'IntegerType', 'LongType', 'FloatType', 'DoubleType',
                'BooleanType', 'DateType', 'TimestampType', 'DecimalType',
                'ArrayType', 'MapType', 'StructField', 'StructType',
                'print', 'len', 'range', 'list', 'dict', 'str', 'int', 'float',
                'bool', 'set', 'tuple', 'type', 'isinstance', 'enumerate', 'zip',
                'map', 'filter', 'sorted', 'reversed', 'abs', 'round',
                'json', 'datetime', 'pd', 'pandas', 'np', 'numpy',
                '__builtins__', '__doc__', '__name__', '__package__'
            }
            
            for name, value in namespace.items():
                if name.startswith('_'):
                    continue
                if name in internal_names:
                    continue
                
                # Handle different types
                if isinstance(value, DataFrame):
                    try:
                        value._ensure_collected()
                        result["dataframes"].append({
                            "name": name,
                            "columns": value._columns,
                            "data": value._data[:100],  # Limit rows
                            "total_rows": len(value._data) if value._data else 0,
                            "preview": value.show(10)
                        })
                    except Exception as e:
                        result["dataframes"].append({
                            "name": name,
                            "error": str(e)
                        })
                elif isinstance(value, (str, int, float, bool, type(None))):
                    result["variables"][name] = value
                elif isinstance(value, (list, dict)):
                    try:
                        result["variables"][name] = json.loads(json.dumps(value, default=str))
                    except:
                        result["variables"][name] = str(value)
                elif hasattr(value, '__dict__'):
                    result["variables"][name] = str(value)
            
        except SyntaxError as e:
            result["error"] = f"Syntax Error: {e.msg} at line {e.lineno}"
            result["output"] = stderr_capture.getvalue()
        except Exception as e:
            result["error"] = f"{type(e).__name__}: {str(e)}"
            result["output"] = stderr_capture.getvalue() + "\n" + traceback.format_exc()
        
        # Calculate duration
        end_time = datetime.utcnow()
        result["duration_ms"] = (end_time - start_time).total_seconds() * 1000
        
        return result
    
    def validate_code(self, code: str) -> Dict[str, Any]:
        """
        Validate Python code without executing it.
        
        Args:
            code: Python code to validate
            
        Returns:
            Dictionary with validation results
        """
        try:
            tree = ast.parse(code)
            
            # Analyze the code
            imports = []
            functions = []
            classes = []
            variables = []
            
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    imports.append(f"{node.module}")
                elif isinstance(node, ast.FunctionDef):
                    functions.append(node.name)
                elif isinstance(node, ast.AsyncFunctionDef):
                    functions.append(f"async {node.name}")
                elif isinstance(node, ast.ClassDef):
                    classes.append(node.name)
                elif isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            variables.append(target.id)
            
            return {
                "valid": True,
                "imports": imports,
                "functions": functions,
                "classes": classes,
                "variables": variables,
                "line_count": len(code.split('\n'))
            }
        except SyntaxError as e:
            return {
                "valid": False,
                "error": f"Syntax Error: {e.msg}",
                "line": e.lineno,
                "offset": e.offset
            }
        except Exception as e:
            return {
                "valid": False,
                "error": str(e)
            }


# Example Python worksheet templates
PYTHON_TEMPLATES = {
    "basic_query": '''# Basic SQL query with Snowpark
df = session.sql("SELECT * FROM my_table LIMIT 10")
print(df.show())
''',
    
    "table_operations": '''# Table operations
# Get a table as DataFrame
customers = session.table("customers")

# Select and filter
result = customers.select("id", "name", "email") \\
    .filter(col("status") == "active") \\
    .limit(100)

print(result.show())
print(f"Total rows: {result.count()}")
''',
    
    "aggregations": '''# Aggregation example
df = session.sql("""
    SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
    ORDER BY emp_count DESC
""")
print(df.show())
''',
    
    "create_dataframe": '''# Create DataFrame from local data
data = [
    ["Alice", 30, "Engineering"],
    ["Bob", 25, "Marketing"],
    ["Carol", 35, "Sales"]
]
schema = ["name", "age", "department"]

df = session.create_dataframe(data, schema)
print(df.show())

# Write to table
df.write_table("my_new_table", mode="overwrite")
print("Table created!")
''',
    
    "joins": '''# Join example
orders = session.table("orders")
customers = session.table("customers")

# Join and aggregate
result = session.sql("""
    SELECT c.name, COUNT(o.id) as order_count, SUM(o.amount) as total_spent
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id
    GROUP BY c.name
    ORDER BY total_spent DESC
    LIMIT 10
""")
print(result.show())
''',
    
    "pandas_integration": '''# Pandas integration
df = session.sql("SELECT * FROM sales_data LIMIT 1000")

# Convert to pandas
pdf = df.to_pandas()

# Pandas operations
print(f"Shape: {pdf.shape}")
print(f"Columns: {list(pdf.columns)}")
print(pdf.describe())
''',
    
    "data_exploration": '''# Data exploration
table_name = "my_table"

# Get row count
count_df = session.sql(f"SELECT COUNT(*) as cnt FROM {table_name}")
row_count = count_df.collect()[0]["cnt"]
print(f"Total rows: {row_count}")

# Sample data
print("\\nSample data:")
sample = session.sql(f"SELECT * FROM {table_name} LIMIT 5")
print(sample.show())

# Column statistics
print("\\nColumn statistics:")
stats = session.sql(f"""
    SELECT 
        column_name,
        data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name.upper()}'
""")
print(stats.show())
'''
}
