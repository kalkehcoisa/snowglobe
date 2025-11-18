"""
SQL Translator - Converts Snowflake SQL to DuckDB compatible SQL
"""

import re
from typing import Tuple, Optional


class SnowflakeToDuckDBTranslator:
    """Translates Snowflake SQL syntax to DuckDB compatible SQL"""
    
    def __init__(self):
        self.function_mappings = {
            # Date/Time functions
            'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP',
            'CURRENT_DATE': 'CURRENT_DATE',
            'CURRENT_TIME': 'CURRENT_TIME',
            'GETDATE': 'CURRENT_TIMESTAMP',
            'SYSDATE': 'CURRENT_TIMESTAMP',
            
            # String functions
            'LEN': 'LENGTH',
            'CHARINDEX': 'STRPOS',
            'STRTOK': 'STRING_SPLIT',
            'INITCAP': 'INITCAP',
            
            # Null handling
            'NVL': 'COALESCE',
            'IFNULL': 'COALESCE',
            
            # Type casting
            'TO_VARCHAR': 'CAST',
            'TO_CHAR': 'CAST',
            'TO_NUMBER': 'CAST',
            'TO_DECIMAL': 'CAST',
            'TO_DOUBLE': 'CAST',
            'TO_BOOLEAN': 'CAST',
            
            # Math functions
            'SQUARE': 'POWER',
            'TRUNC': 'TRUNC',
            'TRUNCATE': 'TRUNC',
            
            # Array functions
            'ARRAY_SIZE': 'LEN',
            'ARRAY_TO_STRING': 'ARRAY_TO_STRING',
        }
        
        self.type_mappings = {
            'NUMBER': 'DOUBLE',
            'NUMERIC': 'DOUBLE',
            'DECIMAL': 'DECIMAL',
            'INT': 'INTEGER',
            'INTEGER': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'TINYINT': 'TINYINT',
            'FLOAT': 'FLOAT',
            'FLOAT4': 'FLOAT',
            'FLOAT8': 'DOUBLE',
            'DOUBLE': 'DOUBLE',
            'DOUBLE PRECISION': 'DOUBLE',
            'REAL': 'REAL',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'VARCHAR',
            'CHARACTER': 'VARCHAR',
            'STRING': 'VARCHAR',
            'TEXT': 'VARCHAR',
            'BINARY': 'BLOB',
            'VARBINARY': 'BLOB',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            'TIMESTAMP_NTZ': 'TIMESTAMP',
            'TIMESTAMP_LTZ': 'TIMESTAMP',
            'TIMESTAMP_TZ': 'TIMESTAMP',
            'VARIANT': 'JSON',
            'OBJECT': 'JSON',
            'ARRAY': 'JSON',
        }
    
    def translate(self, sql: str) -> str:
        """Main translation method"""
        translated = sql.strip()
        
        # Remove trailing semicolon for internal processing
        has_semicolon = translated.endswith(';')
        if has_semicolon:
            translated = translated[:-1].strip()
        
        # Apply translations
        translated = self._translate_data_types(translated)
        translated = self._translate_functions(translated)
        translated = self._translate_dateadd(translated)
        translated = self._translate_datediff(translated)
        translated = self._translate_to_date(translated)
        translated = self._translate_to_timestamp(translated)
        translated = self._translate_iff(translated)
        translated = self._translate_decode(translated)
        translated = self._translate_nvl2(translated)
        translated = self._translate_split_part(translated)
        translated = self._translate_regexp_like(translated)
        translated = self._translate_listagg(translated)
        translated = self._translate_qualify(translated)
        translated = self._translate_sample(translated)
        translated = self._translate_flatten(translated)
        translated = self._translate_lateral(translated)
        translated = self._translate_parse_json(translated)
        translated = self._translate_array_construct(translated)
        translated = self._translate_object_construct(translated)
        translated = self._translate_try_cast(translated)
        translated = self._translate_identifier(translated)
        translated = self._translate_collate(translated)
        translated = self._translate_ilike(translated)
        translated = self._translate_div0(translated)
        translated = self._translate_zeroifnull(translated)
        translated = self._translate_nullifzero(translated)
        translated = self._translate_equal_null(translated)
        translated = self._translate_within_group(translated)
        translated = self._translate_date_trunc(translated)
        translated = self._translate_time_slice(translated)
        translated = self._translate_last_day(translated)
        translated = self._translate_monthname(translated)
        translated = self._translate_dayname(translated)
        translated = self._translate_hash(translated)
        translated = self._translate_md5(translated)
        translated = self._translate_sha1(translated)
        translated = self._translate_base64(translated)
        translated = self._translate_typeof(translated)
        translated = self._translate_is_type(translated)
        translated = self._translate_array_agg(translated)
        translated = self._translate_object_keys(translated)
        translated = self._translate_get_path(translated)
        translated = self._translate_rlike(translated)
        translated = self._translate_square(translated)
        translated = self._translate_ratio_to_report(translated)
        translated = self._translate_conditional_expressions(translated)
        
        return translated
    
    def _translate_data_types(self, sql: str) -> str:
        """Translate Snowflake data types to DuckDB types"""
        result = sql
        
        # Handle NUMBER(p,s) and similar - need to process in specific order
        # to avoid partial matches. Use word boundaries and negative lookahead.
        ordered_types = [
            'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'TIMESTAMP',
            'DOUBLE PRECISION', 'DOUBLE', 'FLOAT8', 'FLOAT4', 'FLOAT',
            'NUMBER', 'NUMERIC', 'DECIMAL',
            'BIGINT', 'SMALLINT', 'TINYINT', 'INTEGER', 'INT',
            'VARCHAR', 'CHARACTER', 'CHAR', 'STRING', 'TEXT',
            'VARBINARY', 'BINARY',
            'BOOLEAN', 'DATE', 'DATETIME', 'TIME',
            'VARIANT', 'OBJECT', 'ARRAY', 'REAL'
        ]
        
        for sf_type in ordered_types:
            if sf_type not in self.type_mappings:
                continue
            duck_type = self.type_mappings[sf_type]
            
            # Match type with optional precision/scale
            # Use word boundary and ensure not part of a larger word
            if sf_type == 'INT':
                # Special case for INT - make sure it's not part of INTEGER
                pattern = rf'\bINT\b(?!EGER)(\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?'
            elif sf_type == 'CHAR':
                # Special case for CHAR - make sure it's not part of CHARACTER or VARCHAR
                pattern = rf'\bCHAR\b(?!ACTER)(\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?'
            else:
                pattern = rf'\b{sf_type}\b(\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?'
            
            def make_replace_func(snowflake_type, duckdb_type):
                def replace_type(match):
                    precision = match.group(1) if match.group(1) else ''
                    precision = precision.strip() if precision else ''
                    if snowflake_type in ['NUMBER', 'NUMERIC', 'DECIMAL'] and precision:
                        return f'DECIMAL{precision}'
                    elif snowflake_type in ['VARCHAR', 'CHAR', 'CHARACTER', 'STRING', 'TEXT']:
                        return f'VARCHAR{precision}' if precision else 'VARCHAR'
                    elif precision:
                        # Keep precision for other types if specified
                        return f'{duckdb_type}{precision}'
                    else:
                        return duckdb_type
                return replace_type
            
            result = re.sub(pattern, make_replace_func(sf_type, duck_type), result, flags=re.IGNORECASE)
        
        return result
    
    def _translate_functions(self, sql: str) -> str:
        """Translate simple function name mappings"""
        result = sql
        
        for sf_func, duck_func in self.function_mappings.items():
            pattern = rf'\b{sf_func}\b'
            result = re.sub(pattern, duck_func, result, flags=re.IGNORECASE)
        
        return result
    
    def _translate_dateadd(self, sql: str) -> str:
        """Translate DATEADD(part, amount, date) to DuckDB syntax"""
        pattern = r'\bDATEADD\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        
        def replace_dateadd(match):
            part = match.group(1).upper()
            amount = match.group(2).strip()
            date_expr = match.group(3).strip()
            
            # Map Snowflake date parts to DuckDB intervals
            part_map = {
                'YEAR': 'YEAR',
                'YEARS': 'YEAR',
                'YY': 'YEAR',
                'YYYY': 'YEAR',
                'MONTH': 'MONTH',
                'MONTHS': 'MONTH',
                'MM': 'MONTH',
                'MON': 'MONTH',
                'DAY': 'DAY',
                'DAYS': 'DAY',
                'DD': 'DAY',
                'D': 'DAY',
                'HOUR': 'HOUR',
                'HOURS': 'HOUR',
                'HH': 'HOUR',
                'MINUTE': 'MINUTE',
                'MINUTES': 'MINUTE',
                'MI': 'MINUTE',
                'SECOND': 'SECOND',
                'SECONDS': 'SECOND',
                'SS': 'SECOND',
                'WEEK': 'WEEK',
                'WEEKS': 'WEEK',
                'WK': 'WEEK',
                'QUARTER': 'QUARTER',
                'QUARTERS': 'QUARTER',
                'QTR': 'QUARTER',
            }
            
            duck_part = part_map.get(part, part)
            return f"({date_expr} + INTERVAL ({amount}) {duck_part})"
        
        return re.sub(pattern, replace_dateadd, sql, flags=re.IGNORECASE)
    
    def _translate_datediff(self, sql: str) -> str:
        """Translate DATEDIFF(part, date1, date2) to DuckDB syntax"""
        pattern = r'\bDATEDIFF\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        
        def replace_datediff(match):
            part = match.group(1).upper()
            date1 = match.group(2).strip()
            date2 = match.group(3).strip()
            
            part_map = {
                'YEAR': 'year',
                'YEARS': 'year',
                'YY': 'year',
                'YYYY': 'year',
                'MONTH': 'month',
                'MONTHS': 'month',
                'MM': 'month',
                'DAY': 'day',
                'DAYS': 'day',
                'DD': 'day',
                'HOUR': 'hour',
                'HOURS': 'hour',
                'MINUTE': 'minute',
                'MINUTES': 'minute',
                'SECOND': 'second',
                'SECONDS': 'second',
            }
            
            duck_part = part_map.get(part, 'day')
            return f"DATE_DIFF('{duck_part}', {date1}, {date2})"
        
        return re.sub(pattern, replace_datediff, sql, flags=re.IGNORECASE)
    
    def _translate_to_date(self, sql: str) -> str:
        """Translate TO_DATE function"""
        # TO_DATE(expr) -> CAST(expr AS DATE)
        pattern = r'\bTO_DATE\s*\(\s*([^,)]+)\s*\)'
        result = re.sub(pattern, r'CAST(\1 AS DATE)', sql, flags=re.IGNORECASE)
        
        # TO_DATE(expr, format) -> strptime(expr, format)::DATE
        pattern = r'\bTO_DATE\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        
        def replace_to_date_format(match):
            expr = match.group(1).strip()
            fmt = match.group(2).strip()
            # Convert Snowflake format to DuckDB format
            duck_fmt = self._convert_date_format(fmt)
            return f"strptime({expr}, {duck_fmt})::DATE"
        
        result = re.sub(pattern, replace_to_date_format, result, flags=re.IGNORECASE)
        return result
    
    def _translate_to_timestamp(self, sql: str) -> str:
        """Translate TO_TIMESTAMP function"""
        # TO_TIMESTAMP(expr) -> CAST(expr AS TIMESTAMP)
        pattern = r'\bTO_TIMESTAMP\s*\(\s*([^,)]+)\s*\)'
        result = re.sub(pattern, r'CAST(\1 AS TIMESTAMP)', sql, flags=re.IGNORECASE)
        
        # TO_TIMESTAMP(expr, format) -> strptime(expr, format)
        pattern = r'\bTO_TIMESTAMP\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        
        def replace_to_timestamp_format(match):
            expr = match.group(1).strip()
            fmt = match.group(2).strip()
            duck_fmt = self._convert_date_format(fmt)
            return f"strptime({expr}, {duck_fmt})"
        
        result = re.sub(pattern, replace_to_timestamp_format, result, flags=re.IGNORECASE)
        return result
    
    def _convert_date_format(self, fmt: str) -> str:
        """Convert Snowflake date format to DuckDB strptime format"""
        # This is a simplified conversion
        # Remove quotes if present
        fmt = fmt.strip()
        if fmt.startswith("'") and fmt.endswith("'"):
            fmt_inner = fmt[1:-1]
        else:
            fmt_inner = fmt
        
        # Common conversions
        conversions = [
            ('YYYY', '%Y'),
            ('YY', '%y'),
            ('MM', '%m'),
            ('DD', '%d'),
            ('HH24', '%H'),
            ('HH12', '%I'),
            ('HH', '%H'),
            ('MI', '%M'),
            ('SS', '%S'),
            ('FF', '%f'),
        ]
        
        for sf_fmt, duck_fmt in conversions:
            fmt_inner = fmt_inner.replace(sf_fmt, duck_fmt)
        
        return f"'{fmt_inner}'"
    
    def _translate_iff(self, sql: str) -> str:
        """Translate IFF(condition, true_val, false_val) to CASE WHEN"""
        pattern = r'\bIFF\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        
        def replace_iff(match):
            condition = match.group(1).strip()
            true_val = match.group(2).strip()
            false_val = match.group(3).strip()
            return f"CASE WHEN {condition} THEN {true_val} ELSE {false_val} END"
        
        return re.sub(pattern, replace_iff, sql, flags=re.IGNORECASE)
    
    def _translate_decode(self, sql: str) -> str:
        """Translate DECODE to CASE WHEN"""
        pattern = r'\bDECODE\s*\(([^)]+)\)'
        
        def replace_decode(match):
            args = self._parse_function_args(match.group(1))
            if len(args) < 3:
                return match.group(0)
            
            expr = args[0]
            cases = []
            i = 1
            default = 'NULL'
            
            while i < len(args):
                if i + 1 < len(args):
                    cases.append(f"WHEN {expr} = {args[i]} THEN {args[i+1]}")
                    i += 2
                else:
                    default = args[i]
                    i += 1
            
            case_str = ' '.join(cases)
            return f"CASE {case_str} ELSE {default} END"
        
        return re.sub(pattern, replace_decode, sql, flags=re.IGNORECASE)
    
    def _translate_nvl2(self, sql: str) -> str:
        """Translate NVL2(expr, not_null_val, null_val)"""
        pattern = r'\bNVL2\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        
        def replace_nvl2(match):
            expr = match.group(1).strip()
            not_null_val = match.group(2).strip()
            null_val = match.group(3).strip()
            return f"CASE WHEN {expr} IS NOT NULL THEN {not_null_val} ELSE {null_val} END"
        
        return re.sub(pattern, replace_nvl2, sql, flags=re.IGNORECASE)
    
    def _translate_split_part(self, sql: str) -> str:
        """Translate SPLIT_PART - DuckDB has this natively"""
        # SPLIT_PART is supported in DuckDB, but with same syntax
        return sql
    
    def _translate_regexp_like(self, sql: str) -> str:
        """Translate REGEXP_LIKE to DuckDB REGEXP_MATCHES"""
        pattern = r'\bREGEXP_LIKE\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        return re.sub(pattern, r'REGEXP_MATCHES(\1, \2)', sql, flags=re.IGNORECASE)
    
    def _translate_listagg(self, sql: str) -> str:
        """Translate LISTAGG to STRING_AGG"""
        pattern = r'\bLISTAGG\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        return re.sub(pattern, r'STRING_AGG(\1, \2)', sql, flags=re.IGNORECASE)
    
    def _translate_qualify(self, sql: str) -> str:
        """Translate QUALIFY clause (Snowflake-specific)"""
        # QUALIFY is supported in DuckDB 0.8+
        # For older versions, we'd need to wrap in a subquery
        return sql
    
    def _translate_sample(self, sql: str) -> str:
        """Translate SAMPLE/TABLESAMPLE"""
        # SAMPLE (n ROWS) -> USING SAMPLE n ROWS
        pattern = r'\bSAMPLE\s*\(\s*(\d+)\s+ROWS?\s*\)'
        result = re.sub(pattern, r'USING SAMPLE \1 ROWS', sql, flags=re.IGNORECASE)
        
        # SAMPLE (n) -> USING SAMPLE n PERCENT (approximate)
        pattern = r'\bSAMPLE\s*\(\s*(\d+(?:\.\d+)?)\s*\)'
        result = re.sub(pattern, r'USING SAMPLE \1 PERCENT', result, flags=re.IGNORECASE)
        
        return result
    
    def _translate_flatten(self, sql: str) -> str:
        """Translate FLATTEN for JSON arrays"""
        # FLATTEN(input => expr) -> UNNEST(expr)
        pattern = r'\bFLATTEN\s*\(\s*(?:INPUT\s*=>\s*)?([^)]+)\s*\)'
        return re.sub(pattern, r'UNNEST(\1)', sql, flags=re.IGNORECASE)
    
    def _translate_lateral(self, sql: str) -> str:
        """Handle LATERAL joins"""
        # LATERAL is supported in DuckDB
        return sql
    
    def _translate_parse_json(self, sql: str) -> str:
        """Translate PARSE_JSON to DuckDB JSON"""
        pattern = r'\bPARSE_JSON\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r'\1::JSON', sql, flags=re.IGNORECASE)
    
    def _translate_object_construct(self, sql: str) -> str:
        """Translate OBJECT_CONSTRUCT to JSON object"""
        pattern = r'\bOBJECT_CONSTRUCT\s*\(([^)]+)\)'
        
        def replace_object_construct(match):
            args = self._parse_function_args(match.group(1))
            if len(args) % 2 != 0:
                return match.group(0)
            
            pairs = []
            for i in range(0, len(args), 2):
                pairs.append(f"{args[i]}: {args[i+1]}")
            
            return "JSON_OBJECT(" + ", ".join(pairs) + ")"
        
        return re.sub(pattern, replace_object_construct, sql, flags=re.IGNORECASE)
    
    def _translate_array_construct(self, sql: str) -> str:
        """Translate ARRAY_CONSTRUCT to DuckDB array"""
        pattern = r'\bARRAY_CONSTRUCT\s*\(([^)]*)\)'
        return re.sub(pattern, r'LIST_VALUE(\1)', sql, flags=re.IGNORECASE)
    
    def _translate_try_cast(self, sql: str) -> str:
        """Translate TRY_CAST to TRY_CAST (supported in DuckDB)"""
        # DuckDB supports TRY_CAST natively
        return sql
    
    def _translate_identifier(self, sql: str) -> str:
        """Translate IDENTIFIER() function"""
        pattern = r'\bIDENTIFIER\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r'\1', sql, flags=re.IGNORECASE)
    
    def _translate_collate(self, sql: str) -> str:
        """Handle COLLATE clause"""
        # Remove Snowflake-specific collation
        pattern = r'\bCOLLATE\s+\'[^\']+\''
        return re.sub(pattern, '', sql, flags=re.IGNORECASE)
    
    def _translate_ilike(self, sql: str) -> str:
        """ILIKE is supported in DuckDB"""
        return sql
    
    def _translate_div0(self, sql: str) -> str:
        """Translate DIV0(a, b) - divide returning 0 on division by zero"""
        pattern = r'\bDIV0\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        
        def replace_div0(match):
            a = match.group(1).strip()
            b = match.group(2).strip()
            return f"CASE WHEN {b} = 0 THEN 0 ELSE {a} / {b} END"
        
        return re.sub(pattern, replace_div0, sql, flags=re.IGNORECASE)
    
    def _translate_zeroifnull(self, sql: str) -> str:
        """Translate ZEROIFNULL(expr)"""
        pattern = r'\bZEROIFNULL\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r'COALESCE(\1, 0)', sql, flags=re.IGNORECASE)
    
    def _translate_nullifzero(self, sql: str) -> str:
        """Translate NULLIFZERO(expr)"""
        pattern = r'\bNULLIFZERO\s*\(\s*([^)]+)\s*\)'
        
        def replace_nullifzero(match):
            expr = match.group(1).strip()
            return f"CASE WHEN {expr} = 0 THEN NULL ELSE {expr} END"
        
        return re.sub(pattern, replace_nullifzero, sql, flags=re.IGNORECASE)
    
    def _translate_equal_null(self, sql: str) -> str:
        """Translate EQUAL_NULL(a, b)"""
        pattern = r'\bEQUAL_NULL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        
        def replace_equal_null(match):
            a = match.group(1).strip()
            b = match.group(2).strip()
            return f"(({a} IS NULL AND {b} IS NULL) OR ({a} = {b}))"
        
        return re.sub(pattern, replace_equal_null, sql, flags=re.IGNORECASE)
    
    def _translate_within_group(self, sql: str) -> str:
        """Handle WITHIN GROUP clause for ordered aggregates"""
        # This is partially supported in DuckDB
        # For now, we'll keep it as is
        return sql
    
    def _translate_date_trunc(self, sql: str) -> str:
        """Translate DATE_TRUNC (same in DuckDB)"""
        # DATE_TRUNC is supported natively in DuckDB
        return sql
    
    def _translate_time_slice(self, sql: str) -> str:
        """Translate TIME_SLICE function"""
        pattern = r'\bTIME_SLICE\s*\(\s*([^,]+)\s*,\s*(\d+)\s*,\s*\'?(\w+)\'?\s*\)'
        
        def replace_time_slice(match):
            date_expr = match.group(1).strip()
            slice_length = match.group(2)
            date_part = match.group(3).upper()
            return f"DATE_TRUNC('{date_part}', {date_expr})"
        
        return re.sub(pattern, replace_time_slice, sql, flags=re.IGNORECASE)
    
    def _translate_last_day(self, sql: str) -> str:
        """Translate LAST_DAY function"""
        pattern = r'\bLAST_DAY\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r"(DATE_TRUNC('month', \1) + INTERVAL '1 month' - INTERVAL '1 day')::DATE", 
                      sql, flags=re.IGNORECASE)
    
    def _translate_monthname(self, sql: str) -> str:
        """Translate MONTHNAME function"""
        pattern = r'\bMONTHNAME\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r"STRFTIME(\1, '%B')", sql, flags=re.IGNORECASE)
    
    def _translate_dayname(self, sql: str) -> str:
        """Translate DAYNAME function"""
        pattern = r'\bDAYNAME\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r"STRFTIME(\1, '%A')", sql, flags=re.IGNORECASE)
    
    def _translate_hash(self, sql: str) -> str:
        """Translate HASH function"""
        pattern = r'\bHASH\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r"HASH(\1)", sql, flags=re.IGNORECASE)
    
    def _translate_md5(self, sql: str) -> str:
        """Translate MD5 function (same in DuckDB)"""
        # MD5 is supported natively
        return sql
    
    def _translate_sha1(self, sql: str) -> str:
        """Translate SHA1 function"""
        pattern = r'\bSHA1\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r"SHA256(\1)", sql, flags=re.IGNORECASE)
    
    def _translate_base64(self, sql: str) -> str:
        """Translate BASE64 functions"""
        # BASE64_ENCODE -> ENCODE
        pattern = r'\bBASE64_ENCODE\s*\(\s*([^)]+)\s*\)'
        result = re.sub(pattern, r"BASE64(\1)", sql, flags=re.IGNORECASE)
        
        # BASE64_DECODE -> DECODE
        pattern = r'\bBASE64_DECODE_STRING\s*\(\s*([^)]+)\s*\)'
        result = re.sub(pattern, r"FROM_BASE64(\1)", result, flags=re.IGNORECASE)
        
        return result
    
    def _translate_typeof(self, sql: str) -> str:
        """Translate TYPEOF function"""
        pattern = r'\bTYPEOF\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r"TYPEOF(\1)", sql, flags=re.IGNORECASE)
    
    def _translate_is_type(self, sql: str) -> str:
        """Translate IS_* type checking functions"""
        # IS_INTEGER, IS_DECIMAL, etc.
        for type_name in ['INTEGER', 'DECIMAL', 'DOUBLE', 'VARCHAR', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'ARRAY', 'OBJECT']:
            pattern = rf'\bIS_{type_name}\s*\(\s*([^)]+)\s*\)'
            result = re.sub(pattern, rf"TYPEOF(\1) = '{type_name.lower()}'", sql, flags=re.IGNORECASE)
            sql = result
        return sql
    
    def _translate_array_agg(self, sql: str) -> str:
        """Translate ARRAY_AGG (supported in DuckDB)"""
        return sql
    
    def _translate_object_keys(self, sql: str) -> str:
        """Translate OBJECT_KEYS function"""
        pattern = r'\bOBJECT_KEYS\s*\(\s*([^)]+)\s*\)'
        return re.sub(pattern, r"JSON_KEYS(\1)", sql, flags=re.IGNORECASE)
    
    def _translate_get_path(self, sql: str) -> str:
        """Translate GET_PATH function for JSON access"""
        pattern = r'\bGET_PATH\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        return re.sub(pattern, r"JSON_EXTRACT(\1, \2)", sql, flags=re.IGNORECASE)
    
    def _translate_rlike(self, sql: str) -> str:
        """Translate RLIKE to REGEXP_MATCHES"""
        pattern = r'\b(\w+)\s+RLIKE\s+([^\s,)]+)'
        return re.sub(pattern, r"REGEXP_MATCHES(\1, \2)", sql, flags=re.IGNORECASE)
    
    def _translate_square(self, sql: str) -> str:
        """Translate SQUARE function to POWER(x, 2)"""
        pattern = r'\bSQUARE\s*\(\s*([^)]+)\s*\)'
        
        def replace_square(match):
            expr = match.group(1).strip()
            return f"POWER({expr}, 2)"
        
        return re.sub(pattern, replace_square, sql, flags=re.IGNORECASE)
    
    def _translate_ratio_to_report(self, sql: str) -> str:
        """Translate RATIO_TO_REPORT window function"""
        pattern = r'\bRATIO_TO_REPORT\s*\(\s*([^)]+)\s*\)\s*OVER\s*\(\s*([^)]*)\s*\)'
        
        def replace_ratio(match):
            expr = match.group(1).strip()
            partition = match.group(2).strip()
            if partition:
                return f"({expr} / SUM({expr}) OVER ({partition}))"
            else:
                return f"({expr} / SUM({expr}) OVER ())"
        
        return re.sub(pattern, replace_ratio, sql, flags=re.IGNORECASE)
    
    def _translate_conditional_expressions(self, sql: str) -> str:
        """Translate Snowflake conditional expressions"""
        # BOOLOR_AGG -> BOOL_OR
        sql = re.sub(r'\bBOOLOR_AGG\s*\(', 'BOOL_OR(', sql, flags=re.IGNORECASE)
        
        # BOOLAND_AGG -> BOOL_AND
        sql = re.sub(r'\bBOOLAND_AGG\s*\(', 'BOOL_AND(', sql, flags=re.IGNORECASE)
        
        # BITOR_AGG -> BIT_OR
        sql = re.sub(r'\bBITOR_AGG\s*\(', 'BIT_OR(', sql, flags=re.IGNORECASE)
        
        # BITAND_AGG -> BIT_AND
        sql = re.sub(r'\bBITAND_AGG\s*\(', 'BIT_AND(', sql, flags=re.IGNORECASE)
        
        return sql
    
    def _parse_function_args(self, args_str: str) -> list:
        """Parse comma-separated function arguments, respecting parentheses"""
        args = []
        current = ''
        depth = 0
        in_string = False
        string_char = None
        
        for char in args_str:
            if char in ('"', "'") and not in_string:
                in_string = True
                string_char = char
                current += char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
                current += char
            elif char == '(' and not in_string:
                depth += 1
                current += char
            elif char == ')' and not in_string:
                depth -= 1
                current += char
            elif char == ',' and depth == 0 and not in_string:
                args.append(current.strip())
                current = ''
            else:
                current += char
        
        if current.strip():
            args.append(current.strip())
        
        return args


def translate_snowflake_to_duckdb(sql: str) -> str:
    """Convenience function to translate SQL"""
    translator = SnowflakeToDuckDBTranslator()
    return translator.translate(sql)
