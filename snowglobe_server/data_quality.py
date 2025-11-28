"""
Data Quality Integration - Soda integration for data quality checks
Enables data validation and quality monitoring
"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime
import duckdb


class SodaDataQualityManager:
    """Manages Soda data quality checks and validation"""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.checks = {}
        self.check_results = []
    
    def register_check(self, check_name: str, table: str, check_type: str,
                      column: Optional[str] = None, condition: Optional[str] = None,
                      threshold: Optional[float] = None) -> Dict[str, Any]:
        """
        Register a data quality check
        
        Args:
            check_name: Name of the check
            table: Table to check
            check_type: Type of check (not_null, unique, values_in_range, freshness, etc.)
            column: Column to check (if applicable)
            condition: Custom condition SQL
            threshold: Threshold for warnings/failures
        """
        check_id = f"{check_name}_{datetime.now().timestamp()}"
        
        self.checks[check_id] = {
            "check_id": check_id,
            "check_name": check_name,
            "table": table,
            "check_type": check_type,
            "column": column,
            "condition": condition,
            "threshold": threshold,
            "enabled": True,
            "created_at": datetime.now().isoformat()
        }
        
        return {
            "success": True,
            "message": f"Data quality check '{check_name}' registered",
            "check_id": check_id
        }
    
    def run_check(self, check_id: str) -> Dict[str, Any]:
        """Run a specific data quality check"""
        if check_id not in self.checks:
            return {"success": False, "error": f"Check {check_id} not found"}
        
        check = self.checks[check_id]
        start_time = datetime.now()
        
        try:
            result = self._execute_check(check)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            check_result = {
                "check_id": check_id,
                "check_name": check["check_name"],
                "table": check["table"],
                "status": result["status"],
                "passed": result["passed"],
                "value": result.get("value"),
                "message": result.get("message"),
                "timestamp": end_time.isoformat(),
                "duration_seconds": duration
            }
            
            self.check_results.append(check_result)
            
            return {
                "success": True,
                "check_result": check_result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to run check: {str(e)}"
            }
    
    def run_all_checks(self, table: Optional[str] = None) -> Dict[str, Any]:
        """Run all registered checks (optionally filtered by table)"""
        results = []
        passed = 0
        failed = 0
        
        for check_id, check in self.checks.items():
            if not check["enabled"]:
                continue
            
            if table and check["table"] != table:
                continue
            
            result = self.run_check(check_id)
            if result["success"]:
                results.append(result["check_result"])
                if result["check_result"]["passed"]:
                    passed += 1
                else:
                    failed += 1
        
        return {
            "success": True,
            "total_checks": len(results),
            "passed": passed,
            "failed": failed,
            "results": results
        }
    
    def _execute_check(self, check: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a specific check type"""
        check_type = check["check_type"]
        table = check["table"]
        column = check["column"]
        
        if check_type == "not_null":
            return self._check_not_null(table, column)
        
        elif check_type == "unique":
            return self._check_unique(table, column)
        
        elif check_type == "values_in_range":
            return self._check_values_in_range(table, column, check.get("min_value"), check.get("max_value"))
        
        elif check_type == "values_in_set":
            return self._check_values_in_set(table, column, check.get("allowed_values", []))
        
        elif check_type == "row_count":
            return self._check_row_count(table, check.get("min_rows"), check.get("max_rows"))
        
        elif check_type == "freshness":
            return self._check_freshness(table, column, check.get("max_age_hours"))
        
        elif check_type == "custom":
            return self._check_custom(table, check["condition"])
        
        else:
            return {
                "status": "ERROR",
                "passed": False,
                "message": f"Unknown check type: {check_type}"
            }
    
    def _check_not_null(self, table: str, column: str) -> Dict[str, Any]:
        """Check for NULL values in column"""
        query = f"SELECT COUNT(*) as null_count FROM {table} WHERE {column} IS NULL"
        result = self.conn.execute(query).fetchone()
        null_count = result[0]
        
        return {
            "status": "PASS" if null_count == 0 else "FAIL",
            "passed": null_count == 0,
            "value": null_count,
            "message": f"Found {null_count} NULL values in {column}"
        }
    
    def _check_unique(self, table: str, column: str) -> Dict[str, Any]:
        """Check for duplicate values in column"""
        query = f"""
        SELECT COUNT(*) - COUNT(DISTINCT {column}) as duplicate_count
        FROM {table}
        """
        result = self.conn.execute(query).fetchone()
        duplicate_count = result[0]
        
        return {
            "status": "PASS" if duplicate_count == 0 else "FAIL",
            "passed": duplicate_count == 0,
            "value": duplicate_count,
            "message": f"Found {duplicate_count} duplicate values in {column}"
        }
    
    def _check_values_in_range(self, table: str, column: str,
                              min_value: Optional[float], max_value: Optional[float]) -> Dict[str, Any]:
        """Check if values are within specified range"""
        conditions = []
        if min_value is not None:
            conditions.append(f"{column} < {min_value}")
        if max_value is not None:
            conditions.append(f"{column} > {max_value}")
        
        where_clause = " OR ".join(conditions)
        query = f"SELECT COUNT(*) as out_of_range FROM {table} WHERE {where_clause}"
        result = self.conn.execute(query).fetchone()
        out_of_range = result[0]
        
        return {
            "status": "PASS" if out_of_range == 0 else "FAIL",
            "passed": out_of_range == 0,
            "value": out_of_range,
            "message": f"Found {out_of_range} values out of range [{min_value}, {max_value}]"
        }
    
    def _check_values_in_set(self, table: str, column: str, allowed_values: List[Any]) -> Dict[str, Any]:
        """Check if values are in allowed set"""
        allowed_str = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in allowed_values])
        query = f"SELECT COUNT(*) as invalid_count FROM {table} WHERE {column} NOT IN ({allowed_str})"
        result = self.conn.execute(query).fetchone()
        invalid_count = result[0]
        
        return {
            "status": "PASS" if invalid_count == 0 else "FAIL",
            "passed": invalid_count == 0,
            "value": invalid_count,
            "message": f"Found {invalid_count} values not in allowed set"
        }
    
    def _check_row_count(self, table: str, min_rows: Optional[int], max_rows: Optional[int]) -> Dict[str, Any]:
        """Check if row count is within expected range"""
        query = f"SELECT COUNT(*) as row_count FROM {table}"
        result = self.conn.execute(query).fetchone()
        row_count = result[0]
        
        passed = True
        message = f"Table has {row_count} rows"
        
        if min_rows is not None and row_count < min_rows:
            passed = False
            message += f" (minimum: {min_rows})"
        
        if max_rows is not None and row_count > max_rows:
            passed = False
            message += f" (maximum: {max_rows})"
        
        return {
            "status": "PASS" if passed else "FAIL",
            "passed": passed,
            "value": row_count,
            "message": message
        }
    
    def _check_freshness(self, table: str, column: str, max_age_hours: float) -> Dict[str, Any]:
        """Check if data is fresh (recent timestamp)"""
        query = f"""
        SELECT MAX({column}) as latest_timestamp,
               EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({column}))) / 3600 as age_hours
        FROM {table}
        """
        result = self.conn.execute(query).fetchone()
        age_hours = result[1] if result[1] is not None else float('inf')
        
        passed = age_hours <= max_age_hours
        
        return {
            "status": "PASS" if passed else "FAIL",
            "passed": passed,
            "value": age_hours,
            "message": f"Data is {age_hours:.2f} hours old (max: {max_age_hours})"
        }
    
    def _check_custom(self, table: str, condition: str) -> Dict[str, Any]:
        """Execute custom SQL condition check"""
        query = f"SELECT COUNT(*) as violation_count FROM {table} WHERE NOT ({condition})"
        result = self.conn.execute(query).fetchone()
        violation_count = result[0]
        
        return {
            "status": "PASS" if violation_count == 0 else "FAIL",
            "passed": violation_count == 0,
            "value": violation_count,
            "message": f"Found {violation_count} violations of custom condition"
        }
    
    def get_check_history(self, check_id: Optional[str] = None, 
                         table: Optional[str] = None,
                         limit: int = 100) -> List[Dict[str, Any]]:
        """Get check execution history"""
        results = self.check_results[-limit:]
        
        if check_id:
            results = [r for r in results if r["check_id"] == check_id]
        
        if table:
            results = [r for r in results if r["table"] == table]
        
        return results
    
    def get_quality_score(self, table: Optional[str] = None) -> Dict[str, Any]:
        """Calculate overall data quality score"""
        recent_results = self.check_results[-100:]
        
        if table:
            recent_results = [r for r in recent_results if r["table"] == table]
        
        if not recent_results:
            return {
                "score": None,
                "total_checks": 0,
                "passed": 0,
                "failed": 0
            }
        
        passed = sum(1 for r in recent_results if r["passed"])
        failed = len(recent_results) - passed
        score = (passed / len(recent_results)) * 100 if recent_results else 0
        
        return {
            "score": round(score, 2),
            "total_checks": len(recent_results),
            "passed": passed,
            "failed": failed
        }
    
    def list_checks(self, table: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all registered checks"""
        checks = list(self.checks.values())
        
        if table:
            checks = [c for c in checks if c["table"] == table]
        
        return checks
    
    def delete_check(self, check_id: str) -> Dict[str, Any]:
        """Delete a check"""
        if check_id in self.checks:
            del self.checks[check_id]
            return {"success": True, "message": f"Check {check_id} deleted"}
        return {"success": False, "error": f"Check {check_id} not found"}
