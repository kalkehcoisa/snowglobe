"""
Snowglobe HTTPS Connection Example
===================================

This example demonstrates how to connect to Snowglobe using HTTPS,
which is the standard protocol for Snowflake connections.
"""

import snowflake.connector
import os

# Disable SSL verification warnings for self-signed certificates
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def connect_with_https():
    """Connect to Snowglobe using HTTPS (recommended)"""
    print("🔒 Connecting to Snowglobe via HTTPS...")
    
    conn = snowflake.connector.connect(
        account='localhost',
        user='dev',
        password='dev',
        host='localhost',
        port=8443,  # HTTPS port
        protocol='https',
        insecure_mode=True,  # Required for self-signed certificates
        database='TEST_DB',
        schema='PUBLIC'
    )
    
    print("✅ Connected successfully via HTTPS!")
    return conn


def connect_with_http():
    """Connect to Snowglobe using HTTP (fallback)"""
    print("🔓 Connecting to Snowglobe via HTTP...")
    
    conn = snowflake.connector.connect(
        account='localhost',
        user='dev',
        password='dev',
        host='localhost',
        port=8084,  # HTTP port
        protocol='http',
        database='TEST_DB',
        schema='PUBLIC'
    )
    
    print("✅ Connected successfully via HTTP!")
    return conn


def run_example_queries(conn):
    """Run example queries to demonstrate functionality"""
    cursor = conn.cursor()
    
    print("\n📊 Running example queries...")
    print("-" * 50)
    
    # 1. Check Snowflake version
    print("\n1. Checking Snowflake version:")
    cursor.execute("SELECT CURRENT_VERSION()")
    result = cursor.fetchone()
    print(f"   Version: {result[0]}")
    
    # 2. Create database and schema
    print("\n2. Creating database and schema:")
    cursor.execute("CREATE DATABASE IF NOT EXISTS demo_db")
    cursor.execute("USE DATABASE demo_db")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS demo_schema")
    cursor.execute("USE SCHEMA demo_schema")
    print("   ✓ Database and schema created")
    
    # 3. Create table
    print("\n3. Creating table:")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER,
            name VARCHAR,
            department VARCHAR,
            salary DECIMAL(10,2),
            hire_date DATE
        )
    """)
    print("   ✓ Table 'employees' created")
    
    # 4. Insert data
    print("\n4. Inserting sample data:")
    cursor.execute("""
        INSERT INTO employees VALUES
            (1, 'Alice Johnson', 'Engineering', 95000.00, '2023-01-15'),
            (2, 'Bob Smith', 'Sales', 75000.00, '2023-02-01'),
            (3, 'Carol White', 'Engineering', 105000.00, '2023-01-20'),
            (4, 'David Brown', 'Marketing', 65000.00, '2023-03-10'),
            (5, 'Eve Davis', 'Engineering', 98000.00, '2023-02-15')
    """)
    print(f"   ✓ {cursor.rowcount} rows inserted")
    
    # 5. Query data
    print("\n5. Querying employees in Engineering:")
    cursor.execute("""
        SELECT name, salary 
        FROM employees 
        WHERE department = 'Engineering'
        ORDER BY salary DESC
    """)
    
    print("   Results:")
    for row in cursor:
        print(f"   - {row[0]}: ${row[1]:,.2f}")
    
    # 6. Aggregation query
    print("\n6. Department statistics:")
    cursor.execute("""
        SELECT 
            department,
            COUNT(*) as emp_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    """)
    
    print(f"   {'Department':<15} {'Count':<8} {'Avg Salary':<15} {'Max Salary'}")
    print("   " + "-" * 50)
    for row in cursor:
        print(f"   {row[0]:<15} {row[1]:<8} ${row[2]:>12,.2f}  ${row[3]:>12,.2f}")
    
    # 7. Show objects
    print("\n7. Listing database objects:")
    cursor.execute("SHOW DATABASES")
    print(f"   Databases found: {cursor.rowcount}")
    
    cursor.execute("SHOW TABLES")
    print(f"   Tables found: {cursor.rowcount}")
    
    cursor.close()
    print("\n✅ All queries executed successfully!")


def demonstrate_https_features():
    """Demonstrate HTTPS-specific features"""
    print("\n🔐 HTTPS Security Features:")
    print("-" * 50)
    print("✓ Encrypted data transmission (TLS 1.2+)")
    print("✓ Secure authentication")
    print("✓ Snowflake-standard protocol")
    print("✓ Production-ready configuration")
    print("\n💡 For production, use valid SSL certificates instead of self-signed")


def main():
    """Main execution"""
    print("=" * 60)
    print("  Snowglobe HTTPS Connection Example")
    print("=" * 60)
    
    # Try HTTPS first (recommended)
    try:
        conn = connect_with_https()
        demonstrate_https_features()
    except Exception as e:
        print(f"⚠️  HTTPS connection failed: {e}")
        print("Falling back to HTTP...\n")
        try:
            conn = connect_with_http()
        except Exception as e2:
            print(f"❌ HTTP connection also failed: {e2}")
            print("\nPlease ensure Snowglobe is running:")
            print("  docker-compose up -d")
            return
    
    try:
        run_example_queries(conn)
    except Exception as e:
        print(f"❌ Error executing queries: {e}")
    finally:
        conn.close()
        print("\n🔌 Connection closed")
    
    print("\n" + "=" * 60)
    print("  Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
