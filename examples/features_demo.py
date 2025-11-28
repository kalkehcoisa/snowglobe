"""
Complete demonstration of Snowglobe features

This example showcases:
1. Hybrid Tables for transactional workloads
2. Dynamic Tables for continuous aggregations
3. File Operations (PUT, GET, COPY)
4. AWS Integrations (S3, Glue, etc.)
5. Data Quality checks with Soda
6. Schema Migrations with Flyway
7. SQL Functions
8. Automated Replication
"""

import requests
import snowflake.connector
import time
import json

# Configuration
SNOWGLOBE_HOST = "localhost"
SNOWGLOBE_PORT = 8084
SNOWGLOBE_API = f"http://{SNOWGLOBE_HOST}:{SNOWGLOBE_PORT}"


def print_section(title):
    """Print section header"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def main():
    print_section("🎯 Snowglobe Features Demo")
    
    # Connect to Snowglobe
    print("📡 Connecting to Snowglobe...")
    conn = snowflake.connector.connect(
        account='localhost',
        user='dev',
        password='dev',
        host=SNOWGLOBE_HOST,
        port=SNOWGLOBE_PORT,
        protocol='http',
        database='DEMO_DB',
        schema='PUBLIC'
    )
    cursor = conn.cursor()
    print("✅ Connected successfully!")
    
    # ========================================================================
    # 1. HYBRID TABLES - Transactional + Analytical Workloads
    # ========================================================================
    print_section("1️⃣  Hybrid Tables (Unistore)")
    
    print("Creating hybrid table for orders...")
    response = requests.post(f"{SNOWGLOBE_API}/api/tables/hybrid/create", json={
        "database": "DEMO_DB",
        "schema": "PUBLIC",
        "table_name": "orders",
        "columns": [
            {"name": "order_id", "type": "INTEGER", "not_null": True},
            {"name": "customer_id", "type": "INTEGER"},
            {"name": "product_name", "type": "VARCHAR"},
            {"name": "amount", "type": "DECIMAL(10,2)"},
            {"name": "status", "type": "VARCHAR"}
        ],
        "primary_key": ["order_id"],
        "indexes": [
            {"column": "customer_id", "name": "idx_customer"},
            {"column": "status", "name": "idx_status"}
        ]
    })
    
    if response.json().get("success"):
        print("✅ Hybrid table 'orders' created")
        print(f"   - Supports transactions: {response.json()['supports_transactions']}")
        print(f"   - Supports analytics: {response.json()['supports_analytics']}")
    
    # Fast single-row UPSERT operations
    print("\nPerforming fast UPSERT operations...")
    orders_data = [
        {"order_id": 1001, "customer_id": 5, "product_name": "Laptop", "amount": 1299.99, "status": "pending"},
        {"order_id": 1002, "customer_id": 7, "product_name": "Mouse", "amount": 29.99, "status": "completed"},
        {"order_id": 1003, "customer_id": 5, "product_name": "Keyboard", "amount": 89.99, "status": "pending"},
    ]
    
    for order in orders_data:
        response = requests.post(f"{SNOWGLOBE_API}/api/tables/hybrid/upsert", json={
            "database": "DEMO_DB",
            "schema": "PUBLIC",
            "table_name": "orders",
            "data": order,
            "primary_key_cols": ["order_id"]
        })
        if response.json().get("success"):
            print(f"   ✅ Upserted order {order['order_id']}")
    
    # Fast primary key lookup
    print("\nPerforming fast PK lookup...")
    response = requests.post(f"{SNOWGLOBE_API}/api/tables/hybrid/get-by-pk", json={
        "database": "DEMO_DB",
        "schema": "PUBLIC",
        "table_name": "orders",
        "pk_values": {"order_id": 1001}
    })
    
    if response.json().get("success") and response.json().get("found"):
        data = response.json()["data"]
        print(f"   ✅ Found order: {data['product_name']} - ${data['amount']}")
    
    # ========================================================================
    # 2. DYNAMIC TABLES - Continuous Data Loading
    # ========================================================================
    print_section("2️⃣  Dynamic Tables")
    
    print("Creating dynamic table for customer summary (auto-refresh every 5 minutes)...")
    response = requests.post(f"{SNOWGLOBE_API}/api/tables/dynamic/create", json={
        "database": "DEMO_DB",
        "schema": "PUBLIC",
        "table_name": "customer_summary",
        "target_lag": "5 minutes",
        "refresh_mode": "AUTO",
        "warehouse": "COMPUTE_WH",
        "query": """
            SELECT customer_id,
                   COUNT(*) as order_count,
                   SUM(amount) as total_spent,
                   AVG(amount) as avg_order_value,
                   MAX(amount) as largest_order
            FROM orders
            GROUP BY customer_id
        """
    })
    
    if response.json().get("success"):
        print("✅ Dynamic table 'customer_summary' created")
        print(f"   - Target lag: {response.json()['target_lag']}")
        print(f"   - Refresh mode: {response.json()['refresh_mode']}")
        print(f"   - Next refresh: {response.json()['next_refresh']}")
    
    # Manual refresh
    print("\nManually triggering refresh...")
    response = requests.post(f"{SNOWGLOBE_API}/api/tables/dynamic/refresh", json={
        "database": "DEMO_DB",
        "schema": "PUBLIC",
        "table_name": "customer_summary"
    })
    
    if response.json().get("success"):
        print(f"✅ Refresh completed in {response.json()['duration_seconds']:.3f}s")
    
    # Query the dynamic table
    print("\nQuerying dynamic table...")
    cursor.execute("SELECT * FROM customer_summary ORDER BY total_spent DESC")
    for row in cursor.fetchall():
        print(f"   Customer {row[0]}: {row[1]} orders, ${row[2]:.2f} total")
    
    # ========================================================================
    # 3. FILE OPERATIONS - PUT, GET, COPY
    # ========================================================================
    print_section("3️⃣  File Operations")
    
    # Create stage
    print("Creating internal stage...")
    cursor.execute("""
        CREATE STAGE IF NOT EXISTS data_stage
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',')
    """)
    print("✅ Stage 'data_stage' created")
    
    # Simulate COPY INTO from stage
    print("\nLoading data from stage...")
    response = requests.post(f"{SNOWGLOBE_API}/api/stages/copy-into-table", json={
        "database": "DEMO_DB",
        "schema": "PUBLIC",
        "table_name": "orders",
        "stage_location": "@data_stage/orders.csv",
        "file_format": {
            "type": "CSV",
            "field_delimiter": ",",
            "skip_header": 1
        }
    })
    
    print("✅ Data loading demonstrated")
    
    # COPY INTO location (export)
    print("\nExporting data to stage...")
    response = requests.post(f"{SNOWGLOBE_API}/api/stages/copy-into-location", json={
        "query_or_table": "SELECT * FROM customer_summary",
        "stage_location": "@data_stage/exports/",
        "file_format": {"type": "CSV"}
    })
    
    if response.json().get("success"):
        print(f"✅ Exported to stage")
    
    # ========================================================================
    # 4. AWS INTEGRATIONS
    # ========================================================================
    print_section("4️⃣  AWS Integrations")
    
    # S3 Storage Integration
    print("Creating S3 storage integration...")
    response = requests.post(f"{SNOWGLOBE_API}/api/aws/storage-integration", json={
        "name": "s3_integration",
        "integration_type": "EXTERNAL_STAGE",
        "storage_provider": "S3",
        "storage_allowed_locations": ["s3://my-bucket/data/"],
        "storage_aws_role_arn": "arn:aws:iam::123456789:role/snowflake-role"
    })
    
    if response.json().get("success"):
        print("✅ S3 storage integration created")
    
    # AWS Glue Catalog Integration
    print("\nCreating Glue catalog integration...")
    response = requests.post(f"{SNOWGLOBE_API}/api/aws/glue-integration", json={
        "name": "glue_catalog",
        "catalog_source": "GLUE",
        "catalog_namespace": "analytics_db",
        "enabled": True
    })
    
    if response.json().get("success"):
        print("✅ Glue catalog integration created")
    
    # Kinesis Streaming
    print("\nCreating Kinesis stream integration...")
    response = requests.post(f"{SNOWGLOBE_API}/api/aws/kinesis-integration", json={
        "stream_name": "orders-stream",
        "aws_role_arn": "arn:aws:iam::123456789:role/kinesis-role"
    })
    
    if response.json().get("success"):
        print("✅ Kinesis stream integration created")
    
    # SageMaker ML Model
    print("\nCreating SageMaker integration...")
    response = requests.post(f"{SNOWGLOBE_API}/api/aws/sagemaker-integration", json={
        "model_name": "fraud-detection",
        "model_endpoint": "fraud-detection-endpoint",
        "aws_role_arn": "arn:aws:iam::123456789:role/sagemaker-role"
    })
    
    if response.json().get("success"):
        print("✅ SageMaker integration created")
    
    # List all integrations
    print("\nListing all AWS integrations...")
    response = requests.get(f"{SNOWGLOBE_API}/api/aws/list-integrations")
    integrations = response.json()
    print(f"✅ Total integrations: {len(integrations)}")
    
    # ========================================================================
    # 5. DATA QUALITY - Soda Integration
    # ========================================================================
    print_section("5️⃣  Data Quality Checks (Soda)")
    
    # Register checks
    print("Registering data quality checks...")
    
    checks = [
        {
            "check_name": "orders_amount_positive",
            "table": "demo_db_public.orders",
            "check_type": "custom",
            "condition": "amount > 0"
        },
        {
            "check_name": "orders_status_valid",
            "table": "demo_db_public.orders",
            "check_type": "values_in_set",
            "column": "status",
            "allowed_values": ["pending", "processing", "completed", "cancelled"]
        },
        {
            "check_name": "orders_id_unique",
            "table": "demo_db_public.orders",
            "check_type": "unique",
            "column": "order_id"
        }
    ]
    
    for check in checks:
        response = requests.post(f"{SNOWGLOBE_API}/api/data-quality/register-check", json=check)
        if response.json().get("success"):
            print(f"   ✅ Registered: {check['check_name']}")
    
    # Run all checks
    print("\nRunning all data quality checks...")
    response = requests.post(f"{SNOWGLOBE_API}/api/data-quality/run-all-checks", json={
        "table": "demo_db_public.orders"
    })
    
    if response.json().get("success"):
        result = response.json()
        print(f"✅ Checks completed:")
        print(f"   - Total: {result['total_checks']}")
        print(f"   - Passed: {result['passed']}")
        print(f"   - Failed: {result['failed']}")
    
    # Get quality score
    response = requests.get(f"{SNOWGLOBE_API}/api/data-quality/quality-score?table=demo_db_public.orders")
    if response.status_code == 200:
        score = response.json()
        if score.get("score") is not None:
            print(f"\n📊 Quality Score: {score['score']}%")
    
    # ========================================================================
    # 6. SCHEMA MIGRATIONS - Flyway
    # ========================================================================
    print_section("6️⃣  Schema Migrations (Flyway)")
    
    # Add migration
    print("Adding database migration...")
    response = requests.post(f"{SNOWGLOBE_API}/api/migrations/add", json={
        "version": "1.0",
        "description": "create_products_table",
        "sql": """
            CREATE TABLE products (
                product_id INTEGER PRIMARY KEY,
                product_name VARCHAR,
                category VARCHAR,
                price DECIMAL(10,2)
            );
        """
    })
    
    if response.json().get("success"):
        print(f"✅ Migration 1.0 added")
    
    # Add another migration
    response = requests.post(f"{SNOWGLOBE_API}/api/migrations/add", json={
        "version": "1.1",
        "description": "add_stock_column",
        "sql": """
            ALTER TABLE products ADD COLUMN stock_quantity INTEGER DEFAULT 0;
        """
    })
    
    if response.json().get("success"):
        print(f"✅ Migration 1.1 added")
    
    # Run migrations
    print("\nApplying migrations...")
    response = requests.post(f"{SNOWGLOBE_API}/api/migrations/migrate")
    
    if response.json().get("success"):
        result = response.json()
        print(f"✅ Applied {len(result['applied'])} migration(s)")
        for version in result["applied"]:
            print(f"   - Version {version}")
    
    # Check migration info
    print("\nMigration status:")
    response = requests.get(f"{SNOWGLOBE_API}/api/migrations/info")
    if response.status_code == 200:
        info = response.json()
        print(f"   Current version: {info['current_version']}")
        print(f"   Applied migrations: {info['applied_migrations']}")
        print(f"   Pending migrations: {info['pending_migrations']}")
    
    # ========================================================================
    # 7. SQL FUNCTIONS
    # ========================================================================
    print_section("7️⃣  SQL Functions")
    
    print("Demonstrating Snowflake SQL functions...")
    
    # String functions
    cursor.execute("SELECT CONCAT('Hello', ' ', 'Snowglobe') as greeting")
    print(f"   String: {cursor.fetchone()[0]}")
    
    # Date functions
    cursor.execute("SELECT CURRENT_DATE() as today")
    print(f"   Date: {cursor.fetchone()[0]}")
    
    # Numeric functions
    cursor.execute("SELECT ROUND(1234.5678, 2) as rounded")
    print(f"   Numeric: {cursor.fetchone()[0]}")
    
    # Conditional functions
    cursor.execute("SELECT IFF(10 > 5, 'yes', 'no') as result")
    print(f"   Conditional: {cursor.fetchone()[0]}")
    
    # Window functions
    cursor.execute("""
        SELECT order_id, amount,
               ROW_NUMBER() OVER (ORDER BY amount DESC) as rank
        FROM orders
        LIMIT 3
    """)
    print("\n   Window function (top orders by amount):")
    for row in cursor.fetchall():
        print(f"      Order {row[0]}: ${row[1]:.2f} (rank {row[2]})")
    
    # Create UDF
    print("\nCreating user-defined function...")
    response = requests.post(f"{SNOWGLOBE_API}/api/functions/create", json={
        "name": "calculate_tax",
        "args": ["amount DECIMAL(10,2)"],
        "return_type": "DECIMAL(10,2)",
        "language": "SQL",
        "body": "amount * 0.08"
    })
    
    if response.json().get("success"):
        print("✅ UDF 'calculate_tax' created")
    
    # ========================================================================
    # 8. AUTOMATED REPLICATION
    # ========================================================================
    print_section("8️⃣  Automated Replication")
    
    # Create replication job
    print("Creating replication job...")
    response = requests.post(f"{SNOWGLOBE_API}/api/replication/create-job", json={
        "job_name": "prod_to_local",
        "source_connection": {
            "account": "production-account",
            "user": "replication_user",
            "password": "secure_password",
            "warehouse": "COMPUTE_WH"
        },
        "objects_to_replicate": [
            "ANALYTICS.*",
            "SALES.PUBLIC.*"
        ],
        "include_data": False,  # Metadata only
        "schedule": "0 2 * * *"  # Daily at 2 AM
    })
    
    if response.json().get("success"):
        job_id = response.json()["job_id"]
        print(f"✅ Replication job created: {job_id}")
    
    # Create snapshot
    print("\nCreating snapshot of current state...")
    response = requests.post(f"{SNOWGLOBE_API}/api/replication/create-snapshot", json={
        "snapshot_name": "demo_snapshot",
        "objects": ["DEMO_DB.*"]
    })
    
    if response.json().get("success"):
        print("✅ Snapshot created: demo_snapshot")
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    print_section("📊 Feature Summary")
    
    features = {
        "Hybrid Tables": "✅ Transactional + Analytical workloads",
        "Dynamic Tables": "✅ Continuous aggregations with auto-refresh",
        "File Operations": "✅ PUT, GET, COPY INTO support",
        "AWS S3 Integration": "✅ External stage storage",
        "AWS Glue Integration": "✅ Catalog metadata sync",
        "AWS Kinesis Integration": "✅ Streaming data ingestion",
        "AWS SageMaker Integration": "✅ ML model inference",
        "Data Quality (Soda)": "✅ Automated quality checks",
        "Schema Migrations (Flyway)": "✅ Version-controlled migrations",
        "SQL Functions (200+)": "✅ Comprehensive function support",
        "User-Defined Functions": "✅ Custom UDFs",
        "Automated Replication": "✅ Sync from production Snowflake"
    }
    
    for feature, status in features.items():
        print(f"  {status:40} - {feature}")
    
    print("\n" + "="*60)
    print("  🎉 All Snowglobe features demonstrated!")
    print("="*60 + "\n")
    
    # Cleanup
    cursor.close()
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
