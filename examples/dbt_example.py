#!/usr/bin/env python3
"""
Example: Using dbt with Snowglobe

This example demonstrates how to use Snowglobe's dbt integration
to develop and test dbt models locally.
"""

import requests
import json
import time
import os
import tempfile

# Snowglobe server configuration
BASE_URL = os.getenv("SNOWGLOBE_URL", "http://localhost:8084")


def print_response(title: str, response: dict):
    """Pretty print API response"""
    print(f"\n{'='*60}")
    print(f"📊 {title}")
    print('='*60)
    print(json.dumps(response, indent=2, default=str))


def check_dbt_status():
    """Check dbt adapter status"""
    response = requests.get(f"{BASE_URL}/api/dbt/status")
    return response.json()


def get_profiles_yml():
    """Get profiles.yml configuration"""
    response = requests.get(f"{BASE_URL}/api/dbt/profiles")
    return response.json()


def register_source(name: str, database: str, schema: str, tables: list):
    """Register a dbt source"""
    response = requests.post(f"{BASE_URL}/api/dbt/sources", json={
        "name": name,
        "database": database,
        "schema_name": schema,
        "tables": tables,
        "description": f"Source: {name}"
    })
    return response.json()


def register_model(name: str, sql: str, materialization: str = "view", **kwargs):
    """Register a dbt model"""
    payload = {
        "name": name,
        "sql": sql,
        "materialization": materialization,
        **kwargs
    }
    response = requests.post(f"{BASE_URL}/api/dbt/models", json=payload)
    return response.json()


def run_model(model_name: str, full_refresh: bool = False):
    """Run a specific model"""
    response = requests.post(
        f"{BASE_URL}/api/dbt/models/{model_name}/run",
        params={"full_refresh": full_refresh}
    )
    return response.json()


def run_all_models(select: str = None, exclude: str = None, full_refresh: bool = False):
    """Run all models"""
    response = requests.post(f"{BASE_URL}/api/dbt/run", json={
        "select": select,
        "exclude": exclude,
        "full_refresh": full_refresh
    })
    return response.json()


def compile_sql(sql: str, vars: dict = None):
    """Compile dbt-style SQL"""
    response = requests.post(f"{BASE_URL}/api/dbt/compile", json={
        "sql": sql,
        "vars": vars or {}
    })
    return response.json()


def get_lineage(model_name: str):
    """Get model lineage"""
    response = requests.get(f"{BASE_URL}/api/dbt/models/{model_name}/lineage")
    return response.json()


def list_models():
    """List all registered models"""
    response = requests.get(f"{BASE_URL}/api/dbt/models")
    return response.json()


def list_sources():
    """List all registered sources"""
    response = requests.get(f"{BASE_URL}/api/dbt/sources")
    return response.json()


def execute_sql(sql: str):
    """Execute raw SQL"""
    response = requests.post(f"{BASE_URL}/api/execute", json={"sql": sql})
    return response.json()


def generate_sample_project(project_dir: str):
    """Generate a sample dbt project"""
    response = requests.post(f"{BASE_URL}/api/dbt/project/generate", json={
        "project_dir": project_dir
    })
    return response.json()


def set_vars(vars: dict):
    """Set dbt variables"""
    response = requests.post(f"{BASE_URL}/api/dbt/vars", json={"vars": vars})
    return response.json()


def run_tests(select: str = None):
    """Run dbt tests"""
    response = requests.post(f"{BASE_URL}/api/dbt/test", json={"select": select})
    return response.json()


def get_docs():
    """Generate documentation"""
    response = requests.get(f"{BASE_URL}/api/dbt/docs")
    return response.json()


def main():
    """Main example workflow"""
    print("\n" + "🎯 "*20)
    print("  Snowglobe dbt Integration Example  ")
    print("🎯 "*20 + "\n")
    
    # Step 1: Check dbt status
    print("\n📌 Step 1: Checking dbt adapter status...")
    status = check_dbt_status()
    print_response("dbt Status", status)
    
    if not status.get("enabled"):
        print("❌ dbt support is not enabled!")
        return
    
    # Step 2: Get profiles.yml
    print("\n📌 Step 2: Getting profiles.yml configuration...")
    profiles = get_profiles_yml()
    print("\n📄 profiles.yml content:")
    print(profiles.get("profiles_yml", ""))
    
    # Step 3: Create source tables in Snowglobe
    print("\n📌 Step 3: Creating source tables...")
    
    # Create RAW schema
    execute_sql("CREATE SCHEMA IF NOT EXISTS RAW")
    
    # Create customers table
    execute_sql("""
        CREATE OR REPLACE TABLE RAW.CUSTOMERS (
            id INTEGER,
            name VARCHAR,
            email VARCHAR,
            country_code VARCHAR(2),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)
    
    # Insert sample data
    execute_sql("""
        INSERT INTO RAW.CUSTOMERS VALUES
            (1, 'Alice Johnson', 'alice@example.com', 'US', '2024-01-01 10:00:00', '2024-01-15 10:00:00'),
            (2, 'Bob Smith', 'bob@example.com', 'CA', '2024-01-02 11:00:00', '2024-01-16 11:00:00'),
            (3, 'Carol White', 'carol@example.com', 'UK', '2024-01-03 12:00:00', '2024-01-17 12:00:00')
    """)
    
    # Create orders table
    execute_sql("""
        CREATE OR REPLACE TABLE RAW.ORDERS (
            id INTEGER,
            customer_id INTEGER,
            amount DECIMAL(10,2),
            status VARCHAR,
            created_at TIMESTAMP
        )
    """)
    
    execute_sql("""
        INSERT INTO RAW.ORDERS VALUES
            (101, 1, 150.00, 'completed', '2024-01-10 09:00:00'),
            (102, 1, 200.00, 'completed', '2024-01-11 10:00:00'),
            (103, 2, 75.50, 'pending', '2024-01-12 11:00:00'),
            (104, 3, 300.00, 'completed', '2024-01-13 12:00:00')
    """)
    
    print("✅ Source tables created!")
    
    # Step 4: Register dbt sources
    print("\n📌 Step 4: Registering dbt sources...")
    
    source_result = register_source(
        name="raw",
        database="SNOWGLOBE",
        schema="RAW",
        tables=[
            {"name": "customers", "identifier": "customers"},
            {"name": "orders", "identifier": "orders"}
        ]
    )
    print_response("Registered Source", source_result)
    
    # Step 5: Set variables
    print("\n📌 Step 5: Setting dbt variables...")
    vars_result = set_vars({
        "start_date": "2024-01-01",
        "environment": "dev"
    })
    print_response("Variables Set", vars_result)
    
    # Step 6: Register dbt models
    print("\n📌 Step 6: Registering dbt models...")
    
    # Staging model for customers
    stg_customers = register_model(
        name="stg_customers",
        sql="""
{{ config(materialized='view') }}

SELECT
    id AS customer_id,
    UPPER(name) AS customer_name,
    LOWER(email) AS email,
    country_code,
    created_at,
    updated_at
FROM {{ source('raw', 'customers') }}
WHERE created_at >= '{{ var("start_date") }}'
        """,
        materialization="view",
        description="Staged customer data",
        tags=["staging", "pii"]
    )
    print_response("Registered stg_customers", stg_customers)
    
    # Staging model for orders
    stg_orders = register_model(
        name="stg_orders",
        sql="""
{{ config(materialized='view') }}

SELECT
    id AS order_id,
    customer_id,
    amount,
    status,
    created_at AS order_date
FROM {{ source('raw', 'orders') }}
        """,
        materialization="view",
        description="Staged order data",
        tags=["staging"]
    )
    print_response("Registered stg_orders", stg_orders)
    
    # Mart model - customer dimension
    dim_customers = register_model(
        name="dim_customers",
        sql="""
{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_name,
    email,
    country_code,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ ref('stg_customers') }}
        """,
        materialization="table",
        description="Customer dimension table",
        tags=["marts", "core"]
    )
    print_response("Registered dim_customers", dim_customers)
    
    # Mart model - customer orders summary
    fct_customer_orders = register_model(
        name="fct_customer_orders",
        sql="""
{{ config(materialized='table') }}

SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.amount) AS total_amount,
    AVG(o.amount) AS avg_order_amount,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM {{ ref('dim_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
        """,
        materialization="table",
        description="Customer orders fact table",
        tags=["marts", "core"]
    )
    print_response("Registered fct_customer_orders", fct_customer_orders)
    
    # Step 7: List all models
    print("\n📌 Step 7: Listing all registered models...")
    models = list_models()
    print_response("Registered Models", models)
    
    # Step 8: Compile SQL example
    print("\n📌 Step 8: Testing SQL compilation...")
    
    sample_sql = """
SELECT *
FROM {{ ref('dim_customers') }} c
JOIN {{ source('raw', 'orders') }} o ON c.customer_id = o.customer_id
WHERE c.created_at >= '{{ var("start_date") }}'
    """
    
    compiled = compile_sql(sample_sql)
    print_response("Compiled SQL", compiled)
    
    # Step 9: Run all models
    print("\n📌 Step 9: Running all models...")
    run_result = run_all_models()
    print_response("Run Results", run_result)
    
    # Step 10: Get model lineage
    print("\n📌 Step 10: Getting model lineage...")
    
    for model_name in ["stg_customers", "dim_customers", "fct_customer_orders"]:
        lineage = get_lineage(model_name)
        print_response(f"Lineage for {model_name}", lineage)
    
    # Step 11: Query the results
    print("\n📌 Step 11: Querying the built models...")
    
    result = execute_sql("SELECT * FROM SNOWGLOBE.PUBLIC.DIM_CUSTOMERS")
    print_response("dim_customers data", result)
    
    result = execute_sql("SELECT * FROM SNOWGLOBE.PUBLIC.FCT_CUSTOMER_ORDERS ORDER BY total_amount DESC")
    print_response("fct_customer_orders data", result)
    
    # Step 12: Generate documentation
    print("\n📌 Step 12: Generating documentation...")
    docs = get_docs()
    print_response("Documentation (summary)", {
        "nodes_count": len(docs.get("nodes", {})),
        "sources_count": len(docs.get("sources", {})),
        "generated_at": docs.get("metadata", {}).get("generated_at"),
    })
    
    # Step 13: Generate sample project
    print("\n📌 Step 13: Generating sample dbt project...")
    with tempfile.TemporaryDirectory() as temp_dir:
        project_dir = f"{temp_dir}/my_dbt_project"
        project_result = generate_sample_project(project_dir)
        print_response("Sample Project Generated", project_result)
    
    print("\n" + "✅ "*20)
    print("  Example completed successfully!  ")
    print("✅ "*20 + "\n")
    
    print("""
📚 Next Steps:
1. Install dbt-snowflake: pip install dbt-snowflake
2. Copy the profiles.yml to ~/.dbt/profiles.yml
3. Create your own dbt project or use the sample
4. Run: dbt run --profiles-dir ~/.dbt
5. Run: dbt test
6. Run: dbt docs generate && dbt docs serve

🔗 Useful API Endpoints:
- GET  /api/dbt/status       - Check dbt status
- POST /api/dbt/compile      - Compile SQL
- POST /api/dbt/run          - Run models
- POST /api/dbt/test         - Run tests
- GET  /api/dbt/models       - List models
- GET  /api/dbt/sources      - List sources
- GET  /api/dbt/docs         - Generate docs
""")


if __name__ == "__main__":
    try:
        main()
    except requests.exceptions.ConnectionError:
        print("""
❌ Could not connect to Snowglobe server!

Make sure Snowglobe is running:
  docker-compose up -d
  
Or:
  python -m snowglobe_server.server
        """)
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
