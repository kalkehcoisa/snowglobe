# 📊 Snowglobe dbt Integration Guide

This guide covers the full dbt (data build tool) support in Snowglobe, enabling you to develop and test dbt projects locally without a real Snowflake account.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Features](#features)
- [API Reference](#api-reference)
- [Examples](#examples)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## Overview

Snowglobe provides comprehensive dbt support including:

- ✅ **Model Materialization** - table, view, incremental, ephemeral
- ✅ **Sources** - Define and reference source tables
- ✅ **Seeds** - Load CSV files as tables
- ✅ **Tests** - Run schema and singular tests
- ✅ **Snapshots** - SCD Type 2 snapshots with timestamp/check strategies
- ✅ **Documentation** - Generate dbt docs compatible manifests
- ✅ **Jinja Compilation** - Full ref(), source(), var(), config() support
- ✅ **Lineage** - Track model dependencies

---

## Quick Start

### 1. Start Snowglobe

```bash
# Using Docker
docker-compose up -d

# Or manually
pip install -r requirements-server.txt
python -m snowglobe_server.server
```

### 2. Configure dbt Profile

Create or update `~/.dbt/profiles.yml`:

```yaml
snowglobe:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: localhost
      user: dbt_user
      password: dbt_password  # Any password works
      role: ACCOUNTADMIN
      database: SNOWGLOBE
      warehouse: COMPUTE_WH
      schema: PUBLIC
      threads: 4
      client_session_keep_alive: false
      # Snowglobe-specific settings
      host: localhost
      port: 8443
      protocol: https
      insecure_mode: true  # For self-signed certificates
```

Or get the profile via API:

```bash
curl http://localhost:8084/api/dbt/profiles
```

### 3. Run dbt Commands

```bash
# Set profile directory if needed
export DBT_PROFILES_DIR=~/.dbt

# Run your dbt project
cd your_dbt_project
dbt run
dbt test
dbt seed
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SNOWGLOBE_PORT` | `8084` | HTTP port |
| `SNOWGLOBE_HTTPS_PORT` | `8443` | HTTPS port for dbt connections |
| `SNOWGLOBE_DATA_DIR` | `/data` | Data directory |

### dbt Project Configuration

Example `dbt_project.yml`:

```yaml
name: my_snowglobe_project
version: '1.0.0'
config-version: 2
profile: snowglobe

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]

vars:
  start_date: '2024-01-01'
  environment: dev

models:
  my_snowglobe_project:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

---

## Features

### Model Materializations

#### View (Default)
```sql
{{ config(materialized='view') }}

SELECT * FROM {{ ref('source_model') }}
```

#### Table
```sql
{{ config(materialized='table') }}

SELECT * FROM {{ source('raw', 'orders') }}
```

#### Incremental
```sql
{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT *
FROM {{ source('raw', 'events') }}
{% if is_incremental() %}
WHERE event_date > (SELECT MAX(event_date) FROM {{ this }})
{% endif %}
```

#### Ephemeral
```sql
{{ config(materialized='ephemeral') }}

-- This model becomes a CTE in downstream models
SELECT DISTINCT customer_id FROM {{ ref('orders') }}
```

### Sources

Define sources in `schema.yml`:

```yaml
version: 2

sources:
  - name: raw
    database: SNOWGLOBE
    schema: RAW
    tables:
      - name: customers
        description: Raw customer data
        columns:
          - name: id
            description: Primary key
          - name: name
          - name: email
        freshness:
          warn_after:
            count: 12
            period: hour
          error_after:
            count: 24
            period: hour
          loaded_at_field: _loaded_at
      
      - name: orders
        description: Raw order data
```

Reference sources:
```sql
SELECT * FROM {{ source('raw', 'customers') }}
```

### Seeds

Place CSV files in `seeds/` directory:

```csv
# seeds/country_codes.csv
code,name,region
US,United States,North America
CA,Canada,North America
UK,United Kingdom,Europe
```

Load seeds:
```bash
dbt seed
```

Configure seed properties in `dbt_project.yml`:
```yaml
seeds:
  my_project:
    +schema: seeds
    country_codes:
      +column_types:
        code: VARCHAR(2)
```

### Tests

#### Schema Tests

In `schema.yml`:
```yaml
models:
  - name: customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['active', 'inactive', 'pending']
      - name: country_id
        tests:
          - relationships:
              to: ref('countries')
              field: id
```

#### Singular Tests

Create SQL files in `tests/`:

```sql
-- tests/assert_positive_amounts.sql
SELECT *
FROM {{ ref('orders') }}
WHERE amount < 0
```

Run tests:
```bash
dbt test
```

### Snapshots

Create snapshot files in `snapshots/`:

```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
) }}

SELECT * FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
```

For check strategy:
```sql
{% snapshot orders_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='id',
    strategy='check',
    check_cols=['status', 'amount'],
) }}

SELECT * FROM {{ source('raw', 'orders') }}

{% endsnapshot %}
```

Run snapshots:
```bash
dbt snapshot
```

### Variables

Define in `dbt_project.yml`:
```yaml
vars:
  start_date: '2024-01-01'
  environment: dev
  important_customers: ['c1', 'c2', 'c3']
```

Use in models:
```sql
SELECT *
FROM {{ ref('customers') }}
WHERE created_at >= '{{ var("start_date") }}'
{% if var("environment") == "dev" %}
LIMIT 1000
{% endif %}
```

Override at runtime:
```bash
dbt run --vars '{"start_date": "2024-06-01"}'
```

---

## API Reference

### dbt Status
```bash
GET /api/dbt/status

# Returns dbt adapter capabilities and version info
```

### Compile SQL
```bash
POST /api/dbt/compile
Content-Type: application/json

{
    "sql": "SELECT * FROM {{ ref('customers') }}",
    "vars": {"my_var": "value"}
}
```

### Run Models
```bash
POST /api/dbt/run
Content-Type: application/json

{
    "select": "+my_model",  # Optional: model selection
    "exclude": "model_to_skip",  # Optional: exclusions
    "full_refresh": false  # Optional: full refresh for incremental
}
```

### Load Seeds
```bash
POST /api/dbt/seed
Content-Type: application/json

{
    "select": "seed_name",  # Optional
    "full_refresh": true  # Optional
}
```

### Run Tests
```bash
POST /api/dbt/test
Content-Type: application/json

{
    "select": "test_name"  # Optional
}
```

### Run Snapshots
```bash
POST /api/dbt/snapshot
Content-Type: application/json

{
    "select": "snapshot_name"  # Optional
}
```

### List/Register Sources
```bash
# List sources
GET /api/dbt/sources

# Register source
POST /api/dbt/sources
Content-Type: application/json

{
    "name": "raw",
    "database": "SNOWGLOBE",
    "schema_name": "RAW",
    "tables": [{"name": "customers"}, {"name": "orders"}],
    "description": "Raw data source"
}
```

### List/Register Models
```bash
# List models
GET /api/dbt/models

# Register model
POST /api/dbt/models
Content-Type: application/json

{
    "name": "dim_customers",
    "sql": "SELECT * FROM {{ ref('stg_customers') }}",
    "materialization": "table",
    "tags": ["pii", "important"],
    "description": "Customer dimension table"
}

# Get model details
GET /api/dbt/models/{model_name}

# Run specific model
POST /api/dbt/models/{model_name}/run?full_refresh=false

# Get model lineage
GET /api/dbt/models/{model_name}/lineage
```

### Source Freshness
```bash
GET /api/dbt/source-freshness?select=raw
```

### Generate Documentation
```bash
GET /api/dbt/docs
```

### Load Project
```bash
POST /api/dbt/project/load
Content-Type: application/json

{
    "project_dir": "/path/to/dbt/project"
}
```

### Generate Sample Project
```bash
POST /api/dbt/project/generate
Content-Type: application/json

{
    "project_dir": "/path/to/new/project"
}
```

### Variables
```bash
# Get variables
GET /api/dbt/vars

# Set variables
POST /api/dbt/vars
Content-Type: application/json

{
    "vars": {"my_var": "value"}
}
```

### Run Results
```bash
GET /api/dbt/run-results
```

### Get Profiles
```bash
GET /api/dbt/profiles
```

---

## Examples

### Example 1: Simple dbt Project

```
my_project/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── schema.yml
│   │   └── stg_customers.sql
│   └── marts/
│       └── dim_customers.sql
├── seeds/
│   └── country_codes.csv
└── tests/
    └── assert_no_orphans.sql
```

**dbt_project.yml:**
```yaml
name: my_project
version: '1.0.0'
config-version: 2
profile: snowglobe
```

**models/staging/stg_customers.sql:**
```sql
{{ config(materialized='view') }}

SELECT
    id AS customer_id,
    UPPER(name) AS customer_name,
    LOWER(email) AS email,
    created_at
FROM {{ source('raw', 'customers') }}
```

**models/marts/dim_customers.sql:**
```sql
{{ config(materialized='table') }}

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    cc.name AS country_name,
    c.created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('country_codes') }} cc
    ON c.country_code = cc.code
```

### Example 2: Using Snowglobe API Directly

```python
import requests

BASE_URL = "http://localhost:8084"

# Register a source
source_response = requests.post(f"{BASE_URL}/api/dbt/sources", json={
    "name": "raw",
    "database": "SNOWGLOBE",
    "schema_name": "RAW",
    "tables": [{"name": "orders"}, {"name": "customers"}]
})
print(source_response.json())

# Register a model
model_response = requests.post(f"{BASE_URL}/api/dbt/models", json={
    "name": "stg_orders",
    "sql": """
        SELECT
            id AS order_id,
            customer_id,
            amount,
            status,
            created_at
        FROM {{ source('raw', 'orders') }}
    """,
    "materialization": "view",
    "description": "Staged orders data"
})
print(model_response.json())

# Run the model
run_response = requests.post(f"{BASE_URL}/api/dbt/models/stg_orders/run")
print(run_response.json())

# Get lineage
lineage_response = requests.get(f"{BASE_URL}/api/dbt/models/stg_orders/lineage")
print(lineage_response.json())
```

### Example 3: Incremental Model

```sql
-- models/fct_events.sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert'
) }}

WITH source_data AS (
    SELECT
        event_id,
        user_id,
        event_type,
        event_data,
        event_timestamp
    FROM {{ source('raw', 'events') }}
    {% if is_incremental() %}
    WHERE event_timestamp > (
        SELECT COALESCE(MAX(event_timestamp), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    event_id,
    user_id,
    event_type,
    event_data::JSON AS event_data,
    event_timestamp,
    CURRENT_TIMESTAMP AS processed_at
FROM source_data
```

---

## Best Practices

### 1. Use Meaningful Model Names
```
staging/
  stg_<source>__<table>.sql
marts/
  dim_<entity>.sql
  fct_<event>.sql
```

### 2. Document Everything
```yaml
models:
  - name: dim_customers
    description: |
      Customer dimension table containing all customer attributes.
      
      **Grain:** One row per customer
      **Update frequency:** Daily
    columns:
      - name: customer_id
        description: Primary key
        tests:
          - unique
          - not_null
```

### 3. Use Tags for Organization
```sql
{{ config(
    materialized='table',
    tags=['pii', 'daily', 'core']
) }}
```

Run specific tags:
```bash
dbt run --select tag:daily
```

### 4. Test Critical Models
- Always add `unique` and `not_null` tests to primary keys
- Use `relationships` tests for foreign keys
- Create singular tests for complex business logic

### 5. Use Full Refresh Wisely
```bash
# Only for incremental models that need rebuild
dbt run --select my_incremental_model --full-refresh
```

---

## Troubleshooting

### Connection Issues

**Problem:** Can't connect to Snowglobe

**Solution:**
1. Ensure Snowglobe is running on the correct ports
2. Use `insecure_mode: true` for self-signed certificates
3. Check the host is `localhost` not `127.0.0.1`

### Compilation Errors

**Problem:** `ref()` or `source()` not found

**Solution:**
1. Ensure models/sources are registered
2. Check spelling matches exactly
3. Load the project first via API

### Test Failures

**Problem:** Tests failing unexpectedly

**Solution:**
1. Review test SQL with `/api/dbt/compile`
2. Run the compiled SQL directly to inspect results
3. Check for data quality issues in source data

### Incremental Model Issues

**Problem:** Incremental model not updating

**Solution:**
1. Verify `is_incremental()` logic is correct
2. Check that the `this` reference resolves properly
3. Use `--full-refresh` if schema changed

---

## Support

For issues with dbt integration:
1. Check the server logs at `/api/logs`
2. Review the run results at `/api/dbt/run-results`
3. Use the `/api/dbt/compile` endpoint to debug SQL
4. Open an issue on GitHub

---

<div align="center">
Made with ❄️ for the dbt community
</div>
