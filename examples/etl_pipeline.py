#!/usr/bin/env python3
"""
Snowglobe ETL Pipeline Example

This example demonstrates building and testing an ETL pipeline
using Snowglobe as a local Snowflake emulator.
"""

from datetime import datetime, timedelta
import random
import snowflake.connector


class SalesETL:
    """ETL pipeline for sales data"""
    
    def __init__(self, connection):
        self.conn = connection
        self.cursor = connection.cursor()
    
    def setup_raw_layer(self):
        """Create raw data layer tables"""
        print("Setting up raw layer...")
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_sales (
                transaction_id VARCHAR,
                product_code VARCHAR,
                quantity VARCHAR,
                unit_price VARCHAR,
                customer_id VARCHAR,
                sale_timestamp VARCHAR,
                region VARCHAR,
                loaded_at TIMESTAMP
            )
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_products (
                product_code VARCHAR,
                product_name VARCHAR,
                category VARCHAR,
                subcategory VARCHAR,
                loaded_at TIMESTAMP
            )
        """)
        
        print("Raw layer tables created!")
    
    def setup_staging_layer(self):
        """Create staging layer tables"""
        print("Setting up staging layer...")
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_sales (
                transaction_id INT,
                product_code VARCHAR,
                quantity INT,
                unit_price DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                customer_id INT,
                sale_date DATE,
                sale_hour INT,
                region VARCHAR,
                processed_at TIMESTAMP
            )
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_products (
                product_code VARCHAR,
                product_name VARCHAR,
                category VARCHAR,
                subcategory VARCHAR,
                processed_at TIMESTAMP
            )
        """)
        
        print("Staging layer tables created!")
    
    def setup_analytics_layer(self):
        """Create analytics layer tables"""
        print("Setting up analytics layer...")
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS analytics_daily_sales (
                sale_date DATE,
                region VARCHAR,
                category VARCHAR,
                total_transactions INT,
                total_quantity INT,
                total_revenue DECIMAL(15,2),
                avg_transaction_value DECIMAL(10,2),
                updated_at TIMESTAMP
            )
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS analytics_product_performance (
                product_code VARCHAR,
                product_name VARCHAR,
                category VARCHAR,
                total_units_sold INT,
                total_revenue DECIMAL(15,2),
                avg_unit_price DECIMAL(10,2),
                num_transactions INT,
                updated_at TIMESTAMP
            )
        """)
        
        print("Analytics layer tables created!")
    
    def extract_raw_data(self, num_transactions=100):
        """Simulate extracting raw data from source systems"""
        print(f"Extracting {num_transactions} raw transactions...")
        
        products = [
            ('PROD001', 'Laptop Pro', 'Electronics', 'Computers'),
            ('PROD002', 'Wireless Mouse', 'Electronics', 'Accessories'),
            ('PROD003', 'USB-C Hub', 'Electronics', 'Accessories'),
            ('PROD004', 'Monitor 27"', 'Electronics', 'Displays'),
            ('PROD005', 'Keyboard', 'Electronics', 'Accessories'),
        ]
        
        # Load products
        for prod in products:
            self.cursor.execute(f"""
                INSERT INTO raw_products VALUES (
                    '{prod[0]}', '{prod[1]}', '{prod[2]}', '{prod[3]}',
                    CURRENT_TIMESTAMP
                )
            """)
        
        # Generate random sales
        regions = ['North', 'South', 'East', 'West']
        base_date = datetime.now() - timedelta(days=30)
        
        for i in range(num_transactions):
            txn_id = f"TXN{i+1:06d}"
            prod = random.choice(products)
            qty = random.randint(1, 10)
            price = random.uniform(10, 500)
            customer = random.randint(1000, 9999)
            days_ago = random.randint(0, 30)
            timestamp = (base_date + timedelta(days=days_ago)).strftime('%Y-%m-%d %H:%M:%S')
            region = random.choice(regions)
            
            self.cursor.execute(f"""
                INSERT INTO raw_sales VALUES (
                    '{txn_id}', '{prod[0]}', '{qty}', '{price:.2f}',
                    '{customer}', '{timestamp}', '{region}',
                    CURRENT_TIMESTAMP
                )
            """)
        
        print(f"Extracted {num_transactions} transactions and {len(products)} products")
    
    def transform_to_staging(self):
        """Transform raw data to staging layer"""
        print("Transforming data to staging layer...")
        
        # Transform products (simple pass-through with timestamp)
        self.cursor.execute("""
            INSERT INTO stg_products
            SELECT 
                product_code,
                product_name,
                category,
                subcategory,
                CURRENT_TIMESTAMP
            FROM raw_products
        """)
        
        # Transform sales with data cleaning and calculations
        self.cursor.execute("""
            INSERT INTO stg_sales
            SELECT 
                CAST(REPLACE(transaction_id, 'TXN', '') AS INT) as transaction_id,
                product_code,
                CAST(quantity AS INT) as quantity,
                CAST(unit_price AS DECIMAL(10,2)) as unit_price,
                CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) as total_amount,
                CAST(customer_id AS INT) as customer_id,
                CAST(sale_timestamp AS DATE) as sale_date,
                EXTRACT(HOUR FROM CAST(sale_timestamp AS TIMESTAMP)) as sale_hour,
                region,
                CURRENT_TIMESTAMP
            FROM raw_sales
        """)
        
        self.cursor.execute("SELECT COUNT(*) FROM stg_sales")
        count = self.cursor.fetchone()[0]
        print(f"Transformed {count} records to staging layer")
    
    def load_analytics(self):
        """Load data into analytics layer"""
        print("Loading analytics layer...")
        
        # Daily sales aggregation
        self.cursor.execute("""
            INSERT INTO analytics_daily_sales
            SELECT 
                s.sale_date,
                s.region,
                p.category,
                COUNT(*) as total_transactions,
                SUM(s.quantity) as total_quantity,
                SUM(s.total_amount) as total_revenue,
                AVG(s.total_amount) as avg_transaction_value,
                CURRENT_TIMESTAMP
            FROM stg_sales s
            JOIN stg_products p ON s.product_code = p.product_code
            GROUP BY s.sale_date, s.region, p.category
        """)
        
        # Product performance
        self.cursor.execute("""
            INSERT INTO analytics_product_performance
            SELECT 
                s.product_code,
                p.product_name,
                p.category,
                SUM(s.quantity) as total_units_sold,
                SUM(s.total_amount) as total_revenue,
                AVG(s.unit_price) as avg_unit_price,
                COUNT(*) as num_transactions,
                CURRENT_TIMESTAMP
            FROM stg_sales s
            JOIN stg_products p ON s.product_code = p.product_code
            GROUP BY s.product_code, p.product_name, p.category
        """)
        
        print("Analytics layer loaded!")
    
    def run_pipeline(self, num_transactions=100):
        """Run the complete ETL pipeline"""
        print("\n=== Starting ETL Pipeline ===\n")
        
        start_time = datetime.now()
        
        self.setup_raw_layer()
        self.setup_staging_layer()
        self.setup_analytics_layer()
        
        self.extract_raw_data(num_transactions)
        self.transform_to_staging()
        self.load_analytics()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n=== Pipeline Complete in {duration:.2f} seconds ===\n")
    
    def generate_report(self):
        """Generate analytics report"""
        print("\n=== Analytics Report ===\n")
        
        # Top performing products
        print("Top 5 Products by Revenue:")
        self.cursor.execute("""
            SELECT 
                product_name,
                total_revenue,
                total_units_sold,
                num_transactions
            FROM analytics_product_performance
            ORDER BY total_revenue DESC
            LIMIT 5
        """)
        
        print(f"{'Product':<20} {'Revenue':>12} {'Units':>10} {'Transactions':>15}")
        print("-" * 60)
        for row in self.cursor:
            print(f"{row[0]:<20} ${row[1]:>11,.2f} {row[2]:>10} {row[3]:>15}")
        
        # Revenue by region
        print("\n\nRevenue by Region:")
        self.cursor.execute("""
            SELECT 
                region,
                SUM(total_revenue) as revenue,
                SUM(total_transactions) as transactions
            FROM analytics_daily_sales
            GROUP BY region
            ORDER BY revenue DESC
        """)
        
        print(f"{'Region':<10} {'Revenue':>15} {'Transactions':>15}")
        print("-" * 45)
        for row in self.cursor:
            print(f"{row[0]:<10} ${row[1]:>14,.2f} {row[2]:>15}")
        
        # Category performance
        print("\n\nPerformance by Category:")
        self.cursor.execute("""
            SELECT 
                category,
                SUM(total_revenue) as revenue,
                SUM(total_quantity) as units
            FROM analytics_daily_sales
            GROUP BY category
            ORDER BY revenue DESC
        """)
        
        print(f"{'Category':<15} {'Revenue':>15} {'Units Sold':>12}")
        print("-" * 45)
        for row in self.cursor:
            print(f"{row[0]:<15} ${row[1]:>14,.2f} {row[2]:>12}")
    
    def cleanup(self):
        """Clean up all tables"""
        print("\nCleaning up tables...")
        tables = [
            'analytics_product_performance',
            'analytics_daily_sales',
            'stg_products',
            'stg_sales',
            'raw_products',
            'raw_sales'
        ]
        
        for table in tables:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        print("Cleanup complete!")


def main():
    """Main function to run the ETL pipeline demo"""
    print("Connecting to Snowglobe...")
    conn = snowflake.connector.connect(
        host='localhost',
        port=8084,
        user='etl_user',
        password='etl_pass',
        database='ANALYTICS_DB',
        schema='PUBLIC',
        account='snowglobe'
    )
    
    etl = SalesETL(conn)
    
    try:
        # Run the pipeline with 200 sample transactions
        etl.run_pipeline(num_transactions=200)
        
        # Generate reports
        etl.generate_report()
        
        # Cleanup (comment out to keep data)
        etl.cleanup()
        
    finally:
        conn.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    main()
