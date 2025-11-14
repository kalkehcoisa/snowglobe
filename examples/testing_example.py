#!/usr/bin/env python3
"""
Snowglobe Testing Example

This example demonstrates how to use Snowglobe for testing
data pipelines and SQL queries.
"""

import pytest
import snowflake.connector


# Fixture for database connection
@pytest.fixture
def db_connection():
    """Create a database connection for testing"""
    conn = snowflake.connector.connect(
        host='localhost',
        port=8084,
        user='test_user',
        password='test_pass',
        database='TEST_DB',
        schema='PUBLIC',
        account='snowglobe'
    )
    yield conn
    conn.close()


@pytest.fixture
def sample_data(db_connection):
    """Set up sample data for tests"""
    cursor = db_connection.cursor()
    
    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INT,
            name VARCHAR,
            price DECIMAL(10,2),
            category VARCHAR
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INT,
            product_id INT,
            quantity INT,
            order_date DATE
        )
    """)
    
    # Insert test data
    products = [
        (1, 'Widget A', 10.00, 'Widgets'),
        (2, 'Widget B', 15.00, 'Widgets'),
        (3, 'Gadget X', 25.00, 'Gadgets'),
        (4, 'Gadget Y', 30.00, 'Gadgets'),
    ]
    
    for p in products:
        cursor.execute(f"INSERT INTO products VALUES ({p[0]}, '{p[1]}', {p[2]}, '{p[3]}')")
    
    orders = [
        (1, 1, 5, '2023-01-01'),
        (2, 2, 3, '2023-01-02'),
        (3, 3, 2, '2023-01-03'),
        (4, 1, 4, '2023-01-04'),
    ]
    
    for o in orders:
        cursor.execute(f"INSERT INTO orders VALUES ({o[0]}, {o[1]}, {o[2]}, '{o[3]}')")
    
    yield cursor
    
    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS products")
    cursor.execute("DROP TABLE IF EXISTS orders")
    cursor.close()


class TestProductQueries:
    """Test product-related queries"""
    
    def test_count_products(self, sample_data):
        """Test counting products"""
        cursor = sample_data
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()[0]
        assert count == 4
    
    def test_average_price(self, sample_data):
        """Test calculating average price"""
        cursor = sample_data
        cursor.execute("SELECT AVG(price) FROM products")
        avg_price = cursor.fetchone()[0]
        assert avg_price == 20.0  # (10 + 15 + 25 + 30) / 4
    
    def test_category_count(self, sample_data):
        """Test counting products by category"""
        cursor = sample_data
        cursor.execute("""
            SELECT category, COUNT(*) as count
            FROM products
            GROUP BY category
            ORDER BY category
        """)
        results = cursor.fetchall()
        assert len(results) == 2
        assert results[0] == ('Gadgets', 2)
        assert results[1] == ('Widgets', 2)
    
    def test_filter_by_price(self, sample_data):
        """Test filtering products by price"""
        cursor = sample_data
        cursor.execute("SELECT name FROM products WHERE price > 20 ORDER BY price")
        results = cursor.fetchall()
        assert len(results) == 2
        assert results[0][0] == 'Gadget X'
        assert results[1][0] == 'Gadget Y'


class TestOrderAnalytics:
    """Test order analytics queries"""
    
    def test_total_quantity_ordered(self, sample_data):
        """Test total quantity ordered"""
        cursor = sample_data
        cursor.execute("SELECT SUM(quantity) FROM orders")
        total = cursor.fetchone()[0]
        assert total == 14  # 5 + 3 + 2 + 4
    
    def test_orders_per_product(self, sample_data):
        """Test counting orders per product"""
        cursor = sample_data
        cursor.execute("""
            SELECT product_id, COUNT(*) as order_count, SUM(quantity) as total_qty
            FROM orders
            GROUP BY product_id
            ORDER BY product_id
        """)
        results = cursor.fetchall()
        
        # Product 1 has 2 orders (5 + 4 = 9 quantity)
        assert results[0] == (1, 2, 9)
        # Product 2 has 1 order (3 quantity)
        assert results[1] == (2, 1, 3)
        # Product 3 has 1 order (2 quantity)
        assert results[2] == (3, 1, 2)
    
    def test_revenue_calculation(self, sample_data):
        """Test calculating revenue by joining tables"""
        cursor = sample_data
        cursor.execute("""
            SELECT 
                p.name,
                SUM(o.quantity * p.price) as revenue
            FROM orders o
            JOIN products p ON o.product_id = p.id
            GROUP BY p.name
            ORDER BY revenue DESC
        """)
        results = cursor.fetchall()
        
        # Widget A: 9 units * $10 = $90
        assert results[0][0] == 'Widget A'
        assert float(results[0][1]) == 90.0


class TestDataTransformations:
    """Test data transformation scenarios"""
    
    def test_case_when_transformation(self, sample_data):
        """Test CASE WHEN transformation"""
        cursor = sample_data
        cursor.execute("""
            SELECT 
                name,
                CASE 
                    WHEN price < 15 THEN 'Budget'
                    WHEN price < 25 THEN 'Mid-Range'
                    ELSE 'Premium'
                END as tier
            FROM products
            ORDER BY price
        """)
        results = cursor.fetchall()
        assert results[0][1] == 'Budget'  # Widget A at $10
        assert results[2][1] == 'Premium'  # Gadget X at $25
    
    def test_cte_transformation(self, sample_data):
        """Test Common Table Expression"""
        cursor = sample_data
        cursor.execute("""
            WITH product_stats AS (
                SELECT 
                    category,
                    AVG(price) as avg_price,
                    COUNT(*) as product_count
                FROM products
                GROUP BY category
            )
            SELECT * FROM product_stats ORDER BY avg_price DESC
        """)
        results = cursor.fetchall()
        # Gadgets average: (25 + 30) / 2 = 27.5
        # Widgets average: (10 + 15) / 2 = 12.5
        assert results[0][0] == 'Gadgets'
        assert float(results[0][1]) == 27.5


class TestDataIntegrity:
    """Test data integrity scenarios"""
    
    def test_no_orphan_orders(self, sample_data):
        """Test that all orders reference valid products"""
        cursor = sample_data
        cursor.execute("""
            SELECT COUNT(*)
            FROM orders o
            LEFT JOIN products p ON o.product_id = p.id
            WHERE p.id IS NULL
        """)
        orphan_count = cursor.fetchone()[0]
        assert orphan_count == 0
    
    def test_unique_product_ids(self, sample_data):
        """Test that product IDs are unique"""
        cursor = sample_data
        cursor.execute("""
            SELECT id, COUNT(*) as cnt
            FROM products
            GROUP BY id
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        assert len(duplicates) == 0


def test_snowflake_compatibility():
    """Test Snowflake-specific syntax compatibility"""
    conn = snowflake.connector.connect(
        host='localhost',
        port=8084,
        user='user',
        password='pass',
        account='snowglobe'
    )
    cursor = conn.cursor()
    
    # Test IFF function (Snowflake-specific)
    cursor.execute("SELECT IFF(10 > 5, 'greater', 'less')")
    result = cursor.fetchone()[0]
    assert result == 'greater'
    
    # Test ZEROIFNULL function
    cursor.execute("SELECT ZEROIFNULL(NULL)")
    result = cursor.fetchone()[0]
    assert result == 0
    
    # Test DIV0 function (divide by zero returns 0)
    cursor.execute("SELECT DIV0(100, 0)")
    result = cursor.fetchone()[0]
    assert result == 0
    
    conn.close()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
