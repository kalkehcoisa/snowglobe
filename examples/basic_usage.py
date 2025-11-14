#!/usr/bin/env python3
"""
Snowglobe Basic Usage Example

This example demonstrates basic usage of Snowglobe as a local Snowflake emulator.
"""

import snowflake.connector


def main():
    # Connect to Snowglobe server
    print("Connecting to Snowglobe server...")
    conn = snowflake.connector.connect(
        host='localhost',
        port=8084,
        user='demo_user',
        password='demo_password',
        database='DEMO_DB',
        schema='PUBLIC',
        account='snowglobe'
    )
    print("Connected!")
    
    # Create a cursor
    cursor = conn.cursor()
    
    # Create a table
    print("\nCreating users table...")
    cursor.execute("""
        CREATE TABLE users (
            id INT,
            name VARCHAR(100),
            email VARCHAR(255),
            age INT,
            created_at TIMESTAMP
        )
    """)
    print("Table created successfully!")
    
    # Insert data
    print("\nInserting sample data...")
    users_data = [
        (1, 'Alice Johnson', 'alice@example.com', 28),
        (2, 'Bob Smith', 'bob@example.com', 35),
        (3, 'Charlie Brown', 'charlie@example.com', 42),
        (4, 'Diana Prince', 'diana@example.com', 31),
        (5, 'Edward Norton', 'edward@example.com', 29),
    ]
    
    for user_id, name, email, age in users_data:
        cursor.execute(
            f"INSERT INTO users (id, name, email, age, created_at) "
            f"VALUES ({user_id}, '{name}', '{email}', {age}, CURRENT_TIMESTAMP)"
        )
    print(f"Inserted {len(users_data)} users")
    
    # Query data
    print("\nQuerying all users:")
    cursor.execute("SELECT id, name, email, age FROM users ORDER BY id")
    
    print(f"{'ID':<5} {'Name':<20} {'Email':<25} {'Age':<5}")
    print("-" * 60)
    for row in cursor:
        print(f"{row[0]:<5} {row[1]:<20} {row[2]:<25} {row[3]:<5}")
    
    # Aggregate query
    print("\nAggregate statistics:")
    cursor.execute("""
        SELECT 
            COUNT(*) as total_users,
            AVG(age) as avg_age,
            MIN(age) as min_age,
            MAX(age) as max_age
        FROM users
    """)
    stats = cursor.fetchone()
    print(f"Total Users: {stats[0]}")
    print(f"Average Age: {stats[1]:.2f}")
    print(f"Age Range: {stats[2]} - {stats[3]}")
    
    # Filter query
    print("\nUsers over 30:")
    cursor.execute("SELECT name, age FROM users WHERE age > 30 ORDER BY age DESC")
    for name, age in cursor:
        print(f"  {name}: {age} years old")
    
    # Update data
    print("\nUpdating Bob's age...")
    cursor.execute("UPDATE users SET age = 36 WHERE name = 'Bob Smith'")
    cursor.execute("SELECT age FROM users WHERE name = 'Bob Smith'")
    new_age = cursor.fetchone()[0]
    print(f"Bob's new age: {new_age}")
    
    # Delete data
    print("\nDeleting Edward...")
    cursor.execute("DELETE FROM users WHERE name = 'Edward Norton'")
    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]
    print(f"Remaining users: {count}")
    
    # Show current context
    print("\nCurrent session context:")
    cursor.execute("SELECT CURRENT_DATABASE()")
    print(f"  Database: {cursor.fetchone()[0]}")
    cursor.execute("SELECT CURRENT_SCHEMA()")
    print(f"  Schema: {cursor.fetchone()[0]}")
    cursor.execute("SELECT CURRENT_WAREHOUSE()")
    print(f"  Warehouse: {cursor.fetchone()[0]}")
    
    # Clean up
    print("\nDropping users table...")
    cursor.execute("DROP TABLE users")
    print("Table dropped!")
    
    # Close connection
    cursor.close()
    conn.close()
    print("\nConnection closed. Demo complete!")


if __name__ == "__main__":
    main()
