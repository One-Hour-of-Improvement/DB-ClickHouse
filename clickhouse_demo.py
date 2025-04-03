from clickhouse_driver import Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_client():
    """Create a ClickHouse client connection"""
    return Client(host='localhost', port=9000, user='default', password='clickhouse')

def create_database(client):
    """Create a new database"""
    client.execute('CREATE DATABASE IF NOT EXISTS demo_db')

def create_table(client):
    """Create a sample table"""
    client.execute('''
        CREATE TABLE IF NOT EXISTS demo_db.sample_data (
            id UInt32,
            timestamp DateTime,
            value Float64,
            category String
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, id)
    ''')

def insert_sample_data(client):
    """Insert sample data into the table"""
    # Generate sample data
    num_rows = 1000
    timestamps = [datetime.now() - timedelta(days=x) for x in range(num_rows)]
    values = np.random.normal(100, 15, num_rows)
    categories = np.random.choice(['A', 'B', 'C', 'D'], num_rows)
    
    data = [(i, ts, val, cat) for i, (ts, val, cat) in enumerate(zip(timestamps, values, categories))]
    
    client.execute(
        'INSERT INTO demo_db.sample_data (id, timestamp, value, category) VALUES',
        data
    )

def run_queries(client):
    """Run some example queries"""
    # Basic query
    print("\nBasic query:")
    result = client.execute('SELECT * FROM demo_db.sample_data LIMIT 5')
    for row in result:
        print(row)

    # Aggregation query
    print("\nAggregation query:")
    result = client.execute('''
        SELECT 
            category,
            COUNT(*) as count,
            AVG(value) as avg_value,
            MAX(value) as max_value,
            MIN(value) as min_value
        FROM demo_db.sample_data
        GROUP BY category
        ORDER BY avg_value DESC
    ''')
    for row in result:
        print(row)

    # Time-based query
    print("\nTime-based query:")
    result = client.execute('''
        SELECT 
            toDate(timestamp) as date,
            COUNT(*) as count,
            AVG(value) as avg_value
        FROM demo_db.sample_data
        GROUP BY date
        ORDER BY date DESC
        LIMIT 5
    ''')
    for row in result:
        print(row)

def main():
    client = create_client()
    
    print("Creating database...")
    create_database(client)
    
    print("Creating table...")
    create_table(client)
    
    print("Inserting sample data...")
    insert_sample_data(client)
    
    print("Running example queries...")
    run_queries(client)

if __name__ == "__main__":
    main() 