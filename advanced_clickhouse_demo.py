from clickhouse_driver import Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import uuid

def create_client():
    """Create a ClickHouse client connection"""
    return Client(host='localhost', port=9000, user='default', password='clickhouse')

def create_advanced_tables(client):
    """Create tables demonstrating advanced ClickHouse features"""
    
    # Create database first
    client.execute('CREATE DATABASE IF NOT EXISTS demo_db')
    
    # 1. Create source table for materialized view
    client.execute('''
        CREATE TABLE IF NOT EXISTS demo_db.raw_events (
            event_id UUID,
            user_id UInt32,
            event_type String,
            event_time DateTime,
            properties Map(String, String),
            metrics Map(String, Float64)
        ) ENGINE = MergeTree()
        ORDER BY (event_time, user_id)
    ''')

    # Create materialized view after source table exists
    client.execute('''
        CREATE MATERIALIZED VIEW IF NOT EXISTS demo_db.event_stats_mv
        ENGINE = AggregatingMergeTree()
        ORDER BY (event_type, date)
        AS SELECT
            event_type,
            toDate(event_time) as date,
            count() as event_count,
            uniq(user_id) as unique_users,
            sum(metrics['duration']) as total_duration
        FROM demo_db.raw_events
        GROUP BY event_type, toDate(event_time)
    ''')

    # 2. Array and Nested Data Types
    client.execute('''
        CREATE TABLE IF NOT EXISTS demo_db.user_sessions (
            session_id UUID,
            user_id UInt32,
            start_time DateTime,
            end_time DateTime,
            page_views Array(String),
            time_spent Array(UInt32),
            device_info Nested(
                name String,
                version String,
                os String
            )
        ) ENGINE = MergeTree()
        ORDER BY (user_id, start_time)
    ''')

    # 3. Time Series with TTL
    client.execute('''
        CREATE TABLE IF NOT EXISTS demo_db.metrics (
            metric_id UInt32,
            timestamp DateTime,
            value Float64,
            tags Map(String, String)
        ) ENGINE = MergeTree()
        ORDER BY (metric_id, timestamp)
        TTL timestamp + INTERVAL 1 MONTH
    ''')

    # 4. Regular MergeTree table (for demonstration)
    client.execute('''
        CREATE TABLE IF NOT EXISTS demo_db.events (
            event_id UUID,
            user_id UInt32,
            event_type String,
            event_time DateTime,
            properties Map(String, String)
        ) ENGINE = MergeTree()
        ORDER BY (event_time, user_id)
    ''')

def generate_sample_data(client):
    """Generate and insert sample data"""
    
    # Generate raw events
    num_events = 10000
    event_types = ['page_view', 'click', 'purchase', 'login', 'logout']
    user_ids = range(1, 1001)
    
    events = []
    for i in range(num_events):
        event_id = str(uuid.uuid4())
        event_time = datetime.now() - timedelta(minutes=np.random.randint(0, 1440))
        user_id = np.random.choice(user_ids)
        event_type = np.random.choice(event_types)
        
        events.append((
            event_id,  # event_id
            user_id,
            event_type,
            event_time,
            {'browser': np.random.choice(['chrome', 'firefox', 'safari']),
             'country': np.random.choice(['US', 'UK', 'CA', 'AU'])},
            {'duration': np.random.uniform(1, 300)}
        ))
    
    client.execute(
        'INSERT INTO demo_db.raw_events (event_id, user_id, event_type, event_time, properties, metrics) VALUES',
        events
    )

    # Generate user sessions
    sessions = []
    for user_id in range(1, 101):
        start_time = datetime.now() - timedelta(days=np.random.randint(0, 30))
        end_time = start_time + timedelta(minutes=np.random.randint(1, 60))
        
        num_pages = np.random.randint(3, 10)
        pages = [f'page_{i}' for i in range(num_pages)]
        times = [np.random.randint(10, 300) for _ in range(num_pages)]
        
        # For nested data, we need to create arrays of the same length
        num_devices = 1  # We'll use 1 device per session
        device_names = ['Chrome'] * num_devices
        device_versions = ['1.0'] * num_devices
        device_os = ['Windows'] * num_devices
        
        sessions.append((
            str(uuid.uuid4()),  # session_id
            user_id,
            start_time,
            end_time,
            pages,
            times,
            device_names,
            device_versions,
            device_os
        ))
    
    client.execute(
        'INSERT INTO demo_db.user_sessions (session_id, user_id, start_time, end_time, page_views, time_spent, device_info.name, device_info.version, device_info.os) VALUES',
        sessions
    )

def demonstrate_features(client):
    """Demonstrate advanced ClickHouse features"""
    
    print("\n1. Materialized View Aggregations:")
    result = client.execute('''
        SELECT 
            event_type,
            date,
            event_count,
            unique_users,
            total_duration
        FROM demo_db.event_stats_mv
        ORDER BY date DESC, event_type
        LIMIT 10
    ''')
    for row in result:
        print(row)

    print("\n2. Array and Nested Data Operations:")
    result = client.execute('''
        SELECT 
            user_id,
            arrayJoin(page_views) as page,
            arrayElement(time_spent, indexOf(page_views, page)) as time_spent
        FROM demo_db.user_sessions
        WHERE user_id = 1
    ''')
    for row in result:
        print(row)

    print("\n3. Map Operations:")
    result = client.execute('''
        SELECT 
            properties['browser'] as browser,
            count() as count,
            avg(metrics['duration']) as avg_duration
        FROM demo_db.raw_events
        GROUP BY browser
        ORDER BY count DESC
    ''')
    for row in result:
        print(row)

    print("\n4. Time Series Analysis:")
    result = client.execute('''
        SELECT 
            toStartOfHour(event_time) as hour,
            count() as events,
            uniq(user_id) as users
        FROM demo_db.raw_events
        GROUP BY hour
        ORDER BY hour DESC
        LIMIT 5
    ''')
    for row in result:
        print(row)

    print("\n5. Advanced Aggregations with Window Functions:")
    result = client.execute('''
        SELECT 
            user_id,
            event_type,
            count() as event_count,
            event_count - lagInFrame(event_count) OVER (PARTITION BY user_id ORDER BY event_type) as diff
        FROM demo_db.raw_events
        GROUP BY user_id, event_type
        ORDER BY user_id, event_type
        LIMIT 10
    ''')
    for row in result:
        print(row)

def main():
    client = create_client()
    
    print("Creating advanced tables...")
    create_advanced_tables(client)
    
    print("Generating sample data...")
    generate_sample_data(client)
    
    print("Demonstrating advanced features...")
    demonstrate_features(client)

if __name__ == "__main__":
    main() 