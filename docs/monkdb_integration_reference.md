# MonkDB Integration Reference Guide

## Core Principles
- MonkDB is an OLAP and analytical database optimized for batch operations
- Writes should be batched where possible (500-2000 rows per batch)
- Use parameterized inserts, not string interpolation
- Avoid SELECT *; always specify columns
- Use cursor.execute() for small datasets, HTTP ingest for large volumes
- Implement retry logic with exponential backoff for failed operations

## Connection Pattern
```python
from monkdb import client
import os

def monkdb_connect():
    """Establish connection to MonkDB with proper error handling."""
    try:
        conn = client.connect(
            f"http://{os.getenv('MONK_USER')}:{os.getenv('MONK_PASS')}@{os.getenv('MONK_HOST')}:{os.getenv('MONK_PORT')}",
            username=os.getenv('MONK_USER')
        )
        return conn
    except Exception as e:
        raise ConnectionError(f"Failed to connect to MonkDB: {e}")
```

## Insert Pattern
```python
# Single record insert
cursor.execute("""
INSERT INTO schema.table (col1, col2, col3)
VALUES (?, ?, ?)
""", (value1, value2, value3))

# Batch insert (preferred for multiple records)
records = [(val1, val2, val3), (val4, val5, val6), ...]
cursor.executemany("""
INSERT INTO schema.table (col1, col2, col3)
VALUES (?, ?, ?)
""", records)

conn.commit()
```

## Bulk Insert Pattern
```python
def bulk_insert(cursor, table_name, columns, records, batch_size=1000):
    """Bulk insert with batching and error handling."""
    placeholders = ', '.join(['?' for _ in columns])
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            cursor.executemany(query, batch)
            cursor.connection.commit()
        except Exception as e:
            cursor.connection.rollback()
            raise Exception(f"Bulk insert failed for batch {i//batch_size + 1}: {e}")
```

## Upsert Pattern (SCD Type 2 Aware)
```python
def upsert_dimension(cursor, table_name, business_key, record_data):
    """
    Upsert with Slowly Changing Dimension Type 2 support.
    - Query to check existing record by business key
    - If no change: ignore
    - If change: close old record (valid_to) + insert new record (valid_from)
    """
    # Check for existing active record
    cursor.execute(f"""
    SELECT * FROM {table_name} 
    WHERE {business_key} = ? AND valid_to IS NULL
    """, (record_data[business_key],))
    
    existing = cursor.fetchone()
    
    if existing:
        # Compare data to detect changes
        if has_changes(existing, record_data):
            # Close existing record
            cursor.execute(f"""
            UPDATE {table_name} 
            SET valid_to = CURRENT_TIMESTAMP 
            WHERE {business_key} = ? AND valid_to IS NULL
            """, (record_data[business_key],))
            
            # Insert new record
            insert_new_record(cursor, table_name, record_data)
    else:
        # Insert new record
        insert_new_record(cursor, table_name, record_data)
    
    cursor.connection.commit()
```

## Query Pattern
```python
# Always specify columns, avoid SELECT *
cursor.execute("""
SELECT ride_id, driver_id, pickup_ts, fare_amount
FROM fact_rides 
WHERE pickup_ts >= ? AND pickup_ts < ?
""", (start_date, end_date))

results = cursor.fetchall()
```

## Error Handling and Retry Logic
```python
import time
import random

def execute_with_retry(cursor, query, params=None, max_retries=3):
    """Execute query with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            
            # Exponential backoff with jitter
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            time.sleep(wait_time)
```

## Environment Variables
```bash
# MonkDB Connection
MONK_HOST=localhost
MONK_PORT=4200
MONK_USER=username
MONK_PASS=password
MONK_SCHEMA=etl

# Batch Processing
MONK_BATCH_SIZE=1000
MONK_MAX_RETRIES=3
MONK_RETRY_DELAY=1
```

## Suggested Defaults
- **Timestamp Format**: UTC ISO format (YYYY-MM-DDTHH:MM:SS.sssZ)
- **Batching Threshold**: 500–2000 rows per batch
- **Retry Strategy**: Exponential backoff with max 3 attempts
- **Connection Pooling**: Use persistent connections where possible
- **Transaction Management**: Commit after each successful batch
- **Error Logging**: Log all failed operations with context

## Schema Conventions
```sql
-- Dimension Tables
CREATE TABLE dim_driver (
    driver_key INTEGER PRIMARY KEY,
    driver_id VARCHAR(50) NOT NULL,
    driver_name VARCHAR(100),
    license_number VARCHAR(50),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL,
    is_current BOOLEAN DEFAULT TRUE
);

-- Fact Tables
CREATE TABLE fact_rides (
    ride_key INTEGER PRIMARY KEY,
    ride_id VARCHAR(50) NOT NULL UNIQUE,
    driver_key INTEGER REFERENCES dim_driver(driver_key),
    vehicle_key INTEGER REFERENCES dim_vehicle(vehicle_key),
    pickup_time_key INTEGER,
    dropoff_time_key INTEGER,
    pickup_lat DECIMAL(10,8),
    pickup_lon DECIMAL(11,8),
    dropoff_lat DECIMAL(10,8),
    dropoff_lon DECIMAL(11,8),
    distance_km DECIMAL(8,2),
    duration_min INTEGER,
    fare_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Performance Optimization
- Use batch inserts instead of row-by-row operations
- Implement connection pooling for high-throughput scenarios
- Use appropriate indexes on frequently queried columns
- Monitor query performance and optimize as needed
- Consider partitioning for large fact tables by date