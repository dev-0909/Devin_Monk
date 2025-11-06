"""
MonkDB client utilities for PySpark ETL operations.
Provides connection management, bulk operations, and SCD-aware upserts.
"""

import os
import time
import random
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager
import structlog
from monkdb.client import connect

logger = structlog.get_logger(__name__)


class MonkDBConnectionError(Exception):
    """Custom exception for MonkDB connection issues."""
    pass


class MonkDBOperationError(Exception):
    """Custom exception for MonkDB operation failures."""
    pass


def monkdb_connect():
    """
    Establish connection to MonkDB with proper error handling.
    
    Returns:
        MonkDB connection object
        
    Raises:
        MonkDBConnectionError: If connection fails
    """
    try:
        host = os.getenv('MONK_HOST', 'localhost')
        port = int(os.getenv('MONK_PORT', '4200'))
        user = os.getenv('MONK_USER', 'admin')
        password = os.getenv('MONK_PASS', 'admin')  # noqa: F841

        logger.info("Connecting to MonkDB", host=host, port=port, user=user)

        # MonkDB connection using servers list
        servers = [f"{host}:{port}"]
        conn = connect(servers=servers)

        # Test connection with a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()

        logger.info("Successfully connected to MonkDB")
        return conn

    except Exception as e:
        logger.error("Failed to connect to MonkDB", error=str(e))
        raise MonkDBConnectionError(f"MonkDB connection failed: {e}") from e


@contextmanager
def get_monk_connection():
    """
    Context manager for MonkDB connections with automatic cleanup.
    
    Yields:
        MonkDB connection object
    """
    conn = None
    try:
        conn = monkdb_connect()
        yield conn
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("MonkDB connection closed")
            except Exception as e:
                logger.warning("Error closing MonkDB connection", error=str(e))


def execute_with_retry(cursor, query: str, params: Optional[Tuple] = None, max_retries: int = 3) -> List[Any]:
    """
    Execute query with exponential backoff retry logic.
    
    Args:
        cursor: MonkDB cursor object
        query: SQL query to execute
        params: Query parameters
        max_retries: Maximum number of retry attempts
        
    Returns:
        Query results
        
    Raises:
        MonkDBOperationError: If all retry attempts fail
    """
    for attempt in range(max_retries):
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            results = cursor.fetchall()

            if attempt > 0:
                logger.info("Query succeeded after retry", attempt=attempt + 1)

            return results

        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(
                    "Query failed after all retries",
                    query=query[:100],
                    attempts=max_retries,
                    error=str(e)
                )
                raise MonkDBOperationError(f"Query failed after {max_retries} attempts: {e}") from e

            # Exponential backoff with jitter
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(
                "Query failed, retrying",
                attempt=attempt + 1,
                wait_time=wait_time,
                error=str(e)
            )
            time.sleep(wait_time)
    
    # This should never be reached due to the exception handling above
    return []


def bulk_insert(cursor, table_name: str, columns: List[str], records: List[Tuple],
                batch_size: int = 1000, max_retries: int = 3) -> int:
    """
    Bulk insert records with batching and error handling.
    
    Args:
        cursor: MonkDB cursor object
        table_name: Target table name
        columns: List of column names
        records: List of record tuples
        batch_size: Number of records per batch
        max_retries: Maximum retry attempts per batch
        
    Returns:
        Total number of records inserted
        
    Raises:
        MonkDBOperationError: If bulk insert fails
    """
    if not records:
        logger.info("No records to insert")
        return 0

    placeholders = ', '.join(['?' for _ in columns])
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    total_inserted = 0
    total_batches = (len(records) + batch_size - 1) // batch_size

    logger.info(
        "Starting bulk insert",
        table=table_name,
        total_records=len(records),
        batch_size=batch_size,
        total_batches=total_batches
    )

    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(records))
        batch = records[start_idx:end_idx]

        for attempt in range(max_retries):
            try:
                cursor.executemany(query, batch)

                batch_inserted = len(batch)
                total_inserted += batch_inserted

                logger.debug(
                    "Batch inserted successfully",
                    batch_num=batch_num + 1,
                    batch_size=batch_inserted,
                    total_inserted=total_inserted
                )
                break

            except Exception as e:

                if attempt == max_retries - 1:
                    logger.error(
                        "Batch insert failed after all retries",
                        batch_num=batch_num + 1,
                        batch_size=len(batch),
                        attempts=max_retries,
                        error=str(e)
                    )
                    raise MonkDBOperationError(
                        f"Bulk insert failed for batch {batch_num + 1}: {e}"
                    ) from e

                # Exponential backoff
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "Batch insert failed, retrying",
                    batch_num=batch_num + 1,
                    attempt=attempt + 1,
                    wait_time=wait_time,
                    error=str(e)
                )
                time.sleep(wait_time)

    logger.info(
        "Bulk insert completed",
        table=table_name,
        total_inserted=total_inserted,
        total_batches=total_batches
    )

    return total_inserted


def upsert_dimension(cursor, table_name: str, business_key: str,
                    record_data: Dict[str, Any], scd_type: int = 2) -> str:
    """
    Upsert dimension record with SCD (Slowly Changing Dimension) support.
    
    Args:
        cursor: MonkDB cursor object
        table_name: Target dimension table name
        business_key: Business key column name
        record_data: Record data dictionary
        scd_type: SCD type (1 or 2)
        
    Returns:
        Operation type: 'inserted', 'updated', or 'unchanged'
        
    Raises:
        MonkDBOperationError: If upsert operation fails
    """
    try:
        business_key_value = record_data.get(business_key)
        if not business_key_value:
            raise ValueError(f"Business key '{business_key}' not found in record data")

        # Check for existing active record
        if scd_type == 2:
            existing_query = f"""
            SELECT * FROM {table_name} 
            WHERE {business_key} = ? AND (valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP)
            """
        else:
            existing_query = f"""
            SELECT * FROM {table_name} 
            WHERE {business_key} = ?
            """

        existing_records = execute_with_retry(cursor, existing_query, (business_key_value,))

        if existing_records:
            existing_record = existing_records[0]

            # Check if data has changed (excluding metadata columns)
            metadata_columns = {'valid_from', 'valid_to', 'is_current', 'created_at', 'updated_at'}
            data_columns = [col for col in record_data.keys() if col not in metadata_columns]

            has_changes = False
            for col in data_columns:
                if col in existing_record and existing_record[col] != record_data.get(col):
                    has_changes = True
                    break

            if not has_changes:
                logger.debug(
                    "No changes detected for dimension record",
                    table=table_name,
                    business_key=business_key,
                    business_key_value=business_key_value
                )
                return 'unchanged'

            if scd_type == 2:
                # SCD Type 2: Close existing record and insert new one
                close_query = f"""
                UPDATE {table_name} 
                SET valid_to = CURRENT_TIMESTAMP, is_current = FALSE
                WHERE {business_key} = ? AND (valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP)
                """
                execute_with_retry(cursor, close_query, (business_key_value,))

                # Insert new record with SCD Type 2 fields
                record_data['valid_from'] = 'CURRENT_TIMESTAMP'
                record_data['valid_to'] = None
                record_data['is_current'] = True

                _insert_record(cursor, table_name, record_data)

                logger.info(
                    "SCD Type 2 update completed",
                    table=table_name,
                    business_key=business_key,
                    business_key_value=business_key_value
                )
                return 'updated'

            else:
                # SCD Type 1: Update existing record
                update_columns = [f"{col} = ?" for col in data_columns if col != business_key]
                update_values = [record_data[col] for col in data_columns if col != business_key]
                update_values.append(business_key_value)

                update_query = f"""
                UPDATE {table_name} 
                SET {', '.join(update_columns)}, updated_at = CURRENT_TIMESTAMP
                WHERE {business_key} = ?
                """
                execute_with_retry(cursor, update_query, tuple(update_values))

                logger.info(
                    "SCD Type 1 update completed",
                    table=table_name,
                    business_key=business_key,
                    business_key_value=business_key_value
                )
                return 'updated'

        else:
            # Insert new record
            if scd_type == 2:
                record_data['valid_from'] = 'CURRENT_TIMESTAMP'
                record_data['valid_to'] = None
                record_data['is_current'] = True

            _insert_record(cursor, table_name, record_data)

            logger.info(
                "New dimension record inserted",
                table=table_name,
                business_key=business_key,
                business_key_value=business_key_value
            )
            return 'inserted'

    except Exception as e:
        logger.error(
            "Dimension upsert failed",
            table=table_name,
            business_key=business_key,
            error=str(e)
        )
        raise MonkDBOperationError(f"Dimension upsert failed: {e}") from e


def _insert_record(cursor, table_name: str, record_data: Dict[str, Any]) -> None:
    """
    Insert a single record into the specified table.
    
    Args:
        cursor: MonkDB cursor object
        table_name: Target table name
        record_data: Record data dictionary
    """
    columns = list(record_data.keys())
    values = list(record_data.values())
    placeholders = ', '.join(['?' for _ in columns])

    # Handle special timestamp values
    processed_values: List[Any] = []
    for value in values:
        if value == 'CURRENT_TIMESTAMP':
            processed_values.append(None)  # Let database handle current timestamp
        else:
            processed_values.append(value)

    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    # Replace CURRENT_TIMESTAMP placeholders in query
    if 'CURRENT_TIMESTAMP' in record_data.values():
        for i, value in enumerate(record_data.values()):
            if value == 'CURRENT_TIMESTAMP':
                query = query.replace('?', 'CURRENT_TIMESTAMP', 1)
                processed_values.pop(i)

    execute_with_retry(cursor, query, tuple(processed_values))


def get_dimension_key(cursor, table_name: str, business_key: str,
                     business_key_value: Any) -> Optional[int]:
    """
    Get dimension key for a given business key value.
    
    Args:
        cursor: MonkDB cursor object
        table_name: Dimension table name
        business_key: Business key column name
        business_key_value: Business key value to lookup
        
    Returns:
        Dimension key (surrogate key) or None if not found
    """
    try:
        # Determine the surrogate key column name (convention: table_name + '_key')
        key_column = table_name.replace('dim_', '') + '_key'

        query = f"""
        SELECT {key_column} FROM {table_name} 
        WHERE {business_key} = ? AND (valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP)
        """

        results = execute_with_retry(cursor, query, (business_key_value,))

        if results:
            return results[0][0]  # Return the key value

        return None

    except Exception as e:
        logger.error(
            "Failed to get dimension key",
            table=table_name,
            business_key=business_key,
            business_key_value=business_key_value,
            error=str(e)
        )
        return None


def bulk_upsert_facts(cursor, table_name: str, records: List[Dict[str, Any]],
                     unique_key: str = 'ride_id', batch_size: int = 1000) -> Dict[str, int]:
    """
    Bulk upsert fact records with deduplication.
    
    Args:
        cursor: MonkDB cursor object
        table_name: Target fact table name
        records: List of fact record dictionaries
        unique_key: Column name for deduplication
        batch_size: Number of records per batch
        
    Returns:
        Dictionary with operation counts: {'inserted': int, 'updated': int, 'skipped': int}
    """
    if not records:
        return {'inserted': 0, 'updated': 0, 'skipped': 0}

    logger.info(
        "Starting bulk fact upsert",
        table=table_name,
        total_records=len(records),
        unique_key=unique_key
    )

    # Deduplicate records by unique key (keep last occurrence)
    unique_records = {}
    for record in records:
        key_value = record.get(unique_key)
        if key_value:
            unique_records[key_value] = record

    deduped_records = list(unique_records.values())
    skipped_count = len(records) - len(deduped_records)

    if skipped_count > 0:
        logger.info(f"Deduplicated {skipped_count} duplicate records")

    # Process in batches
    inserted_count = 0
    updated_count = 0

    for i in range(0, len(deduped_records), batch_size):
        batch = deduped_records[i:i + batch_size]
        batch_results = _upsert_fact_batch(cursor, table_name, batch, unique_key)

        inserted_count += batch_results['inserted']
        updated_count += batch_results['updated']

    results = {
        'inserted': inserted_count,
        'updated': updated_count,
        'skipped': skipped_count
    }

    logger.info(
        "Bulk fact upsert completed",
        table=table_name,
        results=results
    )

    return results


def _upsert_fact_batch(cursor, table_name: str, records: List[Dict[str, Any]],
                      unique_key: str) -> Dict[str, int]:
    """
    Upsert a batch of fact records.
    
    Args:
        cursor: MonkDB cursor object
        table_name: Target fact table name
        records: Batch of fact record dictionaries
        unique_key: Column name for deduplication
        
    Returns:
        Dictionary with batch operation counts
    """
    inserted_count = 0
    updated_count = 0

    for record in records:
        unique_value = record.get(unique_key)
        if not unique_value:
            logger.warning(f"Record missing unique key '{unique_key}', skipping")
            continue

        # Check if record exists
        check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {unique_key} = ?"
        results = execute_with_retry(cursor, check_query, (unique_value,))
        exists = results[0][0] > 0

        if exists:
            # Update existing record
            update_columns = [f"{col} = ?" for col in record.keys() if col != unique_key]
            update_values = [record[col] for col in record.keys() if col != unique_key]
            update_values.append(unique_value)

            update_query = f"""
            UPDATE {table_name} 
            SET {', '.join(update_columns)}, updated_at = CURRENT_TIMESTAMP
            WHERE {unique_key} = ?
            """
            execute_with_retry(cursor, update_query, tuple(update_values))
            updated_count += 1
        else:
            # Insert new record
            record['created_at'] = 'CURRENT_TIMESTAMP'
            record['updated_at'] = 'CURRENT_TIMESTAMP'
            _insert_record(cursor, table_name, record)
            inserted_count += 1


    return {'inserted': inserted_count, 'updated': updated_count}
