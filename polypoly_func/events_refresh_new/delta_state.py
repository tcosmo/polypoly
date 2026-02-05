"""
Delta table state management for refresh job tracking.

Uses Databricks DeltaClient (same as backfill) for Delta operations.
"""
import json
import logging
from typing import Optional

from events_refresh_new.db_client import get_delta_client
from events_refresh_new.config import INITIAL_OFFSET, LIMIT, STATE_TABLE_PATH

logger = logging.getLogger(__name__)


def table_exists() -> bool:
    """Check if the state table exists."""
    client = get_delta_client()
    code = f'''
import json
try:
    df = spark.read.format("delta").load("{STATE_TABLE_PATH}")
    print(json.dumps({{"exists": True, "count": df.count()}}))
except Exception as e:
    if "is not a Delta table" in str(e) or "Path does not exist" in str(e):
        print(json.dumps({{"exists": False}}))
    else:
        print(json.dumps({{"exists": False, "error": str(e)}}))
'''
    result = client.run_delta_operation(code)
    try:
        data = json.loads(result)
        return data.get("exists", False)
    except json.JSONDecodeError:
        return False


def create_table_with_seed():
    """
    Create the state table and seed it with the initial offset.
    """
    logger.info(f"Creating state table at {STATE_TABLE_PATH}")
    client = get_delta_client()

    code = f'''
import json
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType

schema = StructType([
    StructField("offset", LongType(), False),
    StructField("limit", IntegerType(), True),
    StructField("first_run_at", TimestampType(), True),
    StructField("last_run_at", TimestampType(), True),
    StructField("records_fetched", IntegerType(), True),
    StructField("last_bronze_path", StringType(), True),
    StructField("last_error", StringType(), True),
    StructField("last_error_at", TimestampType(), True),
])

# Create seed data
seed_data = [({INITIAL_OFFSET}, {LIMIT}, None, None, None, None, None, None)]
df = spark.createDataFrame(seed_data, schema)

# Write as Delta table
df.write.format("delta").mode("overwrite").save("{STATE_TABLE_PATH}")

print(json.dumps({{"status": "ok", "seed_offset": {INITIAL_OFFSET}}}))
'''
    result = client.run_delta_operation(code)
    logger.info(f"Create table result: {result}")


def get_next_offset() -> int:
    """
    Get the next offset to fetch.

    Logic:
    1. SELECT MIN(offset) WHERE records_fetched IS NULL OR records_fetched < LIMIT
    2. If NULL (all complete): SELECT MAX(offset) + LIMIT
    """
    client = get_delta_client()
    code = f'''
import json

df = spark.read.format("delta").load("{STATE_TABLE_PATH}")

# Find incomplete offsets (NULL or partial page)
incomplete = df.filter((df.records_fetched.isNull()) | (df.records_fetched < {LIMIT}))
min_incomplete = incomplete.agg({{"offset": "min"}}).collect()[0][0]

if min_incomplete is not None:
    print(json.dumps({{"next_offset": min_incomplete, "reason": "incomplete"}}))
else:
    # All complete - advance past max
    max_offset = df.agg({{"offset": "max"}}).collect()[0][0]
    next_offset = max_offset + {LIMIT} if max_offset is not None else {INITIAL_OFFSET}
    print(json.dumps({{"next_offset": next_offset, "reason": "advance"}}))
'''
    result = client.run_delta_operation(code)
    try:
        # Handle result that might have extra output before JSON
        json_start = result.find('{')
        if json_start > 0:
            result = result[json_start:]
        data = json.loads(result)
        next_offset = data["next_offset"]
        logger.debug(f"Next offset: {next_offset} (reason: {data.get('reason')})")
        return next_offset
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Failed to parse next_offset result: {result}")
        raise


def upsert_state(
    offset: int,
    records_fetched: int,
    last_bronze_path: str,
):
    """
    Upsert state for an offset after successful fetch.
    """
    client = get_delta_client()
    # Escape single quotes in path
    path_escaped = last_bronze_path.replace("'", "''")

    code = f'''
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp, coalesce

path = "{STATE_TABLE_PATH}"
delta_table = DeltaTable.forPath(spark, path)

# Check if row exists
existing = spark.read.format("delta").load(path).filter(f"offset = {offset}").count()

if existing > 0:
    # Update existing row
    delta_table.update(
        condition=f"offset = {offset}",
        set={{
            "last_run_at": "current_timestamp()",
            "records_fetched": "{records_fetched}",
            "last_bronze_path": "'{path_escaped}'",
            "last_error": "NULL",
            "last_error_at": "NULL",
        }}
    )
    print(json.dumps({{"status": "updated", "offset": {offset}}}))
else:
    # Insert new row
    from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType
    from datetime import datetime

    schema = StructType([
        StructField("offset", LongType(), False),
        StructField("limit", IntegerType(), True),
        StructField("first_run_at", TimestampType(), True),
        StructField("last_run_at", TimestampType(), True),
        StructField("records_fetched", IntegerType(), True),
        StructField("last_bronze_path", StringType(), True),
        StructField("last_error", StringType(), True),
        StructField("last_error_at", TimestampType(), True),
    ])

    now = datetime.utcnow()
    new_row = spark.createDataFrame(
        [({offset}, {LIMIT}, now, now, {records_fetched}, '{path_escaped}', None, None)],
        schema
    )
    new_row.write.format("delta").mode("append").save(path)
    print(json.dumps({{"status": "inserted", "offset": {offset}}}))
'''
    result = client.run_delta_operation(code)
    logger.debug(f"Upsert result for offset={offset}: {result}")


def upsert_error(offset: int, error: str):
    """
    Record an error for an offset.
    """
    client = get_delta_client()
    # Escape single quotes and sanitize error message
    error_escaped = error.replace("'", "''").replace("\n", " ").replace("\r", "")[:500]

    code = f'''
import json
from delta.tables import DeltaTable
from datetime import datetime

path = "{STATE_TABLE_PATH}"
delta_table = DeltaTable.forPath(spark, path)

# Check if row exists
existing = spark.read.format("delta").load(path).filter(f"offset = {offset}").count()

if existing > 0:
    # Update existing row with error
    delta_table.update(
        condition=f"offset = {offset}",
        set={{
            "last_run_at": "current_timestamp()",
            "last_error": "'{error_escaped}'",
            "last_error_at": "current_timestamp()",
        }}
    )
    print(json.dumps({{"status": "updated_error", "offset": {offset}}}))
else:
    # Insert new row with error
    from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType

    schema = StructType([
        StructField("offset", LongType(), False),
        StructField("limit", IntegerType(), True),
        StructField("first_run_at", TimestampType(), True),
        StructField("last_run_at", TimestampType(), True),
        StructField("records_fetched", IntegerType(), True),
        StructField("last_bronze_path", StringType(), True),
        StructField("last_error", StringType(), True),
        StructField("last_error_at", TimestampType(), True),
    ])

    now = datetime.utcnow()
    new_row = spark.createDataFrame(
        [({offset}, {LIMIT}, now, now, None, None, '{error_escaped}', now)],
        schema
    )
    new_row.write.format("delta").mode("append").save(path)
    print(json.dumps({{"status": "inserted_error", "offset": {offset}}}))
'''
    result = client.run_delta_operation(code)
    logger.debug(f"Upsert error result for offset={offset}: {result}")


def get_previous_count(offset: int) -> Optional[int]:
    """
    Get the previous records_fetched for an offset.
    Returns None if offset doesn't exist or has no records_fetched.
    """
    client = get_delta_client()
    code = f'''
import json

df = spark.read.format("delta").load("{STATE_TABLE_PATH}")
row = df.filter(f"offset = {offset}").select("records_fetched").collect()

if row and row[0][0] is not None:
    print(json.dumps({{"count": row[0][0]}}))
else:
    print(json.dumps({{"count": None}}))
'''
    result = client.run_delta_operation(code)
    try:
        json_start = result.find('{')
        if json_start > 0:
            result = result[json_start:]
        data = json.loads(result)
        return data.get("count")
    except (json.JSONDecodeError, KeyError):
        return None


def get_status() -> dict:
    """Get summary status of the refresh state."""
    if not table_exists():
        return {"exists": False}

    client = get_delta_client()
    code = f'''
import json

df = spark.read.format("delta").load("{STATE_TABLE_PATH}")

total_offsets = df.count()
complete = df.filter(df.records_fetched == {LIMIT}).count()
partial = df.filter((df.records_fetched.isNotNull()) & (df.records_fetched < {LIMIT})).count()
pending = df.filter(df.records_fetched.isNull()).count()
with_errors = df.filter(df.last_error.isNotNull()).count()

# Sum of records fetched (handle nulls)
total_records = df.agg({{"records_fetched": "sum"}}).collect()[0][0] or 0

# Get next offset
incomplete = df.filter((df.records_fetched.isNull()) | (df.records_fetched < {LIMIT}))
min_incomplete = incomplete.agg({{"offset": "min"}}).collect()[0][0]

if min_incomplete is not None:
    next_offset = min_incomplete
else:
    max_offset = df.agg({{"offset": "max"}}).collect()[0][0]
    next_offset = max_offset + {LIMIT} if max_offset is not None else {INITIAL_OFFSET}

print(json.dumps({{
    "exists": True,
    "total_offsets": total_offsets,
    "complete": complete,
    "partial": partial,
    "pending": pending,
    "with_errors": with_errors,
    "total_records_fetched": int(total_records),
    "next_offset": next_offset,
}}))
'''
    result = client.run_delta_operation(code)
    try:
        json_start = result.find('{')
        if json_start > 0:
            result = result[json_start:]
        return json.loads(result)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse status result: {result}")
        return {"exists": True, "error": str(e)}
