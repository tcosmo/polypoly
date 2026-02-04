"""
Create and prefill the backfill_jobs_events Delta table.

Run this once to initialize the events backfill job queue.
Uses direct Delta path (no metastore registration).
"""
from events_backfill.config import (
    JOBS_TABLE_PATH,
    MAX_OFFSET,
    LIMIT,
)
from events_backfill.db_client import get_delta_client


def create_table():
    """Create the Delta table if it doesn't exist."""
    client = get_delta_client()

    code = f'''
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
from delta.tables import DeltaTable

path = "{JOBS_TABLE_PATH}"

# Check if table already exists
if DeltaTable.isDeltaTable(spark, path):
    print("Table already exists")
else:
    schema = StructType([
        StructField("job_id", StringType(), False),
        StructField("offset", LongType(), False),
        StructField("limit", IntegerType(), False),
        StructField("state", StringType(), False),
        StructField("leased_by", StringType(), True),
        StructField("lease_expires_at", TimestampType(), True),
        StructField("attempts", IntegerType(), False),
        StructField("next_attempt_at", TimestampType(), True),
        StructField("last_error_code", StringType(), True),
        StructField("last_error_message", StringType(), True),
        StructField("last_error_at", TimestampType(), True),
        StructField("done_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("payload_path", StringType(), True),
        StructField("records_written", IntegerType(), True),
    ])

    # Create empty Delta table
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").save(path)
    print("Table created")
'''
    result = client.run_delta_operation(code)
    print(f"Table ready at {JOBS_TABLE_PATH}")
    if result:
        print(f"  {result}")


def get_row_count() -> int:
    """Get current row count."""
    client = get_delta_client()
    code = f'''
df = spark.read.format("delta").load("{JOBS_TABLE_PATH}")
print(df.count())
'''
    result = client.run_delta_operation(code)
    return int(result) if result else 0


def prefill_jobs():
    """Insert all job records (offset 0 to MAX_OFFSET, step LIMIT)."""
    client = get_delta_client()

    existing_count = get_row_count()
    if existing_count > 0:
        print(f"Table already has {existing_count} jobs, skipping prefill")
        return

    num_jobs = (MAX_OFFSET // LIMIT) + 1
    print(f"Inserting {num_jobs} jobs (offset 0 to {MAX_OFFSET}, step {LIMIT})...")

    # Insert in batches
    batch_size = 500
    for batch_start in range(0, num_jobs, batch_size):
        batch_end = min(batch_start + batch_size, num_jobs)

        code = f'''
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
from datetime import datetime

path = "{JOBS_TABLE_PATH}"
now = datetime.utcnow()

# Use explicit schema matching the Delta table
schema = StructType([
    StructField("job_id", StringType(), False),
    StructField("offset", LongType(), False),
    StructField("limit", IntegerType(), False),
    StructField("state", StringType(), False),
    StructField("leased_by", StringType(), True),
    StructField("lease_expires_at", TimestampType(), True),
    StructField("attempts", IntegerType(), False),
    StructField("next_attempt_at", TimestampType(), True),
    StructField("last_error_code", StringType(), True),
    StructField("last_error_message", StringType(), True),
    StructField("last_error_at", TimestampType(), True),
    StructField("done_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("payload_path", StringType(), True),
    StructField("records_written", IntegerType(), True),
])

rows = []
for i in range({batch_start}, {batch_end}):
    offset = i * {LIMIT}
    job_id = f"offset={{offset}}&limit={LIMIT}"
    rows.append((
        job_id,
        offset,  # will be cast to LongType by schema
        {LIMIT},
        "pending",
        None,  # leased_by
        None,  # lease_expires_at
        0,     # attempts
        now,   # next_attempt_at
        None,  # last_error_code
        None,  # last_error_message
        None,  # last_error_at
        None,  # done_at
        now,   # updated_at
        now,   # created_at
        None,  # payload_path
        None,  # records_written
    ))

try:
    df = spark.createDataFrame(rows, schema)
    df.write.format("delta").mode("append").save(path)
    print(f"Inserted {{len(rows)}} jobs")
except Exception as e:
    print(f"ERROR: {{e}}")
'''
        result = client.run_delta_operation(code, timeout_seconds=120)
        print(f"  Batch {batch_start}-{batch_end - 1}: {result}")

    print(f"Prefilled {num_jobs} jobs")


def main():
    print("Setting up events backfill jobs table...")

    create_table()
    prefill_jobs()

    print("Done!")


if __name__ == "__main__":
    main()
