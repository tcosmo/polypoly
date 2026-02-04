"""Debug ADLS connectivity and table creation from Databricks cluster."""
from events_backfill.db_client import get_sql_client
from events_backfill.config import CONTAINER, ADLS_ACCOUNT_NAME, TABLE_NAME, SCHEMA_LOCATION


def main():
    client = get_sql_client()

    # Test 1: Check catalogs and schemas
    print("Test 1: List catalogs...")
    try:
        result = client._run_python('''
df = spark.sql("SHOW CATALOGS")
for row in df.collect():
    print(row[0])
''')
        print(result if result else "(no output)")
    except Exception as e:
        print(f"Failed: {e}")

    # Test 2: List schemas in hive_metastore
    print("\nTest 2: List schemas in hive_metastore...")
    try:
        result = client._run_python('''
df = spark.sql("SHOW SCHEMAS IN hive_metastore")
for row in df.collect():
    print(row[0])
''')
        print(result if result else "(no output)")
    except Exception as e:
        print(f"Failed: {e}")

    # Test 3: Create schema
    print("\nTest 3: Create schema hive_metastore.meta...")
    schema_location = SCHEMA_LOCATION
    try:
        result = client._run_python(f'''
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.meta LOCATION '{schema_location}'")
    print("Schema created successfully")
except Exception as e:
    print(f"Error: {{e}}")
''')
        print(result if result else "(no output)")
    except Exception as e:
        print(f"Failed: {e}")

    # Test 4: Verify schema exists
    print("\nTest 4: Verify schema exists...")
    try:
        result = client._run_python('''
df = spark.sql("SHOW SCHEMAS IN hive_metastore")
for row in df.collect():
    print(row[0])
''')
        print(result if result else "(no output)")
    except Exception as e:
        print(f"Failed: {e}")

    # Test 5: Create simple table with location
    print("\nTest 5: Create Delta table with LOCATION...")
    table_location = f"abfss://{CONTAINER}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/bronze/_meta/{TABLE_NAME}"
    try:
        result = client._run_python(f'''
try:
    spark.sql("DROP TABLE IF EXISTS hive_metastore.meta.backfill_jobs_events")
    spark.sql("""
        CREATE TABLE hive_metastore.meta.backfill_jobs_events (
            job_id STRING,
            test_col STRING
        )
        USING DELTA
        LOCATION '{table_location}'
    """)
    print("Table created successfully")
except Exception as e:
    print(f"Error: {{e}}")
''')
        print(result if result else "(no output)")
    except Exception as e:
        print(f"Failed: {e}")

    # Test 6: Insert test data
    print("\nTest 6: Insert test data...")
    try:
        result = client._run_python('''
try:
    spark.sql("INSERT INTO hive_metastore.meta.backfill_jobs_events VALUES ('test1', 'hello')")
    print("Insert successful")
except Exception as e:
    print(f"Error: {e}")
''')
        print(result if result else "(no output)")
    except Exception as e:
        print(f"Failed: {e}")

    # Test 7: Check ADLS for Delta files
    print("\nTest 7: Check ADLS for Delta files...")
    try:
        result = client._run_python(f'''
try:
    files = dbutils.fs.ls("{table_location}")
    for f in files:
        print(f.path)
except Exception as e:
    print(f"Error: {{e}}")
''')
        print(result if result else "(no output)")
    except Exception as e:
        print(f"Failed: {e}")


if __name__ == "__main__":
    main()
