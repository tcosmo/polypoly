# Databricks notebook source
# MAGIC %md
# MAGIC # Events Refresh Monitor
# MAGIC Dashboard to monitor the state of the refresh jobs Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup ADLS Credentials

# COMMAND ----------

storage_account = "polybotdlso94zgo"
container = "polypoly"
STATE_TABLE_PATH = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/_meta/refresh_state_events"

client_id = dbutils.secrets.get(scope="keyv-polybot", key="sp-client-id")
client_secret = dbutils.secrets.get(scope="keyv-polybot", key="sp-client-secret")
tenant_id = dbutils.secrets.get(scope="keyv-polybot", key="tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
)
print(f"ADLS credentials configured")
print(f"State table: {STATE_TABLE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Delta Table

# COMMAND ----------

df = spark.read.format("delta").load(STATE_TABLE_PATH)
df.createOrReplaceTempView("refresh_state")
print(f"Loaded {df.count()} offset records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Offset Status Overview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN records_fetched IS NULL THEN 'pending'
# MAGIC     WHEN records_fetched = 20 THEN 'complete'
# MAGIC     ELSE 'partial'
# MAGIC   END as status,
# MAGIC   COUNT(*) as offset_count,
# MAGIC   SUM(COALESCE(records_fetched, 0)) as total_records,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
# MAGIC FROM refresh_state
# MAGIC GROUP BY 1
# MAGIC ORDER BY
# MAGIC   CASE status
# MAGIC     WHEN 'complete' THEN 1
# MAGIC     WHEN 'partial' THEN 2
# MAGIC     WHEN 'pending' THEN 3
# MAGIC   END

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Offset to Fetch

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH incomplete AS (
# MAGIC   SELECT MIN(offset) as next_offset, 'incomplete' as reason
# MAGIC   FROM refresh_state
# MAGIC   WHERE records_fetched IS NULL OR records_fetched < 20
# MAGIC ),
# MAGIC advance AS (
# MAGIC   SELECT MAX(offset) + 20 as next_offset, 'advance' as reason
# MAGIC   FROM refresh_state
# MAGIC   WHERE records_fetched = 20
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(i.next_offset, a.next_offset) as next_offset,
# MAGIC   CASE WHEN i.next_offset IS NOT NULL THEN 'incomplete' ELSE 'advance' END as reason
# MAGIC FROM incomplete i
# MAGIC CROSS JOIN advance a

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent Activity (Last 24 Hours)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_TRUNC('hour', last_run_at) as hour,
# MAGIC   COUNT(*) as offsets_updated,
# MAGIC   SUM(records_fetched) as records_fetched
# MAGIC FROM refresh_state
# MAGIC WHERE last_run_at >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partial Pages (At the Edge)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   offset,
# MAGIC   records_fetched,
# MAGIC   first_run_at,
# MAGIC   last_run_at,
# MAGIC   last_bronze_path
# MAGIC FROM refresh_state
# MAGIC WHERE records_fetched IS NOT NULL AND records_fetched < 20
# MAGIC ORDER BY offset DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent Errors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   offset,
# MAGIC   records_fetched,
# MAGIC   last_error,
# MAGIC   last_error_at,
# MAGIC   last_run_at
# MAGIC FROM refresh_state
# MAGIC WHERE last_error IS NOT NULL
# MAGIC ORDER BY last_error_at DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_offsets,
# MAGIC   COUNT(*) FILTER (WHERE records_fetched = 20) as complete_offsets,
# MAGIC   COUNT(*) FILTER (WHERE records_fetched IS NOT NULL AND records_fetched < 20) as partial_offsets,
# MAGIC   COUNT(*) FILTER (WHERE records_fetched IS NULL) as pending_offsets,
# MAGIC   COUNT(*) FILTER (WHERE last_error IS NOT NULL) as offsets_with_errors,
# MAGIC   SUM(COALESCE(records_fetched, 0)) as total_records_fetched,
# MAGIC   MIN(offset) as min_offset,
# MAGIC   MAX(offset) as max_offset,
# MAGIC   MIN(first_run_at) as earliest_run,
# MAGIC   MAX(last_run_at) as latest_run
# MAGIC FROM refresh_state

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Offsets (Recent First)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   offset,
# MAGIC   `limit`,
# MAGIC   records_fetched,
# MAGIC   first_run_at,
# MAGIC   last_run_at,
# MAGIC   CASE
# MAGIC     WHEN records_fetched IS NULL THEN 'pending'
# MAGIC     WHEN records_fetched = 20 THEN 'complete'
# MAGIC     ELSE 'partial'
# MAGIC   END as status,
# MAGIC   last_bronze_path,
# MAGIC   last_error
# MAGIC FROM refresh_state
# MAGIC ORDER BY offset DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset Table (Run Manually)
# MAGIC Uncomment and run to reset the state table with a new seed offset:

# COMMAND ----------

# from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType
#
# INITIAL_OFFSET = 188448  # Adjust as needed
# LIMIT = 20
#
# schema = StructType([
#     StructField("offset", LongType(), False),
#     StructField("limit", IntegerType(), True),
#     StructField("first_run_at", TimestampType(), True),
#     StructField("last_run_at", TimestampType(), True),
#     StructField("records_fetched", IntegerType(), True),
#     StructField("last_bronze_path", StringType(), True),
#     StructField("last_error", StringType(), True),
#     StructField("last_error_at", TimestampType(), True),
# ])
#
# seed_data = [(INITIAL_OFFSET, LIMIT, None, None, None, None, None, None)]
# df = spark.createDataFrame(seed_data, schema)
# df.write.format("delta").mode("overwrite").save(STATE_TABLE_PATH)
# print(f"Reset state table with seed offset={INITIAL_OFFSET}")
