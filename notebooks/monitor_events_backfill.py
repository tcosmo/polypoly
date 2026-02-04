# Databricks notebook source
# MAGIC %md
# MAGIC # Events Backfill Monitor
# MAGIC Dashboard to monitor the state of the backfill jobs Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup ADLS Credentials

# COMMAND ----------

storage_account = "polybotdlso94zgo"
container = "polypoly"
JOBS_TABLE_PATH = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/_meta/backfill_jobs_events"

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
print(f"Jobs table: {JOBS_TABLE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Delta Table

# COMMAND ----------

df = spark.read.format("delta").load(JOBS_TABLE_PATH)
df.createOrReplaceTempView("jobs")
print(f"Loaded {df.count()} jobs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Status Overview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   state,
# MAGIC   COUNT(*) as job_count,
# MAGIC   SUM(COALESCE(records_written, 0)) as total_records,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
# MAGIC FROM jobs
# MAGIC GROUP BY state
# MAGIC ORDER BY
# MAGIC   CASE state
# MAGIC     WHEN 'done' THEN 1
# MAGIC     WHEN 'leased' THEN 2
# MAGIC     WHEN 'pending' THEN 3
# MAGIC     WHEN 'failed' THEN 4
# MAGIC   END

# COMMAND ----------

# MAGIC %md
# MAGIC ## Progress Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_TRUNC('hour', done_at) as hour,
# MAGIC   COUNT(*) as jobs_completed,
# MAGIC   SUM(records_written) as records_written
# MAGIC FROM jobs
# MAGIC WHERE state = 'done' AND done_at IS NOT NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Active Leases

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   offset,
# MAGIC   leased_by,
# MAGIC   lease_expires_at,
# MAGIC   CASE
# MAGIC     WHEN lease_expires_at < CURRENT_TIMESTAMP() THEN 'EXPIRED'
# MAGIC     ELSE 'ACTIVE'
# MAGIC   END as lease_status,
# MAGIC   attempts
# MAGIC FROM jobs
# MAGIC WHERE state = 'leased'
# MAGIC ORDER BY lease_expires_at

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent Errors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   offset,
# MAGIC   attempts,
# MAGIC   last_error_code,
# MAGIC   last_error_message,
# MAGIC   last_error_at,
# MAGIC   next_attempt_at
# MAGIC FROM jobs
# MAGIC WHERE last_error_code IS NOT NULL
# MAGIC ORDER BY last_error_at DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failed Jobs (Exhausted Retries)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   offset,
# MAGIC   attempts,
# MAGIC   last_error_code,
# MAGIC   last_error_message,
# MAGIC   last_error_at
# MAGIC FROM jobs
# MAGIC WHERE state = 'failed'
# MAGIC ORDER BY last_error_at DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pending Jobs Ready to Process

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as ready_jobs,
# MAGIC   MIN(offset) as min_offset,
# MAGIC   MAX(offset) as max_offset
# MAGIC FROM jobs
# MAGIC WHERE state = 'pending'
# MAGIC   AND next_attempt_at <= CURRENT_TIMESTAMP()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobs Waiting for Retry (Backoff)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   job_id,
# MAGIC   offset,
# MAGIC   attempts,
# MAGIC   last_error_code,
# MAGIC   next_attempt_at,
# MAGIC   TIMESTAMPDIFF(MINUTE, CURRENT_TIMESTAMP(), next_attempt_at) as minutes_until_retry
# MAGIC FROM jobs
# MAGIC WHERE state = 'pending'
# MAGIC   AND next_attempt_at > CURRENT_TIMESTAMP()
# MAGIC ORDER BY next_attempt_at
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimated Completion

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH stats AS (
# MAGIC   SELECT
# MAGIC     COUNT(*) FILTER (WHERE state = 'done') as done_count,
# MAGIC     COUNT(*) FILTER (WHERE state != 'done') as remaining_count,
# MAGIC     COUNT(*) as total_count,
# MAGIC     MIN(done_at) FILTER (WHERE state = 'done') as first_done,
# MAGIC     MAX(done_at) FILTER (WHERE state = 'done') as last_done
# MAGIC   FROM jobs
# MAGIC ),
# MAGIC rate AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC       WHEN done_count > 0 AND first_done != last_done
# MAGIC       THEN done_count / (TIMESTAMPDIFF(SECOND, first_done, last_done) / 3600.0)
# MAGIC       ELSE NULL
# MAGIC     END as jobs_per_hour
# MAGIC   FROM stats
# MAGIC )
# MAGIC SELECT
# MAGIC   done_count,
# MAGIC   remaining_count,
# MAGIC   total_count,
# MAGIC   ROUND(done_count * 100.0 / total_count, 2) as pct_complete,
# MAGIC   ROUND(jobs_per_hour, 1) as jobs_per_hour,
# MAGIC   CASE
# MAGIC     WHEN jobs_per_hour > 0
# MAGIC     THEN ROUND(remaining_count / jobs_per_hour, 1)
# MAGIC     ELSE NULL
# MAGIC   END as estimated_hours_remaining
# MAGIC FROM rate

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset Failed Jobs (Run Manually)
# MAGIC Uncomment and run to reset all failed jobs back to pending:

# COMMAND ----------

# from delta.tables import DeltaTable
# from datetime import datetime
#
# delta_table = DeltaTable.forPath(spark, JOBS_TABLE_PATH)
# delta_table.update(
#     condition="state = 'failed'",
#     set={
#         "state": "'pending'",
#         "attempts": "0",
#         "next_attempt_at": "current_timestamp()",
#         "last_error_code": "NULL",
#         "last_error_message": "NULL",
#         "last_error_at": "NULL",
#         "updated_at": "current_timestamp()"
#     }
# )
# print("Reset all failed jobs to pending")
