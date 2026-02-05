import os
from dotenv import load_dotenv

load_dotenv()

# Azure / Databricks
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID", "")
ADLS_ACCOUNT_NAME = os.environ["ADLS_ACCOUNT_NAME"]

# Backfill settings
POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"
MAX_OFFSET = 188_000
LIMIT = 500
LEASE_DURATION_SECONDS = 300  # 5 minutes
MAX_ATTEMPTS = 10

# Rate limiting: 500 req / 10s = 50 req/s, but we'll be conservative
REQUESTS_PER_SECOND = 40

# Storage container (single container with bronze/silver subfolders)
CONTAINER = "polypoly"

# Bronze path template
BRONZE_PATH_TEMPLATE = (
    "bronze/polymarket/events/mode=backfill/ingest_date={ingest_date}/"
    "offset={offset:010d}_limit={limit}.jsonl"
)

# Delta table path (direct path, no metastore)
JOBS_TABLE_PATH = f"abfss://{CONTAINER}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/bronze/_meta/backfill_jobs_events"

# Backoff schedule (seconds) by attempt number
BACKOFF_SCHEDULE = {
    1: 30,
    2: 120,  # 2 min
    3: 600,  # 10 min
    4: 3600,  # 1 hour
}
BACKOFF_CAP = 21600  # 6 hours
JITTER_PERCENT = 0.15
