import os
from dotenv import load_dotenv

load_dotenv()

# Databricks config
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID", "")

# Azure Storage
ADLS_ACCOUNT_NAME = os.environ.get("ADLS_ACCOUNT_NAME", "polybotdlso94zgo")

# For production, set ADLS_ACCOUNT_NAME_BRONZE to "datalakeprod"
# For local testing, use same account as state table (polybotdlso94zgo)
ADLS_ACCOUNT_NAME_BRONZE = os.environ.get("ADLS_ACCOUNT_NAME_BRONZE", ADLS_ACCOUNT_NAME)

# Refresh settings
POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"
INITIAL_OFFSET = 188_448  # First offset not covered by backfill
LIMIT = 20

# Rate limiting: 500 req / 10s = 50 req/s, but we'll be conservative
REQUESTS_PER_SECOND = 40

# Storage paths
CONTAINER = "polypoly"
STATE_TABLE_PATH = f"abfss://{CONTAINER}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/bronze/_meta/refresh_state_events"

# Bronze path template (mode=refresh_new to distinguish from backfill)
# Container depends on storage account:
# - polybotdlso94zgo: use "polypoly" container with bronze/ prefix
# - datalakeprod: use "bronze" container directly
BRONZE_CONTAINER = os.environ.get("BRONZE_CONTAINER", CONTAINER)
BRONZE_PATH_PREFIX = "bronze/" if ADLS_ACCOUNT_NAME_BRONZE == ADLS_ACCOUNT_NAME else ""
BRONZE_PATH_TEMPLATE = (
    BRONZE_PATH_PREFIX + "polymarket/events/mode=refresh_new/ingest_date={ingest_date}/"
    "offset={offset:05d}_limit={limit}_{time}.jsonl"
)
