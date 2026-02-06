# Databricks ETL Notebooks

## events_bronze_to_silver.py

Transforms Polymarket events from Bronze (JSONL in ADLS) to Silver (Delta tables).

### Usage

1. Import the notebook into Databricks workspace
2. Attach to a cluster with access to ADLS storage
3. Configure the variables at the top:
   - `STORAGE_ACCOUNT`: Your ADLS storage account name
   - `CONTAINER`: Container name (default: `polypoly`)
   - `INGEST_DATE`: Specific date to process, or `None` for all

4. Run all cells

### Tables Created

| Table | Description | Records (approx) |
|-------|-------------|------------------|
| `silver.events` | Prediction events | 189K |
| `silver.markets` | Trading markets | 442K |
| `silver.series` | Event series (deduplicated) | 900 |
| `silver.tags` | Tags (deduplicated) | 4.7K |
| `silver.event_tags` | Event-tag junction | 1.1M |
| `silver.event_creators` | External creators | 127 |

### Storage Paths

- **Bronze source**: `abfss://polypoly@{account}.dfs.core.windows.net/bronze/polymarket/events/mode=backfill/`
- **Silver destination**: `abfss://polypoly@{account}.dfs.core.windows.net/silver/polymarket/`

### Data Quality

- Fields marked `NOT NULL` in schema will fail if missing
- Boolean fields default to `FALSE` if missing
- Timestamps are parsed from ISO 8601 strings
- Series and tags are deduplicated by slug/id (keeping latest by `updatedAt`)

### Unity Catalog Registration

Uncomment the last cell to register tables in Unity Catalog:

```sql
CREATE TABLE polypoly.silver.events USING DELTA LOCATION '...'
```
